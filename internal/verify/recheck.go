package verify

// Kiểm lại row nghi lệch cho mọi cặp engine (MySQL→MySQL, PG→PG, MySQL→PG; spec 02 §9, 04 §5 Gate 2
// "hot row báo unverifiable_hot"). Cùng quy trình với LiveVerifier nhưng so chuỗi chính tắc
// (canonicalValueExpr / rowExpression) nên dùng được khi hai phía khác engine:
//   1. đọc ảnh source của các khóa nghi lệch (S1), rồi đọc vị trí log P của source (P phủ mọi Txn
//      mà S1 thấy);
//   2. chờ tập đã apply của pipeline trên target phủ P (ApplyWaiter; nil = không có pipeline chạy,
//      bỏ bước này), đọc ảnh target (T);
//   3. đọc lại ảnh source (S2).
// S1 == S2 mà T ≠ S1 là lệch thật; S1 == S2 == T là khớp; S1 ≠ S2 (row đổi trong cửa sổ) thì thử lại,
// đổi trong mọi lần thử là unverifiable_hot — báo riêng, không báo lệch.
//
// Vị trí log theo engine source:
//   - MySQL: @@GLOBAL.gtid_executed; target theo kịp khi tập GTID đã apply chứa nó (Txn rỗng sau khi
//     lọc vẫn được ghi vào tập đã apply nên tập không có lỗ vĩnh viễn).
//   - PG: pg_current_wal_insert_lsn() (commit record được chèn vào WAL TRƯỚC khi Txn visible, nên mọi
//     Txn S1 thấy có commit LSN ≤ P — kể cả synchronous_commit=off); target theo kịp khi lwd của tập đã
//     apply ≥ P. Pipeline chỉ dời lwd tới LSN của Txn router đã nhận; source rảnh (hoặc chỉ có Txn của
//     bảng ngoài publication, pgoutput bỏ Txn rỗng) thì lwd không bao giờ tới P. Vì vậy ghi một
//     watermark (pg_logical_emit_message không transactional, prefix strand_watermark — cùng cơ chế
//     snapshot logic): reader phát nó thành Txn rỗng có LSN ≥ P, lwd được dời qua P. Watermark được
//     ghi lại mỗi giây trong lúc chờ (reader bỏ message rơi vào giữa một Txn đang giải mã). Source là
//     standby (pg_is_in_recovery): P = pg_last_wal_replay_lsn(), không ghi được watermark.

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

// DefaultRecheckRetries là số lần kiểm lại mặc định của một row nghi lệch.
const DefaultRecheckRetries = 5

// recheckVerdict là kết luận về một khóa sau một lần kiểm lại.
type recheckVerdict int

const (
	verdictMatch    recheckVerdict = iota // S1 == S2 == T
	verdictMismatch                       // S1 == S2, T khác: lệch thật
	verdictUnstable                       // S1 ≠ S2: row đổi trong cửa sổ, thử lại
)

// rowObs là ảnh của một khóa trong một lần kiểm lại; ok = false nghĩa là phía đó không có row.
type rowObs struct {
	s1, d, s2       string
	s1ok, dok, s2ok bool
}

// classifyObs phân loại một lần kiểm lại (xem chú thích đầu file).
func classifyObs(o rowObs) recheckVerdict {
	if o.s1ok != o.s2ok || o.s1 != o.s2 {
		return verdictUnstable
	}
	if o.s1ok != o.dok || o.s1 != o.d {
		return verdictMismatch
	}
	return verdictMatch
}

func obsOf(s1, d, s2 map[string]string, k string) rowObs {
	var o rowObs
	o.s1, o.s1ok = s1[k]
	o.d, o.dok = d[k]
	o.s2, o.s2ok = s2[k]
	return o
}

// ApplyMark là vị trí log của source đọc ngay sau một lần đọc source (S1).
type ApplyMark struct {
	gtid string // source MySQL: gtid_executed
	lsn  uint64 // source PG
	text string // dạng in được của vị trí (thông báo lỗi, nội dung watermark)
}

// ApplyWaiter chờ target apply tới vị trí log của source (bước 2 của kiểm lại).
type ApplyWaiter interface {
	// Mark đọc vị trí log hiện tại của source; gọi SAU lần đọc S1.
	Mark(ctx context.Context) (ApplyMark, error)
	// Wait chờ tập đã apply của pipeline trên target phủ m.
	Wait(ctx context.Context, m ApplyMark) error
}

// LogApplyWaiter là ApplyWaiter đọc vị trí log của source và tập đã apply trong _strand_checkpoint /
// _strand_worker_delta của pipeline trên target.
type LogApplyWaiter struct {
	Source       *sql.DB // kết nối source của pipeline (primary: PG cần ghi watermark)
	Meta         *sql.DB // kết nối tới schema chứa _strand_* trên target
	PipelineID   string
	SourceEngine model.Engine
	TargetEngine model.Engine
	Limit        time.Duration // tối đa chờ một mark (mặc định 2 phút)
	Poll         time.Duration // chu kỳ đọc lại tập đã apply (mặc định 100ms)

	pgVersion int // server_version_num của source PG (0 = chưa đọc)
}

func (w *LogApplyWaiter) limit() time.Duration {
	if w.Limit > 0 {
		return w.Limit
	}
	return 2 * time.Minute
}

func (w *LogApplyWaiter) poll() time.Duration {
	if w.Poll > 0 {
		return w.Poll
	}
	return 100 * time.Millisecond
}

// Mark — xem ApplyWaiter.
func (w *LogApplyWaiter) Mark(ctx context.Context) (ApplyMark, error) {
	if w.SourceEngine != model.EnginePG {
		var g string
		if err := w.Source.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&g); err != nil {
			return ApplyMark{}, fmt.Errorf("read source gtid_executed: %w", err)
		}
		g = strings.ReplaceAll(g, "\n", "")
		return ApplyMark{gtid: g, text: g}, nil
	}
	var standby bool
	var s string
	if err := w.Source.QueryRowContext(ctx, `SELECT pg_is_in_recovery(),
		(CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_insert_lsn() END)::text`).Scan(&standby, &s); err != nil {
		return ApplyMark{}, fmt.Errorf("read source WAL position: %w", err)
	}
	l, err := checkpoint.ParseLSN(s)
	if err != nil {
		return ApplyMark{}, err
	}
	m := ApplyMark{lsn: l, text: s}
	if !standby {
		if err := w.emitWatermark(ctx, m); err != nil {
			return ApplyMark{}, err
		}
	}
	return m, nil
}

func (w *LogApplyWaiter) emitWatermark(ctx context.Context, m ApplyMark) error {
	if w.pgVersion == 0 {
		var v string
		if err := w.Source.QueryRowContext(ctx, "SHOW server_version_num").Scan(&v); err != nil {
			return fmt.Errorf("read source server_version_num: %w", err)
		}
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return fmt.Errorf("parse server_version_num %q: %w", v, err)
		}
		w.pgVersion = n
	}
	return snapshot.EmitPGWatermark(ctx, w.Source, w.pgVersion, "verify "+m.text)
}

func (w *LogApplyWaiter) loadApplied(ctx context.Context) (checkpoint.AppliedSet, error) {
	if w.TargetEngine == model.EnginePG {
		return checkpoint.LoadAppliedSnapshotPG(ctx, w.Meta, w.PipelineID, w.SourceEngine)
	}
	return checkpoint.LoadAppliedSnapshotMySQL(ctx, w.Meta, w.PipelineID, w.SourceEngine)
}

// missing trả phần của m mà applied chưa phủ ("" = đã phủ).
func (m ApplyMark) missing(engine model.Engine, applied checkpoint.AppliedSet) (string, error) {
	if engine == model.EnginePG {
		if applied != nil && applied.Lwd().LSN >= m.lsn {
			return "", nil
		}
		return "WAL up to " + m.text, nil
	}
	want, err := checkpoint.ParseMySQLGTIDSet(m.gtid)
	if err != nil {
		return "", err
	}
	if applied == nil {
		return want.String(), nil
	}
	got, ok := applied.(*checkpoint.MySQLAppliedSet)
	if !ok {
		return "", fmt.Errorf("applied set of pipeline is %T, not a MySQL GTID set", applied)
	}
	want.Subtract(got)
	return want.String(), nil
}

// Wait — xem ApplyWaiter.
func (w *LogApplyWaiter) Wait(ctx context.Context, m ApplyMark) error {
	deadline := time.Now().Add(w.limit())
	lastMark := time.Now()
	for {
		applied, err := w.loadApplied(ctx)
		if err != nil {
			return err
		}
		miss, err := m.missing(w.SourceEngine, applied)
		if err != nil {
			return err
		}
		if miss == "" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("verify: target did not apply %s within %s", miss, w.limit())
		}
		if w.SourceEngine == model.EnginePG && time.Since(lastMark) >= time.Second {
			// Watermark trước có thể bị reader bỏ (rơi vào giữa một Txn): ghi lại. Lỗi (vd. source vừa
			// thành standby) không chặn việc chờ.
			_, _ = w.Mark(ctx)
			lastMark = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.poll()):
		}
	}
}

// sideRead đọc ảnh chính tắc của các khóa pending ở một phía: khóa → ảnh; khóa không có trong map là
// phía đó không có row. Có thể trả thêm khóa ngoài pending (bị bỏ qua).
type sideRead func(ctx context.Context, pending []string) (map[string]string, error)

// recheckSuspects kiểm lại các khóa nghi lệch tối đa retries lần (xem chú thích đầu file). Mỗi lần thử
// đọc mọi khóa còn chưa kết luận cùng lúc. Trả khóa lệch thật và khóa unverifiable_hot (đã sắp xếp).
func recheckSuspects(ctx context.Context, suspects []string, retries int, readSrc, readDst sideRead, w ApplyWaiter) (mismatch, hot []string, err error) {
	if retries <= 0 {
		retries = DefaultRecheckRetries
	}
	pending := append([]string(nil), suspects...)
	sort.Strings(pending)
	for attempt := 0; attempt < retries && len(pending) > 0; attempt++ {
		s1, err := readSrc(ctx, pending)
		if err != nil {
			return nil, nil, fmt.Errorf("source: %w", err)
		}
		if w != nil {
			m, err := w.Mark(ctx)
			if err != nil {
				return nil, nil, err
			}
			if err := w.Wait(ctx, m); err != nil {
				return nil, nil, err
			}
		}
		d, err := readDst(ctx, pending)
		if err != nil {
			return nil, nil, fmt.Errorf("target: %w", err)
		}
		s2, err := readSrc(ctx, pending)
		if err != nil {
			return nil, nil, fmt.Errorf("source: %w", err)
		}
		var next []string
		for _, k := range pending {
			switch classifyObs(obsOf(s1, d, s2, k)) {
			case verdictUnstable:
				next = append(next, k)
			case verdictMismatch:
				mismatch = append(mismatch, k)
			}
		}
		pending = next
	}
	return mismatch, pending, nil
}

// diffKeys trả các khóa có ảnh khác nhau (hoặc chỉ có ở một phía) giữa s và d, đã sắp xếp.
func diffKeys(s, d map[string]string) []string {
	var out []string
	for k, sv := range s {
		if dv, ok := d[k]; !ok || dv != sv {
			out = append(out, k)
		}
	}
	for k := range d {
		if _, ok := s[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// keySpan trả [min, max] của các khóa số nguyên (dạng text chính tắc của PK số nguyên).
func keySpan(keys []string) ([2]int64, error) {
	var r [2]int64
	for i, k := range keys {
		v, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return r, fmt.Errorf("primary key value %q is not an int64: %w", k, err)
		}
		if i == 0 || v < r[0] {
			r[0] = v
		}
		if i == 0 || v > r[1] {
			r[1] = v
		}
	}
	return r, nil
}

// compareIntRanges so từng row các range PK số nguyên bằng chuỗi chính tắc rồi kiểm lại các row lệch
// (mọi cặp engine). Kết quả ghi vào rep.
func (mv *MerkleVerifier) compareIntRanges(ctx context.Context, t model.TableID, pkCol string, srcCols, dstCols []ColumnInfo, ranges [][2]int64, rep *FullVerifyReport) error {
	src, dstTable := mv.srcDB(), mv.targetTable(t)
	readSide := func(db *sql.DB, engine model.Engine, table model.TableID, cols []ColumnInfo) sideRead {
		return func(ctx context.Context, pending []string) (map[string]string, error) {
			r, err := keySpan(pending)
			if err != nil {
				return nil, err
			}
			return readCanonicalRows(ctx, db, engine, table, pkCol, cols, r)
		}
	}
	readSrc := readSide(src, mv.cfg.SourceEngine, t, srcCols)
	readDst := readSide(mv.cfg.Target, mv.cfg.TargetEngine, dstTable, dstCols)
	for _, r := range ranges {
		s, err := readCanonicalRows(ctx, src, mv.cfg.SourceEngine, t, pkCol, srcCols, r)
		if err != nil {
			return fmt.Errorf("source: %w", err)
		}
		d, err := readCanonicalRows(ctx, mv.cfg.Target, mv.cfg.TargetEngine, dstTable, pkCol, dstCols, r)
		if err != nil {
			return fmt.Errorf("target: %w", err)
		}
		rep.RangesChecked++
		rep.RowsChecked += len(s)
		suspects := diffKeys(s, d)
		if len(suspects) == 0 {
			continue
		}
		bad, hot, err := recheckSuspects(ctx, suspects, mv.cfg.RecheckRetries, readSrc, readDst, mv.cfg.ApplyWaiter)
		if err != nil {
			return fmt.Errorf("row re-check of %s: %w", t, err)
		}
		rep.Mismatches = append(rep.Mismatches, bad...)
		rep.UnverifiableHot = append(rep.UnverifiableHot, hot...)
	}
	return nil
}

// intColumns đọc cột hai phía (PK số nguyên t.pkCol đứng đầu) theo cùng thứ tự.
func (mv *MerkleVerifier) intColumns(ctx context.Context, t model.TableID, pkCol string) (srcCols, dstCols []ColumnInfo, err error) {
	srcCols, err = GetTableColumns(ctx, mv.srcDB(), mv.cfg.SourceEngine, t, pkCol)
	if err != nil {
		return nil, nil, err
	}
	if !isIntegerType(mv.cfg.SourceEngine, srcCols[0].DataType) {
		return nil, nil, fmt.Errorf("table %s: verify needs an integer primary key, %s is %s", t, pkCol, srcCols[0].DataType)
	}
	dstTable := mv.targetTable(t)
	allDst, err := GetTableColumns(ctx, mv.cfg.Target, mv.cfg.TargetEngine, dstTable, pkCol)
	if err != nil {
		return nil, nil, err
	}
	dstCols, err = matchTargetColumns(srcCols, allDst, dstTable)
	if err != nil {
		return nil, nil, err
	}
	return srcCols, dstCols, nil
}

// VerifyRanges so từng row các range [lo, hi] của bảng t (PK một cột số nguyên pkCol) bằng chuỗi chính
// tắc, kiểm lại row lệch (unverifiable_hot) — sample / delta cho cặp engine mà LiveVerifier không so
// được (PG→PG, MySQL→PG). Không hash, không dùng _strand_verify.
func (mv *MerkleVerifier) VerifyRanges(ctx context.Context, t model.TableID, pkCol string, ranges [][2]int64) (*FullVerifyReport, error) {
	srcCols, dstCols, err := mv.intColumns(ctx, t, pkCol)
	if err != nil {
		return nil, err
	}
	rep := &FullVerifyReport{Table: t.String()}
	if err := mv.compareIntRanges(ctx, t, pkCol, srcCols, dstCols, ranges, rep); err != nil {
		return nil, err
	}
	return rep, nil
}
