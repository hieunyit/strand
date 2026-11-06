package verify

// LiveVerifier so source với target theo range PK khi pipeline đang chạy (design §7, 02 §9).
// Range lệch thì so từng PK; mỗi PK lệch được kiểm lại theo vị trí log, để row đang "bay"
// không bị báo lệch giả:
//   1. đọc row ở source (S1), rồi đọc gtid_executed G (G ⊇ Txn đã ghi S1);
//   2. chờ tập đã apply ở target phủ G, đọc row ở target (T);
//   3. đọc lại row ở source (S2).
// S1 == S2 (row không đổi trong cửa sổ) mà T ≠ S1 thì là lệch thật. Row đổi trong mọi lần thử
// là unverifiable_hot, báo riêng, không báo lệch.

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// LiveConfig cấu hình LiveVerifier.
type LiveConfig struct {
	Source, Target *sql.DB
	// Meta là kết nối tới schema chứa _strand_* trên target (đọc tập đã apply).
	Meta       *sql.DB
	PipelineID string
	// TargetName ánh xạ bảng nguồn sang bảng đích (tables.rename); nil = giữ nguyên.
	TargetName   func(model.TableID) model.TableID
	Retries      int           // số lần kiểm lại một PK (mặc định 5)
	CatchUpLimit time.Duration // thời gian tối đa chờ target theo kịp G (mặc định 2 phút)
	// SkipApplyWait bỏ bước 2 (chờ target apply G) khi kiểm lại một PK. Dùng khi verify chạy
	// độc lập, không có pipeline đang apply (vd. `strand verify --mode full` sau setup): khi đó
	// không ai ghi tiến độ vào _strand_checkpoint nên chờ chỉ hết hạn rồi lỗi. Vẫn giữ S1 == S2
	// để row đang đổi ở source được báo unverifiable_hot thay vì lệch. Mặc định false: hành vi cũ.
	SkipApplyWait bool
}

// LiveReport là kết quả verify một bảng.
type LiveReport struct {
	Table           string
	RangesChecked   int
	RowsChecked     int
	Mismatches      []string // PK lệch thật
	UnverifiableHot []string // PK quá nóng
	// MismatchKeys: cận khóa của các row lệch (PK tổng quát, việc C) — để repair.
	MismatchKeys []snapshot.Bound
}

// LiveVerifier — xem chú thích package.
type LiveVerifier struct{ cfg LiveConfig }

// NewLiveVerifier tạo LiveVerifier.
func NewLiveVerifier(cfg LiveConfig) *LiveVerifier {
	if cfg.Retries <= 0 {
		cfg.Retries = 5
	}
	if cfg.CatchUpLimit <= 0 {
		cfg.CatchUpLimit = 2 * time.Minute
	}
	return &LiveVerifier{cfg: cfg}
}

func qi(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func qt(t model.TableID) string { return qi(t.Schema) + "." + qi(t.Table) }

func (v *LiveVerifier) target(t model.TableID) model.TableID {
	if v.cfg.TargetName != nil {
		return v.cfg.TargetName(t)
	}
	return t
}

type rowImg map[string]string // pk (chuỗi) -> ảnh row (các cột nối lại)

func readRows(ctx context.Context, db *sql.DB, q string, args ...any) (rowImg, []string, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := rowImg{}
	var order []string
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		// Mã hóa không mơ hồ: mỗi cột là "N" (NULL) hoặc "V<độ dài>:<byte>" — dữ liệu chứa dấu phân cách
		// hay chuỗi giống marker NULL không thể làm hai row khác nhau trùng ảnh (review R03).
		var b strings.Builder
		for _, val := range vals {
			if val == nil {
				b.WriteByte('N')
				continue
			}
			b.WriteByte('V')
			b.WriteString(strconv.Itoa(len(val)))
			b.WriteByte(':')
			b.Write(val)
		}
		pk := string(vals[0])
		out[pk] = b.String()
		order = append(order, pk)
	}
	return out, order, rows.Err()
}

// columns trả về danh sách cột (PK đứng đầu) để SELECT cùng thứ tự ở hai phía.
func columnList(ctx context.Context, db *sql.DB, t model.TableID, pk string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, t.Schema, t.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := []string{pk}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		if c != pk {
			cols = append(cols, c)
		}
	}
	return cols, rows.Err()
}

// VerifyRanges so các range [lo, hi] của bảng (PK một cột số nguyên).
func (v *LiveVerifier) VerifyRanges(ctx context.Context, t model.TableID, pk string, ranges [][2]int64) (*LiveReport, error) {
	rep := &LiveReport{Table: t.String()}
	cols, err := columnList(ctx, v.cfg.Source, t, pk)
	if err != nil {
		return nil, err
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = qi(c)
	}
	list := strings.Join(sel, ", ")
	srcQ := fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN ? AND ? ORDER BY %s", list, qt(t), qi(pk), qi(pk))
	dstQ := fmt.Sprintf("SELECT %s FROM %s WHERE %s BETWEEN ? AND ? ORDER BY %s", list, qt(v.target(t)), qi(pk), qi(pk))

	for _, r := range ranges {
		rep.RangesChecked++
		s, _, err := readRows(ctx, v.cfg.Source, srcQ, r[0], r[1])
		if err != nil {
			return nil, err
		}
		d, _, err := readRows(ctx, v.cfg.Target, dstQ, r[0], r[1])
		if err != nil {
			return nil, err
		}
		rep.RowsChecked += len(s)
		suspects := map[string]bool{}
		for k, sv := range s {
			if dv, ok := d[k]; !ok || dv != sv {
				suspects[k] = true
			}
		}
		for k := range d {
			if _, ok := s[k]; !ok {
				suspects[k] = true
			}
		}
		for k := range suspects {
			real, hot, err := v.recheck(ctx, t, pk, list, k)
			if err != nil {
				return nil, err
			}
			switch {
			case real:
				rep.Mismatches = append(rep.Mismatches, k)
			case hot:
				rep.UnverifiableHot = append(rep.UnverifiableHot, k)
			}
		}
	}
	return rep, nil
}

func (v *LiveVerifier) recheck(ctx context.Context, t model.TableID, pk, list, key string) (real, hot bool, err error) {
	srcQ := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", list, qt(t), qi(pk))
	dstQ := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", list, qt(v.target(t)), qi(pk))
	w := v.waiter()
	for attempt := 0; attempt < v.cfg.Retries; attempt++ {
		s1, _, err := readRows(ctx, v.cfg.Source, srcQ, key)
		if err != nil {
			return false, false, err
		}
		if err := waitMark(ctx, w); err != nil {
			return false, false, err
		}
		d, _, err := readRows(ctx, v.cfg.Target, dstQ, key)
		if err != nil {
			return false, false, err
		}
		s2, _, err := readRows(ctx, v.cfg.Source, srcQ, key)
		if err != nil {
			return false, false, err
		}
		switch classifyObs(obsOf(s1, d, s2, key)) {
		case verdictUnstable:
			continue // row đổi trong cửa sổ: thử lại
		case verdictMismatch:
			return true, false, nil
		}
		return false, false, nil
	}
	return false, true, nil
}

// waiter trả ApplyWaiter MySQL→MySQL của LiveVerifier (nil khi SkipApplyWait).
func (v *LiveVerifier) waiter() ApplyWaiter {
	if v.cfg.SkipApplyWait {
		return nil
	}
	return &LogApplyWaiter{Source: v.cfg.Source, Meta: v.cfg.Meta, PipelineID: v.cfg.PipelineID,
		SourceEngine: model.EngineMySQL, TargetEngine: model.EngineMySQL, Limit: v.cfg.CatchUpLimit}
}

// waitMark: bước 2 của kiểm lại — đọc vị trí log source rồi chờ target apply tới đó (w nil: bỏ qua).
func waitMark(ctx context.Context, w ApplyWaiter) error {
	if w == nil {
		return nil
	}
	m, err := w.Mark(ctx)
	if err != nil {
		return err
	}
	return w.Wait(ctx, m)
}

// SampleRanges chọn ngẫu nhiên các range liên tiếp phủ khoảng rate (0..1] số row của bảng,
// mỗi range `rows` khóa (keyset trên source), ít nhất một range.
func SampleRanges(ctx context.Context, db *sql.DB, t model.TableID, pk string, rate float64, rows int64, rng *rand.Rand) ([][2]int64, error) {
	return SampleRangesEngine(ctx, db, model.EngineMySQL, t, pk, rate, rows, rng)
}

// SampleRangesEngine như SampleRanges cho source thuộc engine bất kỳ (PG: ước lượng số row từ
// pg_class.reltuples).
func SampleRangesEngine(ctx context.Context, db *sql.DB, engine model.Engine, t model.TableID, pk string, rate float64, rows int64, rng *rand.Rand) ([][2]int64, error) {
	qpk, qtab, ph1 := QuoteIdent(engine, pk), QuoteTable(engine, t), keyPh(engine, 1)
	if rows <= 0 {
		rows = 1000
	}
	var mn, mx sql.NullInt64
	var est int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qpk, qpk, qtab)).Scan(&mn, &mx); err != nil {
		return nil, err
	}
	if !mn.Valid {
		return [][2]int64{{math.MinInt64, math.MaxInt64}}, nil // bảng rỗng: phát hiện row thừa ở target
	}
	if rate >= 1 {
		// Toàn bộ bảng: keyset walk, mỗi range `rows` khóa thật (không lệch khi PK thưa).
		var out [][2]int64
		next := mn.Int64
		for next <= mx.Int64 {
			var end sql.NullInt64
			q := fmt.Sprintf("SELECT MAX(%s) FROM (SELECT %s FROM %s WHERE %s >= %s ORDER BY %s LIMIT %d) x",
				qpk, qpk, qtab, qpk, ph1, qpk, rows)
			if err := db.QueryRowContext(ctx, q, next).Scan(&end); err != nil {
				return nil, err
			}
			if !end.Valid {
				break
			}
			out = append(out, [2]int64{next, end.Int64})
			if end.Int64 >= mx.Int64 || end.Int64 == math.MaxInt64 {
				break // end+1 tràn int64 khi end == MaxInt64
			}
			next = end.Int64 + 1
		}
		// Hai đầu mở (phủ toàn bộ int64) để phát hiện row thừa ở target ngoài [min, max].
		if mn.Int64 > math.MinInt64 {
			out = append(out, [2]int64{math.MinInt64, mn.Int64 - 1})
		}
		if mx.Int64 < math.MaxInt64 {
			out = append(out, [2]int64{mx.Int64 + 1, math.MaxInt64})
		}
		return out, nil
	}
	if engine == model.EnginePG {
		_ = db.QueryRowContext(ctx, "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = to_regclass($1)", qtab).Scan(&est)
	} else {
		_ = db.QueryRowContext(ctx, "SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			t.Schema, t.Table).Scan(&est)
	}
	n := int(float64(est)*rate/float64(rows)) + 1
	var out [][2]int64
	for i := 0; i < n; i++ {
		start := mn.Int64
		if mx.Int64 > mn.Int64 {
			// mx-mn+1 tràn int64 (Int63n panic với n <= 0) khi khoảng PK rộng hơn MaxInt64.
			if span := uint64(mx.Int64) - uint64(mn.Int64); span < math.MaxInt64 {
				start += rng.Int63n(int64(span) + 1)
			} else if span == math.MaxUint64 {
				start += int64(rng.Uint64()) // [MinInt64, MaxInt64]: mọi giá trị (cộng có wrap)
			} else {
				start += int64(rng.Uint64() % (span + 1))
			}
		}
		var end sql.NullInt64
		q := fmt.Sprintf("SELECT MAX(%s) FROM (SELECT %s FROM %s WHERE %s >= %s ORDER BY %s LIMIT %d) x",
			qpk, qpk, qtab, qpk, ph1, qpk, rows)
		if err := db.QueryRowContext(ctx, q, start).Scan(&end); err != nil {
			return nil, err
		}
		if !end.Valid {
			end.Int64 = mx.Int64
		}
		out = append(out, [2]int64{start, end.Int64})
	}
	return out, nil
}

// KeyRanges gom các khóa CDC đã chạm thành range để verify: khóa được sắp xếp, khóa kế tiếp nhập vào
// range hiện tại nếu cách khóa trước ≤ keyRangeMaxGap và range không rộng quá keyRangeMaxSpan.
// Range gộp có thể kiểm thêm vài row không bị chạm (an toàn), nhưng số query giảm từ 2/khóa xuống
// 2/range — post-seed verify trên hàng chục nghìn khóa không còn mất hàng phút.
const (
	keyRangeMaxGap  = 64
	keyRangeMaxSpan = 1000
)

func KeyRanges(keys []int64) [][2]int64 {
	if len(keys) == 0 {
		return nil
	}
	ks := append([]int64(nil), keys...)
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	var out [][2]int64
	start, prev := ks[0], ks[0]
	for _, k := range ks[1:] {
		if k == prev {
			continue
		}
		// Hiệu tính trên uint64 (k > prev >= start): k-prev tràn int64 khi khóa ở hai đầu int64.
		if uint64(k)-uint64(prev) <= keyRangeMaxGap && uint64(k)-uint64(start) < keyRangeMaxSpan {
			prev = k
			continue
		}
		out = append(out, [2]int64{start, prev})
		start, prev = k, k
	}
	return append(out, [2]int64{start, prev})
}

type rawRow struct {
	raw [][]byte
	img string
}

// readRowsAll như readRows nhưng giữ byte gốc của từng cột (dựng khóa tuple, việc C).
func readRowsAll(ctx context.Context, db *sql.DB, q string, args ...any) ([]rawRow, []string, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []rawRow
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		var b strings.Builder
		raw := make([][]byte, len(vals))
		for i, val := range vals {
			raw[i] = append([]byte{}, val...)
			if val == nil {
				b.WriteByte('N')
				continue
			}
			b.WriteByte('V')
			b.WriteString(strconv.Itoa(len(val)))
			b.WriteByte(':')
			b.Write(val)
		}
		out = append(out, rawRow{raw: raw, img: b.String()})
	}
	return out, cols, rows.Err()
}
