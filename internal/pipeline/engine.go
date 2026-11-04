package pipeline

// Phần phụ thuộc engine của pipeline (P2): source PostgreSQL (reader pgoutput, tập đã apply theo
// LSN) và target PostgreSQL (PGWriter, PGStore). Các cặp được hỗ trợ: MySQL→MySQL (giữ nguyên
// hành vi P1), MySQL→PG (typemap chuyển giá trị), PG→PG (giá trị đi thẳng).

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"strand/internal/assemble"
	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/promote"
	pgreader "strand/internal/reader/pg"
	"strand/internal/seed"
	"strand/internal/snapshot"
	"strand/internal/typemap"
	"strand/internal/writer"
)

// batchWriter là phần chung của MySQLWriter và PGWriter mà pipeline dùng.
type batchWriter interface {
	ApplyBatch(ctx context.Context, batch []*model.Txn, delta writer.DeltaRow) error
	SetPrimaryKey(t model.TableID, cols []string)
	SetCommitOrderer(o *writer.CommitOrderer)
	EnsureWorkerRow(ctx context.Context) error
}

// engineOf đổi source.kind / target.kind sang model.Engine ("" nếu không nhận ra).
func engineOf(kind string) model.Engine {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "mysql":
		return model.EngineMySQL
	case "postgres", "postgresql", "pg":
		return model.EnginePG
	}
	return ""
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:12]
}

// pgAppName: application_name của mọi kết nối target của instance này (≤ 63 byte). Instance mới
// tìm kết nối của instance cũ cùng pipeline qua tiền tố "strand:<hash pipeline>:" (SG-1).
func pgAppName(pipelineID, instance string) string {
	return "strand:" + shortHash(pipelineID) + ":" + shortHash(instance)
}

// openPGTarget mở pool target PG bằng pgx stdlib (PGWriter cần *stdlib.Conn để COPY).
func openPGTarget(dsn, appName string) (*sql.DB, error) {
	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if cc.RuntimeParams == nil {
		cc.RuntimeParams = map[string]string{}
	}
	cc.RuntimeParams["application_name"] = appName
	// Giá trị thời gian từ reader/typemap luôn là UTC.
	cc.RuntimeParams["timezone"] = "UTC"
	return stdlib.OpenDB(*cc), nil
}

// mysqlSourceUTC đặt time_zone='+00:00' cho phiên SQL tới source MySQL khi target là PG: typemap
// đổi TIMESTAMP→timestamptz với giả định session UTC (02 §8 "time_zone của session được cố định").
func mysqlSourceUTC(dsn string) (string, error) {
	c, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	if c.Params == nil {
		c.Params = map[string]string{}
	}
	c.Params["time_zone"] = "'+00:00'"
	return c.FormatDSN(), nil
}

// killStalePG: tương đương killStaleInstances cho target PG (pg_terminate_backend theo application_name).
func (p *Pipeline) killStalePG(ctx context.Context) {
	me := pgAppName(p.id, p.instance)
	rows, err := p.dst.QueryContext(ctx, `
		SELECT pid, pg_terminate_backend(pid) FROM pg_stat_activity
		 WHERE application_name LIKE $1 AND application_name <> $2 AND pid <> pg_backend_pid()`,
		"strand:"+shortHash(p.id)+":%", me)
	if err != nil {
		p.log.Warn("cannot terminate stale instance connections", "error", err)
		return
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if n > 0 {
		p.log.Warn("terminated target connections of a previous instance of this pipeline", "connections", n)
	}
}

// loadTables đọc metadata các bảng nguồn thỏa bộ lọc, theo engine của source.
func (p *Pipeline) loadTables(ctx context.Context, f *tableFilter) (map[model.TableID]*tableMeta, error) {
	if p.srcEngine == model.EnginePG {
		return loadSourceTablesPG(ctx, p.src, f)
	}
	return loadSourceTables(ctx, p.src, f)
}

// loadSourceTablesPG đọc từ catalog của source PG: cột (bỏ cột generated — pgoutput không gửi),
// PK, unique index (không partial, không biểu thức) và FK.
func loadSourceTablesPG(ctx context.Context, db *sql.DB, f *tableFilter) (map[model.TableID]*tableMeta, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT n.nspname, c.relname, c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition
		   AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_%'`)
	if err != nil {
		return nil, fmt.Errorf("list source tables: %w", err)
	}
	type rel struct {
		id  model.TableID
		oid int64
	}
	var rels []rel
	for rows.Next() {
		var r rel
		if err := rows.Scan(&r.id.Schema, &r.id.Table, &r.oid); err != nil {
			rows.Close()
			return nil, err
		}
		if f.Included(r.id) {
			rels = append(rels, r)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make(map[model.TableID]*tableMeta, len(rels))
	for _, r := range rels {
		m := &tableMeta{ID: r.id}
		cr, err := db.QueryContext(ctx, `
			SELECT a.attname, t.typname FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
			 WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
			 ORDER BY a.attnum`, r.oid)
		if err != nil {
			return nil, err
		}
		types := map[string]string{}
		for cr.Next() {
			var c, typ string
			if err := cr.Scan(&c, &typ); err != nil {
				cr.Close()
				return nil, err
			}
			m.Columns = append(m.Columns, c)
			types[c] = typ
		}
		cr.Close()

		ir, err := db.QueryContext(ctx, `
			SELECT ic.relname, i.indisprimary, a.attname
			  FROM pg_index i
			  JOIN pg_class ic ON ic.oid = i.indexrelid
			  JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true
			  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
			 WHERE i.indrelid = $1 AND i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL
			 ORDER BY ic.relname, k.ord`, r.oid)
		if err != nil {
			return nil, err
		}
		idx := map[string][]string{}
		var names []string
		for ir.Next() {
			var n, c string
			var primary bool
			if err := ir.Scan(&n, &primary, &c); err != nil {
				ir.Close()
				return nil, err
			}
			if primary {
				m.PK = append(m.PK, c)
				continue
			}
			if _, ok := idx[n]; !ok {
				names = append(names, n)
			}
			idx[n] = append(idx[n], c)
		}
		ir.Close()
		sort.Strings(names)
		for _, n := range names {
			m.Keys.Unique = append(m.Keys.Unique, assemble.UniqueIndex{Name: n, Columns: idx[n]})
		}
		if len(m.PK) == 1 {
			switch types[m.PK[0]] {
			case "int2", "int4", "int8":
				m.PKNumeric = true
			}
		}

		fr, err := db.QueryContext(ctx, `
			SELECT con.conname, a.attname, pn.nspname, pc.relname
			  FROM pg_constraint con
			  JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON true
			  JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
			  JOIN pg_class pc ON pc.oid = con.confrelid
			  JOIN pg_namespace pn ON pn.oid = pc.relnamespace
			 WHERE con.conrelid = $1 AND con.contype = 'f'
			 ORDER BY con.conname, k.ord`, r.oid)
		if err != nil {
			return nil, err
		}
		fks := map[string]*assemble.ForeignKeyMeta{}
		var fkNames []string
		for fr.Next() {
			var cn, col, ps, pt string
			if err := fr.Scan(&cn, &col, &ps, &pt); err != nil {
				fr.Close()
				return nil, err
			}
			fk, ok := fks[cn]
			if !ok {
				fk = &assemble.ForeignKeyMeta{ParentTable: model.TableID{Schema: ps, Table: pt}}
				fks[cn] = fk
				fkNames = append(fkNames, cn)
			}
			fk.Columns = append(fk.Columns, col)
		}
		fr.Close()
		for _, n := range fkNames {
			m.Keys.ForeignKeys = append(m.Keys.ForeignKeys, *fks[n])
		}
		out[r.id] = m
	}
	return out, nil
}

// initialPositionPG: điểm bắt đầu cho source PG (02 §1, §10):
//   - --start-at <LSN>: dữ liệu ở target đã khớp source tại LSN đó, mọi Txn có commit LSN ≤ LSN coi như
//     đã apply;
//   - --start-at seed: đọc seed.json PG (strand export / pgBackRest / WAL-G): P = consistent_point của
//     slot trong seed; Txn có commit LSN < P nằm trong seed, ≥ P đi qua slot (startFromSeedPG);
//   - bootstrap logic (mặc định): stream từ confirmed_flush của slot (consistent_point nếu slot mới),
//     mọi bảng được snapshot logic song song với stream (snapshot.PGChunkSource).
func (p *Pipeline) initialPositionPG(ctx context.Context, applied checkpoint.AppliedSet) (checkpoint.AppliedSet, error) {
	empty := applied.(*checkpoint.PGAppliedSet).String() == ""
	if p.opts.StartAt != "" {
		if !empty {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
				"--start-at is only valid for a new pipeline; a checkpoint already exists (use strand checkpoint rewind)", nil)
		}
		if p.opts.StartAt == "seed" {
			return p.startFromSeedPG(ctx)
		}
		lsn, err := checkpoint.ParseLSN(p.opts.StartAt)
		if err != nil || lsn == 0 {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
				fmt.Sprintf("invalid start LSN %q for a PostgreSQL source (want X/Y): %v", p.opts.StartAt, err), err)
		}
		set := checkpoint.NewPGAppliedSet(lsn)
		for id := range p.tables {
			if err := p.state.SetDone(id.String(), true); err != nil {
				return nil, err
			}
		}
		p.log.Warn("start-at: target data is assumed to match the source at this LSN; automatic post-seed verify is MySQL→MySQL only, run `strand verify --mode full`",
			"lsn", checkpoint.FormatLSN(lsn))
		return set, p.writeGlobal(ctx, set.String())
	}
	if !empty {
		return applied, nil
	}
	mode := strings.ToLower(p.cfg.Bootstrap.Default)
	switch mode {
	case "", "logical":
		// Chế độ A: chưa có Txn nào được apply bền vững. Stream bắt đầu từ confirmed_flush của slot
		// (slot chỉ tiến theo lwd bền vững, nên đây vẫn là consistent_point nếu lần chạy trước dừng
		// trước checkpoint đầu tiên); mọi bảng snapshot lại từ đầu (SetDone(false) xóa cả plan cũ).
		for id := range p.tables {
			if err := p.state.SetDone(id.String(), false); err != nil {
				return nil, err
			}
		}
		return applied, nil
	case "seed", "start_at":
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
			fmt.Sprintf("bootstrap.default=%s requires --start-at <LSN> for a new pipeline", mode), nil)
	default:
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, "unknown bootstrap.default "+mode, nil)
	}
}

// startFromSeedPG đọc seed.json PG. P = start_position.lsn (consistent_point của slot), slot =
// start_position.slot. Ranh giới chính xác (reports/P2-05): seed chứa Txn có commit LSN < P, slot stream
// Txn có commit LSN ≥ P — commit record có thể bắt đầu ĐÚNG tại P. Vì vậy tập đã apply là "≤ P−1",
// không phải NewPGAppliedSet(P) (sẽ bỏ Txn commit tại đúng P). Reader chấp nhận confirmed_flush = lwd+1.
func (p *Pipeline) startFromSeedPG(ctx context.Context) (checkpoint.AppliedSet, error) {
	path := p.opts.SeedManifest
	if path == "" {
		path = "seed.json"
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, fmt.Sprintf("read seed manifest: %v", err), err)
	}
	var m seed.SeedManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, fmt.Sprintf("parse seed manifest: %v", err), err)
	}
	if m.Engine != "pg" && m.Engine != "postgres" {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("seed manifest engine is %q, the source is PostgreSQL", m.Engine), nil)
	}
	if m.LogCoverage != nil && !m.LogCoverage.OK {
		return nil, exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "seed manifest reports log coverage not OK", nil)
	}
	lsn, err := checkpoint.ParseLSN(m.StartPosition.LSN)
	if err != nil || lsn == 0 {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("seed manifest start_position.lsn %q is not a valid LSN: %v", m.StartPosition.LSN, err), err)
	}
	slot := m.StartPosition.Slot
	if slot == "" {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			"seed manifest has no start_position.slot: the CDC catch-up must read the slot whose consistent_point is P", nil)
	}
	if p.cfg.Source.Slot != "" && p.cfg.Source.Slot != slot {
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			fmt.Sprintf("source.slot is %q but the seed was taken at the consistent_point of slot %q", p.cfg.Source.Slot, slot), nil)
	}
	if err := p.state.SetPGSlot(slot); err != nil {
		return nil, err
	}
	// Bảng có trong seed thì xong; bảng của pipeline không có trong seed được snapshot logic.
	seeded := map[string]bool{}
	for _, t := range m.Tables {
		seeded[strings.Trim(t, `"`)] = true
	}
	for id := range p.tables {
		done := len(m.Tables) == 0 || seeded[id.String()] || (id.Schema == "public" && seeded[id.Table])
		if err := p.state.SetDone(id.String(), done); err != nil {
			return nil, err
		}
		if !done {
			p.log.Warn("table is not in the seed: it will be snapshotted logically", "table", id.String())
		}
	}
	set := checkpoint.NewPGAppliedSet(lsn - 1)
	p.log.Info("start from PostgreSQL seed", "kind", m.Kind, "p", checkpoint.FormatLSN(lsn), "slot", slot,
		"source_identity", m.SourceIdentity)
	p.log.Warn("automatic post-seed verify is MySQL→MySQL only; run `strand verify --mode full` for this pipeline after catch-up")
	return set, p.writeGlobal(ctx, set.String())
}

// pgSlotNames trả tên slot / publication theo cấu hình hoặc mặc định strand_<name> (đã chuẩn hóa).
func (p *Pipeline) pgSlotNames() (slot, pub string) {
	norm := func(s string) string {
		var b strings.Builder
		for _, r := range strings.ToLower(s) {
			if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
				b.WriteRune(r)
			} else {
				b.WriteByte('_')
			}
		}
		out := "strand_" + b.String()
		if len(out) > 63 {
			out = out[:63]
		}
		return out
	}
	slot, pub = p.cfg.Source.Slot, p.cfg.Source.Publication
	if slot == "" && p.state != nil {
		slot = p.state.PGSlot() // slot của seed (--start-at seed)
	}
	if slot == "" {
		slot = norm(p.id)
	}
	if pub == "" {
		pub = norm(p.id)
	}
	return slot, pub
}

// ---------------------------------------------------------------------------
// Xác nhận mốc LSN (Vouch) và lwd bền vững cho slot
// ---------------------------------------------------------------------------

// lsnTracker là phần "coordinator biết luồng Txn" của tập đã apply PG. Reader phát Txn theo đúng
// thứ tự commit LSN và router xử lý tuần tự, nên seq tăng theo LSN. Một mốc B được xác nhận
// (PGAppliedSet.Vouch(B, nil)) khi mọi Txn router đã nhận có LSN ≤ B đều đã commit ở target
// (hoặc bị bỏ vì đã apply / rỗng sau khi lọc) — xem point.
type lsnTracker struct {
	mu   sync.Mutex
	q    []seqLSN // Txn đã Admit mà chưa được xác nhận, theo seq tăng
	seen uint64   // LSN lớn nhất router đã xử lý xong
}

type seqLSN struct {
	seq, lsn uint64
}

// routed ghi nhận router đã xử lý xong Txn có commit LSN lsn; seq = 0 nếu Txn không đi qua scheduler.
func (t *lsnTracker) routed(lsn, seq uint64) {
	if lsn == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if seq != 0 {
		t.q = append(t.q, seqLSN{seq: seq, lsn: lsn})
	}
	if lsn > t.seen {
		t.seen = lsn
	}
}

// seenLSN trả LSN lớn nhất router đã xử lý xong.
func (t *lsnTracker) seenLSN() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.seen
}

// point trả mốc lớn nhất có thể xác nhận khi mọi seq ≤ lwd đã commit ở target (0 = chưa có gì).
//   - còn Txn chưa commit: mốc = LSN của Txn sớm nhất trong số đó − 1 (mọi Txn trước nó đã xong);
//   - không còn: mốc = LSN lớn nhất router đã xử lý.
func (t *lsnTracker) point(lwd uint64) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	i := 0
	for i < len(t.q) && t.q[i].seq <= lwd {
		i++
	}
	t.q = t.q[i:]
	if len(t.q) > 0 {
		return t.q[0].lsn - 1
	}
	return t.seen
}

// vouchGlobal là hook của compactor (relaxed, source PG): dời mốc của tập toàn cục tới phần
// đã commit ở target theo scheduler.
func (p *Pipeline) vouchGlobal(g checkpoint.AppliedSet) {
	if s, ok := g.(*checkpoint.PGAppliedSet); ok {
		s.Vouch(p.lsns.point(p.sched.Lwd()), nil)
	}
}

// pgConfirmLoop báo reader lwd BỀN VỮNG (row toàn cục đã commit ở target) để slot được tiến
// (confirmed_flush_lsn). Không bao giờ báo LSN chưa nằm trong row toàn cục đã commit.
func (p *Pipeline) pgConfirmLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var lwd uint64
		if p.strict {
			lwd = p.strictDurable.Load()
		} else if g := p.comp.LastGlobal(); g != nil {
			lwd = g.Lwd().LSN
		}
		if lwd > 0 {
			p.pgReader.SetConfirmed(lwd)
		}
	}
}

// startSequenceSync chạy đồng bộ định kỳ last_value của sequence độc lập ở source PG vào
// _strand_promote trên target (02 §8, Δt_sync). Trả hàm dừng (chờ goroutine thoát).
func (p *Pipeline) startSequenceSync(ctx context.Context) func() {
	interval := durationOr(p.cfg.Source.SequenceSyncInterval, 5*time.Second)
	schemaSet := map[string]bool{}
	p.tablesMu.RLock()
	for id := range p.tables {
		schemaSet[id.Schema] = true
	}
	p.tablesMu.RUnlock()
	var schemas []string
	for s := range schemaSet {
		schemas = append(schemas, s)
	}
	sort.Strings(schemas)
	seqs, err := promote.DiscoverPGSequences(ctx, p.src, schemas)
	if err != nil {
		p.log.Warn("cannot discover source sequences; periodic sequence sync disabled", "error", err)
		return func() {}
	}
	stop, done := promote.StartPeriodicSequenceSync(ctx, promote.NewPGSourceDriver(p.src), promote.NewPGStateStore(p.dst),
		p.id, seqs, interval, p.log)
	p.log.Info("periodic sequence sync started", "sequences", len(seqs), "interval", interval.String())
	return func() { stop(); <-done }
}

// ---------------------------------------------------------------------------
// MySQL→PG: chuyển giá trị bằng typemap
// ---------------------------------------------------------------------------

type convFunc func(model.Value) (model.Value, error)

// valueConverter giữ hàm chuyển giá trị theo cột của từng bảng nguồn MySQL (02 §8).
type valueConverter struct {
	mu     sync.RWMutex
	tables map[model.TableID]map[string]convFunc
}

func newValueConverter() *valueConverter {
	return &valueConverter{tables: map[model.TableID]map[string]convFunc{}}
}

// load đọc cột của bảng ở source MySQL và dựng hàm chuyển theo typemap (policy mặc định).
// Kiểu không map được → type_unmappable (exit 3).
func (c *valueConverter) load(ctx context.Context, db *sql.DB, id model.TableID) error {
	cols, err := typemap.InspectMySQLColumns(ctx, db, id.Schema, id.Table)
	if err != nil {
		return err
	}
	m := make(map[string]convFunc, len(cols))
	for _, col := range cols {
		cm, err := typemap.MapMySQLColumn(col, typemap.DefaultOptions())
		if err != nil {
			var ee *exitcode.Error
			if errors.As(err, &ee) {
				return err
			}
			return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected, fmt.Sprintf("table %s column %s: %v", id, col.Name, err), err)
		}
		if cm.Convert != nil {
			m[col.Name] = cm.Convert
		}
	}
	c.mu.Lock()
	c.tables[id] = m
	c.mu.Unlock()
	return nil
}

func (c *valueConverter) forget(id model.TableID) {
	c.mu.Lock()
	delete(c.tables, id)
	c.mu.Unlock()
}

// row chuyển mọi giá trị của rc (bảng nguồn src) tại chỗ; Cols/Key/OldKey được cấp phát lại nên
// không đụng tới slice của bản gốc. pk là tên cột PK theo thứ tự của Key.
func (c *valueConverter) row(src model.TableID, rc *model.RowChange, pk []string) error {
	c.mu.RLock()
	m := c.tables[src]
	c.mu.RUnlock()
	if m == nil {
		return fmt.Errorf("no type mapping loaded for table %s", src)
	}
	conv := func(col string, v model.Value) (model.Value, error) {
		f := m[col]
		if f == nil || v == nil {
			return v, nil
		}
		out, err := f(v)
		if err != nil {
			return nil, fmt.Errorf("table %s column %s: %w", src, col, err)
		}
		return out, nil
	}
	if len(rc.Cols) > 0 {
		cols := make([]model.ColValue, len(rc.Cols))
		for i, cv := range rc.Cols {
			cols[i] = cv
			if cv.Unchanged {
				continue
			}
			v, err := conv(cv.Name, cv.Value)
			if err != nil {
				return err
			}
			cols[i].Value = v
		}
		rc.Cols = cols
	}
	keyConv := func(key []model.Value) ([]model.Value, error) {
		if len(key) == 0 {
			return key, nil
		}
		out := make([]model.Value, len(key))
		for i, v := range key {
			if i >= len(pk) {
				out[i] = v
				continue
			}
			cv, err := conv(pk[i], v)
			if err != nil {
				return nil, err
			}
			out[i] = cv
		}
		return out, nil
	}
	var err error
	if rc.Key, err = keyConv(rc.Key); err != nil {
		return err
	}
	if rc.OldKey, err = keyConv(rc.OldKey); err != nil {
		return err
	}
	return nil
}

// sourceOf tìm bảng nguồn có tên đích t (tables.rename).
func (p *Pipeline) sourceOf(t model.TableID) (model.TableID, bool) {
	p.tablesMu.RLock()
	defer p.tablesMu.RUnlock()
	for id := range p.tables {
		if p.filter.Target(id) == t {
			return id, true
		}
	}
	return model.TableID{}, false
}

// pgFlushRows là FlushRows của snapshotter khi target là PG: chuyển giá trị bằng typemap rồi
// thay nguyên range ở target trong một transaction.
func (p *Pipeline) pgFlushRows(ctx context.Context, table model.TableID, pkCol string, columnNames []string,
	rows []*model.RowChange, startPK, endPK int64) error {
	out, err := p.pgFlushImages(table, rows)
	if err != nil {
		return err
	}
	return writer.PGReplaceRange(ctx, p.dst, table, pkCol, columnNames, out, startPK, endPK)
}

// pgFlushKeys là FlushKeys của snapshotter (chunk re-read, 02 §8): như pgFlushRows nhưng thay đúng
// tập khóa keys.
func (p *Pipeline) pgFlushKeys(ctx context.Context, table model.TableID, pkCol string, columnNames []string,
	rows []*model.RowChange, keys []int64) error {
	out, err := p.pgFlushImages(table, rows)
	if err != nil {
		return err
	}
	return writer.PGReplaceKeys(ctx, p.dst, table, pkCol, columnNames, out, keys)
}

// pgFlushImages kiểm tra ảnh đã merge (không còn cột TOAST thiếu giá trị) và chuyển giá trị sang PG.
func (p *Pipeline) pgFlushImages(table model.TableID, rows []*model.RowChange) ([]*model.RowChange, error) {
	src, ok := p.sourceOf(table)
	if !ok {
		return nil, fmt.Errorf("snapshot flush: no source table maps to %s", table)
	}
	out := make([]*model.RowChange, 0, len(rows))
	for _, r := range rows {
		for _, cv := range r.Cols {
			if cv.Unchanged {
				// Không dựng lại được giá trị cột TOAST (không có ảnh trước): không ghi row thiếu.
				return nil, exitcode.New(exitcode.ErrVerifyMismatch, exitcode.DataError,
					fmt.Sprintf("snapshot flush: row %v of %s has an unchanged TOAST column %s without a base image", r.Key, table, cv.Name), nil)
			}
		}
		c := *r
		if p.conv != nil {
			if err := p.conv.row(src, &c, p.table(src).PK); err != nil {
				return nil, err
			}
		}
		out = append(out, &c)
	}
	return out, nil
}

// emptyOnSource: bảng nguồn không có row nào (đọc SAU khi vị trí stream đã chốt).
func (p *Pipeline) emptyOnSource(ctx context.Context, id model.TableID) (bool, error) {
	var one int
	err := p.src.QueryRowContext(ctx, fmt.Sprintf("SELECT 1 FROM `%s`.`%s` LIMIT 1",
		strings.ReplaceAll(id.Schema, "`", "``"), strings.ReplaceAll(id.Table, "`", "``"))).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	return false, err
}

// pgColumns trả tên cột (theo thứ tự) của bảng ở target PG.
func pgColumns(ctx context.Context, db *sql.DB, id model.TableID) ([]string, bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`, id.Schema, id.Table)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, false, err
		}
		cols = append(cols, n)
	}
	return cols, len(cols) > 0, rows.Err()
}

// ---------------------------------------------------------------------------
// Snapshot logic từ source PG (snapshot.PGChunkSource)
// ---------------------------------------------------------------------------

// openPGSnapshotSource mở pool đọc chunk ở source PG với GUC phiên giống kết nối replication, để
// `col::text` có đúng định dạng text của pgoutput (row snapshot và row stream cùng dạng Go).
func openPGSnapshotSource(dsn string) (*sql.DB, error) {
	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if cc.RuntimeParams == nil {
		cc.RuntimeParams = map[string]string{}
	}
	for k, v := range pgreader.SessionParams {
		cc.RuntimeParams[k] = v
	}
	return stdlib.OpenDB(*cc), nil
}

// setupPGSnapshotGate dựng cổng visibility khởi động của snapshot logic: mọi Txn có commit LSN nhỏ hơn
// điểm bắt đầu stream (không đi qua router của tiến trình này) phải visible trước khi đăng ký bảng.
//   - Slot vừa được tạo (export snapshot E tại consistent_point C, stream bắt đầu tại C): E thấy đúng
//     các Txn có commit LSN < C, nên chỉ cần snapshot S "phủ" E (S.Covers(E)).
//   - Slot có sẵn: X = pg_current_xact_id() lấy SAU khi điểm bắt đầu stream đã chốt; Txn có commit LSN
//     nhỏ hơn đã được cấp xid trước X, nên S.xmin > X đủ (có thể phải chờ Txn chạy lâu kết thúc).
//
// Không dựng được (vd. source là standby: không ghi được watermark) thì GateErr ≠ nil.
func (p *Pipeline) setupPGSnapshotGate(ctx context.Context, prd *pgreader.Reader) {
	c := p.pgChunks
	c.ServerVersion = prd.ServerVersion()
	defer func() { _ = prd.ReleaseSnapshot() }()
	if prd.InRecovery() {
		c.GateErr = fmt.Errorf("the source is a standby: logical snapshot needs the primary (watermark via pg_logical_emit_message)")
		return
	}
	if name := prd.SnapshotName(); name != "" {
		e, err := importPGSnapshot(ctx, p.src, name)
		if err == nil {
			c.Gate = func(s *snapshot.PGSnapshot) bool { return s.Covers(e) }
			p.log.Info("snapshot visibility gate: exported snapshot of the new slot", "snapshot", e.String())
			return
		}
		p.log.Warn("cannot import the exported snapshot of the new slot; falling back to the xid gate", "error", err)
	}
	xs, err := currentXactID(ctx, p.src)
	if err != nil {
		c.GateErr = fmt.Errorf("read pg_current_xact_id: %w", err)
		return
	}
	x, err := strconv.ParseUint(xs, 10, 64)
	if err != nil {
		c.GateErr = fmt.Errorf("parse pg_current_xact_id %q: %w", xs, err)
		return
	}
	c.Gate = func(s *snapshot.PGSnapshot) bool { return s.Xmin > x }
}

// currentXactID cấp một xid (pg_current_xact_id) trong Txn riêng. synchronous_commit=local: commit của
// Txn này không chờ standby đồng bộ (walsender của chính Strand có thể là standby đồng bộ duy nhất và
// chưa kết nối lúc khởi động).
func currentXactID(ctx context.Context, db *sql.DB) (string, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SET LOCAL synchronous_commit TO local"); err != nil {
		return "", err
	}
	var xs string
	if err := tx.QueryRowContext(ctx, "SELECT pg_current_xact_id()::text").Scan(&xs); err != nil {
		return "", err
	}
	return xs, tx.Commit()
}

// importPGSnapshot đọc (xmin, xmax, xip) của snapshot export name (kết nối tạo slot còn mở).
func importPGSnapshot(ctx context.Context, db *sql.DB, name string) (*snapshot.PGSnapshot, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(name, "'", "''")+"'"); err != nil {
		return nil, err
	}
	var s string
	if err := tx.QueryRowContext(ctx, "SELECT pg_current_snapshot()::text").Scan(&s); err != nil {
		return nil, err
	}
	return snapshot.ParsePGSnapshot(s)
}

// pgPruneXIDs định kỳ gỡ khỏi XIDTracker các xid đã visible ở source (giữ tập nhỏ khi không snapshot).
func (p *Pipeline) pgPruneXIDs(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if p.xids.Len() == 0 {
			continue
		}
		qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		s, err := snapshot.CurrentPGSnapshot(qctx, p.src)
		cancel()
		if err == nil {
			p.xids.Prune(s)
		}
	}
}

// rendezvousPG chờ router xử lý xong một Txn (thật hoặc watermark) có LSN ≥ high: stream phát theo
// thứ tự commit LSN nên mọi Txn có commit LSN < high đã đi qua range-state (pg_snapshot.go).
func (p *Pipeline) rendezvousPG(ctx context.Context, high uint64) error {
	if p.pgReader != nil {
		if proto, _, _ := p.pgReader.Protocol(); proto != 0 && !p.pgReader.Messages() {
			return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				"logical snapshot from PostgreSQL needs pgoutput messages (PostgreSQL 14+)", nil)
		}
	}
	stop := context.AfterFunc(ctx, func() {
		p.seenMu.Lock()
		p.seenCond.Broadcast()
		p.seenMu.Unlock()
	})
	defer stop()
	p.seenMu.Lock()
	defer p.seenMu.Unlock()
	for p.lsns.seenLSN() < high {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := p.failed(); err != nil {
			return err
		}
		p.seenCond.Wait()
	}
	return nil
}

// keepBeforeRegisterPG (source PG): event của bảng đang chờ snapshot mà range chưa được đăng ký.
// Phần sẽ được đọc lại bởi snapshot (bảng mới: cả bảng — ClearTarget xóa sạch khi đăng ký; plan dở
// từ lần chạy trước: [NextPK, MaxPK]) thì BỎ: Txn của nó nằm trong XIDTracker nên biên và chunk đọc
// sau đều thấy nó (BeforeRegister). Bỏ thay vì apply rồi bị xóa, vì UPDATE của PG có thể thiếu cột TOAST
// mà row chưa có ở target. Phần đã flush của plan dở ([MinPK, NextPK−1]) và phần đuôi (> MaxPK) thì
// apply như range "Xong". UPDATE đổi PK được tách thành tombstone + upsert, xét từng nửa.
// Khóa trong plan.Reread (chờ đọc lại, 02 §8) được coi như phần sẽ đọc lại. UPDATE đổi PK mà cột TOAST
// không đổi, khóa cũ sẽ được đọc lại còn khóa mới thì không: nửa upsert thiếu giá trị TOAST nên khóa mới
// được thêm vào plan.Reread (lưu bền ngay, trước khi Txn được Admit) và nửa upsert bị bỏ (toast_pk.go).
func (p *Pipeline) keepBeforeRegisterPG(rc *model.RowChange) ([]*model.RowChange, error) {
	plan, ok := p.state.Plan(rc.Table.String())
	if !ok {
		return nil, nil
	}
	if plan.Empty {
		return []*model.RowChange{rc}, nil
	}
	reread := make(map[int64]bool, len(plan.Reread))
	for _, k := range plan.Reread {
		reread[k] = true
	}
	inside := func(key []model.Value) bool {
		pk, err := snapshotPK(key)
		return err != nil || plan.Pending(pk) || reread[pk] // Pending xử lý cả plan Finished (MaxPK = MaxInt64)
	}
	if len(rc.OldKey) > 0 && snapshot.HasUnchanged(rc) {
		oldIn, newIn := inside(rc.OldKey), inside(rc.Key)
		switch {
		case !oldIn && !newIn:
			return []*model.RowChange{rc}, nil // SQL điều kiện của writer (cả hai khóa đã ở target)
		case newIn:
			parts := snapshot.SplitStreamEvent(rc)
			if oldIn {
				return nil, nil
			}
			return parts[:1], nil // chỉ xóa khóa cũ; khóa mới sẽ được đọc
		default:
			newPK, err := snapshotPK(rc.Key)
			if err != nil {
				return nil, err
			}
			if _, err := p.snap.AddPlanReread(rc.Table, newPK); err != nil {
				return nil, fmt.Errorf("persist re-read key of %s: %w", rc.Table, err)
			}
			return nil, nil // khóa cũ sẽ được đọc lại (vắng mặt), khóa mới được đọc lại
		}
	}
	var out []*model.RowChange
	for _, part := range snapshot.SplitStreamEvent(rc) {
		if inside(part.Key) {
			continue
		}
		out = append(out, part)
	}
	if len(rc.OldKey) > 0 && len(out) == 2 {
		return []*model.RowChange{rc}, nil // cả hai nửa đều apply: giữ UPDATE gốc
	}
	return out, nil
}

// snapshotPK đọc PK số nguyên một cột (int64 từ reader PG).
func snapshotPK(key []model.Value) (int64, error) {
	if len(key) != 1 {
		return 0, fmt.Errorf("composite key")
	}
	switch v := key[0].(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	}
	return 0, fmt.Errorf("unsupported key type %T", key[0])
}
