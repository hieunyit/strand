package pipeline

// Phần phụ thuộc engine của pipeline (P2): source PostgreSQL (reader pgoutput, tập đã apply theo
// LSN) và target PostgreSQL (PGWriter, PGStore). Các cặp được hỗ trợ: MySQL→MySQL (giữ nguyên
// hành vi P1), MySQL→PG (typemap chuyển giá trị), PG→PG (giá trị đi thẳng).

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
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

// initialPositionPG: điểm bắt đầu cho source PG (02 §1, §10). Snapshot logic từ source PG chưa
// được hỗ trợ (design §6.1 cần snapshot export giữ trên kết nối tạo slot, reader hiện không giữ),
// nên pipeline mới bắt buộc --start-at <LSN>: dữ liệu ở target đã khớp source tại LSN đó
// (thường là consistent_point của slot), mọi Txn có commit LSN ≤ LSN coi như đã apply.
func (p *Pipeline) initialPositionPG(ctx context.Context, applied checkpoint.AppliedSet) (checkpoint.AppliedSet, error) {
	empty := applied.(*checkpoint.PGAppliedSet).String() == ""
	if p.opts.StartAt != "" {
		if !empty {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
				"--start-at is only valid for a new pipeline; a checkpoint already exists (use strand checkpoint rewind)", nil)
		}
		if p.opts.StartAt == "seed" {
			return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
				"--start-at seed is not supported for a PostgreSQL source yet; pass --start-at <LSN> (the consistent_point of the slot the target data matches)", nil)
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
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
			"bootstrap.default=logical is not supported for a PostgreSQL source yet (no logical snapshot from PG): "+
				"copy the data to the target consistently with the slot's consistent_point and start with --start-at <LSN>", nil)
	case "seed", "start_at":
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax,
			fmt.Sprintf("bootstrap.default=%s requires --start-at <LSN> for a new pipeline", mode), nil)
	default:
		return nil, exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.InvalidSyntax, "unknown bootstrap.default "+mode, nil)
	}
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
	src, ok := p.sourceOf(table)
	if !ok {
		return fmt.Errorf("snapshot flush: no source table maps to %s", table)
	}
	out := make([]*model.RowChange, 0, len(rows))
	for _, r := range rows {
		c := *r
		if p.conv != nil {
			if err := p.conv.row(src, &c, p.table(src).PK); err != nil {
				return err
			}
		}
		out = append(out, &c)
	}
	return writer.PGReplaceRange(ctx, p.dst, table, pkCol, columnNames, out, startPK, endPK)
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
