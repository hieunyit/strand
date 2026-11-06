package pipeline

// Mô phỏng KẾT HỢP (F11): chạy luồng điều khiển THẬT của pipeline relaxed (source PG → target PG) trên
// goroutine thật, chỉ thay phần I/O:
//   - THẬT: Pipeline.router/route/keep (bảng ngoài pipeline bị bỏ, beforeRegister/keepBeforeRegister,
//     range-state, làn, Txn tràn đĩa chép lại + writeset), sched.Scheduler (DAG writeset, làn + lây làn,
//     MaxPending/MaxPendingBytes/MaxHoles, overflow), Pipeline.worker/gatherBatch/applyRelaxed/
//     applyInSeqOrder/finishBatch/deltaFor, laneWorker, barrier/drain (routeMu + chờ Lwd ≥ admitted),
//     snapshotTable → MySQLSnapshotter.SnapshotAdd (đăng ký range trong barrier, chunk, rendezvousPG,
//     flush trong barrier, RemoveTable), lsnTracker + vouchGlobal, checkpoint.Compactor (đọc row delta,
//     gộp, vouch, ghi row toàn cục, prune) qua driver database/sql giả, PGWriter.ApplyBatch → applyTx
//     (pgApplier thật, bộ phát hiện F11) qua seam SetTxBeginner, localState thật (plan, done).
//   - CHÉP: phần cuối của SnapshotAdd (SetDone(false) → SetPrimaryKey → tables[id] = m dưới routeMu →
//     go snapshotTable), phần SELECT/biên/watermark của PGChunkSource (giản lược: commit visible nguyên tử).
//   - GIẢ: source (MVCC đơn giản: commit = visible, stream đúng thứ tự commit LSN, watermark), target
//     (pgx.Tx giả diễn giải câu lệnh writer, khóa toàn cục mỗi transaction, undo log, UNIQUE ngoài PK,
//     lỗi tạm thời 40P01 tiêm ngẫu nhiên), FlushRows/ClearTarget của snapshot.
// Bất biến: (1) mọi seq ≤ Lwd của scheduler đã có transaction dữ liệu commit ở target (không Txn nào Done
// mà chưa ghi); (2) mỗi lần compactor ghi row toàn cục, mọi Txn source có row items/accounts mà tập toàn cục
// chứa LSN của nó đã commit ở target; (3) ver của mỗi khóa ở target không giảm; (4) cuối cùng target ==
// source cho cả ba bảng; (5) không lỗi pipeline, không kẹt.

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"

	"strand/internal/checkpoint"
	"strand/internal/config"
	"strand/internal/metrics"
	"strand/internal/model"
	"strand/internal/sched"
	"strand/internal/snapshot"
	"strand/internal/writer"
)

var (
	cItems    = model.TableID{Schema: "app", Table: "items"}    // chỉ PK, cột TOAST doc
	cAccounts = model.TableID{Schema: "app", Table: "accounts"} // UNIQUE(code): bảng làn, ordered
	cEvents   = model.TableID{Schema: "app", Table: "events"}   // bảng được snapshot add (MỚI)
	cTables   = []model.TableID{cItems, cAccounts, cEvents}
)

func cQuote(t model.TableID) string { return fmt.Sprintf("%q.%q", t.Schema, t.Table) }

type cRow map[string]any

func (r cRow) clone() cRow {
	c := cRow{}
	for k, v := range r {
		c[k] = v
	}
	return c
}

func cI64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	}
	panic(fmt.Sprintf("key %T", v))
}

// ---------------------------------------------------------------------------
// Target giả
// ---------------------------------------------------------------------------

type cDelta struct {
	applied string
	batchNo int64
}

type cTarget struct {
	sim     *cSim
	mu      sync.Mutex // một transaction tại một thời điểm (giữ từ begin tới commit/rollback)
	tabs    map[string]map[int64]cRow
	ordered map[string]bool
	unique  map[string]string
	deltas  map[int64]*cDelta
	global  string
	lastVer map[string]int64
	stmts   int64
	errRate int // 1/errRate: câu lệnh trả 40P01 (0 = tắt)
	rng     *rand.Rand

	succSeq map[uint64]bool // seq có transaction dữ liệu đã commit
	succLSN map[uint64]bool
	globals int
}

type cUndo struct {
	tab string
	id  int64
	row cRow // nil = chưa có
}

type cTx struct {
	t       *cTarget
	batch   []*model.Txn
	undo    []cUndo
	saved   map[string]bool
	tmpOf   map[string]string
	tmpRows map[string][]cRow
	dUndo   map[int64]cDelta
	done    bool
}

func (t *cTarget) begin(batch []*model.Txn) *cTx {
	t.mu.Lock()
	return &cTx{t: t, batch: batch, saved: map[string]bool{}, tmpOf: map[string]string{}, tmpRows: map[string][]cRow{},
		dUndo: map[int64]cDelta{}}
}

func (tx *cTx) save(tab string, id int64) {
	k := tab + "/" + strconv.FormatInt(id, 10)
	if tx.saved[k] {
		return
	}
	tx.saved[k] = true
	var prev cRow
	if r, ok := tx.t.tabs[tab][id]; ok {
		prev = r.clone()
	}
	tx.undo = append(tx.undo, cUndo{tab: tab, id: id, row: prev})
}

func (tx *cTx) finish() {
	if !tx.done {
		tx.done = true
		tx.t.mu.Unlock()
	}
}

// checkVers: ver của khóa đã chạm không được nhỏ hơn ver lớn nhất từng commit (gọi khi giữ mu).
func (t *cTarget) checkVers(keys []cUndo, who string) {
	for _, u := range keys {
		r, ok := t.tabs[u.tab][u.id]
		if !ok {
			continue
		}
		k := fmt.Sprintf("%s/%d", u.tab, u.id)
		v := cI64(r["ver"])
		if v < t.lastVer[k] {
			t.sim.errf("%s: %s ver %d -> %d (target went back)", who, k, t.lastVer[k], v)
		}
		if v > t.lastVer[k] {
			t.lastVer[k] = v
		}
	}
}

func (tx *cTx) Commit(context.Context) error {
	if tx.done {
		return pgx.ErrTxClosed
	}
	tx.t.checkVers(tx.undo, "apply")
	for _, b := range tx.batch {
		tx.t.succSeq[b.Seq()] = true
		tx.t.succLSN[b.SourcePos.LSN] = true
	}
	tx.finish()
	return nil
}

func (tx *cTx) Rollback(context.Context) error {
	if tx.done {
		return nil
	}
	for i := len(tx.undo) - 1; i >= 0; i-- {
		u := tx.undo[i]
		if u.row == nil {
			delete(tx.t.tabs[u.tab], u.id)
		} else {
			tx.t.tabs[u.tab][u.id] = u.row
		}
	}
	for w, d := range tx.dUndo {
		*tx.t.deltas[w] = d
	}
	tx.finish()
	return nil
}

var (
	cReTemp   = regexp.MustCompile(`^CREATE TEMP TABLE "([^"]+)" \(LIKE (\S+) INCLUDING DEFAULTS\) ON COMMIT DROP$`)
	cReMerge  = regexp.MustCompile(`^MERGE INTO (\S+) AS _strand_t USING pg_temp\."([^"]+)" AS _strand_s ON \(.*?\) (WHEN MATCHED THEN (?:DO NOTHING|UPDATE SET .*?)) WHEN NOT MATCHED THEN INSERT \(.*?\) OVERRIDING SYSTEM VALUE VALUES \(.*?\); TRUNCATE pg_temp\."([^"]+)"$`)
	cReInsert = regexp.MustCompile(`^INSERT INTO (\S+) \((.*?)\) OVERRIDING SYSTEM VALUE VALUES .* ON CONFLICT \((.*?)\) (DO NOTHING|DO UPDATE SET .*)$`)
	cReDelete = regexp.MustCompile(`^DELETE FROM (\S+) WHERE \((.*?)\) IN \((.*)\)$`)
	cReUpdate = regexp.MustCompile(`^UPDATE (\S+) SET (.*?) WHERE (.*?)(?: AND NOT EXISTS \(SELECT 1 FROM \S+ WHERE (.*)\))?$`)
	cReSel1   = regexp.MustCompile(`^SELECT 1 FROM (\S+) WHERE (.*)$`)
	cReAssign = regexp.MustCompile(`"(\w+)" = \$(\d+)`)
)

func cSplitCols(s string) []string {
	var out []string
	for _, c := range strings.Split(s, ", ") {
		out = append(out, strings.Trim(c, `"`))
	}
	return out
}

func cErr(code, msg string) error { return &pgconn.PgError{Code: code, Message: msg} }

func (t *cTarget) checkUnique(tab string) error {
	col, ok := t.unique[tab]
	if !ok {
		return nil
	}
	seen := map[any]int64{}
	for id, r := range t.tabs[tab] {
		if o, dup := seen[r[col]]; dup {
			return cErr("23505", fmt.Sprintf("duplicate key value %v in %s (ids %d, %d)", r[col], tab, o, id))
		}
		seen[r[col]] = id
	}
	return nil
}

func (tx *cTx) upsert(tab string, rows []cRow, update bool) error {
	touched := map[int64]bool{}
	for _, r := range rows {
		id := cI64(r["id"])
		if touched[id] {
			return cErr("21000", "command cannot affect row a second time")
		}
		touched[id] = true
		cur, ok := tx.t.tabs[tab][id]
		if !ok {
			tx.save(tab, id)
			tx.t.tabs[tab][id] = r.clone()
			continue
		}
		if update {
			tx.save(tab, id)
			for n, v := range r {
				cur[n] = v
			}
		}
	}
	return tx.t.checkUnique(tab)
}

func (tx *cTx) inject() error {
	t := tx.t
	if t.errRate > 0 && t.rng.Intn(t.errRate) == 0 {
		return cErr("40P01", "deadlock detected (injected)")
	}
	if t.rng.Intn(50) == 0 {
		time.Sleep(time.Duration(t.rng.Intn(200)) * time.Microsecond)
	}
	return nil
}

func (tx *cTx) Exec(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	t := tx.t
	t.stmts++
	if strings.HasPrefix(q, "UPDATE _strand_worker_delta SET applied = $1, batch_no = batch_no + 1") {
		wid := int64(args[2].(int))
		d := t.deltas[wid]
		if d == nil {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		if _, ok := tx.dUndo[wid]; !ok {
			tx.dUndo[wid] = *d
		}
		d.applied, d.batchNo = args[0].(string), d.batchNo+1
		return pgconn.NewCommandTag("UPDATE 1"), nil
	}
	if err := tx.inject(); err != nil {
		return pgconn.CommandTag{}, err
	}
	if m := cReTemp.FindStringSubmatch(q); m != nil {
		tx.tmpOf[m[1]] = m[2]
		return pgconn.NewCommandTag("CREATE TABLE"), nil
	}
	if m := cReMerge.FindStringSubmatch(q); m != nil {
		rows := tx.tmpRows[m[2]]
		err := tx.upsert(m[1], rows, strings.Contains(m[3], "UPDATE SET"))
		tx.tmpRows[m[4]] = nil
		return pgconn.NewCommandTag(fmt.Sprintf("MERGE %d", len(rows))), err
	}
	if m := cReInsert.FindStringSubmatch(q); m != nil {
		cols := cSplitCols(m[2])
		var rows []cRow
		for i := 0; i < len(args); i += len(cols) {
			r := cRow{}
			for j, c := range cols {
				r[c] = args[i+j]
			}
			rows = append(rows, r)
		}
		return pgconn.NewCommandTag(fmt.Sprintf("INSERT 0 %d", len(rows))), tx.upsert(m[1], rows, m[4] != "DO NOTHING")
	}
	if m := cReDelete.FindStringSubmatch(q); m != nil {
		n := 0
		for _, a := range args {
			id := cI64(a)
			if _, ok := t.tabs[m[1]][id]; ok {
				tx.save(m[1], id)
				delete(t.tabs[m[1]], id)
				n++
			}
		}
		return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", n)), nil
	}
	if m := cReUpdate.FindStringSubmatch(q); m != nil {
		tab := m[1]
		arg := func(s string) any { i, _ := strconv.Atoi(s); return args[i-1] }
		where := cReAssign.FindStringSubmatch(m[3])
		id := cI64(arg(where[2]))
		cur, ok := t.tabs[tab][id]
		if !ok {
			return pgconn.NewCommandTag("UPDATE 0"), nil
		}
		if m[4] != "" {
			nw := cReAssign.FindStringSubmatch(m[4])
			if _, exists := t.tabs[tab][cI64(arg(nw[2]))]; exists {
				return pgconn.NewCommandTag("UPDATE 0"), nil
			}
		}
		tx.save(tab, id)
		for _, s := range cReAssign.FindAllStringSubmatch(m[2], -1) {
			cur[s[1]] = arg(s[2])
		}
		if nid := cI64(cur["id"]); nid != id {
			if _, exists := t.tabs[tab][nid]; exists {
				return pgconn.CommandTag{}, cErr("23505", "duplicate pk on update")
			}
			tx.save(tab, nid)
			delete(t.tabs[tab], id)
			t.tabs[tab][nid] = cur
		}
		return pgconn.NewCommandTag("UPDATE 1"), t.checkUnique(tab)
	}
	return pgconn.CommandTag{}, fmt.Errorf("fake tx: unsupported statement %q", q)
}

type cRowRes struct {
	vals []any
	err  error
}

func (r cRowRes) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *int:
			*p = r.vals[i].(int)
		case *bool:
			*p = r.vals[i].(bool)
		}
	}
	return nil
}

func (tx *cTx) QueryRow(_ context.Context, q string, args ...any) pgx.Row {
	if strings.HasPrefix(q, "SELECT EXISTS") {
		return cRowRes{vals: []any{tx.t.ordered[args[0].(string)]}}
	}
	if m := cReSel1.FindStringSubmatch(q); m != nil {
		w := cReAssign.FindStringSubmatch(m[2])
		i, _ := strconv.Atoi(w[2])
		if _, ok := tx.t.tabs[m[1]][cI64(args[i-1])]; !ok {
			return cRowRes{err: pgx.ErrNoRows}
		}
		return cRowRes{vals: []any{1}}
	}
	return cRowRes{err: fmt.Errorf("fake tx: unsupported query %q", q)}
}

func (tx *cTx) CopyFrom(_ context.Context, name pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	if err := tx.inject(); err != nil {
		return 0, err
	}
	tmp := name[len(name)-1]
	if _, ok := tx.tmpOf[tmp]; !ok {
		return 0, fmt.Errorf("copy into unknown temp table %s", tmp)
	}
	var n int64
	for src.Next() {
		vals, err := src.Values()
		if err != nil {
			return n, err
		}
		r := cRow{}
		for i, c := range cols {
			r[c] = vals[i]
		}
		tx.tmpRows[tmp] = append(tx.tmpRows[tmp], r)
		n++
	}
	return n, src.Err()
}

type cBatchRes struct {
	tx  *cTx
	q   []*pgx.QueuedQuery
	i   int
	err error
}

func (b *cBatchRes) Exec() (pgconn.CommandTag, error) {
	if b.err != nil {
		return pgconn.CommandTag{}, b.err
	}
	q := b.q[b.i]
	b.i++
	tag, err := b.tx.Exec(context.Background(), q.SQL, q.Arguments...)
	if err != nil {
		b.err = err
	}
	return tag, err
}
func (b *cBatchRes) Query() (pgx.Rows, error) { panic("unused") }
func (b *cBatchRes) QueryRow() pgx.Row        { panic("unused") }
func (b *cBatchRes) Close() error             { return b.err }
func (b *cBatchRes) Tx() pgx.Tx               { return b.tx }

func (tx *cTx) SendBatch(_ context.Context, b *pgx.Batch) pgx.BatchResults {
	return &cBatchRes{tx: tx, q: b.QueuedQueries}
}

func (tx *cTx) Begin(context.Context) (pgx.Tx, error) { panic("unused") }
func (tx *cTx) LargeObjects() pgx.LargeObjects       { panic("unused") }
func (tx *cTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	panic("unused")
}
func (tx *cTx) Query(context.Context, string, ...any) (pgx.Rows, error) { panic("unused") }
func (tx *cTx) Conn() *pgx.Conn                                         { panic("unused") }

// --- driver database/sql giả cho checkpoint.Compactor ---

type cConnector struct{ t *cTarget }

func (c cConnector) Connect(context.Context) (driver.Conn, error) { return cConn(c), nil }
func (c cConnector) Driver() driver.Driver                        { return cDrv{} }

type cDrv struct{}

func (cDrv) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type cConn struct{ t *cTarget }

func (c cConn) Prepare(q string) (driver.Stmt, error) { return nil, fmt.Errorf("prepare unsupported: %s", q) }
func (c cConn) Close() error                          { return nil }
func (c cConn) Begin() (driver.Tx, error)             { return nil, errors.New("no tx") }

type cRows struct {
	cols []string
	data [][]driver.Value
	i    int
}

func (r *cRows) Columns() []string { return r.cols }
func (r *cRows) Close() error      { return nil }
func (r *cRows) Next(dest []driver.Value) error {
	if r.i >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.i])
	r.i++
	return nil
}

func (c cConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	t := c.t
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case strings.HasPrefix(q, "SELECT worker_id, epoch, batch_no, applied FROM _strand_worker_delta"):
		r := &cRows{cols: []string{"worker_id", "epoch", "batch_no", "applied"}}
		var ids []int64
		for id := range t.deltas {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			d := t.deltas[id]
			r.data = append(r.data, []driver.Value{id, int64(1), d.batchNo, d.applied})
		}
		return r, nil
	case strings.HasPrefix(q, "SELECT applied FROM _strand_checkpoint"):
		return &cRows{cols: []string{"applied"}, data: [][]driver.Value{{t.global}}}, nil
	}
	return nil, fmt.Errorf("fake db: unsupported query %q", q)
}

func (c cConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	t := c.t
	t.mu.Lock()
	defer t.mu.Unlock()
	switch {
	case strings.HasPrefix(q, "UPDATE _strand_checkpoint SET applied = $1"):
		t.global = args[0].Value.(string)
		t.globals++
		t.sim.checkGlobal(t.global)
		return driver.RowsAffected(1), nil
	case strings.HasPrefix(q, "UPDATE _strand_worker_delta SET applied = ''"):
		wid, bn := args[1].Value.(int64), args[3].Value.(int64)
		if d := t.deltas[wid]; d != nil && d.batchNo == bn {
			d.applied = ""
			return driver.RowsAffected(1), nil
		}
		return driver.RowsAffected(0), nil
	}
	return nil, fmt.Errorf("fake db: unsupported exec %q", q)
}

// ---------------------------------------------------------------------------
// Writer ghi nhận: bọc PGWriter thật; transaction giả biết batch để ghi nhận commit.
// ---------------------------------------------------------------------------

type cWriter struct {
	*writer.PGWriter
	cur []*model.Txn
}

func (w *cWriter) ApplyBatch(ctx context.Context, batch []*model.Txn, delta writer.DeltaRow) error {
	w.cur = batch
	return w.PGWriter.ApplyBatch(ctx, batch, delta)
}

// ---------------------------------------------------------------------------
// Source + mô phỏng
// ---------------------------------------------------------------------------

type cSrcTxn struct {
	lsn       uint64
	needApply bool // có row items/accounts: router luôn giữ
	desc      string
}

type cSim struct {
	t    *testing.T
	seed int64
	rmu  sync.Mutex
	r    *rand.Rand
	dir  string

	p   *Pipeline
	tgt *cTarget

	srcMu   sync.Mutex
	src     map[model.TableID]map[int64]cRow
	queue   []*model.Txn
	qCond   *sync.Cond
	lsn     uint64
	xid     uint64
	ver     int64
	nextID  int64
	srcTxns []cSrcTxn
	lastLSN uint64

	emu  sync.Mutex
	errs []string
	logs bytes.Buffer // log WARN+ của pipeline
	lmu  sync.Mutex

	st   sync.Map // bộ đếm
	opts snapshot.ChunkOptions
	cfgS string
}

func (s *cSim) intn(n int) int {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	return s.r.Intn(n)
}

func (s *cSim) count(k string, n int) {
	v, _ := s.st.LoadOrStore(k, new(atomic.Int64))
	v.(*atomic.Int64).Add(int64(n))
}

func (s *cSim) errf(f string, a ...any) {
	s.emu.Lock()
	defer s.emu.Unlock()
	if len(s.errs) < 50 {
		s.errs = append(s.errs, fmt.Sprintf(f, a...))
	}
}

func (s *cSim) nErrs() int {
	s.emu.Lock()
	defer s.emu.Unlock()
	return len(s.errs)
}

type lockedWriter struct {
	mu *sync.Mutex
	b  *bytes.Buffer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.b.Len() > 1<<20 {
		return len(p), nil
	}
	return l.b.Write(p)
}

// checkGlobal (giữ tgt.mu): mọi Txn source cần apply mà tập toàn cục chứa phải đã commit ở target.
func (s *cSim) checkGlobal(g string) {
	set, err := checkpoint.ParseAppliedSet(model.EnginePG, g)
	if err != nil {
		s.errf("parse global %q: %v", g, err)
		return
	}
	s.srcMu.Lock()
	txns := s.srcTxns
	s.srcMu.Unlock()
	for _, st := range txns {
		if st.needApply && set.Contains(model.Pos{Engine: model.EnginePG, LSN: st.lsn}) && !s.tgt.succLSN[st.lsn] {
			s.errf("checkpoint vouches LSN %X (%s) that was never committed at target (global %s)", st.lsn, st.desc, g)
		}
	}
}

func (s *cSim) nv() int64 { s.ver++; return s.ver }

func (s *cSim) image(tb model.TableID, op model.Op, id, oldID int64, toast bool) model.RowChange {
	rc := model.RowChange{Table: tb, Op: op, Key: []model.Value{id}}
	if oldID != 0 {
		rc.OldKey = []model.Value{oldID}
	}
	if op == model.OpDelete {
		return rc
	}
	r := s.src[tb][id]
	names := []string{"id", "ver"}
	switch tb {
	case cItems:
		names = append(names, "qty", "doc")
	case cAccounts:
		names = append(names, "code")
		rc.UniqueKeys = []model.UniqueKey{{Name: "accounts_code_key", Key: []model.Value{r["code"]}}}
	}
	for _, n := range names {
		cv := model.ColValue{Name: n, Value: r[n]}
		if n == "doc" && toast {
			cv = model.ColValue{Name: n, Unchanged: true}
		}
		rc.Cols = append(rc.Cols, cv)
	}
	return rc
}

func (s *cSim) pick(tb model.TableID) (int64, bool) {
	var ids []int64
	for id := range s.src[tb] {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return 0, false
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if s.intn(2) == 0 && len(ids) > 6 {
		ids = ids[:6] // khóa nóng
	}
	return ids[s.intn(len(ids))], true
}

func (s *cSim) freeID(tb model.TableID, span int64) int64 {
	for {
		id := 1 + int64(s.intn(int(span)))
		if s.intn(3) == 0 {
			s.nextID++
			id = s.nextID
		}
		if _, ok := s.src[tb][id]; !ok {
			return id
		}
	}
}

// commit (giữ srcMu): Txn visible + vào WAL/stream theo đúng thứ tự commit LSN.
func (s *cSim) commit(tx *model.Txn, desc string) {
	if len(tx.Rows) == 0 && tx.SpillPath == "" {
		return
	}
	s.xid++
	s.lsn += uint64(1 + s.intn(5))
	tx.XID = s.xid
	tx.SourcePos = model.Pos{Engine: model.EnginePG, LSN: s.lsn}
	need := false
	for _, rc := range tx.Rows {
		if rc.Table != cEvents {
			need = true
		}
	}
	if tx.Bytes == 0 {
		tx.Bytes = int64(40 * len(tx.Rows))
	}
	s.srcTxns = append(s.srcTxns, cSrcTxn{lsn: s.lsn, needApply: need, desc: desc})
	s.lastLSN = s.lsn
	s.queue = append(s.queue, tx)
	s.qCond.Broadcast()
}

// watermark (giữ srcMu): logical decoding message không transactional (Txn rỗng, XID 0).
func (s *cSim) watermark() uint64 {
	high := s.lsn + 1
	s.lsn++
	s.lastLSN = s.lsn
	s.queue = append(s.queue, &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: s.lsn}})
	s.qCond.Broadcast()
	return high
}

func (s *cSim) genTxn() {
	s.srcMu.Lock()
	defer s.srcMu.Unlock()
	tx := &model.Txn{}
	add := func(rc model.RowChange) { tx.Rows = append(tx.Rows, rc) }
	itemUpd := func(id int64, toast bool) {
		r := s.src[cItems][id]
		r["ver"], r["qty"] = s.nv(), r["qty"].(int64)+1
		if !toast {
			r["doc"] = fmt.Sprintf("d%d-%d", id, s.ver)
		}
		add(s.image(cItems, model.OpUpdate, id, 0, toast))
	}
	desc := ""
	switch c := s.intn(100); {
	case c < 22:
		desc = "item update"
		if id, ok := s.pick(cItems); ok {
			itemUpd(id, s.intn(3) > 0)
		}
	case c < 27:
		desc = "item insert"
		id := s.freeID(cItems, 40)
		s.src[cItems][id] = cRow{"id": id, "ver": s.nv(), "qty": int64(1), "doc": fmt.Sprintf("n%d", s.ver)}
		add(s.image(cItems, model.OpInsert, id, 0, false))
	case c < 30:
		desc = "item delete"
		if id, ok := s.pick(cItems); ok {
			delete(s.src[cItems], id)
			add(s.image(cItems, model.OpDelete, id, 0, false))
		}
	case c < 33:
		desc = "item pk change"
		if id, ok := s.pick(cItems); ok {
			nid := s.freeID(cItems, 40)
			r := s.src[cItems][id]
			delete(s.src[cItems], id)
			r["id"], r["ver"] = nid, s.nv()
			s.src[cItems][nid] = r
			add(s.image(cItems, model.OpUpdate, nid, id, s.intn(2) == 0))
		}
	case c < 48:
		desc = "events upsert"
		for n := 1 + s.intn(3); n > 0; n-- {
			id := int64(1 + s.intn(60))
			op := model.OpUpdate
			if _, ok := s.src[cEvents][id]; !ok {
				op = model.OpInsert
			}
			s.src[cEvents][id] = cRow{"id": id, "ver": s.nv()}
			add(s.image(cEvents, op, id, 0, false))
		}
	case c < 55:
		desc = "events pk change/delete"
		if id, ok := s.pick(cEvents); ok {
			if s.intn(3) == 0 {
				delete(s.src[cEvents], id)
				add(s.image(cEvents, model.OpDelete, id, 0, false))
				break
			}
			nid := s.freeID(cEvents, 80)
			delete(s.src[cEvents], id)
			s.src[cEvents][nid] = cRow{"id": nid, "ver": s.nv()}
			add(s.image(cEvents, model.OpUpdate, nid, id, false))
		}
	case c < 57:
		desc = "big events insert (spilled)"
		n := 20 + s.intn(200)
		f, err := os.CreateTemp(s.dir, "src-*.spill")
		if err != nil {
			s.errf("spill: %v", err)
			return
		}
		sw := model.NewSpillWriter(f)
		for i := 0; i < n; i++ {
			s.nextID++
			id := s.nextID
			s.src[cEvents][id] = cRow{"id": id, "ver": s.nv()}
			rc := s.image(cEvents, model.OpInsert, id, 0, false)
			if err := sw.Write(&rc); err != nil {
				s.errf("spill write: %v", err)
			}
		}
		if err := sw.Flush(); err != nil {
			s.errf("spill flush: %v", err)
		}
		_ = f.Close()
		tx.SpillPath, tx.RowCount, tx.Bytes = f.Name(), int64(n), int64(60*n)
		s.count("big spilled txns", 1)
	case c < 59:
		desc = "big events insert (RAM)"
		for n := 20 + s.intn(150); n > 0; n-- {
			s.nextID++
			id := s.nextID
			s.src[cEvents][id] = cRow{"id": id, "ver": s.nv()}
			add(s.image(cEvents, model.OpInsert, id, 0, false))
		}
	case c < 68:
		desc = "accounts code swap"
		a, ok1 := s.pick(cAccounts)
		b, ok2 := s.pick(cAccounts)
		if !ok1 || !ok2 || a == b {
			return
		}
		ra, rb := s.src[cAccounts][a], s.src[cAccounts][b]
		ca, cb := ra["code"], rb["code"]
		ra["code"], ra["ver"] = "tmp-"+ca.(string), s.nv()
		add(s.image(cAccounts, model.OpUpdate, a, 0, false))
		rb["code"], rb["ver"] = ca, s.nv()
		add(s.image(cAccounts, model.OpUpdate, b, 0, false))
		ra["code"], ra["ver"] = cb, s.nv()
		add(s.image(cAccounts, model.OpUpdate, a, 0, false))
	case c < 73:
		desc = "accounts move code"
		a, ok1 := s.pick(cAccounts)
		b, ok2 := s.pick(cAccounts)
		if !ok1 || !ok2 || a == b {
			return
		}
		ra, rb := s.src[cAccounts][a], s.src[cAccounts][b]
		x := ra["code"]
		ra["code"], ra["ver"] = fmt.Sprintf("M%d-%d", a, s.ver), s.nv()
		add(s.image(cAccounts, model.OpUpdate, a, 0, false))
		s.commit(tx, "accounts move code (1/2)")
		tx = &model.Txn{}
		rb["code"], rb["ver"] = x, s.nv()
		add(s.image(cAccounts, model.OpUpdate, b, 0, false))
	case c < 76:
		desc = "accounts insert"
		id := s.freeID(cAccounts, 30)
		s.src[cAccounts][id] = cRow{"id": id, "ver": s.nv(), "code": fmt.Sprintf("N%d-%d", id, s.ver)}
		add(s.image(cAccounts, model.OpInsert, id, 0, false))
	default:
		desc = "multi-table items+events+accounts"
		id, ok := s.pick(cItems)
		if !ok {
			return
		}
		itemUpd(id, true)
		for n := 1 + s.intn(5); n > 0; n-- {
			s.nextID++
			e := s.nextID
			s.src[cEvents][e] = cRow{"id": e, "ver": s.nv()}
			add(s.image(cEvents, model.OpInsert, e, 0, false))
		}
		if _, ok := s.src[cAccounts][id]; ok {
			r := s.src[cAccounts][id]
			r["ver"] = s.nv()
			add(s.image(cAccounts, model.OpUpdate, id, 0, false))
		}
	}
	s.commit(tx, desc)
}

// --- ChunkSource giả (phần source của PGChunkSource, commit visible nguyên tử) ---

type cChunks struct{ s *cSim }

// snap (giữ srcMu): mọi xid đã cấp đều đã commit, nên snapshot = [xid+1, xid+1).
func (c cChunks) snap() *snapshot.PGSnapshot {
	ps, err := snapshot.ParsePGSnapshot(fmt.Sprintf("%d:%d:", c.s.xid+1, c.s.xid+1))
	if err != nil {
		panic(err)
	}
	return ps
}

func (c cChunks) stall() {
	if c.s.intn(4) == 0 {
		time.Sleep(time.Duration(c.s.intn(3000)) * time.Microsecond)
	}
}

func (c cChunks) BeforeRegister(context.Context) error {
	c.s.srcMu.Lock()
	defer c.s.srcMu.Unlock()
	ps := c.snap()
	c.s.p.xids.Prune(ps)
	if !snapshot.AllVisible(ps, c.s.p.xids.Pending()) {
		return fmt.Errorf("routed xids not visible")
	}
	return nil
}

func (c cChunks) keys() []int64 {
	var ks []int64
	for k := range c.s.src[cEvents] {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i] < ks[j] })
	return ks
}

func (c cChunks) Bounds(context.Context, model.TableID, string) (*snapshot.TableBounds, error) {
	c.stall()
	c.s.srcMu.Lock()
	defer c.s.srcMu.Unlock()
	ks := c.keys()
	var b snapshot.TableBounds
	if len(ks) > 0 {
		b.MinPK, b.MaxPK, b.Count = ks[0], ks[len(ks)-1], snapshot.SpanRows(ks[0], ks[len(ks)-1])
	}
	return &b, nil
}

func (c cChunks) KeysetEnd(_ context.Context, _ model.TableID, _ string, start, max, rows int64) (int64, error) {
	c.stall()
	c.s.srcMu.Lock()
	defer c.s.srcMu.Unlock()
	var in []int64
	for _, k := range c.keys() {
		if k >= start && k <= max {
			in = append(in, k)
		}
	}
	if int64(len(in)) < rows {
		return max, nil
	}
	return in[rows-1], nil
}

func (c cChunks) ReadChunk(ctx context.Context, sn *snapshot.MySQLSnapshotter, chunk *snapshot.Chunk, _ []string, _ []int,
	table model.TableID) (snapshot.ChunkRead, checkpoint.AppliedSet, error) {
	if chunk.KeySet != nil {
		return snapshot.ChunkRead{}, nil, fmt.Errorf("unexpected re-read chunk")
	}
	c.stall()
	if !chunk.Continuation {
		sn.Manager.BeginReading(chunk)
		c.stall()
		c.s.srcMu.Lock()
		low := c.snap()
		c.s.p.xids.Prune(low)
		c.s.srcMu.Unlock()
		sn.Manager.StartReadingPG(chunk, low)
	}
	queryFn := func(_ context.Context, ch *snapshot.Chunk, maxBytes int64) ([]*model.RowChange, int64, error) {
		c.stall()
		c.s.srcMu.Lock()
		defer c.s.srcMu.Unlock()
		var out []*model.RowChange
		var total int64
		for _, k := range c.keys() {
			if k < ch.StartPK || k > ch.EndPK {
				continue
			}
			v := c.s.src[cEvents][k]["ver"]
			out = append(out, &model.RowChange{Table: table, Op: model.OpInsert, Key: []model.Value{k},
				Cols: []model.ColValue{{Name: "id", Value: k}, {Name: "ver", Value: v}}})
			total += 10
			if maxBytes > 0 && total >= maxBytes {
				break
			}
		}
		return out, total, nil
	}
	read, err := sn.Chunker.ReadChunkAdaptive(ctx, chunk, c.s.opts, queryFn)
	if err != nil {
		return snapshot.ChunkRead{}, nil, err
	}
	if read.Truncated {
		c.s.count("truncated chunk reads", 1)
	}
	c.s.count("chunks", 1)
	c.stall()
	c.s.srcMu.Lock()
	high := c.s.watermark()
	c.s.srcMu.Unlock()
	return read, checkpoint.NewPGAppliedSet(high), nil
}

// --- phần target của snapshot ---

func (s *cSim) flushRows(_ context.Context, table model.TableID, _ string, _ []string, rows []*model.RowChange, startPK, endPK int64) error {
	out, err := s.p.pgFlushImages(table, rows)
	if err != nil {
		return err
	}
	t := s.tgt
	t.mu.Lock()
	defer t.mu.Unlock()
	tab := cQuote(table)
	keep := map[int64]cRow{}
	for _, r := range out {
		row := cRow{}
		for _, cv := range r.Cols {
			row[cv.Name] = cv.Value
		}
		keep[cI64(r.Key[0])] = row
	}
	var touched []cUndo
	for id := range t.tabs[tab] {
		if id >= startPK && id <= endPK {
			if _, ok := keep[id]; !ok {
				delete(t.tabs[tab], id)
			}
		}
	}
	for id, r := range keep {
		t.tabs[tab][id] = r
		touched = append(touched, cUndo{tab: tab, id: id})
	}
	t.checkVers(touched, "snapshot flush")
	s.count("snapshot flushes", 1)
	if s.intn(3) == 0 {
		time.Sleep(time.Duration(s.intn(5000)) * time.Microsecond) // flush dài trong barrier: router bị giữ
	}
	return nil
}

func (s *cSim) clearTarget(_ context.Context, table model.TableID) error {
	t := s.tgt
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tabs[cQuote(table)] = map[int64]cRow{}
	return nil
}

// ---------------------------------------------------------------------------
// Dựng pipeline
// ---------------------------------------------------------------------------

func newCSim(t *testing.T, seed int64) *cSim {
	s := &cSim{t: t, seed: seed, r: rand.New(rand.NewSource(seed)), dir: t.TempDir(), lsn: 0x10000, xid: 1000, ver: 1000, nextID: 100000,
		src: map[model.TableID]map[int64]cRow{cItems: {}, cAccounts: {}, cEvents: {}}}
	s.qCond = sync.NewCond(&s.srcMu)
	for id := int64(1); id <= 20; id++ {
		s.src[cItems][id] = cRow{"id": id, "ver": s.nv(), "qty": int64(0), "doc": fmt.Sprintf("d%d", id)}
		s.src[cAccounts][id] = cRow{"id": id, "ver": s.nv(), "code": fmt.Sprintf("C%d", id)}
	}
	for id := int64(1); id <= int64(40+s.intn(200)); id++ {
		s.src[cEvents][id] = cRow{"id": id, "ver": s.nv()}
	}
	tg := &cTarget{sim: s, tabs: map[string]map[int64]cRow{}, ordered: map[string]bool{cQuote(cAccounts): true},
		unique: map[string]string{cQuote(cAccounts): "code"}, deltas: map[int64]*cDelta{}, lastVer: map[string]int64{},
		rng: rand.New(rand.NewSource(seed * 7)), succSeq: map[uint64]bool{}, succLSN: map[uint64]bool{}}
	if s.intn(3) > 0 {
		tg.errRate = []int{40, 200, 1000}[s.intn(3)]
	}
	for tb, rows := range s.src {
		m := map[int64]cRow{}
		for id, r := range rows {
			m[id] = r.clone() // target có cùng dữ liệu ban đầu (kể cả events, sẽ bị ClearTarget xóa khi add)
		}
		tg.tabs[cQuote(tb)] = m
	}
	s.tgt = tg
	return s
}

func (s *cSim) build(ctx context.Context, cancel context.CancelFunc) {
	t := s.t
	st, err := openLocalState(s.dir, "p")
	if err != nil {
		t.Fatal(err)
	}
	for _, tb := range []model.TableID{cItems, cAccounts} {
		if err := st.SetDone(tb.String(), true); err != nil {
			t.Fatal(err)
		}
	}
	filter, err := newTableFilter([]string{"app.items", "app.accounts"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	maxRows := []int{1, 3, 10, 50, 2000}[s.intn(5)]
	maxBytes := []string{"200", "2KiB", "4MiB"}[s.intn(3)]
	maxWait := []string{"", "1ms", "5ms"}[s.intn(3)]
	cfg := &config.Config{}
	cfg.Target.Batch.MaxRows, cfg.Target.Batch.MaxBytes, cfg.Target.Batch.MaxWait = maxRows, maxBytes, maxWait
	scfg := sched.DefaultConfig()
	scfg.MaxPending = []int{1, 2, 4, 16, 65536}[s.intn(5)]
	scfg.MaxPendingBytes = []int64{100, 2000, 512 << 20}[s.intn(3)]
	scfg.MaxHoles = []int{1, 2, 8, 4096}[s.intn(4)]
	if s.intn(4) == 0 {
		scfg.TableLevelThreshold = 5
	}
	if s.intn(4) == 0 {
		scfg.WritesetBudget = 50
	}
	s.opts = snapshot.ChunkOptions{Timeout: time.Hour, MaxChunkBytes: []int64{60, 1 << 20}[s.intn(2)], MinChunkRows: 1,
		DefaultChunkRows: []int64{3, 10, 40}[s.intn(3)]}
	logger := slog.New(slog.NewTextHandler(lockedWriter{&s.lmu, &s.logs}, &slog.HandlerOptions{Level: slog.LevelWarn}))
	p := &Pipeline{cfg: cfg, log: logger, id: "p", strict: false, srcEngine: model.EnginePG, dstEngine: model.EnginePG,
		filter: filter, state: st, lsns: &lsnTracker{}, xids: snapshot.NewXIDTracker(),
		metrics: metrics.NewMetrics(prometheus.NewRegistry()), health: metrics.NewHealthChecker(time.Hour),
		sched: sched.NewScheduler(scfg), touched: newTouchedKeys(200000), runCtx: ctx, cancel: cancel}
	p.seenCond = sync.NewCond(&p.seenMu)
	p.pauseCond = sync.NewCond(&p.pauseMu)
	p.partRoots.m = map[model.TableID]model.TableID{cEvents: {}} // events không phải partition (không tra DB)
	p.tables = map[model.TableID]*tableMeta{
		cItems:    {ID: cItems, Columns: []string{"id", "ver", "qty", "doc"}, PK: []string{"id"}, PKNumeric: true, AvgRow: 40},
		cAccounts: {ID: cAccounts, Columns: []string{"id", "ver", "code"}, PK: []string{"id"}, PKNumeric: true, AvgRow: 30, LaneTable: true},
	}
	start := checkpoint.NewPGAppliedSet(s.lsn)
	p.applied0 = start
	s.tgt.global = start.String()
	p.laneOn = s.intn(5) > 0 // thật: luôn bật với source PG relaxed; tắt để đi đường applyInSeqOrder
	nWorkers := 1 + s.intn(4)
	total := nWorkers
	if p.laneOn {
		total++
	}
	copyMin := []int{0, 3, 8, 1000}[s.intn(4)]
	for i := 1; i <= total; i++ {
		pw := writer.NewPGWriter(nil, p.id, i, 1)
		pw.SetCopyMinRows(copyMin)
		cw := &cWriter{PGWriter: pw}
		pw.SetTxBeginner(func(context.Context) (pgx.Tx, error) { return s.tgt.begin(cw.cur), nil })
		for id, m := range p.tables {
			cw.SetPrimaryKey(p.filter.Target(id), m.PK)
		}
		s.tgt.deltas[int64(i)] = &cDelta{batchNo: 1}
		p.writers = append(p.writers, cw)
	}
	ccfg := checkpoint.DefaultCompactorConfig(p.id, 1)
	ccfg.Interval = time.Duration(1+s.intn(15)) * time.Millisecond
	p.comp = checkpoint.NewCompactor(sql.OpenDB(cConnector{s.tgt}), nil, p.id, model.EnginePG, model.EnginePG, ccfg)
	p.comp.SetVouch(p.vouchGlobal)
	p.comp.SetLastGlobal(start)
	p.snap = snapshot.NewMySQLSnapshotter(s.opts)
	p.snap.Barrier = p.barrier
	p.snap.TargetName = p.filter.Target
	p.snap.LoadPlan = func(t model.TableID) (snapshot.SnapshotPlan, bool) { return p.state.Plan(t.String()) }
	p.snap.SavePlan = func(t model.TableID, pl snapshot.SnapshotPlan) error { return p.state.SetPlan(t.String(), pl) }
	p.snap.FlushRows = s.flushRows
	p.snap.ClearTarget = s.clearTarget
	p.snap.FlushKeys = func(context.Context, model.TableID, string, []string, []*model.RowChange, []int64) error {
		return errors.New("unexpected FlushKeys")
	}
	p.snap.Chunks = cChunks{s}
	s.p = p
	s.cfgS = fmt.Sprintf("workers %d lane %v maxRows %d maxBytes %s maxWait %q maxPending %d maxPendingBytes %d maxHoles %d tlt %d wsb %d chunkRows %d chunkBytes %d copyMin %d errRate %d",
		nWorkers, p.laneOn, maxRows, maxBytes, maxWait, scfg.MaxPending, scfg.MaxPendingBytes, scfg.MaxHoles,
		scfg.TableLevelThreshold, scfg.WritesetBudget, s.opts.DefaultChunkRows, s.opts.MaxChunkBytes, copyMin, s.tgt.errRate)
}

// snapshotAdd: phần cuối của Pipeline.SnapshotAdd (sau các bước tra DB), rồi snapshotTable THẬT.
func (s *cSim) snapshotAdd(ctx context.Context, done chan<- error) {
	p := s.p
	id := cEvents
	m := &tableMeta{ID: id, Columns: []string{"id", "ver"}, PK: []string{"id"}, PKNumeric: true, AvgRow: 20}
	if err := p.state.SetDone(id.String(), false); err != nil {
		done <- err
		return
	}
	for _, w := range p.writers {
		w.SetPrimaryKey(p.filter.Target(id), m.PK)
	}
	p.routeMu.Lock()
	p.tablesMu.Lock()
	p.tables[id] = m
	p.tablesMu.Unlock()
	p.routeMu.Unlock()
	p.bootstrap.Store(true)
	go func() {
		err := p.snapshotTable(ctx, id)
		if err != nil && ctx.Err() == nil {
			p.fail(fmt.Errorf("snapshot add %s: %w", id, err))
		}
		done <- err
	}()
}

// run: một lần chạy; cancelAt > 0: hủy ctx sau từng ấy Txn (dừng không drain).
func (s *cSim) run(nTxns, cancelAt int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.build(ctx, cancel)
	p := s.p
	txns := make(chan *model.Txn, 1+s.intn(64))
	var wg sync.WaitGroup
	// bơm stream → router
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(txns)
		for {
			s.srcMu.Lock()
			for len(s.queue) == 0 && ctx.Err() == nil {
				s.qCond.Wait()
			}
			if ctx.Err() != nil {
				s.srcMu.Unlock()
				return
			}
			t := s.queue[0]
			s.queue = s.queue[1:]
			s.srcMu.Unlock()
			select {
			case txns <- t:
			case <-ctx.Done():
				return
			}
		}
	}()
	stopQ := context.AfterFunc(ctx, func() { s.srcMu.Lock(); s.qCond.Broadcast(); s.srcMu.Unlock() })
	defer stopQ()
	p.readyCh = p.sched.Ready()
	for i, w := range p.writers {
		wg.Add(1)
		go func(i int, w batchWriter) {
			defer wg.Done()
			if p.laneOn && i == len(p.writers)-1 {
				p.laneWorker(ctx, i+1, w)
				return
			}
			p.worker(ctx, i+1, w)
		}(i, w)
	}
	wg.Add(1)
	go func() { defer wg.Done(); p.router(ctx, txns, nil) }()
	p.comp.Start(ctx)
	// Bất biến (1): seq ≤ Lwd ⇒ transaction dữ liệu đã commit.
	var checked uint64
	checkLwd := func() {
		lwd := p.sched.Lwd()
		s.tgt.mu.Lock()
		for ; checked < lwd; checked++ {
			if !s.tgt.succSeq[checked+1] {
				s.errf("seq %d is below lwd %d but no data transaction of it ever committed at target", checked+1, lwd)
			}
		}
		s.tgt.mu.Unlock()
	}
	chkDone := make(chan struct{})
	var chkWG sync.WaitGroup
	chkWG.Add(1)
	go func() {
		defer chkWG.Done()
		for {
			select {
			case <-chkDone:
				return
			case <-time.After(300 * time.Microsecond):
			}
			checkLwd()
		}
	}()

	snapDone := make(chan error, 1)
	addAt := s.intn(nTxns)
	added := false
	burst := 0
	for i := 0; i < nTxns && ctx.Err() == nil && s.nErrs() == 0; i++ {
		if i == addAt {
			s.snapshotAdd(ctx, snapDone)
			added = true
		}
		if cancelAt > 0 && i == cancelAt {
			cancel()
			break
		}
		s.genTxn()
		if burst > 0 {
			burst--
			continue
		}
		switch x := s.intn(100); {
		case x < 3:
			burst = 20 + s.intn(100)
		case x < 60:
		default:
			time.Sleep(time.Duration(s.intn(300)) * time.Microsecond)
		}
	}
	if !added && cancelAt == 0 {
		s.snapshotAdd(ctx, snapDone)
		added = true
	}
	// Chờ: snapshot xong, stream đã route hết, Lwd ≥ admitted, row toàn cục tới LSN cuối.
	deadline := time.Now().Add(60 * time.Second)
	snapErr := error(nil)
	snapFinished := !added
	for cancelAt == 0 && s.nErrs() == 0 && p.failed() == nil {
		if !snapFinished {
			select {
			case snapErr = <-snapDone:
				snapFinished = true
			default:
			}
		}
		s.srcMu.Lock()
		last := s.lastLSN
		qEmpty := len(s.queue) == 0
		s.srcMu.Unlock()
		s.tgt.mu.Lock()
		gset, _ := checkpoint.ParseAppliedSet(model.EnginePG, s.tgt.global)
		s.tgt.mu.Unlock()
		if snapFinished && qEmpty && p.lsns.seenLSN() >= last && p.sched.Lwd() >= p.admitted.Load() &&
			gset != nil && gset.Lwd().LSN >= last {
			break
		}
		if time.Now().After(deadline) {
			s.errf("stuck: snapshot finished %v, queue empty %v, seen %X/%X, lwd %d admitted %d, lane pending %d, holes %d, global %s",
				snapFinished, qEmpty, p.lsns.seenLSN(), last, p.sched.Lwd(), p.admitted.Load(), p.sched.LanePending(), p.sched.Holes(), s.tgt.global)
			break
		}
		time.Sleep(time.Millisecond)
	}
	if snapErr != nil && cancelAt == 0 {
		s.errf("snapshot: %v", snapErr)
	}
	cancel()
	wg.Wait()
	if added && !snapFinished {
		<-snapDone // goroutine snapshot dừng theo ctx
	}
	p.comp.Stop()
	close(chkDone)
	chkWG.Wait()
	checkLwd()
	if err := p.failed(); err != nil && cancelAt == 0 {
		s.errf("pipeline failed: %v", err)
	}
	if cancelAt == 0 && s.nErrs() == 0 {
		for _, d := range s.diff() {
			s.errf("final: %s", d)
		}
	}
	s.count("txns", len(s.srcTxns))
	s.count("admitted", int(p.admitted.Load()))
	s.count("reorder retries", int(p.reorderRetry.Load()))
	s.count("globals", s.tgt.globals)
	s.count("target stmts", int(s.tgt.stmts))
	if p.laneOn {
		s.count("lane runs", 1)
	}
}

func (s *cSim) diff() []string {
	var out []string
	for _, tb := range cTables {
		got := s.tgt.tabs[cQuote(tb)]
		for id, r := range s.src[tb] {
			g, ok := got[id]
			if !ok {
				out = append(out, fmt.Sprintf("%s/%d missing (source %v)", tb, id, r))
				continue
			}
			for n, v := range r {
				if fmt.Sprint(g[n]) != fmt.Sprint(v) {
					out = append(out, fmt.Sprintf("%s/%d col %s source %v target %v", tb, id, n, v, g[n]))
				}
			}
		}
		for id := range got {
			if _, ok := s.src[tb][id]; !ok {
				out = append(out, fmt.Sprintf("%s/%d extra on target %v", tb, id, got[id]))
			}
		}
	}
	sort.Strings(out)
	if len(out) > 20 {
		out = out[:20]
	}
	return out
}

func TestF11CombinedSim_PipelineRelaxedSnapshotAdd(t *testing.T) {
	n := 30 // F11_COMBINED_SEEDS=2000 để khám phá kỹ
	if v, err := strconv.Atoi(os.Getenv("F11_COMBINED_SEEDS")); err == nil && v > 0 {
		n = v
	}
	first := int64(1)
	if v, err := strconv.ParseInt(os.Getenv("F11_COMBINED_FIRST"), 10, 64); err == nil && v > 0 {
		first = v
	}
	agg := map[string]int64{}
	for seed := first; seed < first+int64(n); seed++ {
		s := newCSim(t, seed)
		nTxns := 200 + s.intn(800)
		cancelAt := 0
		if s.intn(6) == 0 {
			cancelAt = 1 + s.intn(nTxns)
		}
		s.run(nTxns, cancelAt)
		s.st.Range(func(k, v any) bool { agg[k.(string)] += v.(*atomic.Int64).Load(); return true })
		if cancelAt > 0 {
			agg["cancelled runs"]++
		}
		if len(s.errs) > 0 {
			s.lmu.Lock()
			logs := s.logs.String()
			s.lmu.Unlock()
			if len(logs) > 4000 {
				logs = logs[len(logs)-4000:]
			}
			t.Fatalf("seed %d (%s, txns %d, cancelAt %d): %d problems:\n%s\nlogs:\n%s", seed, s.cfgS, nTxns, cancelAt,
				len(s.errs), strings.Join(s.errs, "\n"), logs)
		}
	}
	var ks []string
	for k := range agg {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	var b strings.Builder
	for _, k := range ks {
		fmt.Fprintf(&b, "%s=%d ", k, agg[k])
	}
	t.Logf("%d seeds: %s", n, b.String())
}
