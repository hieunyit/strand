package snapshot

// Mô phỏng tất định (F11) của `strand snapshot add` một bảng PG MỚI trong khi stream đang chạy:
//   - source có MVCC thật sự theo xid: Txn nhận xid lúc bắt đầu, commit record vào WAL (LSN) rồi MỚI
//     visible (ProcArrayEndTransaction) — đúng race đã biết; thứ tự visible giữa các Txn không xung đột
//     có thể khác thứ tự commit; khóa row/unique giữ tới lúc visible nên visibility theo PK là tiền tố
//     của thứ tự commit; snapshot (xmin, xmax, xip) là PGSnapshot thật;
//   - stream phát Txn theo thứ tự commit LSN ngay khi có commit record (Txn lớn bị server stream gửi
//     Stream Commit ngay sau commit record — trường hợp xấu nhất), cùng watermark không transactional;
//   - router theo đúng route()/keep() của pipeline: XIDTracker.Add trước, bảng ngoài pipeline bị bỏ,
//     beforeRegister/keepBeforeRegister (không plan → bỏ), range-state thật (HandleStreamEventWithPKSplitXID);
//   - snapshot là MySQLSnapshotter.SnapshotAdd THẬT với ChunkSource giả chép đúng PGChunkSource
//     (BeginReading → W → S0 → SELECT qua Chunker.ReadChunkAdaptive thật, có cắt theo byte và chia đôi
//     theo timeout → high → watermark), barrier giả (drain rồi chạy fn, router dừng), rendezvous theo
//     seenLSN như rendezvousPG;
//   - target apply relaxed: Txn đã Admit được ghi theo thứ tự bất kỳ miễn tôn trọng xung đột theo khóa
//     (như scheduler), last-writer-wins theo khóa.
// Mỗi lời gọi "DB" của snapshot (và vài điểm giữa các bước của nó) chạy một số bước ngẫu nhiên của thế
// giới (source, router, target), nên mọi xen kẽ ở mức lời gọi DB đều được khám phá theo seed.
// Kiểm tra: ver của mỗi khóa ở target không bao giờ giảm; cuối cùng target == source (cả bảng không
// snapshot, làm đối chứng).

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/sched"
)

const (
	f11Items    = 0 // bảng luôn thuộc pipeline (đối chứng)
	f11Events   = 1 // bảng được snapshot add
	f11Accounts = 2 // bảng làn (LaneTable): Txn chạm nó đi làn tuần tự
)

var (
	f11ItemsTbl    = model.TableID{Schema: "app", Table: "items"}
	f11EventsTbl   = model.TableID{Schema: "app", Table: "events"}
	f11AccountsTbl = model.TableID{Schema: "app", Table: "accounts"}
	f11Tbls        = []model.TableID{f11ItemsTbl, f11EventsTbl, f11AccountsTbl}
)

func f11TblOf(id model.TableID) int {
	for i, t := range f11Tbls {
		if t == id {
			return i
		}
	}
	panic(id)
}

type f11Key struct {
	tbl int
	k   int64
}

type f11Op struct {
	tbl    int
	op     model.Op
	key    int64
	oldKey int64 // ≠ 0: UPDATE đổi PK
	ver    int64
}

type f11Txn struct {
	xid    uint64
	ops    []f11Op
	locks  []f11Key
	lsn    uint64
	state  int // 0 đang chạy, 1 đã commit (chưa visible), 2 visible
	kind   string
	wm     bool // watermark (Txn rỗng của pg_logical_emit_message)
	routed bool
}

type f11Ver struct {
	xid uint64
	ver int64 // 0 = đã xóa
}

// f11Worker: một worker của target (relaxed) — batch đang ghi (chưa commit) hoặc rảnh.
type f11Worker struct {
	lane  bool
	batch []*model.Txn
}

type f11Sim struct {
	t   *testing.T
	r   *rand.Rand
	log []string

	// source
	nextXid uint64
	lsn     uint64
	ver     int64
	nextID  int64
	span    int64
	hist    map[f11Key][]f11Ver // lịch sử đã commit theo thứ tự commit
	head    map[f11Key]int64    // trạng thái sau mọi Txn đã commit (ver; vắng = không có row)
	locked  map[f11Key]*f11Txn
	running []*f11Txn // chưa visible (đang chạy hoặc đã commit)
	wal     []*f11Txn
	gen     bool
	bigN    int

	// router / pipeline
	cursor     int
	curTx      *f11Txn
	curRow     int
	curOut     *model.Txn
	seen       uint64
	tracker    *XIDTracker
	inPipeline bool
	done       bool
	barrierOn  bool
	sch        *sched.Scheduler
	workers    []*f11Worker
	admitted   uint64
	srcOf      map[uint64]*f11Txn

	// target
	dst     map[f11Key]int64
	lastVer map[f11Key]int64 // ver lớn nhất từng thấy ở target (như pgVerSampler)
	viol    []string

	// snapshot
	st       map[string]int // thống kê độ phủ
	name     string
	draining bool
	s        *MySQLSnapshotter
	plan     SnapshotPlan
	hasPlan  bool
	cfg      f11Cfg
}

type f11Cfg struct {
	world       int  // số bước thế giới tối đa mỗi điểm xen kẽ
	pVisible    int  // trọng số bước "visible"
	pCommit     int  // trọng số bước "commit"
	pBegin      int  // trọng số bước "bắt đầu Txn"
	pRoute      int  // trọng số bước router
	pApply      int  // trọng số bước target
	timeoutRate int  // 1/timeoutRate lần SELECT trả DeadlineExceeded (0 = không)
	rowSteps    bool // router trả quyền giữa các row của một Txn
	maxBytes    int64
}

func newF11Sim(t *testing.T, seed int64, cfg f11Cfg) *f11Sim {
	sim := &f11Sim{t: t, r: rand.New(rand.NewSource(seed)), nextXid: 100, lsn: 1000, ver: 1000, nextID: 1000, span: 40,
		hist: map[f11Key][]f11Ver{}, head: map[f11Key]int64{}, locked: map[f11Key]*f11Txn{},
		tracker: NewXIDTracker(), dst: map[f11Key]int64{}, lastVer: map[f11Key]int64{}, gen: true, bigN: 25, cfg: cfg,
		sch: sched.NewScheduler(sched.DefaultConfig()), srcOf: map[uint64]*f11Txn{}, st: map[string]int{}}
	for i := 0; i < 3; i++ {
		sim.workers = append(sim.workers, &f11Worker{})
	}
	sim.workers = append(sim.workers, &f11Worker{lane: true})
	// preload giống hệt ở source và target (target events cũ, không được duy trì trước snapshot add)
	for k := int64(1); k <= 30; k++ {
		for _, tb := range []int{f11Items, f11Events, f11Accounts} {
			sim.ver++
			key := f11Key{tb, k}
			sim.hist[key] = []f11Ver{{xid: 1, ver: sim.ver}}
			sim.head[key] = sim.ver
			sim.dst[key] = sim.ver
			sim.lastVer[key] = sim.ver
		}
	}
	// phần chỉ có ở source (như INSERT generate_series(3001, 20000) của e2e)
	for k := int64(31); k <= 60; k++ {
		sim.ver++
		key := f11Key{f11Events, k}
		sim.hist[key] = []f11Ver{{xid: 1, ver: sim.ver}}
		sim.head[key] = sim.ver
	}
	opts := ChunkOptions{DefaultChunkRows: 8, Timeout: 1 << 40, MinChunkRows: 1, MaxChunkBytes: cfg.maxBytes}
	s := NewMySQLSnapshotter(opts)
	s.Chunks = sim
	s.Barrier = sim.barrier
	s.LoadPlan = func(model.TableID) (SnapshotPlan, bool) { return sim.plan, sim.hasPlan }
	s.SavePlan = func(_ model.TableID, p SnapshotPlan) error { sim.plan, sim.hasPlan = p, true; return nil }
	s.ClearTarget = func(context.Context, model.TableID) error {
		sim.logf("ClearTarget")
		for k := range sim.dst {
			if k.tbl == f11Events {
				delete(sim.dst, k)
			}
		}
		return nil
	}
	s.FlushRows = func(_ context.Context, _ model.TableID, _ string, _ []string, rows []*model.RowChange, a, b int64) error {
		if !sim.barrierOn {
			return fmt.Errorf("flush outside barrier")
		}
		sim.logf("FlushRows [%d,%d] %d rows", a, b, len(rows))
		for k := range sim.dst {
			if k.tbl == f11Events && k.k >= a && k.k <= b {
				delete(sim.dst, k)
			}
		}
		for _, r := range rows {
			k := r.Key[0].(int64)
			if k < a || k > b {
				return fmt.Errorf("flush row %d outside [%d,%d]", k, a, b)
			}
			sim.put(f11Key{f11Events, k}, f11RowVer(r), "flush")
		}
		return nil
	}
	sim.s = s
	return sim
}

func (sim *f11Sim) logf(format string, a ...any) {
	sim.log = append(sim.log, fmt.Sprintf(format, a...))
}

func f11RowVer(r *model.RowChange) int64 {
	for _, c := range r.Cols {
		if c.Name == "ver" {
			return c.Value.(int64)
		}
	}
	return 0
}

func (sim *f11Sim) put(k f11Key, ver int64, why string) {
	if last, ok := sim.lastVer[k]; ok && ver < last {
		sim.viol = append(sim.viol, fmt.Sprintf("%v ver %d -> %d (%s)", k, last, ver, why))
	}
	if ver > sim.lastVer[k] {
		sim.lastVer[k] = ver
	}
	sim.dst[k] = ver
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

func (sim *f11Sim) free(k f11Key) bool {
	_, l := sim.locked[k]
	return !l
}

func (sim *f11Sim) existing(tbl int) []int64 {
	var out []int64
	for k := range sim.head {
		if k.tbl == tbl {
			out = append(out, k.k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (sim *f11Sim) hot() int64 {
	if sim.r.Intn(2) == 0 {
		return 1 + sim.r.Int63n(6)
	}
	return 1 + sim.r.Int63n(sim.span)
}

// begin bắt đầu một Txn mới (ops quyết định ngay, khóa giữ tới lúc visible).
func (sim *f11Sim) begin() {
	tx := &f11Txn{}
	lock := func(k f11Key) bool {
		if !sim.free(k) {
			return false
		}
		for _, l := range tx.locks {
			if l == k {
				return true
			}
		}
		tx.locks = append(tx.locks, k)
		return true
	}
	nv := func() int64 { sim.ver++; return sim.ver }
	switch c := sim.r.Intn(100); {
	case c < 5: // Txn lớn (server stream): INSERT … SELECT nhiều khóa mới
		tx.kind = "big"
		base := sim.nextID + 1
		sim.nextID += int64(sim.bigN) + 3
		for i := 0; i < sim.bigN; i++ {
			k := f11Key{f11Events, base + int64(i)}
			lock(k)
			tx.ops = append(tx.ops, f11Op{tbl: f11Events, op: model.OpInsert, key: k.k, ver: nv()})
		}
	case c < 25: // update item
		tx.kind = "item"
		k := f11Key{f11Items, sim.hot()}
		if _, ok := sim.head[k]; !ok || !lock(k) {
			return
		}
		tx.ops = append(tx.ops, f11Op{tbl: f11Items, op: model.OpUpdate, key: k.k, ver: nv()})
	case c < 40: // upsert events (khóa nóng)
		tx.kind = "upsert"
		k := f11Key{f11Events, sim.hot()}
		if !lock(k) {
			return
		}
		op := model.OpUpdate
		if _, ok := sim.head[k]; !ok {
			op = model.OpInsert
		}
		tx.ops = append(tx.ops, f11Op{tbl: f11Events, op: op, key: k.k, ver: nv()})
	case c < 60: // đổi PK events (mover: khóa đích trong [1, 2·span], hoặc khóa mới)
		tx.kind = "move"
		from := f11Key{f11Events, 1 + sim.r.Int63n(2*sim.span)}
		var to f11Key
		if sim.r.Intn(3) == 0 {
			sim.nextID++
			to = f11Key{f11Events, sim.nextID}
		} else {
			to = f11Key{f11Events, 1 + sim.r.Int63n(2*sim.span)}
		}
		if from == to {
			return
		}
		if _, ok := sim.head[from]; !ok {
			return
		}
		if _, ok := sim.head[to]; ok {
			return // unique violation ở source
		}
		if !lock(from) || !lock(to) {
			return
		}
		tx.ops = append(tx.ops, f11Op{tbl: f11Events, op: model.OpUpdate, key: to.k, oldKey: from.k, ver: nv()})
	case c < 68: // xóa events
		tx.kind = "delete"
		k := f11Key{f11Events, 1 + sim.r.Int63n(2*sim.span)}
		if _, ok := sim.head[k]; !ok || !lock(k) {
			return
		}
		tx.ops = append(tx.ops, f11Op{tbl: f11Events, op: model.OpDelete, key: k.k})
	case c < 76: // account (bảng làn)
		tx.kind = "account"
		k := f11Key{f11Accounts, sim.hot()}
		if _, ok := sim.head[k]; !ok || !lock(k) {
			return
		}
		tx.ops = append(tx.ops, f11Op{tbl: f11Accounts, op: model.OpUpdate, key: k.k, ver: nv()})
	default: // Txn nhiều bảng: item + vài events mới (+ account: làn) như nhánh default / two-phase của e2e
		tx.kind = "multi"
		id := sim.hot()
		k := f11Key{f11Items, id}
		if _, ok := sim.head[k]; !ok || !lock(k) {
			return
		}
		tx.ops = append(tx.ops, f11Op{tbl: f11Items, op: model.OpUpdate, key: k.k, ver: nv()})
		for j := 0; j < 1+sim.r.Intn(3); j++ {
			sim.nextID++
			e := f11Key{f11Events, sim.nextID}
			lock(e)
			tx.ops = append(tx.ops, f11Op{tbl: f11Events, op: model.OpInsert, key: e.k, ver: nv()})
		}
		if sim.r.Intn(3) > 0 {
			a := f11Key{f11Accounts, id}
			if _, ok := sim.head[a]; ok && lock(a) {
				tx.ops = append(tx.ops, f11Op{tbl: f11Accounts, op: model.OpUpdate, key: a.k, ver: nv()})
			}
		}
	}
	if len(tx.ops) == 0 {
		return
	}
	sim.nextXid++
	tx.xid = sim.nextXid
	for _, l := range tx.locks {
		sim.locked[l] = tx
	}
	// head đổi ngay (chỉ Txn này giữ các khóa này tới khi visible)
	sim.running = append(sim.running, tx)
}

func (sim *f11Sim) commit(tx *f11Txn) {
	sim.lsn++
	tx.lsn, tx.state = sim.lsn, 1
	for _, op := range tx.ops {
		if op.oldKey != 0 {
			ok := f11Key{op.tbl, op.oldKey}
			sim.hist[ok] = append(sim.hist[ok], f11Ver{xid: tx.xid})
			delete(sim.head, ok)
		}
		k := f11Key{op.tbl, op.key}
		if op.op == model.OpDelete {
			sim.hist[k] = append(sim.hist[k], f11Ver{xid: tx.xid})
			delete(sim.head, k)
		} else {
			sim.hist[k] = append(sim.hist[k], f11Ver{xid: tx.xid, ver: op.ver})
			sim.head[k] = op.ver
		}
	}
	sim.wal = append(sim.wal, tx)
}

func (sim *f11Sim) visible(tx *f11Txn) {
	tx.state = 2
	for _, l := range tx.locks {
		delete(sim.locked, l)
	}
	for i, x := range sim.running {
		if x == tx {
			sim.running = append(sim.running[:i], sim.running[i+1:]...)
			break
		}
	}
}

// snap: pg_current_snapshot() lúc này.
func (sim *f11Sim) snap() *PGSnapshot {
	s := &PGSnapshot{Xmax: sim.nextXid + 1, Xip: map[uint64]struct{}{}}
	s.Xmin = s.Xmax
	for _, tx := range sim.running {
		s.Xip[tx.xid] = struct{}{}
		if tx.xid < s.Xmin {
			s.Xmin = tx.xid
		}
	}
	return s
}

// readAt: ver của khóa với snapshot s (0 = không có row).
func (sim *f11Sim) readAt(s *PGSnapshot, k f11Key) int64 {
	var v int64
	for _, h := range sim.hist[k] {
		if h.xid == 1 || s.Visible(h.xid) {
			v = h.ver
		}
	}
	return v
}

func (sim *f11Sim) visibleKeys(s *PGSnapshot, tbl int) []int64 {
	var out []int64
	for k := range sim.hist {
		if k.tbl == tbl && sim.readAt(s, k) != 0 {
			out = append(out, k.k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ---------------------------------------------------------------------------
// Router (route/keep của pipeline) và target
// ---------------------------------------------------------------------------

func f11Image(op f11Op) *model.RowChange {
	tbl := f11Tbls[op.tbl]
	rc := &model.RowChange{Table: tbl, Op: op.op, Key: []model.Value{op.key}}
	if op.op != model.OpDelete {
		rc.Cols = []model.ColValue{{Name: "id", Value: op.key}, {Name: "ver", Value: op.ver}}
	}
	if op.oldKey != 0 {
		rc.OldKey = []model.Value{op.oldKey}
	}
	return rc
}

// keepBeforeRegister: chép đúng nhánh không-TOAST của Pipeline.keepBeforeRegister (engine.go).
func (sim *f11Sim) keepBeforeRegister(rc *model.RowChange) []*model.RowChange {
	if !sim.hasPlan {
		return nil
	}
	plan := sim.plan
	if plan.Empty {
		return []*model.RowChange{rc}
	}
	var out []*model.RowChange
	for _, part := range SplitStreamEvent(rc) {
		pk, _ := toInt64(part.Key[0])
		if plan.Pending(pk) {
			continue
		}
		out = append(out, part)
	}
	if len(rc.OldKey) > 0 && len(out) == 2 {
		return []*model.RowChange{rc}
	}
	return out
}

// routeOne: router xử lý tiếp (một phần) Txn kế tiếp của stream. route() giữ routeMu suốt Txn, nhưng
// thao tác range-state của snapshot ngoài barrier (BeginReading, StartReadingPG, CarveChunk,
// SplitReading, FinishReading, SetDone) vẫn xen được GIỮA các row của một Txn: router xử lý một số
// row rồi trả quyền (curOut giữ phần đã xử lý).
func (sim *f11Sim) routeOne() bool {
	if sim.barrierOn {
		return false
	}
	if sim.curTx != nil {
		sim.routeRows(sim.curTx)
		return true
	}
	if sim.cursor >= len(sim.wal) {
		return false
	}
	tx := sim.wal[sim.cursor]
	sim.cursor++
	tx.routed = true
	sim.tracker.Add(tx.xid)
	if !tx.wm && tx.state == 1 {
		sim.st["routed before visible"]++
		if tx.kind == "big" {
			sim.st["big routed before visible"]++
		}
	}
	if tx.wm {
		if tx.lsn > sim.seen {
			sim.seen = tx.lsn
		}
		return true
	}
	sim.curTx, sim.curRow = tx, 0
	sim.curOut = &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: tx.lsn}}
	sim.routeRows(tx)
	return true
}

// finishRoute: chạy nốt Txn đang route (lấy routeMu: barrier, p.tables[id] = m, RemoveTable).
func (sim *f11Sim) finishRoute() {
	for sim.curTx != nil {
		sim.routeRows(sim.curTx)
	}
}

func (sim *f11Sim) routeRows(tx *f11Txn) {
	m := sim.s.Manager
	out := sim.curOut
	pos := out.SourcePos
	n := len(tx.ops) - sim.curRow
	if sim.cfg.rowSteps && n > 1 {
		n = 1 + sim.r.Intn(n)
	}
	for _, op := range tx.ops[sim.curRow : sim.curRow+n] {
		sim.curRow++
		if sim.curRow > 1 && sim.curRow < len(tx.ops) && n < len(tx.ops) {
			sim.st["row routed mid-txn"]++
		}
		rc := f11Image(op)
		parts := []*model.RowChange{rc}
		if op.tbl == f11Events {
			switch {
			case !sim.inPipeline:
				parts = nil
			case !m.IsRegistered(f11EventsTbl) && !sim.done:
				parts = sim.keepBeforeRegister(rc)
				sim.st["dropped before register"]++
				if tx.state == 1 {
					sim.st["dropped before register, not visible"]++
				}
			case m.IsRegistered(f11EventsTbl):
				parts = parts[:0]
				for _, r := range m.HandleStreamEventWithPKSplitXID(rc, pos, tx.xid) {
					st := "?"
					if r.Chunk != nil {
						st = r.Chunk.State.String()
					}
					sim.st[fmt.Sprintf("action %d state %s", r.Action, st)]++
					switch r.Action {
					case ActionApply:
						parts = append(parts, r.Event)
					case ActionReject:
						sim.t.Fatalf("reject %v", r.Event.Key)
					}
				}
			}
		}
		if op.tbl == f11Accounts && len(parts) > 0 {
			out.Lane = true
		}
		for _, p := range parts {
			out.Rows = append(out.Rows, *p)
		}
	}
	if sim.curRow < len(tx.ops) {
		return
	}
	sim.curTx, sim.curOut = nil, nil
	// Txn rỗng sau khi lọc vẫn đi qua scheduler (như route()).
	if out.Lane && !(func() bool {
		for _, op := range tx.ops {
			if op.tbl == f11Accounts {
				return true
			}
		}
		return false
	})() {
		sim.st["lane?"]++
	}
	wasLane := out.Lane
	if err := sim.sch.Admit(context.Background(), out); err != nil {
		sim.t.Fatal(err)
	}
	if out.Lane && !wasLane {
		sim.st["lane contagion"]++
	}
	sim.admitted = out.Seq()
	sim.srcOf[out.Seq()] = tx
	if tx.lsn > sim.seen {
		sim.seen = tx.lsn
	}
}

// take: worker rảnh lấy một batch (worker thường: gatherBatch từ readyCh; worker làn: NextLaneBatch).
func (sim *f11Sim) take(w *f11Worker) bool {
	if w.batch != nil {
		return false
	}
	if w.lane {
		if sim.sch.LanePending() == 0 {
			return false
		}
		// Đầu làn sẵn sàng thì NextLaneBatch trả ngay; không thì chờ hết ctx (đơn luồng: không ai đổi được
		// trạng thái). Máy quá tải có thể làm ctx hết hạn trước lần kiểm tra đầu (nil giả): lúc drain thử
		// lại với hạn dài để không báo kẹt nhầm.
		wait := 5 * time.Millisecond
		if sim.draining {
			wait = 2 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), wait)
		defer cancel()
		w.batch = sim.sch.NextLaneBatch(ctx, 1+sim.r.Intn(4), 0)
		return w.batch != nil
	}
	n := 1 + sim.r.Intn(4)
	for len(w.batch) < n {
		select {
		case t := <-sim.sch.ReadyCh():
			w.batch = append(w.batch, t)
			continue
		default:
		}
		break
	}
	return w.batch != nil
}

// commitBatch: batch của worker commit ở target (row theo thứ tự Txn, thứ tự row), rồi Done.
func (sim *f11Sim) commitBatch(w *f11Worker) {
	for _, t := range w.batch {
		src := sim.srcOf[t.Seq()]
		for i := range t.Rows {
			rc := &t.Rows[i]
			tb := f11TblOf(rc.Table)
			if len(rc.OldKey) > 0 {
				delete(sim.dst, f11Key{tb, rc.OldKey[0].(int64)})
			}
			k := f11Key{tb, rc.Key[0].(int64)}
			if rc.Op == model.OpDelete {
				delete(sim.dst, k)
				continue
			}
			sim.put(k, f11RowVer(rc), fmt.Sprintf("apply xid %d seq %d lane %v", src.xid, t.Seq(), w.lane))
		}
	}
	for _, t := range w.batch {
		sim.sch.Done(t.Seq(), nil)
	}
	w.batch = nil
}

func (sim *f11Sim) applyOne(pick bool) bool {
	ws := sim.workers
	if pick {
		w := ws[sim.r.Intn(len(ws))]
		if w.batch != nil {
			sim.commitBatch(w)
			return true
		}
		return sim.take(w)
	}
	for _, w := range ws {
		if w.batch != nil {
			sim.commitBatch(w)
			return true
		}
	}
	for _, w := range ws {
		if sim.take(w) {
			return true
		}
	}
	return false
}

// dumpSched in trạng thái scheduler/worker khi kẹt.
func (sim *f11Sim) dumpSched() {
	sim.t.Logf("lanePending=%d readyCh=%d", sim.sch.LanePending(), len(sim.sch.ReadyCh()))
	for i, w := range sim.workers {
		var seqs []uint64
		for _, t := range w.batch {
			seqs = append(seqs, t.Seq())
		}
		sim.t.Logf("worker %d lane=%v batch=%v", i, w.lane, seqs)
	}
	for seq := sim.sch.Lwd() + 1; seq <= sim.admitted; seq++ {
		tx := sim.srcOf[seq]
		sim.t.Logf("seq %d xid %d kind %s lsn %d", seq, tx.xid, tx.kind, tx.lsn)
	}
}

func (sim *f11Sim) drain() {
	for guard := 0; sim.sch.Lwd() < sim.admitted; guard++ {
		if sim.applyOne(false) {
			continue
		}
		sim.draining = true
		ok := sim.applyOne(false)
		sim.draining = false
		if !ok || guard > 1000000 {
			sim.dumpSched()
			sim.t.Fatalf("%s: drain stuck: lwd %d admitted %d", sim.name, sim.sch.Lwd(), sim.admitted)
		}
	}
}

// step: một bước ngẫu nhiên của thế giới.
func (sim *f11Sim) step() {
	c := sim.cfg
	total := c.pBegin + c.pCommit + c.pVisible + c.pRoute + c.pApply
	x := sim.r.Intn(total)
	switch {
	case x < c.pBegin:
		if sim.gen && len(sim.running) < 6 {
			sim.begin()
		}
	case x < c.pBegin+c.pCommit:
		var cand []*f11Txn
		for _, tx := range sim.running {
			if tx.state == 0 {
				cand = append(cand, tx)
			}
		}
		if len(cand) > 0 {
			sim.commit(cand[sim.r.Intn(len(cand))])
		}
	case x < c.pBegin+c.pCommit+c.pVisible:
		var cand []*f11Txn
		for _, tx := range sim.running {
			if tx.state == 1 {
				cand = append(cand, tx)
			}
		}
		if len(cand) > 0 {
			sim.visible(cand[sim.r.Intn(len(cand))])
		}
	case x < c.pBegin+c.pCommit+c.pVisible+c.pRoute:
		sim.routeOne()
	default:
		sim.applyOne(true) // worker vẫn chạy trong barrier (drain đã xong, không còn gì mới)
	}
}

func (sim *f11Sim) world() {
	for n := sim.r.Intn(sim.cfg.world + 1); n > 0; n-- {
		sim.step()
	}
}

// ---------------------------------------------------------------------------
// Pipeline: barrier, rendezvous; ChunkSource giả (chép PGChunkSource)
// ---------------------------------------------------------------------------

func (sim *f11Sim) barrier(_ context.Context, fn func() error) error {
	sim.world()
	sim.finishRoute()
	sim.barrierOn = true // routeMu: router dừng
	sim.drain()
	err := fn()
	sim.barrierOn = false
	return err
}

func (sim *f11Sim) rendezvous(_ context.Context, high checkpoint.AppliedSet) error {
	h := high.(*checkpoint.PGAppliedSet).Lwd().LSN
	for guard := 0; sim.seen < h; guard++ {
		if guard > 100000 {
			return fmt.Errorf("rendezvous stuck")
		}
		sim.step()
	}
	return nil
}

func (sim *f11Sim) waitSnapshot(xids []uint32) (*PGSnapshot, error) {
	for guard := 0; ; guard++ {
		sim.world()
		s := sim.snap()
		sim.tracker.Prune(s)
		if AllVisible(s, xids) {
			return s, nil
		}
		if guard > 100000 {
			return nil, fmt.Errorf("waitSnapshot stuck")
		}
		sim.step()
	}
}

func (sim *f11Sim) BeforeRegister(context.Context) error {
	xids := sim.tracker.Pending()
	s, err := sim.waitSnapshot(xids)
	if err == nil {
		sim.logf("BeforeRegister snapshot %s (waited %v)", s, xids)
	}
	return err
}

func (sim *f11Sim) Bounds(_ context.Context, _ model.TableID, _ string) (*TableBounds, error) {
	sim.world()
	s := sim.snap()
	ks := sim.visibleKeys(s, f11Events)
	var b TableBounds
	if len(ks) > 0 {
		b.MinPK, b.MaxPK, b.Count = ks[0], ks[len(ks)-1], SpanRows(ks[0], ks[len(ks)-1])
	}
	sim.logf("Bounds at %s: [%d,%d]", s, b.MinPK, b.MaxPK)
	return &b, nil
}

func (sim *f11Sim) KeysetEnd(_ context.Context, _ model.TableID, _ string, start, max, rows int64) (int64, error) {
	sim.world()
	s := sim.snap()
	var in []int64
	for _, k := range sim.visibleKeys(s, f11Events) {
		if k >= start && k <= max {
			in = append(in, k)
		}
	}
	if int64(len(in)) < rows {
		return max, nil
	}
	return in[rows-1], nil
}

func (sim *f11Sim) ReadChunk(ctx context.Context, s *MySQLSnapshotter, chunk *Chunk, columnNames []string, pkIdxs []int,
	table model.TableID) (ChunkRead, checkpoint.AppliedSet, error) {
	sim.world()
	if !chunk.Continuation {
		s.Manager.BeginReading(chunk)
		sim.world()
		xids := sim.tracker.Pending()
		low, err := sim.waitSnapshot(xids)
		if err != nil {
			return ChunkRead{}, nil, err
		}
		s.Manager.StartReadingPG(chunk, low)
		sim.logf("chunk [%d,%d] S0=%s W=%v", chunk.StartPK, chunk.EndPK, low, xids)
	}
	queryFn := func(qctx context.Context, ch *Chunk, maxBytes int64) ([]*model.RowChange, int64, error) {
		sim.world()
		if _, ok := qctx.Deadline(); ok && sim.cfg.timeoutRate > 0 && sim.r.Intn(sim.cfg.timeoutRate) == 0 {
			sim.st["select timeout split"]++
			return nil, 0, context.DeadlineExceeded
		}
		snap := sim.snap()
		var out []*model.RowChange
		var total int64
		for _, k := range sim.visibleKeys(snap, f11Events) {
			if k < ch.StartPK || k > ch.EndPK {
				continue
			}
			v := sim.readAt(snap, f11Key{f11Events, k})
			out = append(out, &model.RowChange{Table: table, Op: model.OpInsert, Key: []model.Value{k},
				Cols: []model.ColValue{{Name: "id", Value: k}, {Name: "ver", Value: v}}})
			total += 10
			if maxBytes > 0 && total >= maxBytes {
				break
			}
		}
		sim.logf("SELECT [%d,%d] at %s: %d rows", ch.StartPK, ch.EndPK, snap, len(out))
		sim.world()
		return out, total, nil
	}
	read, err := s.Chunker.ReadChunkAdaptive(ctx, chunk, s.opts, queryFn)
	if err != nil {
		return ChunkRead{}, nil, err
	}
	if read.Truncated {
		sim.st["truncated read"]++
	}
	if chunk.Continuation {
		sim.st["continuation chunk"]++
	}
	sim.st["chunks"]++
	sim.world()
	high := sim.lsn + 1 // pg_current_wal_insert_lsn()
	sim.world()
	sim.lsn++ // pg_logical_emit_message: record bắt đầu ở LSN ≥ high
	sim.wal = append(sim.wal, &f11Txn{wm: true, lsn: sim.lsn, state: 2})
	return read, checkpoint.NewPGAppliedSet(high), nil
}

// ---------------------------------------------------------------------------
// Kịch bản một lần chạy
// ---------------------------------------------------------------------------

func (sim *f11Sim) run() error {
	ctx := context.Background()
	for i := 0; i < 30+sim.r.Intn(60); i++ {
		sim.step()
	}
	// SnapshotAdd của pipeline: SetDone(false) → p.tables[id] = m → snapshotTable (goroutine).
	sim.world()
	sim.finishRoute()
	sim.inPipeline = true
	sim.logf("tables[events] = m")
	sim.world()
	// snapshotTable: WaitGate (ngoài barrier) — Prune của XIDTracker chạy xen với router.Add.
	if _, err := sim.waitSnapshot(nil); err != nil {
		return err
	}
	sim.world()
	err := sim.s.SnapshotAdd(ctx, nil, nil, f11EventsTbl, "id", []string{"id", "ver"}, sim.rendezvous)
	if err != nil {
		return err
	}
	sim.done = true // SetDone(true)
	sim.world()
	sim.finishRoute()
	sim.s.Manager.RemoveTable(f11EventsTbl)
	sim.logf("RemoveTable")
	for i := 0; i < 20; i++ {
		sim.step()
	}
	// dừng tải, chờ mọi thứ visible, stream hết, target hết.
	sim.gen = false
	for len(sim.running) > 0 {
		tx := sim.running[0]
		if tx.state == 0 {
			sim.commit(tx)
		}
		sim.visible(tx)
	}
	for sim.routeOne() {
	}
	sim.drain()
	return nil
}

func (sim *f11Sim) check() []string {
	var bad []string
	bad = append(bad, sim.viol...)
	keys := map[f11Key]bool{}
	for k := range sim.head {
		keys[k] = true
	}
	for k := range sim.dst {
		keys[k] = true
	}
	var ks []f11Key
	for k := range keys {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].tbl != ks[j].tbl {
			return ks[i].tbl < ks[j].tbl
		}
		return ks[i].k < ks[j].k
	})
	for _, k := range ks {
		s, okS := sim.head[k]
		d, okD := sim.dst[k]
		if okS != okD || s != d {
			who := ""
			for _, h := range sim.hist[k] {
				who += fmt.Sprintf(" x%d:v%d", h.xid, h.ver)
			}
			bad = append(bad, fmt.Sprintf("%v source %d(%v) target %d(%v) hist:%s", k, s, okS, d, okD, who))
		}
	}
	return bad
}

func f11Seeds(def int) int {
	if v, err := strconv.Atoi(os.Getenv("F11_SEEDS")); err == nil && v > 0 {
		return v
	}
	return def
}

func f11Configs() []f11Cfg {
	return []f11Cfg{
		{world: 3, pBegin: 4, pCommit: 3, pVisible: 3, pRoute: 4, pApply: 3},
		{world: 4, pBegin: 4, pCommit: 4, pVisible: 2, pRoute: 5, pApply: 3, rowSteps: true, maxBytes: 40, timeoutRate: 5},
		{world: 8, pBegin: 3, pCommit: 4, pVisible: 1, pRoute: 6, pApply: 2, timeoutRate: 4, maxBytes: 50},
		{world: 1, pBegin: 5, pCommit: 5, pVisible: 1, pRoute: 8, pApply: 5, maxBytes: 30},
		{world: 12, pBegin: 6, pCommit: 2, pVisible: 2, pRoute: 2, pApply: 1, timeoutRate: 3},
		{world: 2, pBegin: 3, pCommit: 3, pVisible: 1, pRoute: 3, pApply: 2, rowSteps: true},
	}
}

func TestF11Sim_SnapshotAddWhileStreaming(t *testing.T) {
	n := f11Seeds(150) // F11_SEEDS=3000 để khám phá kỹ (≈ 3–4 phút)
	fails := 0
	total := map[string]int{}
	defer func() {
		var ks []string
		for k := range total {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			t.Logf("%-45s %d", k, total[k])
		}
	}()
	for ci, cfg := range f11Configs() {
		for seed := int64(1); seed <= int64(n); seed++ {
			sim := newF11Sim(t, seed*7919+int64(ci), cfg)
			sim.name = fmt.Sprintf("cfg %d seed %d", ci, seed)
			if err := sim.run(); err != nil {
				t.Fatalf("cfg %d seed %d: %v", ci, seed, err)
			}
			for k, v := range sim.st {
				total[k] += v
			}
			if bad := sim.check(); len(bad) > 0 {
				fails++
				if fails <= 3 {
					t.Errorf("cfg %d seed %d: %d problems, first: %v", ci, seed, len(bad), bad[:min(len(bad), 5)])
					if os.Getenv("F11_LOG") != "" {
						for _, l := range sim.log {
							t.Log(l)
						}
					}
				}
			}
		}
	}
	if fails > 0 {
		t.Fatalf("%d failing runs", fails)
	}
}
