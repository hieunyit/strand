package pipeline

// Mô phỏng tất định theo seed của chế độ vị trí binlog (source MySQL không GTID) với kill -9 ở mọi bước.
//
// THẬT: Pipeline.router/route (kiểm vị trí, bỏ Txn đã apply theo applied0, noteRouted), sched.Scheduler,
// Pipeline.worker/gatherBatch/applyRelaxed/finishBatch/deltaFor, strictDispatcher + strictGlobal,
// lsnTracker.pointPos + vouchGlobal, checkpoint.Compactor (đọc delta, MergeChecked, vouch, ghi toàn cục,
// prune; hook bước để chết giữa chừng) qua driver database/sql giả, MySQLPosAppliedSet và JSON của nó
// (mọi lần nạp lại đi qua ParseAppliedSet).
// GIẢ: source (nhật ký binlog sinh sẵn: nhiều file, rotate, khoảng trống event không thuộc Txn, Txn
// rỗng), reader (phát Txn có start ≥ vị trí resume, nhịp ngẫu nhiên), target (kv + checkpoint, ghi dữ
// liệu và row delta/toàn cục trong MỘT bước nguyên tử như transaction thật), "kill -9" (hủy ctx và đóng
// mọi commit sau đó).
//
// Bất biến kiểm (mỗi lần commit và mỗi lần restart):
//   CP-1  mọi Txn đã commit nằm trong tập nạp lại; không Txn nào nằm trong tập mà chưa commit (kể cả
//         mốc do Vouch dời);
//   CP-2  không Txn nào commit hai lần qua mọi lần restart;
//   CP-3  vị trí resume (Lwd) là ranh giới event và mọi Txn chưa apply bắt đầu ≥ Lwd (không mất);
//   CP-4  |holes| + |extra| và độ dài JSON (≤ 64 KB) bị chặn;
//   CP-5  (strict) các Txn commit tạo thành tiền tố của thứ tự binlog;
//   cuối: target == trạng thái mong đợi; ver của khóa không giảm.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"strand/internal/checkpoint"
	"strand/internal/config"
	"strand/internal/metrics"
	"strand/internal/model"
	"strand/internal/sched"
	"strand/internal/snapshot"
	"strand/internal/writer"
)

var pItems = model.TableID{Schema: "app", Table: "items"}

type pTxn struct {
	id               int
	file             string
	seq              uint32
	startOff, endOff uint64
	start, end       uint64 // khóa
	keys             []int64
}

type pDelta struct {
	applied string
	batchNo int64
}

type pSim struct {
	t      *testing.T
	seed   int64
	mu     sync.Mutex // rng + errs
	rng    *rand.Rand
	errs   []string
	mode   string // relaxed | strict | mixed
	strict bool   // chế độ của chu kỳ hiện tại

	log        []pTxn
	byEnd      map[uint64]int
	boundaries map[uint64]bool
	preN       int // Txn [0, preN) đã nằm sẵn ở target (--start-at)
	startKey   uint64

	// target
	tmu        sync.Mutex
	gen        int
	kv         map[int64]int
	lastVer    map[int64]int
	committed  []int
	global     string
	deltas     map[int64]*pDelta
	nextStrict int
	maxSetLen  int
	maxSetN    int
	commits    int
	globals    int

	dir string
}

func (s *pSim) intn(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Intn(n)
}

func (s *pSim) errf(f string, a ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.errs) < 8 {
		s.errs = append(s.errs, fmt.Sprintf(f, a...))
	}
}

func (s *pSim) nErrs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.errs)
}

func newPSim(t *testing.T, seed int64, mode string, nTxns int) *pSim {
	s := &pSim{t: t, seed: seed, rng: rand.New(rand.NewSource(seed)), mode: mode, byEnd: map[uint64]int{}, boundaries: map[uint64]bool{},
		kv: map[int64]int{}, lastVer: map[int64]int{}, deltas: map[int64]*pDelta{}, dir: t.TempDir()}
	// Nhật ký binlog: nhiều file, rotate, khoảng trống event không thuộc Txn, offset file trước có thể lớn hơn file sau.
	seq := uint32(1 + s.rng.Intn(5))
	off := uint64(4)
	first, _ := checkpoint.BinlogKey(seq, 4)
	s.boundaries[first] = true
	filePer := 3 + s.rng.Intn(40)
	inFile := 0
	for i := 0; i < nTxns; i++ {
		if inFile >= filePer {
			seq += uint32(1 + s.rng.Intn(2)*s.rng.Intn(2)) // thỉnh thoảng nhảy số
			off = 4 + uint64(s.rng.Intn(3))*60             // sau format_desc / previous_gtids
			inFile = 0
			filePer = 3 + s.rng.Intn(40)
			k, _ := checkpoint.BinlogKey(seq, off)
			s.boundaries[k] = true
		}
		if s.rng.Intn(6) == 0 {
			off += uint64(20 + s.rng.Intn(60)) // event không thuộc Txn (anonymous gtid bị bỏ, heartbeat...)
		}
		start := off
		size := uint64(60 + s.rng.Intn(3000))
		end := start + size
		tx := pTxn{id: i, file: checkpoint.FormatBinlogName("mysql-bin", seq), seq: seq, startOff: start, endOff: end}
		tx.start, _ = checkpoint.BinlogKey(seq, start)
		tx.end, _ = checkpoint.BinlogKey(seq, end)
		for k := 0; k < s.rng.Intn(4); k++ { // 0 key = Txn rỗng sau lọc (vẫn đi qua scheduler)
			tx.keys = append(tx.keys, int64(s.rng.Intn(12)))
		}
		// bỏ khóa trùng trong cùng Txn
		seen := map[int64]bool{}
		var ks []int64
		for _, k := range tx.keys {
			if !seen[k] {
				seen[k] = true
				ks = append(ks, k)
			}
		}
		tx.keys = ks
		s.log = append(s.log, tx)
		s.byEnd[tx.end] = i
		s.boundaries[tx.start], s.boundaries[tx.end] = true, true
		off = end
		inFile++
	}
	s.committed = make([]int, nTxns)
	// --start-at: dữ liệu target khớp source tại kết thúc Txn preN-1 (hoặc đầu file đầu tiên).
	s.preN = s.rng.Intn(nTxns/3 + 1)
	if s.preN == 0 {
		s.startKey = first
	} else {
		s.startKey = s.log[s.preN-1].end
		for i := 0; i < s.preN; i++ {
			s.committed[i] = 1
			for _, k := range s.log[i].keys {
				s.kv[k], s.lastVer[k] = i+1, i+1
			}
		}
	}
	s.nextStrict = s.preN
	set := checkpoint.NewMySQLPosAppliedSet("mysql-bin", s.startKey)
	set.SetSource("server_id=1")
	s.global = set.String()
	return s
}

// ---------------------------------------------------------------------------
// Target + writer giả
// ---------------------------------------------------------------------------

func (s *pSim) loadSet(global string, deltas []string) (checkpoint.AppliedSet, error) {
	g, err := checkpoint.ParseAppliedSet(model.EngineMySQL, global)
	if err != nil {
		return nil, fmt.Errorf("parse global %q: %w", global, err)
	}
	for _, d := range deltas {
		w, err := checkpoint.ParseAppliedSet(model.EngineMySQL, d)
		if err != nil {
			return nil, fmt.Errorf("parse delta %q: %w", d, err)
		}
		if g, err = checkpoint.MergeChecked(g, w); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// checkSetLocked: CP-1 (không phantom) và CP-4 cho một chuỗi applied vừa được ghi cùng commit. Gọi khi giữ tmu.
func (s *pSim) checkSetLocked(what, str string, extraCommitted map[int]bool) {
	if str == "" {
		return
	}
	set, err := checkpoint.ParseAppliedSet(model.EngineMySQL, str)
	if err != nil {
		s.errf("%s: unparsable checkpoint %q: %v", what, str, err)
		return
	}
	if len(str) > checkpoint.MaxDeltaBytes {
		s.errf("%s: checkpoint is %d bytes (> 64 KB)", what, len(str))
	}
	if len(str) > s.maxSetLen {
		s.maxSetLen = len(str)
	}
	if ps, ok := set.(*checkpoint.MySQLPosAppliedSet); ok {
		if n := ps.NumHoles() + ps.NumExtra(); n > s.maxSetN {
			s.maxSetN = n
		}
		if ps.NumHoles() > 4096 {
			s.errf("%s: %d holes", what, ps.NumHoles())
		}
	}
	for i := range s.log {
		if set.Contains(checkpoint.PosOfKey("mysql-bin", s.log[i].end)) && s.committed[i] == 0 && !extraCommitted[i] {
			s.errf("%s: checkpoint contains txn %d (%s) that was never committed (CP-1/data loss)\n  checkpoint=%s", what, i, checkpoint.FormatBinlogKey(s.log[i].end), str)
			return
		}
	}
}

type pWriter struct {
	s   *pSim
	id  int64
	gen int
	// crashAfterCommit: xác suất (1/n) chết NGAY SAU commit, trước khi báo thành công (kill -9 giữa commit và ack).
	crashAfter  int
	crashBefore int
	kill        func()
}

func (w *pWriter) SetPrimaryKey(model.TableID, []string)  {}
func (w *pWriter) SetCommitOrderer(*writer.CommitOrderer) {}
func (w *pWriter) EnsureWorkerRow(context.Context) error  { return nil }

func (w *pWriter) ApplyBatch(ctx context.Context, batch []*model.Txn, delta writer.DeltaRow) error {
	s := w.s
	if w.crashBefore > 0 && s.intn(w.crashBefore) == 0 {
		w.kill()
		return errors.New("killed before commit")
	}
	s.tmu.Lock()
	defer s.tmu.Unlock()
	if s.gen != w.gen || ctx.Err() != nil {
		return errors.New("process is dead: commit refused")
	}
	// Chuỗi checkpoint của commit này (tính TRƯỚC khi ghi dữ liệu, như writer thật gọi Global sau lượt commit).
	var gstr string
	if delta.Global != nil {
		g, err := delta.Global()
		if err != nil {
			return err
		}
		gstr = g
	}
	inBatch := map[int]bool{}
	for _, t := range batch {
		id, ok := s.byEnd[mustKey(t.SourcePos)]
		if !ok {
			s.errf("unknown txn position %v", t.SourcePos)
			continue
		}
		inBatch[id] = true
	}
	// Dữ liệu
	for _, t := range batch {
		id := s.byEnd[mustKey(t.SourcePos)]
		if s.strict && s.mode == "strict" {
			if id != s.nextStrict {
				s.errf("CP-5: strict commit of txn %d but the next txn in binlog order is %d", id, s.nextStrict)
			}
			s.nextStrict = id + 1
		}
		s.committed[id]++
		if s.committed[id] > 1 {
			s.errf("CP-2: txn %d (%s) committed %d times", id, checkpoint.FormatBinlogKey(s.log[id].end), s.committed[id])
		}
		for _, rc := range t.Rows {
			k := rc.Key[0].(int64)
			ver := 0
			for _, c := range rc.Cols {
				if c.Name == "ver" {
					ver = int(c.Value.(int64))
				}
			}
			if ver < s.lastVer[k] {
				s.errf("key %d went back: ver %d -> %d (txn %d)", k, s.lastVer[k], ver, id)
			}
			s.kv[k], s.lastVer[k] = ver, ver
		}
	}
	// Checkpoint cùng "transaction"
	switch {
	case delta.Global != nil:
		s.checkSetLocked("strict global", gstr, nil)
		s.global = gstr
	default:
		s.checkSetLocked(fmt.Sprintf("delta w%d", w.id), delta.Applied, nil)
		d := s.deltas[w.id]
		if d == nil {
			d = &pDelta{}
			s.deltas[w.id] = d
		}
		d.applied, d.batchNo = delta.Applied, d.batchNo+1
	}
	s.commits++
	if w.crashAfter > 0 && s.intn(w.crashAfter) == 0 {
		s.gen++
		w.kill()
		return errors.New("killed after commit")
	}
	return nil
}

func mustKey(p model.Pos) uint64 {
	_, seq, err := checkpoint.SplitBinlogName(p.File)
	if err != nil {
		panic(err)
	}
	k, err := checkpoint.BinlogKey(seq, p.BinlogPos)
	if err != nil {
		panic(err)
	}
	return k
}

// --- driver database/sql giả cho checkpoint.Compactor ---

type pConnector struct {
	s    *pSim
	gen  int
	hook func(string) // gọi trước mỗi câu lệnh
}

func (c pConnector) Connect(context.Context) (driver.Conn, error) { return pConn(c), nil }
func (c pConnector) Driver() driver.Driver                        { return pDrv{} }

type pDrv struct{}

func (pDrv) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type pConn struct {
	s    *pSim
	gen  int
	hook func(string)
}

func (c pConn) Prepare(q string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare unsupported: %s", q)
}
func (c pConn) Close() error              { return nil }
func (c pConn) Begin() (driver.Tx, error) { return nil, errors.New("no tx") }

type pRows struct {
	cols []string
	data [][]driver.Value
	i    int
}

func (r *pRows) Columns() []string { return r.cols }
func (r *pRows) Close() error      { return nil }
func (r *pRows) Next(dest []driver.Value) error {
	if r.i >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.i])
	r.i++
	return nil
}

func (c pConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	s := c.s
	s.tmu.Lock()
	defer s.tmu.Unlock()
	if s.gen != c.gen {
		return nil, errors.New("process is dead")
	}
	switch {
	case strings.HasPrefix(q, "SELECT worker_id, epoch, batch_no, applied FROM _strand_worker_delta"):
		r := &pRows{cols: []string{"worker_id", "epoch", "batch_no", "applied"}}
		for id, d := range s.deltas {
			r.data = append(r.data, []driver.Value{id, int64(1), d.batchNo, d.applied})
		}
		return r, nil
	case strings.HasPrefix(q, "SELECT applied FROM _strand_checkpoint"):
		return &pRows{cols: []string{"applied"}, data: [][]driver.Value{{s.global}}}, nil
	}
	return nil, fmt.Errorf("fake db: unsupported query %q", q)
}

func (c pConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	s := c.s
	s.tmu.Lock()
	defer s.tmu.Unlock()
	if s.gen != c.gen {
		return nil, errors.New("process is dead")
	}
	switch {
	case strings.HasPrefix(q, "UPDATE _strand_checkpoint SET applied = $1"):
		str := args[0].Value.(string)
		s.checkSetLocked("compactor global", str, nil)
		s.global = str
		s.globals++
		return driver.RowsAffected(1), nil
	case strings.HasPrefix(q, "UPDATE _strand_worker_delta SET applied = ''"):
		wid, bn := args[1].Value.(int64), args[3].Value.(int64)
		if d := s.deltas[wid]; d != nil && d.batchNo == bn {
			d.applied = ""
			return driver.RowsAffected(1), nil
		}
		return driver.RowsAffected(0), nil
	}
	return nil, fmt.Errorf("fake db: unsupported exec %q", q)
}

// ---------------------------------------------------------------------------
// Một chu kỳ chạy (một "process")
// ---------------------------------------------------------------------------

type pCycle struct {
	s       *pSim
	p       *Pipeline
	cancel  context.CancelFunc
	killed  atomic.Bool
	emitted atomic.Int64
	resume  uint64
	total   int // Txn dự kiến phát (đã lọc theo resume)
}

func (c *pCycle) kill() {
	if c.killed.CompareAndSwap(false, true) {
		c.s.tmu.Lock()
		c.s.gen++ // mọi commit sau đó bị từ chối
		c.s.tmu.Unlock()
		c.cancel()
	}
}

// killLocked: như kill nhưng gọi khi đã giữ tmu (trong ApplyBatch).
func (c *pCycle) killLocked() {
	if c.killed.CompareAndSwap(false, true) {
		c.cancel()
	}
}

func (s *pSim) boundaryOK(k uint64) bool { return s.boundaries[k] }

// restartCheck: nạp tập đã apply như LoadApplied và kiểm CP-1/CP-2/CP-3 trước khi chạy tiếp.
func (s *pSim) restartCheck() (*checkpoint.MySQLPosAppliedSet, bool) {
	s.tmu.Lock()
	var ds []string
	for _, d := range s.deltas {
		if strings.TrimSpace(d.applied) != "" {
			ds = append(ds, d.applied)
		}
	}
	global := s.global
	committed := append([]int(nil), s.committed...)
	s.tmu.Unlock()
	loaded, err := s.loadSet(global, ds)
	if err != nil {
		s.errf("restart: %v", err)
		return nil, false
	}
	set, ok := loaded.(*checkpoint.MySQLPosAppliedSet)
	if !ok {
		s.errf("restart: loaded %T", loaded)
		return nil, false
	}
	resume := set.LwdKey()
	if !s.boundaryOK(resume) {
		s.errf("CP-3: resume key %s is not an event boundary", checkpoint.FormatBinlogKey(resume))
	}
	for i := range s.log {
		in := set.ContainsKey(s.log[i].end)
		if committed[i] > 0 && !in {
			s.errf("CP-1/CP-2: txn %d (%s) is committed but the reloaded set lacks it (it would be applied again)\n  global=%s deltas=%v", i, checkpoint.FormatBinlogKey(s.log[i].end), global, ds)
		}
		if in && committed[i] == 0 {
			s.errf("CP-1: reloaded set contains txn %d (%s) that never committed", i, checkpoint.FormatBinlogKey(s.log[i].end))
		}
		if committed[i] == 0 && s.log[i].start < resume {
			s.errf("CP-3: txn %d (%s) is not applied but starts before the resume key %s (would be lost... or skipped)", i, checkpoint.FormatBinlogKey(s.log[i].start), checkpoint.FormatBinlogKey(resume))
		}
	}
	return set, s.nErrs() == 0
}

// runCycle chạy một process từ trạng thái đã lưu. killAfter ≥ 0: kill -9 sau khi reader phát ngần ấy Txn.
// Trả true nếu chạy xong hết (không bị kill).
func (s *pSim) runCycle(strict bool, killAfter int, hookKill bool, crashAfter, crashBefore int) bool {
	set, ok := s.restartCheck()
	if !ok {
		return true
	}
	s.strict = strict
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cy := &pCycle{s: s, cancel: cancel, resume: set.LwdKey()}

	st, err := openLocalState(s.dir, "p")
	if err != nil {
		s.t.Fatal(err)
	}
	if err := st.SetDone(pItems.String(), true); err != nil {
		s.t.Fatal(err)
	}
	filter, err := newTableFilter([]string{"app.items"}, nil, nil)
	if err != nil {
		s.t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Target.Batch.MaxRows = []int{1, 3, 10, 2000}[s.intn(4)]
	cfg.Target.Batch.MaxBytes = []string{"200", "2KiB", "4MiB"}[s.intn(3)]
	cfg.Target.Batch.MaxWait = []string{"", "1ms"}[s.intn(2)]
	scfg := sched.DefaultConfig()
	scfg.Serial = strict
	scfg.MaxPending = []int{2, 4, 16, 65536}[s.intn(4)]
	scfg.MaxHoles = []int{2, 8, 4096}[s.intn(3)]
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := &Pipeline{cfg: cfg, log: logger, id: "p", strict: strict, srcEngine: model.EngineMySQL, dstEngine: model.EnginePG,
		filter: filter, state: st, lsns: &lsnTracker{}, posMode: true, binlogBase: "mysql-bin", srcIdent: "server_id=1",
		metrics: metrics.NewMetrics(prometheus.NewRegistry()), health: metrics.NewHealthChecker(time.Hour),
		sched: sched.NewScheduler(scfg), touched: newTouchedKeys(200000), runCtx: ctx, cancel: cancel}
	p.seenCond = sync.NewCond(&p.seenMu)
	p.pauseCond = sync.NewCond(&p.pauseMu)
	p.tables = map[model.TableID]*tableMeta{
		pItems: {ID: pItems, Columns: []string{"id", "ver"}, PK: []string{"id"}, PKNumeric: true, AvgRow: 20},
	}
	p.snap = snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())
	p.applied0 = set.Clone()
	p.strictApplied = set.Clone()
	p.seen = set.Clone()
	p.lastRouteEnd = set.LwdKey()
	cy.p = p

	s.tmu.Lock()
	gen := s.gen
	s.tmu.Unlock()
	nWorkers := 1 + s.intn(3)
	if strict {
		nWorkers = 1
	}
	for i := 1; i <= nWorkers; i++ {
		w := &pWriter{s: s, id: int64(i), gen: gen, crashAfter: crashAfter, crashBefore: crashBefore, kill: cy.killLocked}
		if crashBefore > 0 {
			w.kill = cy.killLocked
		}
		p.writers = append(p.writers, w)
	}
	ccfg := checkpoint.DefaultCompactorConfig("p", 1)
	ccfg.Interval = time.Duration(1+s.intn(8)) * time.Millisecond
	p.comp = checkpoint.NewCompactor(sql.OpenDB(pConnector{s: s, gen: gen}), nil, "p", model.EngineMySQL, model.EnginePG, ccfg)
	if !strict {
		p.comp.SetVouch(p.vouchGlobal)
	}
	if hookKill {
		var steps atomic.Int64
		p.comp.SetStepHook(func(step checkpoint.CompactionStep) error {
			if s.intn(25) == 0 || steps.Add(1) > 400 {
				cy.kill()
				return errors.New("killed during compaction at " + step.String())
			}
			return nil
		})
	}
	// Như run(): gộp row delta còn lại vào row toàn cục TRƯỚC khi worker chạy.
	if err := p.comp.Compact(ctx); err != nil {
		if cy.killed.Load() {
			return false
		}
		s.errf("startup compaction: %v", err)
		return true
	}
	p.comp.Start(ctx)
	defer p.comp.Stop()

	// reader giả: phát Txn có start ≥ resume theo thứ tự binlog
	var todo []int
	for i := range s.log {
		if s.log[i].start >= cy.resume {
			todo = append(todo, i)
		}
	}
	cy.total = len(todo)
	txns := make(chan *model.Txn, 1+s.intn(32))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(txns)
		burst := 0
		for n, i := range todo {
			if killAfter >= 0 && n == killAfter {
				cy.kill()
				return
			}
			lt := s.log[i]
			t := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, File: lt.file, BinlogPos: lt.endOff},
				BinlogFile: lt.file, BinlogPos: lt.startOff, SourceTS: int64(i + 1)}
			for _, k := range lt.keys {
				t.Rows = append(t.Rows, model.RowChange{Table: pItems, Op: model.OpInsert, Key: []model.Value{k},
					Cols: []model.ColValue{{Name: "id", Value: k}, {Name: "ver", Value: int64(i + 1)}}})
			}
			select {
			case txns <- t:
			case <-ctx.Done():
				return
			}
			cy.emitted.Add(1)
			if burst > 0 {
				burst--
				continue
			}
			switch x := s.intn(100); {
			case x < 4:
				burst = 10 + s.intn(60)
			case x < 50:
			default:
				time.Sleep(time.Duration(s.intn(200)) * time.Microsecond)
			}
		}
	}()
	p.readyCh = p.sched.Ready()
	if strict {
		ordered := make(chan *model.Txn, 1024)
		go p.strictDispatcher(ctx, p.sched.Ready(), ordered)
		p.readyCh = ordered
	}
	for i, w := range p.writers {
		wg.Add(1)
		go func(i int, w batchWriter) {
			defer wg.Done()
			p.worker(ctx, i+1, w)
		}(i, w)
	}
	routerDone := make(chan struct{})
	go func() { defer close(routerDone); p.router(ctx, txns, nil) }()

	// chờ: bị kill, hoặc đã route hết và apply hết
	deadline := time.Now().Add(40 * time.Second)
	for {
		if cy.killed.Load() || p.failed() != nil {
			break
		}
		if int(cy.emitted.Load()) == cy.total && p.routed.Load() == uint64(cy.total) && p.sched.Lwd() >= p.admitted.Load() {
			break
		}
		if time.Now().After(deadline) {
			s.errf("stuck: emitted %d/%d routed %d lwd %d admitted %d", cy.emitted.Load(), cy.total, p.routed.Load(), p.sched.Lwd(), p.admitted.Load())
			break
		}
		time.Sleep(200 * time.Microsecond)
	}
	finished := !cy.killed.Load() && p.failed() == nil
	if finished {
		// dừng sạch: compaction cuối như run()
		p.comp.Stop()
		if err := p.comp.Compact(ctx); err != nil && !cy.killed.Load() {
			s.errf("final compaction: %v", err)
		}
		if !strict {
			// compaction cuối có vouch: mốc phải tới Txn cuối đã nhận
			s.tmu.Lock()
			g, err := checkpoint.ParseAppliedSet(model.EngineMySQL, s.global)
			s.tmu.Unlock()
			if err == nil && len(todo) > 0 {
				if ps := g.(*checkpoint.MySQLPosAppliedSet); ps.LwdKey() < s.log[todo[len(todo)-1]].end && !cy.killed.Load() {
					// chấp nhận nếu còn delta chưa gộp: kiểm bằng nạp lại đầy đủ
					s.tmu.Lock()
					var ds []string
					for _, d := range s.deltas {
						ds = append(ds, d.applied)
					}
					gs := s.global
					s.tmu.Unlock()
					full, _ := s.loadSet(gs, ds)
					if fp, ok := full.(*checkpoint.MySQLPosAppliedSet); !ok || fp.LwdKey() < s.log[todo[len(todo)-1]].end {
						s.errf("after a clean stop the checkpoint lwd %s is below the last txn %s (lag in the checkpoint: CP-3 liveness)",
							checkpoint.FormatBinlogKey(ps.LwdKey()), checkpoint.FormatBinlogKey(s.log[todo[len(todo)-1]].end))
					}
				}
			}
		}
	}
	cancel()
	// kill: chặn mọi commit muộn rồi chờ goroutine thoát
	s.tmu.Lock()
	s.gen++
	s.tmu.Unlock()
	done := make(chan struct{})
	go func() { wg.Wait(); <-routerDone; close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		s.errf("goroutines did not stop")
	}
	p.sched.Close()
	if e := p.failed(); e != nil && !cy.killed.Load() {
		s.errf("pipeline failed: %v", e)
	}
	return finished
}

func (s *pSim) finalCheck() {
	s.tmu.Lock()
	defer s.tmu.Unlock()
	want := map[int64]int{}
	for i := range s.log {
		for _, k := range s.log[i].keys {
			want[k] = i + 1
		}
		if s.committed[i] != 1 {
			s.errf("txn %d (%s) committed %d times in the end", i, checkpoint.FormatBinlogKey(s.log[i].end), s.committed[i])
		}
	}
	for k, v := range want {
		if s.kv[k] != v {
			s.errf("key %d: target ver %d, source ver %d", k, s.kv[k], v)
		}
	}
	for k := range s.kv {
		if _, ok := want[k]; !ok {
			s.errf("key %d exists at target but never at source", k)
		}
	}
}

func runPosSim(t *testing.T, seed int64) {
	modes := []string{"relaxed", "strict", "mixed"}
	mode := modes[int(seed%3)]
	s := newPSim(t, seed, mode, 60+int(seed%7)*25)
	cycles := 0
	for ; cycles < 60; cycles++ {
		strict := mode == "strict" || (mode == "mixed" && s.intn(2) == 0)
		killAfter, hook, ca, cb := -1, false, 0, 0
		if cycles < 12 {
			switch s.intn(4) {
			case 0:
				killAfter = s.intn(40)
			case 1:
				hook = true
			case 2:
				ca = 1 + s.intn(8)
			case 3:
				cb = 1 + s.intn(8)
				killAfter = s.intn(60)
			}
		}
		if s.runCycle(strict, killAfter, hook, ca, cb) {
			break
		}
		if s.nErrs() > 0 {
			break
		}
	}
	if s.nErrs() == 0 {
		// một chu kỳ cuối bắt buộc không kill: nạp lại, kiểm, rồi chạy tới hết
		if _, ok := s.restartCheck(); ok {
			s.runCycle(mode == "strict", -1, false, 0, 0)
		}
		if s.nErrs() == 0 {
			s.restartCheck()
		}
	}
	if s.nErrs() == 0 {
		s.finalCheck()
	}
	if s.nErrs() > 0 {
		t.Fatalf("seed %d mode %s (cycles %d, commits %d, globals %d, maxSetLen %d, maxSetN %d):\n%s", seed, mode, cycles, s.commits, s.globals,
			s.maxSetLen, s.maxSetN, strings.Join(s.errs, "\n"))
	}
	if os.Getenv("STRAND_SIM_VERBOSE") != "" {
		t.Logf("seed %d mode %s ok: cycles %d commits %d globals %d maxSetLen %d maxSetN %d", seed, mode, cycles, s.commits, s.globals, s.maxSetLen, s.maxSetN)
	}
}

// TestBinlogPosSimCrashRestart: vị trí binlog + kill -9 ở mọi bước, relaxed / strict / chuyển chế độ.
func TestBinlogPosSimCrashRestart(t *testing.T) {
	n := 45
	if v := os.Getenv("STRAND_SIM_SEEDS"); v != "" {
		if x, err := strconv.Atoi(v); err == nil && x > 0 {
			n = x
		}
	}
	for seed := int64(1); seed <= int64(n); seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) { runPosSim(t, seed) })
	}
}
