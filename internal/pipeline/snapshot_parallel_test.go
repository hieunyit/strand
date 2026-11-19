package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/config"
	"strand/internal/governor"
	"strand/internal/model"
	"strand/internal/sched"
	"strand/internal/snapshot"
)

func tid(name string) model.TableID { return model.TableID{Schema: "db", Table: name} }

func poolItems(names ...string) []poolTable {
	out := make([]poolTable, len(names))
	for i, n := range names {
		out[i] = poolTable{id: tid(n)}
	}
	return out
}

// --- worker pool ---

func TestRunTablePool_LimitsConcurrencyAndRunsEveryTableOnce(t *testing.T) {
	for _, n := range []int{1, 3, 8} {
		var cur, peak atomic.Int32
		var mu sync.Mutex
		runs := map[model.TableID]int{}
		items := poolItems("a", "b", "c", "d", "e", "f", "g")
		err := runTablePool(context.Background(), n, items, nil, func(_ context.Context, id model.TableID) error {
			c := cur.Add(1)
			for {
				p := peak.Load()
				if c <= p || peak.CompareAndSwap(p, c) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			cur.Add(-1)
			mu.Lock()
			runs[id]++
			mu.Unlock()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		want := int32(min(n, len(items)))
		if peak.Load() != want {
			t.Fatalf("n=%d: peak concurrency %d, want %d", n, peak.Load(), want)
		}
		for _, it := range items {
			if runs[it.id] != 1 {
				t.Fatalf("n=%d: table %s ran %d times", n, it.id, runs[it.id])
			}
		}
	}
}

// max_parallel = 1: đúng thứ tự cũ, từng bảng một.
func TestRunTablePool_OneWorkerKeepsOrder(t *testing.T) {
	var got []string
	items := poolItems("c", "a", "b")
	if err := runTablePool(context.Background(), 1, items, nil, func(_ context.Context, id model.TableID) error {
		got = append(got, id.Table)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "c,a,b" {
		t.Fatalf("order %v, want c,a,b", got)
	}
}

// Target PG có FK: bảng con chỉ bắt đầu khi bảng cha (cũng đang chờ) đã xong, dù có worker rảnh.
func TestRunTablePool_ParentsBeforeChildren(t *testing.T) {
	items := []poolTable{{id: tid("child"), deps: []model.TableID{tid("parent")}}, {id: tid("parent")}, {id: tid("other")}}
	var mu sync.Mutex
	done := map[string]bool{}
	err := runTablePool(context.Background(), 3, items, nil, func(_ context.Context, id model.TableID) error {
		mu.Lock()
		if id.Table == "child" && !done["parent"] {
			mu.Unlock()
			return fmt.Errorf("child started before parent finished")
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		mu.Lock()
		done[id.Table] = true
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 3 {
		t.Fatalf("done %v", done)
	}
}

func TestRunTablePool_FirstErrorCancelsTheRest(t *testing.T) {
	boom := errors.New("boom")
	var started atomic.Int32
	items := poolItems("a", "b", "c", "d", "e")
	err := runTablePool(context.Background(), 2, items, nil, func(ctx context.Context, id model.TableID) error {
		started.Add(1)
		if id.Table == "a" {
			time.Sleep(10 * time.Millisecond)
			return boom
		}
		<-ctx.Done() // bảng đang chạy dừng khi bảng khác lỗi
		return ctx.Err()
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if n := started.Load(); n != 2 {
		t.Fatalf("%d tables started after a failure, want 2 (no new table after the first error)", n)
	}
}

// Cổng của pace (pause --snapshot, breaker, windows) giữ MỌI worker trước khi nhận bảng.
func TestRunTablePool_GateHoldsEveryWorker(t *testing.T) {
	p := &Pipeline{govAlgo: "static", govTick: 5 * time.Millisecond, cfg: &config.Config{}}
	p.snapPaused.Store(true)
	var started atomic.Int32
	errc := make(chan error, 1)
	go func() {
		errc <- runTablePool(context.Background(), 3, poolItems("a", "b", "c"), p.paceGate, func(context.Context, model.TableID) error {
			started.Add(1)
			return nil
		})
	}()
	time.Sleep(100 * time.Millisecond)
	if n := started.Load(); n != 0 {
		t.Fatalf("%d tables started while the snapshot was paused", n)
	}
	if g := p.snapGated.Load(); g != 3 {
		t.Fatalf("gated workers = %d, want 3", g)
	}
	p.snapPaused.Store(false)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if started.Load() != 3 {
		t.Fatalf("started %d, want 3", started.Load())
	}
}

// fakeGov: governor tối giản với breaker điều khiển được.
type fakeGov struct{ breaker atomic.Bool }

func (g *fakeGov) Tick(context.Context) error       { return nil }
func (g *fakeGov) CurrentRate() (float64, float64)  { return 1e9, 1e9 }
func (g *fakeGov) Zone() governor.Zone              { return governor.ZoneGreen }
func (g *fakeGov) Pressure() float64                { return 0 }
func (g *fakeGov) BreakerOpen() bool                { return g.breaker.Load() }
func (g *fakeGov) RecordQueryLatency(time.Duration) {}
func (g *fakeGov) SetSoftRate(float64)              {}
func (g *fakeGov) BackgroundRunning() bool          { return true }
func (g *fakeGov) SensitivityUnknown() bool         { return false }

// Breaker mở: mọi worker đang chạy dừng ở pace sau chunk hiện tại, và không worker nào nhận bảng mới.
func TestBreakerHoldsAllParallelReaders(t *testing.T) {
	gov := &fakeGov{}
	p := &Pipeline{govAlgo: "onoff", gov: gov, govTick: 5 * time.Millisecond, cfg: &config.Config{}}
	p.setSoftRate(1e9)
	gov.breaker.Store(true)
	var paced atomic.Int32
	errc := make(chan error, 4)
	for _, n := range []string{"a", "b", "c"} {
		go func(n string) {
			errc <- p.paceTable(context.Background(), tid(n), 16384, time.Now())
			paced.Add(1)
		}(n)
	}
	var started atomic.Int32
	go func() {
		errc <- runTablePool(context.Background(), 2, poolItems("x", "y"), p.paceGate, func(context.Context, model.TableID) error {
			started.Add(1)
			return nil
		})
	}()
	time.Sleep(100 * time.Millisecond)
	if paced.Load() != 0 || started.Load() != 0 {
		t.Fatalf("breaker open: %d readers passed pace, %d tables started", paced.Load(), started.Load())
	}
	if g := p.snapGated.Load(); g != 5 {
		t.Fatalf("gated = %d, want 5 (3 readers + 2 idle workers)", g)
	}
	gov.breaker.Store(false)
	for i := 0; i < 4; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	if started.Load() != 2 {
		t.Fatalf("started %d tables after the breaker closed, want 2", started.Load())
	}
}

// --- trạng thái từng bảng ---

// Regression: snapChunkRows / curAvgRow là biến chung của pipeline — bảng B (chạy song song) ghi đè cỡ chunk
// và avg row của bảng A. Nay mỗi bảng có trạng thái riêng.
func TestNextChunkRowsIsPerTable(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "static")
	p.govAlgo = "static"
	p.setSoftRate(1e9)
	p.snapChunkRows.Store(1000)
	a, b := tid("a"), tid("b")
	p.startSnapRun(a, 100)
	p.startSnapRun(b, 16384)
	// B nhanh: cỡ chunk của B tăng gấp đôi.
	if got := p.nextChunkRows(b, snapshot.ChunkStat{Rows: 1000, ReadRows: 1000, Bytes: 100_000, Elapsed: 50 * time.Millisecond}); got != 2000 {
		t.Fatalf("b next = %d, want 2000", got)
	}
	// A chưa có số liệu cỡ (Rows = 0) và chunk rỗng (không biết byte): dùng cỡ và avg row CỦA A.
	ra := p.snapRunOf(a)
	if ra.chunkRows.Load() != 1000 {
		t.Fatalf("a chunk rows = %d, want 1000 (untouched by b)", ra.chunkRows.Load())
	}
	got := p.nextChunkRows(a, snapshot.ChunkStat{Elapsed: 250 * time.Millisecond})
	if got != 1000 {
		t.Fatalf("a next = %d, want 1000 (own size, own avg row 100 B)", got)
	}
	if p.snapRunOf(b).chunkRows.Load() != 2000 {
		t.Fatalf("b chunk rows changed by a")
	}
}

// Chi phí chunk khi không biết byte: tính theo avg row / cỡ chunk của chính bảng đó.
func TestPaceCostUsesTableState(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "static")
	p.govAlgo = "static"
	p.cfg.Resources.Source.IOPS.Hard = 0
	p.setSoftRate(100) // 100 trang/giây
	small, big := tid("small"), tid("big")
	p.startSnapRun(small, 16)  // 16 B × 1024 hàng = 1 trang → 10 ms
	p.startSnapRun(big, 16384) // 16 KiB × 1024 hàng = 1024 trang → 10 s
	p.snapRunOf(small).chunkRows.Store(1024)
	p.snapRunOf(big).chunkRows.Store(1024)
	start := time.Now()
	if err := p.paceTable(context.Background(), small, 0, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("pace of the small table took %v: cost used another table's row size", el)
	}
}

// --- barrier dùng chung ---

func barrierPipeline() *Pipeline {
	p := &Pipeline{cfg: &config.Config{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.sched = sched.NewScheduler(sched.DefaultConfig())
	p.seenCond = sync.NewCond(&p.seenMu)
	p.pauseCond = sync.NewCond(&p.pauseMu)
	return p
}

// Flush của nhiều bảng chạy song song trong MỘT lần dừng router; router (routeMu) bị giữ suốt cửa sổ.
func TestBarrierSharesOneRouterStopAcrossTables(t *testing.T) {
	p := barrierPipeline()
	p.snapReaders.Store(3)
	var inside atomic.Int32
	all := make(chan struct{})
	var once sync.Once
	errc := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() {
			errc <- p.barrier(context.Background(), func() error {
				if inside.Add(1) == 3 {
					once.Do(func() { close(all) })
				}
				if p.routeMu.TryLock() {
					p.routeMu.Unlock()
					return fmt.Errorf("router not stopped during fn")
				}
				select {
				case <-all: // cả ba fn cùng ở trong cửa sổ
					return nil
				case <-time.After(5 * time.Second):
					return fmt.Errorf("only %d of 3 flushes ran concurrently: barrier serialises tables", inside.Load())
				}
			})
		}()
	}
	for i := 0; i < 3; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	if w := p.bgroup.windows.Load(); w != 1 {
		t.Fatalf("router stops = %d, want 1", w)
	}
	if !p.routeMu.TryLock() {
		t.Fatal("routeMu still held after every fn finished")
	}
	p.routeMu.Unlock()
}

// Router chỉ chạy lại khi MỌI fn trong cửa sổ đã xong.
func TestBarrierReleasesRouterAfterLastFlush(t *testing.T) {
	p := barrierPipeline()
	p.snapReaders.Store(2)
	slow := make(chan struct{})
	errc := make(chan error, 2)
	entered := make(chan struct{}, 2)
	go func() {
		errc <- p.barrier(context.Background(), func() error { entered <- struct{}{}; return nil })
	}()
	go func() {
		errc <- p.barrier(context.Background(), func() error { entered <- struct{}{}; <-slow; return nil })
	}()
	<-entered
	<-entered
	routed := make(chan struct{})
	go func() { p.routeMu.Lock(); close(routed); p.routeMu.Unlock() }()
	// fn nhanh đã xong (nó chỉ báo entered rồi trả về); fn chậm còn trong cửa sổ: router phải chờ. (Người mở
	// cửa sổ chỉ trả về khi mọi fn xong, nên không chờ errc ở đây.)
	select {
	case <-routed:
		t.Fatal("router ran while a flush of another table was still inside the barrier")
	case <-time.After(100 * time.Millisecond):
	}
	close(slow)
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	<-routed
}

// Một reader (max_parallel = 1): không chờ gom, hành vi như barrier cũ.
func TestBarrierSingleReaderDoesNotWait(t *testing.T) {
	p := barrierPipeline()
	p.snapReaders.Store(1)
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := p.barrier(context.Background(), func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el > 100*time.Millisecond {
		t.Fatalf("5 single-reader barriers took %v", el)
	}
	if w := p.bgroup.windows.Load(); w != 5 {
		t.Fatalf("windows = %d, want 5", w)
	}
}

// Cửa sổ chỉ mở sau khi drain: Txn đã nhận phải commit trước mọi fn.
func TestBarrierDrainsBeforeAnyFn(t *testing.T) {
	p := barrierPipeline()
	p.snapReaders.Store(2)
	p.admitted.Store(1) // một Txn đã nhận, chưa apply (Lwd = 0)
	ran := make(chan struct{}, 2)
	errc := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errc <- p.barrier(context.Background(), func() error { ran <- struct{}{}; return nil }) }()
	}
	select {
	case <-ran:
		t.Fatal("fn ran before the router drained")
	case <-time.After(150 * time.Millisecond):
	}
	p.admitted.Store(0) // "applied"
	for i := 0; i < 2; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

// Pipeline lỗi khi đang drain: mọi người chờ nhận lỗi, không ai chạy fn.
func TestBarrierDrainFailureReachesJoiners(t *testing.T) {
	p := barrierPipeline()
	p.snapReaders.Store(2)
	p.admitted.Store(1)
	var ran atomic.Int32
	errc := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { errc <- p.barrier(context.Background(), func() error { ran.Add(1); return nil }) }()
	}
	time.Sleep(50 * time.Millisecond)
	p.fail(errors.New("target down"))
	for i := 0; i < 2; i++ {
		if err := <-errc; err == nil || !strings.Contains(err.Error(), "target down") {
			t.Fatalf("err = %v, want the pipeline failure", err)
		}
	}
	if ran.Load() != 0 {
		t.Fatal("fn ran although the drain failed")
	}
}

// --- quyền snapshot / kết thúc bootstrap ---

// `snapshot add` một bảng còn trong hàng đợi của bootstrap bị từ chối (trước đây bảng bị snapshot hai lần).
func TestSnapshotAddRejectsTableQueuedInBootstrap(t *testing.T) {
	p := &Pipeline{snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())}
	p.claimBootstrapTables([]model.TableID{tid("a"), tid("b")})
	err := p.SnapshotAdd(context.Background(), "db.b")
	if err == nil || !strings.Contains(err.Error(), "already being snapshotted") {
		t.Fatalf("snapshot add of a queued table: %v", err)
	}
}

// Bootstrap chỉ xong khi pool xong VÀ không còn snapshot add nào chạy.
func TestBootstrapFinishesAfterPoolAndAdds(t *testing.T) {
	p := &Pipeline{snap: snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions()),
		log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.bootstrap.Store(true)
	p.retentionRisk.Store(true)
	p.claimBootstrapTables([]model.TableID{tid("a")})
	if !p.claimSnapshotAdd(tid("added")) {
		t.Fatal("claim")
	}
	p.releaseSnapshotAdd(tid("a"))
	p.snapAddMu.Lock()
	p.snapPoolRunning = false
	p.snapAddMu.Unlock()
	if p.maybeFinishBootstrap() || !p.bootstrap.Load() {
		t.Fatal("bootstrap finished while a snapshot add is still running")
	}
	p.releaseSnapshotAdd(tid("added"))
	if !p.maybeFinishBootstrap() || p.bootstrap.Load() || p.retentionRisk.Load() {
		t.Fatal("bootstrap did not finish after the last snapshot")
	}
}

// Bảng lớn trước (LPT) khi N > 1; N = 1 giữ thứ tự cũ.
func TestSnapshotOrderLargestFirst(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}, srcEngine: model.EngineMySQL, dstEngine: model.EngineMySQL,
		tables: map[model.TableID]*tableMeta{
			tid("a"): {ID: tid("a"), AvgRow: 100},
			tid("b"): {ID: tid("b"), AvgRow: 100},
			tid("c"): {ID: tid("c"), AvgRow: 1000},
		}}
	p.tableRowsEst.Store(tid("a"), int64(10))
	p.tableRowsEst.Store(tid("b"), int64(1_000_000))
	p.tableRowsEst.Store(tid("c"), int64(1_000))
	in := []model.TableID{tid("a"), tid("b"), tid("c")}
	names := func(items []poolTable) string {
		var s []string
		for _, it := range items {
			s = append(s, it.id.Table)
		}
		return strings.Join(s, ",")
	}
	if got := names(p.snapshotOrder(context.Background(), in, 1)); got != "a,b,c" {
		t.Fatalf("n=1 order %s, want a,b,c", got)
	}
	if got := names(p.snapshotOrder(context.Background(), in, 3)); got != "b,c,a" {
		t.Fatalf("n=3 order %s, want b,c,a", got)
	}
}

func TestSnapParallel(t *testing.T) {
	p := &Pipeline{cfg: &config.Config{}, srcEngine: model.EngineMySQL}
	if p.snapParallel() != 1 {
		t.Fatal("default must be 1")
	}
	p.cfg.Bootstrap.Logical.MaxParallel = 4
	if p.snapParallel() != 4 {
		t.Fatal("max_parallel 4")
	}
	p.srcEngine = model.EnginePG
	if p.snapParallel() != 1 {
		t.Fatal("PostgreSQL source must stay sequential")
	}
}
