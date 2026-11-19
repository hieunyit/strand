package pipeline

// Snapshot logic song song nhiều bảng (bootstrap.logical.max_parallel).
//
// Mô hình: tối đa N bảng được snapshot cùng lúc (worker pool, mỗi worker lấy bảng chờ kế tiếp); chunk của
// MỘT bảng vẫn tuần tự (keyset walk), bảng khác nhau chạy song song. N = 1 (mặc định) là hành vi cũ.
//
// Cấu trúc dùng chung và cách chúng hành xử khi nhiều bảng chạy cùng lúc:
//   - Ngân sách đọc nguồn: mọi reader cùng qua paceTable → paceGate (pause --snapshot, breaker, windows,
//     governor) và cùng chia token bucket snapBucket: tổng tốc độ đọc ≤ R như khi chỉ có một bảng. Worker
//     còn qua paceGate TRƯỚC khi nhận bảng mới (breaker mở thì không bảng nào bắt đầu).
//   - Range-state (snapshot.RangeStateManager): một bản đồ bảng → range dưới một mutex; bảng khác nhau
//     không chung chunk. FlushChunk không còn giữ mutex suốt lần ghi target (xem snapshot.FlushChunk),
//     nên reader của bảng khác không đứng chờ một flush.
//   - Barrier (router dừng + drain) bao quanh mỗi flush: là điều kiện của RS-1 cho unique key (target nhất
//     quán tại vị trí router). barrierGroup cho flush của NHIỀU bảng dùng chung MỘT lần dừng router: các
//     flush chạy song song trong cửa sổ đó (mỗi flush chỉ ghi range của bảng mình; FK của target PG được
//     giữ bằng thứ tự cha-trước-con của pool).
//   - Điểm hẹn watermark: mỗi chunk chờ trên seenCond (Broadcast) / lsns (vị trí binlog) / LSN (PG) —
//     nhiều chunk của nhiều bảng chờ đồng thời là an toàn; low/high đọc trên kết nối riêng của chunk.
//   - Trạng thái từng bảng (cỡ chunk thích ứng, avg row, số row đã ghi, tốc độ): snapRun, không còn là
//     biến chung của pipeline.
//   - State cục bộ (plan, done): localState có mutex, mỗi bảng một plan; updatePlan tuần tự hóa
//     đọc-sửa-ghi plan. Restart: mỗi bảng chưa Done chạy tiếp từ plan riêng của nó.
//   - Quyền snapshot một bảng (snapAdding): pool giành mọi bảng chờ ngay từ đầu, nên `snapshot add` một bảng
//     đang chờ trong hàng bị từ chối thay vì snapshot hai lần; bootstrap chỉ xong khi pool và mọi snapshot
//     add đều xong.
//   - Source PG: luôn tuần tự (max_parallel bị bỏ qua, có log) — xem snapParallel.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"strand/internal/config"
	"strand/internal/model"
)

// snapParallel: số bảng snapshot đồng thời có hiệu lực. Source PG luôn 1: chunk PG đọc qua một cổng
// visibility chung (PGChunkSource.WaitGate, XIDTracker) và kết thúc bảng bằng compaction + đối chiếu
// checkpoint trong barrier (finishPGSnapshot), chunk re-read của UPDATE đổi PK có TOAST (02 §8) giả định
// flush giữ khóa range-state — chưa được chứng minh cho nhiều bảng cùng lúc, nên giữ tuần tự.
func (p *Pipeline) snapParallel() int {
	if p.cfg == nil {
		return 1
	}
	n := p.cfg.Bootstrap.Logical.MaxParallel
	if n <= 0 {
		n = 1
	}
	if n > config.MaxSnapshotParallel {
		n = config.MaxSnapshotParallel
	}
	if p.srcEngine == model.EnginePG {
		n = 1
	}
	return n
}

// ---------------------------------------------------------------------------
// Trạng thái từng bảng đang snapshot
// ---------------------------------------------------------------------------

// snapRun: trạng thái của một bảng đang snapshot (một lần chạy).
type snapRun struct {
	id        model.TableID
	started   time.Time
	avgRow    atomic.Int64 // AVG_ROW_LENGTH của bảng (chi phí chunk khi không biết byte)
	chunkRows atomic.Int64 // cỡ chunk hiện hành của bảng (luật 250 ms)
	rows      atomic.Int64 // số row đã flush trong lần chạy này
	rate      rateWindow
}

func (p *Pipeline) startSnapRun(id model.TableID, avgRow int64) *snapRun {
	r := &snapRun{id: id, started: time.Now()}
	r.avgRow.Store(avgRow)
	r.chunkRows.Store(p.snapChunkRows.Load())
	r.rate.add(r.started, 0)
	p.snapRunMu.Lock()
	if p.snapRuns == nil {
		p.snapRuns = map[model.TableID]*snapRun{}
	}
	p.snapRuns[id] = r
	p.snapRunMu.Unlock()
	return r
}

func (p *Pipeline) endSnapRun(id model.TableID) {
	p.snapRunMu.Lock()
	delete(p.snapRuns, id)
	p.snapRunMu.Unlock()
}

func (p *Pipeline) snapRunOf(id model.TableID) *snapRun {
	p.snapRunMu.Lock()
	defer p.snapRunMu.Unlock()
	return p.snapRuns[id]
}

// runningSnapshots: các bảng đang snapshot (đã sắp theo tên).
func (p *Pipeline) runningSnapshots() []*snapRun {
	p.snapRunMu.Lock()
	out := make([]*snapRun, 0, len(p.snapRuns))
	for _, r := range p.snapRuns {
		out = append(out, r)
	}
	p.snapRunMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].id.String() < out[j].id.String() })
	return out
}

func (p *Pipeline) runningSnapshotNames() []string {
	runs := p.runningSnapshots()
	out := make([]string, len(runs))
	for i, r := range runs {
		out[i] = r.id.String()
	}
	return out
}

// noteSnapFlushed: snapshot.MySQLSnapshotter.OnFlushed.
func (p *Pipeline) noteSnapFlushed(table model.TableID, rows int64) {
	if r := p.snapRunOf(table); r != nil {
		r.rows.Add(rows)
	}
}

// sampleSnapRuns lấy mẫu tốc độ từng bảng (progressLoop).
func (p *Pipeline) sampleSnapRuns(now time.Time) {
	for _, r := range p.runningSnapshots() {
		r.rate.add(now, r.rows.Load())
	}
}

// ---------------------------------------------------------------------------
// Barrier dùng chung
// ---------------------------------------------------------------------------

const (
	bgIdle = iota
	bgGather
	bgOpen
)

// barrierGather: thời gian tối đa người mở cửa sổ chờ reader của bảng khác đang tới barrier (router vẫn
// chạy trong lúc này để điểm hẹn của họ xong). barrierJoinMax: cửa sổ đã mở lâu hơn mức này thì không nhận
// thêm người (router không bị giữ mãi).
var (
	barrierGather  = 250 * time.Millisecond
	barrierJoinMax = 500 * time.Millisecond
)

// barrierGroup: một lần dừng router (routeMu + drain) dùng chung cho fn của nhiều bảng. Trạng thái:
// idle → gather (người mở đã đăng ký, router còn chạy, người khác tới thì ghi danh) → open (router dừng,
// đã drain; mọi người đã ghi danh chạy fn song song, người tới muộn vào thẳng nếu cửa sổ còn trẻ) → idle
// khi mọi fn xong (nhả routeMu).
type barrierGroup struct {
	mu       sync.Mutex
	state    int
	gen      uint64
	arrived  int // người đã ghi danh cho cửa sổ đang gather (gồm người mở)
	active   int // fn đang/ sắp chạy trong cửa sổ open
	openedAt time.Time
	failErr  error // drain thất bại ở cửa sổ gen
	changed  chan struct{}
	// windows / maxShared: số cửa sổ đã mở và số fn lớn nhất cùng chạy trong một cửa sổ (test, log).
	windows   atomic.Int64
	maxShared atomic.Int64
}

func (g *barrierGroup) notifyLocked() {
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}

func (g *barrierGroup) waitChLocked() <-chan struct{} {
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
	return g.changed
}

// barrierWanted: số reader snapshot đang hướng tới barrier (đang chạy, không ở trong pace).
func (p *Pipeline) barrierWanted() int {
	n := int(p.snapReaders.Load() - p.snapPacing.Load())
	if n < 1 {
		n = 1
	}
	return n
}

// barrier dừng router, chờ mọi Txn đã nhận được commit, rồi chạy fn (02 §5). fn của nhiều bảng có thể
// chạy đồng thời trong cùng một lần dừng (barrierGroup); hợp đồng giữ nguyên với từng fn: router không
// route Txn nào trong lúc fn chạy, và mọi Txn router đã nhận trước đó đã commit ở target.
func (p *Pipeline) barrier(ctx context.Context, fn func() error) error {
	g := &p.bgroup
	g.mu.Lock()
	for {
		switch g.state {
		case bgOpen:
			if time.Since(g.openedAt) < barrierJoinMax {
				g.active++
				p.noteShared(g)
				g.mu.Unlock()
				return p.barrierRun(g, fn)
			}
		case bgGather:
			gen := g.gen
			g.arrived++
			g.notifyLocked()
			for g.gen == gen && g.state == bgGather {
				ch := g.waitChLocked()
				g.mu.Unlock()
				select {
				case <-ch:
					g.mu.Lock()
				case <-ctx.Done():
					g.mu.Lock()
					switch {
					case g.gen == gen && g.state == bgGather:
						g.arrived--
					case g.gen == gen && g.state == bgOpen:
						g.active-- // đã được tính khi mở cửa sổ
						g.notifyLocked()
					}
					g.mu.Unlock()
					return ctx.Err()
				}
			}
			if g.gen == gen && g.state == bgOpen {
				g.mu.Unlock()
				return p.barrierRun(g, fn)
			}
			if g.gen == gen && g.failErr != nil {
				err := g.failErr
				if errors.Is(err, context.Canceled) && ctx.Err() == nil {
					continue // ctx của người mở bị hủy, không phải của mình: thử mở cửa sổ khác
				}
				g.mu.Unlock()
				return err
			}
			continue
		case bgIdle:
			g.mu.Unlock()
			return p.barrierLead(ctx, g, fn)
		}
		// Cửa sổ đang mở đã quá già: chờ nó đóng rồi xét lại.
		ch := g.waitChLocked()
		g.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		g.mu.Lock()
	}
}

func (p *Pipeline) noteShared(g *barrierGroup) {
	for {
		cur := g.maxShared.Load()
		if int64(g.active) <= cur || g.maxShared.CompareAndSwap(cur, int64(g.active)) {
			return
		}
	}
}

// barrierRun chạy fn của một người đã được tính vào g.active rồi trả chỗ.
func (p *Pipeline) barrierRun(g *barrierGroup, fn func() error) error {
	err := fn()
	g.mu.Lock()
	g.active--
	g.notifyLocked()
	g.mu.Unlock()
	return err
}

// barrierLead mở một cửa sổ: gather → routeMu + drain → open → chờ mọi fn xong → nhả routeMu.
func (p *Pipeline) barrierLead(ctx context.Context, g *barrierGroup, fn func() error) error {
	g.mu.Lock()
	if g.state != bgIdle { // người khác vừa mở trước: quay lại làm người tham gia
		g.mu.Unlock()
		return p.barrier(ctx, fn)
	}
	g.state = bgGather
	g.gen++
	gen := g.gen
	g.arrived = 1
	g.failErr = nil
	g.notifyLocked()
	// Gather: router vẫn chạy, reader khác đang ở điểm hẹn kịp ghi danh. Một reader (max_parallel = 1) thì
	// barrierWanted = 1 và không chờ gì (hành vi cũ).
	deadline := time.Now().Add(barrierGather)
	for g.arrived < p.barrierWanted() {
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		ch := g.waitChLocked()
		g.mu.Unlock()
		t := time.NewTimer(min(left, 10*time.Millisecond))
		select {
		case <-ch:
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
		g.mu.Lock()
		if ctx.Err() != nil {
			break
		}
	}
	g.mu.Unlock()

	p.routeMu.Lock()
	defer p.routeMu.Unlock()
	var err error
	if cerr := ctx.Err(); cerr != nil {
		err = cerr
	} else if !p.drain(10 * time.Minute) {
		if err = p.failed(); err == nil {
			err = fmt.Errorf("snapshot barrier: pipeline did not drain")
		}
	}
	g.mu.Lock()
	if err != nil {
		g.failErr = err
		g.state = bgIdle
		g.arrived = 0
		g.notifyLocked()
		g.mu.Unlock()
		return err
	}
	g.state = bgOpen
	g.openedAt = time.Now()
	g.active = g.arrived
	g.arrived = 0
	g.windows.Add(1)
	p.noteShared(g)
	g.notifyLocked()
	g.mu.Unlock()

	err = fn()

	g.mu.Lock()
	g.active--
	g.notifyLocked()
	for g.active > 0 {
		ch := g.waitChLocked()
		g.mu.Unlock()
		<-ch // fn của người khác tự quan sát ctx của họ; chờ họ xong trước khi router chạy lại
		g.mu.Lock()
	}
	if g.gen == gen {
		g.state = bgIdle
	}
	g.notifyLocked()
	g.mu.Unlock()
	return err
}

// ---------------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------------

// poolTable: một bảng chờ snapshot; deps: bảng (cũng đang chờ) phải xong trước khi bảng này bắt đầu
// (target PG có FK: cha trước con, orderSnapshotTables).
type poolTable struct {
	id   model.TableID
	deps []model.TableID
}

// runTablePool chạy fn cho từng bảng của items, tối đa n bảng cùng lúc. Worker rảnh lấy bảng ĐẦU TIÊN
// (theo thứ tự items) chưa chạy mà mọi deps đã xong; trước khi nhận bảng, worker qua gate (cổng của pace:
// breaker, pause, windows). Lỗi đầu tiên hủy các bảng khác và được trả về.
func runTablePool(ctx context.Context, n int, items []poolTable, gate func(context.Context) error,
	fn func(context.Context, model.TableID) error) error {
	if n < 1 {
		n = 1
	}
	if n > len(items) {
		n = len(items)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		mu       sync.Mutex
		changed  = make(chan struct{})
		started  = make([]bool, len(items))
		done     = map[model.TableID]bool{}
		pending  = map[model.TableID]bool{}
		running  int
		firstErr error
	)
	for _, it := range items {
		pending[it.id] = true
	}
	notify := func() { close(changed); changed = make(chan struct{}) }
	// next: chỉ số bảng kế tiếp chạy được, -1 nếu chưa có (chờ), -2 nếu hết việc.
	next := func() int {
		left := false
		for i, it := range items {
			if started[i] {
				continue
			}
			left = true
			ready := true
			for _, d := range it.deps {
				if pending[d] && !done[d] {
					ready = false
					break
				}
			}
			if ready {
				return i
			}
		}
		if !left {
			return -2
		}
		return -1
	}
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if gate != nil {
					if err := gate(cctx); err != nil {
						return
					}
				}
				mu.Lock()
				i := -1
				for firstErr == nil && cctx.Err() == nil {
					if i = next(); i != -1 {
						break
					}
					if running == 0 {
						// Không bảng nào chạy được và không bảng nào đang chạy: deps không thể thỏa.
						firstErr = fmt.Errorf("snapshot order: tables wait for each other: %s", unstartedNames(items, started))
						cancel()
						notify()
						break
					}
					ch := changed
					mu.Unlock()
					select {
					case <-ch:
					case <-cctx.Done():
					}
					mu.Lock()
				}
				if i < 0 || firstErr != nil || cctx.Err() != nil {
					mu.Unlock()
					return
				}
				started[i] = true
				running++
				mu.Unlock()

				err := fn(cctx, items[i].id)

				mu.Lock()
				running--
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
					cancel()
				} else {
					done[items[i].id] = true
				}
				notify()
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for i := range items {
		if !done[items[i].id] {
			return fmt.Errorf("snapshot of %s did not run", items[i].id)
		}
	}
	return nil
}

func unstartedNames(items []poolTable, started []bool) string {
	var out []string
	for i, it := range items {
		if !started[i] {
			out = append(out, it.id.String())
		}
	}
	return strings.Join(out, ", ")
}

// snapshotOrder dựng hàng đợi của pool. N = 1: giữ nguyên thứ tự cũ (tên, hoặc cha trước con với target
// PG). N > 1: bảng còn nhiều việc nhất trước (LPT — longest processing time first): bảng lớn bắt đầu sớm
// nên phần đuôi (lúc chỉ còn một bảng lớn chạy một mình, các worker khác rảnh) ngắn nhất; bảng nhỏ lấp chỗ
// trống cuối. "Việc" = ước lượng row × avg row × phần chưa xong của plan (restart: bảng dở dang được tính
// theo phần còn lại). Phụ thuộc FK (target PG) vẫn được pool giữ qua deps.
func (p *Pipeline) snapshotOrder(ctx context.Context, tables []model.TableID, n int) []poolTable {
	inSet := map[model.TableID]bool{}
	for _, id := range tables {
		inSet[id] = true
	}
	fkOrder := p.dstEngine == model.EnginePG && p.cfg != nil && p.cfg.Bootstrap.CreateSchemaAuto()
	items := make([]poolTable, len(tables))
	for i, id := range tables {
		items[i].id = id
		if !fkOrder {
			continue
		}
		if m := p.table(id); m != nil {
			for _, fk := range m.Keys.ForeignKeys {
				if fk.ParentTable != id && inSet[fk.ParentTable] {
					items[i].deps = append(items[i].deps, fk.ParentTable)
				}
			}
		}
	}
	if n <= 1 || len(items) < 2 {
		return items
	}
	work := make(map[model.TableID]float64, len(items))
	for _, it := range items {
		rows := p.tableRowsEstimate(ctx, it.id)
		avg := int64(200)
		if m := p.table(it.id); m != nil && m.AvgRow > 0 {
			avg = m.AvgRow
		}
		left := 1.0
		if p.state != nil {
			left = 1 - tableProgress(p.state, it.id.String(), nil)/100
		}
		work[it.id] = float64(rows) * float64(avg) * left
	}
	sortByWork(items, work)
	return items
}

// sortByWork: việc giảm dần, bằng nhau thì theo tên (ổn định, dễ đoán).
func sortByWork(items []poolTable, work map[model.TableID]float64) {
	sort.SliceStable(items, func(i, j int) bool {
		wi, wj := work[items[i].id], work[items[j].id]
		if wi != wj {
			return wi > wj
		}
		return items[i].id.String() < items[j].id.String()
	})
}

// tableRowsEstimate: ước lượng row của một bảng (thống kê source, cache dùng chung với admin Tables).
func (p *Pipeline) tableRowsEstimate(ctx context.Context, id model.TableID) int64 {
	if v, ok := p.tableRowsEst.Load(id); ok {
		return v.(int64)
	}
	if p.src == nil {
		return 0
	}
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	n := p.estimateRows(cctx, []model.TableID{id})
	if cctx.Err() == nil {
		p.tableRowsEst.Store(id, n)
	}
	return n
}

// runSnapshots snapshot các bảng chờ của bootstrap với tối đa snapParallel() bảng cùng lúc.
func (p *Pipeline) runSnapshots(ctx context.Context, tables []model.TableID) {
	n := p.snapParallel()
	p.snapTablesTotal.Store(int64(len(tables)))
	p.snapRowsEst.Store(p.estimateRows(ctx, tables))
	// Giành mọi bảng chờ ngay từ đầu: `snapshot add` của một bảng còn trong hàng bị từ chối (nếu không,
	// bảng được snapshot hai lần song song — một bởi add, một khi tới lượt trong pool).
	p.claimBootstrapTables(tables)
	items := p.snapshotOrder(ctx, tables, n)
	order := make([]string, len(items))
	for i, it := range items {
		order[i] = it.id.String()
	}
	p.log.Info("bootstrap snapshot", "tables", len(tables), "max_parallel", n, "order", strings.Join(order, ","))
	err := runTablePool(ctx, n, items, p.paceGate, func(ctx context.Context, id model.TableID) error {
		if err := p.snapshotTable(ctx, id); err != nil {
			return fmt.Errorf("snapshot %s: %w", id, err)
		}
		p.releaseSnapshotAdd(id)
		return nil
	})
	if err != nil {
		if ctx.Err() == nil {
			p.fail(err)
		}
		return
	}
	p.snapAddMu.Lock()
	p.snapPoolRunning = false
	p.snapAddMu.Unlock()
	if p.maybeFinishBootstrap() {
		p.log.Info("bootstrap finished: pipeline synced", "windows", p.bgroup.windows.Load(), "max_shared_flushes", p.bgroup.maxShared.Load())
		return
	}
	p.log.Info("bootstrap tables finished; waiting for running snapshot add", "running", strings.Join(p.runningSnapshotNames(), ","))
	p.snapAddMu.Lock()
	p.snapPoolDeferred = true
	p.snapAddMu.Unlock()
}

// claimBootstrapTables giành quyền snapshot mọi bảng chờ của bootstrap (xem runSnapshots) và đánh dấu pool
// đang chạy.
func (p *Pipeline) claimBootstrapTables(tables []model.TableID) {
	p.snapAddMu.Lock()
	defer p.snapAddMu.Unlock()
	if p.snapAdding == nil {
		p.snapAdding = map[model.TableID]bool{}
	}
	for _, id := range tables {
		p.snapAdding[id] = true
	}
	p.snapPoolRunning = true
}

// maybeFinishBootstrap: hết bootstrap khi pool đã xong, không còn snapshot add nào và range-state rỗng.
// Trả true nếu lần gọi này vừa kết thúc bootstrap.
func (p *Pipeline) maybeFinishBootstrap() bool {
	p.snapAddMu.Lock()
	busy := len(p.snapAdding) > 0 || p.snapPoolRunning
	deferred := p.snapPoolDeferred
	if !busy {
		p.snapPoolDeferred = false
	}
	p.snapAddMu.Unlock()
	if busy || (p.snap != nil && p.snap.Manager.TableCount() != 0) {
		return false
	}
	if !p.bootstrap.Swap(false) {
		return false
	}
	p.retentionRisk.Store(false)
	if deferred {
		p.log.Info("bootstrap finished: pipeline synced")
	}
	return true
}
