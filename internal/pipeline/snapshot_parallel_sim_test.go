package pipeline

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// Mô phỏng: nhiều bảng snapshot song song (flush qua barrier dùng chung, ghi target ngoài khóa range-state)
// trong khi router liên tục đưa event của chính các bảng đó qua range-state. Target giả: event Apply được
// ghi ngay (router đồng bộ = đã drain), flush chunk thay nguyên range bằng row đã merge. Kết thúc, target phải
// giữ ảnh MỚI NHẤT của mọi khóa (RS-1: không mất event bị buffer, không ảnh cũ đè ảnh mới), và router không
// bao giờ gặp chunk đang ghi ngoài khóa (ActionReject).
func TestParallelSnapshotWithRouterSim(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) { runParallelRouterSim(t, seed) })
	}
}

func runParallelRouterSim(t *testing.T, seed int64) {
	const tables, keys, chunk = 4, 400, 50
	p := barrierPipeline()
	p.snap = snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())
	p.snap.Barrier = p.barrier
	p.snapReaders.Store(tables)
	g0, j0 := barrierGather, barrierJoinMax
	barrierGather, barrierJoinMax = 5*time.Millisecond, 20*time.Millisecond
	defer func() { barrierGather, barrierJoinMax = g0, j0 }()

	var tmu sync.Mutex
	target := map[string]int64{} // "t/k" → version (0 = row snapshot)
	latest := map[string]int64{} // version mới nhất đã route (ghi bởi router)
	deleted := map[string]bool{} // khóa bị xóa sau cùng
	srcVer := map[string]int64{} // "source": version hiện tại của khóa (snapshot đọc từ đây)
	srcGone := map[string]bool{}
	key := func(tb model.TableID, k int64) string { return fmt.Sprintf("%s/%d", tb.Table, k) }
	verOf := func(rc *model.RowChange) int64 { return rc.Cols[1].Value.(int64) }
	p.snap.FlushRows = func(_ context.Context, tb model.TableID, _ string, _ []string, rows []*model.RowChange, lo, hi int64) error {
		time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond) // ghi target "chậm", ngoài khóa
		tmu.Lock()
		defer tmu.Unlock()
		for k := lo; k <= hi; k++ {
			delete(target, key(tb, k))
		}
		for _, r := range rows {
			target[key(tb, r.Key[0].(int64))] = verOf(r)
		}
		return nil
	}
	ids := make([]model.TableID, tables)
	for i := range ids {
		ids[i] = tid(fmt.Sprintf("t%d", i))
		p.snap.Manager.RegisterTable(ids[i], "id", []*snapshot.Chunk{{ID: 1, Table: ids[i], PKCol: "id", StartPK: 1, EndPK: keys,
			State: snapshot.StatePending, BufferedChanges: map[snapshot.Key]*model.RowChange{}}}, keys)
	}
	row := func(tb model.TableID, k, v int64, op model.Op) *model.RowChange {
		return &model.RowChange{Table: tb, Op: op, Key: []model.Value{k}, Cols: []model.ColValue{{Name: "id", Value: k}, {Name: "ver", Value: v}}}
	}

	// Router: event tuần tự, mỗi event dưới routeMu (như router thật); Apply ghi thẳng target.
	var ver atomic.Int64
	stop := make(chan struct{})
	routerDone := make(chan error, 1)
	go func() {
		r := rand.New(rand.NewSource(seed))
		for {
			select {
			case <-stop:
				routerDone <- nil
				return
			default:
			}
			tb := ids[r.Intn(tables)]
			k := 1 + r.Int63n(keys)
			op := model.OpUpdate
			if r.Intn(5) == 0 {
				op = model.OpDelete
			}
			p.routeMu.Lock()
			v := ver.Add(1)
			ev := row(tb, k, v, op)
			tmu.Lock()
			srcVer[key(tb, k)], srcGone[key(tb, k)] = v, op == model.OpDelete
			latest[key(tb, k)], deleted[key(tb, k)] = v, op == model.OpDelete
			tmu.Unlock()
			act, _ := p.snap.Manager.HandleStreamEvent(ev, model.Pos{})
			switch act {
			case snapshot.ActionReject:
				p.routeMu.Unlock()
				routerDone <- fmt.Errorf("router met a chunk being written outside the range-state lock (key %s)", key(tb, k))
				return
			case snapshot.ActionApply:
				tmu.Lock()
				if op == model.OpDelete {
					delete(target, key(tb, k))
				} else {
					target[key(tb, k)] = v
				}
				tmu.Unlock()
			}
			p.routeMu.Unlock()
			time.Sleep(time.Duration(r.Intn(50)) * time.Microsecond)
		}
	}()

	// Mỗi bảng một worker: keyset walk tuần tự, đọc "source" sau khi chunk sang Đang đọc, flush trong barrier.
	var wg sync.WaitGroup
	errs := make(chan error, tables)
	for _, tb := range ids {
		wg.Add(1)
		go func(tb model.TableID) {
			defer wg.Done()
			for lo := int64(1); lo <= keys; lo += chunk {
				hi := lo + chunk - 1
				c, err := p.snap.Manager.CarveChunk(tb, lo, hi)
				if err != nil {
					errs <- err
					return
				}
				p.snap.Manager.BeginReading(c) // từ đây event được buffer (không còn "Chưa đọc")
				var rows []*model.RowChange
				tmu.Lock()
				for k := lo; k <= hi; k++ {
					if !srcGone[key(tb, k)] {
						rows = append(rows, row(tb, k, srcVer[key(tb, k)], model.OpInsert))
					}
				}
				tmu.Unlock()
				p.snap.Manager.StartReading(c, nil)
				p.snap.Manager.FinishReading(c, nil, rows)
				// "Điểm hẹn": router phải xử lý thêm vài event sau khi đọc (như chờ router tới high).
				for v0, until := ver.Load(), time.Now().Add(5*time.Second); ver.Load() < v0+25 && time.Now().Before(until); {
					time.Sleep(50 * time.Microsecond)
				}
				if err := p.barrier(context.Background(), func() error {
					return p.snap.FlushChunk(context.Background(), nil, c, tb, "id", []string{"id", "ver"})
				}); err != nil {
					errs <- err
					return
				}
			}
		}(tb)
	}
	wg.Wait()
	close(stop)
	if err := <-routerDone; err != nil {
		t.Fatal(err)
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	tmu.Lock()
	defer tmu.Unlock()
	bad := 0
	for _, tb := range ids {
		for k := int64(1); k <= keys; k++ {
			kk := key(tb, k)
			got, ok := target[kk]
			switch {
			case deleted[kk]:
				if ok {
					bad++
					t.Errorf("%s deleted at the source but present on the target (ver %d)", kk, got)
				}
			case !ok:
				bad++
				t.Errorf("%s missing on the target (latest ver %d)", kk, latest[kk])
			case got != latest[kk]:
				bad++
				t.Errorf("%s target ver %d, want latest %d", kk, got, latest[kk])
			}
			if bad > 10 {
				t.FailNow()
			}
		}
	}
	if w := p.bgroup.windows.Load(); w == 0 {
		t.Fatal("no barrier windows")
	}
	t.Logf("seed %d: %d events, %d router stops, up to %d flushes in one stop", seed, ver.Load(), p.bgroup.windows.Load(), p.bgroup.maxShared.Load())
}
