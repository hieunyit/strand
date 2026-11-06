package pipeline

import (
	"context"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/sched"
)

// G2-11 (e2e HotRow): row nóng 1.000 update/giây là MỘT chuỗi Txn phụ thuộc. Trước đây worker lấy Txn
// đầu chuỗi, chờ max_wait (50 ms) gom thêm — không Txn nào tới được vì Txn kế tiếp chỉ sẵn sàng sau khi
// Txn này Done — rồi commit một mình: ~20 Txn/giây cho cả pipeline (e2e: tập đã apply tiến ~2.300 GTID
// trong 2 phút chờ của verify). Mô phỏng vòng worker relaxed (gatherBatch + Done) với scheduler thật:
// 600 Txn cùng một khóa phải xong trong vài batch, không phải 600 batch × 50 ms.
func TestWorkerLoop_HotRowChainIsGroupCommitted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s := sched.NewScheduler(sched.DefaultConfig())
	tbl := model.TableID{Schema: "app", Table: "accounts"}
	const n = 600
	for i := 0; i < n; i++ {
		x := &model.Txn{Rows: []model.RowChange{{Table: tbl, Op: model.OpUpdate, Key: []model.Value{int64(1)},
			Cols: []model.ColValue{{Name: "id", Value: int64(1)}, {Name: "balance", Value: int64(i)}}}}}
		if err := s.Admit(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	lim := batchLimits{maxRows: 2000, maxBytes: 4 << 20, maxWait: 50 * time.Millisecond, chain: s.TakeChained}
	start := time.Now()
	var batches, applied int
	var last uint64
	for applied < n {
		var first *model.Txn
		select {
		case first = <-s.Ready():
		case <-ctx.Done():
			t.Fatalf("stalled after %d txns in %d batches (%v)", applied, batches, time.Since(start))
		}
		batch, _, _ := gatherBatch(ctx, first, s.Ready(), lim)
		batches++
		for _, x := range batch {
			if x.Seq() <= last {
				t.Fatalf("txn seq %d applied after seq %d: chain order broken", x.Seq(), last)
			}
			last = x.Seq()
			s.Done(x.Seq(), nil)
		}
		applied += len(batch)
	}
	// 600 Txn, tối đa 256 Txn mỗi batch → 3 batch; chừa biên cho máy chậm.
	if batches > 6 || time.Since(start) > 2*time.Second {
		t.Fatalf("hot-row chain of %d txns took %d batches in %v (one commit + max_wait per txn)", n, batches, time.Since(start))
	}
	if s.Lwd() != uint64(n) {
		t.Fatalf("lwd = %d, want %d", s.Lwd(), n)
	}
}

// Strict không gom chuỗi (thứ tự commit theo seq do orderer bảo đảm, batch chỉ gồm seq liên tiếp).
func TestGatherBatch_StrictIgnoresChain(t *testing.T) {
	called := false
	lim := batchLimits{maxRows: 1000, maxBytes: 1 << 20, strict: true,
		chain: func([]*model.Txn, func(*model.Txn) bool) []*model.Txn { called = true; return nil }}
	b, _, _ := gatherBatch(context.Background(), btxn(1, 1, 1), feed(), lim)
	if len(b) != 1 || called {
		t.Fatalf("strict batch = %d txns, chain called = %v", len(b), called)
	}
}
