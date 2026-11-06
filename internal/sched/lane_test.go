package sched

import (
	"context"
	"testing"
	"time"

	"strand/internal/model"
)

// invariant: CO-1
// Làn tuần tự (model.Txn.Lane, review F12): Txn làn không phụ thuộc nhau qua writeset nhưng ra khỏi
// làn đúng thứ tự Admit; đầu làn chưa sẵn sàng thì không Txn làn nào sau nó được trả; chuỗi đã sẵn sàng
// ra trong một lần (group commit); phụ thuộc với Txn thường giữ nguyên ở cả hai chiều.
func TestLaneOrderBatchingAndDependencies(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(DefaultConfig())
	tbl := model.TableID{Schema: "app", Table: "accounts"}
	other := model.TableID{Schema: "app", Table: "items"}
	row := func(tb model.TableID, k int) model.RowChange {
		return model.RowChange{Table: tb, Op: model.OpUpdate, Key: []model.Value{k}}
	}
	n0 := &model.Txn{Rows: []model.RowChange{row(other, 1)}}
	l1 := &model.Txn{Lane: true, Rows: []model.RowChange{row(other, 1), row(tbl, 10)}} // chờ n0 (khóa items/1)
	l2 := &model.Txn{Lane: true, Rows: []model.RowChange{row(tbl, 20)}}                // sẵn sàng ngay
	l3 := &model.Txn{Lane: true, Rows: []model.RowChange{row(tbl, 10)}}                // chung khóa với l1: không chờ l1 qua DAG
	n4 := &model.Txn{Rows: []model.RowChange{row(tbl, 20)}}                            // Txn thường sau l2, chung khóa: chờ l2
	for _, x := range []*model.Txn{n0, l1, l2, l3, n4} {
		if err := s.Admit(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	if got := <-s.Ready(); got != n0 {
		t.Fatalf("first ready txn = seq %d, want n0", got.Seq())
	}

	// Đầu làn (l1) chờ n0: không được trả l2/l3 dù chúng đã sẵn sàng.
	tctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	if b := s.NextLaneBatch(tctx, 256, 0); b != nil {
		t.Fatalf("lane returned %d txns while its head waits on n0", len(b))
	}
	cancel()

	s.Done(n0.Seq(), nil)
	b := s.NextLaneBatch(ctx, 256, 0)
	if len(b) != 3 || b[0] != l1 || b[1] != l2 || b[2] != l3 {
		seqs := []uint64{}
		for _, x := range b {
			seqs = append(seqs, x.Seq())
		}
		t.Fatalf("lane batch = %v, want [l1 l2 l3] = [%d %d %d]", seqs, l1.Seq(), l2.Seq(), l3.Seq())
	}
	// n4 (thường) phụ thuộc l2: chưa ready cho tới khi l2 Done.
	select {
	case x := <-s.Ready():
		t.Fatalf("n4 (seq %d) ready before the lane txn it depends on", x.Seq())
	case <-time.After(100 * time.Millisecond):
	}
	for _, x := range b {
		s.Done(x.Seq(), nil)
	}
	select {
	case x := <-s.Ready():
		if x != n4 {
			t.Fatalf("ready = seq %d, want n4", x.Seq())
		}
	case <-time.After(time.Second):
		t.Fatal("n4 not ready after the lane batch was done")
	}
	s.Done(n4.Seq(), nil)
	if s.Lwd() != n4.Seq() {
		t.Fatalf("lwd = %d, want %d", s.Lwd(), n4.Seq())
	}
}

// Giới hạn batch của làn: maxTxns / maxRows cắt chuỗi, phần còn lại ra ở lần sau theo đúng thứ tự.
func TestLaneBatchLimits(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(DefaultConfig())
	tbl := model.TableID{Schema: "app", Table: "accounts"}
	var all []*model.Txn
	for i := 0; i < 5; i++ {
		x := &model.Txn{Lane: true, Rows: []model.RowChange{{Table: tbl, Op: model.OpUpdate, Key: []model.Value{i}}}}
		if err := s.Admit(ctx, x); err != nil {
			t.Fatal(err)
		}
		all = append(all, x)
	}
	b1 := s.NextLaneBatch(ctx, 2, 0)
	b2 := s.NextLaneBatch(ctx, 256, 2)
	b3 := s.NextLaneBatch(ctx, 256, 0)
	if len(b1) != 2 || len(b2) != 2 || len(b3) != 1 || b1[0] != all[0] || b2[0] != all[2] || b3[0] != all[4] {
		t.Fatalf("batches %d/%d/%d not in admit order", len(b1), len(b2), len(b3))
	}
	if s.LanePending() != 0 {
		t.Fatalf("lane pending = %d", s.LanePending())
	}
}
