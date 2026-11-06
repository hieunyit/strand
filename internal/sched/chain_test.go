package sched

import (
	"context"
	"testing"

	"strand/internal/model"
)

func chainRow(tb model.TableID, k int) model.RowChange {
	return model.RowChange{Table: tb, Op: model.OpUpdate, Key: []model.Value{k}}
}

// invariant: CO-1
// Row nóng (G2-11): chuỗi Txn cùng khóa được lấy cùng batch với Txn đầu chuỗi, đúng thứ tự phụ thuộc.
// Txn còn phụ thuộc một Txn ngoài batch, và Txn làn, thì không bị lấy.
func TestTakeChained_HotRowChainJoinsBatch(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(DefaultConfig())
	tbl := model.TableID{Schema: "app", Table: "accounts"}
	h1 := &model.Txn{Rows: []model.RowChange{chainRow(tbl, 1)}}
	o2 := &model.Txn{Rows: []model.RowChange{chainRow(tbl, 2)}}                   // độc lập
	h3 := &model.Txn{Rows: []model.RowChange{chainRow(tbl, 1)}}                   // chờ h1
	x4 := &model.Txn{Rows: []model.RowChange{chainRow(tbl, 1), chainRow(tbl, 2)}} // chờ h3 và o2 (o2 ngoài batch)
	h5 := &model.Txn{Rows: []model.RowChange{chainRow(tbl, 3)}}                   // độc lập
	h6 := &model.Txn{Rows: []model.RowChange{chainRow(tbl, 3), chainRow(tbl, 9)}} // chờ h5
	for _, x := range []*model.Txn{h1, o2, h3, x4, h5, h6} {
		if err := s.Admit(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	ready := map[*model.Txn]bool{}
	for i := 0; i < 3; i++ {
		ready[<-s.Ready()] = true
	}
	if !ready[h1] || !ready[o2] || !ready[h5] {
		t.Fatalf("ready set must be h1, o2, h5")
	}
	all := func(*model.Txn) bool { return true }
	got := s.TakeChained([]*model.Txn{h1}, all)
	if len(got) != 1 || got[0] != h3 {
		t.Fatalf("chain of h1 = %v, want [h3] (x4 still waits on o2 outside the batch)", seqs(got))
	}
	// Nhận h5 vào batch: h6 (chờ h5) cũng được lấy; accept từ chối thì dừng.
	if got := s.TakeChained([]*model.Txn{h5}, func(*model.Txn) bool { return false }); len(got) != 0 {
		t.Fatalf("accept=false must take nothing, got %v", seqs(got))
	}
	if got := s.TakeChained([]*model.Txn{h5}, all); len(got) != 1 || got[0] != h6 {
		t.Fatalf("chain of h5 = %v, want [h6]", seqs(got))
	}
	// Done theo thứ tự batch: không Txn đã lấy nào được phát lại; x4 sẵn sàng khi h3 và o2 xong.
	for _, x := range []*model.Txn{h1, h3, h5, h6} {
		s.Done(x.Seq(), nil)
	}
	select {
	case r := <-s.Ready():
		t.Fatalf("seq %d emitted while x4 still waits on o2", r.Seq())
	default:
	}
	s.Done(o2.Seq(), nil)
	if r := <-s.Ready(); r != x4 {
		t.Fatalf("ready = seq %d, want x4", r.Seq())
	}
	s.Done(x4.Seq(), nil)
	if s.Lwd() != h6.Seq() || len(s.inDegree) != 0 || len(s.pendingTxns) != 0 || s.pendingBytes != 0 {
		t.Fatalf("after all Done: lwd=%d inDegree=%v pending=%d bytes=%d", s.Lwd(), s.inDegree, len(s.pendingTxns), s.pendingBytes)
	}
}

// Chuỗi dài (row nóng): một lần TakeChained lấy hết chuỗi đã Admit, và Txn làn không bao giờ bị lấy.
func TestTakeChained_LongChainAndLane(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(DefaultConfig())
	tbl := model.TableID{Schema: "app", Table: "accounts"}
	var txns []*model.Txn
	for i := 0; i < 500; i++ {
		x := &model.Txn{Rows: []model.RowChange{chainRow(tbl, 1)}}
		if err := s.Admit(ctx, x); err != nil {
			t.Fatal(err)
		}
		txns = append(txns, x)
	}
	first := <-s.Ready()
	if first != txns[0] {
		t.Fatal("head of the chain must be ready first")
	}
	got := s.TakeChained([]*model.Txn{first}, func(*model.Txn) bool { return true })
	if len(got) != 499 {
		t.Fatalf("took %d of the 499 chained txns", len(got))
	}
	for i, x := range got {
		if x != txns[i+1] {
			t.Fatalf("chain order broken at %d", i)
		}
	}

	lane := NewScheduler(DefaultConfig())
	n1 := &model.Txn{Rows: []model.RowChange{chainRow(tbl, 1)}}
	l2 := &model.Txn{Lane: true, Rows: []model.RowChange{chainRow(tbl, 1)}}
	for _, x := range []*model.Txn{n1, l2} {
		if err := lane.Admit(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	<-lane.Ready()
	if got := lane.TakeChained([]*model.Txn{n1}, func(*model.Txn) bool { return true }); len(got) != 0 {
		t.Fatalf("lane txn taken by a regular worker: %v", seqs(got))
	}
}

func seqs(ts []*model.Txn) []uint64 {
	var out []uint64
	for _, x := range ts {
		out = append(out, x.Seq())
	}
	return out
}
