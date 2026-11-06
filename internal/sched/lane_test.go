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
	n4 := &model.Txn{Rows: []model.RowChange{row(tbl, 20)}}                            // Txn thường sau l2, chung khóa: lây làn
	n5 := &model.Txn{Rows: []model.RowChange{row(other, 2)}}                           // Txn thường độc lập: worker thường
	for _, x := range []*model.Txn{n0, l1, l2, l3, n4, n5} {
		if err := s.Admit(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		got := <-s.Ready()
		if got != n0 && got != n5 {
			t.Fatalf("ready txn = seq %d, want n0 or n5", got.Seq())
		}
		if got == n5 {
			s.Done(n5.Seq(), nil)
		}
	}

	// Đầu làn (l1) chờ n0: không được trả l2/l3 dù chúng đã sẵn sàng.
	tctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	if b := s.NextLaneBatch(tctx, 256, 0); b != nil {
		t.Fatalf("lane returned %d txns while its head waits on n0", len(b))
	}
	cancel()

	s.Done(n0.Seq(), nil)
	b := s.NextLaneBatch(ctx, 256, 0)
	want := []*model.Txn{l1, l2, l3, n4}
	if len(b) != len(want) || b[0] != l1 || b[1] != l2 || b[2] != l3 || b[3] != n4 || !n4.Lane {
		seqs := []uint64{}
		for _, x := range b {
			seqs = append(seqs, x.Seq())
		}
		t.Fatalf("lane batch = %v, want [l1 l2 l3 n4] = [%d %d %d %d]", seqs, l1.Seq(), l2.Seq(), l3.Seq(), n4.Seq())
	}
	select {
	case x := <-s.Ready():
		t.Fatalf("seq %d on the normal ready queue, want nothing", x.Seq())
	case <-time.After(50 * time.Millisecond):
	}
	for _, x := range b {
		s.Done(x.Seq(), nil)
	}
	if s.Lwd() != n5.Seq() {
		t.Fatalf("lwd = %d, want %d", s.Lwd(), n5.Seq())
	}
}

// Lây làn (nút thắt snapshot add PG relaxed): chuỗi khóa nóng l1 → n2 → n3 (n2, n3 là Txn thường chỉ
// phụ thuộc Txn làn hoặc Txn đã lây làn) ra cùng MỘT batch làn. Trước đây n2 chờ l1 commit ở làn, n3
// chờ n2 ở worker thường — mỗi bước một commit.
func TestLaneAbsorbsDependentChain(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(DefaultConfig())
	acc := model.TableID{Schema: "app", Table: "accounts"}
	items := model.TableID{Schema: "app", Table: "items"}
	l1 := &model.Txn{Lane: true, Rows: []model.RowChange{
		{Table: acc, Op: model.OpUpdate, Key: []model.Value{1}}, {Table: items, Op: model.OpUpdate, Key: []model.Value{7}}}}
	n2 := &model.Txn{Rows: []model.RowChange{
		{Table: items, Op: model.OpUpdate, Key: []model.Value{7}}, {Table: items, Op: model.OpUpdate, Key: []model.Value{8}}}}
	n3 := &model.Txn{Rows: []model.RowChange{{Table: items, Op: model.OpUpdate, Key: []model.Value{8}}}}
	for _, x := range []*model.Txn{l1, n2, n3} {
		if err := s.Admit(ctx, x); err != nil {
			t.Fatal(err)
		}
	}
	b := s.NextLaneBatch(ctx, 256, 0)
	if len(b) != 3 || b[0] != l1 || b[1] != n2 || b[2] != n3 {
		t.Fatalf("lane batch has %d txns, want the whole chain [l1 n2 n3] in admit order", len(b))
	}
	select {
	case x := <-s.Ready():
		t.Fatalf("seq %d on the normal ready queue", x.Seq())
	default:
	}
	for _, x := range b {
		s.Done(x.Seq(), nil)
	}
	if s.Lwd() != n3.Seq() {
		t.Fatalf("lwd = %d, want %d", s.Lwd(), n3.Seq())
	}
}

// Serial (strict): không lây làn — mọi Txn qua Ready() như cũ.
func TestLaneNoContagionWhenSerial(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Serial = true
	s := NewScheduler(cfg)
	tbl := model.TableID{Schema: "app", Table: "items"}
	l1 := &model.Txn{Lane: true, Rows: []model.RowChange{{Table: tbl, Op: model.OpUpdate, Key: []model.Value{1}}}}
	n2 := &model.Txn{Rows: []model.RowChange{{Table: tbl, Op: model.OpUpdate, Key: []model.Value{1}}}}
	for _, x := range []*model.Txn{l1, n2} {
		if err := s.Admit(context.Background(), x); err != nil {
			t.Fatal(err)
		}
	}
	if n2.Lane {
		t.Fatal("n2 joined the lane under Serial")
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
