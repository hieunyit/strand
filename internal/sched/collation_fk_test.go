package sched

import (
	"context"
	"testing"
	"time"

	"strand/internal/assemble"
	"strand/internal/model"
)

// readyNow trả về Txn sẵn sàng tiếp theo nếu có ngay (không chờ).
func readyNow(s *Scheduler) *model.Txn {
	select {
	case t := <-s.Ready():
		return t
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}

// invariant: CO-1 — review R01. UNIQUE với collation _ci: T1 đổi 'a'→'b', T2 chèn 'A' (DB coi 'A'='a').
// T2 phải chờ T1; nếu chạy trước, ON DUPLICATE KEY UPDATE đè row id=1 và mất row id=2.
func TestCO1_CaseInsensitiveUniqueKeysConflict(t *testing.T) {
	tid := model.TableID{Schema: "app", Table: "ci"}
	cases := []struct{ old, new, other string }{
		{"a", "b", "A"},            // hoa/thường
		{"cafe", "x", "café"},      // dấu (ai)
		{"strasse", "x", "STRAßE"}, // ß = ss (UCA) — và 's' (general_ci)
		{"abc", "x", "abc  "},      // PAD SPACE
		{"ǆ", "x", "Ǆ"},            // chữ ghép titlecase
	}
	for _, c := range cases {
		first := &model.Txn{Rows: []model.RowChange{{Table: tid, Op: model.OpUpdate, Key: []model.Value{int64(1)},
			UniqueKeys: []model.UniqueKey{{Name: "uk_code", Key: []model.Value{c.new}, OldKey: []model.Value{c.old}}}}}}
		second := &model.Txn{Rows: []model.RowChange{{Table: tid, Op: model.OpInsert, Key: []model.Value{int64(2)},
			UniqueKeys: []model.UniqueKey{{Name: "uk_code", Key: []model.Value{c.other}}}}}}
		s := NewScheduler(DefaultConfig())
		ctx := context.Background()
		if err := s.Admit(ctx, first); err != nil {
			t.Fatal(err)
		}
		if err := s.Admit(ctx, second); err != nil {
			t.Fatal(err)
		}
		if got := readyNow(s); got != first {
			t.Fatalf("%q: first ready = %v", c.other, got)
		}
		if got := readyNow(s); got != nil {
			t.Fatalf("%q vs %q: T2 ready before T1 done — missing collation dependency", c.old, c.other)
		}
		s.Done(first.Seq(), nil)
		if got := readyNow(s); got != second {
			t.Fatalf("%q: T2 not released after T1 done", c.other)
		}
		s.Close()
	}
}

// Khóa chuỗi chứa dấu phân cách không được trùng hash với khóa khác (length prefix).
func TestCO1_CompositeKeyWithSeparatorsDoesNotFalselyMatch(t *testing.T) {
	tid := model.TableID{Schema: "app", Table: "t"}
	a := &model.Txn{Rows: []model.RowChange{{Table: tid, Key: []model.Value{"x y", "z"}}}}
	b := &model.Txn{Rows: []model.RowChange{{Table: tid, Key: []model.Value{"x", "y z"}}}}
	wa, wb := ComputeWriteset(a)[tid.String()], ComputeWriteset(b)[tid.String()]
	if wa[0] == wb[0] {
		t.Fatal("different composite keys share a writeset hash")
	}
}

// invariant: CO-1 — review R11. FK của con tham chiếu UNIQUE(code) của cha (không phải PK): sau khi
// phân giải FK theo metadata, row con phải chờ Txn cha chạm cùng giá trị code.
func TestCO1_ForeignKeyToUniqueIndexOrdersChildAfterParent(t *testing.T) {
	p := model.TableID{Schema: "app", Table: "parent"}
	c := model.TableID{Schema: "app", Table: "child"}
	fk, ok := assemble.ResolveForeignKey(
		assemble.ForeignKeyMeta{Columns: []string{"parent_code"}, RefColumns: []string{"code"}, ParentTable: p},
		[]string{"id"}, []assemble.UniqueIndex{{Name: "uk_code", Columns: []string{"code"}}})
	if !ok || fk.ParentIndex != "uk_code" {
		t.Fatalf("resolve = %+v %v", fk, ok)
	}
	first := &model.Txn{Rows: []model.RowChange{{Table: p, Op: model.OpInsert, Key: []model.Value{int64(1)},
		UniqueKeys: []model.UniqueKey{{Name: "uk_code", Key: []model.Value{"ABC"}}}}}}
	child := model.RowChange{Table: c, Op: model.OpInsert, Key: []model.Value{int64(9)}}
	child.AddForeignKeyIndex(fk.ParentTable, fk.ParentIndex, []model.Value{"ABC"})
	second := &model.Txn{Rows: []model.RowChange{child}}
	s := NewScheduler(DefaultConfig())
	defer s.Close()
	ctx := context.Background()
	if err := s.Admit(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := s.Admit(ctx, second); err != nil {
		t.Fatal(err)
	}
	if got := readyNow(s); got != first {
		t.Fatalf("first ready = %v", got)
	}
	if got := readyNow(s); got != nil {
		t.Fatal("child ready before parent done: missing FK-to-UNIQUE dependency")
	}
}

// FK nhiều cột khai báo theo thứ tự khác PK cha: cột con được sắp theo thứ tự PK cha.
func TestResolveForeignKey_ReordersToParentKeyOrder(t *testing.T) {
	fk, ok := assemble.ResolveForeignKey(
		assemble.ForeignKeyMeta{Columns: []string{"cb", "ca"}, RefColumns: []string{"b", "a"}},
		[]string{"a", "b"}, nil)
	if !ok || fk.ParentIndex != "" || fk.Columns[0] != "ca" || fk.Columns[1] != "cb" {
		t.Fatalf("resolve = %+v %v", fk, ok)
	}
}
