package pipeline

import (
	"testing"

	"strand/internal/model"
)

func TestAddedTablesMerge(t *testing.T) {
	f, err := newTableFilter([]string{"shop.customers"}, []string{"shop.secret"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pay := model.TableID{Schema: "shop", Table: "payments"}
	if f.Included(pay) {
		t.Fatal("payments must not be included before merge")
	}
	dropped := mergeAddedTables(f, []string{"shop.payments", "shop.secret", "bad"})
	if !f.Included(pay) {
		t.Fatal("added table must be included after merge")
	}
	if f.Included(model.TableID{Schema: "shop", Table: "secret"}) {
		t.Fatal("exclude must win over added")
	}
	if len(dropped) != 2 || dropped[0] != "shop.secret" || dropped[1] != "bad" {
		t.Fatalf("dropped = %v", dropped)
	}
}

func TestAddedTablesPersistInLocalState(t *testing.T) {
	dir := t.TempDir()
	s, err := openLocalState(dir, "p")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddTable("shop.payments"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddTable("shop.payments"); err != nil { // idempotent
		t.Fatal(err)
	}
	if err := s.SetDone("shop.payments", true); err != nil {
		t.Fatal(err)
	}
	s2, err := openLocalState(dir, "p")
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.AddedTables(); len(got) != 1 || got[0] != "shop.payments" {
		t.Fatalf("added after reopen = %v", got)
	}
	if err := s2.DropAddedTable("shop.payments"); err != nil {
		t.Fatal(err)
	}
	s3, _ := openLocalState(dir, "p")
	if got := s3.AddedTables(); len(got) != 0 {
		t.Fatalf("added after drop = %v", got)
	}
}
