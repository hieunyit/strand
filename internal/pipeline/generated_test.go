package pipeline

import (
	"testing"

	"strand/internal/model"
)

func TestDropTargetGeneratedCopiesAndKeepsOrder(t *testing.T) {
	cols := []model.ColValue{{Name: "id", Value: int64(1)}, {Name: "s", Value: int64(2)}, {Name: "a", Value: int64(1)}, {Name: "v", Value: int64(3)}}
	got := dropTargetGenerated(cols, map[string]bool{"s": true, "v": true})
	if len(got) != 2 || got[0].Name != "id" || got[1].Name != "a" {
		t.Fatalf("got %+v", got)
	}
	got[0].Name = "changed"
	if cols[0].Name != "id" {
		t.Fatalf("original slice was modified (shared with range-state buffers)")
	}
	if same := dropTargetGenerated(cols, nil); &same[0] != &cols[0] {
		t.Fatalf("no generated columns: must return input unchanged")
	}
	m := &tableMeta{Columns: []string{"id", "s", "a", "v"}, TargetGenerated: map[string]bool{"s": true, "v": true}}
	if w := writableColumns(m); len(w) != 2 || w[0] != "id" || w[1] != "a" {
		t.Fatalf("writableColumns = %v", w)
	}
	if len(m.Columns) != 4 {
		t.Fatalf("source column list (positional, used by the assembler registry) must stay intact")
	}
}
