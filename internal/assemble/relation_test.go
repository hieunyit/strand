package assemble

import (
	"testing"

	"strand/internal/model"
	"strand/internal/reader"
)

func TestAssembler_TableMapSchemaChangedSetsDDL(t *testing.T) {
	asm := New()
	asm.Registry().RegisterTable("public", "t", []string{"id"}, []string{"id", "val"})

	pos := model.Pos{Engine: model.EnginePG, LSN: 200}
	events := []reader.Event{
		{Type: reader.EventQuery, Query: "BEGIN", Pos: pos},
		{
			Type:          reader.EventTableMap,
			Schema:        "public",
			Table:         "t",
			Columns:       []string{"id", "val", "extra"},
			PKColumns:     []string{"id"},
			Pos:           pos,
			SchemaChanged: true,
		},
		{
			Type:   reader.EventRows,
			Op:     model.OpInsert,
			Schema: "public",
			Table:  "t",
			Pos:    pos,
			Rows:   [][]any{{int64(1), "hello", int64(42)}},
		},
		{Type: reader.EventQuery, Query: "COMMIT", Pos: pos},
	}

	var finishedTxn *model.Txn
	for _, ev := range events {
		txn, err := asm.Push(ev)
		if err != nil {
			t.Fatalf("push failed: %v", err)
		}
		if txn != nil {
			finishedTxn = txn
		}
	}

	if finishedTxn == nil {
		t.Fatal("expected finished txn at COMMIT")
	}
	if finishedTxn.DDL == "" {
		t.Fatal("expected txn.DDL to be set when SchemaChanged=true")
	}
	if finishedTxn.DDL != "ALTER TABLE public.t" {
		t.Fatalf("expected DDL 'ALTER TABLE public.t', got %q", finishedTxn.DDL)
	}
	// Verify rows were parsed with the 3 columns (id, val, extra)
	if len(finishedTxn.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(finishedTxn.Rows))
	}
	rc := finishedTxn.Rows[0]
	cols := map[string]any{}
	for _, c := range rc.Cols {
		cols[c.Name] = c.Value
	}
	if cols["extra"] != int64(42) {
		t.Fatalf("expected extra=42, got %v in %v", cols["extra"], cols)
	}
}
