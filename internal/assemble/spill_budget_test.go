package assemble_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/reader"
)

func TestSpillByteBudget(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sizes []int
	}{
		{"one_large_row", []int{64 << 20}},
		{"several_wide_rows", []int{33 << 20, 33 << 20}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sb := assemble.NewSpillBuffer(10000, dir)
			rows := make([]model.RowChange, 0, len(tc.sizes))
			parent := model.TableID{Schema: "app", Table: "parent"}
			for i, size := range tc.sizes {
				row := model.RowChange{Table: model.TableID{Schema: "app", Table: "child"}, Op: model.OpInsert,
					Key: []model.Value{int64(i)}, Cols: []model.ColValue{{Name: "payload", Value: bytes.Repeat([]byte{byte(i + 1)}, size)}},
					ForeignKeys: []model.ForeignKey{{ParentTable: parent, ParentKey: []model.Value{int64(i)}}}}
				rows = append(rows, row)
				if err := sb.Append(row); err != nil {
					t.Fatal(err)
				}
			}
			if !sb.IsSpilled() {
				t.Fatal("wide transaction remained in memory beyond 64 MiB")
			}
			var txn model.Txn
			if err := sb.Finish(&txn); err != nil {
				t.Fatal(err)
			}
			if len(txn.Rows) != 0 || txn.RowCount != int64(len(rows)) {
				t.Fatalf("unexpected transaction: %d in-memory rows, count %d", len(txn.Rows), txn.RowCount)
			}
			if !reflect.DeepEqual(txn.SpilledTables, []model.TableID{rows[0].Table, parent}) {
				t.Fatalf("lost table dependencies: %v", txn.SpilledTables)
			}
			it, err := assemble.OpenRowIterator(&txn)
			if err != nil {
				t.Fatal(err)
			}
			for i := range rows {
				got, err := it.Next()
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(*got, rows[i]) {
					t.Fatalf("row %d changed during spill", i)
				}
			}
			if _, err := it.Next(); err != io.EOF {
				t.Fatalf("expected EOF, got %v", err)
			}
			if err := it.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(txn.SpillPath); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(txn.SpillPath); !os.IsNotExist(err) {
				t.Fatalf("spill file remains: %v", err)
			}
		})
	}
}

func TestSpillByteBudgetCreateError(t *testing.T) {
	sb := assemble.NewSpillBuffer(10000, filepath.Join(t.TempDir(), "missing"))
	err := sb.Append(model.RowChange{Cols: []model.ColValue{{Name: "payload", Value: bytes.Repeat([]byte("x"), 64<<20)}}})
	if err == nil {
		t.Fatal("expected spill creation error")
	}
}

func TestSpillByteBudgetCountsKeyPayload(t *testing.T) {
	dir := t.TempDir()
	sb := assemble.NewSpillBuffer(10000, dir)
	row := model.RowChange{Table: model.TableID{Schema: "app", Table: "t"}, Op: model.OpUpdate,
		OldKey:      []model.Value{bytes.Repeat([]byte("k"), 22<<20)},
		UniqueKeys:  []model.UniqueKey{{Name: "u", Key: []model.Value{bytes.Repeat([]byte("u"), 22<<20)}}},
		ForeignKeys: []model.ForeignKey{{ParentTable: model.TableID{Schema: "app", Table: "parent"}, ParentKey: []model.Value{bytes.Repeat([]byte("f"), 22<<20)}}},
	}
	if err := sb.Append(row); err != nil {
		t.Fatal(err)
	}
	if !sb.IsSpilled() {
		t.Fatal("key payload exceeded byte budget without spilling")
	}
	var txn model.Txn
	if err := sb.Finish(&txn); err != nil {
		t.Fatal(err)
	}
	it, err := assemble.OpenRowIterator(&txn)
	if err != nil {
		t.Fatal(err)
	}
	got, err := it.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.OldKey, row.OldKey) || !reflect.DeepEqual(got.UniqueKeys, row.UniqueKeys) || !reflect.DeepEqual(got.ForeignKeys, row.ForeignKeys) {
		t.Fatal("key payload changed during spill")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(txn.SpillPath); err != nil {
		t.Fatal(err)
	}
}

func TestSpillAbandonedTransactionCleanup(t *testing.T) {
	dir := t.TempDir()
	a := assemble.New(assemble.WithMaxInMemRows(1), assemble.WithSpillDir(dir))
	mustPush(t, a, gtid("123"))
	mustPush(t, a, reader.Event{Type: reader.EventQuery, Query: "BEGIN"})
	mustPush(t, a, reader.Event{Type: reader.EventRows, Schema: "app", Table: "t", Op: model.OpInsert, Rows: [][]any{{int64(1)}, {int64(2)}}})
	files, err := filepath.Glob(filepath.Join(dir, "strand-txn-*.spill"))
	if err != nil || len(files) != 1 {
		t.Fatalf("expected spill file: %v %v", files, err)
	}
	in := make(chan reader.Event)
	close(in)
	out, errs := a.Process(context.Background(), in)
	for range out {
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	files, err = filepath.Glob(filepath.Join(dir, "strand-txn-*.spill"))
	if err != nil || len(files) != 0 {
		t.Fatalf("abandoned spill remains: %v %v", files, err)
	}
}
