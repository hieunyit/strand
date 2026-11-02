package assemble_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"reflect"
	"testing"
	"time"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/reader"
)

func gtid(n string) reader.Event {
	return reader.Event{Type: reader.EventGTID, Pos: model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:" + n}}
}

// invariant: CP-1
// A spilled Txn used to keep only (op, int64 key): every column value was lost,
// so the writer had nothing to apply. All fields must survive the spill.
func TestCP1_SpilledTxnKeepsAllColumns(t *testing.T) {
	asm := assemble.New(assemble.WithMaxInMemRows(2), assemble.WithSpillDir(t.TempDir()))
	asm.Registry().RegisterTable("app", "t", []string{"code"}, []string{"id", "code", "note", "at"})

	ts := time.Date(2026, 11, 2, 10, 0, 0, 123, time.UTC)
	var rows [][]any
	for i := 0; i < 5; i++ {
		rows = append(rows, []any{int64(i), "k" + string(rune('a'+i)), []byte{0, 1, byte(i)}, ts})
	}
	mustPush(t, asm, gtid("1"))
	mustPush(t, asm, reader.Event{Type: reader.EventQuery, Query: "BEGIN"})
	mustPush(t, asm, reader.Event{Type: reader.EventRows, Schema: "app", Table: "t", Op: model.OpInsert, Rows: rows})
	mustPush(t, asm, reader.Event{Type: reader.EventRows, Schema: "app", Table: "t", Op: model.OpUpdate,
		Rows: [][]any{{int64(0), "ka", nil, ts}, {int64(0), "kz", []byte("x"), ts}}})
	txn, err := asm.Push(reader.Event{Type: reader.EventXID})
	if err != nil || txn == nil {
		t.Fatalf("txn not finished: %v", err)
	}
	if !txn.IsSpilled() {
		t.Fatalf("expected spilled txn")
	}
	if want := []model.TableID{{Schema: "app", Table: "t"}}; !reflect.DeepEqual(txn.SpilledTables, want) {
		t.Fatalf("SpilledTables = %v, want %v", txn.SpilledTables, want)
	}

	var got []*model.RowChange
	if err := txn.ForEachRow(func(rc *model.RowChange) error { got = append(got, rc); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 6 {
		t.Fatalf("read %d rows, want 6", len(got))
	}
	r := got[2]
	if r.Key[0] != "kc" || len(r.Cols) != 4 || r.Cols[1].Name != "code" ||
		!reflect.DeepEqual(r.Cols[2].Value, []byte{0, 1, 2}) || !r.Cols[3].Value.(time.Time).Equal(ts) {
		t.Fatalf("spilled row lost data: %+v", r)
	}
	u := got[5]
	if u.Op != model.OpUpdate || u.Key[0] != "kz" || len(u.OldKey) != 1 || u.OldKey[0] != "ka" {
		t.Fatalf("spilled PK-changing update wrong: %+v", u)
	}
}

// invariant: CP-1, CP-2
// A DDL transaction (GTID + QUERY, no BEGIN/XID) must be emitted as its own Txn.
// It used to be merged into the next Txn, so its GTID was never recorded.
func TestCP1_DDLTxnEmittedSeparately(t *testing.T) {
	asm := assemble.New()
	mustPush(t, asm, gtid("1"))
	ddl, err := asm.Push(reader.Event{Type: reader.EventQuery, Query: "ALTER TABLE t ADD c INT", Schema: "app"})
	if err != nil {
		t.Fatal(err)
	}
	if ddl == nil || ddl.SourcePos.GTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:1" || ddl.DDL == "" {
		t.Fatalf("DDL txn not emitted on its own: %+v", ddl)
	}
	mustPush(t, asm, gtid("2"))
	mustPush(t, asm, reader.Event{Type: reader.EventQuery, Query: "BEGIN"})
	mustPush(t, asm, reader.Event{Type: reader.EventRows, Schema: "app", Table: "t", Op: model.OpInsert, Rows: [][]any{{int64(1)}}})
	dml, err := asm.Push(reader.Event{Type: reader.EventXID})
	if err != nil || dml == nil || dml.SourcePos.GTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:2" || dml.DDL != "" {
		t.Fatalf("DML txn wrong: %+v err=%v", dml, err)
	}
}

// A new GTID while the previous Txn is still open means events were lost: fail loudly.
func TestCP1_GTIDInsideOpenTxnIsError(t *testing.T) {
	asm := assemble.New()
	mustPush(t, asm, gtid("1"))
	mustPush(t, asm, reader.Event{Type: reader.EventQuery, Query: "BEGIN"})
	mustPush(t, asm, reader.Event{Type: reader.EventRows, Schema: "app", Table: "t", Op: model.OpInsert, Rows: [][]any{{int64(1)}}})
	if _, err := asm.Push(gtid("2")); err == nil {
		t.Fatalf("expected error: GTID 2 arrived before Txn 1 terminated")
	}
}

// Column names and PK must come from the TABLE_MAP metadata (binlog_row_metadata=FULL),
// not be guessed as col_N with PK = first column.
func TestTableMapMetadataDefinesSchema(t *testing.T) {
	asm := assemble.New()
	mustPush(t, asm, gtid("1"))
	mustPush(t, asm, reader.Event{Type: reader.EventQuery, Query: "BEGIN"})
	mustPush(t, asm, reader.Event{Type: reader.EventTableMap, Schema: "app", Table: "users",
		Columns: []string{"name", "user_id"}, PKColumns: []string{"user_id"}})
	mustPush(t, asm, reader.Event{Type: reader.EventRows, Schema: "app", Table: "users", Op: model.OpInsert,
		Rows: [][]any{{"alice", int64(42)}}})
	txn, err := asm.Push(reader.Event{Type: reader.EventXID})
	if err != nil {
		t.Fatal(err)
	}
	rc := txn.Rows[0]
	if rc.Key[0] != int64(42) || rc.Cols[0].Name != "name" || rc.Cols[1].Name != "user_id" {
		t.Fatalf("schema from TABLE_MAP ignored: %+v", rc)
	}
}

func mustPush(t *testing.T, a *assemble.Assembler, ev reader.Event) {
	t.Helper()
	if _, err := a.Push(ev); err != nil {
		t.Fatalf("push %s: %v", ev.Type, err)
	}
}
