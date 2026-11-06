package model

import (
	"reflect"
	"testing"
)

func collectorRows() []RowChange {
	child := TableID{Schema: "app", Table: "child"}
	parent := TableID{Schema: "app", Table: "parent"}
	rows := []RowChange{
		{Table: child, Op: OpInsert, Key: []Value{int64(1)}},
		{Table: child, Op: OpUpdate, Key: []Value{int64(2)}, OldKey: []Value{int64(3)}},
		{Table: child, Op: OpDelete, Key: []Value{"k"}},
	}
	rows[0].AddUniqueKey("uk", []Value{"a"}, []Value{"b"})
	rows[1].AddUniqueKey("uk_prefix", []Value{"\x00prefix-index"})
	rows[1].AddForeignKey(parent, []Value{int64(9)})
	rows[2].AddForeignKeyIndex(parent, "uk_ref", []Value{"r"})
	rows[2].AddForeignKey(child, []Value{int64(1)}) // FK tự tham chiếu
	return rows
}

// invariant: CO-1. Writeset gom khi ghi file tràn trùng khớp writeset của cùng Txn trong RAM (mọi loại
// khóa: PK, OldKey, UNIQUE + OldKey, khóa hằng index prefix, FK theo PK và theo UNIQUE của bảng cha).
func TestCO1_WritesetCollectorMatchesInMemoryWriteset(t *testing.T) {
	rows := collectorRows()
	want := (&Txn{Rows: rows}).WritesetByTable()
	c := NewWritesetCollector(50000, 2000000)
	for i := range rows {
		c.Add(&rows[i])
	}
	got := c.Writeset()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("collector writeset = %v, want %v", got, want)
	}
	sp := &Txn{SpillPath: "/x", SpilledTables: []TableID{rows[0].Table}, SpillWriteset: got}
	if ws := sp.WritesetByTable(); !reflect.DeepEqual(ws, want) {
		t.Fatalf("spilled WritesetByTable = %v, want %v", ws, want)
	}
	n := 0
	for _, k := range want {
		n += len(k)
	}
	if sp.SpillWritesetKeys() != n {
		t.Fatalf("SpillWritesetKeys = %d, want %d", sp.SpillWritesetKeys(), n)
	}
	// Scheduler thêm bảng vào map trả về: không được sửa SpillWriteset của Txn.
	sp.WritesetByTable()["other"] = nil
	if _, ok := sp.SpillWriteset["other"]; ok {
		t.Fatalf("WritesetByTable must return a copy of SpillWriteset")
	}
}

// Trần mỗi bảng: bảng vượt bị bỏ khóa (cấp bảng), bảng khác giữ; trần tổng: bỏ hết.
func TestCO2_WritesetCollectorLimits(t *testing.T) {
	big := TableID{Schema: "app", Table: "big"}
	small := TableID{Schema: "app", Table: "small"}
	c := NewWritesetCollector(10, 100)
	for i := 0; i < 11; i++ {
		c.Add(&RowChange{Table: big, Key: []Value{int64(i)}})
	}
	c.Add(&RowChange{Table: small, Key: []Value{int64(1)}})
	ws := c.Writeset()
	if _, ok := ws["app.big"]; ok {
		t.Fatalf("table over the per-table limit must be dropped: %v", ws)
	}
	if len(ws["app.small"]) != 1 {
		t.Fatalf("table under the limit must keep its keys: %v", ws)
	}
	if c.n != 1 {
		t.Fatalf("retained count = %d, want 1", c.n)
	}

	c = NewWritesetCollector(1000, 20)
	for i := 0; i < 21; i++ {
		c.Add(&RowChange{Table: big, Key: []Value{int64(i)}})
	}
	if ws := c.Writeset(); ws != nil {
		t.Fatalf("over total limit must drop every key, got %v", ws)
	}
}
