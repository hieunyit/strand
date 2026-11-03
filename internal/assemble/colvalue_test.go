package assemble

import (
	"testing"

	"strand/internal/model"
	"strand/internal/reader"
)

// Reader PG đưa cột TOAST không đổi vào row dưới dạng model.ColValue{Unchanged: true}. Assembler phải
// giữ nguyên cờ trong Cols, lấy giá trị thật cho Key, và không đưa cột vắng mặt vào unique key.
func TestColValueUnchangedPassthrough(t *testing.T) {
	a := New(WithKeyMetadata(map[model.TableID]KeyMeta{
		{Schema: "public", Table: "t"}: {Unique: []UniqueIndex{{Name: "uk_big", Columns: []string{"big"}}}},
	}))
	pos := model.Pos{Engine: model.EnginePG, LSN: 100}
	evs := []reader.Event{
		{Type: reader.EventQuery, Query: "BEGIN", Pos: pos},
		{Type: reader.EventTableMap, Pos: pos, Schema: "public", Table: "t", Columns: []string{"id", "big", "other"}, PKColumns: []string{"id"}},
		{Type: reader.EventRows, Pos: pos, Schema: "public", Table: "t", Op: model.OpUpdate, Rows: [][]any{
			{int64(1), model.ColValue{Name: "big", Unchanged: true}, int64(1)},
			{int64(1), model.ColValue{Name: "big", Unchanged: true}, int64(2)},
		}},
		{Type: reader.EventQuery, Query: "COMMIT", Pos: pos},
	}
	var txn *model.Txn
	for _, ev := range evs {
		got, err := a.Push(ev)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			txn = got
		}
	}
	if txn == nil || len(txn.Rows) != 1 {
		t.Fatalf("phải ra 1 Txn 1 row: %+v", txn)
	}
	rc := txn.Rows[0]
	if len(rc.Key) != 1 || rc.Key[0] != int64(1) || rc.OldKey != nil {
		t.Fatalf("Key sai: %+v", rc)
	}
	if rc.Cols[1] != (model.ColValue{Name: "big", Unchanged: true}) {
		t.Fatalf("cờ Unchanged bị mất: %+v", rc.Cols[1])
	}
	if rc.Cols[2] != (model.ColValue{Name: "other", Value: int64(2)}) {
		t.Fatalf("cột thường sai: %+v", rc.Cols[2])
	}
	if len(rc.UniqueKeys) != 0 {
		t.Fatalf("cột vắng mặt không được thành unique key: %+v", rc.UniqueKeys)
	}
}
