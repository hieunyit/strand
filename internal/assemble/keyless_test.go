package assemble

import (
	"testing"

	"strand/internal/model"
	"strand/internal/reader"
)

// Bảng keyless: TABLE_MAP không có PK nhưng pipeline khai báo keyless → khóa = mọi cột (ảnh đầy đủ), UPDATE mang
// ảnh trước ở OldKey, và mọi Txn chạm bảng xung đột writeset (tuần tự hóa theo thứ tự commit) dù giá trị khác nhau.
func TestKeylessTableKeyIsFullRowAndSerializes(t *testing.T) {
	kl := model.TableID{Schema: "app", Table: "kl"}
	other := model.TableID{Schema: "app", Table: "ok"}
	a := New(
		WithKeyMetadata(map[model.TableID]KeyMeta{
			kl:    {Unique: []UniqueIndex{{Name: KeylessIndexName, Columns: []string{"a", "b"}, Prefix: true}}},
			other: {},
		}),
		WithKeylessFunc(func(id model.TableID) bool { return id == kl }),
	)
	run := func(gtid, table string, op model.Op, rows [][]any) *model.Txn {
		t.Helper()
		pos := model.Pos{Engine: model.EngineMySQL, GTID: gtid}
		evs := []reader.Event{
			{Type: reader.EventGTID, Pos: pos},
			{Type: reader.EventQuery, Query: "BEGIN", Pos: pos},
			{Type: reader.EventTableMap, Pos: pos, Schema: "app", Table: table, Columns: []string{"a", "b"}}, // no PKColumns
			{Type: reader.EventRows, Pos: pos, Schema: "app", Table: table, Op: op, Rows: rows},
			{Type: reader.EventXID, Pos: pos, XID: 1},
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
		if txn == nil {
			t.Fatal("no txn")
		}
		return txn
	}
	shares := func(x, y *model.Txn) bool {
		wy := y.WritesetByTable()
		for tbl, hs := range x.WritesetByTable() {
			set := map[uint64]bool{}
			for _, h := range hs {
				set[h] = true
			}
			for _, h := range wy[tbl] {
				if set[h] {
					return true
				}
			}
		}
		return false
	}
	const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562:"
	upd := run(u+"1", "kl", model.OpUpdate, [][]any{{int64(1), "x"}, {int64(1), "y"}})
	ins := run(u+"2", "kl", model.OpInsert, [][]any{{int64(7), "zzz"}})
	del := run(u+"3", "kl", model.OpDelete, [][]any{{int64(9), nil}})

	var rcs []model.RowChange
	if err := upd.ForEachRow(func(r *model.RowChange) error { rcs = append(rcs, *r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(rcs) != 1 || len(rcs[0].Key) != 2 || len(rcs[0].OldKey) != 2 {
		t.Fatalf("keyless UPDATE must carry the full after-image as Key and the before-image as OldKey: %+v", rcs)
	}
	if rcs[0].OldKey[1] != "x" || rcs[0].Key[1] != "y" {
		t.Fatalf("before/after images swapped: %+v", rcs[0])
	}
	rcs = nil
	if err := del.ForEachRow(func(r *model.RowChange) error { rcs = append(rcs, *r); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(rcs[0].Key) != 2 || rcs[0].Key[1] != nil {
		t.Fatalf("keyless DELETE key must be the full before-image incl. NULLs: %+v", rcs[0])
	}
	if !shares(upd, ins) || !shares(ins, del) {
		t.Fatal("all Txns touching a keyless table must conflict (serialized in commit order)")
	}
	o1 := run(u+"4", "ok", model.OpInsert, [][]any{{int64(1), "p"}})
	if shares(upd, o1) {
		t.Fatal("other tables must stay parallel")
	}
}
