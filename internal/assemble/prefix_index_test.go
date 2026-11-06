package assemble

import (
	"testing"

	"strand/internal/model"
	"strand/internal/reader"
)

// review N01: UNIQUE KEY uk(code(3)) — hai giá trị khác nhau ('xyz1' cũ 'abc1' vs 'abc2') trùng prefix
// ở target. Writeset phải làm mọi Txn chạm index prefix xung đột (khóa hằng theo bảng+index), nếu không
// T2 (INSERT 'abc2') có thể apply trước T1 (UPDATE 'abc1'→'xyz1') và ODKU đè lên row 1.
func TestPrefixUniqueIndexWritesetConflicts(t *testing.T) {
	tid := model.TableID{Schema: "app", Table: "t"}
	other := model.TableID{Schema: "app", Table: "u"}
	a := New(WithKeyMetadata(map[model.TableID]KeyMeta{
		tid:   {Unique: []UniqueIndex{{Name: "uk", Columns: []string{"code"}, Prefix: true}}},
		other: {Unique: []UniqueIndex{{Name: "uk", Columns: []string{"code"}}}},
	}))
	run := func(gtid string, table string, op model.Op, rows [][]any) *model.Txn {
		t.Helper()
		pos := model.Pos{Engine: model.EngineMySQL, GTID: gtid}
		evs := []reader.Event{
			{Type: reader.EventGTID, Pos: pos},
			{Type: reader.EventQuery, Query: "BEGIN", Pos: pos},
			{Type: reader.EventTableMap, Pos: pos, Schema: "app", Table: table, Columns: []string{"id", "code"}, PKColumns: []string{"id"}},
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
			t.Fatal("không ra Txn")
		}
		return txn
	}
	shares := func(x, y *model.Txn) bool {
		wx, wy := x.WritesetByTable(), y.WritesetByTable()
		for tbl, hs := range wx {
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
	t1 := run(u+"1", "t", model.OpUpdate, [][]any{{int64(1), "abc1"}, {int64(1), "xyz1"}})
	t2 := run(u+"2", "t", model.OpInsert, [][]any{{int64(2), "abc2"}})
	t3 := run(u+"3", "t", model.OpDelete, [][]any{{int64(5), "qqq9"}})
	if !shares(t1, t2) {
		t.Fatalf("UPDATE và INSERT cùng index prefix phải xung đột writeset")
	}
	if !shares(t2, t3) {
		t.Fatalf("DELETE (before-image) cùng index prefix phải xung đột writeset")
	}
	// Bảng không có index prefix: giá trị khác nhau vẫn song song được.
	o1 := run(u+"4", "u", model.OpInsert, [][]any{{int64(1), "abc1"}})
	o2 := run(u+"5", "u", model.OpInsert, [][]any{{int64(2), "abc2"}})
	if shares(o1, o2) {
		t.Fatalf("index không prefix với giá trị khác nhau không được xung đột")
	}
	if shares(t1, o1) {
		t.Fatalf("bảng khác không được xung đột")
	}
}
