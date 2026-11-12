package assemble

import (
	"strings"
	"testing"

	"strand/internal/model"
	"strand/internal/reader"
)

func my57Pos() model.Pos { return model.Pos{Engine: model.EngineMySQL, GTID: "u:1"} }

// MySQL 5.7: registry (từ information_schema) có 2 cột nhưng row binlog có 3 (bảng đã ALTER, registry chưa
// nạp lại) => lỗi rõ ràng, không gán sai tên cột.
func TestM57RowColumnCountMismatchFailsClosed(t *testing.T) {
	a := New()
	a.Registry().RegisterTable("app", "t", []string{"id"}, []string{"id", "a"})
	_, err := a.Push(reader.Event{Type: reader.EventRows, Pos: my57Pos(), Schema: "app", Table: "t",
		Op: model.OpInsert, Rows: [][]any{{int64(1), "x", "y"}}})
	if err == nil || !strings.Contains(err.Error(), "3 columns but the registered schema has 2") {
		t.Fatalf("err=%v", err)
	}
}

// TABLE_MAP của 5.7 do reader điền cột từ information_schema, không có PK (bảng không PK): giữ PK pipeline
// đã đăng ký (unique key) khi cột không đổi; PK trong event thì ghi đè.
func TestM57TableMapKeepsRegisteredPKWhenEventHasNone(t *testing.T) {
	a := New()
	a.Registry().RegisterTable("app", "t", []string{"b"}, []string{"a", "b"})
	_, _ = a.Push(reader.Event{Type: reader.EventTableMap, Pos: my57Pos(), Schema: "app", Table: "t", Columns: []string{"a", "b"}})
	if got := a.Registry().GetTable("app", "t").PKIndices; len(got) != 1 || got[0] != 1 {
		t.Fatalf("PKIndices=%v", got)
	}
	_, _ = a.Push(reader.Event{Type: reader.EventTableMap, Pos: my57Pos(), Schema: "app", Table: "t", Columns: []string{"a", "b", "c"}, PKColumns: []string{"c"}})
	if got := a.Registry().GetTable("app", "t").PKIndices; len(got) != 1 || got[0] != 2 {
		t.Fatalf("PKIndices=%v", got)
	}
}
