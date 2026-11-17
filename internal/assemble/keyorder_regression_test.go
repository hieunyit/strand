package assemble

import (
	"reflect"
	"strand/internal/model"
	"strand/internal/reader"
	"testing"
)

func TestPGRelationPreservesResolvedIdentityOrder(t *testing.T) {
	a := New()
	a.registry.RegisterTable("app", "t", []string{"b", "a"}, []string{"a", "b"})
	pos := model.Pos{Engine: model.EnginePG}
	for _, columns := range [][]string{{"a", "b"}, {"a", "b", "c"}} {
		_, err := a.Push(reader.Event{Type: reader.EventTableMap, Schema: "app", Table: "t", Columns: columns, PKColumns: []string{"a", "b"}, SchemaChanged: len(columns) == 3, Pos: pos})
		if err != nil {
			t.Fatal(err)
		}
		sch := a.registry.GetTable("app", "t")
		if got := sch.ExtractPK([]any{int64(1), int64(2), "x"}); !reflect.DeepEqual(got, []model.Value{int64(2), int64(1)}) {
			t.Fatalf("logical key swapped by Relation: %v", got)
		}
	}
	// An actual identity-membership change must be accepted at the DDL barrier.
	_, err := a.Push(reader.Event{Type: reader.EventTableMap, Schema: "app", Table: "t", Columns: []string{"a", "b", "c"}, PKColumns: []string{"c"}, SchemaChanged: true, Pos: pos})
	if err != nil {
		t.Fatal(err)
	}
	if got := a.registry.GetTable("app", "t").PKColumns; !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatalf("real identity change ignored: %v", got)
	}
}
