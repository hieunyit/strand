package pipeline

import (
	"reflect"
	"testing"

	"strand/internal/model"
)

func TestParseMySQLCascadeFKs(t *testing.T) {
	child := model.TableID{Schema: "sales", Table: "phones"}
	ddl := `CREATE TABLE "phones" (
  "a" int,
  CONSTRAINT "phone_parent_fk" FOREIGN KEY ("a","b") REFERENCES "crm"."parent" ("k,1","k""2") ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT "ignored_fk" FOREIGN KEY ("c") REFERENCES "other" ("id")
)`
	got, err := parseMySQLCascadeFKs(child, ddl)
	want := []cascadeFK{{Child: child, Parent: model.TableID{Schema: "crm", Table: "parent"},
		Columns: []string{"a", "b"}, References: []string{"k,1", `k"2`}, OnDelete: "SET NULL", OnUpdate: "CASCADE"}}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("FKs = %#v, error = %v; want %#v", got, err, want)
	}
}
