package pipeline

import (
	"reflect"
	"strings"
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

func TestCheckCascadeOrder(t *testing.T) {
	fk := func(parent, child, onDelete, onUpdate string) cascadeFK {
		return cascadeFK{
			Parent:  model.TableID{Schema: "app", Table: parent},
			Child:   model.TableID{Schema: "app", Table: child},
			Columns: []string{"parent_id"}, References: []string{"id"},
			OnDelete: onDelete, OnUpdate: onUpdate,
		}
	}
	for _, tc := range []struct {
		name        string
		fks         []cascadeFK
		wantOverlap string
	}{
		{
			name: "conflicting actions can follow constraint name order",
			fks: []cascadeFK{
				fk("parent", "child", "SET NULL", "RESTRICT"),
				fk("parent", "child", "CASCADE", "RESTRICT"),
			},
			wantOverlap: "from app.parent can reach app.child",
		},
		{
			name: "set null update reaches same table as another delete",
			fks: []cascadeFK{
				fk("parent", "child", "SET NULL", "RESTRICT"),
				fk("child", "grandchild", "RESTRICT", "CASCADE"),
				fk("parent", "grandchild", "CASCADE", "RESTRICT"),
			},
			wantOverlap: "from app.parent can reach app.grandchild",
		},
		{
			name: "independent branches and a chain",
			fks: []cascadeFK{
				fk("parent", "child", "CASCADE", "CASCADE"),
				fk("parent", "sibling", "SET NULL", "SET NULL"),
				fk("child", "grandchild", "CASCADE", "RESTRICT"),
			},
		},
		{
			name: "actions triggered by different operations",
			fks: []cascadeFK{
				fk("parent", "child", "SET NULL", "RESTRICT"),
				fk("parent", "child", "RESTRICT", "CASCADE"),
			},
		},
		{
			name: "actions triggered by unrelated parents",
			fks: []cascadeFK{
				fk("parent", "child", "CASCADE", "RESTRICT"),
				fk("other", "child", "SET NULL", "RESTRICT"),
			},
		},
		{
			name: "single self-referential action",
			fks:  []cascadeFK{fk("tree", "tree", "CASCADE", "RESTRICT")},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkCascadeOrder(tc.fks)
			if tc.wantOverlap == "" {
				if err != nil {
					t.Fatalf("unexpected rejection: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantOverlap) {
				t.Fatalf("error = %v, want %q", err, tc.wantOverlap)
			}
		})
	}
}
