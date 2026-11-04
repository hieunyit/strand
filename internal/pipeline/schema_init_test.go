package pipeline

import (
	"strand/internal/assemble"
	"strand/internal/model"
	"testing"
)

func TestRS1_SnapshotParentsBeforeChildren(t *testing.T) {
	child := model.TableID{Schema: "app", Table: "a_child"}
	parent := model.TableID{Schema: "app", Table: "z_parent"}
	metas := map[model.TableID]*tableMeta{child: {Keys: assemble.KeyMeta{ForeignKeys: []assemble.ForeignKeyMeta{{ParentTable: parent}}}}, parent: {}}
	got, err := orderSnapshotTables([]model.TableID{child, parent}, metas)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != parent || got[1] != child {
		t.Fatalf("order=%v", got)
	}
	metas[parent].Keys.ForeignKeys = []assemble.ForeignKeyMeta{{ParentTable: child}}
	if _, err := orderSnapshotTables([]model.TableID{child, parent}, metas); err == nil {
		t.Fatal("cycle accepted")
	}
	got, err = orderSnapshotTables([]model.TableID{child}, metas)
	if err != nil || len(got) != 1 {
		t.Fatalf("existing parent should not be scheduled: %v %v", got, err)
	}
}
