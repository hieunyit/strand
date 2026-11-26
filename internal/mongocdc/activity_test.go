package mongocdc

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Change events are counted per collection (replace counts as an update) and reported by the admin
// `tables` command with the time of the last applied batch and its source cluster time.
func TestCollectionActivityInTables(t *testing.T) {
	b := newBatch()
	for _, op := range []string{"insert", "insert", "update", "replace", "delete", "createIndexes"} {
		b.countColl("orders", op)
	}
	b.countColl("users", "update")
	o := NewObserver("p/app")
	o.setCollection("orders", CollDone, progressSnapshot, 10, 10)
	o.setCollection("users", CollDone, progressSnapshot, 3, 3)
	o.setCollection("idle", CollDone, progressSnapshot, 1, 1)
	src := uint32(time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC).Unix())
	before := time.Now()
	o.collApplied(b.collOps, bson.Timestamp{T: src})
	o.collApplied(b.collOps, bson.Timestamp{T: src})
	h := &AdminHandler{Pipeline: "p", Parts: []AdminPart{{SourceDB: "app", Obs: o}}}
	tabs, err := h.Tables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for i, ti := range tabs {
		got[ti.Table] = i
	}
	ord := tabs[got["app.orders"]]
	if ord.Inserts != 4 || ord.Updates != 4 || ord.Deletes != 2 {
		t.Fatalf("orders: %+v", ord)
	}
	if ord.LastChangeAt == nil || ord.LastChangeAt.Before(before) || ord.LastSourceAt == nil || ord.LastSourceAt.Unix() != int64(src) {
		t.Fatalf("orders times: %v %v", ord.LastChangeAt, ord.LastSourceAt)
	}
	if u := tabs[got["app.users"]]; u.Updates != 2 || u.Changes() != 2 {
		t.Fatalf("users: %+v", u)
	}
	if i := tabs[got["app.idle"]]; i.LastChangeAt != nil || i.Changes() != 0 {
		t.Fatalf("idle: %+v", i)
	}
}
