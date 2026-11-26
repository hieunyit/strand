package mongocdc

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// The change stream counts the applied events per collection: the Tables view shows them with the time of
// the last change.
func TestContainer_CollectionActivityFromStream(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	db := "activity"
	seedDocs(t, p.srcC.Database(db), "orders", 10, 1, intID)
	seedDocs(t, p.srcC.Database(db), "quiet", 5, 2, intID)
	cfg := p.cfg(t, "activity", db)
	obs := NewObserver("activity")
	cfg.Observer = obs
	r := start(cfg)
	defer r.stop(t)
	p.waitIdentical(t, db, time.Minute, r)
	deadline := time.Now().Add(time.Minute)
	for obs.Snapshot().Phase != PhaseStreaming {
		if time.Now().After(deadline) {
			t.Fatal("pipeline did not reach streaming")
		}
		time.Sleep(50 * time.Millisecond)
	}
	orders := p.srcC.Database(db).Collection("orders")
	if _, err := orders.InsertMany(ctx, []any{bson.D{{Key: "_id", Value: int32(100)}}, bson.D{{Key: "_id", Value: int32(101)}}, bson.D{{Key: "_id", Value: int32(102)}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := orders.UpdateOne(ctx, bson.D{{Key: "_id", Value: int32(100)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := orders.DeleteOne(ctx, bson.D{{Key: "_id", Value: int32(101)}}); err != nil {
		t.Fatal(err)
	}
	p.waitIdentical(t, db, time.Minute, r)
	h := &AdminHandler{Pipeline: "activity", Parts: []AdminPart{{SourceDB: db, Obs: obs}}}
	for {
		tabs, err := h.Tables(ctx)
		if err != nil {
			t.Fatal(err)
		}
		byName := map[string]int{}
		for i, ti := range tabs {
			byName[ti.Table] = i
		}
		o, q := tabs[byName[db+".orders"]], tabs[byName[db+".quiet"]]
		if o.Inserts == 3 && o.Updates == 1 && o.Deletes == 1 && o.LastChangeAt != nil && o.LastSourceAt != nil {
			if q.Changes() != 0 || q.LastChangeAt != nil {
				t.Fatalf("quiet collection has activity: %+v", q)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("orders activity: %+v", o)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
