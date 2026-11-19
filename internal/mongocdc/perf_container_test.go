package mongocdc

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Throughput on local containers (numbers are logged; sizes via STRAND_MONGO_PERF_DOCS).
func TestContainer_Throughput(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	db := "perf"
	n := envInt("STRAND_MONGO_PERF_DOCS", 100000)
	pad := strings.Repeat("p", 900)
	coll := p.srcC.Database(db).Collection("c")
	for i := 0; i < n; i += 1000 {
		docs := make([]any, 0, 1000)
		for j := i; j < i+1000 && j < n; j++ {
			docs = append(docs, bson.D{{Key: "_id", Value: j}, {Key: "v", Value: j}, {Key: "pad", Value: pad}})
		}
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			t.Fatal(err)
		}
	}
	cfg := p.cfg(t, "perf", db)
	cfg.CheckpointEvery = time.Second
	cfg.MaxAwait = 500 * time.Millisecond
	began := time.Now()
	r := start(cfg)
	states := p.dstC.Database(stateDatabase).Collection(stateCollection)
	for {
		r.failIfExited(t)
		var st syncState
		if err := states.FindOne(ctx, bson.D{{Key: "_id", Value: "perf"}}).Decode(&st); err == nil && st.InitialDone {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	snap := time.Since(began)
	t.Logf("snapshot: %d docs (~1 KiB) in %s = %.0f docs/s", n, snap.Round(time.Millisecond), float64(n)/snap.Seconds())

	// Stream: m inserts (InsertMany), then m updates (one UpdateMany), each phase timed
	// from the first source write until the target has caught up.
	m := n / 2
	tc := p.dstC.Database(db).Collection("c")
	waitFor := func(what string, filter bson.D, want int, began time.Time) {
		t.Helper()
		for {
			r.failIfExited(t)
			got, _ := tc.CountDocuments(ctx, filter)
			if int(got) == want {
				el := time.Since(began)
				t.Logf("stream %s: %d events applied in %s = %.0f events/s", what, m, el.Round(time.Millisecond), float64(m)/el.Seconds())
				return
			}
			if time.Since(began) > 10*time.Minute {
				t.Fatalf("stream %s did not catch up: %d/%d", what, got, want)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	began = time.Now()
	for i := 0; i < m; i += 1000 {
		docs := make([]any, 0, 1000)
		for j := i; j < i+1000 && j < m; j++ {
			docs = append(docs, bson.D{{Key: "_id", Value: n + j}, {Key: "v", Value: j}, {Key: "pad", Value: pad}})
		}
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			t.Fatal(err)
		}
	}
	waitFor("inserts", bson.D{}, n+m, began)
	began = time.Now()
	if _, err := coll.UpdateMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: m}}}}, bson.D{{Key: "$set", Value: bson.D{{Key: "u", Value: 1}}}}); err != nil {
		t.Fatal(err)
	}
	waitFor("updates", bson.D{{Key: "u", Value: 1}}, m, began)
	p.waitIdentical(t, db, 2*time.Minute, r)
	r.stop(t)
}
