package mongocdc

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"strand/internal/testutil"
)

// Throughput on local containers, single-node replica sets (numbers are logged).
// Sizes: STRAND_MONGO_PERF_DOCS (snapshot documents) and STRAND_MONGO_PERF_EVENTS.
func TestContainer_Throughput(t *testing.T) {
	p := newPair(t)
	runThroughput(t, p, "single-node")
}

// Throughput with 3-member source and target replica sets (w:majority on 3 members).
func TestContainer_ThroughputThreeNode(t *testing.T) {
	src := testutil.StartMongoCluster(t, testutil.MongoClusterOptions{Members: 3})
	dst := testutil.StartMongoCluster(t, testutil.MongoClusterOptions{Members: 3})
	p := &pair{srcURI: src.AdminURI(), dstURI: dst.AdminURI()}
	p.srcC, p.dstC = mustClient(t, p.srcURI), mustClient(t, p.dstURI)
	runThroughput(t, p, "3-node")
}

// runThroughput measures the snapshot (docs/s) and the apply rate of a change backlog
// (events/s) per operation mix: the backlog is produced while the pipeline is stopped,
// then the pipeline restarts and the time until a sentinel written last on the source
// is visible on the target is measured.
func runThroughput(t *testing.T, p *pair, topology string) {
	ctx := context.Background()
	db := "perf"
	n := envInt("STRAND_MONGO_PERF_DOCS", 100000)
	m := envInt("STRAND_MONGO_PERF_EVENTS", 50000)
	pad := strings.Repeat("p", 900)
	coll := p.srcC.Database(db).Collection("c")
	doc := func(id int) bson.D {
		return bson.D{{Key: "_id", Value: id}, {Key: "v", Value: id}, {Key: "tags", Value: bson.A{"a", "b", id % 7}},
			{Key: "nested", Value: bson.D{{Key: "x", Value: id}, {Key: "y", Value: "z"}}}, {Key: "pad", Value: pad}}
	}
	insertRange := func(from, to int) {
		for i := from; i < to; i += 1000 {
			docs := make([]any, 0, 1000)
			for j := i; j < i+1000 && j < to; j++ {
				docs = append(docs, doc(j))
			}
			if _, err := coll.InsertMany(ctx, docs); err != nil {
				t.Fatal(err)
			}
		}
	}
	insertRange(0, n)
	cfg := p.cfg(t, "perf", db)
	cfg.CheckpointEvery = time.Second
	cfg.MaxAwait = 500 * time.Millisecond
	if os.Getenv("STRAND_MONGO_PERF_PARALLEL") != "" {
		insertParallelColls(t, p.srcC.Database(db), n)
	}
	setPerfOptions(&cfg)
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
	total := n
	if os.Getenv("STRAND_MONGO_PERF_PARALLEL") != "" {
		total = 5 * n
	}
	t.Logf("[%s] snapshot: %d docs (~1 KiB) in %s = %.0f docs/s", topology, total, snap.Round(time.Millisecond), float64(total)/snap.Seconds())
	r.stop(t)

	sentinel := 0
	phase := func(name string, produce func() int) {
		t.Helper()
		events := produce()
		sentinel++
		sid := fmt.Sprintf("sentinel-%d", sentinel)
		if _, err := p.srcC.Database(db).Collection("sentinel").InsertOne(ctx, bson.D{{Key: "_id", Value: sid}}); err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		r := start(cfg)
		tc := p.dstC.Database(db).Collection("sentinel")
		for {
			r.failIfExited(t)
			if n, _ := tc.CountDocuments(ctx, bson.D{{Key: "_id", Value: sid}}); n == 1 {
				break
			}
			if time.Since(began) > 15*time.Minute {
				t.Fatalf("%s backlog not applied", name)
			}
			time.Sleep(20 * time.Millisecond)
		}
		el := time.Since(began)
		r.stop(t)
		t.Logf("[%s] stream %-7s: %d events applied in %s = %.0f events/s", topology, name, events, el.Round(time.Millisecond), float64(events)/el.Seconds())
	}
	phase("insert", func() int { insertRange(n, n+m); return m })
	phase("update", func() int {
		// One small $set per document: the event carries a few bytes of update description,
		// the document is ~1 KiB.
		res, err := coll.UpdateMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$lt", Value: m}}}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "nested.x", Value: -1}}}, {Key: "$inc", Value: bson.D{{Key: "v", Value: 1}}}})
		if err != nil {
			t.Fatal(err)
		}
		return int(res.ModifiedCount)
	})
	phase("delete", func() int {
		res, err := coll.DeleteMany(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: n}}}})
		if err != nil {
			t.Fatal(err)
		}
		return int(res.DeletedCount)
	})
	phase("mixed", func() int {
		// 30% insert, 50% update, 20% delete from 8 concurrent writers.
		var wg sync.WaitGroup
		var mu sync.Mutex
		ops := 0
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				r := rand.New(rand.NewSource(int64(w)))
				done := 0
				for i := 0; i < m/8; i++ {
					id := r.Intn(n + m)
					var err error
					switch x := r.Intn(10); {
					case x < 3:
						_, err = coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, doc(id)[1:], options.Replace().SetUpsert(true))
					case x < 8:
						_, err = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$set", Value: bson.D{{Key: "nested.y", Value: i}}}, {Key: "$push", Value: bson.D{{Key: "tags", Value: i}}}})
					default:
						_, err = coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
					}
					if err == nil {
						done++
					}
				}
				mu.Lock()
				ops += done
				mu.Unlock()
			}(w)
		}
		wg.Wait()
		return ops
	})
	r = start(cfg)
	p.waitIdentical(t, db, 5*time.Minute, r)
	r.stop(t)
}

func insertParallelColls(t *testing.T, db *mongo.Database, n int) {
	ctx := context.Background()
	pad := strings.Repeat("q", 900)
	var wg sync.WaitGroup
	for c := 1; c <= 4; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < n; i += 1000 {
				docs := make([]any, 0, 1000)
				for j := i; j < i+1000 && j < n; j++ {
					docs = append(docs, bson.D{{Key: "_id", Value: j}, {Key: "pad", Value: pad}})
				}
				if _, err := db.Collection(fmt.Sprintf("p%d", c)).InsertMany(ctx, docs); err != nil {
					t.Error(err)
					return
				}
			}
		}(c)
	}
	wg.Wait()
}
