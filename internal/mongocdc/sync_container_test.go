package mongocdc

// Integration tests on real MongoDB single-node replica sets (testcontainers): source
// and target are separate containers. Convergence is checked byte for byte per _id.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// An oversized source transaction spanning many one-event getMore batches is rejected
// before any member is written or its resume token is committed. A larger limit resumes
// from the previous checkpoint and applies the complete transaction atomically.
func TestContainer_OversizedTransactionFailsClosedAndResumes(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	db, pipeline := "oversized_txn", "oversized-txn"
	if err := p.srcC.Database(db).CreateCollection(ctx, "docs"); err != nil {
		t.Fatal(err)
	}
	stateColl := p.dstC.Database(stateDatabase, targetDBOptions()).Collection(stateCollection)
	readState := func() syncState {
		t.Helper()
		var state syncState
		if err := stateColl.FindOne(ctx, bson.D{{Key: "_id", Value: pipeline}}).Decode(&state); err != nil {
			t.Fatal(err)
		}
		return state
	}
	observer := NewObserver(pipeline)
	cfg := p.cfg(t, pipeline, db)
	cfg.StreamBatch = 1 // also sets change-stream batchSize to one event per getMore
	cfg.Observer = observer
	r := start(cfg)
	deadline := time.Now().Add(time.Minute)
	for observer.Snapshot().Phase != PhaseStreaming {
		r.failIfExited(t)
		if time.Now().After(deadline) {
			t.Fatal("pipeline did not reach streaming")
		}
		time.Sleep(20 * time.Millisecond)
	}
	before := readState()
	sess, err := p.srcC.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	_, err = sess.WithTransaction(ctx, func(tx context.Context) (any, error) {
		coll := p.srcC.Database(db).Collection("docs")
		for i := 0; i < 25; i++ {
			if _, err := coll.InsertOne(tx, bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i}}); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}, options.Transaction().SetWriteConcern(writeconcern.Majority()))
	sess.EndSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-r.done:
		if !errors.Is(err, ErrSourceTransactionTooLarge) {
			t.Fatalf("pipeline error = %v, want oversized source transaction error", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("pipeline did not reject oversized transaction")
	}
	target := p.dstC.Database(db).Collection("docs")
	if n, err := target.CountDocuments(ctx, bson.D{}); err != nil || n != 0 {
		t.Fatalf("target has %d transaction documents after rejection, err=%v; want none", n, err)
	}
	afterFailure := readState()
	if !bytes.Equal(before.ResumeToken, afterFailure.ResumeToken) {
		t.Fatal("checkpoint advanced into the rejected transaction")
	}

	resumeCfg := p.cfg(t, pipeline, db)
	resumeCfg.StreamBatch = 50
	r = start(resumeCfg)
	p.waitIdentical(t, db, time.Minute, r)
	if n, err := target.CountDocuments(ctx, bson.D{}); err != nil || n != 25 {
		t.Fatalf("target has %d documents after atomic replay, err=%v; want 25", n, err)
	}
	if afterResume := readState(); bytes.Equal(afterFailure.ResumeToken, afterResume.ResumeToken) {
		t.Fatal("checkpoint did not advance after replaying the full transaction")
	}
	r.stop(t)
}

func TestContainer_TransactionCreateDDLIsRejectedBeforeCheckpoint(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	db, pipeline := "txn_create", "txn-create"
	observer := NewObserver(pipeline)
	cfg := p.cfg(t, pipeline, db)
	cfg.Observer = observer
	r := start(cfg)
	deadline := time.Now().Add(time.Minute)
	for observer.Snapshot().Phase != PhaseStreaming {
		r.failIfExited(t)
		if time.Now().After(deadline) {
			t.Fatal("pipeline did not reach streaming")
		}
		time.Sleep(20 * time.Millisecond)
	}
	stateColl := p.dstC.Database(stateDatabase, targetDBOptions()).Collection(stateCollection)
	var before syncState
	if err := stateColl.FindOne(ctx, bson.D{{Key: "_id", Value: pipeline}}).Decode(&before); err != nil {
		t.Fatal(err)
	}
	sess, err := p.srcC.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	_, err = sess.WithTransaction(ctx, func(tx context.Context) (any, error) {
		if err := p.srcC.Database(db).CreateCollection(tx, "docs"); err != nil {
			return nil, err
		}
		_, err := p.srcC.Database(db).Collection("docs").InsertOne(tx, bson.D{{Key: "_id", Value: 1}})
		return nil, err
	}, options.Transaction().SetWriteConcern(writeconcern.Majority()))
	sess.EndSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-r.done:
		if !errors.Is(err, ErrSourceTransactionDDL) {
			t.Fatalf("pipeline error = %v, want source transaction DDL error", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("pipeline did not reject transactional create")
	}
	names, err := p.dstC.Database(db).ListCollectionNames(ctx, bson.D{{Key: "name", Value: "docs"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 0 {
		t.Fatal("target collection was created despite rejecting the source transaction")
	}
	var after syncState
	if err := stateColl.FindOne(ctx, bson.D{{Key: "_id", Value: pipeline}}).Decode(&after); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before.ResumeToken, after.ResumeToken) {
		t.Fatal("checkpoint advanced past DDL in the rejected source transaction")
	}
}

// Snapshot of N documents while concurrent insert/update/replace/delete traffic runs
// through the snapshot and into streaming; repeated on fresh databases.
func TestContainer_SnapshotUnderConcurrentLoad(t *testing.T) {
	p := newPair(t)
	seed := testSeed(t)
	rounds := envInt("STRAND_MONGO_ROUNDS", 3)
	n := envInt("STRAND_MONGO_DOCS", 20000)
	for round := 0; round < rounds; round++ {
		t.Run(fmt.Sprintf("round%d", round), func(t *testing.T) {
			db := fmt.Sprintf("load%d", round)
			colls := []string{"a", "b", "mixed"}
			seedDocs(t, p.srcC.Database(db), "a", n, seed+int64(round), intID)
			seedDocs(t, p.srcC.Database(db), "b", n/2, seed+int64(round)+1, intID)
			seedDocs(t, p.srcC.Database(db), "mixed", n/2, seed+int64(round)+2, mixedID)
			load := startLoad(t, p.srcC.Database(db), colls[:2], n+n/5, 6, seed+int64(round)*100, intID)
			loadMixed := startLoad(t, p.srcC.Database(db), colls[2:], n/2+n/10, 2, seed+int64(round)*100+50, mixedID)
			cfg := p.cfg(t, "load-"+db, db)
			cfg.SnapshotBatch = 300 // many snapshot batches while the load runs
			r := start(cfg)
			time.Sleep(time.Duration(3+round) * time.Second)
			ops := load.halt(t) + loadMixed.halt(t)
			docs := p.waitIdentical(t, db, 2*time.Minute, r)
			r.stop(t)
			t.Logf("round %d: %d concurrent ops; %d documents identical", round, ops, docs)
		})
	}
}

// Large (~15 MiB) documents through snapshot, insert, update and replace.
func TestContainer_LargeDocuments(t *testing.T) {
	p := newPair(t)
	db := "large"
	ctx := context.Background()
	big := func(id int, c string) bson.D {
		return bson.D{{Key: "_id", Value: id}, {Key: "s", Value: strings.Repeat(c, 15<<20)}}
	}
	if _, err := p.srcC.Database(db).Collection("c").InsertOne(ctx, big(1, "a")); err != nil {
		t.Fatal(err)
	}
	r := start(p.cfg(t, "large", db))
	p.waitIdentical(t, db, time.Minute, r)
	coll := p.srcC.Database(db).Collection("c")
	if _, err := coll.InsertOne(ctx, big(2, "b")); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: 1}}, bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: 1}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: 2}}, big(2, "c")); err != nil {
		t.Fatal(err)
	}
	p.waitIdentical(t, db, time.Minute, r)
	r.stop(t)
}

// Collection drop, rename (selected->selected, selected->excluded, excluded->selected),
// and dropDatabase while streaming.
func TestContainer_DropRenameDropDatabase(t *testing.T) {
	p := newPair(t)
	db := "ddl"
	ctx := context.Background()
	sdb := p.srcC.Database(db)
	for _, c := range []string{"keep", "dropme", "ren1", "ren2", "x_hidden"} {
		seedDocs(t, sdb, c, 500, 1, intID)
	}
	cfg := p.cfg(t, "ddl", db)
	cfg.Exclude = []string{db + ".x_*"}
	r := start(cfg)
	waitSelected := func() {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		for {
			r.failIfExited(t)
			a, _ := dbDigest(ctx, sdb)
			b, _ := dbDigest(ctx, p.dstC.Database(db))
			for name := range a {
				if strings.HasPrefix(name, "x_") {
					delete(a, name)
				}
			}
			if d := diffDigest(a, b); d == "" {
				return
			} else if time.Now().After(deadline) {
				t.Fatalf("not converged: %s", d)
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
	waitSelected()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(sdb.Collection("dropme").Drop(ctx))
	adminRename := func(from, to string) {
		t.Helper()
		must(p.srcC.Database("admin").RunCommand(ctx, bson.D{{Key: "renameCollection", Value: db + "." + from}, {Key: "to", Value: db + "." + to}}).Err())
	}
	adminRename("ren1", "ren1_new")     // selected -> selected
	adminRename("ren2", "x_ren2")       // selected -> excluded
	adminRename("x_hidden", "unhidden") // excluded -> selected
	_, err := sdb.Collection("ren1_new").InsertOne(ctx, bson.D{{Key: "_id", Value: "after-rename"}})
	must(err)
	_, err = sdb.Collection("unhidden").UpdateOne(ctx, bson.D{{Key: "_id", Value: int32(3)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: -1}}}})
	must(err)
	waitSelected()
	// dropDatabase invalidates the stream; it must restart after the invalidate and
	// continue with the recreated database.
	must(sdb.Drop(ctx))
	waitSelected()
	seedDocs(t, sdb, "reborn", 300, 2, intID)
	waitSelected()
	r.failIfExited(t)
	r.stop(t)
	// Restart from the checkpoint written after the invalidate.
	r = start(cfg)
	_, err = sdb.Collection("reborn").InsertOne(ctx, bson.D{{Key: "_id", Value: "after-restart"}})
	must(err)
	waitSelected()
	r.stop(t)
}

// Source multi-document transactions must commit atomically on the target: a reader
// using snapshot reads on the target never observes a partial transfer.
func TestContainer_TransactionsAtomicOnTarget(t *testing.T) {
	p := newPair(t)
	db := "bank"
	ctx := context.Background()
	const accounts, initial = 50, 1000
	var docs []any
	for i := 0; i < accounts; i++ {
		docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "bal", Value: initial}})
	}
	if _, err := p.srcC.Database(db).Collection("acct").InsertMany(ctx, docs); err != nil {
		t.Fatal(err)
	}
	// A second collection also changed inside each transaction.
	if _, err := p.srcC.Database(db).Collection("ledger").InsertOne(ctx, bson.D{{Key: "_id", Value: "count"}, {Key: "n", Value: 0}}); err != nil {
		t.Fatal(err)
	}
	r := start(p.cfg(t, "bank", db))
	p.waitIdentical(t, db, time.Minute, r)

	stop := make(chan struct{})
	var violations, observations atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // target observer
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			sess, err := p.dstC.StartSession()
			if err != nil {
				continue
			}
			_, _ = sess.WithTransaction(ctx, func(tx context.Context) (any, error) {
				cur, err := p.dstC.Database(db).Collection("acct").Find(tx, bson.D{})
				if err != nil {
					return nil, err
				}
				var all []struct {
					Bal int `bson:"bal"`
				}
				if err := cur.All(tx, &all); err != nil {
					return nil, err
				}
				sum := 0
				for _, a := range all {
					sum += a.Bal
				}
				var ledger struct {
					N int `bson:"n"`
				}
				_ = p.dstC.Database(db).Collection("ledger").FindOne(tx, bson.D{{Key: "_id", Value: "count"}}).Decode(&ledger)
				observations.Add(1)
				if sum != accounts*initial || len(all) != accounts {
					violations.Add(1)
				}
				return nil, nil
			}, options.Transaction().SetReadConcern(readconcern.Snapshot()))
			sess.EndSession(ctx)
		}
	}()
	rng := rand.New(rand.NewSource(testSeed(t)))
	txns := envInt("STRAND_MONGO_TXNS", 400)
	for i := 0; i < txns; i++ {
		sess, err := p.srcC.StartSession()
		if err != nil {
			t.Fatal(err)
		}
		from, to, amt := rng.Intn(accounts), rng.Intn(accounts), rng.Intn(50)+1
		_, err = sess.WithTransaction(ctx, func(tx context.Context) (any, error) {
			c := p.srcC.Database(db).Collection("acct")
			for k := 0; k < 3; k++ { // several statements per transaction
				if _, err := c.UpdateOne(tx, bson.D{{Key: "_id", Value: from}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "bal", Value: -amt}}}}); err != nil {
					return nil, err
				}
				if _, err := c.UpdateOne(tx, bson.D{{Key: "_id", Value: to}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "bal", Value: amt}}}}); err != nil {
					return nil, err
				}
			}
			_, err := p.srcC.Database(db).Collection("ledger").UpdateOne(tx, bson.D{{Key: "_id", Value: "count"}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}})
			return nil, err
		}, options.Transaction().SetWriteConcern(writeconcern.Majority()))
		sess.EndSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	p.waitIdentical(t, db, time.Minute, r)
	close(stop)
	wg.Wait()
	r.stop(t)
	t.Logf("%d source transactions; %d target snapshot observations; %d partial states", txns, observations.Load(), violations.Load())
	if violations.Load() != 0 {
		t.Fatalf("observed %d partially applied transactions on the target", violations.Load())
	}
}

// Real oplog rollover while the pipeline is stopped: the saved resume token falls off
// the (minimum-size, 990 MB) oplog; restarting must fail closed. (A forged old resume
// point is not a valid stand-in: MongoDB accepts any start before the oplog's first
// entry while the oplog has never rolled over.)
func TestContainer_OplogRolloverFailsClosed(t *testing.T) {
	src, dst := testutilPairWithOplog(t)
	p := &pair{srcURI: src, dstURI: dst, srcC: mustClient(t, src), dstC: mustClient(t, dst)}
	db := "roll"
	ctx := context.Background()
	seedDocs(t, p.srcC.Database(db), "c", 10, 1, intID)
	cfg := p.cfg(t, "roll", db)
	r := start(cfg)
	p.waitIdentical(t, db, time.Minute, r)
	r.stop(t)
	var h helloReply
	if err := p.srcC.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h); err != nil || h.OperationTime.T == 0 {
		t.Fatalf("source operationTime: %v %+v", err, h)
	}
	stoppedAt := h.OperationTime // >= the checkpointed position
	// Churn through another database until the oplog's first entry is past the checkpoint.
	filler := p.srcC.Database("filler").Collection("f")
	blob := strings.Repeat("z", 1<<20)
	oplog := p.srcC.Database("local").Collection("oplog.rs")
	next := 0
	deadline := time.Now().Add(8 * time.Minute)
	for {
		docs := make([]any, 50)
		for j := range docs {
			docs[j] = bson.D{{Key: "_id", Value: next}, {Key: "s", Value: blob}}
			next++
		}
		if _, err := filler.InsertMany(ctx, docs); err != nil {
			t.Fatal(err)
		}
		if _, err := filler.DeleteMany(ctx, bson.D{}); err != nil {
			t.Fatal(err)
		}
		var first struct {
			TS bson.Timestamp `bson:"ts"`
		}
		if err := oplog.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "$natural", Value: 1}})).Decode(&first); err != nil {
			t.Fatal(err)
		}
		if first.TS.After(stoppedAt) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("oplog not truncated past %v after %d MiB churn (first entry %v)", stoppedAt, next, first.TS)
		}
	}
	runCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	err := Run(runCtx, cfg)
	if !errors.Is(err, ErrResyncRequired) {
		t.Fatalf("want ErrResyncRequired after oplog rollover, got %v", err)
	}
	t.Logf("fail-closed after %d MiB churn: %v", next, err)
	if n, _ := p.dstC.Database(db).Collection("c").CountDocuments(ctx, bson.D{}); n != 10 {
		t.Fatalf("target changed after fail-closed: %d docs", n)
	}
}

// Two instances racing for the same target: exactly one obtains the lease and writes.
func TestContainer_TwoInstancesRaceForLease(t *testing.T) {
	p := newPair(t)
	for round := 0; round < envInt("STRAND_MONGO_ROUNDS", 3); round++ {
		db := fmt.Sprintf("race%d", round)
		seedDocs(t, p.srcC.Database(db), "c", 2000, int64(round), intID)
		cfg := p.cfg(t, "race-"+db, db)
		var started sync.WaitGroup
		gate := make(chan struct{})
		rs := make([]*running, 2)
		for i := range rs {
			started.Add(1)
			go func(i int) {
				defer started.Done()
				<-gate
				rs[i] = start(cfg)
			}(i)
		}
		close(gate)
		started.Wait()
		// One must fail with the lease error quickly; the other keeps running.
		var loser, winner *running
		select {
		case err := <-rs[0].done:
			loser, winner = rs[0], rs[1]
			if err == nil || !strings.Contains(err.Error(), "holds the lease") {
				t.Fatalf("round %d: unexpected loser error: %v", round, err)
			}
		case err := <-rs[1].done:
			loser, winner = rs[1], rs[0]
			if err == nil || !strings.Contains(err.Error(), "holds the lease") {
				t.Fatalf("round %d: unexpected loser error: %v", round, err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d: neither instance was refused the lease", round)
		}
		loser.cancel()
		p.waitIdentical(t, db, time.Minute, winner)
		winner.stop(t)
	}
}

// Fencing: once the lease is taken over (e.g. the old owner was paused past its TTL),
// the old owner cannot commit a single further write and stops with ErrLeaseLost.
func TestContainer_FencedAfterLeaseTakeover(t *testing.T) {
	p := newPair(t)
	db := "fence"
	ctx := context.Background()
	seedDocs(t, p.srcC.Database(db), "c", 100, 1, intID)
	cfg := p.cfg(t, "fence", db)
	cfg.LeaseDuration = time.Hour // the refresh loop must not be what notices the loss
	r := start(cfg)
	p.waitIdentical(t, db, time.Minute, r)
	// Simulate another instance taking over after expiry.
	locks := p.dstC.Database(stateDatabase).Collection(lockCollection)
	res, err := locks.UpdateOne(ctx, bson.D{{Key: "_id", Value: db}}, bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: "intruder"}}}})
	if err != nil || res.ModifiedCount != 1 {
		t.Fatalf("steal lease: %v %+v", err, res)
	}
	// A new owner rewrites every fence document of the database (claimFences).
	if res, err := locks.UpdateMany(ctx, bson.D{{Key: "_id", Value: bson.Regex{Pattern: "^" + db + "#"}}}, bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: "intruder"}}}}); err != nil || res.ModifiedCount == 0 {
		t.Fatalf("steal fences: %v %+v", err, res)
	}
	if _, err := p.srcC.Database(db).Collection("c").InsertOne(ctx, bson.D{{Key: "_id", Value: "after-takeover"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-r.done:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("want ErrLeaseLost, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("stale owner kept running after takeover")
	}
	n, err := p.dstC.Database(db).Collection("c").CountDocuments(ctx, bson.D{{Key: "_id", Value: "after-takeover"}})
	if err != nil || n != 0 {
		t.Fatalf("stale owner wrote after takeover: n=%d err=%v", n, err)
	}
}

// Snapshot resumes per collection after a crash (here: cancellation mid-snapshot) and
// pages correctly across _id values of many BSON types.
func TestContainer_SnapshotResumeMixedIDs(t *testing.T) {
	p := newPair(t)
	db := "resume"
	ctx := context.Background()
	seedDocs(t, p.srcC.Database(db), "m", 7000, 3, mixedID)
	cfg := p.cfg(t, "resume", db)
	cfg.SnapshotBatch = 100
	for i := 0; i < 5; i++ {
		r := start(cfg)
		time.Sleep(time.Duration(150+100*i) * time.Millisecond)
		r.cancel()
		if err := <-r.done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		var pr collProgress
		if err := p.dstC.Database(stateDatabase).Collection(progressCollection).FindOne(ctx, bson.D{{Key: "_id", Value: progressID{"resume", "m"}}}).Decode(&pr); err == nil {
			t.Logf("after stop %d: copied=%d done=%v", i, pr.Copied, pr.Done)
		}
	}
	r := start(cfg)
	p.waitIdentical(t, db, time.Minute, r)
	r.stop(t)
	var pr collProgress
	if err := p.dstC.Database(stateDatabase).Collection(progressCollection).FindOne(ctx, bson.D{{Key: "_id", Value: progressID{"resume", "m"}}}).Decode(&pr); err != nil {
		t.Fatal(err)
	}
	if pr.Copied != 7000 || !pr.Done {
		t.Fatalf("snapshot copied %d documents in total across restarts (done=%v), want exactly 7000 (no re-copy, no skip)", pr.Copied, pr.Done)
	}
}

// The first start refuses to overwrite an existing target collection.
func TestContainer_RefusesExistingTarget(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	seedDocs(t, p.srcC.Database("ex"), "c", 10, 1, intID)
	if _, err := p.dstC.Database("ex").Collection("c").InsertOne(ctx, bson.D{{Key: "_id", Value: "precious"}}); err != nil {
		t.Fatal(err)
	}
	err := Run(ctx, p.cfg(t, "ex", "ex"))
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("want refusal, got %v", err)
	}
	if n, _ := p.dstC.Database("ex").Collection("c").CountDocuments(ctx, bson.D{}); n != 1 {
		t.Fatalf("target modified: %d docs", n)
	}
}

// Regression: in a batch with a non-canonical _id (Decimal128, document) the collection
// takes the slow path and updates are looked up on the source; a numeric _id looked up
// there must not be mistaken for a deleted document (keys differ for numbers).
func TestContainer_SlowPathLookupKeepsNumericIDs(t *testing.T) {
	p := newPair(t)
	db := "slowlk"
	ctx := context.Background()
	dec, _ := bson.ParseDecimal128("7.5")
	c := p.srcC.Database(db).Collection("m")
	if _, err := c.InsertMany(ctx, []any{bson.D{{Key: "_id", Value: dec}, {Key: "v", Value: 0}},
		bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: 0}}, bson.D{{Key: "_id", Value: int64(2)}, {Key: "v", Value: 0}}}); err != nil {
		t.Fatal(err)
	}
	r := start(p.cfg(t, "slowlk", db))
	p.waitIdentical(t, db, time.Minute, r)
	for i := 0; i < 20; i++ {
		sess, err := p.srcC.StartSession()
		if err != nil {
			t.Fatal(err)
		}
		_, err = sess.WithTransaction(ctx, func(tx context.Context) (any, error) {
			for _, id := range []any{dec, int32(1), int64(2)} {
				if _, err := c.UpdateOne(tx, bson.D{{Key: "_id", Value: id}}, bson.D{{Key: "$inc", Value: bson.D{{Key: "v", Value: 1}}}}); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		sess.EndSession(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	p.waitIdentical(t, db, time.Minute, r)
	r.stop(t)
}

// A lease left by a crashed process on another host (not refreshed any more) is taken
// over once it expires, instead of exiting with "lease held".
func TestContainer_WaitsForStaleForeignLease(t *testing.T) {
	p := newPair(t)
	db := "stale"
	ctx := context.Background()
	seedDocs(t, p.srcC.Database(db), "c", 100, 1, intID)
	locks := p.dstC.Database(stateDatabase).Collection(lockCollection)
	if _, err := locks.InsertOne(ctx, bson.D{{Key: "_id", Value: db}, {Key: "owner", Value: "dead"}, {Key: "pipeline", Value: "stale"},
		{Key: "info", Value: ownerInfo{Host: "other-host", BootID: "b", PidNS: "n", PID: 1, PIDStart: "1"}},
		{Key: "expires_at", Value: time.Now().Add(4 * time.Second)}}); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	r := start(p.cfg(t, "stale", db))
	p.waitIdentical(t, db, time.Minute, r)
	r.stop(t)
	if el := time.Since(began); el < 2*time.Second {
		t.Fatalf("took over a live-looking foreign lease after %s", el)
	}
}
