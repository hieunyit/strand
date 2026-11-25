package mongocdc

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func rangeProgress(t *testing.T, p *pair, pipeline, coll string) (collProgress, []collProgress) {
	t.Helper()
	ctx := context.Background()
	progress := p.dstC.Database(stateDatabase).Collection(progressCollection)
	var parent collProgress
	_ = progress.FindOne(ctx, bson.D{{Key: "_id", Value: progressID{Pipeline: pipeline, Collection: coll}}}).Decode(&parent)
	cur, err := progress.Find(ctx, bson.D{{Key: "_id.p", Value: pipeline}, {Key: "_id.c", Value: coll}, {Key: "kind", Value: progressPart}})
	if err != nil {
		t.Fatal(err)
	}
	var ranges []collProgress
	if err := cur.All(ctx, &ranges); err != nil {
		t.Fatal(err)
	}
	return parent, ranges
}

// A large collection is split into _id ranges copied by several workers. The ranges
// cover _ids of many BSON types, resume after repeated interruptions without copying
// a document twice or skipping one, and changes made during the copy are replayed.
func TestContainer_SplitCollectionCopy(t *testing.T) {
	p := newPair(t)
	db := "split"
	seedDocs(t, p.srcC.Database(db), "big", 8000, 5, mixedID)
	seedDocs(t, p.srcC.Database(db), "small", 300, 6, intID)
	cfg := p.cfg(t, "split", db)
	cfg.SnapshotBatch = 100
	cfg.SnapshotParallel = 4
	cfg.SplitMinDocs = 1000

	load := startLoad(t, p.srcC.Database(db), []string{"big", "small"}, 9000, 2, 9, mixedID)
	interrupted := 0
	for i := 0; i < 4; i++ {
		r := start(cfg)
		time.Sleep(time.Duration(300+150*i) * time.Millisecond)
		r.cancel()
		if err := <-r.done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		parent, ranges := rangeProgress(t, p, "split", "big")
		t.Logf("after stop %d: parts=%d done=%v ranges=%d", i, parent.Parts, parent.Done, len(ranges))
		if !parent.Done && len(ranges) > 0 {
			interrupted++
		}
	}
	r := start(cfg)
	time.Sleep(2 * time.Second)
	ops := load.halt(t)
	p.waitIdentical(t, db, 2*time.Minute, r)
	r.stop(t)
	t.Logf("%d concurrent ops, %d interruptions inside the split copy", ops, interrupted)

	parent, ranges := rangeProgress(t, p, "split", "big")
	// ~8000 documents (the load deletes some), 1000 per range at least, 4 workers x 4:
	// more ranges than workers.
	if parent.Parts <= 4 || len(ranges) != parent.Parts || !parent.Done {
		t.Fatalf("split: parent %+v, %d ranges", parent, len(ranges))
	}
	var sum int64
	for _, rp := range ranges {
		if !rp.Done || rp.Copied == 0 {
			t.Fatalf("range %d: done=%v copied=%d", rp.ID.Part, rp.Done, rp.Copied)
		}
		sum += rp.Copied
	}
	if sum != parent.Copied {
		t.Fatalf("ranges copied %d, collection record %d", sum, parent.Copied)
	}
	if small, ranges := rangeProgress(t, p, "split", "small"); small.Parts != 0 || len(ranges) != 0 || !small.Done {
		t.Fatalf("a collection below 2 x split_min_docs must not be split: %+v %d", small, len(ranges))
	}
}

// Without load the ranges add up to exactly the source documents: no document is
// copied twice or skipped at a range boundary, also across interruptions.
func TestContainer_SplitCollectionExactCount(t *testing.T) {
	p := newPair(t)
	db := "splitx"
	seedDocs(t, p.srcC.Database(db), "m", 9000, 7, mixedID)
	cfg := p.cfg(t, "splitx", db)
	cfg.SnapshotBatch = 100
	cfg.SnapshotParallel = 3
	cfg.SplitMinDocs = 1000
	for i := 0; i < 3; i++ {
		r := start(cfg)
		time.Sleep(time.Duration(250+200*i) * time.Millisecond)
		r.cancel()
		if err := <-r.done; err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	r := start(cfg)
	p.waitIdentical(t, db, time.Minute, r)
	r.stop(t)
	parent, ranges := rangeProgress(t, p, "splitx", "m")
	var sum int64
	for _, rp := range ranges {
		sum += rp.Copied
	}
	if parent.Parts != 9 || len(ranges) != 9 || sum != 9000 || parent.Copied != 9000 {
		t.Fatalf("parts=%d ranges=%d sum=%d parent=%d, want 9 ranges adding up to exactly 9000", parent.Parts, len(ranges), sum, parent.Copied)
	}
}

// A copy by one worker that was interrupted is split between the workers when it is
// resumed with several (a pipeline started with max_parallel 1, or by a version
// without split copies): the first range carries on from the last copied _id with
// the count so far, and the ranges add up to exactly the source documents.
func TestContainer_SplitResumedOneRangeCopy(t *testing.T) {
	p := newPair(t)
	db := "resplit"
	seedDocs(t, p.srcC.Database(db), "m", 20000, 8, mixedID)
	cfg := p.cfg(t, "resplit", db)
	cfg.SnapshotBatch = 50
	cfg.SnapshotParallel = 1
	cfg.SplitMinDocs = 1000

	ctx := context.Background()
	progress := p.dstC.Database(stateDatabase).Collection(progressCollection)
	r := start(cfg)
	deadline := time.Now().Add(time.Minute)
	for {
		var pr collProgress
		_ = progress.FindOne(ctx, bson.D{{Key: "_id", Value: progressID{Pipeline: "resplit", Collection: "m"}}}).Decode(&pr)
		if pr.Copied >= 2000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the one-worker copy did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	r.cancel()
	if err := <-r.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	before, ranges := rangeProgress(t, p, "resplit", "m")
	if before.Done || before.Parts > 1 || len(ranges) != 0 || before.Copied == 0 {
		t.Fatalf("want an unfinished one-range copy, got %+v and %d ranges", before, len(ranges))
	}

	cfg.SnapshotParallel = 4
	r = start(cfg)
	p.waitIdentical(t, db, time.Minute, r)
	r.stop(t)
	parent, ranges := rangeProgress(t, p, "resplit", "m")
	var sum int64
	for _, rp := range ranges {
		if !rp.Done {
			t.Fatalf("range %d not done", rp.ID.Part)
		}
		sum += rp.Copied
	}
	if parent.Parts < 2 || len(ranges) != parent.Parts || sum != 20000 || parent.Copied != 20000 || !parent.Done {
		t.Fatalf("resumed copy: parts=%d ranges=%d sum=%d parent=%d done=%v (copied %d before the restart), want it split with ranges adding up to 20000",
			parent.Parts, len(ranges), sum, parent.Copied, parent.Done, before.Copied)
	}
	if ranges[0].Copied <= before.Copied {
		t.Fatalf("the first range must carry the %d documents copied before the restart: %d", before.Copied, ranges[0].Copied)
	}
}
