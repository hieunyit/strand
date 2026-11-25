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
	if parent.Parts != 4 || len(ranges) != 4 || !parent.Done {
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
	if parent.Parts != 3 || len(ranges) != 3 || sum != 9000 || parent.Copied != 9000 {
		t.Fatalf("parts=%d ranges=%d sum=%d parent=%d, want 3 ranges adding up to exactly 9000", parent.Parts, len(ranges), sum, parent.Copied)
	}
}
