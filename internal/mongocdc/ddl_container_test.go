package mongocdc

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func indexCanon(t *testing.T, c *mongo.Collection) string {
	specs, err := listIndexSpecs(context.Background(), c)
	if err != nil {
		return "err: " + err.Error()
	}
	var out []string
	for _, s := range specs {
		out = append(out, string(s.canon))
	}
	sort.Strings(out)
	return strings.Join(out, "|")
}

func validatorOf(t *testing.T, db *mongo.Database, coll string) bson.Raw {
	cur, err := db.ListCollections(context.Background(), bson.D{{Key: "name", Value: coll}})
	if err != nil {
		t.Fatal(err)
	}
	defer cur.Close(context.Background())
	var info collectionInfo
	if cur.Next(context.Background()) {
		_ = cur.Decode(&info)
	}
	v, _ := info.Options.LookupErr("validator")
	return v.Value
}

// mongodb.indexes: replicate — collection options at creation (validator), every index
// kind (compound, unique, TTL, partial, text, hidden) after the snapshot, later index
// creation/drop and validator changes; only Strand-created indexes are dropped.
func TestContainer_ReplicateIndexesAndOptions(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	db := "idx"
	sdb := p.srcC.Database(db)
	validator := bson.D{{Key: "$jsonSchema", Value: bson.D{{Key: "bsonType", Value: "object"}, {Key: "required", Value: bson.A{"v"}}}}}
	if err := sdb.CreateCollection(ctx, "c", options.CreateCollection().SetValidator(validator).SetValidationAction("error")); err != nil {
		t.Fatal(err)
	}
	seedDocs(t, sdb, "c", 2000, 1, intID)
	models := []mongo.IndexModel{
		{Keys: bson.D{{Key: "v", Value: 1}, {Key: "s", Value: -1}}, Options: options.Index().SetName("v_s")},
		{Keys: bson.D{{Key: "i64", Value: 1}}, Options: options.Index().SetName("uniq").SetUnique(true)},
		{Keys: bson.D{{Key: "date", Value: 1}}, Options: options.Index().SetName("ttl").SetExpireAfterSeconds(1 << 30)},
		{Keys: bson.D{{Key: "f", Value: 1}}, Options: options.Index().SetName("partial").SetPartialFilterExpression(bson.D{{Key: "v", Value: bson.D{{Key: "$gt", Value: 5}}}})},
		{Keys: bson.D{{Key: "s", Value: "text"}}, Options: options.Index().SetName("txt")},
		{Keys: bson.D{{Key: "oid", Value: 1}}, Options: options.Index().SetName("hidden").SetHidden(true)},
	}
	if _, err := sdb.Collection("c").Indexes().CreateMany(ctx, models); err != nil {
		t.Fatal(err)
	}
	cfg := p.cfg(t, "idx", db)
	cfg.ReplicateIndexes = true
	cfg.IndexReconcileEvery = time.Second
	r := start(cfg)
	p.waitIdentical(t, db, time.Minute, r)
	tc := p.dstC.Database(db).Collection("c")
	waitEq := func(what string) {
		t.Helper()
		deadline := time.Now().Add(time.Minute)
		for {
			r.failIfExited(t)
			if a, b := indexCanon(t, sdb.Collection("c")), indexCanon(t, tc); a == b {
				return
			} else if time.Now().After(deadline) {
				t.Fatalf("%s: indexes differ\nsource %s\ntarget %s", what, a, b)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	waitEq("after snapshot")
	if !bytes.Equal(validatorOf(t, sdb, "c"), validatorOf(t, p.dstC.Database(db), "c")) {
		t.Fatal("validator not replicated at creation")
	}
	// Target-only index made by an operator is kept.
	if _, err := tc.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "nested.z", Value: 1}}, Options: options.Index().SetName("local_only")}); err != nil {
		t.Fatal(err)
	}
	if err := sdb.Collection("c").Indexes().DropOne(ctx, "partial"); err != nil {
		t.Fatal(err)
	}
	if _, err := sdb.Collection("c").Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "v", Value: -1}}, Options: options.Index().SetName("later")}); err != nil {
		t.Fatal(err)
	}
	newValidator := bson.D{{Key: "v", Value: bson.D{{Key: "$exists", Value: true}}}}
	if err := sdb.RunCommand(ctx, bson.D{{Key: "collMod", Value: "c"}, {Key: "validator", Value: newValidator}, {Key: "validationLevel", Value: "moderate"}}).Err(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		names := map[string]bool{}
		specs, _ := listIndexSpecs(ctx, tc)
		for n := range specs {
			names[n] = true
		}
		if names["later"] && !names["partial"] && names["local_only"] && bytes.Equal(validatorOf(t, sdb, "c"), validatorOf(t, p.dstC.Database(db), "c")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("index/validator changes not reconciled: %v", names)
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Writes keep flowing (bypassDocumentValidation) and stay identical.
	load := startLoad(t, sdb, []string{"c"}, 2500, 2, 3, intID)
	time.Sleep(2 * time.Second)
	load.halt(t)
	p.waitIdentical(t, db, time.Minute, r)
	r.stop(t)
}

func TestContainer_CreateEmptyCollectionWithValidatorAndIndex(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	db := "empty_create"
	cfg := p.cfg(t, "empty-create", db)
	cfg.ReplicateIndexes = true
	cfg.IndexReconcileEvery = time.Hour
	cfg.Observer = NewObserver("empty-create")
	r := start(cfg)
	defer r.stop(t)
	// Wait until the stream is established before issuing the DDL.
	deadline := time.Now().Add(time.Minute)
	for cfg.Observer.Snapshot().Phase != PhaseStreaming {
		if time.Now().After(deadline) {
			t.Fatal("pipeline did not reach streaming")
		}
		time.Sleep(20 * time.Millisecond)
	}
	validator := bson.D{{Key: "v", Value: bson.D{{Key: "$exists", Value: true}}}}
	if err := p.srcC.Database(db).CreateCollection(ctx, "empty", options.CreateCollection().SetValidator(validator)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.srcC.Database(db).Collection("empty").Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "v", Value: 1}}, Options: options.Index().SetName("v_idx"),
	}); err != nil {
		t.Fatal(err)
	}
	target := p.dstC.Database(db)
	deadline = time.Now().Add(time.Minute)
	for {
		names, err := target.ListCollectionNames(ctx, bson.D{{Key: "name", Value: "empty"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(names) == 1 && bytes.Equal(validatorOf(t, p.srcC.Database(db), "empty"), validatorOf(t, target, "empty")) {
			if _, ok := func() (indexSpec, bool) {
				specs, e := listIndexSpecs(ctx, target.Collection("empty"))
				if e != nil {
					return indexSpec{}, false
				}
				s, ok := specs["v_idx"]
				return s, ok
			}(); ok {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("empty collection metadata was not replicated")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// A collection re-copy (repair) is checkpointed per batch: interrupted, it resumes from
// its last batch (no restart from zero, nothing skipped); a rename past the snapshot
// overlap renames the target collection in place.
func TestContainer_RecopyResumesAndRenameInPlace(t *testing.T) {
	p := newPair(t)
	ctx := context.Background()
	db := "recopy"
	seedDocs(t, p.srcC.Database(db), "big", 20000, 1, mixedID)
	cfg := p.cfg(t, "recopy", db)
	cfg.SnapshotBatch = 100
	obs := NewObserver("recopy")
	cfg.Observer = obs
	r := start(cfg)
	p.waitIdentical(t, db, time.Minute, r)
	for obs.Snapshot().Phase != PhaseStreaming {
		time.Sleep(50 * time.Millisecond)
	}
	if err := obs.RequestRecopy(ctx, "big"); err != nil {
		t.Fatal(err)
	}
	progress := p.dstC.Database(stateDatabase).Collection(progressCollection)
	var pr collProgress
	for {
		if err := progress.FindOne(ctx, bson.D{{Key: "_id", Value: progressID{"recopy", "big"}}}).Decode(&pr); err == nil && pr.Copied >= 3000 {
			break
		}
		r.failIfExited(t)
		time.Sleep(20 * time.Millisecond)
	}
	r.cancel() // crash mid re-copy
	if err := <-r.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_ = progress.FindOne(ctx, bson.D{{Key: "_id", Value: progressID{"recopy", "big"}}}).Decode(&pr)
	stoppedAt := pr.Copied
	if pr.Kind != progressRecopy || stoppedAt == 0 || stoppedAt >= 20000 {
		t.Fatalf("re-copy progress after interruption: %+v", pr)
	}
	// Resume: the target copy continues from the last batch.
	cfg.Observer = NewObserver("recopy")
	r = start(cfg)
	p.waitIdentical(t, db, time.Minute, r)
	if n, _ := progress.CountDocuments(ctx, bson.D{{Key: "_id", Value: progressID{"recopy", "big"}}}); n != 0 {
		t.Fatal("finished re-copy record not removed")
	}
	snap := cfg.Observer.Snapshot()
	var resumed int64
	for _, c := range snap.Collections {
		if c.Name == "big" {
			resumed = c.Copied
		}
	}
	if resumed != 20000 || snap.SnapshotDocs != 20000-stoppedAt {
		t.Fatalf("re-copy did not resume: total %d, copied after restart %d, before %d", resumed, snap.SnapshotDocs, stoppedAt)
	}
	t.Logf("re-copy interrupted at %d documents, resumed with %d more", stoppedAt, snap.SnapshotDocs)

	// Rename in place (past the overlap): quick, and later writes land in the new name.
	began := time.Now()
	if err := p.srcC.Database("admin").RunCommand(ctx, bson.D{{Key: "renameCollection", Value: db + ".big"}, {Key: "to", Value: db + ".big2"}}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.srcC.Database(db).Collection("big2").InsertOne(ctx, bson.D{{Key: "_id", Value: "after-rename"}}); err != nil {
		t.Fatal(err)
	}
	p.waitIdentical(t, db, time.Minute, r)
	if cfg.Observer.Snapshot().SnapshotDocs != 20000-stoppedAt {
		t.Fatal("rename past the overlap re-copied the collection")
	}
	t.Logf("rename of a 20000-document collection applied in %s without re-copy", time.Since(began).Round(time.Millisecond))
	r.stop(t)
}
