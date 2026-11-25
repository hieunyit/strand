package mongocdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Error codes that mean "the collection went away while we were scanning it".
const (
	codeNamespaceNotFound = 26
	codeQueryPlanKilled   = 175
	codeCursorKilled      = 237
)

type collectionInfo struct {
	Name    string   `bson:"name"`
	Type    string   `bson:"type"`
	Options bson.Raw `bson:"options"`
}

func listSelectedCollections(ctx context.Context, source *mongo.Database, cfg Config) ([]collectionInfo, error) {
	cursor, err := source.ListCollections(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list source collections: %w", err)
	}
	defer cursor.Close(ctx)
	var out []collectionInfo
	for cursor.Next(ctx) {
		var info collectionInfo
		if err := cursor.Decode(&info); err != nil {
			return nil, fmt.Errorf("decode source collection metadata: %w", err)
		}
		qualified := cfg.SourceDB + "." + info.Name
		if strings.HasPrefix(info.Name, "system.") || !selected(cfg, qualified) {
			continue
		}
		if info.Type != "collection" {
			return nil, fmt.Errorf("%s has MongoDB collection type %q; only ordinary collections can be replicated (add an exclude pattern)", qualified, info.Type)
		}
		info.Options = append(bson.Raw(nil), info.Options...)
		out = append(out, info)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("read source collection metadata: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (r *runner) loadProgress(ctx context.Context, kind string) (map[string]*collProgress, error) {
	cur, err := r.progress.Find(ctx, bson.D{{Key: "_id.p", Value: r.cfg.Pipeline}, {Key: "kind", Value: kind}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	out := map[string]*collProgress{}
	for cur.Next(ctx) {
		var p collProgress
		if err := cur.Decode(&p); err != nil {
			return nil, err
		}
		p.LastID = bson.RawValue{Type: p.LastID.Type, Value: append([]byte(nil), p.LastID.Value...)}
		out[p.ID.Collection] = &p
	}
	return out, cur.Err()
}

// snapshot copies every selected collection, up to SnapshotParallel at a time (largest
// first), resuming per collection from the last committed _id. Each written batch
// commits atomically with its progress record, fenced by the worker's lease slot.
func (r *runner) snapshot(ctx context.Context) error {
	cfg := r.cfg
	infos, err := listSelectedCollections(ctx, r.source, cfg)
	if err != nil {
		return err
	}
	progress, err := r.loadProgress(ctx, progressSnapshot)
	if err != nil {
		return fmt.Errorf("load MongoDB snapshot progress: %w", err)
	}
	sourceNames := make(map[string]bool, len(infos))
	for _, in := range infos {
		sourceNames[in.Name] = true
	}
	if cfg.OverwriteTarget {
		targetNames, err := r.target.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return fmt.Errorf("list target collections before snapshot: %w", err)
		}
		for _, name := range targetNames {
			if strings.HasPrefix(name, "system.") || sourceNames[name] || progress[name] != nil || !selected(cfg, cfg.SourceDB+"."+name) {
				continue
			}
			if err := r.dropTargetCollection(ctx, name); err != nil {
				return fmt.Errorf("drop stale target collection %s: %w", name, err)
			}
		}
	}
	var colls []*collCopy
	for _, in := range infos {
		est, _ := r.source.Collection(in.Name).EstimatedDocumentCount(ctx)
		p := progress[in.Name]
		switch {
		case p != nil && p.Done:
			r.obs.setCollection(in.Name, CollDone, progressSnapshot, p.Copied, max(est, p.Copied))
			continue
		case p != nil:
			r.obs.setCollection(in.Name, CollPending, progressSnapshot, p.Copied, est)
		default:
			r.obs.setCollection(in.Name, CollPending, progressSnapshot, 0, est)
		}
		c := &collCopy{info: in, estimate: est, parent: p, parts: 1}
		switch {
		case p != nil && p.Parts > 1:
			c.parts = p.Parts // a split copy resumes with its ranges
		case p == nil:
			c.parts = r.splitParts(in, est)
		}
		colls = append(colls, c)
	}
	// Largest first: with parallel workers the longest copy starts immediately; the
	// ranges of a split collection are handed out one after the other.
	sort.SliceStable(colls, func(i, j int) bool { return colls[i].estimate > colls[j].estimate })
	type job struct {
		c    *collCopy
		part int // 1..c.parts
	}
	var jobs []job
	for _, c := range colls {
		for part := 1; part <= c.parts; part++ {
			jobs = append(jobs, job{c, part})
		}
	}

	workCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	slots := make(chan int, cfg.SnapshotParallel)
	for s := 1; s <= cfg.SnapshotParallel; s++ {
		slots <- s
	}
	var wg sync.WaitGroup
	for _, j := range jobs {
		var slot int
		select {
		case slot = <-slots:
		case <-workCtx.Done():
		}
		if workCtx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(j job, slot int) {
			defer wg.Done()
			defer func() { slots <- slot }()
			c := j.c
			c.once.Do(func() { c.err = r.prepareCopy(workCtx, slot, c) })
			if c.err != nil {
				cancel(fmt.Errorf("prepare target collection %s: %w", c.info.Name, c.err))
				return
			}
			if err := r.copyPart(workCtx, slot, c, j.part); err != nil {
				cancel(fmt.Errorf("copy collection %s.%s: %w", cfg.SourceDB, c.info.Name, err))
			}
		}(j, slot)
	}
	wg.Wait()
	if err := context.Cause(workCtx); err != nil && workCtx.Err() != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	return nil
}

// collCopy is the snapshot copy of one collection: one range, or several ranges of
// _id copied by different workers (split copy).
type collCopy struct {
	info     collectionInfo
	estimate int64
	parts    int // planned ranges; prepareCopy may lower it (fewer distinct _id bounds)
	once     sync.Once
	err      error
	parent   *collProgress   // the collection's progress document
	ranges   []*collProgress // split copy: ranges 1..parts
	mu       sync.Mutex
	left     int // ranges not finished
	start    time.Time
}

// splitParts is the number of ranges a new copy of a collection is split into: one
// per worker, each of at least SplitMinDocs documents. A collection with a non-simple
// default collation is not split (its _id order follows the collation).
func (r *runner) splitParts(info collectionInfo, estimate int64) int {
	if r.cfg.SnapshotParallel <= 1 || r.cfg.SplitMinDocs <= 0 || estimate < 2*int64(r.cfg.SplitMinDocs) {
		return 1
	}
	if _, err := info.Options.LookupErr("collation"); err == nil {
		return 1
	}
	return int(min(int64(r.cfg.SnapshotParallel), estimate/int64(r.cfg.SplitMinDocs)))
}

// prepareCopy creates the target collection and the progress records of a new copy,
// or loads the ranges of a split copy that is resumed.
func (r *runner) prepareCopy(ctx context.Context, slot int, c *collCopy) error {
	c.start = time.Now()
	if c.parent != nil {
		if c.parts <= 1 {
			return nil
		}
		return r.loadRanges(ctx, c)
	}
	var bounds []bson.RawValue
	if c.parts > 1 {
		var err error
		if bounds, err = r.sampleBounds(ctx, c.info.Name, c.parts); err != nil {
			r.cfg.Logger.Warn("MongoDB: cannot sample _id bounds; the collection is copied by one worker", "collection", r.cfg.SourceDB+"."+c.info.Name, "error", err)
			bounds = nil
		}
		c.parts = len(bounds) + 1
	}
	if c.parts <= 1 {
		p, err := r.startCopy(ctx, slot, c.info, progressSnapshot, c.estimate)
		c.parent = p
		return err
	}
	if err := r.recreateTargetCollection(ctx, c.info); err != nil {
		return err
	}
	parent := &collProgress{ID: progressID{Pipeline: r.cfg.Pipeline, Collection: c.info.Name}, Kind: progressSnapshot, Estimate: c.estimate, Parts: c.parts}
	ranges := make([]*collProgress, c.parts)
	for i := range ranges {
		rp := &collProgress{ID: progressID{Pipeline: r.cfg.Pipeline, Collection: c.info.Name, Part: i + 1}, Kind: progressPart}
		if i > 0 {
			rp.Lo = bounds[i-1]
		}
		if i < len(bounds) {
			rp.Hi = bounds[i]
		}
		ranges[i] = rp
	}
	err := r.lease.txn(ctx, slot, func(tx context.Context) error {
		if _, err := r.progress.DeleteMany(tx, bson.D{{Key: "_id.p", Value: r.cfg.Pipeline}, {Key: "_id.c", Value: c.info.Name}, {Key: "kind", Value: progressPart}}); err != nil {
			return err
		}
		if _, err := r.progress.ReplaceOne(tx, bson.D{{Key: "_id", Value: parent.ID}}, parent, options.Replace().SetUpsert(true)); err != nil {
			return err
		}
		docs := make([]any, len(ranges))
		for i, rp := range ranges {
			docs[i] = rp
		}
		_, err := r.progress.InsertMany(tx, docs)
		return err
	})
	if err != nil {
		return err
	}
	c.parent, c.ranges, c.left = parent, ranges, len(ranges)
	r.cfg.Logger.Info("MongoDB collection split between workers", "pipeline", r.cfg.Pipeline, "collection", r.cfg.SourceDB+"."+c.info.Name, "ranges", c.parts, "estimate", c.estimate)
	return nil
}

// loadRanges reads the ranges of a split copy being resumed.
func (r *runner) loadRanges(ctx context.Context, c *collCopy) error {
	cur, err := r.progress.Find(ctx, bson.D{{Key: "_id.p", Value: r.cfg.Pipeline}, {Key: "_id.c", Value: c.info.Name}, {Key: "kind", Value: progressPart}})
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	ranges := make([]*collProgress, c.parts)
	for cur.Next(ctx) {
		var rp collProgress
		if err := cur.Decode(&rp); err != nil {
			return err
		}
		if rp.ID.Part < 1 || rp.ID.Part > c.parts {
			continue
		}
		for _, v := range []*bson.RawValue{&rp.LastID, &rp.Lo, &rp.Hi} {
			*v = bson.RawValue{Type: v.Type, Value: append([]byte(nil), v.Value...)}
		}
		ranges[rp.ID.Part-1] = &rp
	}
	if err := cur.Err(); err != nil {
		return err
	}
	var copied int64
	for i, rp := range ranges {
		if rp == nil {
			return fmt.Errorf("progress of range %d/%d of %s is missing; start the pipeline under a new name", i+1, c.parts, c.info.Name)
		}
		copied += rp.Copied
		if !rp.Done {
			c.left++
		}
	}
	c.ranges = ranges
	r.obs.setCollection(c.info.Name, CollPending, progressSnapshot, copied, c.estimate)
	return nil
}

// sampleBounds picks up to parts-1 _id values that split the collection into ranges
// of about the same number of documents, from a random sample sorted in _id order.
func (r *runner) sampleBounds(ctx context.Context, name string, parts int) ([]bson.RawValue, error) {
	size := min(parts*100, 10000)
	cur, err := r.source.Collection(name).Aggregate(ctx, mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: size}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: 1}}}},
	})
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var ids []bson.RawValue
	for cur.Next(ctx) {
		id := cur.Current.Lookup("_id")
		ids = append(ids, bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)})
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}
	var bounds []bson.RawValue
	for k := 1; k < parts && len(ids) > 0; k++ {
		b := ids[k*len(ids)/parts]
		if n := len(bounds); n > 0 && bounds[n-1].Type == b.Type && bytes.Equal(bounds[n-1].Value, b.Value) {
			continue
		}
		bounds = append(bounds, b)
	}
	return bounds, nil
}

// copyPart copies range part of a collection (part 1 of a collection copied as one
// range is the whole collection).
func (r *runner) copyPart(ctx context.Context, slot int, c *collCopy, part int) error {
	if len(c.ranges) == 0 {
		if part > 1 {
			return nil // prepareCopy found fewer bounds than planned
		}
		return r.copyCollection(ctx, slot, c.parent)
	}
	if part > len(c.ranges) {
		return nil
	}
	rp := c.ranges[part-1]
	finished := false
	if !rp.Done {
		r.obs.setCollectionState(c.info.Name, CollRunning)
		if err := r.copyCollection(ctx, slot, rp); err != nil {
			return err
		}
		finished = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if finished {
		c.left--
	}
	// The last range to finish (or, after a crash between the last range and the
	// collection record, the first range looked at) marks the collection copied.
	if c.left > 0 || c.parent.Done {
		return nil
	}
	var copied int64
	for _, x := range c.ranges {
		copied += x.Copied
	}
	err := r.lease.txn(ctx, slot, func(tx context.Context) error {
		_, err := r.progress.UpdateOne(tx, bson.D{{Key: "_id", Value: c.parent.ID}}, bson.D{{Key: "$set", Value: bson.D{{Key: "done", Value: true}, {Key: "copied", Value: copied}}}})
		return err
	})
	if err != nil {
		return err
	}
	c.parent.Done, c.parent.Copied = true, copied
	r.obs.setCollection(c.info.Name, CollDone, progressSnapshot, copied, max(c.estimate, copied))
	r.cfg.Logger.Info("MongoDB collection copy complete", "pipeline", r.cfg.Pipeline, "collection", r.cfg.SourceDB+"."+c.info.Name, "kind", progressSnapshot,
		"documents", copied, "ranges", len(c.ranges), "seconds", time.Since(c.start).Seconds())
	return nil
}

// startCopy (re)creates the target collection and records an empty progress document.
// The target collection is dropped first: it is either ours from a crashed attempt
// (snapshot started) or explicitly allowed to be overwritten.
func (r *runner) startCopy(ctx context.Context, slot int, info collectionInfo, kind string, estimate int64) (*collProgress, error) {
	if err := r.recreateTargetCollection(ctx, info); err != nil {
		return nil, err
	}
	p := &collProgress{ID: progressID{Pipeline: r.cfg.Pipeline, Collection: info.Name}, Kind: kind, Estimate: estimate}
	err := r.lease.txn(ctx, slot, func(tx context.Context) error {
		_, err := r.progress.ReplaceOne(tx, bson.D{{Key: "_id", Value: p.ID}}, p, options.Replace().SetUpsert(true))
		return err
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

// recreateTargetCollection drops and creates the target collection, with the source's
// collection options when indexes/options are replicated.
func (r *runner) recreateTargetCollection(ctx context.Context, info collectionInfo) error {
	return r.lease.withDDL(ctx, "recreate collection "+info.Name, func() error {
		if err := dropCollectionIfPresent(ctx, r.target, info.Name); err != nil {
			return err
		}
		cmd := bson.D{{Key: "create", Value: info.Name}}
		if r.cfg.ReplicateIndexes {
			cmd = append(cmd, creatableOptions(info.Options)...)
		}
		return r.target.RunCommand(ctx, append(cmd, bson.E{Key: "writeConcern", Value: durableDDLWriteConcern()})).Err()
	})
}

func (r *runner) dropTargetCollection(ctx context.Context, name string) error {
	return r.lease.withDDL(ctx, "drop collection "+name, func() error {
		return dropCollectionIfPresent(ctx, r.target, name)
	})
}

// copyCollection pages through the source collection in _id order from the progress
// point. Resume uses the _id index bound min() (not $gt, which would type-bracket and
// skip _ids of other BSON types); the inclusive bound document is skipped by exact byte
// comparison. Documents changed during the scan are fixed up by change-stream replay.
func (r *runner) copyCollection(ctx context.Context, slot int, prog *collProgress) error {
	log := r.cfg.Logger
	name := prog.ID.Collection
	findOpts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetHint(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(r.cfg.SnapshotBatch))
	switch {
	case !prog.LastID.IsZero():
		findOpts.SetMin(bson.D{{Key: "_id", Value: prog.LastID}})
	case !prog.Lo.IsZero():
		findOpts.SetMin(bson.D{{Key: "_id", Value: prog.Lo}}) // inclusive: the previous range ends before it
	}
	if !prog.Hi.IsZero() {
		findOpts.SetMax(bson.D{{Key: "_id", Value: prog.Hi}}) // exclusive
	}
	part := prog.Kind == progressPart
	if !part {
		r.obs.setCollection(name, CollRunning, prog.Kind, prog.Copied, prog.Estimate)
	}
	// The copy must not read a state older than the stream position it is replayed
	// from: a majority read may return a snapshot that lags behind the change-stream
	// start token captured just before (observed on 4.4: changes between the two were
	// neither copied nor replayed). Reading with afterClusterTime >= the source's
	// latest applied time makes the server wait until the majority snapshot covers it.
	at, err := r.sourceTime(ctx)
	if err != nil {
		return err
	}
	sess, err := r.srcClient.StartSession(options.Session().SetCausalConsistency(true))
	if err != nil {
		return err
	}
	defer sess.EndSession(context.Background())
	if err := sess.AdvanceOperationTime(&at); err != nil {
		return err
	}
	cursor, err := r.source.Collection(name).Find(mongo.NewSessionContext(ctx, sess), bson.D{}, findOpts)
	if err != nil {
		if verr := r.vanished(ctx, name, err); verr != nil {
			return verr
		}
		return r.finishCopy(ctx, slot, prog)
	}
	// The reader fills the next batch while the previous one commits on the target.
	readCtx, stopRead := context.WithCancel(ctx)
	batches := make(chan snapBatch, 2)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		defer cursor.Close(context.Background())
		r.readSnapshot(readCtx, cursor, prog.LastID, batches)
	}()
	defer func() {
		stopRead()
		<-readDone
	}()

	start := time.Now()
	lastLog := start
	startCopied := prog.Copied
	target := r.target.Collection(name)
	insertOpts := options.InsertMany().SetOrdered(false)
	if r.cfg.ReplicateIndexes {
		insertOpts.SetBypassDocumentValidation(true)
	}
	for {
		if err := r.obs.waitIfPaused(ctx, prog.Kind == progressSnapshot); err != nil {
			return err
		}
		var sb snapBatch
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sb = <-batches:
		}
		if sb.err != nil {
			if verr := r.vanished(ctx, name, sb.err); verr != nil {
				return verr
			}
			sb.last = true
		}
		if len(sb.docs) > 0 {
			err := r.lease.txn(ctx, slot, func(tx context.Context) error {
				if _, err := target.InsertMany(tx, sb.docs, insertOpts); err != nil {
					return err
				}
				_, err := r.progress.UpdateOne(tx, bson.D{{Key: "_id", Value: prog.ID}}, bson.D{
					{Key: "$set", Value: bson.D{{Key: "last_id", Value: sb.lastID}}},
					{Key: "$inc", Value: bson.D{{Key: "copied", Value: int64(len(sb.docs))}}},
				})
				return err
			})
			if err != nil {
				return err
			}
			prog.LastID = sb.lastID
			prog.Copied += int64(len(sb.docs))
			r.obs.copied(name, int64(len(sb.docs)))
		}
		if time.Since(lastLog) >= progressLogInterval {
			lastLog = time.Now()
			rate := float64(prog.Copied-startCopied) / time.Since(start).Seconds()
			if part {
				log.Info("MongoDB snapshot progress", "pipeline", r.cfg.Pipeline, "collection", name, "range", prog.ID.Part, "copied", prog.Copied, "docs_per_sec", int64(rate))
			} else {
				log.Info("MongoDB snapshot progress", "pipeline", r.cfg.Pipeline, "collection", name, "copied", prog.Copied, "estimate", prog.Estimate, "docs_per_sec", int64(rate))
			}
		}
		if sb.last {
			if err := r.finishCopy(ctx, slot, prog); err != nil {
				return err
			}
			if part {
				log.Info("MongoDB collection range copied", "pipeline", r.cfg.Pipeline, "collection", r.cfg.SourceDB+"."+name, "range", prog.ID.Part, "documents", prog.Copied, "seconds", time.Since(start).Seconds())
				return nil
			}
			log.Info("MongoDB collection copy complete", "pipeline", r.cfg.Pipeline, "collection", r.cfg.SourceDB+"."+name, "kind", prog.Kind, "documents", prog.Copied, "seconds", time.Since(start).Seconds())
			return nil
		}
	}
}

// finishCopy marks a snapshot copy done (a re-copy's record is removed by its caller,
// together with the checkpoint that follows it).
func (r *runner) finishCopy(ctx context.Context, slot int, prog *collProgress) error {
	if prog.Kind == progressSnapshot || prog.Kind == progressPart {
		err := r.lease.txn(ctx, slot, func(tx context.Context) error {
			_, err := r.progress.UpdateOne(tx, bson.D{{Key: "_id", Value: prog.ID}}, bson.D{{Key: "$set", Value: bson.D{{Key: "done", Value: true}}}})
			return err
		})
		if err != nil {
			return err
		}
	}
	prog.Done = true
	if prog.Kind != progressPart { // a split copy is done when its last range is (copyPart)
		r.obs.setCollection(prog.ID.Collection, CollDone, prog.Kind, prog.Copied, max(prog.Estimate, prog.Copied))
	}
	return nil
}

// snapBatch is one snapshot write batch; last marks the end of the scan, err a scan error.
type snapBatch struct {
	docs   []any
	lastID bson.RawValue
	last   bool
	err    error
}

func (r *runner) readSnapshot(ctx context.Context, cursor *mongo.Cursor, resumeID bson.RawValue, out chan<- snapBatch) {
	send := func(sb snapBatch) bool {
		select {
		case out <- sb:
			return true
		case <-ctx.Done():
			return false
		}
	}
	sb := snapBatch{docs: make([]any, 0, r.cfg.SnapshotBatch)}
	size := 0
	first := true
	for cursor.Next(ctx) {
		raw := cursor.Current
		id, err := raw.LookupErr("_id")
		if err != nil {
			send(snapBatch{err: fmt.Errorf("source document without _id: %w", err)})
			return
		}
		if first {
			first = false
			if !resumeID.IsZero() && id.Type == resumeID.Type && bytes.Equal(id.Value, resumeID.Value) {
				continue // inclusive min() bound: already copied
			}
		}
		doc := append(bson.Raw(nil), raw...)
		sb.docs = append(sb.docs, doc)
		sb.lastID = bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
		size += len(doc)
		if len(sb.docs) >= r.cfg.SnapshotBatch || size >= snapshotBatchBytes {
			if !send(sb) {
				return
			}
			sb = snapBatch{docs: make([]any, 0, r.cfg.SnapshotBatch)}
			size = 0
		}
	}
	if err := cursor.Err(); err != nil {
		if len(sb.docs) > 0 && !send(sb) {
			return
		}
		send(snapBatch{err: err})
		return
	}
	sb.last = true
	send(sb)
}

// vanished turns a scan error into success when the source collection was dropped or
// renamed during the scan: the change stream replays the drop/rename afterwards.
func (r *runner) vanished(ctx context.Context, name string, scanErr error) error {
	if !serverErrorCode(scanErr, codeNamespaceNotFound, codeQueryPlanKilled, codeCursorKilled) {
		return scanErr
	}
	names, err := r.source.ListCollectionNames(ctx, bson.D{{Key: "name", Value: name}})
	if err != nil {
		return errors.Join(scanErr, err)
	}
	if len(names) != 0 {
		return scanErr
	}
	r.cfg.Logger.Warn("MongoDB source collection disappeared during copy; change stream replay will apply the drop/rename", "collection", r.cfg.SourceDB+"."+name)
	return nil
}

func dropCollectionIfPresent(ctx context.Context, db *mongo.Database, name string) error {
	names, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: name}})
	if err != nil || len(names) == 0 {
		return err
	}
	return db.Collection(name).Drop(ctx)
}
