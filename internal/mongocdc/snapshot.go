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
			if err := r.lease.check(ctx); err != nil {
				return err
			}
			if err := r.target.Collection(name).Drop(ctx); err != nil {
				return fmt.Errorf("drop stale target collection %s: %w", name, err)
			}
		}
	}
	type job struct {
		info     collectionInfo
		estimate int64
	}
	var jobs []job
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
		jobs = append(jobs, job{in, est})
	}
	// Largest first: with parallel workers the longest copy starts immediately.
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].estimate > jobs[j].estimate })

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
			p := progress[j.info.Name]
			if p == nil {
				var err error
				if p, err = r.startCopy(workCtx, slot, j.info, progressSnapshot, j.estimate); err != nil {
					cancel(fmt.Errorf("prepare target collection %s: %w", j.info.Name, err))
					return
				}
			}
			if err := r.copyCollection(workCtx, slot, p); err != nil {
				cancel(fmt.Errorf("copy collection %s.%s: %w", cfg.SourceDB, j.info.Name, err))
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

// startCopy (re)creates the target collection and records an empty progress document.
// The target collection is dropped first: it is either ours from a crashed attempt
// (snapshot started) or explicitly allowed to be overwritten.
func (r *runner) startCopy(ctx context.Context, slot int, info collectionInfo, kind string, estimate int64) (*collProgress, error) {
	if err := r.lease.check(ctx); err != nil {
		return nil, err
	}
	if err := r.recreateTargetCollection(ctx, info); err != nil {
		return nil, err
	}
	p := &collProgress{ID: progressID{r.cfg.Pipeline, info.Name}, Kind: kind, Estimate: estimate}
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
	if err := dropCollectionIfPresent(ctx, r.target, info.Name); err != nil {
		return err
	}
	cmd := bson.D{{Key: "create", Value: info.Name}}
	if r.cfg.ReplicateIndexes {
		cmd = append(cmd, creatableOptions(info.Options)...)
	}
	return r.target.RunCommand(ctx, cmd).Err()
}

// copyCollection pages through the source collection in _id order from the progress
// point. Resume uses the _id index bound min() (not $gt, which would type-bracket and
// skip _ids of other BSON types); the inclusive bound document is skipped by exact byte
// comparison. Documents changed during the scan are fixed up by change-stream replay.
func (r *runner) copyCollection(ctx context.Context, slot int, prog *collProgress) error {
	log := r.cfg.Logger
	name := prog.ID.Collection
	findOpts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetHint(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(r.cfg.SnapshotBatch))
	if !prog.LastID.IsZero() {
		findOpts.SetMin(bson.D{{Key: "_id", Value: prog.LastID}})
	}
	r.obs.setCollection(name, CollRunning, prog.Kind, prog.Copied, prog.Estimate)
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
			log.Info("MongoDB snapshot progress", "pipeline", r.cfg.Pipeline, "collection", name, "copied", prog.Copied, "estimate", prog.Estimate, "docs_per_sec", int64(rate))
		}
		if sb.last {
			if err := r.finishCopy(ctx, slot, prog); err != nil {
				return err
			}
			log.Info("MongoDB collection copy complete", "pipeline", r.cfg.Pipeline, "collection", r.cfg.SourceDB+"."+name, "kind", prog.Kind, "documents", prog.Copied, "seconds", time.Since(start).Seconds())
			return nil
		}
	}
}

// finishCopy marks a snapshot copy done (a re-copy's record is removed by its caller,
// together with the checkpoint that follows it).
func (r *runner) finishCopy(ctx context.Context, slot int, prog *collProgress) error {
	if prog.Kind == progressSnapshot {
		err := r.lease.txn(ctx, slot, func(tx context.Context) error {
			_, err := r.progress.UpdateOne(tx, bson.D{{Key: "_id", Value: prog.ID}}, bson.D{{Key: "$set", Value: bson.D{{Key: "done", Value: true}}}})
			return err
		})
		if err != nil {
			return err
		}
	}
	prog.Done = true
	r.obs.setCollection(prog.ID.Collection, CollDone, prog.Kind, prog.Copied, max(prog.Estimate, prog.Copied))
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
