package mongocdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	Name string `bson:"name"`
	Type string `bson:"type"`
}

func listSelectedCollections(ctx context.Context, source *mongo.Database, cfg Config) ([]string, error) {
	cursor, err := source.ListCollections(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list source collections: %w", err)
	}
	defer cursor.Close(ctx)
	var names []string
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
			return nil, fmt.Errorf("%s has MongoDB collection type %q; only ordinary collections can be replicated (add an --exclude pattern)", qualified, info.Type)
		}
		names = append(names, info.Name)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("read source collection metadata: %w", err)
	}
	sort.Strings(names)
	return names, nil
}

// snapshot copies every selected collection, resuming per collection from the last
// committed _id. Each written batch commits atomically with its progress record.
func (r *runner) snapshot(ctx context.Context) error {
	cfg := r.cfg
	names, err := listSelectedCollections(ctx, r.source, cfg)
	if err != nil {
		return err
	}
	sourceNames := make(map[string]bool, len(names))
	for _, n := range names {
		sourceNames[n] = true
	}
	if cfg.OverwriteTarget {
		targetNames, err := r.target.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return fmt.Errorf("list target collections before snapshot: %w", err)
		}
		for _, name := range targetNames {
			if strings.HasPrefix(name, "system.") || sourceNames[name] || r.state.progress(name) != nil || !selected(cfg, cfg.SourceDB+"."+name) {
				continue
			}
			if err := r.target.Collection(name).Drop(ctx); err != nil {
				return fmt.Errorf("drop stale target collection %s: %w", name, err)
			}
		}
	}
	for _, name := range names {
		prog := r.state.progress(name)
		if prog != nil && prog.Done {
			continue
		}
		if prog == nil {
			// Not started: the target collection, if any, is either ours from a crashed
			// attempt (SnapshotStarted) or explicitly allowed to be overwritten.
			if err := recreateTargetCollection(ctx, r.target, name); err != nil {
				return fmt.Errorf("recreate target collection %s: %w", name, err)
			}
			r.state.Collections = append(r.state.Collections, collProgress{Name: name})
			if err := r.persist(ctx); err != nil {
				return err
			}
		}
		if err := r.copyCollection(ctx, name); err != nil {
			return fmt.Errorf("copy collection %s.%s: %w", cfg.SourceDB, name, err)
		}
	}
	return nil
}

func recreateTargetCollection(ctx context.Context, db *mongo.Database, name string) error {
	if err := dropCollectionIfPresent(ctx, db, name); err != nil {
		return err
	}
	return db.CreateCollection(ctx, name)
}

// copyCollection pages through the source collection in _id order. Resume uses the
// _id index bound min() (not $gt, which would type-bracket and skip _ids of other BSON
// types); the inclusive bound document is skipped by exact byte comparison.
// Documents changed during the scan are fixed up by change-stream replay.
func (r *runner) copyCollection(ctx context.Context, name string) error {
	log := r.cfg.Logger
	findOpts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetHint(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(r.cfg.SnapshotBatch))
	prog := r.state.progress(name)
	if !prog.LastID.IsZero() {
		findOpts.SetMin(bson.D{{Key: "_id", Value: prog.LastID}})
	}
	cursor, err := r.source.Collection(name).Find(ctx, bson.D{}, findOpts)
	if err != nil {
		return r.vanished(ctx, name, err)
	}
	defer cursor.Close(ctx)

	start := time.Now()
	lastLog := start
	startCopied := prog.Copied
	docs := make([]any, 0, r.cfg.SnapshotBatch)
	var lastID bson.RawValue
	batchBytes := 0
	first := true
	flush := func(done bool) error {
		if len(docs) == 0 && !done {
			return nil
		}
		target := r.target.Collection(name)
		next := r.state.clone()
		p := next.progress(name)
		if len(docs) > 0 {
			p.LastID = lastID
			p.Copied += int64(len(docs))
		}
		p.Done = done
		err := r.lease.txn(ctx, func(tx context.Context) error {
			if len(docs) > 0 {
				if _, err := target.InsertMany(tx, docs, options.InsertMany().SetOrdered(false)); err != nil {
					return err
				}
			}
			return r.saveState(tx, next)
		})
		if err != nil {
			return err
		}
		r.state = next
		docs = docs[:0]
		batchBytes = 0
		if time.Since(lastLog) >= progressLogInterval {
			lastLog = time.Now()
			rate := float64(p.Copied-startCopied) / time.Since(start).Seconds()
			log.Info("MongoDB snapshot progress", "pipeline", r.cfg.Pipeline, "collection", name, "copied", p.Copied, "docs_per_sec", int64(rate))
		}
		return nil
	}
	for cursor.Next(ctx) {
		raw := cursor.Current
		id, err := raw.LookupErr("_id")
		if err != nil {
			return fmt.Errorf("source document without _id: %w", err)
		}
		if first {
			first = false
			if !prog.LastID.IsZero() && id.Type == prog.LastID.Type && bytes.Equal(id.Value, prog.LastID.Value) {
				continue // inclusive min() bound: already copied
			}
		}
		doc := append(bson.Raw(nil), raw...)
		docs = append(docs, doc)
		lastID = bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
		batchBytes += len(doc)
		if len(docs) >= r.cfg.SnapshotBatch || batchBytes >= snapshotBatchBytes {
			if err := flush(false); err != nil {
				return err
			}
		}
	}
	if err := cursor.Err(); err != nil {
		if verr := r.vanished(ctx, name, err); verr != nil {
			return verr
		}
	}
	if err := flush(true); err != nil {
		return err
	}
	p := r.state.progress(name)
	log.Info("MongoDB collection snapshot complete", "pipeline", r.cfg.Pipeline, "collection", r.cfg.SourceDB+"."+name, "documents", p.Copied, "seconds", time.Since(start).Seconds())
	return nil
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
	r.cfg.Logger.Warn("MongoDB source collection disappeared during snapshot; change stream replay will apply the drop/rename", "collection", r.cfg.SourceDB+"."+name)
	return nil
}

func dropCollectionIfPresent(ctx context.Context, db *mongo.Database, name string) error {
	names, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: name}})
	if err != nil || len(names) == 0 {
		return err
	}
	return db.Collection(name).Drop(ctx)
}
