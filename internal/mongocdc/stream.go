package mongocdc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	codeChangeStreamHistoryLost = 286
	codeChangeStreamFatal       = 280
	codeInvalidResumeToken      = 260
)

type streamEvent struct {
	ID            bson.Raw `bson:"_id"`
	OperationType string   `bson:"operationType"`
	NS            struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	To struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"to"`
	FullDocument bson.RawValue  `bson:"fullDocument"`
	DocumentKey  bson.Raw       `bson:"documentKey"`
	ClusterTime  bson.Timestamp `bson:"clusterTime"`
	TxnNumber    *int64         `bson:"txnNumber"`
	LSID         bson.Raw       `bson:"lsid"`
}

// txnKey identifies the source transaction an event belongs to ("" if none).
func (e *streamEvent) txnKey() string {
	if e.TxnNumber == nil {
		return ""
	}
	return fmt.Sprintf("%d/%x", *e.TxnNumber, []byte(e.LSID))
}

func streamOptions(maxAwait time.Duration) *options.ChangeStreamOptionsBuilder {
	// updateLookup: update events carry the current majority-committed image of the
	// document; replaying events in order converges to the source state (see applyBatch).
	// showExpandedEvents is not requested (6.0+ only); index/collection-option DDL
	// is not replicated.
	return options.ChangeStream().SetFullDocument(options.UpdateLookup).SetMaxAwaitTime(maxAwait)
}

// streamPipeline drops the update description: it is unused (full images are applied)
// and would double the size of large-document update events.
func streamPipeline() mongo.Pipeline {
	return mongo.Pipeline{bson.D{{Key: "$project", Value: bson.D{{Key: "updateDescription", Value: 0}}}}}
}

// currentResumeToken opens a change stream "now" and returns its post-batch resume
// token, i.e. a position at or before every write a subsequent majority read can see.
func currentResumeToken(ctx context.Context, db *mongo.Database, maxAwait time.Duration) (bson.Raw, error) {
	// batchSize 0: the initial aggregate returns no events, only the post-batch resume
	// token of the stream's start position. (With a non-empty first batch the driver
	// exposes no token for the position before those events.)
	cs, err := db.Watch(ctx, streamPipeline(), streamOptions(maxAwait).SetBatchSize(0))
	if err != nil {
		return nil, err
	}
	defer cs.Close(context.Background())
	token := append(bson.Raw(nil), cs.ResumeToken()...)
	if len(token) == 0 {
		return nil, errors.New("source returned no post-batch resume token for the stream start")
	}
	return token, nil
}

func openStream(ctx context.Context, db *mongo.Database, state syncState, maxAwait time.Duration) (*mongo.ChangeStream, error) {
	opts := streamOptions(maxAwait)
	if len(state.ResumeToken) > 0 {
		if state.StartAfter {
			opts.SetStartAfter(state.ResumeToken)
		} else {
			opts.SetResumeAfter(state.ResumeToken)
		}
	} else {
		// Checkpoints written by the first experimental version kept only a start time.
		opts.SetStartAtOperationTime(&state.StartAt)
	}
	cs, err := db.Watch(ctx, streamPipeline(), opts)
	if err != nil {
		return nil, classifyStreamError(err)
	}
	return cs, nil
}

func classifyStreamError(err error) error {
	if serverErrorCode(err, codeChangeStreamHistoryLost, codeChangeStreamFatal, codeInvalidResumeToken) {
		return fmt.Errorf("%w: %v", ErrResyncRequired, err)
	}
	return err
}

// docOp is the net effect of the events of one batch on one document.
type docOp struct {
	coll string
	id   bson.RawValue
	doc  bson.Raw // nil: delete
	seq  int
}

// batch accumulates document events between DDL boundaries. Several events for the
// same document collapse to the last one; ops are applied in order of their last
// occurrence, which also keeps numerically-equal _ids of different BSON types (1 vs
// 1.0, the same document for the unique _id index) in source order.
type batch struct {
	ops    map[string]*docOp
	seq    int
	bytes  int
	events int
	token  bson.Raw
	txn    string
	last   bson.Timestamp
}

func newBatch() *batch { return &batch{ops: make(map[string]*docOp)} }

func (b *batch) add(coll string, id bson.RawValue, doc bson.Raw) {
	key := coll + "\x00" + string([]byte{byte(id.Type)}) + string(id.Value)
	b.seq++
	b.events++
	b.bytes += len(doc) + len(id.Value)
	if op, ok := b.ops[key]; ok {
		op.doc, op.seq = doc, b.seq
		return
	}
	b.ops[key] = &docOp{coll: coll, id: id, doc: doc, seq: b.seq}
}

func (b *batch) empty() bool { return b.events == 0 }

// models returns per-collection ordered write models.
func (b *batch) models() (map[string][]mongo.WriteModel, []string) {
	ops := make([]*docOp, 0, len(b.ops))
	for _, op := range b.ops {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].seq < ops[j].seq })
	out := make(map[string][]mongo.WriteModel)
	var colls []string
	for _, op := range ops {
		filter := bson.D{{Key: "_id", Value: op.id}}
		var m mongo.WriteModel
		if op.doc == nil {
			m = mongo.NewDeleteOneModel().SetFilter(filter)
		} else {
			m = mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(op.doc).SetUpsert(true)
		}
		if _, ok := out[op.coll]; !ok {
			colls = append(colls, op.coll)
		}
		out[op.coll] = append(out[op.coll], m)
	}
	sort.Strings(colls)
	return out, colls
}

type streamStats struct {
	events, batches, writes int64
	start, lastLog          time.Time
	lastClusterTime         bson.Timestamp
}

func (r *runner) stream(ctx context.Context) error {
	cfg := r.cfg
	cs, err := openStream(ctx, r.source, r.state, cfg.MaxAwait)
	if err != nil {
		return fmt.Errorf("open MongoDB change stream: %w", err)
	}
	defer func() { _ = cs.Close(context.Background()) }()
	cfg.Logger.Info("MongoDB change stream started", "pipeline", cfg.Pipeline)

	stats := &streamStats{start: time.Now(), lastLog: time.Now()}
	b := newBatch()
	lastCheckpoint := time.Now()
	persistedToken := append(bson.Raw(nil), r.state.ResumeToken...)
	flush := func() error {
		if b.empty() && (len(b.token) == 0 || bytes.Equal(b.token, persistedToken)) {
			return nil
		}
		if err := r.applyBatch(ctx, b); err != nil {
			return err
		}
		stats.events += int64(b.events)
		stats.writes += int64(len(b.ops))
		stats.batches++
		if b.last.T != 0 {
			stats.lastClusterTime = b.last
		}
		persistedToken = append(bson.Raw(nil), r.state.ResumeToken...)
		lastCheckpoint = time.Now()
		b = newBatch()
		r.logStreamProgress(stats, false)
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !cs.TryNext(ctx) {
			if err := cs.Err(); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("MongoDB change stream stopped: %w", classifyStreamError(err))
			}
			// Empty getMore: the server had nothing more. Commit what we have, and keep the
			// post-batch resume token fresh so an idle pipeline does not fall off the oplog.
			if tok := cs.ResumeToken(); len(tok) > 0 {
				b.token = append(b.token[:0:0], tok...)
			}
			if !b.empty() || time.Since(lastCheckpoint) >= cfg.CheckpointEvery {
				if err := flush(); err != nil {
					return err
				}
			}
			r.logStreamProgress(stats, false)
			continue
		}
		raw := append(bson.Raw(nil), cs.Current...)
		var event streamEvent
		if err := bson.Unmarshal(raw, &event); err != nil {
			return fmt.Errorf("decode MongoDB change event: %w", err)
		}
		token := append(bson.Raw(nil), cs.ResumeToken()...)
		if len(token) == 0 {
			return errors.New("MongoDB change stream returned an empty resume token")
		}
		// Size-based cut only between source transactions, so one source transaction
		// commits atomically on the target (unless it is enormous).
		txn := event.txnKey()
		full := b.events >= cfg.StreamBatch || b.bytes >= streamBatchBytes || time.Since(lastCheckpoint) >= cfg.CheckpointEvery
		if full && (b.empty() || txn == "" || txn != b.txn) {
			if err := flush(); err != nil {
				return err
			}
		} else if !b.empty() && (b.events >= 20*cfg.StreamBatch || b.bytes >= 8*streamBatchBytes) {
			cfg.Logger.Warn("MongoDB source transaction exceeds the apply batch limit; it is split across target transactions", "pipeline", cfg.Pipeline)
			if err := flush(); err != nil {
				return err
			}
		}
		ddl, err := r.addEvent(b, &event)
		if err != nil {
			return fmt.Errorf("apply MongoDB event %s on %s.%s: %w", event.OperationType, event.NS.DB, event.NS.Coll, err)
		}
		b.txn = txn
		if event.ClusterTime.T != 0 {
			b.last = event.ClusterTime
		}
		if !ddl {
			b.token = token
			continue
		}
		// DDL boundary: commit document changes before it, apply it, then checkpoint past it.
		if err := flush(); err != nil { // commits the token of the last document event
			return err
		}
		if err := r.applyDDL(ctx, &event); err != nil {
			return fmt.Errorf("apply MongoDB event %s on %s.%s: %w", event.OperationType, event.NS.DB, event.NS.Coll, err)
		}
		b.token = token
		if event.OperationType == "invalidate" {
			next := r.state.clone()
			next.ResumeToken, next.StartAfter = token, true
			if err := r.lease.txn(ctx, func(tx context.Context) error { return r.saveState(tx, next) }); err != nil {
				return fmt.Errorf("persist MongoDB invalidate resume token: %w", err)
			}
			r.state = next
			persistedToken = token
			b = newBatch()
			_ = cs.Close(ctx)
			cs, err = openStream(ctx, r.source, r.state, cfg.MaxAwait)
			if err != nil {
				return fmt.Errorf("reopen MongoDB change stream after invalidate: %w", err)
			}
			continue
		}
		if err := flush(); err != nil {
			return err
		}
	}
}

func (r *runner) logStreamProgress(stats *streamStats, force bool) {
	if !force && time.Since(stats.lastLog) < progressLogInterval {
		return
	}
	elapsed := time.Since(stats.start).Seconds()
	stats.lastLog = time.Now()
	attrs := []any{"pipeline", r.cfg.Pipeline, "events", stats.events, "document_writes", stats.writes, "batches", stats.batches, "events_per_sec", int64(float64(stats.events) / elapsed)}
	if stats.lastClusterTime.T != 0 {
		attrs = append(attrs, "lag_seconds", time.Now().Unix()-int64(stats.lastClusterTime.T))
	}
	r.cfg.Logger.Info("MongoDB change stream progress", attrs...)
}

// addEvent folds a document event into the batch; it reports true for events that
// must be applied on their own (DDL, invalidate).
func (r *runner) addEvent(b *batch, event *streamEvent) (bool, error) {
	cfg := r.cfg
	switch event.OperationType {
	case "dropDatabase", "rename", "drop", "invalidate":
		return true, nil
	}
	if event.NS.DB != cfg.SourceDB || event.NS.Coll == "" || !selected(cfg, cfg.SourceDB+"."+event.NS.Coll) {
		return false, nil
	}
	switch event.OperationType {
	case "insert", "replace", "update":
		id, err := event.DocumentKey.LookupErr("_id")
		if err != nil {
			return false, fmt.Errorf("documentKey without _id: %w", err)
		}
		if event.FullDocument.IsZero() || event.FullDocument.Type == bson.TypeNull {
			// update with updateLookup on a document deleted since: its delete event follows.
			b.add(event.NS.Coll, id, nil)
			return false, nil
		}
		if event.FullDocument.Type != bson.TypeEmbeddedDocument {
			return false, fmt.Errorf("fullDocument has BSON type %s", event.FullDocument.Type)
		}
		doc := bson.Raw(event.FullDocument.Value)
		if fid, err := doc.LookupErr("_id"); err != nil || fid.Type != id.Type || !bytes.Equal(fid.Value, id.Value) {
			return false, errors.New("fullDocument _id does not match documentKey")
		}
		b.add(event.NS.Coll, id, doc)
		return false, nil
	case "delete":
		id, err := event.DocumentKey.LookupErr("_id")
		if err != nil {
			return false, fmt.Errorf("documentKey without _id: %w", err)
		}
		b.add(event.NS.Coll, id, nil)
		return false, nil
	case "create", "modify", "createIndexes", "dropIndexes", "shardCollection", "refineCollectionShardKey", "reshardCollection":
		// Only emitted with showExpandedEvents; collection options and indexes are not replicated.
		return false, nil
	default:
		return false, fmt.Errorf("unsupported MongoDB change event type %q; refusing to advance checkpoint", event.OperationType)
	}
}

// applyBatch commits the document writes and the resume token in one fenced target
// transaction: after a crash either both are durable or neither is.
func (r *runner) applyBatch(ctx context.Context, b *batch) error {
	models, colls := b.models()
	next := r.state.clone()
	if len(b.token) > 0 {
		next.ResumeToken, next.StartAfter = b.token, false
	}
	err := r.lease.txn(ctx, func(tx context.Context) error {
		for _, coll := range colls {
			if _, err := r.target.Collection(coll).BulkWrite(tx, models[coll], options.BulkWrite().SetOrdered(true)); err != nil {
				return fmt.Errorf("write %s.%s: %w", r.cfg.TargetDB, coll, err)
			}
		}
		return r.saveState(tx, next)
	})
	if err != nil {
		return err
	}
	r.state = next
	return nil
}

// applyDDL applies a collection-level event outside a transaction (MongoDB cannot drop
// inside one). It is idempotent, so replay after a crash before the checkpoint is safe.
func (r *runner) applyDDL(ctx context.Context, event *streamEvent) error {
	cfg := r.cfg
	if err := r.lease.txn(ctx, func(context.Context) error { return nil }); err != nil {
		return err // re-check ownership right before the unfenced DDL
	}
	switch event.OperationType {
	case "invalidate":
		return nil
	case "dropDatabase":
		if event.NS.DB != cfg.SourceDB {
			return nil
		}
		names, err := r.target.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return err
		}
		for _, name := range names {
			if selected(cfg, cfg.SourceDB+"."+name) {
				if err := r.target.Collection(name).Drop(ctx); err != nil {
					return err
				}
			}
		}
		cfg.Logger.Warn("MongoDB source database dropped; dropped selected target collections", "pipeline", cfg.Pipeline)
		return nil
	case "drop":
		if event.NS.DB != cfg.SourceDB || !selected(cfg, cfg.SourceDB+"."+event.NS.Coll) {
			return nil
		}
		cfg.Logger.Warn("MongoDB source collection dropped; dropping target collection", "pipeline", cfg.Pipeline, "collection", event.NS.Coll)
		return dropCollectionIfPresent(ctx, r.target, event.NS.Coll)
	case "rename":
		return r.applyRename(ctx, event)
	}
	return nil
}

func (r *runner) applyRename(ctx context.Context, event *streamEvent) error {
	cfg := r.cfg
	if event.NS.DB != cfg.SourceDB {
		return nil
	}
	oldSelected := selected(cfg, cfg.SourceDB+"."+event.NS.Coll)
	if event.To.DB != cfg.SourceDB || event.To.Coll == "" {
		if oldSelected {
			return dropCollectionIfPresent(ctx, r.target, event.NS.Coll)
		}
		return nil
	}
	newSelected := selected(cfg, cfg.SourceDB+"."+event.To.Coll)
	if oldSelected {
		if err := dropCollectionIfPresent(ctx, r.target, event.NS.Coll); err != nil {
			return err
		}
	}
	if !newSelected {
		return nil
	}
	cfg.Logger.Warn("MongoDB source collection renamed; re-copying destination collection from source", "pipeline", cfg.Pipeline, "from", event.NS.Coll, "to", event.To.Coll)
	// Materialize from the current source instead of renaming the target copy: later
	// events (replayed idempotently) bring it to the right state.
	if err := recreateTargetCollection(ctx, r.target, event.To.Coll); err != nil {
		return err
	}
	return r.recopy(ctx, event.To.Coll)
}

// recopy copies a whole collection with fenced batches, without snapshot progress.
func (r *runner) recopy(ctx context.Context, name string) error {
	findOpts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetHint(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(int32(r.cfg.SnapshotBatch))
	cursor, err := r.source.Collection(name).Find(ctx, bson.D{}, findOpts)
	if err != nil {
		return r.vanished(ctx, name, err)
	}
	defer cursor.Close(ctx)
	docs := make([]any, 0, r.cfg.SnapshotBatch)
	size := 0
	flush := func() error {
		if len(docs) == 0 {
			return nil
		}
		err := r.lease.txn(ctx, func(tx context.Context) error {
			_, err := r.target.Collection(name).InsertMany(tx, docs, options.InsertMany().SetOrdered(false))
			return err
		})
		docs, size = docs[:0], 0
		return err
	}
	for cursor.Next(ctx) {
		doc := append(bson.Raw(nil), cursor.Current...)
		docs = append(docs, doc)
		size += len(doc)
		if len(docs) >= r.cfg.SnapshotBatch || size >= snapshotBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := cursor.Err(); err != nil {
		if verr := r.vanished(ctx, name, err); verr != nil {
			return verr
		}
	}
	return flush()
}
