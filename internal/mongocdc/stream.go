package mongocdc

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/semaphore"
)

const (
	codeChangeStreamHistoryLost = 286
	codeChangeStreamFatal       = 280
	codeInvalidResumeToken      = 260
)

// ErrSourceTransactionTooLarge is returned before applying or checkpointing a source
// transaction that cannot be kept atomic within the configured safety bound.
var ErrSourceTransactionTooLarge = errors.New("mongodb cdc: source transaction exceeds the atomic apply limit; refusing to split it")
var ErrSourceTransactionDDL = errors.New("mongodb cdc: source transaction contains DDL that cannot be applied atomically; refusing to advance checkpoint")

func sourceTxnContainsDDL(operation, txn string) bool {
	if txn == "" {
		return false
	}
	// create/createIndexes/dropIndexes/modify are allowed in MongoDB 4.4+
	// transactions; they are reconciled inline without a batch boundary.
	switch operation {
	case "drop", "dropDatabase", "rename", "invalidate":
		return true
	default:
		return false
	}
}

func sourceTxnOverLimit(b *batch, txn string, eventBytes, eventLimit, byteLimit int) bool {
	if txn == "" {
		return false
	}
	if txn != b.txn {
		return eventBytes >= byteLimit
	}
	return b.txnEvents >= eventLimit || b.txnBytes+eventBytes >= byteLimit
}

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
	FullDocument      bson.RawValue  `bson:"fullDocument"`
	DocumentKey       bson.Raw       `bson:"documentKey"`
	UpdateDescription *updateDesc    `bson:"updateDescription"`
	ClusterTime       bson.Timestamp `bson:"clusterTime"`
	TxnNumber         *int64         `bson:"txnNumber"`
	LSID              bson.Raw       `bson:"lsid"`
}

// txnKey identifies the source transaction an event belongs to ("" if none).
func (e *streamEvent) txnKey() string {
	if e.TxnNumber == nil {
		return ""
	}
	return fmt.Sprintf("%d/%x", *e.TxnNumber, []byte(e.LSID))
}

func streamOptions(cfg Config, srcWire int32) *options.ChangeStreamOptionsBuilder {
	opts := options.ChangeStream().SetMaxAwaitTime(cfg.MaxAwait)
	if cfg.StreamBatch > 0 {
		maxBatchSize := int(^uint32(0) >> 1)
		opts.SetBatchSize(int32(min(cfg.StreamBatch, maxBatchSize)))
	}
	if cfg.UpdateMode == UpdateModeLookup {
		// updateLookup: update events carry the current majority-committed image of the
		// document; replaying events in order converges to the source state.
		opts.SetFullDocument(options.UpdateLookup)
	}
	if srcWire >= wireVersion60 {
		// 6.0+: DDL events (create, createIndexes, modify, ...) trigger an index/option
		// reconcile, and 6.1+ reports disambiguatedPaths for ambiguous update paths.
		opts.SetShowExpandedEvents(true)
	}
	return opts
}

func batchFlushDue(b *batch, txn string, eventLimit, byteLimit int, checkpointDue bool) bool {
	full := b.events >= eventLimit || b.bytes >= byteLimit || checkpointDue
	return full && (b.empty() || txn == "" || txn != b.txn)
}

// streamPipeline drops the update description in lookup mode: it is unused (full images
// are applied) and would double the size of large-document update events.
func streamPipeline(cfg Config) mongo.Pipeline {
	if cfg.UpdateMode == UpdateModeLookup {
		return mongo.Pipeline{bson.D{{Key: "$project", Value: bson.D{{Key: "updateDescription", Value: 0}}}}}
	}
	return mongo.Pipeline{}
}

// currentResumeToken opens a change stream "now" and returns its post-batch resume
// token, i.e. a position at or before every write a subsequent majority read can see.
func currentResumeToken(ctx context.Context, db *mongo.Database, maxAwait time.Duration) (bson.Raw, error) {
	// batchSize 0: the initial aggregate returns no events, only the post-batch resume
	// token of the stream's start position. (With a non-empty first batch the driver
	// exposes no token for the position before those events.)
	cs, err := db.Watch(ctx, mongo.Pipeline{}, options.ChangeStream().SetMaxAwaitTime(maxAwait).SetBatchSize(0))
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

func openStream(ctx context.Context, db *mongo.Database, state syncState, cfg Config, srcWire int32) (*mongo.ChangeStream, error) {
	opts := streamOptions(cfg, srcWire)
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
	cs, err := db.Watch(ctx, streamPipeline(cfg), opts)
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
	coll    string
	id      bson.RawValue
	doc     bson.Raw      // final image (nil: deleted, pending deltas, or lookup)
	deleted bool          // final state: absent
	deltas  []*updateDesc // to apply to the target's current image at apply time
	lookup  bool          // re-read the current image from the source at apply time
	seq     int
}

// batch accumulates document events between DDL boundaries. Several events for the
// same document collapse (keyed by MongoDB _id equality, see idKey).
type batch struct {
	ops       map[string]*docOp
	slow      map[string]bool // collections with _ids idKey cannot canonicalize
	seq       int
	bytes     int
	events    int
	byOp      map[string]int
	token     bson.Raw
	txn       string
	txnEvents int
	txnBytes  int
	last      bson.Timestamp
	// collOps: change events per collection (inserts, updates incl. replaces, deletes) for the
	// Tables view; counted as events, before same-document events collapse.
	collOps map[string]*collOpCount
}

func newBatch() *batch {
	return &batch{ops: make(map[string]*docOp), slow: make(map[string]bool), byOp: make(map[string]int),
		collOps: make(map[string]*collOpCount)}
}

// collOpCount counts the change events of one collection in a batch.
type collOpCount struct{ inserts, updates, deletes int64 }

func (b *batch) countColl(coll, op string) {
	c := b.collOps[coll]
	if c == nil {
		c = &collOpCount{}
		b.collOps[coll] = c
	}
	switch op {
	case "insert":
		c.inserts++
	case "update", "replace":
		c.updates++
	case "delete":
		c.deletes++
	}
}

// idKey returns a key equal for _ids the unique _id index treats as equal. Numbers of
// different BSON types compare by value (1 == 1.0 == NumberLong(1)); other scalar types
// compare by bytes within their own type. Decimal128, documents, arrays and symbols are
// not canonicalized (ok=false): such collections take the ordered per-document path.
func idKey(id bson.RawValue) (string, bool) {
	switch id.Type {
	case bson.TypeInt32:
		return fmt.Sprintf("n%d", id.Int32()), true
	case bson.TypeInt64:
		return fmt.Sprintf("n%d", id.Int64()), true
	case bson.TypeDouble:
		f := id.Double()
		if f == math.Trunc(f) && math.Abs(f) < 1<<63 { // exact in int64
			return fmt.Sprintf("n%d", int64(f)), true
		}
		if math.IsNaN(f) {
			return "nNaN", true
		}
		return fmt.Sprintf("f%v", f), true
	case bson.TypeString, bson.TypeObjectID, bson.TypeBinary, bson.TypeDateTime, bson.TypeTimestamp, bson.TypeBoolean, bson.TypeNull, bson.TypeMinKey, bson.TypeMaxKey:
		return string([]byte{byte(id.Type)}) + string(id.Value), true
	}
	return string([]byte{byte(id.Type)}) + string(id.Value), false
}

func exactKey(id bson.RawValue) string { return string([]byte{byte(id.Type)}) + string(id.Value) }

func (b *batch) op(coll string, id bson.RawValue, size int) *docOp {
	k, ok := idKey(id)
	if !ok {
		b.slow[coll] = true
	}
	key := coll + "\x00" + k
	b.seq++
	b.events++
	b.bytes += size + len(id.Value)
	op, found := b.ops[key]
	if !found {
		op = &docOp{coll: coll}
		b.ops[key] = op
	}
	op.id, op.seq = id, b.seq
	return op
}

// add records a full image (doc != nil) or a delete (doc == nil).
func (b *batch) add(coll string, id bson.RawValue, doc bson.Raw) {
	op := b.op(coll, id, len(doc))
	op.doc, op.deleted, op.deltas, op.lookup = doc, doc == nil, nil, false
}

// addLookup records that the document's current image must be read from the source.
func (b *batch) addLookup(coll string, id bson.RawValue) {
	op := b.op(coll, id, 0)
	op.doc, op.deleted, op.deltas, op.lookup = nil, false, nil, true
}

// addDelta records an update description. It is applied immediately to an image already
// in the batch, queued for the target's image otherwise.
func (b *batch) addDelta(coll string, id bson.RawValue, d *updateDesc) {
	size := len(d.UpdatedFields) + 16*len(d.RemovedFields)
	k, ok := idKey(id)
	existing := b.ops[coll+"\x00"+k]
	op := b.op(coll, id, size)
	switch {
	case op.lookup:
		// The source image read at apply time already includes this update.
	case op.doc != nil:
		if img, err := applyUpdate(op.doc, d); err == nil {
			op.doc = img
		} else {
			op.doc, op.lookup = nil, true
		}
	case op.deleted:
		// An update after a delete within one batch means a re-insert we did not see.
		op.deleted, op.lookup = false, true
	case existing == nil && (!ok || b.slow[coll]):
		// Non-canonical _ids: equal _ids of different BSON types may be pending as
		// other entries; read the source image instead of a possibly stale target base.
		op.lookup = true
	default:
		op.deltas = append(op.deltas, d)
	}
}

func (b *batch) empty() bool { return b.events == 0 }

// collWrites is the net effect of a batch on one collection.
type collWrites struct {
	fast   bool
	ids    []any // fast path: every touched _id (deleted first)
	docs   []any // fast path: final images (inserted after the delete)
	models []mongo.WriteModel
}

// writes groups the batch per collection; every op must be resolved (doc or deleted).
// Fast path (all _ids canonicalized, so every key is a distinct document): one
// deleteMany({_id: {$in}}) + one insertMany inside the transaction, i.e.
// replace-or-insert / delete-if-exists. Otherwise: ordered replace/delete models in
// order of last occurrence, which keeps source order between equal _ids of different
// BSON types.
func (b *batch) writes() (map[string]*collWrites, []string) {
	ops := b.sorted()
	out := make(map[string]*collWrites)
	var colls []string
	for _, op := range ops {
		w, ok := out[op.coll]
		if !ok {
			w = &collWrites{fast: !b.slow[op.coll]}
			out[op.coll] = w
			colls = append(colls, op.coll)
		}
		if w.fast {
			w.ids = append(w.ids, op.id)
			if op.doc != nil {
				w.docs = append(w.docs, op.doc)
			}
			continue
		}
		filter := bson.D{{Key: "_id", Value: op.id}}
		if op.doc == nil {
			w.models = append(w.models, mongo.NewDeleteOneModel().SetFilter(filter))
		} else {
			w.models = append(w.models, mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(op.doc).SetUpsert(true))
		}
	}
	sort.Strings(colls)
	return out, colls
}

func (b *batch) sorted() []*docOp {
	ops := make([]*docOp, 0, len(b.ops))
	for _, op := range b.ops {
		ops = append(ops, op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].seq < ops[j].seq })
	return ops
}

type streamStats struct {
	events, batches, writes, lookups int64
	applyTime                        time.Duration
	start, lastLog                   time.Time
	lastClusterTime                  bson.Timestamp
	loggedEvents                     int64 // events at the last progress line
}

// progressDue: a progress line is due streamProgressInterval after the last one when events arrived
// since, otherwise only after streamIdleLogInterval (idle heartbeat).
func (s *streamStats) progressDue(now time.Time) bool {
	since := now.Sub(s.lastLog)
	if s.events != s.loggedEvents {
		return since >= streamProgressInterval
	}
	return since >= streamIdleLogInterval
}

// progressAttrs: the fields of a progress line at now. Counters are totals since the stream started;
// events_per_sec is the rate since the previous line (one decimal), not the average since the start.
func (s *streamStats) progressAttrs(now time.Time) []any {
	rate := 0.0
	if d := now.Sub(s.lastLog).Seconds(); d > 0 {
		rate = math.Round(float64(s.events-s.loggedEvents)/d*10) / 10
	}
	return []any{"events", s.events, "events_since_last", s.events - s.loggedEvents, "document_writes", s.writes,
		"lookups", s.lookups, "batches", s.batches, "events_per_sec", rate, "apply_seconds", s.applyTime.Seconds()}
}

func tokenString(token bson.Raw) string {
	if v, err := token.LookupErr("_data"); err == nil {
		if s, ok := v.StringValueOK(); ok {
			return s
		}
	}
	return hex.EncodeToString(token)
}

func (r *runner) stream(ctx context.Context) (err error) {
	ctx, stopStream := context.WithCancelCause(ctx)
	defer func() {
		if cause := context.Cause(ctx); errors.Is(cause, ErrLeaseLost) || errors.Is(cause, ErrDDLUncertain) {
			err = cause
		}
		stopStream(nil)
	}()
	cfg := r.cfg
	r.obs.setConsistentAt(r.state.ConsistentAt)
	r.obs.setPhase(PhaseStreaming)
	// Interrupted re-copies (rename during the snapshot overlap, repair) finish first:
	// the checkpoint already points past their start.
	if err := r.resumeRecopies(ctx); err != nil {
		return err
	}
	cs, err := openStream(ctx, r.source, r.state, cfg, r.srcWire)
	if err != nil {
		return fmt.Errorf("open MongoDB change stream: %w", err)
	}
	// The reader goroutine fetches and copies events while the previous batch commits on
	// the target (source reads and target writes overlap; the channel bounds memory).
	items, readerDone, stopReader, budget := startReader(ctx, cs)
	defer func() {
		stopReader()
		<-readerDone
		if cs != nil {
			_ = cs.Close(context.Background())
		}
	}()
	bgCtx, stopBG := context.WithCancel(ctx)
	defer stopBG()
	go r.monitorOplog(bgCtx)
	if cfg.ReplicateIndexes {
		r.reconcileTrigger = make(chan struct{}, 1)
		reconcileDone := make(chan struct{})
		go func() {
			defer close(reconcileDone)
			r.reconcileLoop(bgCtx, stopStream)
		}()
		defer func() {
			stopBG()
			<-reconcileDone
		}()
	}
	cfg.Logger.Info("MongoDB change stream started", "pipeline", cfg.Pipeline, "update_mode", cfg.UpdateMode)

	stats := &streamStats{start: time.Now(), lastLog: time.Now()}
	b := newBatch()
	lastCheckpoint := time.Now()
	persistedToken := append(bson.Raw(nil), r.state.ResumeToken...)
	flush := func() error {
		if b.empty() && (len(b.token) == 0 || bytes.Equal(b.token, persistedToken)) {
			return nil
		}
		applyStart := time.Now()
		lookups, err := r.applyBatch(ctx, b)
		if err != nil {
			return err
		}
		stats.applyTime += time.Since(applyStart)
		stats.events += int64(b.events)
		stats.writes += int64(len(b.ops))
		stats.lookups += int64(lookups)
		stats.batches++
		if b.last.T != 0 {
			stats.lastClusterTime = b.last
			if b.last.After(r.state.ConsistentAt) {
				r.consistent.Store(true)
			}
		}
		r.obs.applied(b.events, b.byOp, len(b.ops), lookups, b.last, tokenString(r.state.ResumeToken))
		r.obs.collApplied(b.collOps, b.last)
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
		if b.empty() {
			if err := r.obs.waitIfPaused(ctx, false); err != nil {
				return err
			}
		}
		var it streamItem
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req := <-r.obs.repairs:
			if err := flush(); err != nil {
				req.resp <- err
				return err
			}
			if err := r.repair(ctx, req); err != nil {
				return err
			}
			continue
		case it = <-items:
			budget.Release(it.bytes())
		}
		if it.err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("MongoDB change stream stopped: %w", classifyStreamError(it.err))
		}
		if it.raw == nil {
			// Empty getMore: the server had nothing more. Commit what we have, and keep the
			// post-batch resume token fresh so an idle pipeline does not fall off the oplog.
			if len(it.token) > 0 {
				b.token = it.token
			}
			if !b.empty() || time.Since(lastCheckpoint) >= cfg.CheckpointEvery {
				if err := flush(); err != nil {
					return err
				}
			}
			if b.empty() && len(items) == 0 {
				r.obs.setCaughtUp(true)
				r.consistent.Store(true)
			}
			r.logStreamProgress(stats, false)
			continue
		}
		r.obs.setCaughtUp(false)
		var event streamEvent
		if err := bson.Unmarshal(it.raw, &event); err != nil {
			return fmt.Errorf("decode MongoDB change event: %w", err)
		}
		token := it.token
		if len(token) == 0 {
			return errors.New("MongoDB change stream returned an empty resume token")
		}
		// Size-based cuts occur only between source transactions. A transaction beyond
		// the atomic safety bound fails closed before any of its events are applied.
		txn := event.txnKey()
		// Only changes this pipeline replicates count: traffic on other or
		// excluded collections can neither block nor split the pipeline.
		sel := event.NS.DB == cfg.SourceDB && event.NS.Coll != "" && selected(cfg, cfg.SourceDB+"."+event.NS.Coll)
		if sel && sourceTxnContainsDDL(event.OperationType, txn) {
			return fmt.Errorf("%w (%s)", ErrSourceTransactionDDL, event.OperationType)
		}
		checkpointDue := time.Since(lastCheckpoint) >= cfg.CheckpointEvery
		if sel && sourceTxnOverLimit(b, txn, len(it.raw), 20*cfg.StreamBatch, cfg.MaxTransactionBytes) {
			return fmt.Errorf("%w (events=%d bytes=%d)", ErrSourceTransactionTooLarge, b.txnEvents+1, b.txnBytes+len(it.raw))
		}
		if batchFlushDue(b, txn, cfg.StreamBatch, streamBatchBytes, checkpointDue) {
			if err := flush(); err != nil {
				return err
			}
		}
		priorTxn := b.txn
		ddl, err := r.addEvent(b, &event)
		if err != nil {
			return fmt.Errorf("apply MongoDB event %s on %s.%s: %w", event.OperationType, event.NS.DB, event.NS.Coll, err)
		}
		if event.OperationType == "create" && txn != "" {
			// A create inside a source transaction is no batch boundary, but an
			// empty collection still has to exist on the target. Creating it
			// before the transaction's writes are applied exposes no partial
			// data, and the checkpoint stays where it is.
			if err := r.createTargetCollection(ctx, event.NS.DB, event.NS.Coll); err != nil {
				return fmt.Errorf("apply MongoDB event create on %s.%s: %w", event.NS.DB, event.NS.Coll, err)
			}
		}
		switch {
		case txn == "":
			b.txnEvents, b.txnBytes = 0, 0
		case txn != priorTxn:
			b.txnEvents, b.txnBytes = 0, 0
			fallthrough
		default:
			if sel {
				b.txnEvents++
				b.txnBytes += len(it.raw)
			}
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
		recopy, err := r.applyDDL(ctx, &event, token)
		if err != nil {
			return fmt.Errorf("apply MongoDB event %s on %s.%s: %w", event.OperationType, event.NS.DB, event.NS.Coll, err)
		}
		if recopy != "" {
			// The checkpoint already points past the rename (committed with the re-copy
			// record); copy the destination collection before reading further events.
			persistedToken = append(bson.Raw(nil), r.state.ResumeToken...)
			if err := r.resumeRecopies(ctx); err != nil {
				return err
			}
			r.obs.setPhase(PhaseStreaming)
			lastCheckpoint = time.Now()
			continue
		}
		b.token = token
		if event.OperationType == "invalidate" {
			next := r.state.clone()
			next.ResumeToken, next.StartAfter = token, true
			if err := r.lease.txn(ctx, 0, func(tx context.Context) error { return r.saveState(tx, next) }); err != nil {
				return fmt.Errorf("persist MongoDB invalidate resume token: %w", err)
			}
			r.state = next
			persistedToken = token
			b = newBatch()
			stopReader()
			<-readerDone // the reader stops by itself after an invalidate
			_ = cs.Close(ctx)
			cs, err = openStream(ctx, r.source, r.state, cfg, r.srcWire)
			if err != nil {
				cs = nil
				return fmt.Errorf("reopen MongoDB change stream after invalidate: %w", err)
			}
			items, readerDone, stopReader, budget = startReader(ctx, cs)
			continue
		}
		if err := flush(); err != nil {
			return err
		}
	}
}

// streamItem is one event (raw != nil), an empty getMore (raw == nil, token = post-batch
// resume token), or a terminal error.
type streamItem struct {
	raw   bson.Raw
	token bson.Raw
	err   error
}

func (it streamItem) bytes() int64 {
	return int64(len(it.raw) + len(it.token))
}

const (
	readerBuffer      = 4096
	readerBufferBytes = 64 << 20
)

func startReader(parent context.Context, cs *mongo.ChangeStream) (<-chan streamItem, <-chan struct{}, context.CancelFunc, *semaphore.Weighted) {
	ctx, cancel := context.WithCancel(parent)
	out := make(chan streamItem, readerBuffer)
	done := make(chan struct{})
	budget := semaphore.NewWeighted(readerBufferBytes)
	go func() {
		defer close(done)
		send := func(it streamItem) bool {
			if err := budget.Acquire(ctx, it.bytes()); err != nil {
				return false
			}
			select {
			case out <- it:
				return true
			case <-ctx.Done():
				budget.Release(it.bytes())
				return false
			}
		}
		for {
			if !cs.TryNext(ctx) {
				if err := cs.Err(); err != nil {
					send(streamItem{err: err})
					return
				}
				if !send(streamItem{token: append(bson.Raw(nil), cs.ResumeToken()...)}) {
					return
				}
				continue
			}
			raw := append(bson.Raw(nil), cs.Current...)
			if !send(streamItem{raw: raw, token: append(bson.Raw(nil), cs.ResumeToken()...)}) {
				return
			}
			if op, ok := raw.Lookup("operationType").StringValueOK(); ok && op == "invalidate" {
				return // the stream is closed by the server; the consumer reopens it
			}
		}
	}()
	return out, done, cancel, budget
}

func (r *runner) logStreamProgress(stats *streamStats, force bool) {
	now := time.Now()
	if !force && !stats.progressDue(now) {
		return
	}
	attrs := append([]any{"pipeline", r.cfg.Pipeline}, stats.progressAttrs(now)...)
	stats.lastLog, stats.loggedEvents = now, stats.events
	if stats.lastClusterTime.T != 0 {
		attrs = append(attrs, "lag_seconds", r.obs.Snapshot().LagSeconds)
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
	case "create":
		// Inside a transaction the create cannot be a batch boundary; the
		// stream loop creates the target collection without a checkpoint.
		if event.txnKey() == "" {
			return true, nil
		}
	}
	if event.NS.DB != cfg.SourceDB || event.NS.Coll == "" || !selected(cfg, cfg.SourceDB+"."+event.NS.Coll) {
		return false, nil
	}
	b.byOp[event.OperationType]++
	b.countColl(event.NS.Coll, event.OperationType)
	switch event.OperationType {
	case "insert", "replace", "update":
		id, err := event.DocumentKey.LookupErr("_id")
		if err != nil {
			return false, fmt.Errorf("documentKey without _id: %w", err)
		}
		if event.OperationType == "update" && cfg.UpdateMode != UpdateModeLookup {
			if event.UpdateDescription == nil {
				b.addLookup(event.NS.Coll, id)
				return false, nil
			}
			event.UpdateDescription.exactPaths = r.srcWire >= wireVersion61
			event.UpdateDescription.noCreate = r.srcWire >= wireVersion50
			b.addDelta(event.NS.Coll, id, event.UpdateDescription)
			return false, nil
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
	case "create", "modify", "createIndexes", "dropIndexes":
		// showExpandedEvents (6.0+): indexes and collection options are reconciled from
		// the source when replicated; the target collection itself is created on demand.
		r.triggerReconcile()
		return false, nil
	case "shardCollection", "refineCollectionShardKey", "reshardCollection":
		return false, nil
	default:
		return false, fmt.Errorf("unsupported MongoDB change event type %q; refusing to advance checkpoint", event.OperationType)
	}
}

// resolve turns every pending delta into a final image (target image + update
// descriptions) and every lookup into the source's current image or a delete. It
// returns the number of source lookups.
func (r *runner) resolve(ctx context.Context, b *batch) (int, error) {
	pending := map[string][]*docOp{}
	for _, op := range b.ops {
		if len(op.deltas) > 0 && !op.lookup && op.doc == nil && !op.deleted {
			pending[op.coll] = append(pending[op.coll], op)
		}
	}
	for coll, ops := range pending {
		images, err := fetchByID(ctx, r.target.Collection(coll), ops)
		if err != nil {
			return 0, fmt.Errorf("read target images of %s.%s: %w", r.cfg.TargetDB, coll, err)
		}
		for _, op := range ops {
			k, _ := idKey(op.id)
			img := images[k]
			if img == nil {
				op.lookup = true
				continue
			}
			for _, d := range op.deltas {
				if img, err = applyUpdate(img, d); err != nil {
					break
				}
			}
			if err != nil {
				op.lookup = true
				continue
			}
			op.doc, op.deltas = img, nil
		}
	}
	lookups := map[string][]*docOp{}
	n := 0
	for _, op := range b.ops {
		if op.lookup {
			lookups[op.coll] = append(lookups[op.coll], op)
			n++
		}
	}
	if n == 0 {
		return 0, nil
	}
	if b.last.T == 0 {
		return 0, errors.New("cannot look up source images without a change stream cluster time")
	}
	// A majority read on a secondary can lag the event we are about to
	// checkpoint. Wait until that secondary can serve a causally ordered read.
	session, err := r.srcClient.StartSession(options.Session().SetCausalConsistency(true))
	if err != nil {
		return 0, err
	}
	defer session.EndSession(context.Background())
	if err := session.AdvanceOperationTime(&b.last); err != nil {
		return 0, err
	}
	sourceCtx := mongo.NewSessionContext(ctx, session)
	for coll, ops := range lookups {
		images, err := fetchByID(sourceCtx, r.source.Collection(coll), ops)
		if err != nil {
			return 0, fmt.Errorf("look up source documents of %s.%s: %w", r.cfg.SourceDB, coll, err)
		}
		for _, op := range ops {
			k, _ := idKey(op.id)
			if b.slow[coll] {
				k = exactKey(op.id)
			}
			op.doc, op.lookup, op.deltas = images[k], false, nil
			op.deleted = op.doc == nil
		}
	}
	return n, nil
}

// fetchByID reads the documents of ops by _id; the result is keyed by idKey (and, for
// non-canonical _ids, by exact bytes).
func fetchByID(ctx context.Context, coll *mongo.Collection, ops []*docOp) (map[string]bson.Raw, error) {
	out := make(map[string]bson.Raw, len(ops))
	for start := 0; start < len(ops); start += 1000 {
		end := min(start+1000, len(ops))
		ids := make(bson.A, 0, end-start)
		for _, op := range ops[start:end] {
			ids = append(ids, op.id)
		}
		cur, err := coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
		if err != nil {
			return nil, err
		}
		for cur.Next(ctx) {
			doc := append(bson.Raw(nil), cur.Current...)
			id := doc.Lookup("_id")
			// Both keys: slow-path (non-canonical) collections look up by exact bytes, others
			// by idKey; for numeric _ids the two differ.
			k, _ := idKey(id)
			out[exactKey(id)] = doc
			out[k] = doc
		}
		err = cur.Err()
		_ = cur.Close(ctx)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// applyBatch commits the document writes and the resume token in one fenced target
// transaction: after a crash either both are durable or neither is.
func (r *runner) applyBatch(ctx context.Context, b *batch) (int, error) {
	lookups, err := r.resolve(ctx, b)
	if err != nil {
		return 0, err
	}
	writes, colls := b.writes()
	next := r.state.clone()
	if len(b.token) > 0 {
		next.ResumeToken, next.StartAfter = b.token, false
	}
	if b.last.T != 0 {
		next.AppliedTime = b.last
	}
	insertOpts := options.InsertMany().SetOrdered(false)
	bulkOpts := options.BulkWrite().SetOrdered(true)
	if r.cfg.ReplicateIndexes {
		insertOpts.SetBypassDocumentValidation(true)
		bulkOpts.SetBypassDocumentValidation(true)
	}
	err = r.lease.txn(ctx, 0, func(tx context.Context) error {
		for _, coll := range colls {
			w, c := writes[coll], r.target.Collection(coll)
			var err error
			if w.fast {
				if _, err = c.DeleteMany(tx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: w.ids}}}}); err == nil && len(w.docs) > 0 {
					_, err = c.InsertMany(tx, w.docs, insertOpts)
				}
			} else {
				_, err = c.BulkWrite(tx, w.models, bulkOpts)
			}
			if err != nil {
				return fmt.Errorf("write %s.%s: %w", r.cfg.TargetDB, coll, err)
			}
		}
		return r.saveState(tx, next)
	})
	if err != nil {
		return 0, err
	}
	r.state = next
	return lookups, nil
}

// applyDDL applies a collection-level event outside a transaction (MongoDB cannot drop
// inside one). It is idempotent, so replay after a crash before the checkpoint is safe.
// For a rename that needs the destination re-copied it returns the collection name;
// the re-copy record and the checkpoint past the event are then already committed.
// createTargetCollection creates a selected collection on the target with the
// source's creation options. It is idempotent and does not checkpoint, so it
// is also safe for a create inside a source transaction.
func (r *runner) createTargetCollection(ctx context.Context, db, coll string) error {
	cfg := r.cfg
	if db != cfg.SourceDB || coll == "" || !selected(cfg, cfg.SourceDB+"."+coll) {
		return nil
	}
	info, err := r.sourceCollectionInfo(ctx, coll)
	if errors.Is(err, errUnsupportedCollectionType) {
		// Views and time-series collections are not replicated; the snapshot
		// skips them the same way.
		cfg.Logger.Warn("MongoDB create of a non-replicable collection type ignored; add an exclude pattern to silence this", "collection", cfg.SourceDB+"."+coll, "error", err)
		return nil
	}
	if err != nil {
		return err
	}
	// Source create has already happened. Create the matching target namespace
	// under the durable DDL lease guard; a crash replays this idempotent create.
	return r.lease.withDDL(ctx, "create collection "+coll, func() error {
		names, err := r.target.ListCollectionNames(ctx, bson.D{{Key: "name", Value: coll}})
		if err != nil || len(names) > 0 {
			return err
		}
		cmd := bson.D{{Key: "create", Value: coll}}
		if cfg.ReplicateIndexes {
			cmd = append(cmd, creatableOptions(info.Options)...)
		}
		return r.target.RunCommand(ctx, append(cmd, bson.E{Key: "writeConcern", Value: durableDDLWriteConcern()})).Err()
	})
}

func (r *runner) applyDDL(ctx context.Context, event *streamEvent, token bson.Raw) (string, error) {
	cfg := r.cfg
	switch event.OperationType {
	case "invalidate":
		return "", nil
	case "create":
		if err := r.createTargetCollection(ctx, event.NS.DB, event.NS.Coll); err != nil {
			return "", err
		}
		next := r.state.clone()
		next.ResumeToken, next.StartAfter = token, false
		if event.ClusterTime.T != 0 {
			next.AppliedTime = event.ClusterTime
		}
		if err := r.lease.txn(ctx, 0, func(tx context.Context) error { return r.saveState(tx, next) }); err != nil {
			return "", err
		}
		r.state = next
		r.triggerReconcile()
		return "", nil
	case "dropDatabase":
		if event.NS.DB != cfg.SourceDB {
			return "", nil
		}
		names, err := r.target.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return "", err
		}
		for _, name := range names {
			if selected(cfg, cfg.SourceDB+"."+name) {
				if err := r.dropTargetCollection(ctx, name); err != nil {
					return "", err
				}
				r.obs.removeCollection(name)
			}
		}
		cfg.Logger.Warn("MongoDB source database dropped; dropped selected target collections", "pipeline", cfg.Pipeline)
		return "", nil
	case "drop":
		if event.NS.DB != cfg.SourceDB || !selected(cfg, cfg.SourceDB+"."+event.NS.Coll) {
			return "", nil
		}
		cfg.Logger.Warn("MongoDB source collection dropped; dropping target collection", "pipeline", cfg.Pipeline, "collection", event.NS.Coll)
		r.obs.removeCollection(event.NS.Coll)
		return "", r.dropTargetCollection(ctx, event.NS.Coll)
	case "rename":
		return r.applyRename(ctx, event, token)
	}
	return "", nil
}

func (r *runner) applyRename(ctx context.Context, event *streamEvent, token bson.Raw) (string, error) {
	cfg := r.cfg
	if event.NS.DB != cfg.SourceDB {
		return "", nil
	}
	oldSelected := selected(cfg, cfg.SourceDB+"."+event.NS.Coll)
	if event.To.DB != cfg.SourceDB || event.To.Coll == "" {
		if oldSelected {
			r.obs.removeCollection(event.NS.Coll)
			return "", r.dropTargetCollection(ctx, event.NS.Coll)
		}
		return "", nil
	}
	newSelected := selected(cfg, cfg.SourceDB+"."+event.To.Coll)
	if oldSelected && newSelected && r.state.ConsistentAt.T != 0 && event.ClusterTime.After(r.state.ConsistentAt) {
		// Past the snapshot overlap the target collection is exactly the source
		// collection at this event: rename it in place (O(1), any size).
		done, err := r.renameTarget(ctx, event.NS.Coll, event.To.Coll)
		if err != nil {
			return "", err
		}
		if done {
			cfg.Logger.Info("MongoDB source collection renamed; renamed target collection", "pipeline", cfg.Pipeline, "from", event.NS.Coll, "to", event.To.Coll)
			r.obs.removeCollection(event.NS.Coll)
			return "", nil
		}
	}
	if oldSelected {
		r.obs.removeCollection(event.NS.Coll)
		if err := r.dropTargetCollection(ctx, event.NS.Coll); err != nil {
			return "", err
		}
	}
	if !newSelected {
		return "", nil
	}
	// During the snapshot overlap (or into the selection) the destination is materialized
	// from the current source; later events are replayed on top. The re-copy record is
	// committed together with the checkpoint past this event, so a crash resumes the
	// copy from its last batch instead of starting over.
	cfg.Logger.Warn("MongoDB source collection renamed; re-copying destination collection from source", "pipeline", cfg.Pipeline, "from", event.NS.Coll, "to", event.To.Coll)
	if err := r.beginRecopy(ctx, event.To.Coll, token); err != nil {
		return "", err
	}
	return event.To.Coll, nil
}

// renameTarget renames the target collection from -> to (dropping an existing to).
// It reports false when from does not exist on the target (nothing to rename).
func (r *runner) renameTarget(ctx context.Context, from, to string) (bool, error) {
	names, err := r.target.ListCollectionNames(ctx, bson.D{{Key: "name", Value: from}})
	if err != nil {
		return false, err
	}
	if len(names) == 0 {
		// Replay after a crash that happened after the rename: the destination exists.
		dest, err := r.target.ListCollectionNames(ctx, bson.D{{Key: "name", Value: to}})
		return err == nil && len(dest) == 1, err
	}
	err = r.lease.withDDL(ctx, "rename collection "+from+" to "+to, func() error {
		return r.tgtClient.Database("admin").RunCommand(ctx, bson.D{
			{Key: "renameCollection", Value: r.cfg.TargetDB + "." + from}, {Key: "to", Value: r.cfg.TargetDB + "." + to}, {Key: "dropTarget", Value: true}, {Key: "writeConcern", Value: durableDDLWriteConcern()},
		}).Err()
	})
	return err == nil, err
}

// beginRecopy commits a re-copy record for coll together with a checkpoint at token
// (the stream resumes after it once the copy is complete).
func (r *runner) beginRecopy(ctx context.Context, coll string, token bson.Raw) error {
	next := r.state.clone()
	if len(token) > 0 {
		next.ResumeToken, next.StartAfter = token, false
	}
	p := collProgress{ID: progressID{Pipeline: r.cfg.Pipeline, Collection: coll}, Kind: progressRecopy}
	err := r.lease.txn(ctx, 0, func(tx context.Context) error {
		if _, err := r.progress.ReplaceOne(tx, bson.D{{Key: "_id", Value: p.ID}}, p, options.Replace().SetUpsert(true)); err != nil {
			return err
		}
		return r.saveState(tx, next)
	})
	if err != nil {
		return err
	}
	r.state = next
	return nil
}

// resumeRecopies runs every pending re-copy (from its last committed batch).
func (r *runner) resumeRecopies(ctx context.Context) error {
	pending, err := r.loadProgress(ctx, progressRecopy)
	if err != nil {
		return fmt.Errorf("load MongoDB re-copy progress: %w", err)
	}
	names := make([]string, 0, len(pending))
	for n := range pending {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		p := pending[name]
		r.obs.setPhase(PhaseRepair)
		if p.LastID.IsZero() && p.Copied == 0 {
			info, err := r.sourceCollectionInfo(ctx, name)
			if err != nil {
				return err
			}
			if err := r.recreateTargetCollection(ctx, info); err != nil {
				return fmt.Errorf("recreate target collection %s: %w", name, err)
			}
			p.Estimate, _ = r.source.Collection(name).EstimatedDocumentCount(ctx)
		}
		if err := r.copyCollection(ctx, 0, p); err != nil {
			return fmt.Errorf("re-copy collection %s.%s: %w", r.cfg.SourceDB, name, err)
		}
		err := r.lease.txn(ctx, 0, func(tx context.Context) error {
			_, err := r.progress.DeleteOne(tx, bson.D{{Key: "_id", Value: p.ID}})
			return err
		})
		if err != nil {
			return err
		}
	}
	if len(names) > 0 {
		r.obs.setPhase(PhaseStreaming)
		r.triggerReconcile()
	}
	return nil
}

var errUnsupportedCollectionType = errors.New("unsupported MongoDB collection type")

func (r *runner) sourceCollectionInfo(ctx context.Context, name string) (collectionInfo, error) {
	info := collectionInfo{Name: name, Type: "collection"}
	cur, err := r.source.ListCollections(ctx, bson.D{{Key: "name", Value: name}})
	if err != nil {
		return info, err
	}
	defer cur.Close(ctx)
	if cur.Next(ctx) {
		if err := cur.Decode(&info); err != nil {
			return info, err
		}
		info.Options = append(bson.Raw(nil), info.Options...)
		if info.Type != "" && info.Type != "collection" {
			return info, fmt.Errorf("%w: %s.%s has MongoDB collection type %q; only ordinary collections can be replicated", errUnsupportedCollectionType, r.cfg.SourceDB, name, info.Type)
		}
	}
	return info, cur.Err()
}

// repair re-copies one collection on operator request (admin socket snapshot_add /
// strand mongodb repair): its target copy is replaced by the current source content and
// the change stream continues from the position reached before the copy.
func (r *runner) repair(ctx context.Context, req repairRequest) error {
	coll := req.coll
	if !selected(r.cfg, r.cfg.SourceDB+"."+coll) {
		req.resp <- fmt.Errorf("collection %s.%s is not selected by this pipeline", r.cfg.SourceDB, coll)
		return nil
	}
	if names, err := r.source.ListCollectionNames(ctx, bson.D{{Key: "name", Value: coll}}); err != nil {
		req.resp <- err
		return err
	} else if len(names) == 0 {
		req.resp <- fmt.Errorf("collection %s.%s does not exist on the source", r.cfg.SourceDB, coll)
		return nil
	}
	if err := r.beginRecopy(ctx, coll, nil); err != nil {
		req.resp <- err
		return err
	}
	req.resp <- nil
	r.cfg.Logger.Warn("MongoDB collection re-copy requested; change apply pauses until it completes", "pipeline", r.cfg.Pipeline, "collection", coll)
	return r.resumeRecopies(ctx)
}
