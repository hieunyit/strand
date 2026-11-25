// Package mongocdc implements MongoDB-to-MongoDB bootstrap and change-stream replication.
// Documents stay BSON; the relational transaction model is deliberately not involved.
//
// Correctness model (see docs/mongodb-cdc.md):
//   - The change-stream start position is captured (a resume token from a freshly opened
//     stream) BEFORE the first document is read, so every write that the snapshot may
//     miss is replayed afterwards. Replay converges: insert/replace carry a full image,
//     update events are applied as update descriptions to the target image (or re-read
//     from the source when that is not exact), delete is delete-if-exists.
//   - Every target write batch is committed in a target transaction together with
//     (a) the resume token / snapshot progress it reaches and (b) a fencing update of a
//     fence document owned by this process. A process whose lease was taken over cannot
//     commit anything, however long it was paused.
//   - Lost oplog history fails closed with ErrResyncRequired.
//   - Transient failures (elections, network errors, step-downs) restart the work from
//     the last committed checkpoint inside the same process, keeping the lease.
package mongocdc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const (
	stateDatabase      = "_strand_cdc"
	stateCollection    = "mongo_sync_state"
	progressCollection = "mongo_sync_progress"
	lockCollection     = "mongo_sync_locks"
	indexCollection    = "mongo_sync_indexes"

	defaultLeaseDuration = 45 * time.Second
	defaultSnapshotBatch = 1000
	snapshotBatchBytes   = 8 << 20
	defaultStreamBatch   = 2000
	maxStreamBatch       = 1_000_000
	streamBatchBytes     = 8 << 20
	// defaultMaxTransactionBytes keeps a source transaction atomic up to 64 MiB of
	// selected change events; maxTransactionBytes is the configurable ceiling.
	defaultMaxTransactionBytes = 8 * streamBatchBytes
	maxTransactionBytes        = 4096 << 20
	defaultCheckpointEvery     = time.Second
	defaultMaxAwait            = 500 * time.Millisecond
	defaultRetryBackoffMax     = 30 * time.Second
	defaultIndexReconcile      = time.Minute
	progressLogInterval        = 10 * time.Second
	minWireVersion44           = 9  // MongoDB 4.4: implicit collection creation inside transactions.
	wireVersion50              = 13 // MongoDB 5.0: $v:2 update diffs, $setField.
	wireVersion60              = 17 // MongoDB 6.0: showExpandedEvents.
	wireVersion61              = 18 // MongoDB 6.1: disambiguatedPaths in update descriptions.
	maxSnapshotParallel        = 32

	// UpdateModeDelta applies update descriptions to the target image (default).
	UpdateModeDelta = "delta"
	// UpdateModeLookup requests fullDocument: updateLookup and writes the full image.
	UpdateModeLookup = "lookup"
	// UpdateModeAuto (""): delta on 4.4 and 6.1+, lookup on 5.0-6.0 (where a field name
	// containing '.' cannot be told apart from a nested path in an update description).
	UpdateModeAuto = "auto"
)

// ErrResyncRequired reports that the source no longer has the change history needed to
// continue from the saved checkpoint. The pipeline must be reseeded under a new name
// (or with its checkpoint removed); silently starting "from now" would lose changes.
var ErrResyncRequired = errors.New("mongodb cdc: source change history is no longer available; a fresh snapshot (new pipeline name) is required")

// ErrLeaseLost reports that another process took over the pipeline lease. No write
// of the stale process can commit after that point (writes are fenced).
var ErrLeaseLost = errors.New("mongodb cdc: lease lost to another process")

// Config defines one MongoDB-to-MongoDB replication. Credentials in URIs are never logged.
type Config struct {
	Pipeline     string
	SourceURI    string
	TargetURI    string
	SourceDialer options.ContextDialer
	TargetDialer options.ContextDialer
	SourceDB     string
	TargetDB     string
	Include      []string // database.collection glob patterns
	Exclude      []string
	// OverwriteTarget allows snapshot bootstrap to drop and recreate matching target collections.
	// It is intentionally opt-in because bootstrap can destroy existing data.
	OverwriteTarget bool
	Logger          *slog.Logger

	// Tuning; zero means default.
	LeaseDuration time.Duration // lease TTL on the target (target clock); refreshed at TTL/3
	SnapshotBatch int           // documents per snapshot write transaction
	StreamBatch   int           // change events per apply transaction
	// MaxTransactionBytes bounds one atomic source transaction (selected events only).
	MaxTransactionBytes int
	CheckpointEvery     time.Duration // idle resume-token checkpoint interval
	MaxAwait            time.Duration // change stream getMore maxAwaitTimeMS
	SnapshotParallel    int           // collections copied concurrently (default 1, max 32)
	// UpdateMode: UpdateModeDelta (default) or UpdateModeLookup.
	UpdateMode string
	// ReplicateIndexes creates the source's indexes and collection options (validators,
	// collation, TTL, ...) on the target and keeps them in sync (opt-in).
	ReplicateIndexes    bool
	IndexReconcileEvery time.Duration
	RetryBackoffMax     time.Duration // cap of the backoff between automatic restarts
	// Observer receives progress for the admin socket and metrics (optional).
	Observer *Observer
}

func (c *Config) setDefaults() {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = defaultLeaseDuration
	}
	if c.SnapshotBatch <= 0 {
		c.SnapshotBatch = defaultSnapshotBatch
	}
	if c.StreamBatch <= 0 {
		c.StreamBatch = defaultStreamBatch
	}
	if c.MaxTransactionBytes <= 0 {
		c.MaxTransactionBytes = defaultMaxTransactionBytes
	}
	if c.CheckpointEvery <= 0 {
		c.CheckpointEvery = defaultCheckpointEvery
	}
	if c.MaxAwait <= 0 {
		c.MaxAwait = defaultMaxAwait
	}
	if c.SnapshotParallel <= 0 {
		c.SnapshotParallel = 1
	}
	if c.IndexReconcileEvery <= 0 {
		c.IndexReconcileEvery = defaultIndexReconcile
	}
	if c.RetryBackoffMax <= 0 {
		c.RetryBackoffMax = defaultRetryBackoffMax
	}
	if c.Observer == nil {
		c.Observer = NewObserver(c.Pipeline)
	}
}

// collProgress is the copy progress of one collection (initial snapshot or re-copy),
// one document per collection in _strand_cdc.mongo_sync_progress.
type collProgress struct {
	ID       progressID    `bson:"_id"`
	Kind     string        `bson:"kind"` // progressSnapshot | progressRecopy
	LastID   bson.RawValue `bson:"last_id,omitempty"`
	Copied   int64         `bson:"copied"`
	Done     bool          `bson:"done"`
	Estimate int64         `bson:"estimate,omitempty"`
}

type progressID struct {
	Pipeline   string `bson:"p"`
	Collection string `bson:"c"`
}

const (
	progressSnapshot = "snapshot"
	progressRecopy   = "recopy"
)

// legacyProgress is the per-collection progress kept inside the state document by the
// first (experimental) version; it is migrated to progress documents on load.
type legacyProgress struct {
	Name   string        `bson:"name"`
	LastID bson.RawValue `bson:"last_id,omitempty"`
	Copied int64         `bson:"copied"`
	Done   bool          `bson:"done"`
}

type syncState struct {
	ID              string         `bson:"_id"`
	SourceID        string         `bson:"source_id"`
	TargetID        string         `bson:"target_id"`
	ScopeID         string         `bson:"scope_id"`
	SourceDB        string         `bson:"source_db"`
	TargetDB        string         `bson:"target_db"`
	SnapshotStarted bool           `bson:"snapshot_started,omitempty"`
	InitialDone     bool           `bson:"initial_done"`
	StartAt         bson.Timestamp `bson:"start_at"`
	// ConsistentAt: source cluster time after the initial snapshot completed. Events
	// after it are applied to a target that matches the source at the previous event.
	ConsistentAt bson.Timestamp   `bson:"consistent_at,omitempty"`
	ResumeToken  bson.Raw         `bson:"resume_token,omitempty"`
	StartAfter   bool             `bson:"start_after,omitempty"`
	AppliedTime  bson.Timestamp   `bson:"applied_time,omitempty"` // cluster time of the last applied event
	Collections  []legacyProgress `bson:"collections,omitempty"`
	UpdatedAt    time.Time        `bson:"updated_at"`
}

func (s syncState) clone() syncState {
	s.Collections = nil
	s.ResumeToken = append(bson.Raw(nil), s.ResumeToken...)
	return s
}

// Run snapshots selected collections, then applies and checkpoints change events.
// It returns ctx.Err() on cancellation, ErrLeaseLost when fenced out, and
// ErrResyncRequired when the oplog no longer covers the checkpoint. Transient errors
// (failover, network) are retried from the last checkpoint with backoff.
func Run(ctx context.Context, cfg Config) (err error) {
	if ctx == nil {
		return errors.New("mongodb cdc: context is required")
	}
	cfg.setDefaults()
	obs := cfg.Observer
	defer func() {
		err = redactError(err, cfg.SourceURI, cfg.TargetURI)
		if err != nil && !errors.Is(err, context.Canceled) {
			obs.setFailed(err.Error())
		}
		obs.setPhase(PhaseStopped)
	}()
	if err := validateConfig(cfg); err != nil {
		return err
	}
	obs.setPhase(PhaseStarting)
	source, err := connect(ctx, cfg.SourceURI, cfg.SourceDialer, nil)
	if err != nil {
		return fmt.Errorf("connect MongoDB source: %w", err)
	}
	defer disconnect(source)
	target, err := connect(ctx, cfg.TargetURI, cfg.TargetDialer, durableTargetWriteConcern())
	if err != nil {
		return fmt.Errorf("connect MongoDB target: %w", err)
	}
	defer disconnect(target)

	srcHello, err := hello(ctx, source)
	if err != nil {
		return fmt.Errorf("MongoDB source hello: %w", err)
	}
	tgtHello, err := hello(ctx, target)
	if err != nil {
		return fmt.Errorf("MongoDB target hello: %w", err)
	}
	if err := checkTopology("source", srcHello); err != nil {
		return err
	}
	if err := checkTopology("target", tgtHello); err != nil {
		return err
	}
	cfg.UpdateMode = resolveUpdateMode(cfg.UpdateMode, srcHello.MaxWireVersion)
	sourceEndpoint, err := canonicalEndpoint(cfg.SourceURI)
	if err != nil {
		return fmt.Errorf("canonicalize MongoDB source URI: %w", err)
	}
	targetEndpoint, err := canonicalEndpoint(cfg.TargetURI)
	if err != nil {
		return fmt.Errorf("canonicalize MongoDB target URI: %w", err)
	}
	sourceID := identityHash(srcHello.SetName, srcHello.Msg, sourceEndpoint)
	targetID := identityHash(tgtHello.SetName, tgtHello.Msg, targetEndpoint)
	if cfg.SourceDB == cfg.TargetDB && sameDeployment(srcHello, tgtHello, sourceEndpoint, targetEndpoint) {
		return errors.New("mongodb cdc: source and target resolve to the same MongoDB deployment and database; refusing a self-replicating pipeline")
	}

	control := target.Database(stateDatabase, targetDBOptions())
	// A lease that is not refreshed expires within one TTL (e.g. the previous owner
	// crashed on another host): wait for it instead of exiting, up to 2 TTLs.
	var l *lease
	waitUntil := time.Now().Add(2*cfg.LeaseDuration + 5*time.Second)
	for {
		l, err = acquireLease(ctx, target, control.Collection(lockCollection), cfg.TargetDB, cfg.Pipeline, cfg.LeaseDuration, cfg.SnapshotParallel+1, cfg.Logger)
		if err == nil {
			break
		}
		if !errors.Is(err, ErrLeaseHeld) || time.Now().After(waitUntil) {
			return err
		}
		obs.setPhase(PhaseStarting)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(min(time.Second, cfg.LeaseDuration/3)):
		}
	}
	defer l.release()

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		l.refreshLoop(runCtx, cfg.Logger, cancel, obs.fenced)
	}()
	defer func() {
		cancel(nil)
		<-refreshDone
	}()

	backoff := time.Second
	for attempt := 0; ; attempt++ {
		r := &runner{
			cfg: cfg, obs: obs, srcClient: source, tgtClient: target,
			source:   source.Database(cfg.SourceDB, options.Database().SetReadConcern(readconcern.Majority())),
			target:   target.Database(cfg.TargetDB, targetDBOptions()),
			states:   control.Collection(stateCollection),
			progress: control.Collection(progressCollection),
			indexes:  control.Collection(indexCollection),
			sourceID: sourceID, targetID: targetID, lease: l, srcWire: srcHello.MaxWireVersion,
		}
		began := time.Now()
		err = r.run(runCtx, srcHello.OperationTime)
		if cause := context.Cause(runCtx); cause != nil && errors.Is(cause, ErrLeaseLost) {
			return ErrLeaseLost
		}
		if err == nil || ctx.Err() != nil || errors.Is(err, ErrLeaseLost) || !retryable(err) {
			if errors.Is(err, ErrLeaseLost) {
				obs.fenced()
			}
			return err
		}
		if time.Since(began) > 2*cfg.RetryBackoffMax {
			backoff = time.Second
		}
		obs.retried(err)
		cfg.Logger.Warn("MongoDB CDC transient error; resuming from the last checkpoint", "pipeline", cfg.Pipeline,
			"error", redactString(err.Error(), cfg.SourceURI, cfg.TargetURI), "retry_in", backoff.String(), "attempt", attempt+1)
		obs.setPhase(PhaseRetrying)
		select {
		case <-runCtx.Done():
			if cause := context.Cause(runCtx); errors.Is(cause, ErrLeaseLost) {
				return ErrLeaseLost
			}
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, cfg.RetryBackoffMax)
	}
}

// targetDBOptions: target reads use readConcern local on the primary. This process is
// the only writer and every write it made was acknowledged w:majority (durable across
// failover), so local reads see exactly the committed state. A majority read is NOT
// enough: the majority snapshot visible to readers may lag the commit point just
// acknowledged, and a stale pre-image (e.g. of a document deleted by the previous
// batch) would then be updated and written back.
func targetDBOptions() *options.DatabaseOptionsBuilder {
	return options.Database().SetReadConcern(readconcern.Local()).SetReadPreference(readpref.Primary())
}

// resolveUpdateMode picks the update application for the source version.
func resolveUpdateMode(mode string, srcWire int32) string {
	if mode == UpdateModeDelta || mode == UpdateModeLookup {
		return mode
	}
	if srcWire >= wireVersion50 && srcWire < wireVersion61 {
		return UpdateModeLookup
	}
	return UpdateModeDelta
}

// retryable reports errors after which restarting from the last committed checkpoint
// is expected to succeed: network errors, elections and step-downs, server shutdown,
// killed cursors, write-concern timeouts and unknown commit results. Everything else
// (auth, configuration, lost history, unsupported events) stops the process.
func retryable(err error) bool {
	if err == nil || errors.Is(err, ErrResyncRequired) || errors.Is(err, ErrDDLUncertain) || errors.Is(err, ErrLeaseLost) || errors.Is(err, context.Canceled) {
		return false
	}
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var le mongo.LabeledError
	if errors.As(err, &le) {
		for _, l := range []string{"TransientTransactionError", "UnknownTransactionCommitResult", "RetryableWriteError", "ResumableChangeStreamError", "NetworkError"} {
			if le.HasErrorLabel(l) {
				return true
			}
		}
	}
	if serverErrorCode(err,
		6, 7, 89, 91, 9001, // HostUnreachable, HostNotFound, NetworkTimeout, ShutdownInProgress, SocketException
		10107, 13435, 13436, 189, 11600, 11602, // not primary / step-down / interrupted
		43, 175, 237, // CursorNotFound, QueryPlanKilled, CursorKilled
		64, 262, 50, // WriteConcernFailed, ExceededTimeLimit, MaxTimeMSExpired
		112, 117, 133, 134, 251, // WriteConflict, ConflictingOperationInProgress, FailedToSatisfyReadPreference, ReadConcernMajorityNotAvailableYet, NoSuchTransaction
	) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "server selection error") || strings.Contains(msg, "connection pool") ||
		strings.Contains(msg, "not primary") || strings.Contains(msg, "node is recovering")
}

type runner struct {
	cfg                  Config
	obs                  *Observer
	srcClient, tgtClient *mongo.Client
	source               *mongo.Database
	target               *mongo.Database
	states               *mongo.Collection
	progress             *mongo.Collection
	indexes              *mongo.Collection
	sourceID, targetID   string
	lease                *lease
	state                syncState
	srcWire              int32
	// consistent: applied events are past the snapshot overlap (ConsistentAt) or the
	// stream has caught up; unique indexes may be built from then on.
	consistent       atomic.Bool
	reconcileTrigger chan struct{}
}

func (r *runner) run(ctx context.Context, operationTime bson.Timestamp) error {
	cfg := r.cfg
	state, created, err := loadOrCreateState(ctx, r.states, cfg, r.sourceID, r.targetID, operationTime)
	if err != nil {
		return err
	}
	r.state = state
	if len(r.state.Collections) > 0 {
		if err := r.migrateLegacyProgress(ctx); err != nil {
			return fmt.Errorf("migrate MongoDB snapshot progress: %w", err)
		}
	}
	if !r.state.InitialDone {
		r.obs.setPhase(PhaseSnapshot)
		if created || len(r.state.ResumeToken) == 0 && !r.state.SnapshotStarted {
			if !cfg.OverwriteTarget {
				if err := ensureTargetCollectionsAbsent(ctx, r.target, cfg); err != nil {
					return err
				}
			}
			// Capture the change-stream start position BEFORE reading any document.
			// Every change after this token is replayed after the snapshot.
			token, err := currentResumeToken(ctx, r.source, cfg.MaxAwait)
			if err != nil {
				return fmt.Errorf("capture MongoDB change stream start position: %w", err)
			}
			r.state.ResumeToken = token
			r.state.StartAfter = false
		}
		r.state.SnapshotStarted = true
		if err := r.persist(ctx); err != nil {
			return fmt.Errorf("save MongoDB snapshot-start checkpoint: %w", err)
		}
		cfg.Logger.Info("MongoDB initial snapshot started", "pipeline", cfg.Pipeline, "source_database", cfg.SourceDB, "target_database", cfg.TargetDB, "parallel", cfg.SnapshotParallel)
		if err := r.snapshot(ctx); err != nil {
			return fmt.Errorf("MongoDB initial snapshot: %w", err)
		}
		at, err := r.sourceTime(ctx)
		if err != nil {
			return fmt.Errorf("read MongoDB source cluster time: %w", err)
		}
		r.state.ConsistentAt = at
		r.state.InitialDone = true
		if err := r.persist(ctx); err != nil {
			return fmt.Errorf("save MongoDB snapshot checkpoint: %w", err)
		}
		cfg.Logger.Info("MongoDB initial snapshot complete", "pipeline", cfg.Pipeline)
	}
	if r.state.ConsistentAt.T == 0 {
		// Checkpoint of the first experimental version: treat everything up to now as
		// possibly overlapping the snapshot (safe: renames re-copy instead of renaming).
		at, err := r.sourceTime(ctx)
		if err != nil {
			return fmt.Errorf("read MongoDB source cluster time: %w", err)
		}
		r.state.ConsistentAt = at
		if err := r.persist(ctx); err != nil {
			return err
		}
	}
	return r.stream(ctx)
}

// sourceTime returns the source's latest applied cluster time: >= every change the
// stream has returned and >= the start position of a stream opened before the call.
func (r *runner) sourceTime(ctx context.Context) (bson.Timestamp, error) {
	h, err := hello(ctx, r.srcClient)
	if err != nil {
		return bson.Timestamp{}, err
	}
	at := h.OperationTime
	for _, ts := range []bson.Timestamp{h.LastWrite.MajorityOpTime.TS, h.LastWrite.OpTime.TS} {
		if ts.After(at) {
			at = ts
		}
	}
	return at, nil
}

// persist saves the state with a fenced transaction (slot 0).
func (r *runner) persist(ctx context.Context) error {
	st := r.state.clone()
	if err := r.lease.txn(ctx, 0, func(tx context.Context) error { return r.saveState(tx, st) }); err != nil {
		return err
	}
	r.state = st
	return nil
}

// saveState writes st; callers adopt st as r.state only after the transaction commits.
func (r *runner) saveState(ctx context.Context, st syncState) error {
	st.UpdatedAt = time.Now().UTC()
	st.Collections = nil
	result, err := r.states.ReplaceOne(ctx, bson.D{{Key: "_id", Value: st.ID}, {Key: "source_id", Value: r.sourceID}}, st)
	if err != nil {
		return err
	}
	if result.MatchedCount != 1 {
		return errors.New("checkpoint row disappeared or source identity changed")
	}
	return nil
}

func (r *runner) migrateLegacyProgress(ctx context.Context) error {
	legacy := r.state.Collections
	st := r.state.clone()
	err := r.lease.txn(ctx, 0, func(tx context.Context) error {
		for _, c := range legacy {
			p := collProgress{ID: progressID{r.cfg.Pipeline, c.Name}, Kind: progressSnapshot, LastID: c.LastID, Copied: c.Copied, Done: c.Done}
			if _, err := r.progress.ReplaceOne(tx, bson.D{{Key: "_id", Value: p.ID}}, p, options.Replace().SetUpsert(true)); err != nil {
				return err
			}
		}
		return r.saveState(tx, st)
	})
	if err != nil {
		return err
	}
	r.state = st
	return nil
}

func durableTargetWriteConcern() *writeconcern.WriteConcern {
	journal := true
	return &writeconcern.WriteConcern{W: writeconcern.WCMajority, Journal: &journal}
}

func ensureTargetCollectionsAbsent(ctx context.Context, target *mongo.Database, cfg Config) error {
	names, err := target.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("check target collections before bootstrap: %w", err)
	}
	for _, name := range names {
		if selected(cfg, cfg.SourceDB+"."+name) {
			return fmt.Errorf("target collection %s already exists; use --overwrite-target (mongodb.overwrite_target: true) only if replacing its contents is intended", name)
		}
	}
	return nil
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.Pipeline) == "" || len(cfg.Pipeline) > 128 {
		return errors.New("mongodb cdc: pipeline name is required and must be at most 128 bytes")
	}
	if strings.TrimSpace(cfg.SourceURI) == "" || strings.TrimSpace(cfg.TargetURI) == "" || strings.TrimSpace(cfg.SourceDB) == "" || strings.TrimSpace(cfg.TargetDB) == "" {
		return errors.New("mongodb cdc: source/target URI and database are required")
	}
	if cfg.SourceDB == stateDatabase || cfg.TargetDB == stateDatabase {
		return fmt.Errorf("mongodb cdc: %q is reserved for Strand checkpoints", stateDatabase)
	}
	for _, db := range []string{cfg.SourceDB, cfg.TargetDB} {
		if db == "admin" || db == "local" || db == "config" {
			return fmt.Errorf("mongodb cdc: system database %q cannot be replicated", db)
		}
	}
	if cfg.UpdateMode != "" && cfg.UpdateMode != UpdateModeAuto && cfg.UpdateMode != UpdateModeDelta && cfg.UpdateMode != UpdateModeLookup {
		return fmt.Errorf("mongodb cdc: invalid update mode %q (want %s | %s)", cfg.UpdateMode, UpdateModeDelta, UpdateModeLookup)
	}
	if cfg.SnapshotParallel > maxSnapshotParallel {
		return fmt.Errorf("mongodb cdc: snapshot parallelism %d exceeds %d", cfg.SnapshotParallel, maxSnapshotParallel)
	}
	if cfg.StreamBatch > maxStreamBatch {
		return fmt.Errorf("mongodb cdc: stream batch %d exceeds %d", cfg.StreamBatch, maxStreamBatch)
	}
	if cfg.MaxTransactionBytes > maxTransactionBytes {
		return fmt.Errorf("mongodb cdc: max transaction size %d MiB exceeds %d MiB", cfg.MaxTransactionBytes>>20, maxTransactionBytes>>20)
	}
	if len(cfg.Include) == 0 {
		return errors.New("mongodb cdc: at least one --include database.collection pattern is required")
	}
	for _, pattern := range append(append([]string(nil), cfg.Include...), cfg.Exclude...) {
		if strings.TrimSpace(pattern) == "" {
			return errors.New("mongodb cdc: collection patterns must not be empty")
		}
		if _, err := path.Match(pattern, "validation.collection"); err != nil {
			return fmt.Errorf("mongodb cdc: invalid collection pattern %q: %w", pattern, err)
		}
		dbPattern, _, ok := strings.Cut(pattern, ".")
		if !ok || dbPattern == "" {
			return fmt.Errorf("mongodb cdc: collection pattern %q must be database.collection", pattern)
		}
		matches, err := path.Match(dbPattern, cfg.SourceDB)
		if err != nil || !matches {
			return fmt.Errorf("mongodb cdc: collection pattern %q does not select source database %q", pattern, cfg.SourceDB)
		}
	}
	return nil
}

func connect(ctx context.Context, uri string, dialer options.ContextDialer, wc *writeconcern.WriteConcern) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri).SetRetryWrites(true).SetRetryReads(true)
	if dialer != nil {
		clientOptions.SetDialer(dialer)
	}
	if wc != nil {
		clientOptions.SetWriteConcern(wc)
	}
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return client, nil
}

func disconnect(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = client.Disconnect(ctx)
}

type helloReply struct {
	SetName         string   `bson:"setName"`
	Hosts           []string `bson:"hosts"`
	Msg             string   `bson:"msg"`
	MaxWireVersion  int32    `bson:"maxWireVersion"`
	TopologyVersion struct {
		ProcessID bson.ObjectID `bson:"processId"`
	} `bson:"topologyVersion"`
	OperationTime bson.Timestamp `bson:"operationTime"`
	LastWrite     struct {
		MajorityOpTime struct {
			TS bson.Timestamp `bson:"ts"`
		} `bson:"majorityOpTime"`
		OpTime struct {
			TS bson.Timestamp `bson:"ts"`
		} `bson:"opTime"`
	} `bson:"lastWrite"`
}

func hello(ctx context.Context, client *mongo.Client) (helloReply, error) {
	var h helloReply
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}, options.RunCmd().SetReadPreference(readpref.Primary())).Decode(&h)
	return h, err
}

// checkTopology accepts only replica sets: change streams need one on the source,
// and target transactions (fenced, atomic checkpoints) need one on the target.
// Sharded clusters are refused because they have not been validated.
func checkTopology(role string, h helloReply) error {
	if h.Msg == "isdbgrid" {
		return fmt.Errorf("mongodb cdc: %s is a sharded cluster (mongos); only replica sets are supported", role)
	}
	if h.SetName == "" {
		return fmt.Errorf("mongodb cdc: %s is not a replica set member; change streams and transactions require a replica set (a single-node replica set is fine)", role)
	}
	if h.MaxWireVersion < minWireVersion44 {
		return fmt.Errorf("mongodb cdc: %s MongoDB server is older than 4.4; not supported", role)
	}
	return nil
}

// sameDeployment detects a pipeline pointing at itself. Replica set name and member
// host list are not unique (every container-default set is rs0 on localhost:27017), so
// the primary's process id (hello.topologyVersion, 4.4+) is compared instead.
func sameDeployment(a, b helloReply, endpointA, endpointB string) bool {
	if endpointA == endpointB {
		return true
	}
	return a.TopologyVersion.ProcessID != (bson.ObjectID{}) && a.TopologyVersion.ProcessID == b.TopologyVersion.ProcessID
}

func identityHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}

func canonicalEndpoint(uri string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(uri))
	if err != nil || parsed.Host == "" {
		return "", errors.New("MongoDB URI has no valid host")
	}
	hosts := strings.Split(parsed.Host, ",")
	for i, seed := range hosts {
		seed = strings.TrimSpace(seed)
		host, port, splitErr := net.SplitHostPort(seed)
		if splitErr != nil {
			host = strings.Trim(seed, "[]")
			port = "27017"
		}
		hosts[i] = strings.ToLower(net.JoinHostPort(host, port))
	}
	sort.Strings(hosts)
	return strings.ToLower(parsed.Scheme) + "://" + strings.Join(hosts, ","), nil
}

func loadOrCreateState(ctx context.Context, coll *mongo.Collection, cfg Config, sourceID, targetID string, startAt bson.Timestamp) (syncState, bool, error) {
	scopeID := replicationScopeID(cfg)
	var state syncState
	err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: cfg.Pipeline}}).Decode(&state)
	if err == nil {
		if state.SourceID != sourceID || state.TargetID != targetID || state.ScopeID != scopeID || state.SourceDB != cfg.SourceDB || state.TargetDB != cfg.TargetDB {
			return syncState{}, false, errors.New("MongoDB checkpoint belongs to a different source, target, database, or collection filter; use a new pipeline name")
		}
		return state, false, nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return syncState{}, false, fmt.Errorf("load MongoDB checkpoint: %w", err)
	}
	state = syncState{ID: cfg.Pipeline, SourceID: sourceID, TargetID: targetID, ScopeID: scopeID, SourceDB: cfg.SourceDB, TargetDB: cfg.TargetDB, StartAt: startAt, UpdatedAt: time.Now().UTC()}
	if _, err := coll.InsertOne(ctx, state); err != nil {
		return syncState{}, false, fmt.Errorf("create MongoDB checkpoint: %w", err)
	}
	return state, true, nil
}

func replicationScopeID(cfg Config) string {
	include := append([]string(nil), cfg.Include...)
	exclude := append([]string(nil), cfg.Exclude...)
	sort.Strings(include)
	sort.Strings(exclude)
	material := strings.Join([]string{cfg.SourceDB, cfg.TargetDB, strings.Join(include, "\x00"), strings.Join(exclude, "\x00")}, "\x01")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}

func selected(cfg Config, qualified string) bool {
	included := false
	for _, pattern := range cfg.Include {
		if ok, _ := path.Match(pattern, qualified); ok {
			included = true
			break
		}
	}
	if !included {
		return false
	}
	for _, pattern := range cfg.Exclude {
		if ok, _ := path.Match(pattern, qualified); ok {
			return false
		}
	}
	return true
}

// redactError strips URI credentials from an error message while keeping errors.Is/As.
func redactError(err error, uris ...string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	redacted := redactString(msg, uris...)
	if redacted == msg {
		return err
	}
	return &redactedError{msg: redacted, err: err}
}

func redactString(msg string, uris ...string) string {
	for _, uri := range uris {
		for _, secret := range uriSecrets(uri) {
			msg = strings.ReplaceAll(msg, secret, "***")
		}
	}
	return msg
}

// uriSecrets returns the strings that must never appear in logs: the full URI, the
// raw and decoded userinfo and password.
func uriSecrets(uri string) []string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return nil
	}
	out := []string{uri}
	_, rest, ok := strings.Cut(uri, "://")
	if !ok {
		return out
	}
	at := strings.LastIndex(strings.SplitN(rest, "/", 2)[0], "@")
	if at < 0 {
		return out
	}
	userinfo := rest[:at]
	out = append(out, userinfo)
	if _, pass, ok := strings.Cut(userinfo, ":"); ok && pass != "" {
		out = append(out, pass)
		if dec, err := url.PathUnescape(pass); err == nil && dec != pass {
			out = append(out, dec)
		}
	}
	// Longest first so a short secret does not break a longer match.
	sort.Slice(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}

type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

func serverErrorCode(err error, codes ...int) bool {
	var se mongo.ServerError
	if !errors.As(err, &se) {
		return false
	}
	for _, c := range codes {
		if se.HasErrorCode(c) {
			return true
		}
	}
	return false
}
