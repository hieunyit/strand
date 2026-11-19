// Package mongocdc implements MongoDB-to-MongoDB bootstrap and change-stream replication.
// Documents stay BSON; the relational transaction model is deliberately not involved.
//
// Correctness model (see docs/mongodb-cdc.md):
//   - The change-stream start position is captured (a resume token from a freshly opened
//     stream) BEFORE the first document is read, so every write that the snapshot may
//     miss is replayed afterwards. Replay is idempotent: insert/replace/update become
//     an upsert of a full document image by _id, delete is delete-if-exists.
//   - Every target write batch is committed in a target transaction together with
//     (a) the resume token / snapshot progress it reaches and (b) a fencing update of
//     the lease document owned by this process. A process whose lease was taken over
//     cannot commit anything, however long it was paused.
//   - Lost oplog history fails closed with ErrResyncRequired.
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
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const (
	stateDatabase   = "_strand_cdc"
	stateCollection = "mongo_sync_state"
	lockCollection  = "mongo_sync_locks"

	defaultLeaseDuration   = 45 * time.Second
	defaultSnapshotBatch   = 1000
	snapshotBatchBytes     = 8 << 20
	defaultStreamBatch     = 2000
	streamBatchBytes       = 8 << 20
	defaultCheckpointEvery = time.Second
	defaultMaxAwait        = 500 * time.Millisecond
	progressLogInterval    = 10 * time.Second
	minWireVersion44       = 9 // MongoDB 4.4: implicit collection creation inside transactions.
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
	Pipeline  string
	SourceURI string
	TargetURI string
	SourceDB  string
	TargetDB  string
	Include   []string // database.collection glob patterns
	Exclude   []string
	// OverwriteTarget allows snapshot bootstrap to drop and recreate matching target collections.
	// It is intentionally opt-in because bootstrap can destroy existing data.
	OverwriteTarget bool
	Logger          *slog.Logger

	// Tuning; zero means default.
	LeaseDuration   time.Duration // lease TTL on the target (target clock); refreshed at TTL/3
	SnapshotBatch   int           // documents per snapshot write transaction
	StreamBatch     int           // change events per apply transaction
	CheckpointEvery time.Duration // idle resume-token checkpoint interval
	MaxAwait        time.Duration // change stream getMore maxAwaitTimeMS
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
	if c.CheckpointEvery <= 0 {
		c.CheckpointEvery = defaultCheckpointEvery
	}
	if c.MaxAwait <= 0 {
		c.MaxAwait = defaultMaxAwait
	}
}

type collProgress struct {
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
	ResumeToken     bson.Raw       `bson:"resume_token,omitempty"`
	StartAfter      bool           `bson:"start_after,omitempty"`
	Collections     []collProgress `bson:"collections,omitempty"`
	UpdatedAt       time.Time      `bson:"updated_at"`
}

func (s syncState) clone() syncState {
	s.Collections = append([]collProgress(nil), s.Collections...)
	s.ResumeToken = append(bson.Raw(nil), s.ResumeToken...)
	return s
}

func (s *syncState) progress(name string) *collProgress {
	for i := range s.Collections {
		if s.Collections[i].Name == name {
			return &s.Collections[i]
		}
	}
	return nil
}

// Run snapshots selected collections, then applies and checkpoints change events.
// It returns ctx.Err() on cancellation, ErrLeaseLost when fenced out, and
// ErrResyncRequired when the oplog no longer covers the checkpoint.
func Run(ctx context.Context, cfg Config) (err error) {
	if ctx == nil {
		return errors.New("mongodb cdc: context is required")
	}
	cfg.setDefaults()
	defer func() { err = redactError(err, cfg.SourceURI, cfg.TargetURI) }()
	if err := validateConfig(cfg); err != nil {
		return err
	}
	source, err := connect(ctx, cfg.SourceURI, nil)
	if err != nil {
		return fmt.Errorf("connect MongoDB source: %w", err)
	}
	defer disconnect(source)
	target, err := connect(ctx, cfg.TargetURI, durableTargetWriteConcern())
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

	control := target.Database(stateDatabase)
	states := control.Collection(stateCollection)
	l, err := acquireLease(ctx, target, control.Collection(lockCollection), cfg.TargetDB, cfg.Pipeline, cfg.LeaseDuration)
	if err != nil {
		return err
	}
	defer l.release()

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		l.refreshLoop(runCtx, cfg.Logger, cancel)
	}()
	defer func() {
		cancel(nil)
		<-refreshDone
	}()

	sourceDB := source.Database(cfg.SourceDB, options.Database().SetReadConcern(readconcern.Majority()))
	targetDB := target.Database(cfg.TargetDB)
	r := &runner{cfg: cfg, source: sourceDB, target: targetDB, states: states, sourceID: sourceID, lease: l}

	err = r.run(runCtx, targetID, srcHello.OperationTime)
	if cause := context.Cause(runCtx); cause != nil && errors.Is(cause, ErrLeaseLost) {
		return ErrLeaseLost
	}
	return err
}

type runner struct {
	cfg      Config
	source   *mongo.Database
	target   *mongo.Database
	states   *mongo.Collection
	sourceID string
	lease    *lease
	state    syncState
}

func (r *runner) run(ctx context.Context, targetID string, operationTime bson.Timestamp) error {
	cfg := r.cfg
	state, created, err := loadOrCreateState(ctx, r.states, cfg, r.sourceID, targetID, operationTime)
	if err != nil {
		return err
	}
	r.state = state
	if !r.state.InitialDone {
		if created || len(r.state.ResumeToken) == 0 && !r.state.SnapshotStarted {
			if !cfg.OverwriteTarget {
				if err := ensureTargetCollectionsAbsent(ctx, r.target, cfg); err != nil {
					return err
				}
			}
			// Capture the change-stream start position BEFORE reading any document.
			// Every change after this token is replayed after the snapshot; replay is idempotent.
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
		cfg.Logger.Info("MongoDB initial snapshot started", "pipeline", cfg.Pipeline, "source_database", cfg.SourceDB, "target_database", cfg.TargetDB, "resumed_collections", len(r.state.Collections))
		if err := r.snapshot(ctx); err != nil {
			return fmt.Errorf("MongoDB initial snapshot: %w", err)
		}
		r.state.InitialDone = true
		if err := r.persist(ctx); err != nil {
			return fmt.Errorf("save MongoDB snapshot checkpoint: %w", err)
		}
		cfg.Logger.Info("MongoDB initial snapshot complete", "pipeline", cfg.Pipeline)
	}
	return r.stream(ctx)
}

// persist saves the state with a fenced transaction.
func (r *runner) persist(ctx context.Context) error {
	st := r.state.clone()
	return r.lease.txn(ctx, func(tx context.Context) error { return r.saveState(tx, st) })
}

// saveState writes st; callers adopt st as r.state only after the transaction commits.
func (r *runner) saveState(ctx context.Context, st syncState) error {
	st.UpdatedAt = time.Now().UTC()
	result, err := r.states.ReplaceOne(ctx, bson.D{{Key: "_id", Value: st.ID}, {Key: "source_id", Value: r.sourceID}}, st)
	if err != nil {
		return err
	}
	if result.MatchedCount != 1 {
		return errors.New("checkpoint row disappeared or source identity changed")
	}
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
			return fmt.Errorf("target collection %s already exists; use --overwrite-target only if replacing its contents is intended", name)
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

func connect(ctx context.Context, uri string, wc *writeconcern.WriteConcern) (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(uri).SetRetryWrites(true).SetRetryReads(true)
	if wc != nil {
		clientOptions.SetWriteConcern(wc)
	}
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
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
}

func hello(ctx context.Context, client *mongo.Client) (helloReply, error) {
	var h helloReply
	err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&h)
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
