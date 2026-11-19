// Package mongocdc implements MongoDB-to-MongoDB bootstrap and change-stream replication.
// Documents stay BSON; the relational transaction model is deliberately not involved.
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
	"sync"
	"time"

	"github.com/google/uuid"
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
	lockCollection     = "mongo_sync_locks"
	leaseDuration      = 45 * time.Second
	leaseRefresh       = 15 * time.Second
	snapshotBatch      = 500
	snapshotBatchBytes = 8 << 20
	checkpointBatch    = 100
	checkpointInterval = time.Second
)

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
	UpdatedAt       time.Time      `bson:"updated_at"`
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
	FullDocument bson.RawValue `bson:"fullDocument"`
	DocumentKey  bson.Raw      `bson:"documentKey"`
}

// Run snapshots selected collections, then applies and checkpoints change events.
// Snapshot is restartable: a crash before InitialDone causes the selected target
// collections to be rebuilt, then changes since StartAt are replayed.
func Run(ctx context.Context, cfg Config) error {
	if ctx == nil {
		return errors.New("mongodb cdc: context is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
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

	sourceID, operationTime, err := sourceIdentityAndTime(ctx, source, cfg.SourceURI)
	if err != nil {
		return err
	}
	targetID, err := deploymentIdentity(ctx, target, cfg.TargetURI)
	if err != nil {
		return fmt.Errorf("inspect MongoDB target topology: %w", err)
	}
	sourceTopology, err := topologyIdentity(ctx, source, cfg.SourceURI)
	if err != nil {
		return fmt.Errorf("inspect MongoDB source topology: %w", err)
	}
	targetTopology, err := topologyIdentity(ctx, target, cfg.TargetURI)
	if err != nil {
		return fmt.Errorf("inspect MongoDB target topology: %w", err)
	}
	sourceEndpoint, err := canonicalEndpoint(cfg.SourceURI)
	if err != nil {
		return fmt.Errorf("canonicalize MongoDB source URI: %w", err)
	}
	targetEndpoint, err := canonicalEndpoint(cfg.TargetURI)
	if err != nil {
		return fmt.Errorf("canonicalize MongoDB target URI: %w", err)
	}
	if cfg.SourceDB == cfg.TargetDB && (sourceTopology == targetTopology || sourceEndpoint == targetEndpoint) {
		return errors.New("mongodb cdc: source and target resolve to the same MongoDB deployment and database; refusing a self-replicating pipeline")
	}
	control := target.Database(stateDatabase)
	states := control.Collection(stateCollection)
	leaseKey := cfg.TargetDB
	owner, err := acquireLease(ctx, control.Collection(lockCollection), leaseKey, cfg.Pipeline)
	if err != nil {
		return err
	}
	defer releaseLease(control.Collection(lockCollection), leaseKey, owner)

	runCtx, cancel := context.WithCancel(ctx)
	var leaseWG sync.WaitGroup
	leaseWG.Add(1)
	go func() {
		defer leaseWG.Done()
		refreshLease(runCtx, control.Collection(lockCollection), leaseKey, owner, cfg.Pipeline, cfg.Logger, cancel)
	}()
	defer func() {
		cancel()
		leaseWG.Wait()
	}()

	state, err := loadOrCreateState(runCtx, states, cfg, sourceID, targetID, operationTime)
	if err != nil {
		return err
	}
	sourceDB := source.Database(cfg.SourceDB, options.Database().SetReadConcern(readconcern.Majority()))
	targetDB := target.Database(cfg.TargetDB)
	if !state.InitialDone {
		restartingSnapshot := state.SnapshotStarted
		if !restartingSnapshot && !cfg.OverwriteTarget {
			if err := ensureTargetCollectionsAbsent(runCtx, targetDB, cfg); err != nil {
				return err
			}
		}
		if !restartingSnapshot {
			state.SnapshotStarted = true
			state.UpdatedAt = time.Now().UTC()
			if err := persistState(runCtx, states, sourceID, state); err != nil {
				return fmt.Errorf("save MongoDB snapshot-start checkpoint: %w", err)
			}
		}
		cfg.Logger.Info("MongoDB initial snapshot started", "pipeline", cfg.Pipeline, "source_database", cfg.SourceDB, "target_database", cfg.TargetDB, "start_at", state.StartAt)
		snapshotCfg := cfg
		snapshotCfg.OverwriteTarget = cfg.OverwriteTarget || restartingSnapshot
		if err := snapshotDatabase(runCtx, sourceDB, targetDB, snapshotCfg); err != nil {
			return fmt.Errorf("MongoDB initial snapshot: %w", err)
		}
		state.InitialDone = true
		state.UpdatedAt = time.Now().UTC()
		if err := persistState(runCtx, states, sourceID, state); err != nil {
			return fmt.Errorf("save MongoDB snapshot checkpoint: %w", err)
		}
		cfg.Logger.Info("MongoDB initial snapshot complete", "pipeline", cfg.Pipeline)
	}
	// Open after bootstrap. The persisted startAt timestamp covers changes made during
	// the scan; opening a cursor before a long snapshot can leave it idle until timeout.
	stream, err := openStream(runCtx, sourceDB, state)
	if err != nil {
		return fmt.Errorf("open MongoDB change stream (oplog history may have expired; reseed is required): %w", err)
	}
	defer func() { _ = stream.Close(context.Background()) }()

	checkpointedAt := time.Now()
	eventsSinceCheckpoint := 0
	for runCtx.Err() == nil {
		if !stream.Next(runCtx) {
			if runCtx.Err() != nil {
				break
			}
			if err := stream.Err(); err != nil {
				return fmt.Errorf("MongoDB change stream stopped (resume from last persisted token): %w", err)
			}
			continue
		}
		var event streamEvent
		if err := stream.Decode(&event); err != nil {
			return fmt.Errorf("decode MongoDB change event: %w", err)
		}
		if err := applyEvent(runCtx, sourceDB, targetDB, cfg, event); err != nil {
			return fmt.Errorf("apply MongoDB event %s on %s.%s: %w", event.OperationType, event.NS.DB, event.NS.Coll, err)
		}
		token := append(bson.Raw(nil), stream.ResumeToken()...)
		if len(token) == 0 {
			return errors.New("MongoDB change stream returned an empty resume token")
		}
		state.ResumeToken = token
		state.StartAfter = event.OperationType == "invalidate"
		state.UpdatedAt = time.Now().UTC()
		if state.StartAfter {
			if err := persistState(runCtx, states, sourceID, state); err != nil {
				return fmt.Errorf("persist MongoDB invalidate resume token: %w", err)
			}
			_ = stream.Close(runCtx)
			stream, err = openStream(runCtx, sourceDB, state)
			if err != nil {
				return fmt.Errorf("reopen MongoDB change stream after invalidate: %w", err)
			}
			eventsSinceCheckpoint = 0
			checkpointedAt = time.Now()
			continue
		}
		eventsSinceCheckpoint++
		if eventsSinceCheckpoint >= checkpointBatch || time.Since(checkpointedAt) >= checkpointInterval {
			if err := persistState(runCtx, states, sourceID, state); err != nil {
				return fmt.Errorf("persist MongoDB resume token after target apply: %w", err)
			}
			eventsSinceCheckpoint = 0
			checkpointedAt = time.Now()
		}
	}
	return runCtx.Err()
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

func persistState(ctx context.Context, coll *mongo.Collection, sourceID string, state syncState) error {
	result, err := coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: state.ID}, {Key: "source_id", Value: sourceID}}, state)
	if err != nil {
		return err
	}
	if result.MatchedCount != 1 {
		return errors.New("checkpoint row disappeared or source identity changed")
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
	if len(cfg.Include) == 0 {
		return errors.New("mongodb cdc: tables.include must contain database.collection patterns")
	}
	for _, pattern := range append(append([]string(nil), cfg.Include...), cfg.Exclude...) {
		if strings.TrimSpace(pattern) == "" {
			return errors.New("mongodb cdc: collection patterns must not be empty")
		}
		if _, err := path.Match(pattern, "validation.collection"); err != nil {
			return fmt.Errorf("mongodb cdc: invalid collection pattern %q: %w", pattern, err)
		}
	}
	for _, pattern := range append(append([]string(nil), cfg.Include...), cfg.Exclude...) {
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
	clientOptions := options.Client().ApplyURI(uri)
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

func sourceIdentityAndTime(ctx context.Context, client *mongo.Client, uri string) (string, bson.Timestamp, error) {
	var hello struct {
		SetName       string         `bson:"setName"`
		Hosts         []string       `bson:"hosts"`
		Msg           string         `bson:"msg"`
		OperationTime bson.Timestamp `bson:"operationTime"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return "", bson.Timestamp{}, fmt.Errorf("MongoDB source hello: %w", err)
	}
	if hello.OperationTime.T == 0 {
		return "", bson.Timestamp{}, errors.New("MongoDB source did not return operationTime; use a replica set or sharded cluster with change streams")
	}
	endpoint, err := canonicalEndpoint(uri)
	if err != nil {
		return "", bson.Timestamp{}, fmt.Errorf("canonicalize MongoDB source URI: %w", err)
	}
	material := strings.Join([]string{hello.SetName, hello.Msg, endpoint}, "|")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:]), hello.OperationTime, nil
}

func deploymentIdentity(ctx context.Context, client *mongo.Client, uri string) (string, error) {
	var hello struct {
		SetName string   `bson:"setName"`
		Hosts   []string `bson:"hosts"`
		Msg     string   `bson:"msg"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return "", err
	}
	endpoint, err := canonicalEndpoint(uri)
	if err != nil {
		return "", err
	}
	identity := hello.SetName + "|" + hello.Msg + "|" + endpoint
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:]), nil
}

func topologyIdentity(ctx context.Context, client *mongo.Client, uri string) (string, error) {
	var hello struct {
		SetName string   `bson:"setName"`
		Hosts   []string `bson:"hosts"`
		Msg     string   `bson:"msg"`
	}
	if err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		return "", err
	}
	identity := ""
	if hello.SetName != "" {
		hosts := append([]string(nil), hello.Hosts...)
		sort.Strings(hosts)
		identity = hello.SetName + "|" + strings.Join(hosts, ",")
	} else {
		endpoint, err := canonicalEndpoint(uri)
		if err != nil {
			return "", err
		}
		identity = hello.Msg + "|" + endpoint
	}
	sum := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(sum[:]), nil
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

func loadOrCreateState(ctx context.Context, coll *mongo.Collection, cfg Config, sourceID, targetID string, startAt bson.Timestamp) (syncState, error) {
	scopeID := replicationScopeID(cfg)
	var state syncState
	err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: cfg.Pipeline}}).Decode(&state)
	if err == nil {
		if state.SourceID != sourceID || state.TargetID != targetID || state.ScopeID != scopeID || state.SourceDB != cfg.SourceDB || state.TargetDB != cfg.TargetDB {
			return syncState{}, errors.New("MongoDB checkpoint belongs to a different source, target, database, or collection filter; use a new pipeline name")
		}
		return state, nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return syncState{}, fmt.Errorf("load MongoDB checkpoint: %w", err)
	}
	state = syncState{ID: cfg.Pipeline, SourceID: sourceID, TargetID: targetID, ScopeID: scopeID, SourceDB: cfg.SourceDB, TargetDB: cfg.TargetDB, StartAt: startAt, UpdatedAt: time.Now().UTC()}
	if _, err := coll.InsertOne(ctx, state); err != nil {
		return syncState{}, fmt.Errorf("create MongoDB checkpoint: %w", err)
	}
	return state, nil
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

func openStream(ctx context.Context, db *mongo.Database, state syncState) (*mongo.ChangeStream, error) {
	// Do not request showExpandedEvents: it requires MongoDB 6.0+, while the
	// selected Go driver supports MongoDB 4.4+. DDL/index replication is out of scope.
	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if len(state.ResumeToken) > 0 {
		if state.StartAfter {
			opts.SetStartAfter(state.ResumeToken)
		} else {
			opts.SetResumeAfter(state.ResumeToken)
		}
	} else {
		opts.SetStartAtOperationTime(&state.StartAt)
	}
	return db.Watch(ctx, mongo.Pipeline{}, opts)
}

func snapshotDatabase(ctx context.Context, source, target *mongo.Database, cfg Config) error {
	cursor, err := source.ListCollections(ctx, bson.D{})
	if err != nil {
		return fmt.Errorf("list source collections: %w", err)
	}
	defer cursor.Close(ctx)
	type collectionInfo struct {
		Name string `bson:"name"`
		Type string `bson:"type"`
	}
	var collections []collectionInfo
	for cursor.Next(ctx) {
		var info collectionInfo
		if err := cursor.Decode(&info); err != nil {
			return fmt.Errorf("decode source collection metadata: %w", err)
		}
		collections = append(collections, info)
	}
	if err := cursor.Err(); err != nil {
		return fmt.Errorf("read source collection metadata: %w", err)
	}
	sort.Slice(collections, func(i, j int) bool { return collections[i].Name < collections[j].Name })
	sourceNames := make(map[string]bool)
	for _, info := range collections {
		qualified := cfg.SourceDB + "." + info.Name
		if strings.HasPrefix(info.Name, "system.") || !selected(cfg, qualified) {
			continue
		}
		if info.Type != "collection" {
			return fmt.Errorf("%s has MongoDB collection type %q; only ordinary collections can be bootstrapped", qualified, info.Type)
		}
		sourceNames[info.Name] = true
	}
	if cfg.OverwriteTarget {
		targetNames, err := target.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return fmt.Errorf("list target collections before snapshot: %w", err)
		}
		for _, name := range targetNames {
			if strings.HasPrefix(name, "system.") || sourceNames[name] || !selected(cfg, cfg.SourceDB+"."+name) {
				continue
			}
			if err := target.Collection(name).Drop(ctx); err != nil {
				return fmt.Errorf("drop stale target collection %s: %w", name, err)
			}
		}
	}
	for _, info := range collections {
		name := info.Name
		qualified := cfg.SourceDB + "." + name
		if strings.HasPrefix(name, "system.") || !selected(cfg, qualified) {
			continue
		}
		if err := prepareTargetCollection(ctx, target, name, cfg.OverwriteTarget); err != nil {
			return fmt.Errorf("recreate target collection %s: %w", name, err)
		}
		if err := copyCollection(ctx, source.Collection(name), target.Collection(name)); err != nil {
			return fmt.Errorf("copy collection %s: %w", qualified, err)
		}
		cfg.Logger.Info("MongoDB collection snapshot complete", "pipeline", cfg.Pipeline, "collection", qualified)
	}
	return nil
}

func prepareTargetCollection(ctx context.Context, db *mongo.Database, name string, overwrite bool) error {
	names, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: name}})
	if err != nil {
		return err
	}
	for _, existing := range names {
		if existing == name {
			if !overwrite {
				return fmt.Errorf("target collection %s already exists; use --overwrite-target only if replacing its contents is intended", name)
			}
			if err := db.Collection(name).Drop(ctx); err != nil {
				return err
			}
			break
		}
	}
	return db.CreateCollection(ctx, name)
}

func copyCollection(ctx context.Context, source, target *mongo.Collection) error {
	cursor, err := source.Find(ctx, bson.D{}, options.Find().SetBatchSize(snapshotBatch))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	docs := make([]any, 0, snapshotBatch)
	batchBytes := 0
	flush := func() error {
		if len(docs) == 0 {
			return nil
		}
		_, err := target.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
		docs = docs[:0]
		batchBytes = 0
		return err
	}
	for cursor.Next(ctx) {
		doc := append(bson.Raw(nil), cursor.Current...)
		docs = append(docs, doc)
		batchBytes += len(doc)
		if len(docs) >= snapshotBatch || batchBytes >= snapshotBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return err
	}
	return flush()
}

func applyEvent(ctx context.Context, source, target *mongo.Database, cfg Config, event streamEvent) error {
	if event.OperationType == "dropDatabase" {
		if event.NS.DB == cfg.SourceDB {
			return dropSelectedCollections(ctx, target, cfg)
		}
		return nil
	}
	if event.NS.DB != cfg.SourceDB {
		return nil
	}
	if event.OperationType == "rename" {
		return applyRename(ctx, source, target, cfg, event)
	}
	if event.NS.Coll == "" || !selected(cfg, cfg.SourceDB+"."+event.NS.Coll) {
		return nil
	}
	coll := target.Collection(event.NS.Coll)
	switch event.OperationType {
	case "insert", "replace", "update":
		if event.FullDocument.IsZero() || event.FullDocument.Type == bson.TypeNull {
			return deleteDocument(ctx, coll, event.DocumentKey)
		}
		var fullDocument bson.Raw
		if err := event.FullDocument.Unmarshal(&fullDocument); err != nil {
			return fmt.Errorf("decode change event fullDocument: %w", err)
		}
		idValue, err := fullDocument.LookupErr("_id")
		if err != nil {
			return err
		}
		var id any
		if err := idValue.Unmarshal(&id); err != nil {
			return err
		}
		_, err = coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: id}}, fullDocument, options.Replace().SetUpsert(true))
		return err
	case "delete":
		return deleteDocument(ctx, coll, event.DocumentKey)
	case "create":
		names, err := target.ListCollectionNames(ctx, bson.D{{Key: "name", Value: event.NS.Coll}})
		if err != nil {
			return err
		}
		if len(names) == 0 {
			return target.CreateCollection(ctx, event.NS.Coll)
		}
		return nil
	case "drop":
		names, err := target.ListCollectionNames(ctx, bson.D{{Key: "name", Value: event.NS.Coll}})
		if err != nil || len(names) == 0 {
			return err
		}
		return coll.Drop(ctx)
	case "modify", "createIndexes", "dropIndexes", "endOfTransaction", "invalidate":
		return nil
	default:
		return fmt.Errorf("unsupported MongoDB change event type %q; refusing to advance checkpoint", event.OperationType)
	}
}

func applyRename(ctx context.Context, source, target *mongo.Database, cfg Config, event streamEvent) error {
	if event.To.DB != cfg.SourceDB || event.To.Coll == "" {
		if selected(cfg, cfg.SourceDB+"."+event.NS.Coll) {
			return dropCollectionIfPresent(ctx, target, event.NS.Coll)
		}
		return nil
	}
	oldSelected := selected(cfg, cfg.SourceDB+"."+event.NS.Coll)
	newSelected := selected(cfg, cfg.SourceDB+"."+event.To.Coll)
	if !oldSelected && !newSelected {
		return nil
	}
	if !newSelected {
		return dropCollectionIfPresent(ctx, target, event.NS.Coll)
	}
	if oldSelected {
		if err := dropCollectionIfPresent(ctx, target, event.NS.Coll); err != nil {
			return err
		}
	}
	// Materialize from the source instead of renaming the snapshot copy: the rename
	// may have happened while that copy was being read, leaving the old target partial.
	if err := prepareTargetCollection(ctx, target, event.To.Coll, true); err != nil {
		return err
	}
	return copyCollection(ctx, source.Collection(event.To.Coll), target.Collection(event.To.Coll))
}

func dropCollectionIfPresent(ctx context.Context, db *mongo.Database, name string) error {
	names, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: name}})
	if err != nil || len(names) == 0 {
		return err
	}
	return db.Collection(name).Drop(ctx)
}

func dropSelectedCollections(ctx context.Context, target *mongo.Database, cfg Config) error {
	names, err := target.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return err
	}
	for _, name := range names {
		if selected(cfg, cfg.SourceDB+"."+name) {
			if err := target.Collection(name).Drop(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteDocument(ctx context.Context, coll *mongo.Collection, raw bson.Raw) error {
	idValue, err := raw.LookupErr("_id")
	if err != nil {
		return err
	}
	var id any
	if err := idValue.Unmarshal(&id); err != nil {
		return err
	}
	_, err = coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
	return err
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

func acquireLease(ctx context.Context, coll *mongo.Collection, id, pipeline string) (string, error) {
	owner := uuid.NewString()
	expired := bson.D{{Key: "$lte", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$expires_at", time.Unix(0, 0).UTC()}}}, "$$NOW"}}}
	filter := bson.D{{Key: "_id", Value: id}, {Key: "$expr", Value: expired}}
	update := mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: owner}, {Key: "pipeline", Value: pipeline}, {Key: "expires_at", Value: bson.D{{Key: "$add", Value: bson.A{"$$NOW", leaseDuration.Milliseconds()}}}}}}}}
	_, err := coll.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
	if mongo.IsDuplicateKeyError(err) {
		return "", errors.New("another MongoDB CDC process holds the lease for this target database")
	}
	if err != nil {
		return "", fmt.Errorf("acquire MongoDB CDC lease: %w", err)
	}
	return owner, nil
}

func refreshLease(ctx context.Context, coll *mongo.Collection, id, owner, pipeline string, log *slog.Logger, cancel context.CancelFunc) {
	ticker := time.NewTicker(leaseRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 5*time.Second)
			update := mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{{Key: "expires_at", Value: bson.D{{Key: "$add", Value: bson.A{"$$NOW", leaseDuration.Milliseconds()}}}}}}}}
			res, err := coll.UpdateOne(timeoutCtx, bson.D{{Key: "_id", Value: id}, {Key: "owner", Value: owner}}, update)
			timeoutCancel()
			if err != nil || res.MatchedCount == 0 {
				log.Error("MongoDB CDC lease lost; stopping the stream", "pipeline", pipeline, "target_database", id, "error", err)
				cancel()
				return
			}
		}
	}
}

func releaseLease(coll *mongo.Collection, id, owner string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}, {Key: "owner", Value: owner}})
}
