package mongocdc

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Check is one preflight result.
type Check struct {
	Name   string
	Status string // ok | warn | fail
	Detail string
}

// Check statuses.
const (
	CheckOK   = "ok"
	CheckWarn = "warn"
	CheckFail = "fail"
)

// Plan is the snapshot estimate of `strand plan` / `strand validate`.
type Plan struct {
	Collections     []PlanCollection
	Documents       int64
	Bytes           int64
	EstimatedCopy   time.Duration
	OplogWindow     time.Duration // 0 = unknown
	CheckpointFound bool
}

// PlanCollection is one selected source collection.
type PlanCollection struct {
	Name      string
	Documents int64
	Bytes     int64
}

// AssumedSnapshotDocsPerSec is the conservative per-worker copy rate used for the ETA
// (measured ~11-12k documents/s of ~1 KiB per worker on local 3-node replica sets).
const AssumedSnapshotDocsPerSec = 5000

// Preflight connects to both sides and checks topology, version, privileges, the
// selected collections, the oplog window against the estimated snapshot time and the
// target state. It never writes.
func Preflight(ctx context.Context, cfg Config) (checks []Check, plan Plan, err error) {
	cfg.setDefaults()
	defer func() { err = redactError(err, cfg.SourceURI, cfg.TargetURI) }()
	add := func(name, status, format string, a ...any) {
		checks = append(checks, Check{name, status, redactString(fmt.Sprintf(format, a...), cfg.SourceURI, cfg.TargetURI)})
	}
	if err := validateConfig(cfg); err != nil {
		add("config", CheckFail, "%v", err)
		return checks, plan, nil
	}
	clients := map[string]*mongo.Client{}
	hellos := map[string]helloReply{}
	for _, side := range []struct{ role, uri string }{{"source", cfg.SourceURI}, {"target", cfg.TargetURI}} {
		c, err := connect(ctx, side.uri, nil)
		if err != nil {
			add(side.role+" connection", CheckFail, "%v", err)
			continue
		}
		defer disconnect(c)
		h, err := hello(ctx, c)
		if err != nil {
			add(side.role+" connection", CheckFail, "hello: %v", err)
			continue
		}
		var bi struct {
			Version string `bson:"version"`
		}
		_ = c.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&bi)
		if err := checkTopology(side.role, h); err != nil {
			add(side.role+" topology", CheckFail, "%v", err)
			continue
		}
		add(side.role+" topology", CheckOK, "replica set %q, %d member(s), MongoDB %s", h.SetName, len(h.Hosts), bi.Version)
		clients[side.role], hellos[side.role] = c, h
	}
	src, dst := clients["source"], clients["target"]
	if src == nil || dst == nil {
		return checks, plan, nil
	}
	se, _ := canonicalEndpoint(cfg.SourceURI)
	te, _ := canonicalEndpoint(cfg.TargetURI)
	if cfg.SourceDB == cfg.TargetDB && sameDeployment(hellos["source"], hellos["target"], se, te) {
		add("self replication", CheckFail, "source and target are the same deployment and database")
	}
	mode := resolveUpdateMode(cfg.UpdateMode, hellos["source"].MaxWireVersion)
	add("update mode", CheckOK, "%s (source wire version %d)", mode, hellos["source"].MaxWireVersion)

	// Privileges.
	srcNeed := []need{{cfg.SourceDB, "", []string{"find", "changeStream", "listCollections"}}}
	if cfg.ReplicateIndexes {
		srcNeed = append(srcNeed, need{cfg.SourceDB, "", []string{"listIndexes"}})
	}
	dstNeed := []need{
		{cfg.TargetDB, "", []string{"find", "insert", "update", "remove", "createCollection", "dropCollection", "renameCollectionSameDB", "listCollections", "createIndex"}},
		{stateDatabase, "", []string{"find", "insert", "update", "remove", "createCollection"}},
	}
	if cfg.ReplicateIndexes {
		dstNeed = append(dstNeed, need{cfg.TargetDB, "", []string{"listIndexes", "dropIndex", "collMod", "bypassDocumentValidation"}})
	}
	for _, side := range []struct {
		role  string
		c     *mongo.Client
		needs []need
	}{{"source", src, srcNeed}, {"target", dst, dstNeed}} {
		missing, authOn, err := missingPrivileges(ctx, side.c, side.needs)
		switch {
		case err != nil:
			add(side.role+" privileges", CheckWarn, "could not read privileges: %v", err)
		case !authOn:
			add(side.role+" privileges", CheckWarn, "authentication is disabled on the %s (enable SCRAM-SHA-256 or x.509 in production)", side.role)
		case len(missing) > 0:
			add(side.role+" privileges", CheckFail, "missing actions: %s (see docs/mongodb-cdc.md, least-privilege roles)", strings.Join(missing, ", "))
		default:
			add(side.role+" privileges", CheckOK, "all required actions granted")
		}
	}

	// Collections and sizes.
	sdb := src.Database(cfg.SourceDB)
	infos, err := listSelectedCollections(ctx, sdb, cfg)
	if err != nil {
		add("collections", CheckFail, "%v", err)
		return checks, plan, nil
	}
	if len(infos) == 0 {
		add("collections", CheckWarn, "no source collection matches the include/exclude patterns yet")
	}
	for _, in := range infos {
		var st struct {
			Count int64 `bson:"count"`
			Size  int64 `bson:"size"`
		}
		if err := sdb.RunCommand(ctx, bson.D{{Key: "collStats", Value: in.Name}}).Decode(&st); err != nil {
			st.Count, _ = sdb.Collection(in.Name).EstimatedDocumentCount(ctx)
		}
		plan.Collections = append(plan.Collections, PlanCollection{in.Name, st.Count, st.Size})
		plan.Documents += st.Count
		plan.Bytes += st.Size
	}
	sort.Slice(plan.Collections, func(i, j int) bool { return plan.Collections[i].Documents > plan.Collections[j].Documents })
	workers := int64(min(cfg.SnapshotParallel, max(len(infos), 1)))
	plan.EstimatedCopy = time.Duration(float64(plan.Documents)/float64(AssumedSnapshotDocsPerSec*workers)*float64(time.Second)) + time.Second
	if len(plan.Collections) > 0 {
		// A single huge collection is copied by one worker.
		if big := time.Duration(float64(plan.Collections[0].Documents) / AssumedSnapshotDocsPerSec * float64(time.Second)); big > plan.EstimatedCopy {
			plan.EstimatedCopy = big
		}
	}
	add("collections", CheckOK, "%d selected, ~%d documents, ~%d MiB; estimated snapshot %s at %d docs/s per worker, %d worker(s)",
		len(infos), plan.Documents, plan.Bytes>>20, plan.EstimatedCopy.Round(time.Second), AssumedSnapshotDocsPerSec, workers)

	if w, err := oplogWindow(ctx, src); err != nil {
		add("oplog window", CheckWarn, "cannot read local.oplog.rs (%v); grant read on local or the clusterMonitor role to check the window", err)
	} else {
		plan.OplogWindow = w
		var ls struct {
			LogSizeMB float64 `bson:"logSizeMB"`
		}
		_ = src.Database("local").RunCommand(ctx, bson.D{{Key: "collStats", Value: "oplog.rs"}, {Key: "scale", Value: 1 << 20}}).Decode(&ls)
		switch {
		case plan.EstimatedCopy > w/2:
			add("oplog window", CheckFail, "estimated snapshot %s exceeds 50%% of the source oplog window %s: the change history could roll over before the stream starts; enlarge the oplog (replSetResizeOplog / minRetentionHours) or raise bootstrap.logical.max_parallel",
				plan.EstimatedCopy.Round(time.Second), w.Round(time.Second))
		default:
			add("oplog window", CheckOK, "%s of history (oplog %.0f MiB); estimated snapshot %s", w.Round(time.Second), ls.LogSizeMB, plan.EstimatedCopy.Round(time.Second))
		}
	}

	// Target state.
	var state syncState
	if err := dst.Database(stateDatabase).Collection(stateCollection).FindOne(ctx, bson.D{{Key: "_id", Value: cfg.Pipeline}}).Decode(&state); err == nil {
		plan.CheckpointFound = true
		if state.ScopeID != replicationScopeID(cfg) || state.SourceDB != cfg.SourceDB || state.TargetDB != cfg.TargetDB {
			add("target checkpoint", CheckFail, "a checkpoint for pipeline %q exists with a different source/target database or collection filter; use a new name", cfg.Pipeline)
		} else {
			add("target checkpoint", CheckOK, "checkpoint found (initial snapshot done: %v); sync resumes from it", state.InitialDone)
		}
	} else {
		names, err := dst.Database(cfg.TargetDB).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			add("target collections", CheckFail, "%v", err)
		} else {
			var clash []string
			for _, n := range names {
				if selected(cfg, cfg.SourceDB+"."+n) {
					clash = append(clash, n)
				}
			}
			switch {
			case len(clash) == 0:
				add("target collections", CheckOK, "no selected collection exists on the target")
			case cfg.OverwriteTarget:
				add("target collections", CheckWarn, "will DROP and recreate existing target collections: %s (mongodb.overwrite_target)", strings.Join(clash, ", "))
			default:
				add("target collections", CheckFail, "target collections already exist: %s; set mongodb.overwrite_target: true to replace them", strings.Join(clash, ", "))
			}
		}
	}
	return checks, plan, nil
}

type need struct {
	db, coll string
	actions  []string
}

// missingPrivileges lists required actions not granted to the connected user.
func missingPrivileges(ctx context.Context, c *mongo.Client, needs []need) ([]string, bool, error) {
	var st struct {
		AuthInfo struct {
			Users []bson.Raw `bson:"authenticatedUsers"`
			Privs []struct {
				Resource struct {
					DB          *string `bson:"db"`
					Collection  *string `bson:"collection"`
					AnyResource bool    `bson:"anyResource"`
				} `bson:"resource"`
				Actions []string `bson:"actions"`
			} `bson:"authenticatedUserPrivileges"`
		} `bson:"authInfo"`
	}
	if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "connectionStatus", Value: 1}, {Key: "showPrivileges", Value: true}}).Decode(&st); err != nil {
		return nil, false, err
	}
	if len(st.AuthInfo.Users) == 0 {
		return nil, false, nil
	}
	var missing []string
	for _, n := range needs {
		for _, a := range n.actions {
			ok := false
			for _, p := range st.AuthInfo.Privs {
				r := p.Resource
				dbOK := r.AnyResource || (r.DB != nil && (*r.DB == "" || *r.DB == n.db))
				collOK := r.AnyResource || (r.Collection != nil && (*r.Collection == "" || *r.Collection == n.coll))
				if !dbOK || !collOK {
					continue
				}
				for _, pa := range p.Actions {
					if pa == a || pa == "anyAction" {
						ok = true
					}
				}
			}
			if !ok {
				missing = append(missing, n.db+":"+a)
			}
		}
	}
	return missing, true, nil
}
