package mongocdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// lease is the single-writer guard for one target database.
//
// Liveness: the lease document (_id = target database) carries an expiry evaluated only
// with the target server clock ($$NOW), refreshed every TTL/3. Another process may take
// it over after expiry, or immediately when the previous owner ran on the same host (same
// hostname, kernel boot id and PID namespace) and its process is provably gone (the PID
// no longer exists or now belongs to a process with a different start time).
//
// Safety does not depend on expiry: every target write commits in a transaction that
// also updates one of the owner's fence documents (_id = "<database>#<slot>") under the
// filter owner == ours. A new owner first rewrites every fence document of the database
// (in a transaction fenced by the lease document), so a stale owner's transaction either
// committed before that point or aborts (WriteConflict / no match). Parallel writers of
// one owner use different slots and do not conflict with each other.
type lease struct {
	client   *mongo.Client
	coll     *mongo.Collection
	key      string
	owner    string
	pipeline string
	ttl      time.Duration
	slots    int
	ddlMu    sync.Mutex
}

// ownerInfo identifies the process holding a lease.
type ownerInfo struct {
	Host     string `bson:"host,omitempty"`
	BootID   string `bson:"boot_id,omitempty"`
	PidNS    string `bson:"pid_ns,omitempty"`
	PID      int    `bson:"pid,omitempty"`
	PIDStart string `bson:"pid_start,omitempty"`
}

func selfOwnerInfo() ownerInfo {
	host, _ := os.Hostname()
	pid := os.Getpid()
	return ownerInfo{Host: host, BootID: bootID(), PidNS: pidNamespace(), PID: pid, PIDStart: processStart(pid)}
}

func bootID() string {
	b, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func pidNamespace() string {
	ns, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return ""
	}
	return ns
}

// processStart returns the start time (clock ticks since boot) of pid, "" if unknown or gone.
func processStart(pid int) string {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return ""
	}
	// Field 2 (comm) may contain spaces and parentheses: fields after the last ')'.
	s := string(b)
	i := strings.LastIndexByte(s, ')')
	if i < 0 {
		return ""
	}
	fields := strings.Fields(s[i+1:])
	// fields[0] is field 3 (state); starttime is field 22.
	if len(fields) < 20 {
		return ""
	}
	return fields[19]
}

// provablyGone reports that the lease holder ran on this machine, in this PID namespace
// and this boot, and that its process no longer exists. Any missing identity field makes
// the answer "not provable" (false): the caller then waits for the lease to expire.
func provablyGone(holder, self ownerInfo) bool {
	if holder.Host == "" || holder.BootID == "" || holder.PidNS == "" || holder.PID <= 0 || holder.PIDStart == "" {
		return false
	}
	if holder.Host != self.Host || holder.BootID != self.BootID || holder.PidNS != self.PidNS {
		return false
	}
	if holder.PID == self.PID && holder.PIDStart == self.PIDStart {
		return false // ourselves (should not happen: owner ids differ)
	}
	return processStart(holder.PID) != holder.PIDStart
}

func expiresIn(ttl time.Duration) bson.D {
	return bson.D{{Key: "$add", Value: bson.A{"$$NOW", ttl.Milliseconds()}}}
}

type leaseDoc struct {
	Owner     string    `bson:"owner"`
	Pipeline  string    `bson:"pipeline"`
	ExpiresAt time.Time `bson:"expires_at"`
	Info      ownerInfo `bson:"info"`
	DDL       string    `bson:"ddl_in_progress"`
}

// ErrLeaseHeld reports that another live process owns the lease.
var ErrLeaseHeld = errors.New("mongodb cdc: lease held by another process")

// ErrDDLUncertain means the previous target DDL may still run. A new owner
// cannot safely start until the operation has been checked on the target.
var ErrDDLUncertain = errors.New("mongodb cdc: target DDL outcome requires operator review")

func acquireLease(ctx context.Context, client *mongo.Client, coll *mongo.Collection, key, pipeline string, ttl time.Duration, slots int, log *slog.Logger) (*lease, error) {
	owner := uuid.NewString()
	self := selfOwnerInfo()
	// One atomic upsert keyed only by _id ($expr is not allowed in an upsert predicate):
	// the pipeline takes ownership only when the current lease is absent or expired
	// on the target clock, and the returned post-image tells us who owns it.
	expired := bson.D{{Key: "$lte", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$expires_at", time.Unix(0, 0).UTC()}}}, "$$NOW"}}}
	available := bson.D{{Key: "$and", Value: bson.A{
		expired,
		bson.D{{Key: "$eq", Value: bson.A{bson.D{{Key: "$type", Value: "$ddl_in_progress"}}, "missing"}}},
	}}}
	take := func(field string, v any) bson.E {
		return bson.E{Key: field, Value: bson.D{{Key: "$cond", Value: bson.A{available, v, "$" + field}}}}
	}
	update := mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{
		take("owner", bson.D{{Key: "$literal", Value: owner}}), take("pipeline", bson.D{{Key: "$literal", Value: pipeline}}),
		take("info", bson.D{{Key: "$literal", Value: self}}),
		take("acquired_at", "$$NOW"), take("expires_at", expiresIn(ttl)),
	}}}}
	var got leaseDoc
	err := coll.FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: key}}, update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&got)
	if mongo.IsDuplicateKeyError(err) { // lost a concurrent upsert race
		err = coll.FindOne(ctx, bson.D{{Key: "_id", Value: key}}).Decode(&got)
	}
	if err != nil {
		return nil, fmt.Errorf("acquire MongoDB CDC lease: %w", err)
	}
	if got.Owner != owner {
		if got.DDL != "" {
			return nil, fmt.Errorf("%w: %s on database %q (lease owner %s)", ErrDDLUncertain, got.DDL, key, got.Owner)
		}
		if !provablyGone(got.Info, self) {
			return nil, fmt.Errorf("%w: another MongoDB CDC process (pipeline %q, host %q, pid %d) holds the lease for target database %q until %s (target clock); stop it or wait for expiry",
				ErrLeaseHeld, got.Pipeline, got.Info.Host, got.Info.PID, key, got.ExpiresAt.UTC().Format(time.RFC3339))
		}
		// The previous owner crashed on this host: take over now instead of waiting for
		// the TTL. Conditional on the exact previous owner, so two restarting processes
		// cannot both win; the fence rewrite below cuts off anything still in flight.
		res, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: key}, {Key: "owner", Value: got.Owner}, {Key: "ddl_in_progress", Value: bson.D{{Key: "$exists", Value: false}}}}, mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{
			{Key: "owner", Value: bson.D{{Key: "$literal", Value: owner}}}, {Key: "pipeline", Value: bson.D{{Key: "$literal", Value: pipeline}}},
			{Key: "info", Value: bson.D{{Key: "$literal", Value: self}}}, {Key: "acquired_at", Value: "$$NOW"}, {Key: "expires_at", Value: expiresIn(ttl)},
		}}}})
		if err != nil {
			return nil, fmt.Errorf("take over MongoDB CDC lease: %w", err)
		}
		if res.ModifiedCount != 1 {
			return nil, fmt.Errorf("%w: lease for target database %q changed hands during takeover", ErrLeaseHeld, key)
		}
		log.Warn("MongoDB CDC lease taken over from a dead process on this host", "pipeline", pipeline, "target_database", key, "previous_pid", got.Info.PID)
	}
	l := &lease{client: client, coll: coll, key: key, owner: owner, pipeline: pipeline, ttl: ttl, slots: max(slots, 1)}
	if err := l.claimFences(ctx); err != nil {
		l.release()
		return nil, err
	}
	return l, nil
}

func (l *lease) fenceID(slot int) string { return l.key + "#" + strconv.Itoa(slot) }

// claimFences makes this owner the owner of every fence document of the database (and
// creates its slots) in one transaction that is itself fenced by the lease document.
func (l *lease) claimFences(ctx context.Context) error {
	sess, err := l.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(context.Background())
	_, err = sess.WithTransaction(ctx, func(tx context.Context) (any, error) {
		res, err := l.coll.UpdateOne(tx, bson.D{{Key: "_id", Value: l.key}, {Key: "owner", Value: l.owner}},
			mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{{Key: "expires_at", Value: expiresIn(l.ttl)}}}}})
		if err != nil {
			return nil, err
		}
		if res.MatchedCount == 0 {
			return nil, ErrLeaseLost
		}
		prefix := bson.Regex{Pattern: "^" + regexp.QuoteMeta(l.key+"#")}
		if _, err := l.coll.UpdateMany(tx, bson.D{{Key: "_id", Value: prefix}}, bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: l.owner}}}}); err != nil {
			return nil, err
		}
		for s := 0; s < l.slots; s++ {
			if _, err := l.coll.UpdateOne(tx, bson.D{{Key: "_id", Value: l.fenceID(s)}}, bson.D{{Key: "$set", Value: bson.D{{Key: "owner", Value: l.owner}}}},
				options.UpdateOne().SetUpsert(true)); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
	if errors.Is(err, ErrLeaseLost) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("claim MongoDB CDC fence documents: %w", err)
	}
	return nil
}

// fence makes the surrounding transaction's commit conditional on this process still
// owning fence slot `slot`.
func (l *lease) fence(ctx context.Context, slot int) error {
	res, err := l.coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: l.fenceID(slot)}, {Key: "owner", Value: l.owner}},
		bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}})
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return ErrLeaseLost
	}
	return nil
}

// refresh extends the lease if and only if this process still owns it.
func (l *lease) refresh(ctx context.Context) error {
	update := mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{{Key: "expires_at", Value: expiresIn(l.ttl)}}}}}
	res, err := l.coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: l.key}, {Key: "owner", Value: l.owner}}, update)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		return ErrLeaseLost
	}
	return nil
}

// txn runs fn in a target transaction fenced by slot. Transient transaction errors are
// retried by the driver; fn must be idempotent.
func (l *lease) txn(ctx context.Context, slot int, fn func(tx context.Context) error) error {
	sess, err := l.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(context.Background())
	_, err = sess.WithTransaction(ctx, func(tx context.Context) (any, error) {
		if err := l.fence(tx, slot); err != nil {
			return nil, err
		}
		return nil, fn(tx)
	})
	if errors.Is(err, ErrLeaseLost) {
		return ErrLeaseLost
	}
	return err
}

func durableDDLWriteConcern() bson.D {
	wc := durableTargetWriteConcern()
	return bson.D{{Key: "w", Value: wc.W}, {Key: "j", Value: *wc.Journal}}
}

// withDDL keeps a durable guard on the lease document while MongoDB runs DDL
// outside a transaction. Expiry and process death never override this guard.
func (l *lease) withDDL(ctx context.Context, description string, fn func() error) error {
	l.ddlMu.Lock()
	defer l.ddlMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	filter := bson.D{{Key: "_id", Value: l.key}, {Key: "owner", Value: l.owner}}
	unlocked := append(append(bson.D(nil), filter...),
		bson.E{Key: "ddl_in_progress", Value: bson.D{{Key: "$exists", Value: false}}},
		bson.E{Key: "$expr", Value: bson.D{{Key: "$gt", Value: bson.A{"$expires_at", "$$NOW"}}}})
	res, err := l.coll.UpdateOne(ctx, unlocked, bson.D{{Key: "$set", Value: bson.D{{Key: "ddl_in_progress", Value: description}}}})
	if err != nil {
		return fmt.Errorf("%w: acquire guard for %s: %v", ErrDDLUncertain, description, err)
	}
	if res.MatchedCount != 1 {
		var current leaseDoc
		err := l.coll.FindOne(ctx, filter).Decode(&current)
		if errors.Is(err, mongo.ErrNoDocuments) || err == nil && current.DDL == "" {
			return ErrLeaseLost
		}
		return fmt.Errorf("%w: could not acquire guard for %s", ErrDDLUncertain, description)
	}
	if err := fn(); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrDDLUncertain, description, err)
	}
	// A successful command has a known outcome even if the pipeline is stopping.
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	res, err = l.coll.UpdateOne(cleanup, filter, bson.D{{Key: "$unset", Value: bson.D{{Key: "ddl_in_progress", Value: ""}}}})
	if err != nil || res.MatchedCount != 1 {
		return fmt.Errorf("%w: clear guard for %s: %v", ErrDDLUncertain, description, err)
	}
	return nil
}

func (l *lease) refreshLoop(ctx context.Context, log *slog.Logger, cancel context.CancelCauseFunc, onLost func()) {
	interval := l.ttl / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var failingSince time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			timeoutCtx, timeoutCancel := context.WithTimeout(ctx, interval)
			err := l.refresh(timeoutCtx)
			timeoutCancel()
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, ErrLeaseLost) {
				log.Error("MongoDB CDC lease lost; stopping", "pipeline", l.pipeline, "target_database", l.key)
				if onLost != nil {
					onLost()
				}
				cancel(ErrLeaseLost)
				return
			}
			if err != nil {
				// A transient target error (e.g. an election) does not prove the lease is
				// lost; writes stay fenced anyway. Give up only once the TTL has certainly
				// elapsed without a successful refresh.
				if failingSince.IsZero() {
					failingSince = time.Now()
				}
				log.Warn("MongoDB CDC lease refresh failed", "pipeline", l.pipeline, "error", err, "failing_for", time.Since(failingSince).Round(time.Second).String())
				if time.Since(failingSince) >= l.ttl {
					if onLost != nil {
						onLost()
					}
					cancel(fmt.Errorf("%w: refresh failing for %s: %v", ErrLeaseLost, time.Since(failingSince).Round(time.Second), err))
					return
				}
				continue
			}
			failingSince = time.Time{}
		}
	}
}

func (l *lease) release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = l.coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: l.key}, {Key: "owner", Value: l.owner}, {Key: "ddl_in_progress", Value: bson.D{{Key: "$exists", Value: false}}}})
}
