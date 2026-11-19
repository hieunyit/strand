package mongocdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// lease is the single-writer guard for one target database.
//
// Expiry is evaluated only with the target server clock ($$NOW), so client clock skew
// does not matter. Expiry alone is not trusted for safety: every target write commits in
// a transaction that also updates the lease document under the filter owner == ours
// (fence). Once another process has taken the lease (after expiry), the stale owner's
// fence matches nothing and its transaction aborts, so a process paused for longer than
// the TTL (GC, SIGSTOP, VM freeze) cannot write after waking up. Concurrent takeover
// and fence conflict on the same document; MongoDB serializes them (WriteConflict).
type lease struct {
	client   *mongo.Client
	coll     *mongo.Collection
	key      string
	owner    string
	pipeline string
	ttl      time.Duration
}

func expiresIn(ttl time.Duration) bson.D {
	return bson.D{{Key: "$add", Value: bson.A{"$$NOW", ttl.Milliseconds()}}}
}

func acquireLease(ctx context.Context, client *mongo.Client, coll *mongo.Collection, key, pipeline string, ttl time.Duration) (*lease, error) {
	owner := uuid.NewString()
	// One atomic upsert keyed only by _id ($expr is not allowed in an upsert predicate):
	// the pipeline takes ownership only when the current lease is absent or expired
	// on the target clock, and the returned post-image tells us who owns it.
	expired := bson.D{{Key: "$lte", Value: bson.A{bson.D{{Key: "$ifNull", Value: bson.A{"$expires_at", time.Unix(0, 0).UTC()}}}, "$$NOW"}}}
	take := func(field string, v any) bson.E {
		return bson.E{Key: field, Value: bson.D{{Key: "$cond", Value: bson.A{expired, v, "$" + field}}}}
	}
	update := mongo.Pipeline{bson.D{{Key: "$set", Value: bson.D{
		take("owner", bson.D{{Key: "$literal", Value: owner}}), take("pipeline", bson.D{{Key: "$literal", Value: pipeline}}),
		take("acquired_at", "$$NOW"), take("expires_at", expiresIn(ttl)),
	}}}}
	var got struct {
		Owner     string    `bson:"owner"`
		Pipeline  string    `bson:"pipeline"`
		ExpiresAt time.Time `bson:"expires_at"`
	}
	err := coll.FindOneAndUpdate(ctx, bson.D{{Key: "_id", Value: key}}, update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&got)
	if mongo.IsDuplicateKeyError(err) { // lost a concurrent upsert race
		err = coll.FindOne(ctx, bson.D{{Key: "_id", Value: key}}).Decode(&got)
	}
	if err != nil {
		return nil, fmt.Errorf("acquire MongoDB CDC lease: %w", err)
	}
	if got.Owner != owner {
		return nil, fmt.Errorf("another MongoDB CDC process (pipeline %q) holds the lease for target database %q until %s (target clock); stop it or wait for expiry", got.Pipeline, key, got.ExpiresAt.UTC().Format(time.RFC3339))
	}
	return &lease{client: client, coll: coll, key: key, owner: owner, pipeline: pipeline, ttl: ttl}, nil
}

// fence extends the lease if and only if this process still owns it. Inside a
// transaction it makes the transaction's commit conditional on ownership.
func (l *lease) fence(ctx context.Context) error {
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

// txn runs fn in a target transaction that is fenced by the lease. Transient
// transaction errors are retried by the driver; fn must be idempotent.
func (l *lease) txn(ctx context.Context, fn func(tx context.Context) error) error {
	sess, err := l.client.StartSession()
	if err != nil {
		return err
	}
	defer sess.EndSession(context.Background())
	_, err = sess.WithTransaction(ctx, func(tx context.Context) (any, error) {
		if err := l.fence(tx); err != nil {
			return nil, err
		}
		return nil, fn(tx)
	})
	if errors.Is(err, ErrLeaseLost) {
		return ErrLeaseLost
	}
	return err
}

func (l *lease) refreshLoop(ctx context.Context, log *slog.Logger, cancel context.CancelCauseFunc) {
	interval := l.ttl / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			timeoutCtx, timeoutCancel := context.WithTimeout(ctx, interval)
			err := l.fence(timeoutCtx)
			timeoutCancel()
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, ErrLeaseLost) {
				log.Error("MongoDB CDC lease lost; stopping", "pipeline", l.pipeline, "target_database", l.key)
				cancel(ErrLeaseLost)
				return
			}
			if err != nil {
				// A transient target error does not prove the lease is lost; writes stay
				// fenced anyway. Give up only when the TTL has certainly elapsed.
				failures++
				log.Warn("MongoDB CDC lease refresh failed", "pipeline", l.pipeline, "error", err, "consecutive_failures", failures)
				if failures >= 3 {
					cancel(fmt.Errorf("%w: refresh failed %d times: %v", ErrLeaseLost, failures, err))
					return
				}
				continue
			}
			failures = 0
		}
	}
}

func (l *lease) release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = l.coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: l.key}, {Key: "owner", Value: l.owner}})
}
