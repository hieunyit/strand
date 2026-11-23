package mongocdc

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"

	"strand/internal/testutil"
)

func TestCDCLeaseBlocksTakeoverDuringUncertainDDL(t *testing.T) {
	rs := testutil.MongoReplicaSet(t)
	client := mustClient(t, rs.URI)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	coll := client.Database(stateDatabase, targetDBOptions()).Collection(lockCollection)
	acquire := func() (*lease, error) {
		return acquireLease(ctx, client, coll, "review_target", "review", time.Minute, 1, quietLogger(t))
	}
	first, err := acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()
	if err := first.withDDL(ctx, "confirmed DDL", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	err = first.withDDL(ctx, "uncertain DDL", func() error {
		// Make the lease immediately eligible for ordinary expiry-based takeover.
		_, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: first.key}}, bson.D{
			{Key: "$set", Value: bson.D{{Key: "expires_at", Value: time.Unix(0, 0)}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := acquire(); !errors.Is(err, ErrDDLUncertain) {
			t.Fatalf("takeover while DDL is active: %v", err)
		}
		return errors.New("connection dropped after sending DDL")
	})
	if !errors.Is(err, ErrDDLUncertain) {
		t.Fatalf("guard error = %v", err)
	}
	first.release()
	if _, err := acquire(); !errors.Is(err, ErrDDLUncertain) {
		t.Fatalf("takeover after uncertain DDL: %v", err)
	}
	if _, err := coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: first.key}}, bson.D{
		{Key: "$unset", Value: bson.D{{Key: "ddl_in_progress", Value: ""}}},
	}); err != nil {
		t.Fatal(err)
	}
	second, err := acquire()
	if err != nil {
		t.Fatal(err)
	}
	defer second.release()
	if err := first.withDDL(ctx, "stale index operation", func() error {
		t.Error("the previous owner reached the DDL command")
		return nil
	}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old owner was not fenced: %v", err)
	}

}

func TestCDCSourceLookupCarriesEventClusterTime(t *testing.T) {
	rs := testutil.MongoReplicaSet(t)
	times := make(chan bson.Timestamp, 1)
	client, err := mongo.Connect(options.Client().ApplyURI(rs.URI).SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName == "find" {
				ts, inc, _ := e.Command.Lookup("readConcern", "afterClusterTime").TimestampOK()
				select {
				case times <- bson.Timestamp{T: ts, I: inc}:
				default:
				}
			}
		},
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	defer client.Disconnect(ctx)
	source := client.Database("review_source", options.Database().SetReadConcern(readconcern.Majority()))
	if _, err := source.Collection("docs").InsertOne(ctx, bson.D{{Key: "_id", Value: int32(1)}}); err != nil {
		t.Fatal(err)
	}
	r := &runner{source: source, srcClient: client}
	at, err := r.sourceTime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := newBatch()
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}})
	if err != nil {
		t.Fatal(err)
	}
	b.addLookup("docs", bson.Raw(raw).Lookup("_id"))
	b.last = at
	if n, err := r.resolve(ctx, b); err != nil || n != 1 || b.sorted()[0].doc == nil {
		t.Fatalf("lookup n=%d err=%v", n, err)
	}
	select {
	case got := <-times:
		if got != at {
			t.Fatalf("afterClusterTime = %v, want %v", got, at)
		}
	default:
		t.Fatal("did not observe source lookup")
	}
}
