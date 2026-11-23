package runner

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// FormatOplogTS renders an oplog timestamp as "<seconds>.<increment>".
func FormatOplogTS(ts bson.Timestamp) string {
	return strconv.FormatUint(uint64(ts.T), 10) + "." + strconv.FormatUint(uint64(ts.I), 10)
}

// ParseOplogTS parses "<seconds>.<increment>".
func ParseOplogTS(s string) (bson.Timestamp, error) {
	a, b, ok := strings.Cut(s, ".")
	t, err1 := strconv.ParseUint(a, 10, 32)
	i, err2 := strconv.ParseUint(b, 10, 32)
	if !ok || err1 != nil || err2 != nil {
		return bson.Timestamp{}, fmt.Errorf("invalid oplog timestamp %q", s)
	}
	return bson.Timestamp{T: uint32(t), I: uint32(i)}, nil
}

// latestOplogTS returns the newest oplog entry timestamp of a replica set.
func latestOplogTS(ctx context.Context, client *mongo.Client) (bson.Timestamp, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var doc struct {
		TS bson.Timestamp `bson:"ts"`
	}
	err := client.Database("local").Collection("oplog.rs").FindOne(cctx, bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetProjection(bson.D{{Key: "ts", Value: 1}})).Decode(&doc)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("read latest oplog entry (is the source a replica set?): %w", err)
	}
	return doc.TS, nil
}

// mongoHistory identifies the replica set and checks its majority commit
// point. Read history from the primary to avoid archiving a divergent tail.
func mongoHistory(ctx context.Context, client *mongo.Client) (string, int64, bson.Timestamp, error) {
	admin := client.Database("admin")
	var config struct {
		Config struct {
			Settings struct {
				ID bson.ObjectID `bson:"replicaSetId"`
			}
		}
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "replSetGetConfig", Value: 1}}).Decode(&config); err != nil {
		return "", 0, bson.Timestamp{}, fmt.Errorf("read replica set identity: %w", err)
	}
	var status struct {
		State   int   `bson:"myState"`
		Term    int64 `bson:"term"`
		OpTimes struct {
			Committed struct {
				TS bson.Timestamp
			} `bson:"lastCommittedOpTime"`
		}
	}
	if err := admin.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&status); err != nil {
		return "", 0, bson.Timestamp{}, fmt.Errorf("read majority oplog position: %w", err)
	}
	if config.Config.Settings.ID.IsZero() || status.State != 1 || status.Term <= 0 || status.OpTimes.Committed.TS.T == 0 {
		return "", 0, bson.Timestamp{}, fmt.Errorf("MongoDB PITR needs an identified replica set primary with a majority commit position")
	}
	return config.Config.Settings.ID.Hex(), status.Term, status.OpTimes.Committed.TS, nil
}

func awaitMongoCommit(ctx context.Context, client *mongo.Client, sourceID string, term int64, at bson.Timestamp) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if at.T == 0 {
		return fmt.Errorf("mongodump did not contain an oplog boundary")
	}
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		id, currentTerm, committed, err := mongoHistory(ctx, client)
		if err != nil {
			return err
		}
		if id != sourceID || currentTerm != term {
			return fmt.Errorf("MongoDB replica set or primary term changed before oplog commit was verified")
		}
		if at.Compare(committed) <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for MongoDB majority oplog commit: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
