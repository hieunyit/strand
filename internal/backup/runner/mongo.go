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

// LatestOplogTS returns the newest oplog entry timestamp of a replica set.
func LatestOplogTS(ctx context.Context, uri string) (bson.Timestamp, error) {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(20 * time.Second))
	if err != nil {
		return bson.Timestamp{}, err
	}
	defer client.Disconnect(context.Background())
	var doc struct {
		TS bson.Timestamp `bson:"ts"`
	}
	err = client.Database("local").Collection("oplog.rs").FindOne(cctx, bson.D{},
		options.FindOne().SetSort(bson.D{{Key: "$natural", Value: -1}}).SetProjection(bson.D{{Key: "ts", Value: 1}})).Decode(&doc)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("read latest oplog entry (is the source a replica set?): %w", err)
	}
	return doc.TS, nil
}
