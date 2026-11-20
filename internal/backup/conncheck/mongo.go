package conncheck

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"strand/internal/backup"
)

func passwordOfURI(raw string) string {
	if u, err := url.Parse(raw); err == nil && u.User != nil {
		p, _ := u.User.Password()
		return p
	}
	return ""
}

// MongoURI builds the MongoDB connection URI for target t.
func MongoURI(t Target) string {
	uri := t.URI
	if uri == "" {
		port := t.Port
		if port == 0 {
			port = 27017
		}
		u := url.URL{
			Scheme:   "mongodb",
			Host:     hostPort(Target{Host: t.Host, Port: port, Engine: t.Engine}),
			Path:     "/",
			RawQuery: "directConnection=true",
		}
		if t.User != "" {
			u.User = url.UserPassword(t.User, t.Password)
		}
		uri = u.String()
	}
	return uri
}

func checkMongo(ctx context.Context, t Target, res *Result) {
	uri := MongoURI(t)
	opts := options.Client().ApplyURI(uri).SetServerSelectionTimeout(8 * time.Second).SetConnectTimeout(8 * time.Second)
	if t.TLSMode != "" && t.TLSMode != "disable" {
		serverName := t.TLSServerName
		if serverName == "" {
			serverName = t.Host
		}
		cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
		if t.TLSMode == "require" {
			cfg.InsecureSkipVerify = true
		} else if t.CACertPEM != "" {
			pool, err := caPool(t)
			if err != nil {
				res.fail("tls", err)
				return
			}
			cfg.RootCAs = pool
		}
		opts.SetTLSConfig(cfg)
	}
	start := time.Now()
	client, err := mongo.Connect(opts)
	if err != nil {
		res.fail("connect", err)
		return
	}
	defer client.Disconnect(context.WithoutCancel(ctx))
	admin := client.Database("admin")
	var hello bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		res.fail("connect", err)
		return
	}
	res.LatencyMS = time.Since(start).Milliseconds()
	res.OK = true
	res.add("connect", "ok", fmt.Sprintf("%d ms", res.LatencyMS))

	var info bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&info); err != nil {
		res.add("version", "warn", err.Error())
	} else if v, ok := info["version"].(string); ok {
		res.Version = v
		res.add("version", "ok", v)
	}
	var caps backup.PITRCaps
	if set, _ := hello["setName"].(string); set != "" {
		caps.ReplicaSet = true
		res.ReplicaSet = set
		res.IsReplicaSet = boolp(true)
		res.add("replica_set", "ok", "replica set "+set)
	} else {
		res.IsReplicaSet = boolp(false)
		res.add("replica_set", "warn", "standalone server: no oplog, PITR unavailable")
	}
	names, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		res.add("databases", "warn", err.Error())
	} else {
		for _, n := range names {
			if n != "admin" && n != "local" && n != "config" {
				res.Databases = append(res.Databases, n)
			}
		}
		sort.Strings(res.Databases)
		res.add("databases", "ok", fmt.Sprintf("%d user database(s)", len(res.Databases)))
	}
	res.assessPITR(caps)
}
