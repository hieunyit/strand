package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// A MongoDB restore that fails after mongorestore wrote data drops what it created, like the MySQL and
// PostgreSQL restores: the target database, or the databases restored into an empty deployment.
func TestMongoRestoreDropsPartialDataOnFailure(t *testing.T) {
	requireDocker(t)
	dir := shared(t)
	tools := filepath.Join(dir, "tools")
	if err := os.Mkdir(tools, 0o755); err != nil {
		t.Fatal(err)
	}
	vars := startRPPMinIO(t, dir, "mongo-backups")
	// The source is a one-member replica set: a whole-deployment mongodump reads the oplog.
	srcPort := freePort(t)
	srcContainer := startContainer(t, "mongo-rs", dir, "mongo:7.0", "--replSet", "rs0", "--port", fmt.Sprint(srcPort), "--bind_ip", "127.0.0.1")
	initiate := fmt.Sprintf(`for i in $(seq 1 60); do mongosh --quiet --port %[1]d --eval 'try { rs.status().ok } catch (e) { rs.initiate({_id: "rs0", members: [{_id: 0, host: "127.0.0.1:%[1]d"}]}).ok }' && break; sleep 1; done`, srcPort)
	docker(t, "exec", srcContainer, "sh", "-c", initiate)
	srcURI := fmt.Sprintf("mongodb://127.0.0.1:%d/?directConnection=true", srcPort)
	src := mongoFixture{Container: srcContainer, Port: srcPort, Client: connectMongoForTest(t, srcURI)}
	dst := startRPPMongo(t, dir)
	vars[EnvToolsDir] = tools
	vars["STRAND_BACKUP_TMPDIR"] = dir
	vars["SRC_MONGO_URI"] = srcURI
	vars["DST_MONGO_URI"] = fmt.Sprintf("mongodb://127.0.0.1:%d", dst.Port)
	wrapper(t, tools, "mongodump", src.Container, "mongodump")
	// Restores everything, then fails (e.g. a late index build error or a lost connection).
	// With the marker file, another client creates a database on the target while the restore runs
	// (review DBA): cleanup must not drop it.
	foreignMarker := filepath.Join(dir, "foreign-db")
	failing := fmt.Sprintf("#!/bin/sh\ndocker exec -i %[1]s mongorestore \"$@\" || exit 1\n"+
		"if [ -f %[2]s ]; then docker exec %[1]s mongosh --quiet mongodb://127.0.0.1:%[3]d --eval 'db.getSiblingDB(\"otherapp\").x.insertOne({a: 1})' >/dev/null || exit 3; fi\n"+
		"echo \"simulated failure after the load\" >&2\nexit 1\n", src.Container, foreignMarker, dst.Port)
	if err := os.WriteFile(filepath.Join(tools, "mongorestore"), []byte(failing), 0o755); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	deadline := time.Now().Add(time.Minute)
	for {
		var hello bson.M
		if err := src.Client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err == nil && hello["isWritablePrimary"] == true {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica set did not elect a primary")
		}
		time.Sleep(500 * time.Millisecond)
	}
	for i := 1; i <= 20; i++ {
		if _, err := src.Client.Database("testdb").Collection("items").InsertOne(ctx, bson.M{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	e := testEnv(t, vars, store.NewMem())
	run := func(id string, dbs []string) backup.Record {
		rec, err := e.RunJob(ctx, backup.Job{ID: id, Name: id, EverySeconds: 3600, Bucket: "mongo-backups",
			JobOptions: backup.JobOptions{Method: backup.MethodMongodump, Source: "mongo-src",
				Conn: backup.ConnSpec{URIEnv: "SRC_MONGO_URI", Databases: dbs}}})
		if err != nil || rec.Status != backup.StatusComplete {
			t.Fatalf("mongodump %s: %v %v", id, rec.Status, err)
		}
		return rec
	}
	userDatabases := func() []string {
		names, err := src.Client.ListDatabaseNames(ctx, bson.D{})
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, n := range names {
			if n != "admin" && n != "config" && n != "local" {
				out = append(out, n)
			}
		}
		return out
	}

	one := run("bjob-mongo-db", []string{"testdb"})
	err := e.Restore(ctx, RestorePlan{Chain: []backup.Record{one}}, RestoreTarget{Conn: backup.ConnSpec{URIEnv: "SRC_MONGO_URI"}, Database: "testdb_restored"})
	if err == nil || !strings.Contains(err.Error(), "simulated failure") || strings.Contains(err.Error(), "cleanup failed") {
		t.Fatalf("failing restore: %v", err)
	}
	if dbs := userDatabases(); len(dbs) != 1 || dbs[0] != "testdb" {
		t.Fatalf("failed restore left partial data: databases %v", dbs)
	}

	// Whole deployment into an empty one (the failing tool runs against the source container's binary,
	// the target URI points to the second server).
	all := run("bjob-mongo-all", nil)
	if err := os.WriteFile(foreignMarker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err = e.Restore(ctx, RestorePlan{Chain: []backup.Record{all}}, RestoreTarget{Conn: backup.ConnSpec{URIEnv: "DST_MONGO_URI"}, Deployment: true})
	if err == nil || !strings.Contains(err.Error(), "simulated failure") || strings.Contains(err.Error(), "cleanup failed") {
		t.Fatalf("failing deployment restore: %v", err)
	}
	restoreErr := err
	names, err := dst.Client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		t.Fatal(err)
	}
	foreign := false
	for _, n := range names {
		switch n {
		case "admin", "config", "local":
		case "otherapp":
			foreign = true
		default:
			t.Fatalf("failed deployment restore left database %q (all: %v)", n, names)
		}
	}
	if !foreign {
		t.Fatalf("cleanup dropped database otherapp, created on the target by another client during the restore (all: %v)", names)
	}
	if !strings.Contains(restoreErr.Error(), `"otherapp"`) {
		t.Fatalf("cleanup must report the database it left in place: %v", restoreErr)
	}
}

func connectMongoForTest(t *testing.T, uri string) *mongo.Client {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}
