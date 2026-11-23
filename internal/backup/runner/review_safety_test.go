package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"strand/internal/backup"
	"strand/internal/testutil"
	"strand/internal/web/store"
)

func TestPGRestoreVerifiesPayloadPastTarEOF(t *testing.T) {
	var data bytes.Buffer
	tarWriter := tar.NewWriter(&data)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "PG_VERSION", Mode: 0600, Size: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write([]byte("17")); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	data.WriteString("trailing bytes after tar EOF")
	payload := bytes.Clone(data.Bytes())
	digest := fmt.Sprintf("%x", sha256.Sum256(payload))
	corruptPayload := append(bytes.Clone(payload[:len(payload)-1]), '!')
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/corrupt.tar") {
			_, _ = w.Write(corruptPayload)
			return
		}
		_, _ = w.Write(payload)
	}))
	defer server.Close()
	env := testEnv(t, map[string]string{
		"STRAND_BACKUP_S3_ENDPOINT":             server.URL,
		"STRAND_BACKUP_S3_ACCESS_KEY":           "test",
		"STRAND_BACKUP_S3_SECRET_KEY":           "test",
		"STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true",
	}, store.NewMem())
	base := backup.Record{ID: "base", Engine: backup.EnginePostgres, Method: backup.MethodPGBaseBackup,
		Bucket: "backup-fixture", ObjectKey: "base.tar", SHA256: digest, Bytes: int64(len(payload))}
	parent := t.TempDir()
	target := filepath.Join(parent, "postgres")
	plan := RestorePlan{Chain: []backup.Record{base}}
	staging, err := env.tmpDir()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(staging)
	paths, err := env.stageLogs(t.Context(), []backup.Record{base}, staging, func(backup.Record) string { return "segment" })
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]os.FileMode{staging: 0700, paths[0]: 0600} {
		stat, err := os.Stat(path)
		if err != nil || stat.Mode().Perm() != want {
			t.Fatalf("staging permissions for %s = %v, %v", path, stat, err)
		}
	}

	if err := env.Restore(t.Context(), plan, RestoreTarget{Dir: target}); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(target, "PG_VERSION")); err != nil || string(got) != "17" {
		t.Fatalf("restored PG_VERSION = %q, %v", got, err)
	}

	// Alter only the part that tar would never read.
	plan.Chain[0].ObjectKey = "corrupt.tar"
	bad := filepath.Join(parent, "corrupt")
	if err := env.Restore(t.Context(), plan, RestoreTarget{Dir: bad}); err == nil {
		t.Fatal("restore accepted corruption after the end of the tar")
	}
	if _, err := os.Stat(bad); !os.IsNotExist(err) {
		t.Fatalf("corrupt payload published the target: %v", err)
	}
	if leftovers, err := filepath.Glob(filepath.Join(parent, ".strand-restore-*")); err != nil || len(leftovers) != 0 {
		t.Fatalf("staging leftovers = %v, %v", leftovers, err)
	}
}

func TestPITRPostgreSQLRequiresLineageAndVerifyOnReplay(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-2 * time.Hour)
	// The WAL is ingested hours after the base. Ingest time never proves
	// that replay can reach the requested wall-clock point.
	base := backup.Record{ID: "base", Source: "prod", Engine: backup.EnginePostgres, Method: backup.MethodPGBaseBackup,
		Status: backup.StatusComplete, JobID: "job", CreatedAt: old, EndPosition: backup.Position{LSN: "0/18"},
		Details: map[string]string{"source_identity": "server1", "timeline": "1"}}
	wal := func(id, server, timeline, seq, start, end string) backup.Record {
		return backup.Record{ID: id, Source: "prod", Engine: backup.EnginePostgres, Method: backup.MethodWAL,
			Kind: backup.KindLog, Status: backup.StatusComplete, JobID: "job", CreatedAt: now,
			Position: backup.Position{LSN: start}, EndPosition: backup.Position{LSN: end},
			Details: map[string]string{"source_identity": server, "seq": seq, "timeline": timeline}}
	}
	target := now.Add(-time.Hour)
	first := wal("first", "server1", "1", "1", "0/10", "0/20")
	next := wal("next", "server1", "1", "2", "0/20", "0/30")
	foreign := wal("foreign", "server2", "1", "1", "0/10", "0/20")
	fork := wal("fork", "server1", "2", "2", "0/20", "0/30")
	noIdentity := wal("legacy", "", "1", "1", "0/10", "0/20")
	rs := []backup.Record{base, foreign, first, fork, next, noIdentity}
	plan, err := PlanRestore(rs, nil, &target)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.VerifyOnReplay || len(plan.Logs) != 2 || plan.Logs[0].ID != first.ID || plan.Logs[1].ID != next.ID {
		t.Fatalf("wrong provisional WAL chain: %+v", plan)
	}
	if RecoveryWindow(rs).Continuous {
		t.Fatal("ingest time was treated as a verified PostgreSQL recovery window")
	}
	if _, err := PlanRestore([]backup.Record{base, foreign, fork, next, noIdentity}, nil, &target); err == nil {
		t.Fatal("missing WAL segment that contains the base endpoint was accepted")
	}
}

func TestNativeToolsKeepTLSAndStopAfterExit(t *testing.T) {
	local := backup.ConnSpec{Host: "127.0.0.1", TLSServerName: "database.example", TLSMode: "verify-full"}
	inv := Invocation{Args: []string{"--host=127.0.0.1"}}
	if err := addNativePGTLS(&inv, local); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(inv.Env, "PGSSLMODE=verify-full") || !slices.Contains(inv.Env, "PGHOSTADDR=127.0.0.1") ||
		!slices.Contains(inv.Args, "--host=database.example") {
		t.Fatalf("native PostgreSQL lost the TLS policy: %v %v", inv.Env, inv.Args)
	}
	if _, err := MongoURI(func(string) string { return "mongodb://db/?tlsInsecure=true" },
		backup.ConnSpec{URIEnv: "URI", TLSMode: "verify-full"}); err == nil {
		t.Fatal("MongoDB URI overrode an explicit TLS verification policy")
	}

	for _, mode := range []string{"normal", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			getenv := func(string) string { return "" }
			command := "printf 'done\\n'"
			if mode == "cancel" {
				command = "printf 'ready\\n'; exec sleep 30"
			}
			p, err := Start(ctx, getenv, Invocation{Tool: "sh", Args: []string{"-c", command}}, nil, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if mode == "cancel" {
				buf := make([]byte, len("ready\n"))
				if _, err := io.ReadFull(p, buf); err != nil {
					t.Fatal(err)
				}
				cancel()
			}
			data, err := io.ReadAll(p)
			if mode == "normal" && (err != nil || strings.TrimSpace(string(data)) != "done") {
				t.Fatalf("normal execution: %q, %v", data, err)
			}
			if mode == "cancel" && err == nil {
				t.Fatal("canceled execution succeeded")
			}
			select {
			case <-p.done:
			default:
				t.Fatal("process exit did not release the cancellation watcher")
			}
		})
	}
}

func TestMongoRestoreDestinationStripsCredentials(t *testing.T) {
	got, err := MongoRestoreDestination("mongodb://user:secret@db.example:27017/?directConnection=true", "target")
	if err != nil || got != "mongodb://db.example:27017/target" {
		t.Fatalf("destination = %q, %v", got, err)
	}
	got, err = MongoRestoreDestination("mongodb://db.example:27017/", "")
	if err != nil || got != "mongodb://db.example:27017/*" {
		t.Fatalf("deployment = %q, %v", got, err)
	}
}

func TestMySQLRecoveryWindowKeepsStandaloneSnapshotsOutside(t *testing.T) {
	now := time.Now().UTC()
	anchor := now.Add(-time.Hour)
	base := backup.Record{ID: "base", Source: "prod", Engine: backup.EngineMySQL, Method: backup.MethodMySQLDump,
		Status: backup.StatusComplete, JobID: "job", CreatedAt: anchor,
		Position: backup.Position{BinlogFile: "binlog.000001", BinlogPos: 100, GTIDSet: "server:1"},
		Details:  map[string]string{"source_identity": "server1"}}
	log := backup.Record{ID: "log", Source: base.Source, Engine: base.Engine, Method: backup.MethodBinlog,
		Kind: backup.KindLog, Status: backup.StatusComplete, JobID: base.JobID, CreatedAt: now,
		Position:    backup.Position{BinlogFile: base.Position.BinlogFile, BinlogPos: 4},
		EndPosition: backup.Position{BinlogFile: base.Position.BinlogFile, BinlogPos: 200},
		Details:     map[string]string{"source_identity": "server1", "seq": "1"}}
	old := base
	old.ID, old.JobID, old.CreatedAt = "unrelated-old", "old-job", now.Add(-48*time.Hour)
	newer := old
	newer.ID, newer.CreatedAt = "unrelated-new", now.Add(time.Hour)
	window := RecoveryWindow([]backup.Record{old, newer, base, log})
	if !window.Continuous || !window.Start.Equal(anchor) || !window.End.Equal(now) {
		t.Fatalf("continuous recovery window included unrelated snapshots: %+v", window)
	}
	// A newer snapshot without coverage must not invalidate the window
	// certified by an older base. An explicit selection stays pinned.
	newer.CreatedAt = now.Add(-time.Minute)
	records := []backup.Record{old, newer, base, log}
	plan, err := PlanRestore(records, nil, &now)
	if err != nil || plan.Base().ID != base.ID {
		t.Fatalf("restore did not select the newest covered base: %+v %v", plan, err)
	}
	if _, err := PlanRestore(records, &newer, &now); err == nil {
		t.Fatal("restore silently changed an explicit base")
	}
	copy := log
	copy.ID, copy.CopyOf = "log-copy", log.ID
	if gaps := LogGaps(append(records, copy)); len(gaps) != 0 {
		t.Fatalf("copy of a log reported a gap: %v", gaps)
	}
}

func TestMongoRestoreRefusesExistingDatabaseOrDeployment(t *testing.T) {
	rs := testutil.MongoReplicaSet(t)
	c, err := connectBackupMongo(rs.URI, backup.ConnSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(context.Background())
	if _, err := c.Database("occupied").Collection("items").InsertOne(t.Context(), bson.D{{Key: "_id", Value: 1}}); err != nil {
		t.Fatal(err)
	}
	env := testEnv(t, map[string]string{"TARGET_MONGO_URI": rs.URI}, store.NewMem())
	conn := backup.ConnSpec{URIEnv: "TARGET_MONGO_URI"}
	base := backup.Record{ID: "mongo", Engine: backup.EngineMongoDB, Method: backup.MethodMongodump,
		Details: map[string]string{"databases": "source"}}
	plan := RestorePlan{Chain: []backup.Record{base}}
	if err := env.Restore(t.Context(), plan, RestoreTarget{Conn: conn, Database: "occupied"}); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("restore into existing MongoDB database: %v", err)
	}
	plan.Chain[0].Details = nil
	if err := env.Restore(t.Context(), plan, RestoreTarget{Conn: conn, Deployment: true}); err == nil || !strings.Contains(err.Error(), "empty deployment") {
		t.Fatalf("restore into existing MongoDB deployment: %v", err)
	}
	if err := env.Restore(t.Context(), plan, RestoreTarget{Conn: conn, Dir: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "explicit deployment target") {
		t.Fatalf("the filesystem path was treated as a deployment confirmation: %v", err)
	}
}
