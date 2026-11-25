package runner

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// mysqlbinlog --stop-datetime truncates fractional seconds, so a MySQL point-in-time target with a
// fraction would replay up to a second less than asked. It is refused, as for MongoDB.
func TestMySQLPITRRefusesFractionalSecondTarget(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	end := now.Add(-time.Hour)
	base := backup.Record{ID: "base", Source: "prod", Engine: backup.EngineMySQL, Method: backup.MethodMySQLDump,
		Status: backup.StatusComplete, JobID: "job", CreatedAt: end.Add(-time.Minute), StartTime: &end, EndTime: &end,
		Position: backup.Position{BinlogFile: "binlog.000001", BinlogPos: 100, GTIDSet: "server:1"},
		Details:  map[string]string{"source_identity": "server1", "databases": "shop"}}
	log := backup.Record{ID: "log", Source: base.Source, Engine: base.Engine, Method: backup.MethodBinlog,
		Kind: backup.KindLog, Status: backup.StatusComplete, JobID: base.JobID, CreatedAt: now, EndTime: &now,
		Position:    backup.Position{BinlogFile: base.Position.BinlogFile, BinlogPos: 4},
		EndPosition: backup.Position{BinlogFile: base.Position.BinlogFile, BinlogPos: 200},
		Details:     map[string]string{"source_identity": "server1", "seq": "1"}}
	whole := now.Add(-time.Minute)
	if _, err := PlanRestore([]backup.Record{base, log}, nil, &whole); err != nil {
		t.Fatalf("whole-second target: %v", err)
	}
	fractional := whole.Add(250 * time.Millisecond)
	for _, chosen := range []*backup.Record{nil, &base} {
		if plan, err := PlanRestore([]backup.Record{base, log}, chosen, &fractional); err == nil || !strings.Contains(err.Error(), "whole-second") {
			t.Fatalf("fractional target planned (chosen %v): %+v %v", chosen != nil, plan, err)
		}
	}
	env := &Env{}
	plan := RestorePlan{Chain: []backup.Record{base}, Logs: []backup.Record{log}, Target: &fractional}
	if err := env.Restore(t.Context(), plan, RestoreTarget{Database: "shop_restore"}); err == nil || !strings.Contains(err.Error(), "whole-second") {
		t.Fatalf("restore accepted a fractional MySQL target: %v", err)
	}
}

// recovery.signal and recovery settings in postgresql.auto.conf only work from PostgreSQL 12: a
// point-in-time restore of an older base backup is refused instead of publishing a data directory that
// would start without recovering.
func TestPGPITRRefusesPostgreSQL11(t *testing.T) {
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	for name, content := range map[string]string{"PG_VERSION": "11\n", "backup_label": "START WAL LOCATION: 0/2000028\n"} {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	payload := data.Bytes()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(payload) }))
	defer server.Close()
	env := testEnv(t, map[string]string{
		"STRAND_BACKUP_S3_ENDPOINT":             server.URL,
		"STRAND_BACKUP_S3_ACCESS_KEY":           "test",
		"STRAND_BACKUP_S3_SECRET_KEY":           "test",
		"STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true",
	}, store.NewMem())
	base := backup.Record{ID: "base", Engine: backup.EnginePostgres, Method: backup.MethodPGBaseBackup, Bucket: "backup-fixture",
		ObjectKey: "base.tar", SHA256: fmt.Sprintf("%x", sha256.Sum256(payload)), Bytes: int64(len(payload)),
		EndPosition: backup.Position{LSN: "0/2000100"}, Details: map[string]string{"timeline": "1"}}
	wal := backup.Record{ID: "wal", Engine: backup.EnginePostgres, Method: backup.MethodWAL, Kind: backup.KindLog, Bucket: "backup-fixture",
		ObjectKey: "wal", SHA256: base.SHA256, Bytes: base.Bytes, Details: map[string]string{"segment": "000000010000000000000002"}}
	at := time.Now().UTC().Truncate(time.Second)
	target := filepath.Join(t.TempDir(), "pg11")
	err := env.Restore(t.Context(), RestorePlan{Chain: []backup.Record{base}, Logs: []backup.Record{wal}, Target: &at}, RestoreTarget{Dir: target})
	if err == nil || !strings.Contains(err.Error(), "PostgreSQL 11 is not supported") {
		t.Fatalf("point-in-time restore of PostgreSQL 11: %v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("refused restore published %s: %v", target, err)
	}
	// Without logs the base backup is restored as is.
	if err := env.Restore(t.Context(), RestorePlan{Chain: []backup.Record{base}}, RestoreTarget{Dir: target}); err != nil {
		t.Fatal(err)
	}
}
