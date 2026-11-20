package web

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func backupCommandDir(t *testing.T) (dir, script string) {
	t.Helper()
	dir = t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	script = filepath.Join(dir, "mysql-backup")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvBackupCommandDir, dir)
	return dir, script
}

func jobJSON(command, artifact string) string {
	b, _ := json.Marshal(map[string]any{"name": "nightly-" + filepath.Base(command) + filepath.Base(artifact), "command": command, "every_seconds": 3600,
		"artifact_template": artifact, "manifest_template": "/var/backups/{id}.json", "bucket": "central-backups", "enabled": false})
	return string(b)
}

// Backup jobs execute programs as the Web service account: only executables in the operator-designated
// directory are accepted, paths cannot climb out with "..", and every change is audited fail-closed.
func TestBackupJobCommandAllowlistTraversalAndAudit(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()
	post := func(body string) (int, string) {
		resp, b := e.do(c, "POST", "/api/v1/backup-jobs", body, map[string]string{"X-CSRF-Token": csrf, "Content-Type": "application/json"})
		return resp.StatusCode, b
	}
	t.Setenv(EnvBackupCommandDir, "")
	if code, body := post(jobJSON("/bin/sh", "/var/backups/{id}.sql")); code != 400 || !strings.Contains(body, EnvBackupCommandDir) {
		t.Fatalf("job with backup command dir unset: %d %s", code, body)
	}
	_, script := backupCommandDir(t)
	for _, cmd := range []string{"/bin/sh", "/usr/bin/env"} {
		if code, body := post(jobJSON(cmd, "/var/backups/{id}.sql")); code != 400 {
			t.Fatalf("command %s outside the allowlisted dir accepted: %d %s", cmd, code, body)
		}
	}
	if code, body := post(jobJSON(script, "/var/backups/{id}/../../etc")); code != 400 {
		t.Fatalf("artifact template with '..' accepted: %d %s", code, body)
	}
	if code, body := post(jobJSON(script, "/var/backups/{id}.sql")); code != 201 {
		t.Fatalf("valid job: %d %s", code, body)
	}
	// Without a CSRF token a cookie session cannot create jobs.
	if resp, _ := e.do(c, "POST", "/api/v1/backup-jobs", jobJSON(script, "/var/backups/x-{id}.sql"), map[string]string{"Content-Type": "application/json"}); resp.StatusCode != 403 {
		t.Fatalf("POST without CSRF = %d", resp.StatusCode)
	}
	jobs, _ := st.ListBackupJobs(context.Background())
	if len(jobs) != 1 {
		t.Fatalf("%d jobs stored", len(jobs))
	}
	rows, _ := st.ListAudit(context.Background(), 50)
	found := false
	for _, r := range rows {
		found = found || r.Action == "backup_job.create"
	}
	if !found {
		t.Fatal("job creation not audited")
	}
	// Audit unavailable: changes are refused and nothing is stored.
	e.srv.audit.Close()
	if code, _ := post(jobJSON(script, "/var/backups/y-{id}.sql")); code != 500 {
		t.Fatalf("create with broken audit = %d", code)
	}
	if resp, _ := e.do(c, "DELETE", "/api/v1/backup-jobs/"+jobs[0].ID, "", map[string]string{"X-CSRF-Token": csrf}); resp.StatusCode != 500 {
		t.Fatalf("delete with broken audit = %d", resp.StatusCode)
	}
	if jobs, _ := st.ListBackupJobs(context.Background()); len(jobs) != 1 {
		t.Fatalf("job set changed without an audit record: %d", len(jobs))
	}
}

func TestBackupJobTemplateTraversalRejected(t *testing.T) {
	j := backup.Job{ID: "j1", Name: "n", Command: "/opt/b/run", EverySeconds: 3600, ArtifactTemplate: "/var/backups/{id}/../../etc",
		ManifestTemplate: "/var/backups/{id}.json", Bucket: "central-backups"}
	if err := backup.ValidateJob(j); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("traversal template err = %v", err)
	}
	j.ArtifactTemplate = "/var/backups/{id}.sql"
	if err := backup.ValidateJob(j); err != nil {
		t.Fatal(err)
	}
}

type flakyJobStore struct {
	store.Store
	failures atomic.Int32
}

func (f *flakyJobStore) GetBackupJob(ctx context.Context, id string) (backup.Job, bool, error) {
	if f.failures.Add(-1) >= 0 {
		return backup.Job{}, false, errors.New("metadata database unavailable")
	}
	return f.Store.GetBackupJob(ctx, id)
}

type startRecorder struct {
	backupStopTestRunner
	started atomic.Int32
}

func (r *startRecorder) Start(context.Context, JobSpec) (string, error) {
	r.started.Add(1)
	return "proc-1", nil
}

// The scheduler must come back after a metadata-database outage longer than a few retries.
func TestReconcileKeepsRetryingThroughOutage(t *testing.T) {
	_, script := backupCommandDir(t)
	old := reconcileDelay
	reconcileDelay.first, reconcileDelay.max = time.Millisecond, 2*time.Millisecond
	defer func() { reconcileDelay = old }()
	mem := store.NewMem()
	j := backup.Job{ID: "bjob-1", Name: "n", Command: script, EverySeconds: 3600, ArtifactTemplate: "/var/backups/{id}.sql",
		ManifestTemplate: "/var/backups/{id}.json", Bucket: "central-backups", Enabled: true}
	if err := mem.CreateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	fs := &flakyJobStore{Store: mem}
	fs.failures.Store(20) // far more than the previous 6-attempt budget
	runner := &startRecorder{backupStopTestRunner: backupStopTestRunner{jobs: map[string]JobStatus{}}}
	s := &Server{cfg: &Config{}, db: &metaDB{st: fs}, jobs: runner, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan struct{})
	go func() { s.reconcileBackupSchedule(backupJobPipeline("bjob-1")); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("reconcile did not finish")
	}
	if runner.started.Load() != 1 {
		t.Fatalf("scheduler started %d times after the outage, want 1", runner.started.Load())
	}
}

func TestAPICreateBackupJob_PITRValidation(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()
	post := func(body string) (int, string) {
		resp, b := e.do(c, "POST", "/api/v1/backup-jobs", body, map[string]string{"X-CSRF-Token": csrf, "Content-Type": "application/json"})
		return resp.StatusCode, b
	}

	makeJob := func(method string, dbs []string) string {
		m := map[string]any{
			"name":          "pitr-job",
			"method":        method,
			"every_seconds": 3600,
			"bucket":        "central-backups",
			"source":        "src",
			"log_archive":   true,
			"conn": map[string]any{
				"host":         "127.0.0.1",
				"port":         3306,
				"user":         "root",
				"password_env": "PW",
				"databases":    dbs,
			},
		}
		b, _ := json.Marshal(m)
		return string(b)
	}

	// 1. mariadb-dump + log_archive rejected with 400
	{
		code, body := post(makeJob("mariadb-dump", []string{"db1"}))
		if code != 400 || !strings.Contains(body, "MariaDB does not support binlog archiving") {
			t.Fatalf("expected 400 MariaDB error, got: %d %s", code, body)
		}
	}

	// 2. pg_dump + log_archive rejected with 400
	{
		code, body := post(makeJob("pg_dump", []string{"db1"}))
		if code != 400 || !strings.Contains(body, "PITR needs pg_basebackup as the base backup") {
			t.Fatalf("expected 400 pg_dump error, got: %d %s", code, body)
		}
	}

	// 3. mysqldump + multi-db + log_archive rejected with 400
	{
		code, body := post(makeJob("mysqldump", []string{"db1", "db2"}))
		if code != 400 || !strings.Contains(body, "PITR requires exactly one database") {
			t.Fatalf("expected 400 multi-db error, got: %d %s", code, body)
		}
	}
}

// Jobs saved before the PITR rules existed (e.g. mariadb-dump with log_archive) must stay manageable: they
// are listed with a warning and can still be stopped, instead of every write failing validation.
func TestLegacyPITRJobStillStoppable(t *testing.T) {
	st := store.NewMem()
	legacy := backup.Job{ID: "legacy1", Name: "legacy", EverySeconds: 3600, Bucket: "central-backups", Enabled: true}
	legacy.Method, legacy.Source = backup.MethodMariaDBDump, "src"
	legacy.LogArchive = true
	legacy.Conn = backup.ConnSpec{Host: "127.0.0.1", Port: 3306, User: "root", PasswordEnv: "PW", Databases: []string{"db1"}}
	if err := st.CreateBackupJob(context.Background(), legacy); err != nil {
		t.Fatalf("stored legacy job unreadable/unwritable by the store: %v", err)
	}
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()
	resp, body := e.do(c, "GET", "/api/v1/backup-jobs", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, "MariaDB does not support binlog archiving") {
		t.Fatalf("list should flag the legacy job: %d %s", resp.StatusCode, body)
	}
	resp, body = e.do(c, "POST", "/api/v1/backup-jobs/legacy1/stop", "", map[string]string{"X-CSRF-Token": csrf})
	if resp.StatusCode >= 500 {
		t.Fatalf("stop of legacy PITR job failed: %d %s", resp.StatusCode, body)
	}
	j, _, _ := st.GetBackupJob(context.Background(), "legacy1")
	if j.Enabled {
		t.Fatal("legacy job still enabled after stop")
	}
}
