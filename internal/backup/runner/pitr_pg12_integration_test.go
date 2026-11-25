package runner

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// PostgreSQL 12 ends recovery and promotes without an error when the WAL runs out before
// recovery_target_time (PostgreSQL 13+ refuses to start). A point-in-time restore of a PostgreSQL 12
// base backup must prove from the archived WAL that recovery will stop at the target, and refuse a
// target the archive does not reach; a reachable target still recovers to exactly the earlier rows.
func TestPITRPostgres12RefusesUnreachedTarget(t *testing.T) {
	dir, tools, vars := pitrSetup(t)
	port := freePort(t)
	pgc := startContainer(t, "pg12", dir, "-e", "POSTGRES_PASSWORD=pgpw", "postgres:12", "-p", fmt.Sprint(port),
		"-c", "wal_level=replica", "-c", "max_wal_senders=5", "-c", "listen_addresses=127.0.0.1")
	dsn := fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", port)
	db := waitSQL(t, "pgx", dsn)
	time.Sleep(2 * time.Second) // the entrypoint restarts the server once after init
	db = waitSQL(t, "pgx", dsn)
	vars["PG_BACKUP_PW"] = "pgpw"
	for k, v := range vars {
		t.Setenv(k, v)
	}
	wrapper(t, tools, "pg_basebackup", pgc, "pg_basebackup", "PGPASSWORD")
	mustExec(t, db, "CREATE TABLE items (id int primary key)")
	mustExec(t, db, "INSERT INTO items SELECT generate_series(1, 100)")
	e := testEnv(t, vars, store.NewMem())
	job := backup.Job{ID: "bjob-pg12pitr", Name: "pg12 pitr", EverySeconds: 3600, Bucket: "central-backups",
		JobOptions: backup.JobOptions{Method: backup.MethodPGBaseBackup, Source: "pg12-prod", Compress: true, Encrypt: true, LogArchive: true,
			Conn: backup.ConnSpec{Host: "127.0.0.1", Port: port, User: "postgres", PasswordEnv: "PG_BACKUP_PW"}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	archErr := make(chan error, 1)
	go func() { archErr <- e.ArchiveWAL(ctx, job) }()
	time.Sleep(2 * time.Second)
	base, err := e.RunJob(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, "INSERT INTO items SELECT generate_series(101, 200)")
	time.Sleep(1100 * time.Millisecond)
	target := time.Now().UTC()
	time.Sleep(1100 * time.Millisecond)
	mustExec(t, db, "INSERT INTO items SELECT generate_series(201, 300)")
	mustExec(t, db, "SELECT pg_switch_wal()")
	waitLogs(t, e, job.ID, target.Add(time.Second))
	all, _ := e.Catalog.ListAll(context.Background(), backup.Filter{Source: job.Source, Status: backup.StatusComplete}, 0)

	// A target after the last archived transaction: PostgreSQL 12 would silently promote at the end of
	// the archive (all 300 rows) instead of failing.
	unreachable := time.Now().UTC().Add(time.Hour)
	provisional, err := PlanRestore(all, &base, &unreachable)
	if err != nil || !provisional.VerifyOnReplay {
		t.Fatalf("plan: %v", err)
	}
	unreachedDir := filepath.Join(dir, "pg12-unreached")
	err = e.Restore(context.Background(), provisional, RestoreTarget{Dir: unreachedDir})
	if err == nil || !strings.Contains(err.Error(), "does not reach the target time") {
		t.Fatalf("PostgreSQL 12 restore to a target beyond the archive: want a refusal, got %v", err)
	}
	if _, err := os.Stat(unreachedDir); !os.IsNotExist(err) {
		t.Fatalf("refused restore published its data directory: %v", err)
	}

	plan, err := PlanRestore(all, nil, &target)
	if err != nil || plan.Base().ID != base.ID {
		t.Fatalf("plan %+v %v", plan, err)
	}
	data := filepath.Join(dir, "pg12-restored")
	if err := e.Restore(context.Background(), plan, RestoreTarget{Dir: data}); err != nil {
		t.Fatal(err)
	}
	restoredPort := freePort(t)
	name := startContainer(t, "pg12restore", dir, "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-v", data+":/pgdata",
		"-e", "PGDATA=/pgdata", "postgres:12", "postgres", "-D", "/pgdata", "-p", fmt.Sprint(restoredPort), "-c", "listen_addresses=127.0.0.1")
	rdb := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", restoredPort))
	deadline := time.Now().Add(time.Minute)
	for {
		var inRecovery bool
		if err := rdb.QueryRow("SELECT pg_is_in_recovery()").Scan(&inRecovery); err == nil && !inRecovery {
			break
		}
		if time.Now().After(deadline) {
			out, _ := exec.Command("docker", "logs", "--tail", "30", name).CombinedOutput()
			t.Fatalf("restored server did not promote:\n%s", out)
		}
		time.Sleep(time.Second)
	}
	var n int
	if err := rdb.QueryRow("SELECT count(*) FROM items").Scan(&n); err != nil || n != 200 {
		t.Fatalf("restored rows %d (want 200): %v", n, err)
	}
	out, _ := exec.Command("docker", "logs", name).CombinedOutput()
	if !strings.Contains(string(out), "recovery stopping before commit") {
		t.Fatalf("recovery did not stop at the target:\n%s", out)
	}
	cancel()
	if err := <-archErr; err != nil {
		t.Fatalf("archiver: %v", err)
	}
}
