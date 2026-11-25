package runner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// DROP DATABASE ... WITH (FORCE) only exists from PostgreSQL 13. On PostgreSQL 12 a test restore must
// still drop its scratch database, and a failed restore the database it created, even while another
// session is connected to it.
func TestPGRestoreCleanupOnPostgreSQL12(t *testing.T) {
	requireDocker(t)
	dir := shared(t)
	vars := startMinIO(t, dir, "pg-cleanup")
	vars["STRAND_BACKUP_TMPDIR"] = dir
	vars["PG_PW"] = "pgpw"
	port := freePort(t)
	c := startContainer(t, "pg12", dir, "-e", "POSTGRES_PASSWORD=pgpw", "postgres:12", "-p", fmt.Sprint(port), "-c", "listen_addresses=127.0.0.1")
	dsn := func(db string) string {
		return fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/%s?sslmode=disable", port, db)
	}
	db := waitSQL(t, "pgx", dsn("postgres"))
	time.Sleep(2 * time.Second) // the entrypoint restarts the server once after init
	db = waitSQL(t, "pgx", dsn("postgres"))
	mustExec(t, db, "CREATE DATABASE app")
	app := waitSQL(t, "pgx", dsn("app"))
	mustExec(t, app, "CREATE TABLE t (id serial primary key, v text)")
	mustExec(t, app, "INSERT INTO t (v) SELECT 'v' || g FROM generate_series(1, 25) g")

	tools, failing := filepath.Join(dir, "tools"), filepath.Join(dir, "tools-failing")
	for _, d := range []string{tools, failing} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
		wrapper(t, d, "pg_dump", c, "pg_dump", "PGPASSWORD")
	}
	wrapper(t, tools, "pg_restore", c, "pg_restore", "PGPASSWORD")
	// Loads the archive, leaves a session connected to the new database, then fails.
	failRestore := fmt.Sprintf(`#!/bin/sh
docker exec -i -e PGPASSWORD %[1]s pg_restore "$@" || exit 1
docker exec -d -e PGPASSWORD %[1]s psql -h 127.0.0.1 -p %[2]d -U postgres -d r_fail -c "SELECT pg_sleep(300)"
sleep 2
echo "simulated failure after the load" >&2
exit 1
`, c, port)
	if err := os.WriteFile(filepath.Join(failing, "pg_restore"), []byte(failRestore), 0o755); err != nil {
		t.Fatal(err)
	}

	e := testEnv(t, vars, store.NewMem())
	conn := backup.ConnSpec{Host: "127.0.0.1", Port: port, User: "postgres", PasswordEnv: "PG_PW"}
	job := backup.Job{ID: "bjob-pg12", Name: "pg12", EverySeconds: 3600, Bucket: "pg-cleanup",
		JobOptions: backup.JobOptions{Method: backup.MethodPGDump, Source: "pg12", Conn: conn, Scratch: conn}}
	job.Conn.Databases = []string{"app"}
	vars[EnvToolsDir] = tools
	ctx := context.Background()
	rec, err := e.RunJob(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	databases := func(like string) []string {
		rows, err := db.Query("SELECT datname FROM pg_database WHERE datname LIKE $1", like)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var n string
			if err := rows.Scan(&n); err != nil {
				t.Fatal(err)
			}
			out = append(out, n)
		}
		return out
	}

	res, err := e.TestRestore(ctx, job)
	if err != nil || res.Status != backup.CheckOK || res.Metrics["rows"] != "25" {
		t.Fatalf("test restore: %+v %v", res, err)
	}
	if left := databases("strand_verify_%"); len(left) != 0 {
		t.Fatalf("test restore on PostgreSQL 12 left its scratch database: %v", left)
	}

	vars[EnvToolsDir] = failing
	err = e.Restore(ctx, RestorePlan{Chain: []backup.Record{rec}}, RestoreTarget{Conn: conn, Database: "r_fail"})
	if err == nil || !strings.Contains(err.Error(), "simulated failure") {
		t.Fatalf("failing restore: %v", err)
	}
	if strings.Contains(err.Error(), "cleanup failed") {
		t.Fatalf("cleanup reported a failure: %v", err)
	}
	if left := databases("r_fail"); len(left) != 0 {
		t.Fatalf("failed restore on PostgreSQL 12 left the database it created (error: %v)", err)
	}
}
