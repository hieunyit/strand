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

// TestPGDumpRestoreOlderServers runs Strand's own backup and restore (RunJob, Restore) of PostgreSQL 15 and
// 16 sources into 15, 16 and 17 targets with real clients: the default pg_dump/pg_restore are 17+ (the
// Web image's when STRAND_TEST_TOOLS_IMAGE is set, e.g. strand:local; else postgres:18's), the legacy
// ones pg_dump-16/pg_restore-16 (the image's, else postgres:16's). A restore into a server <= 16 must pick
// pg_restore-16 and succeed with --exit-on-error; an archive written by pg_dump 17+ must be refused for
// such a target with a clear error, and still restore into 17.
func TestPGDumpRestoreOlderServers(t *testing.T) {
	requireDocker(t)
	dir := shared(t)
	vars := startMinIO(t, dir, "pg-compat")
	vars["STRAND_BACKUP_TMPDIR"] = dir
	vars["PG_PW"] = "pgpw"

	// Tool containers (long-running, shared dir mounted) and docker-exec wrappers named like the tools.
	toolBox := func(name, image string, entrypoint bool) string {
		args := []string{"--user", "root"}
		if entrypoint {
			args = append(args, "--entrypoint", "sleep", image, "infinity")
		} else {
			args = append(args, image, "sleep", "infinity")
		}
		return startContainer(t, name, dir, args...)
	}
	full := filepath.Join(dir, "tools-full")
	noLegacy := filepath.Join(dir, "tools-nolegacy")
	for _, d := range []string{full, noLegacy} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	sources := []int{15, 16}
	if img := os.Getenv("STRAND_TEST_TOOLS_IMAGE"); img != "" {
		box := toolBox("tools", img, true)
		for _, d := range []string{full, noLegacy} {
			wrapper(t, d, "pg_dump", box, "pg_dump", "PGPASSWORD")
			wrapper(t, d, "pg_restore", box, "pg_restore", "PGPASSWORD")
		}
		wrapper(t, full, "pg_dump-16", box, "pg_dump-16", "PGPASSWORD")
		wrapper(t, full, "pg_restore-16", box, "pg_restore-16", "PGPASSWORD")
	} else {
		box18, box16 := toolBox("tools18", "postgres:18", false), toolBox("tools16", "postgres:16", false)
		for _, d := range []string{full, noLegacy} {
			wrapper(t, d, "pg_dump", box18, "pg_dump", "PGPASSWORD")
			wrapper(t, d, "pg_restore", box18, "pg_restore", "PGPASSWORD")
		}
		wrapper(t, full, "pg_dump-16", box16, "pg_dump", "PGPASSWORD")
		wrapper(t, full, "pg_restore-16", box16, "pg_restore", "PGPASSWORD")
		// A postgres:16 client older (minor) than the postgres:15 server can dump server-internal column
		// options the 16 server rejects (seen with 16.15 vs 15.20): without the image, 16 sources only.
		sources = []int{16}
	}

	servers := map[int]int{} // major -> port
	for _, major := range []int{15, 16, 17} {
		port := freePort(t)
		startContainer(t, fmt.Sprintf("pg%d", major), dir, "-e", "POSTGRES_PASSWORD=pgpw", fmt.Sprintf("postgres:%d", major),
			"-p", fmt.Sprint(port), "-c", "listen_addresses=127.0.0.1")
		db := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", port))
		time.Sleep(2 * time.Second) // the entrypoint restarts the server once after init
		db = waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/postgres?sslmode=disable", port))
		mustExec(t, db, "CREATE DATABASE app")
		app := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/app?sslmode=disable", port))
		mustExec(t, app, "CREATE TABLE t (id serial primary key, v text)")
		mustExec(t, app, "INSERT INTO t (v) SELECT 'v' || g FROM generate_series(1, 25) g")
		servers[major] = port
	}
	conn := func(major int, dbs ...string) backup.ConnSpec {
		return backup.ConnSpec{Host: "127.0.0.1", Port: servers[major], User: "postgres", PasswordEnv: "PG_PW", Databases: dbs}
	}
	ctx := context.Background()
	e := testEnv(t, vars, store.NewMem()) // its getenv reads vars on each call: EnvToolsDir is switched below
	backupOf := func(t *testing.T, tools string, major int) backup.Record {
		t.Helper()
		vars[EnvToolsDir] = tools
		rec, err := e.RunJob(ctx, backup.Job{ID: fmt.Sprintf("bjob-pg%d", major), Name: "pgdump", EverySeconds: 3600, Bucket: "pg-compat",
			JobOptions: backup.JobOptions{Method: backup.MethodPGDump, Source: fmt.Sprintf("pg%d", major), Conn: conn(major, "app")}})
		if err != nil || rec.Status != backup.StatusComplete {
			t.Fatalf("pg_dump of PostgreSQL %d: %v %v", major, rec.Status, err)
		}
		return rec
	}
	restore := func(t *testing.T, rec backup.Record, major int, name string) error {
		t.Helper()
		vars[EnvToolsDir] = full
		if err := e.Restore(ctx, RestorePlan{Chain: []backup.Record{rec}}, RestoreTarget{Conn: conn(major), Database: name}); err != nil {
			return err
		}
		db := waitSQL(t, "pgx", fmt.Sprintf("postgres://postgres:pgpw@127.0.0.1:%d/%s?sslmode=disable", servers[major], name))
		var n int
		if err := db.QueryRow("SELECT count(*) FROM t").Scan(&n); err != nil || n != 25 {
			t.Fatalf("restored rows into PostgreSQL %d: %d %v", major, n, err)
		}
		return nil
	}

	for _, src := range sources {
		rec := backupOf(t, full, src)
		dsts := []int{src, 17}
		if src < 16 {
			dsts = append(dsts, 16)
		}
		for _, dst := range dsts {
			if err := restore(t, rec, dst, fmt.Sprintf("r%d_%d", src, dst)); err != nil {
				t.Fatalf("restore PostgreSQL %d backup into PostgreSQL %d: %v", src, dst, err)
			}
		}
	}
	// A backup of 16 taken without pg_dump-16 (archive 1.16): refused for 16 with a clear error, fine for 17.
	rec := backupOf(t, noLegacy, 16)
	if err := restore(t, rec, 16, "r_new_16"); err == nil || !strings.Contains(err.Error(), "written by pg_dump 17 or newer") {
		t.Fatalf("restore of a pg_dump 17+ archive into PostgreSQL 16: want a clear error, got %v", err)
	}
	if err := restore(t, rec, 17, "r_new_17"); err != nil {
		t.Fatalf("restore of a pg_dump 17+ archive into PostgreSQL 17: %v", err)
	}
}
