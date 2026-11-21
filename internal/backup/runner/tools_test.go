package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// fakeTool writes an executable script named name into dir that prints out.
func fakeTool(t *testing.T, dir, name, out string) {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\necho '"+out+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o755); err != nil {
		t.Fatal(err)
	}
}

func envOf(vars map[string]string) Getenv { return func(k string) string { return vars[k] } }

func TestToolsNeededPerMethod(t *testing.T) {
	cases := []struct {
		method          string
		backup, restore []string
		pitr            []string
		testRestore     []string
	}{
		{backup.MethodMySQLDump, []string{"mysqldump"}, []string{"mysql"}, []string{"mysql", "mysqlbinlog"}, []string{"mysql"}},
		{backup.MethodMariaDBDump, []string{"mariadb-dump"}, []string{"mysql"}, []string{"mysql", "mysqlbinlog"}, []string{"mysql"}},
		{backup.MethodMydumper, []string{"mydumper"}, nil, nil, nil},
		{backup.MethodPGDump, []string{"pg_dump"}, []string{"pg_restore"}, []string{"pg_restore"}, []string{"pg_restore"}},
		{backup.MethodPGBaseBackup, []string{"pg_basebackup"}, nil, nil, nil},
		{backup.MethodMongodump, []string{"mongodump"}, []string{"mongorestore"}, []string{"mongorestore"}, nil},
		{backup.MethodBinlog, nil, nil, nil, nil},
		{backup.MethodWAL, nil, nil, nil, nil},
		{backup.MethodOplog, nil, nil, nil, nil},
		{backup.MethodCustom, nil, nil, nil, nil},
	}
	for _, c := range cases {
		if got := BackupTools(c.method); !reflect.DeepEqual(got, c.backup) {
			t.Errorf("BackupTools(%s) = %v, want %v", c.method, got, c.backup)
		}
		if got := RestoreTools(c.method, false); !reflect.DeepEqual(got, c.restore) {
			t.Errorf("RestoreTools(%s) = %v, want %v", c.method, got, c.restore)
		}
		if got := RestoreTools(c.method, true); !reflect.DeepEqual(got, c.pitr) {
			t.Errorf("RestoreTools(%s, logs) = %v, want %v", c.method, got, c.pitr)
		}
		if got := TestRestoreTools(c.method); !reflect.DeepEqual(got, c.testRestore) {
			t.Errorf("TestRestoreTools(%s) = %v, want %v", c.method, got, c.testRestore)
		}
	}
	// Every tool a method needs is listed in KnownTools (the status page shows them all).
	known := map[string]bool{}
	for _, k := range KnownTools {
		known[k] = true
	}
	for _, c := range cases {
		for _, tl := range append(append(append(c.backup, c.restore...), c.pitr...), c.testRestore...) {
			if !known[tl] {
				t.Errorf("tool %s missing from KnownTools", tl)
			}
		}
	}
	j := backup.Job{JobOptions: backup.JobOptions{Method: backup.MethodPGDump, TestRestore: true}}
	if got := JobToolNeeds(j); len(got) != 2 || got[0].Tool != "pg_dump" || got[1].Tool != "pg_restore" || got[1].Purpose != ToolForTestRestore {
		t.Fatalf("JobToolNeeds = %+v", got)
	}
}

func TestCheckToolsUsesResolveToolLookup(t *testing.T) {
	empty := t.TempDir()
	err := CheckJobTools(envOf(map[string]string{EnvToolsDir: empty}), backup.Job{JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump}})
	var tm *ToolMissingError
	if !errors.As(err, &tm) || !reflect.DeepEqual(tm.Tools(), []string{"mysqldump"}) {
		t.Fatalf("err = %v", err)
	}
	msg := err.Error()
	for _, want := range []string{"tool_missing", "mysqldump", "STRAND_BACKUP_TOOLS_DIR", "Strand Web host/image"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q lacks %q", msg, want)
		}
	}
	if got := backup.NormalizeErrorCode(err); got != backup.ErrCodeToolMissing {
		t.Fatalf("NormalizeErrorCode = %q", got)
	}
	dir := t.TempDir()
	fakeTool(t, dir, "mysqldump", "mysqldump  Ver 8.4.3")
	if err := CheckJobTools(envOf(map[string]string{EnvToolsDir: dir}), backup.Job{JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump}}); err != nil {
		t.Fatal(err)
	}
	// Same permission rules as a run: a group-writable tool is refused.
	if err := os.Chmod(filepath.Join(dir, "mysqldump"), 0o775); err != nil {
		t.Fatal(err)
	}
	if err := CheckJobTools(envOf(map[string]string{EnvToolsDir: dir}), backup.Job{JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump}}); !errors.As(err, &tm) {
		t.Fatalf("group-writable tool accepted: %v", err)
	}
	// STRAND_WEB_BACKUP_COMMAND_DIR is searched when STRAND_BACKUP_TOOLS_DIR is unset.
	cmdDir := t.TempDir()
	fakeTool(t, cmdDir, "pg_dump", "pg_dump (PostgreSQL) 16.4")
	if err := CheckTools(envOf(map[string]string{EnvWebCommandDir: cmdDir}), BackupToolNeeds(backup.MethodPGDump)); err != nil {
		t.Fatal(err)
	}
	// Custom and log methods need no native tool.
	if err := CheckJobTools(envOf(map[string]string{EnvToolsDir: empty}), backup.Job{JobOptions: backup.JobOptions{Method: backup.MethodCustom}}); err != nil {
		t.Fatal(err)
	}
}

func TestToolsStatusReportsPathVersionAndMethods(t *testing.T) {
	dir := t.TempDir()
	fakeTool(t, dir, "mysql", "mysql  Ver 8.4.3 for Linux")
	st := ToolsStatus(context.Background(), envOf(map[string]string{EnvToolsDir: dir}))
	if len(st) != len(KnownTools)+len(OptionalTools()) {
		t.Fatalf("%d tools", len(st))
	}
	by := map[string]ToolStatus{}
	for _, s := range st {
		by[s.Name] = s
	}
	// Version-specific companions are listed as optional with their purpose; missing is not an error for jobs.
	for _, o := range OptionalTools() {
		if s := by[o.Name]; !s.Optional || s.Found || s.Purpose == "" {
			t.Fatalf("optional %s = %+v", o.Name, s)
		}
	}
	m := by["mysql"]
	if !m.Found || m.Path != filepath.Join(dir, "mysql") || m.Version != "mysql  Ver 8.4.3 for Linux" ||
		!reflect.DeepEqual(m.RestoreMethods, []string{"mariadb-dump", "mysqldump"}) {
		t.Fatalf("mysql = %+v", m)
	}
	d := by["mysqldump"]
	if d.Found || d.Path != "" || d.Error == "" || !reflect.DeepEqual(d.BackupMethods, []string{"mysqldump"}) {
		t.Fatalf("mysqldump = %+v", d)
	}
	if !reflect.DeepEqual(by["mysqlbinlog"].RestoreMethods, []string{"mariadb-dump", "mysqldump"}) {
		t.Fatalf("mysqlbinlog = %+v", by["mysqlbinlog"])
	}
	// The version is cached: a changed script with the same size/mtime is not re-probed; a new file is.
	fakeTool(t, dir, "mysql", "mysql  Ver 9.0.1 for Linux, a longer line")
	if v := ToolsStatus(context.Background(), envOf(map[string]string{EnvToolsDir: dir}))[2].Version; v != "mysql  Ver 9.0.1 for Linux, a longer line" {
		t.Fatalf("changed tool version = %q", v)
	}
}

// A missing native tool must fail the run before the repository is opened or a catalog record (restore
// point) is created.
func TestRunJobMissingToolCreatesNoRecord(t *testing.T) {
	st := store.NewMem()
	e := testEnv(t, map[string]string{EnvToolsDir: t.TempDir()}, st)
	touched := false
	e.Repo = func(ctx context.Context, id string) (backup.Repo, bool, error) {
		touched = true
		return backup.Repo{}, false, errors.New("repository must not be opened")
	}
	job := backup.Job{ID: "bjob-x", Name: "x", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "src",
		Repository: "repo1", Conn: backup.ConnSpec{Host: "db", Databases: []string{"app"}}}}
	_, err := e.RunJob(context.Background(), job)
	var tm *ToolMissingError
	if !errors.As(err, &tm) {
		t.Fatalf("err = %v, want ToolMissingError", err)
	}
	if touched {
		t.Fatal("repository opened although the tool is missing")
	}
	rs, err := e.Catalog.List(context.Background(), backup.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rs) != 0 {
		t.Fatalf("catalog records created: %+v", rs)
	}
}

// verify --test-restore without the restore tool: refused before any check is recorded (it used to record a
// failed test_restore check against a good backup).
func TestTestRestoreMissingToolRecordsNoCheck(t *testing.T) {
	st := store.NewMem()
	e := testEnv(t, map[string]string{EnvToolsDir: t.TempDir()}, st)
	ctx := context.Background()
	rec, _, err := e.Catalog.Create(ctx, backup.Record{ID: "bk-t1", Source: "src", Engine: "mysql", Tool: backup.MethodMySQLDump, JobID: "bjob-t", Bucket: "bkt-one", ObjectKey: "src/bk-t1/dump.sql.zst",
		ManifestURI: "s3://bkt-one/src/bk-t1/manifest.json", Method: backup.MethodMySQLDump, Kind: backup.KindFull, Status: backup.StatusQueued, CreatedAt: time.Now().Add(-time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range []backup.Status{backup.StatusUploading, backup.StatusVerifying} {
		if _, _, err := e.Catalog.UpdateStatus(ctx, rec.ID, st, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := e.Catalog.Finalize(ctx, rec.ID, backup.StatusVerifying, 10, strings.Repeat("a", 64), rec.ManifestURI, time.Now()); err != nil {
		t.Fatal(err)
	}
	job := backup.Job{ID: "bjob-t", Name: "t", JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "src", TestRestore: true}}
	_, err = e.TestRestore(ctx, job)
	var tm *ToolMissingError
	if !errors.As(err, &tm) {
		t.Fatalf("err = %v, want ToolMissingError", err)
	}
	checks, err := e.Catalog.Checks(ctx, rec.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) != 0 {
		t.Fatalf("checks recorded: %+v", checks)
	}
}
