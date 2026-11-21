package web

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/runner"
	"strand/internal/web/store"
)

// fakeToolsDir creates a directory with a fake executable for every native tool (TestMain points
// STRAND_BACKUP_TOOLS_DIR at it so built-in jobs pass the tool preflight).
func fakeToolsDir() (string, error) {
	dir, err := os.MkdirTemp("", "strand-web-tools-*")
	if err != nil {
		return "", err
	}
	for _, name := range runner.KnownTools {
		if err := writeFakeTool(dir, name); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func writeFakeTool(dir, name string) error {
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\necho '"+name+" fake 1.0'\n"), 0o755); err != nil {
		return err
	}
	return os.Chmod(p, 0o755)
}

func inlineJobBody(name string, enabled bool) map[string]any {
	return map[string]any{"name": name, "method": "mysqldump", "bucket": "central-backups", "enabled": enabled, "every_seconds": 3600,
		"source": name + "-src", "conn": map[string]any{"host": "db", "user": "u", "password_env": "PW", "databases": []string{"app"}}}
}

func assertToolMissing(t *testing.T, code int, out string, tool string) {
	t.Helper()
	m := decode(t, out)
	msg, _ := m["error"].(string)
	if code != 409 || m["code"] != "tool_missing" || !strings.Contains(msg, tool) || !strings.Contains(msg, "STRAND_BACKUP_TOOLS_DIR") ||
		!strings.Contains(msg, "Strand Web host/image") {
		t.Fatalf("want 409 tool_missing naming %s, got %d %s", tool, code, out)
	}
	if tools, _ := m["tools"].([]any); len(tools) == 0 || tools[0] != tool {
		t.Fatalf("tools = %v", m["tools"])
	}
}

// An enabled job whose native tool is missing is never created, enabled, resumed or run; a disabled one may
// be saved (with a warning) and becomes runnable once the tool is installed.
func TestBackupJobRefusedWithoutNativeTool(t *testing.T) {
	tools := t.TempDir()
	t.Setenv(runner.EnvToolsDir, tools)
	e := newCPEnv(t)
	a := e.admin()

	code, out := a.do("POST", "/api/v1/backup-jobs", inlineJobBody("enabled-no-tool", true))
	assertToolMissing(t, code, out, "mysqldump")
	if jobs, _ := e.st.ListBackupJobs(context.Background()); len(jobs) != 0 {
		t.Fatalf("job stored although refused: %+v", jobs)
	}

	out = a.must("POST", "/api/v1/backup-jobs", inlineJobBody("prepared", false), 201)
	res := decode(t, out)
	id := res["job"].(map[string]any)["id"].(string)
	if w, _ := res["warnings"].([]any); len(w) == 0 || !strings.Contains(w[0].(string), "tool_missing") {
		t.Fatalf("disabled job saved without a tool warning: %s", out)
	}

	code, out = a.do("PUT", "/api/v1/backup-jobs/"+id, inlineJobBody("prepared", true))
	assertToolMissing(t, code, out, "mysqldump")
	code, out = a.do("POST", "/api/v1/backup-jobs/"+id+"/start", nil)
	assertToolMissing(t, code, out, "mysqldump")
	code, out = a.do("POST", "/api/v1/backup-jobs/"+id+"/run", nil)
	assertToolMissing(t, code, out, "mysqldump")
	if j, _, _ := e.st.GetBackupJob(context.Background(), id); j.Enabled {
		t.Fatal("job enabled although its tool is missing")
	}
	if specs := e.rec.take(); len(specs) != 0 {
		t.Fatalf("processes started: %+v", specs)
	}

	// The job list, the job API and the pages show the missing tool and disable Run/Resume.
	list := a.must("GET", "/api/v1/backup-jobs", nil, 200)
	if !strings.Contains(list, `"tools_missing"`) || !strings.Contains(list, `"tool_error"`) {
		t.Fatalf("job list lacks the tool status: %s", list)
	}
	if got := a.must("GET", "/api/v1/backup-jobs/"+id, nil, 200); !strings.Contains(got, `"tools_missing"`) {
		t.Fatalf("job lacks the tool status: %s", got)
	}
	for _, p := range []string{"/backups/jobs", "/backups/jobs/" + id} {
		_, page := a.do("GET", p, nil)
		if !strings.Contains(page, "tool missing") || !strings.Contains(page, `data-job-action="run" data-id="`+id+`" disabled`) ||
			!strings.Contains(page, `data-job-action="resume" data-id="`+id+`" disabled`) {
			t.Fatalf("%s does not show the missing tool / disabled actions", p)
		}
	}

	// Installing the tool makes the job runnable.
	if err := writeFakeTool(tools, "mysqldump"); err != nil {
		t.Fatal(err)
	}
	a.must("POST", "/api/v1/backup-jobs/"+id+"/start", nil, 200)
	a.must("POST", "/api/v1/backup-jobs/"+id+"/run", nil, 202)
	if specs := e.rec.take(); len(specs) != 2 {
		t.Fatalf("processes started = %d, want 2", len(specs))
	}
	if _, page := a.do("GET", "/backups/jobs", nil); strings.Contains(page, "tool missing") {
		t.Fatal("job list still shows the missing tool")
	}
}

// Test restore needs the restore tool: an enabled job with test restore is refused without it.
func TestBackupJobTestRestoreNeedsRestoreTool(t *testing.T) {
	tools := t.TempDir()
	if err := writeFakeTool(tools, "pg_dump"); err != nil {
		t.Fatal(err)
	}
	t.Setenv(runner.EnvToolsDir, tools)
	err := jobToolError(backup.Job{Enabled: true, JobOptions: backup.JobOptions{Method: backup.MethodPGDump, TestRestore: true}})
	if !isToolMissing(err) || !strings.Contains(err.Error(), "pg_restore") || strings.Contains(err.Error(), "pg_dump not found") {
		t.Fatalf("err = %v", err)
	}
	if err := runToolError(backup.Job{JobOptions: backup.JobOptions{Method: backup.MethodPGDump, TestRestore: true}}); err != nil {
		t.Fatalf("Run now needs only the backup tool: %v", err)
	}
}

// At boot and in the reconcile loop a job whose tool is missing is not started and not retried.
func TestSchedulerSkipsJobWithoutNativeTool(t *testing.T) {
	t.Setenv(runner.EnvToolsDir, t.TempDir())
	old := reconcileDelay
	reconcileDelay.first, reconcileDelay.max = time.Millisecond, 2*time.Millisecond
	defer func() { reconcileDelay = old }()
	mem := store.NewMem()
	j := backup.Job{ID: "bjob-nt", Name: "nt", EverySeconds: 3600, Bucket: "central-backups", Enabled: true,
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "s", Conn: backup.ConnSpec{Host: "db", Databases: []string{"app"}}}}
	if err := mem.CreateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	rec := &startRecorder{backupStopTestRunner: backupStopTestRunner{jobs: map[string]JobStatus{}}}
	s := &Server{cfg: &Config{}, db: &metaDB{st: mem}, jobs: rec, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan struct{})
	go func() { s.reconcileBackupSchedule(backupJobPipeline(j.ID)); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile kept retrying a job whose tool is missing")
	}
	if n := rec.started.Load(); n != 0 {
		t.Fatalf("reconcile started the scheduler %d time(s)", n)
	}

	e := newCPEnv(t)
	if err := e.st.CreateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	if err := e.srv.resumeBackupJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if specs := e.rec.take(); len(specs) != 0 {
		t.Fatalf("boot resume started %d process(es)", len(specs))
	}
	e.srv.reconcileMu.Lock()
	n := len(e.srv.reconciling)
	e.srv.reconcileMu.Unlock()
	if n != 0 {
		t.Fatalf("boot resume left %d reconcile loop(s)", n)
	}
}

// A restore is refused before its process starts when the restore tool of the backup's method is missing.
func TestRestoreRefusedWithoutRestoreTool(t *testing.T) {
	tools := t.TempDir()
	t.Setenv(runner.EnvToolsDir, tools)
	e := newP56Env(t, nil)
	a := e.admin()
	seedComplete(t, e.st, "bk-nt", "j", e.srv.now().UTC().Add(-time.Hour), backup.KindFull)
	req := map[string]any{"backup_id": "bk-nt", "connection_id": restoreConn(t, e.st, "mysql"), "target_database": "r1", "confirm": "r1"}
	code, out := a.do("POST", "/api/v1/backup-restores", req)
	assertToolMissing(t, code, out, "mysql")
	if specs := e.rec.take(); len(specs) != 0 {
		t.Fatalf("restore process started: %+v", specs)
	}
	if err := writeFakeTool(tools, "mysql"); err != nil {
		t.Fatal(err)
	}
	a.must("POST", "/api/v1/backup-restores", req, 202)
}

// GET /api/v1/backup-tools and the Backup tools page are admin only and report every tool.
func TestBackupToolsAPIAndPage(t *testing.T) {
	tools := t.TempDir()
	if err := writeFakeTool(tools, "pg_dump"); err != nil {
		t.Fatal(err)
	}
	t.Setenv(runner.EnvToolsDir, tools)
	e := newCPEnv(t)
	a := e.admin()
	res := decode(t, a.must("GET", "/api/v1/backup-tools", nil, 200))
	list := res["tools"].([]any)
	if len(list) != len(runner.KnownTools) {
		t.Fatalf("%d tools", len(list))
	}
	by := map[string]map[string]any{}
	for _, x := range list {
		m := x.(map[string]any)
		by[m["name"].(string)] = m
	}
	if pg := by["pg_dump"]; pg["found"] != true || pg["path"] != filepath.Join(tools, "pg_dump") || pg["version"] != "pg_dump fake 1.0" {
		t.Fatalf("pg_dump = %v", pg)
	}
	if md := by["mysqldump"]; md["found"] != false || md["path"] != nil || md["backup_methods"].([]any)[0] != "mysqldump" {
		t.Fatalf("mysqldump = %v", md)
	}
	_, page := a.do("GET", "/backups/tools", nil)
	if !strings.Contains(page, `id="bk-tools-table"`) || !strings.Contains(page, filepath.Join(tools, "pg_dump")) || !strings.Contains(page, "pg_dump fake 1.0") {
		t.Fatal("tools page lacks the tool status")
	}
	if _, conns := a.do("GET", "/backups/connections", nil); !strings.Contains(sectionTabsHTML(conns), `href="/backups/tools"`) {
		t.Fatal("Backup resources tabs lack Backup tools")
	}
	// The wizard carries the tool status of every method.
	if _, wiz := a.do("GET", "/backups/jobs/new", nil); !strings.Contains(wiz, "method_tools") || !strings.Contains(wiz, `id="wiz-tool-status"`) {
		t.Fatal("job wizard lacks the tool status")
	}
	for _, role := range []store.Role{store.RoleViewer, store.RoleOperator} {
		cl := e.roleClient(role)
		for _, p := range []string{"/api/v1/backup-tools", "/backups/tools"} {
			if code, _ := cl.do("GET", p, nil); code != 403 {
				t.Fatalf("%s GET %s = %d, want 403", role, p, code)
			}
		}
		if _, conns := cl.do("GET", "/backups/connections", nil); strings.Contains(sectionTabsHTML(conns), `href="/backups/tools"`) {
			t.Fatalf("%s sees the admin-only Backup tools tab", role)
		}
	}
}

func sectionTabsHTML(page string) string {
	i := strings.Index(page, `class="tabs sectiontabs"`)
	if i < 0 {
		return ""
	}
	return page[i : i+strings.Index(page[i:], `</nav>`)]
}
