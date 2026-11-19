package web

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/web/store"
)

// File mode installation -> `strand web db migrate` (dry run, real, re-run) -> web in DB mode sees everything
// -> `strand web db export` gives back a file-mode tree with the same content.
func TestDBMigrateVerifyAndExport(t *testing.T) {
	cfgDir := t.TempDir()
	e := newEnv(t, func(e *testEnv, c *Config) { c.ConfigDir = cfgDir })
	c, csrf := e.loginOK()
	e.setExit(0)
	if code, _, _, out := createNP(t, e, c, csrf, newOpts("filepipe", "SrcSecretPw", "DstSecretPw")); code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	if code, body := e.act(c, csrf, "orders", "verify", map[string]string{"mode": "full"}); code != 200 {
		t.Fatalf("verify: %d %v", code, body)
	}
	waitFor(t, "job done", func() bool {
		for _, j := range e.srv.jobs.List("") {
			if j.Running {
				return false
			}
		}
		return true
	})
	cfg := e.srv.cfg
	st := store.NewMem()
	box, _ := store.NewBox(dbKey)
	ctx := context.Background()
	var out strings.Builder

	rep, err := migrateWith(ctx, st, cfg, box, MigrateOptions{DryRun: true, Out: &out}, func() (bool, string, error) { return false, "", nil })
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, it := range rep.Items {
		if it.Status != "new" {
			t.Errorf("dry run: %s %s is %s", it.Kind, it.Name, it.Status)
		}
		kinds[it.Kind]++
	}
	if kinds["admin"] != 1 || kinds["pipeline"] != 2 || kinds["job"] != 1 || kinds["job_output"] != 1 || kinds["audit"] != 1 {
		t.Fatalf("plan kinds = %v\n%s", kinds, out.String())
	}
	if n, _ := st.CountAudit(ctx); n != 0 {
		t.Fatal("dry run wrote to the database")
	}

	out.Reset()
	rep, err = migrateWith(ctx, st, cfg, box, MigrateOptions{Out: &out}, nil)
	if err != nil || !rep.Verified {
		t.Fatalf("migrate: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "verified:") {
		t.Fatalf("report:\n%s", out.String())
	}
	auditLines, _ := countAuditLines(filepath.Join(cfg.StateDir, "audit.log"))
	if n, _ := st.CountAudit(ctx); int(n) != auditLines {
		t.Fatalf("audit rows %d, file lines %d", n, auditLines)
	}
	row, _, _ := st.GetPipeline(ctx, "filepipe")
	if row.Origin != store.OriginCreated || len(row.EnvEnc) == 0 {
		t.Fatalf("created pipeline row: %+v", row)
	}
	if row, _, _ := st.GetPipeline(ctx, "orders"); row.Origin != store.OriginFile {
		t.Fatalf("declared pipeline origin %q", row.Origin)
	}
	// Re-run: nothing new, still verified, no duplicate audit rows.
	out.Reset()
	if rep, err = migrateWith(ctx, st, cfg, box, MigrateOptions{Out: &out}, nil); err != nil || !rep.Verified {
		t.Fatalf("re-run: %v\n%s", err, out.String())
	}
	for _, it := range rep.Items {
		if it.Status != "present" {
			t.Errorf("after re-run: %s %s is %s", it.Kind, it.Name, it.Status)
		}
	}
	if n, _ := st.CountAudit(ctx); int(n) != auditLines {
		t.Fatalf("re-run duplicated audit rows: %d", n)
	}

	// Web in DB mode on the migrated database: no second import, pipelines and job output there.
	c2 := *cfg
	srv, err := New(Options{Config: &c2, Version: "test", Commander: e.cmd, Store: st, SecretKey: dbKey})
	if err != nil {
		t.Fatal(err)
	}
	if srv.reg.Get("filepipe") == nil || srv.reg.Get("orders") == nil {
		t.Fatal("pipelines missing in DB mode")
	}
	jobs := srv.jobs.List("orders")
	if len(jobs) != 1 {
		t.Fatalf("jobs in DB mode: %d", len(jobs))
	}
	b, _ := st.ReadJobOutput(ctx, jobs[0].ID, 0, 1<<20)
	if !strings.Contains(string(b), "line two") {
		t.Fatalf("job output not migrated: %q", b)
	}

	// Export back to files.
	dir := filepath.Join(t.TempDir(), "export")
	out.Reset()
	if err := exportWith(ctx, st, cfg, box, ExportOptions{Dir: dir, Out: &out}); err != nil {
		t.Fatal(err)
	}
	wy, err := LoadConfig(filepath.Join(dir, "web.yaml"))
	if err != nil || wy.PasswordHash != cfg.PasswordHash || wy.DBEnabled() {
		t.Fatalf("exported web.yaml: %v %+v", err, wy)
	}
	env, _ := os.ReadFile(filepath.Join(dir, "pipelines", "filepipe.env"))
	if !strings.Contains(string(env), "SrcSecretPw") {
		t.Fatal(".env not exported decrypted")
	}
	var doc createdFileDoc
	pj, _ := os.ReadFile(filepath.Join(dir, "state", CreatedFile))
	if json.Unmarshal(pj, &doc) != nil || len(doc.Pipelines) != 1 || doc.Pipelines[0].Name != "filepipe" {
		t.Fatalf("pipelines.json: %s", pj)
	}
	lb, _ := os.ReadFile(filepath.Join(dir, "state", "jobs", jobs[0].ID+".log"))
	if string(lb) != string(b) {
		t.Fatal("job log differs")
	}
	f, _ := os.Open(filepath.Join(dir, "state", "audit.log"))
	defer f.Close()
	n := 0
	for sc := bufio.NewScanner(f); sc.Scan(); n++ {
		var a AuditEntry
		if json.Unmarshal(sc.Bytes(), &a) != nil || a.Action == "" {
			t.Fatalf("audit line %d not an entry: %s", n, sc.Text())
		}
	}
	if total, _ := st.CountAudit(ctx); int64(n) != total {
		t.Fatalf("exported %d audit lines, database has %d", n, total)
	}
	if err := exportWith(ctx, st, cfg, box, ExportOptions{Dir: dir}); err == nil {
		t.Fatal("export into a non-empty directory accepted")
	}
}
