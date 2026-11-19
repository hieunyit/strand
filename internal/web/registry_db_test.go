package web

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/web/store"
)

// DB mode: a pipeline declared in web.yaml is registered in the database (config captured), an operator's
// on-disk edit is never overwritten silently (set aside + banner + adopt), and a vanished web.yaml entry is
// flagged, not deleted.
func TestDeclaredPipelineRegisteredInDatabase(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	ctx := context.Background()
	row, ok, _ := st.GetPipeline(ctx, "orders")
	p := e.srv.reg.Get("orders")
	disk, _ := os.ReadFile(p.ConfigPath)
	if !ok || row.Origin != store.OriginFile || row.ConfigYAML != string(disk) {
		t.Fatalf("declared pipeline not registered: ok=%v origin=%q", ok, row.Origin)
	}
	c, csrf := e.loginOK()
	e.setExit(0)

	// Hand edit on disk, then an action that runs strand: DB copy wins, disk version set aside, banner shown.
	hand := string(disk) + "\n# hand edit\n"
	must0(t, os.WriteFile(p.ConfigPath, []byte(hand), 0o640))
	if code, body := e.act(c, csrf, "orders", "verify", map[string]string{"mode": "full"}); code != 200 {
		t.Fatalf("verify: %d %v", code, body)
	}
	if b, _ := os.ReadFile(p.ConfigPath); string(b) != string(disk) {
		t.Fatal("database copy not restored")
	}
	row, _, _ = st.GetPipeline(ctx, "orders")
	cs := parseConflicts(row.Conflicts)
	if len(cs) != 1 || cs[0].File != "config" {
		t.Fatalf("conflicts = %q", row.Conflicts)
	}
	if b, err := os.ReadFile(cs[0].Path); err != nil || string(b) != hand {
		t.Fatalf("disk version not set aside at %s: %v", cs[0].Path, err)
	}
	e.srv.reg.Refresh(ctx)
	_, page := e.do(c, "GET", "/pipelines/orders", "", nil)
	if !strings.Contains(page, "differed from the database copy") || !strings.Contains(page, cs[0].Path) {
		t.Fatal("conflict banner missing")
	}
	// Adopt: validated, then stored in the DB and on disk; conflict cleared.
	waitFor(t, "verify job done", func() bool {
		for _, j := range e.srv.jobs.List("orders") {
			if j.Running {
				return false
			}
		}
		return true
	})
	if code, body := e.act(c, csrf, "orders", "conflict-adopt", map[string]string{"path": cs[0].Path}); code != 200 {
		t.Fatalf("adopt: %d %v", code, body)
	}
	row, _, _ = st.GetPipeline(ctx, "orders")
	if row.ConfigYAML != hand || row.Conflicts != "[]" {
		t.Fatalf("after adopt: yaml hand=%v conflicts=%s", row.ConfigYAML == hand, row.Conflicts)
	}
	if b, _ := os.ReadFile(p.ConfigPath); string(b) != hand {
		t.Fatal("adopted version not on disk")
	}

	// web.yaml entry removed: pipeline kept and flagged; not re-materialised; restore and forget.
	c2 := *e.srv.cfg
	c2.Pipelines = nil
	e.srv.releaseLease() // web restarts with the edited web.yaml
	srv, err := New(Options{Config: &c2, Version: "test", Commander: e.cmd, Now: e.clock.Now, Store: st})
	if err != nil {
		t.Fatal(err)
	}
	q := srv.reg.Get("orders")
	if q == nil || !strings.Contains(q.Missing, "web.yaml") {
		t.Fatalf("vanished entry not flagged: %+v", q)
	}
	if row, _, _ := st.GetPipeline(ctx, "orders"); row.MissingSince.IsZero() {
		t.Fatal("missing_since not recorded")
	}
	must0(t, os.Remove(p.ConfigPath))
	srv.materialize("orders")
	if fileExists(p.ConfigPath) {
		t.Fatal("missing pipeline re-materialised automatically")
	}
	if _, err := srv.actions.m["registry-restore"].Run(ctx, srv, ActionRequest{Pipeline: q}); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(p.ConfigPath); string(b) != hand {
		t.Fatal("restore did not write the database copy")
	}
	if _, err := srv.actions.m["registry-forget"].Run(ctx, srv, ActionRequest{Pipeline: srv.reg.Get("orders")}); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.GetPipeline(ctx, "orders"); ok || srv.reg.Get("orders") != nil {
		t.Fatal("forget left the pipeline registered")
	}
	if !fileExists(p.ConfigPath) {
		t.Fatal("forget deleted the file")
	}
}

// Scan: a new file is discovered into the database; a removed file is flagged (not deleted); file mode
// keeps the old behaviour (no DB, no flags).
func TestScannedPipelineDiscoveryAndRemoval(t *testing.T) {
	st := store.NewMem()
	scanDir := t.TempDir()
	e := dbEnv(t, st, func(e *testEnv, c *Config) { c.Scan = []string{filepath.Join(scanDir, "*.yaml")} }, nil)
	ctx := context.Background()
	src, _ := os.ReadFile(e.srv.reg.Get("orders").ConfigPath)
	f := filepath.Join(scanDir, "found.yaml")
	must0(t, os.WriteFile(f, src, 0o640))
	e.srv.reg.Refresh(ctx)
	row, ok, _ := st.GetPipeline(ctx, "found")
	if !ok || row.Origin != store.OriginScan || row.ConfigPath != f {
		t.Fatalf("scanned file not discovered: %+v", row)
	}
	must0(t, os.Remove(f))
	e.srv.reg.Refresh(ctx)
	p := e.srv.reg.Get("found")
	if p == nil || !strings.Contains(p.Missing, "removed from disk") {
		t.Fatalf("removed file not flagged: %+v", p)
	}
	warned := false
	for _, w := range e.srv.reg.Warnings() {
		warned = warned || strings.Contains(w, `"found"`)
	}
	if !warned {
		t.Fatal("no warning for the removed file")
	}
	if _, err := e.srv.actions.m["registry-restore"].Run(ctx, e.srv, ActionRequest{Pipeline: p}); err != nil {
		t.Fatal(err)
	}
	if q := e.srv.reg.Get("found"); q == nil || q.Missing != "" || !fileExists(f) {
		t.Fatalf("restore: %+v", q)
	}
}
