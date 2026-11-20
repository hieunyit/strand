package web

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
	"strand/internal/spool"
)

// TestStandardPageSkeletonAndNoInlineStyles asserts that every registered page adheres
// to the shared skeleton (breadcrumb, pagehead, content cards, no inline styles) and tabs markup where relevant.
func TestStandardPageSkeletonAndNoInlineStyles(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c, _ := e.loginOK()

	allPages := []struct {
		path     string
		hasTabs  bool
		hasTable bool
	}{
		{"/", false, true},
		{"/pipelines", false, true},
		{"/pipelines/new", false, false},
		{"/jobs", false, true},
		{"/backups", true, true},
		{"/mcp", false, true},
		{"/settings", true, false},
		{"/settings/db", true, false},
		{"/help/schema", false, false},
		{"/tools/spool-inspect", false, false},
		{"/archive", false, false},
		{"/tunnels", false, false},
		{"/pipelines/orders", true, false},
		{"/pipelines/orders/tables", true, true},
		{"/pipelines/orders/checkpoint", true, false},
		{"/pipelines/orders/errors", true, true},
		{"/pipelines/orders/config", true, true},
		{"/pipelines/orders/logs", true, false},
		{"/pipelines/orders/jobs", true, true},
		{"/pipelines/orders/schema", true, false},
		{"/pipelines/orders/objects", true, false},
		{"/pipelines/orders/repair", true, false},
		{"/pipelines/orders/promote", true, false},
		{"/pipelines/orders/seed", true, false},
	}

	for _, tc := range allPages {
		t.Run(tc.path, func(t *testing.T) {
			resp, body := e.do(c, "GET", tc.path, "", nil)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s returned HTTP %d: %s", tc.path, resp.StatusCode, body)
			}

			// 1. Shared Skeleton: Breadcrumbs
			if !strings.Contains(body, `class="breadcrumb"`) {
				t.Errorf("%s missing <nav class=\"breadcrumb\">", tc.path)
			}
			if !strings.Contains(body, `<ol>`) {
				t.Errorf("%s breadcrumbs missing <ol>", tc.path)
			}

			// 2. Shared Skeleton: Pagehead with h1 and subtitle
			if !strings.Contains(body, `class="pagehead"`) {
				t.Errorf("%s missing <header class=\"pagehead\">", tc.path)
			}
			if !strings.Contains(body, `<h1>`) || !strings.Contains(body, `</h1>`) {
				t.Errorf("%s missing <h1> inside pagehead", tc.path)
			}

			// 3. Shared Skeleton: Content cards
			if !strings.Contains(body, `class="card`) && !strings.Contains(body, `class="cardhead"`) {
				t.Errorf("%s missing standard content cards", tc.path)
			}

			// 4. Tabs component where relevant
			if tc.hasTabs {
				if !strings.Contains(body, `class="tabs"`) {
					t.Errorf("%s expected to have tabs component", tc.path)
				}
				if !strings.Contains(body, `role="tablist"`) {
					t.Errorf("%s tabs missing role=\"tablist\"", tc.path)
				}
				if !strings.Contains(body, `role="tab"`) {
					t.Errorf("%s tabs missing role=\"tab\"", tc.path)
				}
			}

			// 5. Tables component where relevant
			if tc.hasTable {
				if !strings.Contains(body, `data-table`) {
					t.Errorf("%s expected table to use data-table class", tc.path)
				}
				if !strings.Contains(body, `tablewrap`) {
					t.Errorf("%s expected table to be wrapped in .tablewrap", tc.path)
				}
			}

			// 6. Zero inline styles (CSP compliance)
			if strings.Contains(body, ` style="`) || strings.Contains(body, ` style='`) {
				t.Errorf("%s contains inline style attribute (CSP violation)", tc.path)
			}
			if strings.Contains(body, `<style`) {
				t.Errorf("%s contains <style> block (CSP violation)", tc.path)
			}
			if strings.Contains(body, `onclick=`) || strings.Contains(body, `onload=`) {
				t.Errorf("%s contains inline event handlers (CSP violation)", tc.path)
			}
		})
	}
}

// TestTemplateComponentVocabulary checks every standardised template file. Login,
// users, and account are owned by the RBAC change and intentionally excluded.
// to verify all tables, buttons, cards, and styles follow the standard design system vocabulary.
func TestTemplateComponentVocabulary(t *testing.T) {
	entries, err := fs.ReadDir(templateFS, "templates")
	if err != nil {
		t.Fatalf("read templateFS: %v", err)
	}

	tableTagRE := regexp.MustCompile(`<table\b([^>]*)>`)
	buttonTagRE := regexp.MustCompile(`<button\b([^>]*)>`)
	cardDivRE := regexp.MustCompile(`<div\b[^>]*\bclass="[^"]*\bcard\b[^"]*"[^>]*>`)

	for _, entry := range entries {
		name := entry.Name()
		if name == "login.html" || name == "users.html" || name == "account.html" {
			// Explicitly left alone per requirement (feat/web-rbac owns these pages).
			continue
		}

		data, err := fs.ReadFile(templateFS, "templates/"+name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		content := string(data)

		// 1. Tables: every <table ...> must have class containing data-table
		tableMatches := tableTagRE.FindAllStringSubmatch(content, -1)
		for _, m := range tableMatches {
			attrs := m[1]
			if !strings.Contains(attrs, "data-table") {
				t.Errorf("%s: table tag <%s> missing data-table class", name, m[0])
			}
		}

		// 2. Buttons: every <button ...> must have btn, sortbtn, iconbtn, tab, or chip class
		buttonMatches := buttonTagRE.FindAllStringSubmatch(content, -1)
		for _, m := range buttonMatches {
			attrs := m[1]
			classMatch := regexp.MustCompile(`class="([^"]*)"`).FindStringSubmatch(attrs)
			if len(classMatch) < 2 {
				t.Errorf("%s: button <%s> has no class attribute", name, m[0])
				continue
			}
			cls := classMatch[1]
			valid := strings.Contains(cls, "btn") ||
				strings.Contains(cls, "sortbtn") ||
				strings.Contains(cls, "iconbtn") ||
				strings.Contains(cls, "tab") ||
				strings.Contains(cls, "chip") ||
				strings.Contains(cls, "segbtn")
			if !valid {
				t.Errorf("%s: button class %q outside vocabulary (must include btn, sortbtn, iconbtn, tab, or chip)", name, cls)
			}
		}

		// 3. Cards: no ad-hoc <div class="card..."> variants
		cardDivMatches := cardDivRE.FindAllString(content, -1)
		for _, m := range cardDivMatches {
			t.Errorf("%s: found ad-hoc card div <%s>; use <section class=\"card\"> instead", name, m)
		}

		// 4. Zero inline styles or inline scripts in templates
		if strings.Contains(content, ` style="`) || strings.Contains(content, ` style='`) {
			t.Errorf("%s contains inline style attribute (CSP violation)", name)
		}
		if strings.Contains(content, `<style`) {
			t.Errorf("%s contains <style> block (CSP violation)", name)
		}
		if strings.Contains(content, `onclick=`) || strings.Contains(content, `onload=`) {
			t.Errorf("%s contains inline event handlers (CSP violation)", name)
		}
	}
}

func TestSeedWizardNetworkMethodsOnly(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()

	// 1. Host-only methods (xtrabackup, pgbackrest) must be rejected
	for _, badMethod := range []string{"xtrabackup", "pgbackrest"} {
		code, res := e.act(c, csrf, "orders", "seed-begin", map[string]string{
			"backup_dir":   "/tmp/test-seed",
			"seed_type":    badMethod,
			"confirm_name": "orders",
		})
		if code == 200 {
			t.Fatalf("expected seed-begin with %s to be rejected, got 200: %v", badMethod, res)
		}
	}

	// 2. Network method (clone) succeeds
	code, res := e.act(c, csrf, "orders", "seed-begin", map[string]string{
		"backup_dir":   "/tmp/test-seed",
		"seed_type":    "clone",
		"confirm_name": "orders",
	})
	if code != 200 {
		t.Fatalf("seed-begin with clone failed: %d %v", code, res)
	}

	// 3. Verify state at /pipelines/orders/seed
	resp, body := e.do(c, "GET", "/pipelines/orders/seed", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET seed wizard returned HTTP %d", resp.StatusCode)
	}
	if !strings.Contains(body, "Step 1: Check backup and dry-run plan") {
		t.Fatalf("expected Step 1 in seed wizard body: %s", body)
	}

	// 4. Run plan
	code, res = e.act(c, csrf, "orders", "seed-plan", nil)
	if code != 200 {
		t.Fatalf("seed-plan failed: %d %v", code, res)
	}

	// 5. Run apply
	code, res = e.act(c, csrf, "orders", "seed-apply", map[string]string{"confirm_name": "orders"})
	if code != 200 {
		t.Fatalf("seed-apply failed: %d %v", code, res)
	}

	// 6. Run start
	code, res = e.act(c, csrf, "orders", "seed-start", nil)
	if code != 200 {
		t.Fatalf("seed-start failed: %d %v", code, res)
	}

	// Verify done
	resp, body = e.do(c, "GET", "/pipelines/orders/seed", "", nil)
	if !strings.Contains(body, "completed successfully") && !strings.Contains(body, "done") {
		t.Fatalf("expected seed wizard completed, got: %s", body)
	}
}

func TestJobDownloadEndpointSecurity(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()

	// 1. Non-export job (verify) must be rejected for download
	code, res := e.act(c, csrf, "orders", "verify", map[string]string{"mode": "sample", "table": "public.orders"})
	if code != 200 {
		t.Fatalf("start verify failed: %d %v", code, res)
	}
	verifyJobID, _ := res["job_id"].(string)

	resp, _ := e.do(c, "GET", "/api/v1/jobs/"+verifyJobID+"/download", "", nil)
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("expected download of non-export job to fail, got HTTP %d", resp.StatusCode)
	}

	// Cancel verify job so orders pipeline can run export
	_, _ = e.act(c, csrf, "orders", ActJobCancel, map[string]string{"job_id": verifyJobID})
	waitFor(t, "verify job cancellation", func() bool { j, ok := e.srv.jobs.Get(verifyJobID); return ok && !j.Running })

	// 2. Export job download succeeds and is audited
	code, res = e.act(c, csrf, "orders", "export", nil)
	if code != 200 {
		t.Fatalf("start export failed: %d %v", code, res)
	}
	exportJobID, _ := res["job_id"].(string)
	if exportJobID == "" {
		t.Fatalf("expected export job_id in response: %v", res)
	}

	resp, _ = e.do(c, "GET", "/api/v1/jobs/"+exportJobID+"/download", "", nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("unfinished export must not be downloadable, got HTTP %d", resp.StatusCode)
	}
	waitFor(t, "export job completion", func() bool { j, ok := e.srv.jobs.Get(exportJobID); return ok && !j.Running })
	j, ok := e.srv.jobs.Get(exportJobID)
	if !ok || j.ExitCode == nil || *j.ExitCode != 0 {
		t.Fatalf("export job did not succeed: %+v", j)
	}
	artifact, err := exportOutputArg(j.Args)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(artifact), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifact, []byte("real export artifact"), 0600); err != nil {
		t.Fatal(err)
	}
	resp, body := e.do(c, "GET", "/api/v1/jobs/"+exportJobID+"/download", "", nil)
	if resp.StatusCode != http.StatusOK || body != "real export artifact" {
		t.Fatalf("download must stream the completed artifact, got status=%d body=%q", resp.StatusCode, body)
	}
	checked, err := os.Lstat(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(artifact); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("must not stream"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, artifact); err != nil {
		t.Fatal(err)
	}
	if _, err := openCheckedRegularFile(artifact, checked); err == nil {
		t.Fatal("artifact identity change must be rejected")
	}
	cd := resp.Header.Get("Content-Disposition")
	if !strings.Contains(cd, "attachment") || !strings.Contains(cd, exportJobID) {
		t.Fatalf("expected Content-Disposition with export job id, got: %s", cd)
	}

	// Check audit log
	if audits := e.auditFor("job.download"); len(audits) == 0 {
		t.Fatalf("expected audit entry for job.download, found none")
	}

	// Cancel export job so the background process exits before cleanup
	_, _ = e.act(c, csrf, "orders", ActJobCancel, map[string]string{"job_id": exportJobID})
}

func TestSecurityControlsAndAuditing(t *testing.T) {
	e := opsEnv(t, false, nil)
	e.sampleNow()
	c, csrf := e.loginOK()
	hdr := map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}

	// 1. Tunnels: reject non-loopback listen
	badTunnelJSON, _ := json.Marshal(map[string]string{
		"ssh":         "bastion.example.com:22",
		"user":        "strand",
		"remote":      "db.internal:3306",
		"listen":      "0.0.0.0:3306", // non-loopback
		"known_hosts": "/path/to/known_hosts",
	})
	resp, body := e.do(c, "POST", "/api/v1/tunnels", string(badTunnelJSON), hdr)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "loopback") {
		t.Fatalf("expected loopback error, got %d: %s", resp.StatusCode, body)
	}

	// 2. Tunnels: reject missing known_hosts
	badHostsJSON, _ := json.Marshal(map[string]string{
		"ssh":    "bastion.example.com:22",
		"user":   "strand",
		"remote": "db.internal:3306",
		"listen": "127.0.0.1:0",
	})
	resp, body = e.do(c, "POST", "/api/v1/tunnels", string(badHostsJSON), hdr)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "known_hosts") {
		t.Fatalf("expected known_hosts error, got %d: %s", resp.StatusCode, body)
	}

	// 3. Tunnels: reject key outside STRAND_WEB_SSH_KEY_DIR
	keyDir := filepath.Join(t.TempDir(), "allowed-keys")
	_ = os.MkdirAll(keyDir, 0700)
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostSigner, err := ssh.NewSignerFromKey(hostPriv)
	if err != nil {
		t.Fatal(err)
	}
	knownHostsFile := filepath.Join(t.TempDir(), "known_hosts")
	knownHostsLine := "bastion.example.com " + string(ssh.MarshalAuthorizedKey(hostSigner.PublicKey()))
	if err := os.WriteFile(knownHostsFile, []byte(strings.TrimSpace(knownHostsLine)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}

	badKeyJSON, _ := json.Marshal(map[string]string{
		"ssh":           "bastion.example.com:22",
		"user":          "strand",
		"remote":        "db.internal:3306",
		"listen":        "127.0.0.1:0",
		"known_hosts":   knownHostsFile,
		"identity_file": "/etc/shadow",
	})
	resp, body = e.do(c, "POST", "/api/v1/tunnels", string(badKeyJSON), hdr)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "STRAND_WEB_SSH_KEY_DIR") {
		t.Fatalf("expected key dir error, got %d: %s", resp.StatusCode, body)
	}

	// 4. Spool inspect accepts a pipeline selector, never a client-supplied path.
	badSpoolJSON, _ := json.Marshal(map[string]string{
		"spool_dir": "/etc/passwd",
	})
	resp, body = e.do(c, "POST", "/api/v1/tools/spool-inspect", string(badSpoolJSON), hdr)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected arbitrary path payload to be rejected, got %d: %s", resp.StatusCode, body)
	}

	// 6. DB export: reject export dir outside state_dir/exports
	badExportDir := "/var/log/private"
	_, err = e.srv.validateExportDir(badExportDir)
	if err == nil {
		t.Fatalf("expected validateExportDir to reject %s", badExportDir)
	}

	// 7. Archive binlog: reject archive dir outside state_dir/archive
	badArchiveDir := "/root/archive"
	_, err = e.srv.validateArchiveDir(badArchiveDir)
	if err == nil {
		t.Fatalf("expected validateArchiveDir to reject %s", badArchiveDir)
	}
	exportRoot := filepath.Join(e.srv.cfg.StateDir, "exports")
	if err := os.Remove(exportRoot); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(outside, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, exportRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := e.srv.validateExportDir(""); err == nil {
		t.Fatal("export allow-list root symlink must be rejected")
	}
	archiveRoot := filepath.Join(e.srv.cfg.StateDir, "archive")
	if err := os.Remove(archiveRoot); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, archiveRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := e.srv.validateArchiveDir(""); err == nil {
		t.Fatal("archive allow-list root symlink must be rejected")
	}
}

func TestSpoolInspectScansConfiguredPipelineOffline(t *testing.T) {
	e := opsEnv(t, false, nil)
	// Admin socket unreachable plus no live PID represents a stopped pipeline.
	if err := os.Remove(e.sock); err != nil {
		t.Fatal(err)
	}
	c, csrf := e.loginOK()
	p := e.srv.reg.Get("orders")
	dir := p.Cfg.Resources.Spool.Path
	spill := filepath.Join(dir, "spill")
	if err := os.MkdirAll(spill, 0700); err != nil {
		t.Fatal(err)
	}
	sp, err := spool.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := sp.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(spill, "orphan.sidecar"), []byte("payload"), 0600); err != nil {
		t.Fatal(err)
	}
	hdr := map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}
	resp, body := e.do(c, "POST", "/api/v1/tools/spool-inspect", `{"pipeline":"orders"}`, hdr)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("inspect configured spool: status=%d body=%s", resp.StatusCode, body)
	}
	var result struct {
		Report struct {
			DiscoveredFiles int `json:"discovered_files"`
		} `json:"report"`
	}
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		t.Fatal(err)
	}
	if result.Report.DiscoveredFiles != 1 {
		t.Fatalf("expected real inventory of one file, got %+v", result.Report)
	}
}

func TestDBAdminExportsFromOpenMetadataStore(t *testing.T) {
	e := opsEnv(t, false, nil)
	e.sampleNow()
	c, csrf := e.loginOK()
	out := filepath.Join(e.srv.cfg.StateDir, "exports", "metadata-test")
	form := url.Values{"csrf": {csrf}, "dir": {out}}
	hdr := map[string]string{csrfHeader: csrf, "Content-Type": "application/x-www-form-urlencoded"}
	resp, body := e.do(c, "POST", "/settings/db/export", form.Encode(), hdr)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, "Export complete") {
		t.Fatalf("real metadata export failed: status=%d body=%s", resp.StatusCode, body)
	}
	if _, err := os.Stat(filepath.Join(out, "pipelines", "MANIFEST.json")); err != nil {
		t.Fatalf("metadata export manifest missing: %v", err)
	}
	if audits := e.auditFor("db.export"); len(audits) < 2 {
		t.Fatalf("expected start and success audit records, got %d", len(audits))
	}
}

func TestArchiveStartsAndStopsRealJobWithoutCredentialInArgs(t *testing.T) {
	e := opsEnv(t, false, nil)
	e.sampleNow()
	e.hold(true)
	t.Cleanup(func() { e.hold(false) })
	c, csrf := e.loginOK()
	hdr := map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}
	resp, body := e.do(c, "POST", "/api/v1/archive/start", `{"pipeline":"orders"}`, hdr)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("start archive: status=%d body=%s", resp.StatusCode, body)
	}
	var started struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal([]byte(body), &started); err != nil {
		t.Fatal(err)
	}
	if started.JobID == "" {
		t.Fatalf("archive start omitted job ID: %s", body)
	}
	waitFor(t, "archive process invocation", func() bool { return len(e.fakeCalls()) > 0 })
	call := e.fakeCalls()[0]
	if !reflect.DeepEqual(call.Args[:2], []string{"archive", "binlog"}) {
		t.Fatalf("unexpected archive command: %v", call.Args)
	}
	if !containsString(call.Args, "--config") || !containsString(call.Args, "--to") {
		t.Fatalf("archive command must receive pipeline config and output directory: %v", call.Args)
	}
	for _, arg := range call.Args {
		if strings.Contains(arg, "SuperSecretSrc") {
			t.Fatalf("source credential leaked in argv: %v", call.Args)
		}
	}
	resp, body = e.do(c, "POST", "/api/v1/archive/stop", `{"job_id":"`+started.JobID+`"}`, hdr)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stop archive: status=%d body=%s", resp.StatusCode, body)
	}
	waitFor(t, "archive job stop", func() bool { j, ok := e.srv.jobs.Get(started.JobID); return ok && !j.Running })
	finished, _ := e.srv.jobs.Get(started.JobID)
	archiveDir := e.srv.archiveOutputDir(&finished)
	if err := os.WriteFile(filepath.Join(archiveDir, "mysql-bin.000001"), []byte("binlog"), 0600); err != nil {
		t.Fatal(err)
	}
	_, page := e.do(c, "GET", "/archive", "", nil)
	if !strings.Contains(page, "mysql-bin.000001") {
		t.Fatalf("archive page must keep showing the latest job's files after stop: %s", page)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
