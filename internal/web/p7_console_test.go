package web

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// The sidebar lists every console feature in four groups; admin-only pages are not linked for other roles
// (a viewer used to see Settings and get a 403), and the operational tools (archive, tunnels, spool,
// metadata database, config reference) are reachable from the navigation instead of only by URL.
func TestSidebarGroupsAndRoleLinks(t *testing.T) {
	e := newCPEnv(t)
	adminOnly := []string{`href="/users"`, `href="/settings"`, `href="/settings/db"`, `href="/archive"`, `href="/tunnels"`,
		`href="/tools/spool-inspect"`, `href="/pipelines/new"`, `href="/backups/keys"`, `href="/backups/notifications"`, `href="/backups/activity"`}
	everyone := []string{`href="/pipelines"`, `href="/backups/jobs"`, `href="/backups/reports"`, `href="/help/schema"`, `href="/account"`, `href="/mcp"`}
	nav := func(page string) string {
		i, j := strings.Index(page, `<nav aria-label="Main">`), strings.Index(page, `</nav>`)
		if i < 0 || j < i {
			t.Fatal("main navigation not found")
		}
		return page[i:j]
	}
	code, body := e.admin().do("GET", "/", nil)
	if code != 200 {
		t.Fatalf("admin overview %d", code)
	}
	n := nav(body)
	for _, g := range []string{"Monitor", "Data movement", "Data protection", "System"} {
		if !strings.Contains(n, `aria-expanded="true"`) || !strings.Contains(n, ">"+g+"</button>") {
			t.Errorf("nav group %q missing or not a toggle", g)
		}
	}
	for _, h := range append(append([]string{}, adminOnly...), everyone...) {
		if !strings.Contains(n, h) {
			t.Errorf("admin nav lacks %s", h)
		}
	}
	for _, role := range []store.Role{store.RoleViewer, store.RoleOperator} {
		code, body := e.roleClient(role).do("GET", "/", nil)
		if code != 200 {
			t.Fatalf("%s overview %d", role, code)
		}
		n := nav(body)
		for _, h := range adminOnly {
			if strings.Contains(n, h) {
				t.Errorf("%s nav links admin-only %s", role, h)
			}
		}
		for _, h := range everyone {
			if !strings.Contains(n, h) {
				t.Errorf("%s nav lacks %s", role, h)
			}
		}
	}
}

// Settings are grouped in sections, each its own form: saving General posts only its fields, so the other
// sections keep their values. The environment name and colour drive the chip in the top bar.
func TestEnvironmentChipAndSectionedSettings(t *testing.T) {
	e := dbEnv(t, store.NewMem(), nil, nil)
	c, csrf := e.loginOK()
	h := map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}
	if resp, body := e.do(c, "POST", "/api/v1/settings", `{"trust_forwarded_for": true}`, h); resp.StatusCode != 200 {
		t.Fatalf("seed security setting: %d %s", resp.StatusCode, body)
	}
	if resp, _ := e.do(c, "POST", "/api/v1/settings", `{"environment_tone": "purple"}`, h); resp.StatusCode != 400 {
		t.Fatalf("unknown tone accepted: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "POST", "/api/v1/settings", `{"environment_name": "prod\u0007"}`, h); resp.StatusCode != 400 {
		t.Fatalf("control character accepted: %d", resp.StatusCode)
	}
	_, page := e.do(c, "GET", "/settings", "", nil)
	for _, id := range []string{`id="sec-general"`, `id="sec-security"`, `id="sec-audit"`, `id="sec-about"`, `class="settings-nav"`} {
		if !strings.Contains(page, id) {
			t.Errorf("settings page lacks %s", id)
		}
	}
	form := "csrf=" + csrf + "&environment_name=production&environment_tone=danger&scan=&config_dir=&sample_interval_sec=5&shutdown_stop_pipelines=false"
	resp, page := e.do(c, "POST", "/settings", form, map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if resp.StatusCode != 200 || !strings.Contains(page, "Saved:") {
		t.Fatalf("save General: %d", resp.StatusCode)
	}
	if !strings.Contains(page, `class="envchip env-danger"`) || !strings.Contains(page, ">production</span>") {
		t.Error("environment chip not shown with its colour")
	}
	_, body := e.do(c, "GET", "/api/v1/settings", "", nil)
	var got struct {
		Settings []SettingView `json:"settings"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatalf("settings JSON: %v (%.200s)", err, body)
	}
	seen := false
	for _, it := range got.Settings {
		if it.Key == "trust_forwarded_for" {
			seen = true
			if it.Value != true {
				t.Errorf("saving the General section changed a Security setting: %v", it.Value)
			}
		}
	}
	if !seen {
		t.Fatalf("trust_forwarded_for not in %.200s", body)
	}
}

func TestOverviewDataProtectionAndAttention(t *testing.T) {
	e := newCPEnv(t)
	// 1. Initial overview with backup catalog configured
	code, body := e.admin().do("GET", "/", nil)
	if code != 200 {
		t.Fatalf("GET / returned %d", code)
	}
	for _, want := range []string{
		`id="overview-backup-strip"`,
		`Protected sources`,
		`href="/backups/jobs"`,
		`Failing jobs`,
		`href="/backups/jobs?status=failing"`,
		`RPO breaches`,
		`href="/backups/reports"`,
		`Last test restore`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("overview lacks %s", want)
		}
	}

	// 2. Add a failing job and verify it merges into the Needs attention card
	ctx := context.Background()
	job := backup.Job{
		ID:                  "job-fail-1",
		Name:                "shop-critical",
		EverySeconds:        3600,
		JobOptions:          backup.JobOptions{Method: backup.MethodMySQLDump, Source: "shop", Conn: backup.ConnSpec{Host: "127.0.0.1"}},
		Bucket:              "central-backups",
		ConsecutiveFailures: 2,
		LastRunStatus:       "failed",
	}
	if err := e.st.CreateBackupJob(ctx, job); err != nil {
		t.Fatalf("create backup job: %v", err)
	}

	code, body = e.admin().do("GET", "/", nil)
	if code != 200 {
		t.Fatalf("GET / after failing job: %d", code)
	}
	if !strings.Contains(body, "shop-critical") {
		t.Error("failing backup job not shown in Needs attention")
	}
	if !strings.Contains(body, `href="/backups/jobs/job-fail-1?tab=runs">View log</a>`) {
		t.Error("Needs attention missing View log link for backup job")
	}
	if !strings.Contains(body, `href="/backups/jobs/job-fail-1?tab=settings">Edit</a>`) {
		t.Error("Needs attention missing Edit link for backup job")
	}

	// 3. Env without backup catalog hides the data protection row
	plainEnv := newEnv(t, nil)
	plainEnv.srv.backups = nil
	c, _ := plainEnv.loginOK()
	resp, body := plainEnv.do(c, "GET", "/", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("plain env GET / returned %d", resp.StatusCode)
	}
	if strings.Contains(body, "overview-backup-strip") {
		t.Error("plain env without backups should hide Data protection strip")
	}
}

func TestUsersDrawerAndRoleBadges(t *testing.T) {
	e := newCPEnv(t)
	// Admin access to /users
	code, body := e.admin().do("GET", "/users", nil)
	if code != 200 {
		t.Fatalf("GET /users returned %d", code)
	}
	for _, want := range []string{
		`<dialog class="drawer" id="user-drawer"`,
		`data-open-drawer="user-drawer"`,
		`data-confirm-user="`,
		`class="data-table grid"`,
		`<caption class="sr-only">Users</caption>`,
		`st-synced`, // admin badge
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/users missing %s", want)
		}
	}

	// Viewer and operator receive 403
	for _, role := range []store.Role{store.RoleViewer, store.RoleOperator} {
		c, _ := e.roleClient(role).do("GET", "/users", nil)
		if c != 403 {
			t.Errorf("expected 403 for %s on /users, got %d", role, c)
		}
	}
}

func TestLoginExpiredNotice(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()

	// 1. Fresh visit without cookie redirects to /login without expired=1
	resp, _ := e.do(c, "GET", "/pipelines", "", nil)
	if resp.StatusCode != 302 {
		t.Fatalf("expected 302, got %d", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if strings.Contains(loc, "expired=1") {
		t.Errorf("fresh visit redirect should not have expired=1: %s", loc)
	}

	// 2. Direct visit to /login has no session expired notice
	resp, body := e.do(c, "GET", "/login", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if strings.Contains(body, "Your session expired. Sign in again.") {
		t.Error("clean /login should not show session expired message")
	}

	// 3. Visit with invalid/expired cookie redirects to /login with expired=1
	resp, _ = e.do(c, "GET", "/pipelines", "", map[string]string{
		"Cookie": sessionCookie + "=invalid-token-12345",
	})
	if resp.StatusCode != 302 {
		t.Fatalf("expected 302, got %d", resp.StatusCode)
	}
	loc = resp.Header.Get("Location")
	if !strings.Contains(loc, "expired=1") {
		t.Errorf("expired cookie redirect missing expired=1: %s", loc)
	}
	if !strings.Contains(loc, "next=%2Fpipelines") {
		t.Errorf("expired cookie redirect missing next: %s", loc)
	}

	// 4. Visiting /login?expired=1 renders the notice
	resp, body = e.do(c, "GET", "/login?expired=1", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if !strings.Contains(body, "Your session expired. Sign in again.") {
		t.Error("login page with ?expired=1 missing expired session message")
	}

	// 5. Open redirect prevention
	resp, body = e.do(c, "GET", "/login?next=//evil.com", "", nil)
	if strings.Contains(body, `value="//evil.com"`) {
		t.Error("open redirect not prevented in login next value")
	}
}
