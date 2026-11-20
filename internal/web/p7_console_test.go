package web

import (
	"strings"
	"testing"

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
