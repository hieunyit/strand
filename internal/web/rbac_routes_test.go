package web

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Every write route registered in the server source is classified fail-closed: viewer never, operator only for
// the routes listed on purpose, everything else admin. A new write route is admin-only until listed.
func TestRBACEveryWriteRouteFailClosed(t *testing.T) {
	re := regexp.MustCompile(`mux\.Handle(?:Func)?\("(POST|PUT|DELETE|PATCH|GET) ([^"]+)"`)
	files, _ := filepath.Glob("*.go")
	type route struct{ method, path string }
	var routes []route
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			routes = append(routes, route{m[1], m[2]})
		}
	}
	if len(routes) < 40 {
		t.Fatalf("found only %d routes; the route regexp no longer matches the server source", len(routes))
	}
	operatorOK := map[string]bool{
		"POST /api/v1/backup-jobs/{id}/run": true, "POST /api/v1/backup-jobs/{id}/start": true,
		"POST /api/v1/backup-jobs/{id}/stop": true, "POST /api/v1/backup-jobs/{id}/verify": true,
		"POST /api/v1/backup-jobs/{id}/retention-preview": true, "POST /api/v1/backups/{id}/verify": true,
	}
	concrete := strings.NewReplacer("{id}", "42", "{name}", "orders", "{username}", "bob", "{action}", "start")
	for _, rt := range routes {
		if rt.method == "GET" {
			continue
		}
		path := concrete.Replace(rt.path)
		perm := RequiredPermissionForRequest(httptest.NewRequest(rt.method, path, nil))
		key := rt.method + " " + rt.path
		self := strings.HasPrefix(rt.path, "/account") || strings.HasPrefix(rt.path, "/api/v1/account") ||
			rt.path == "/login" || rt.path == "/logout" || rt.path == "/api/v1/login" || rt.path == "/api/v1/logout"
		switch {
		case self:
			if perm != PermRead {
				t.Errorf("%s: own-account/session route needs %s, want read", key, perm)
			}
		case strings.Contains(rt.path, "/actions/"):
			if perm == PermRead {
				t.Errorf("%s: pipeline action allowed for viewer", key)
			}
		case operatorOK[key]:
			if perm != PermOperator {
				t.Errorf("%s: want operator, got %s", key, perm)
			}
		default:
			if perm != PermAdmin {
				t.Errorf("%s: want admin (fail closed), got %s", key, perm)
			}
		}
	}
	// The old Exact rule with a literal "{id}" never matched: restores fell through to operator.
	if p := RequiredPermissionForRequest(httptest.NewRequest("POST", "/api/v1/backups/7/restore", nil)); p != PermAdmin {
		t.Errorf("backup restore needs %s, want admin", p)
	}
	if p := RequiredPermissionForRequest(httptest.NewRequest("POST", "/api/v1/backup-restores", nil)); p != PermAdmin {
		t.Errorf("restore wizard needs %s, want admin", p)
	}
	if p := RequiredPermissionForRequest(httptest.NewRequest("POST", "/api/v1/some-new-thing", nil)); p != PermAdmin {
		t.Errorf("unlisted write route needs %s, want admin (fail closed)", p)
	}
	for _, p := range []string{
		"/api/v1/pipelines/orders/env", "/api/v1/pipelines/orders/config/download", "/api/v1/users", "/settings", "/audit",
		"/api/v1/jobs/42/download", "/settings/db", "/tools/spool-inspect", "/archive", "/tunnels",
	} {
		if got := RequiredPermissionForRequest(httptest.NewRequest("GET", p, nil)); got != PermAdmin {
			t.Errorf("GET %s needs %s, want admin", p, got)
		}
	}
}
