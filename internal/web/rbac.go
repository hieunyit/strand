package web

import (
	"fmt"
	"net/http"
	"strings"

	"strand/internal/web/store"
)

// Permission specifies an RBAC authorization requirement.
type Permission string

const (
	// PermRead: read-only access (dashboards, pipelines, metrics, jobs, backups).
	PermRead Permission = "read"
	// PermOperator: operational controls (start/stop/pause/resume/throttle, verify/repair jobs, snapshot add, run backups).
	PermOperator Permission = "operator"
	// PermAdmin: full access including user management, settings, config edits, pipeline creation/deletion, backups restore/delete.
	PermAdmin Permission = "admin"
)

// RoleAllows returns whether role satisfies the required permission level.
func RoleAllows(role store.Role, perm Permission) bool {
	switch perm {
	case PermRead:
		return role == store.RoleAdmin || role == store.RoleOperator || role == store.RoleViewer
	case PermOperator:
		return role == store.RoleAdmin || role == store.RoleOperator
	case PermAdmin:
		return role == store.RoleAdmin
	default:
		return false
	}
}

// ActionPermissions maps pipeline actions to their minimum required permission level.
var ActionPermissions = map[string]Permission{
	// Operator actions
	ActStart:          PermOperator,
	ActStop:           PermOperator,
	ActRestart:        PermOperator,
	ActPause:          PermOperator,
	ActPauseSnapshot:  PermOperator,
	ActResume:         PermOperator,
	ActResumeSnapOnly: PermOperator,
	ActThrottle:       PermOperator,
	ActSnapshotAdd:    PermOperator,
	ActCheckpointShow: PermOperator,
	ActVerify:         PermOperator,
	ActJobCancel:      PermOperator,

	// Admin-only actions
	ActRewind:         PermAdmin,
	ActDelete:         PermAdmin,
	"promote":         PermAdmin,
	"config-save":     PermAdmin,
	"config-validate": PermAdmin,
}

// routeRule: method ("" = any) + path pattern ("{x}" matches exactly one path segment; a trailing "/**"
// matches the prefix and everything below it) → required permission.
type routeRule struct {
	Method  string
	Pattern string
	Perm    Permission
}

// writeRules lists the write routes that do NOT need admin. Every other POST/PUT/PATCH/DELETE requires admin
// (fail closed: a newly added write route is admin-only until it is listed here on purpose).
var writeRules = []routeRule{
	// Own session and account (every signed-in user).
	{"POST", "/login", PermRead}, {"POST", "/logout", PermRead},
	{"POST", "/api/v1/login", PermRead}, {"POST", "/api/v1/logout", PermRead},
	{"", "/account/**", PermRead}, {"", "/api/v1/account/**", PermRead},
	// Operator: run/verify backups, preview retention (no deletes, no restores, no edits).
	{"POST", "/api/v1/backup-jobs/{id}/run", PermOperator},
	{"POST", "/api/v1/backup-jobs/{id}/start", PermOperator},
	{"POST", "/api/v1/backup-jobs/{id}/stop", PermOperator},
	{"POST", "/api/v1/backup-jobs/{id}/pause", PermOperator},
	{"POST", "/api/v1/backup-jobs/{id}/resume", PermOperator},
	{"POST", "/api/v1/backup-jobs/{id}/verify", PermOperator},
	{"POST", "/api/v1/backup-jobs/{id}/retention-preview", PermOperator},
	{"POST", "/api/v1/backups/{id}/verify", PermOperator},
	{"POST", "/api/v1/backup-runs/{id}/cancel", PermOperator},
	// Operator: test a backup connection (a read-only login probe; creating/editing/deleting connections
	// and policies stays admin through the fail-closed default).
	{"POST", "/api/v1/backup-connections/test", PermOperator},
	{"POST", "/api/v1/backup-connections/{id}/test", PermOperator},
	// Operator: test a backup repository (list/put/get/delete of one tiny test object plus an Object Lock
	// check: the same class of probe as a connection test, and operators run the backups that depend on it).
	// Creating/editing/deleting repositories stays admin. Notification channel tests stay admin: they make
	// the server contact an external destination the admin configured.
	{"POST", "/api/v1/backup-repositories/{id}/test", PermOperator},
}

// readRules lists sensitive READ routes that need more than viewer.
var readRules = []routeRule{
	{"GET", "/users/**", PermAdmin}, {"GET", "/users", PermAdmin}, {"GET", "/api/v1/users/**", PermAdmin}, {"GET", "/api/v1/users", PermAdmin},
	{"GET", "/settings", PermAdmin}, {"GET", "/api/v1/settings", PermAdmin},
	{"GET", "/audit/**", PermAdmin}, {"GET", "/audit", PermAdmin}, {"GET", "/api/v1/audit/**", PermAdmin}, {"GET", "/api/v1/audit", PermAdmin},
	{"GET", "/api/v1/pipelines/{name}/env", PermAdmin},
	{"GET", "/api/v1/pipelines/{name}/config/download", PermAdmin},
	{"GET", "/pipelines/new", PermAdmin},
	// Added by Web UI standardisation: sensitive operational pages and exported database artifacts.
	{"GET", "/api/v1/jobs/{id}/download", PermAdmin},
	{"GET", "/settings/db", PermAdmin},
	{"GET", "/tools/spool-inspect", PermAdmin},
	{"GET", "/archive", PermAdmin},
	{"GET", "/tunnels", PermAdmin},
	// Backup operations data that is not for every viewer: encryption key ids/fingerprints, where alerts go,
	// and the backup activity feed (an audit view with users and addresses).
	{"GET", "/api/v1/backup-keys", PermAdmin}, {"GET", "/backups/keys", PermAdmin},
	{"GET", "/api/v1/backup-notify-channels/**", PermAdmin}, {"GET", "/api/v1/backup-notify-channels", PermAdmin},
	{"GET", "/api/v1/backup-notify-rules/**", PermAdmin}, {"GET", "/api/v1/backup-notify-rules", PermAdmin},
	{"GET", "/backups/notifications", PermAdmin},
	{"GET", "/api/v1/backup-activity", PermAdmin}, {"GET", "/backups/activity", PermAdmin},
}

// RoutePermissions is kept for documentation/tests: the effective rules.
var RoutePermissions = append(append([]routeRule(nil), writeRules...), readRules...)

func matchPattern(pattern, path string) bool {
	if strings.HasSuffix(pattern, "/**") {
		base := strings.TrimSuffix(pattern, "/**")
		return path == base || strings.HasPrefix(path, base+"/")
	}
	ps, xs := strings.Split(strings.Trim(pattern, "/"), "/"), strings.Split(strings.Trim(path, "/"), "/")
	if len(ps) != len(xs) {
		return false
	}
	for i := range ps {
		if strings.HasPrefix(ps[i], "{") && strings.HasSuffix(ps[i], "}") {
			if xs[i] == "" {
				return false
			}
			continue
		}
		if ps[i] != xs[i] {
			return false
		}
	}
	return true
}

func lookupRule(rules []routeRule, method, path string) (Permission, bool) {
	for _, r := range rules {
		if r.Method != "" && r.Method != method {
			continue
		}
		if matchPattern(r.Pattern, path) {
			return r.Perm, true
		}
	}
	return "", false
}

func isWrite(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	return true
}

// RequiredPermissionForRequest returns the minimum permission for a request. Fail closed: any write route not
// listed in writeRules (and not a pipeline action listed in ActionPermissions) requires admin.
func RequiredPermissionForRequest(r *http.Request) Permission {
	path, method := r.URL.Path, r.Method
	if isWrite(method) {
		if matchPattern("/api/v1/pipelines/{name}/actions/{action}", path) {
			act := strings.Split(strings.Trim(path, "/"), "/")[5]
			if perm, ok := ActionPermissions[act]; ok {
				return perm
			}
			return PermAdmin
		}
		if perm, ok := lookupRule(writeRules, method, path); ok {
			return perm
		}
		return PermAdmin
	}
	if perm, ok := lookupRule(readRules, method, path); ok {
		return perm
	}
	return PermRead
}

func RenderForbiddenHTML(w http.ResponseWriter, perm Permission) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusForbidden)
	fmt.Fprintf(w, `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>403 Forbidden · Strand</title>
<link rel="stylesheet" href="/static/app.css">
</head>
<body class="loginpage">
<main>
<div class="card">
  <div class="logobig">
    <svg class="ic logo" aria-hidden="true" focusable="false"><use href="/static/icons.svg#i-logo"/></svg>
    <div><h1>Strand</h1><p class="tag">Access denied</p></div>
  </div>
  <p class="warnbox err" role="alert">Forbidden: this action requires %s role.</p>
  <p><a class="btn primary" href="/">Return to Overview</a></p>
</div>
</main>
</body>
</html>`, perm)
}
