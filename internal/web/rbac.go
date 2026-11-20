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

// RouteRule maps an HTTP method and path pattern to a required permission.
type RouteRule struct {
	Method string
	Prefix string
	Exact  string
	Perm   Permission
}

// Central permissions table checked by middleware.
var RoutePermissions = []RouteRule{
	// User management (admin only)
	{Prefix: "/users", Perm: PermAdmin},
	{Prefix: "/api/v1/users", Perm: PermAdmin},

	// Settings (admin only: no settings for operator or viewer)
	{Prefix: "/settings", Perm: PermAdmin},
	{Prefix: "/api/v1/settings", Perm: PermAdmin},

	// Audit log (admin only)
	{Prefix: "/audit", Perm: PermAdmin},
	{Prefix: "/api/v1/audit", Perm: PermAdmin},

	// Sensitive reads/writes: env editor (admin only)
	{Prefix: "/api/v1/pipelines/{name}/env", Perm: PermAdmin},

	// Pipeline creation & deletion (admin only)
	{Method: "POST", Prefix: "/pipelines/new", Perm: PermAdmin},
	{Method: "GET", Prefix: "/pipelines/new", Perm: PermAdmin},
	{Method: "POST", Exact: "/api/v1/pipelines", Perm: PermAdmin},
	{Method: "POST", Prefix: "/api/v1/source/databases", Perm: PermAdmin},
	{Method: "DELETE", Prefix: "/api/v1/pipelines/", Perm: PermAdmin},

	// Configuration edits (admin only)
	{Method: "POST", Prefix: "/api/v1/pipelines/", Exact: "/api/v1/pipelines/{name}/config", Perm: PermAdmin},
	{Method: "POST", Prefix: "/api/v1/pipelines/", Exact: "/api/v1/pipelines/{name}/config/validate", Perm: PermAdmin},
	{Method: "POST", Prefix: "/api/v1/pipelines/", Exact: "/api/v1/pipelines/{name}/tuning", Perm: PermAdmin},

	// Backups: run/start/stop is operator; create/update/delete/restore is admin
	{Method: "POST", Prefix: "/api/v1/backup-jobs/", Exact: "/api/v1/backup-jobs/{id}/run", Perm: PermOperator},
	{Method: "POST", Prefix: "/api/v1/backup-jobs/", Exact: "/api/v1/backup-jobs/{id}/start", Perm: PermOperator},
	{Method: "POST", Prefix: "/api/v1/backup-jobs/", Exact: "/api/v1/backup-jobs/{id}/stop", Perm: PermOperator},
	{Method: "POST", Exact: "/api/v1/backup-jobs", Perm: PermAdmin},
	{Method: "PUT", Prefix: "/api/v1/backup-jobs/", Perm: PermAdmin},
	{Method: "DELETE", Prefix: "/api/v1/backup-jobs/", Perm: PermAdmin},
	{Method: "POST", Prefix: "/api/v1/backups/", Exact: "/api/v1/backups/{id}/restore", Perm: PermAdmin},

	// Account management: any authenticated user may manage their own account
	{Prefix: "/account", Perm: PermRead},
	{Prefix: "/api/v1/account", Perm: PermRead},
}

// RequiredPermissionForRequest inspects an HTTP request and returns its required permission.
func RequiredPermissionForRequest(r *http.Request) Permission {
	path := r.URL.Path
	method := r.Method

	// Pipeline actions: check specific action in URL
	// e.g. /api/v1/pipelines/orders/actions/start
	if strings.HasPrefix(path, "/api/v1/pipelines/") && strings.Contains(path, "/actions/") {
		parts := strings.Split(path, "/")
		if len(parts) >= 6 && parts[4] == "actions" {
			act := parts[5]
			if perm, ok := ActionPermissions[act]; ok {
				return perm
			}
			return PermAdmin
		}
	}

	// Sensitive reads/writes: env editor
	if strings.HasPrefix(path, "/api/v1/pipelines/") && strings.HasSuffix(path, "/env") {
		return PermAdmin
	}

	// Config and tuning POSTs
	if (method == http.MethodPost || method == http.MethodPut) && strings.HasPrefix(path, "/api/v1/pipelines/") {
		if strings.HasSuffix(path, "/config") || strings.HasSuffix(path, "/config/validate") || strings.HasSuffix(path, "/tuning") {
			return PermAdmin
		}
	}

	// Backup job execution vs administration
	if strings.HasPrefix(path, "/api/v1/backup-jobs/") {
		if strings.HasSuffix(path, "/run") || strings.HasSuffix(path, "/start") || strings.HasSuffix(path, "/stop") {
			return PermOperator
		}
		if method == http.MethodPut || method == http.MethodDelete || method == http.MethodPost {
			return PermAdmin
		}
	}

	// Check table rules
	for _, rule := range RoutePermissions {
		if rule.Method != "" && rule.Method != method {
			continue
		}
		if rule.Exact != "" && rule.Exact == path {
			return rule.Perm
		}
		if rule.Prefix != "" && (path == rule.Prefix || strings.HasPrefix(path, rule.Prefix+"/")) {
			return rule.Perm
		}
	}

	// Default for any write method (POST, PUT, DELETE, PATCH): at least operator
	if method == http.MethodPost || method == http.MethodPut || method == http.MethodDelete || method == http.MethodPatch {
		return PermOperator
	}

	// Default for reads (GET, HEAD, OPTIONS): viewer allowed
	return PermRead
}

// RenderForbiddenHTML outputs a standard, professional 403 response for browser navigation.
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
