package web

import (
	"encoding/json"
	"net/http"
	"strconv"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// Pages for repositories, notifications, keys and activity. They render server-side from the same data as the
// JSON API; the forms and buttons are driven by static/bkops.js through window.strandApi (CSRF + RBAC).

func (s *Server) backupOpsRoutes(mux *http.ServeMux) {
	mux.Handle("GET /backups/repositories", s.auth(false, s.pageBackupsRepos))
	mux.Handle("GET /backups/repositories/{id}", s.auth(false, s.pageBackupsRepoDetail))
	mux.Handle("GET /backups/notifications", s.auth(false, s.pageBackupsNotifications))
	mux.Handle("GET /backups/keys", s.auth(false, s.pageBackupsKeys))
	mux.Handle("GET /backups/activity", s.auth(false, s.pageBackupsActivity))
}

func opsCrumbs(label, url string) []BreadcrumbItem {
	c := []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}}
	if url != "" {
		c = append(c, BreadcrumbItem{Label: label, URL: url})
	}
	return c
}

type opsBody struct {
	CanWrite bool // operator or admin
	IsAdmin  bool
}

func roleBody(sess *Session) opsBody {
	return opsBody{CanWrite: sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator), IsAdmin: sess != nil && sess.Role == store.RoleAdmin}
}

type reposPageBody struct {
	opsBody
	Repos         []repoView
	SecretStorage bool
}

func (s *Server) pageBackupsRepos(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil {
		http.Error(w, "backup service unavailable: configure metadata database", http.StatusServiceUnavailable)
		return
	}
	rs, _ := s.db.st.ListBackupRepos(r.Context())
	usage, _ := s.db.st.BackupRepoUsage(r.Context())
	body := reposPageBody{opsBody: roleBody(sess), SecretStorage: s.db.box != nil}
	for _, rp := range rs {
		body.Repos = append(body.Repos, s.repoView(rp, usage))
	}
	crumbs := append(opsCrumbs("", ""), BreadcrumbItem{Label: "Repositories"})
	s.render(w, "backups_repositories", sess, pageData{Title: "Repositories", Subtitle: "Where backups are stored: S3-compatible buckets, optionally immutable (Object Lock)",
		Active: "backups-repos", Breadcrumbs: crumbs, Body: body})
}

type repoBar struct {
	Source string
	Bytes  int64
	Count  int
	Copies int
}

type repoDetailBody struct {
	opsBody
	Repo     repoView
	Steps    []RepoTestStep
	Sources  []repoBar
	MaxBytes int64
	Jobs     []string
	Policies []string
}

func (s *Server) pageBackupsRepoDetail(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil {
		http.Error(w, "backup service unavailable: configure metadata database", http.StatusServiceUnavailable)
		return
	}
	rp, ok, err := s.db.st.GetBackupRepo(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, "could not read the repository", http.StatusServiceUnavailable)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	usage, _ := s.db.st.BackupRepoUsage(r.Context())
	body := repoDetailBody{opsBody: roleBody(sess), Repo: s.repoView(rp, usage)}
	for _, u := range usage {
		if u.Repository == rp.ID {
			body.Sources = append(body.Sources, repoBar{Source: u.Source, Bytes: u.Bytes, Count: u.Count, Copies: u.Copies})
			if u.Bytes > body.MaxBytes {
				body.MaxBytes = u.Bytes
			}
		}
	}
	body.Jobs, body.Policies = s.repoUsedBy(r.Context(), rp.ID)
	if len(body.Repo.TestSteps) > 0 {
		_ = jsonUnmarshal(body.Repo.TestSteps, &body.Steps)
	}
	crumbs := append(opsCrumbs("Repositories", "/backups/repositories"), BreadcrumbItem{Label: rp.Name})
	s.render(w, "backups_repository_detail", sess, pageData{Title: rp.Name, Subtitle: "Repository " + rp.ID, Active: "backups-repos", Breadcrumbs: crumbs, Body: body})
}

type notifyPageBody struct {
	opsBody
	Channels []channelView
	Rules    []backup.NotifyRule
	Names    map[string]string
	Events   []string
	Policies []backup.Policy
	Jobs     []backup.Job
}

func (s *Server) pageBackupsNotifications(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil {
		http.Error(w, "backup service unavailable: configure metadata database", http.StatusServiceUnavailable)
		return
	}
	cs, _ := s.db.st.ListNotifyChannels(r.Context())
	rules, _ := s.db.st.ListNotifyRules(r.Context())
	body := notifyPageBody{opsBody: roleBody(sess), Rules: rules, Events: backup.NotifyEvents, Names: map[string]string{}}
	if env, ok := s.envChannel(); ok {
		body.Channels = append(body.Channels, env)
		body.Names[env.ID] = env.Name
	}
	for _, c := range cs {
		body.Channels = append(body.Channels, channelView{NotifyChannel: c})
		body.Names[c.ID] = c.Name
	}
	body.Policies, _ = s.db.st.ListBackupPolicies(r.Context())
	body.Jobs, _ = s.db.st.ListBackupJobs(r.Context())
	crumbs := append(opsCrumbs("", ""), BreadcrumbItem{Label: "Notifications"})
	s.render(w, "backups_notifications", sess, pageData{Title: "Notifications", Subtitle: "Channels and the rules that route backup events to them",
		Active: "backups-notify", Breadcrumbs: crumbs, Body: body})
}

type keysPageBody struct {
	opsBody
	Info keysInfo
}

func (s *Server) pageBackupsKeys(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil {
		http.Error(w, "backup service unavailable: configure metadata database", http.StatusServiceUnavailable)
		return
	}
	info, err := s.keysInfo(r.Context())
	if err != nil {
		http.Error(w, "could not read the backup catalog", http.StatusServiceUnavailable)
		return
	}
	crumbs := append(opsCrumbs("", ""), BreadcrumbItem{Label: "Encryption keys"})
	s.render(w, "backups_keys", sess, pageData{Title: "Encryption keys", Subtitle: "Key ids configured in the environment and the backups written with each (key material is never shown)",
		Active: "backups-keys", Breadcrumbs: crumbs, Body: keysPageBody{opsBody: roleBody(sess), Info: info}})
}

type activityPageBody struct {
	opsBody
	Items  []activityItem
	Next   string
	User   string
	Action string
}

func (s *Server) pageBackupsActivity(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil {
		http.Error(w, "backup service unavailable: configure metadata database", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	prefix := q.Get("action")
	if prefix == "" || len(prefix) < 6 || prefix[:6] != "backup" {
		prefix = "backup"
	}
	var cursor int64
	if v := q.Get("cursor"); v != "" {
		cursor, _ = strconv.ParseInt(v, 10, 64)
	}
	items, next, err := s.backupActivity(r, prefix, q.Get("user"), nil, nil, cursor, 50)
	if err != nil {
		http.Error(w, "could not read the audit log", http.StatusServiceUnavailable)
		return
	}
	crumbs := append(opsCrumbs("", ""), BreadcrumbItem{Label: "Activity"})
	s.render(w, "backups_activity", sess, pageData{Title: "Backup activity", Subtitle: "Every backup write, with who did it and what changed (secrets are never recorded)",
		Active: "backups-activity", Breadcrumbs: crumbs, Body: activityPageBody{opsBody: roleBody(sess), Items: items, Next: next, User: q.Get("user"), Action: prefix}})
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
