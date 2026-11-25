package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"

	"strand/internal/backup"
	"strand/internal/backup/runner"
	"strand/internal/web/store"
)

// Pages of the backup console for connections, policies and the job wizard (spec 4.3, 4.7). All of them
// read through the metadata store; every write goes through the existing /api/v1 endpoints, so RBAC and
// auditing are those of the API. Viewers see the pages without any write control, operators may test a
// connection, admins create/edit/delete.

func init() {
	pageNames = append(pageNames, "backups_connections", "backups_connection_detail", "backups_policies",
		"backups_policy_detail", "backups_job_wizard")
}

// partialTemplates are shared {{define}} blocks parsed into every page.
var partialTemplates = []string{"templates/_backup_partials.html"}

func (s *Server) backupUIRoutes(mux *http.ServeMux) {
	mux.Handle("GET /backups/connections", s.auth(false, s.pageBackupConnections))
	mux.Handle("GET /backups/connections/{id}", s.auth(false, s.pageBackupConnectionDetail))
	mux.Handle("GET /backups/policies", s.auth(false, s.pageBackupPolicies))
	mux.Handle("GET /backups/policies/{id}", s.auth(false, s.pageBackupPolicyDetail))
	mux.Handle("GET /backups/jobs/new", s.auth(false, s.pageBackupJobWizard(false)))
	mux.Handle("GET /backups/jobs/{id}/edit", s.auth(false, s.pageBackupJobWizard(true)))
	mux.Handle("POST /api/v1/backup-jobs/{id}/convert-connection", s.auth(true, s.apiBackupJobConvertConnection))
}

type backupRoles struct{ CanTest, IsAdmin bool }

func rolesOf(sess *Session) backupRoles {
	return backupRoles{
		CanTest: sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator),
		IsAdmin: sess != nil && sess.Role == store.RoleAdmin,
	}
}

func (s *Server) needDBPage(w http.ResponseWriter) bool {
	if s.db == nil || s.db.st == nil {
		http.Error(w, "backup service unavailable: configure metadata database", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// retentionWords describes a retention policy in plain words (mirrored by retentionWords in backups_ui.js).
func retentionWords(r backup.RetentionPolicy) string {
	var parts []string
	add := func(n int, label string) {
		if n > 0 {
			parts = append(parts, strconv.Itoa(n)+" "+label)
		}
	}
	add(r.KeepLast, "most recent")
	add(r.KeepHourly, "hourly")
	add(r.KeepDaily, "daily")
	add(r.KeepWeekly, "weekly")
	add(r.KeepMonthly, "monthly")
	add(r.KeepYearly, "yearly")
	out := ""
	if len(parts) == 0 && r.KeepWithinDays == 0 {
		out = "Keep every backup"
	} else if len(parts) > 0 {
		out = "Keep " + strings.Join(parts, ", ") + " backups"
	}
	if r.KeepWithinDays > 0 {
		if out == "" {
			out = "Keep"
		} else {
			out += " and"
		}
		out += " every backup of the last " + strconv.Itoa(r.KeepWithinDays) + " days"
	}
	if r.MaxAgeDays > 0 {
		out += "; expire anything older than " + strconv.Itoa(r.MaxAgeDays) + " days"
	}
	return out
}

func connStatusClass(c backup.Connection) string {
	switch c.LastTestStatus {
	case "ok":
		return "ok"
	case "warning":
		return "warning"
	case "failed":
		return "failed"
	}
	return "untested"
}

type connDrawer struct {
	Profiles      []sshProfileView
	StoredAllowed bool
	Conn          *backup.Connection
	Wizard        bool
}

type connRow struct {
	backup.Connection
	Jobs        int
	StatusClass string
}

type backupConnectionsBody struct {
	backupRoles
	Rows    []connRow
	Q       string
	Engine  string
	Status  string
	Drawer  connDrawer
	Total   int
	Unavail string
}

func (s *Server) jobCounts(ctx context.Context) (conn, pol map[string]int, jobs []backup.Job) {
	conn, pol = map[string]int{}, map[string]int{}
	jobs, _ = s.db.st.ListBackupJobs(ctx)
	for _, j := range jobs {
		if j.ConnectionID != "" {
			conn[j.ConnectionID]++
		}
		if j.PolicyID != "" {
			pol[j.PolicyID]++
		}
	}
	return
}

func (s *Server) storedAllowed() bool { return s.db != nil && s.db.box != nil }

func (s *Server) pageBackupConnections(w http.ResponseWriter, r *http.Request, sess *Session) {
	if !s.needDBPage(w) {
		return
	}
	cs, err := s.db.st.ListBackupConnections(r.Context())
	body := backupConnectionsBody{backupRoles: rolesOf(sess), Drawer: connDrawer{Profiles: s.sshProfileViews(r.Context()), StoredAllowed: s.storedAllowed()}}
	if err != nil {
		body.Unavail = "Could not read the backup connections."
	}
	counts, _, _ := s.jobCounts(r.Context())
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	body.Q, body.Engine, body.Status = q, r.URL.Query().Get("engine"), r.URL.Query().Get("status")
	for _, c := range cs {
		if q != "" && !strings.Contains(strings.ToLower(c.Name), q) && !strings.Contains(strings.ToLower(c.Host), q) {
			continue
		}
		if body.Engine != "" && c.Engine != body.Engine {
			continue
		}
		row := connRow{Connection: c, Jobs: counts[c.ID], StatusClass: connStatusClass(c)}
		if body.Status != "" && row.StatusClass != body.Status {
			continue
		}
		body.Rows = append(body.Rows, row)
	}
	body.Total = len(body.Rows)
	s.render(w, "backups_connections", sess, pageData{Title: "Connections", Active: "backups-connections", Body: body,
		Subtitle:    "Reusable database sources for backup jobs. Test a connection before using it.",
		Breadcrumbs: []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Connections"}}})
}

type backupConnectionDetailBody struct {
	backupRoles
	Conn          connRow
	Tab           string
	Used          []backup.Job
	Drawer        connDrawer
	TestedAtLabel string
}

func (s *Server) pageBackupConnectionDetail(w http.ResponseWriter, r *http.Request, sess *Session) {
	if !s.needDBPage(w) {
		return
	}
	c, ok, err := s.db.st.GetBackupConnection(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, "could not read the connection", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	tab := r.URL.Query().Get("tab")
	if tab != "used-by" {
		tab = "overview"
	}
	_, _, jobs := s.jobCounts(r.Context())
	var used []backup.Job
	for _, j := range jobs {
		if j.ConnectionID == c.ID {
			used = append(used, j)
		}
	}
	body := backupConnectionDetailBody{backupRoles: rolesOf(sess), Tab: tab, Used: used,
		Conn:   connRow{Connection: c, Jobs: len(used), StatusClass: connStatusClass(c)},
		Drawer: connDrawer{Profiles: s.sshProfileViews(r.Context()), StoredAllowed: s.storedAllowed(), Conn: &c}}
	s.render(w, "backups_connection_detail", sess, pageData{Title: c.Name, Active: "backups-connections", Body: body,
		Subtitle:    strings.ToUpper(c.Engine) + " source " + c.Host,
		Breadcrumbs: []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Connections", URL: "/backups/connections"}, {Label: c.Name}}})
}

// ---- policies ----

type policyRow struct {
	backup.Policy
	Jobs      int
	Words     string
	Human     string
	VerifyTxt string
}

func verifyWords(sec int) string {
	switch {
	case sec <= 0:
		return "off"
	case sec%(30*86400) == 0:
		return "monthly"
	case sec%(7*86400) == 0:
		return "weekly"
	case sec%86400 == 0:
		return "daily"
	}
	return "every " + strconv.Itoa(sec/60) + " min"
}

func mkPolicyRow(p backup.Policy, jobs int) policyRow {
	return policyRow{Policy: p, Jobs: jobs, Words: retentionWords(p.Retention), Human: p.Schedule.Human(), VerifyTxt: verifyWords(p.VerifyEverySeconds)}
}

type policyDrawer struct {
	Policy *backup.Policy
	Repos  []backup.Repo
	JSON   string // the policy as JSON for the edit form (no secrets in a policy)
}

type backupPoliciesBody struct {
	backupRoles
	Rows    []policyRow
	Drawer  policyDrawer
	Unavail string
}

func (s *Server) pageBackupPolicies(w http.ResponseWriter, r *http.Request, sess *Session) {
	if !s.needDBPage(w) {
		return
	}
	ps, err := s.db.st.ListBackupPolicies(r.Context())
	body := backupPoliciesBody{backupRoles: rolesOf(sess)}
	if err != nil {
		body.Unavail = "Could not read the backup policies."
	}
	_, counts, _ := s.jobCounts(r.Context())
	for _, p := range ps {
		body.Rows = append(body.Rows, mkPolicyRow(p, counts[p.ID]))
	}
	body.Drawer.Repos, _ = s.db.st.ListBackupRepos(r.Context())
	s.render(w, "backups_policies", sess, pageData{Title: "Policies", Active: "backups-policies", Body: body,
		Subtitle:    "A policy bundles schedule, retention, protection and verification. Jobs follow their policy.",
		Breadcrumbs: []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Policies"}}})
}

type backupPolicyDetailBody struct {
	backupRoles
	Policy   policyRow
	Tab      string
	Used     []backup.Job
	NextRuns []previewRun
	Drawer   policyDrawer
}

func (s *Server) pageBackupPolicyDetail(w http.ResponseWriter, r *http.Request, sess *Session) {
	if !s.needDBPage(w) {
		return
	}
	p, ok, err := s.db.st.GetBackupPolicy(r.Context(), r.PathValue("id"))
	if err != nil {
		http.Error(w, "could not read the policy", http.StatusInternalServerError)
		return
	}
	if !ok {
		http.NotFound(w, r)
		return
	}
	tab := r.URL.Query().Get("tab")
	if tab != "used-by" {
		tab = "overview"
	}
	_, _, jobs := s.jobCounts(r.Context())
	var used []backup.Job
	for _, j := range jobs {
		if j.PolicyID == p.ID {
			used = append(used, j)
		}
	}
	runs, _ := schedulePreview(p.Schedule, s.now(), 5)
	body := backupPolicyDetailBody{backupRoles: rolesOf(sess), Policy: mkPolicyRow(p, len(used)), Tab: tab, Used: used, NextRuns: runs,
		Drawer: policyDrawer{Policy: &p}}
	body.Drawer.Repos, _ = s.db.st.ListBackupRepos(r.Context())
	raw, _ := json.Marshal(p)
	body.Drawer.JSON = string(raw)
	s.render(w, "backups_policy_detail", sess, pageData{Title: p.Name, Active: "backups-policies", Body: body,
		Subtitle:    p.Description,
		Breadcrumbs: []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Policies", URL: "/backups/policies"}, {Label: p.Name}}})
}

// ---- job wizard ----

type wizConn struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Engine     string `json:"engine"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	SecretMode string `json:"secret_mode"`
	Status     string `json:"status"`
}

type wizPolicy struct {
	backup.Policy
	Words string `json:"retention_words"`
	Human string `json:"schedule_human"`
}

type wizRepo struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Bucket     string `json:"bucket"`
	LockMode   string `json:"lock_mode,omitempty"`
	Configured bool   `json:"configured"`
}

type wizData struct {
	Connections   []wizConn   `json:"connections"`
	Policies      []wizPolicy `json:"policies"`
	Repos         []wizRepo   `json:"repos"`
	KeysPresent   bool        `json:"keys_present"`
	KeyID         string      `json:"key_id,omitempty"`
	StoredAllowed bool        `json:"stored_allowed"`
	Job           *backup.Job `json:"job,omitempty"`
	Edit          bool        `json:"edit"`
	Legacy        bool        `json:"legacy,omitempty"`
	// Native tool status (backup_tools.go): tools by name, the tools each method's backups and test
	// restores run.
	Tools            map[string]wizTool  `json:"tools,omitempty"`
	MethodTools      map[string][]string `json:"method_tools,omitempty"`
	TestRestoreTools map[string][]string `json:"test_restore_tools,omitempty"`
	ToolHint         string              `json:"tool_hint,omitempty"`
}

type backupJobWizardBody struct {
	backupRoles
	Edit   bool
	JobID  string
	Data   string // JSON for data-wizard (no secrets: names, hosts, env variable names)
	Drawer connDrawer
	Policy policyDrawer
}

func (s *Server) wizardData(ctx context.Context, job *backup.Job) wizData {
	d := wizData{Connections: []wizConn{}, Policies: []wizPolicy{}, Repos: []wizRepo{}, StoredAllowed: s.storedAllowed(), Job: job, Edit: job != nil}
	cs, _ := s.db.st.ListBackupConnections(ctx)
	for _, c := range cs {
		d.Connections = append(d.Connections, wizConn{ID: c.ID, Name: c.Name, Engine: c.Engine, Host: c.Host, Port: c.Port,
			SecretMode: c.SecretMode, Status: connStatusClass(c)})
	}
	ps, _ := s.db.st.ListBackupPolicies(ctx)
	for _, p := range ps {
		d.Policies = append(d.Policies, wizPolicy{Policy: p, Words: retentionWords(p.Retention), Human: p.Schedule.Human()})
	}
	rs, _ := s.db.st.ListBackupRepos(ctx)
	for _, rp := range rs {
		d.Repos = append(d.Repos, wizRepo{ID: rp.ID, Name: rp.Name, Bucket: rp.Bucket, LockMode: rp.LockMode,
			Configured: os.Getenv(rp.EnvPrefix+"ENDPOINT") != "" && os.Getenv(rp.EnvPrefix+"ACCESS_KEY") != ""})
	}
	if ring, err := backup.LoadKeyring(os.Getenv); err == nil && ring != nil && ring.Active != nil {
		d.KeysPresent, d.KeyID = true, ring.Active.ID
	}
	if job != nil {
		d.Legacy = !job.IsCustom() && job.ConnectionID == ""
	}
	return d
}

func (s *Server) pageBackupJobWizard(edit bool) authedHandler {
	return func(w http.ResponseWriter, r *http.Request, sess *Session) {
		if !s.needDBPage(w) {
			return
		}
		var job *backup.Job
		title, crumb := "New backup job", "New job"
		if edit {
			j, ok, err := s.db.st.GetBackupJob(r.Context(), r.PathValue("id"))
			if err != nil {
				http.Error(w, "could not read the job", http.StatusInternalServerError)
				return
			}
			if !ok {
				http.NotFound(w, r)
				return
			}
			job = &j
			title, crumb = "Edit "+j.Name, "Edit"
		}
		d := s.wizardData(r.Context(), job)
		d.Tools, d.MethodTools, d.TestRestoreTools = wizardTools(r)
		d.ToolHint = runner.ToolHint
		raw, _ := json.Marshal(d)
		body := backupJobWizardBody{backupRoles: rolesOf(sess), Edit: edit, Data: string(raw), Drawer: connDrawer{Profiles: s.sshProfileViews(r.Context()), StoredAllowed: s.storedAllowed(), Wizard: true}}
		if job != nil {
			body.JobID = job.ID
		}
		body.Policy.Repos, _ = s.db.st.ListBackupRepos(r.Context())
		crumbs := []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Jobs", URL: "/backups/jobs"}}
		if edit {
			crumbs = append(crumbs, BreadcrumbItem{Label: job.Name, URL: "/backups/jobs/" + job.ID})
		}
		crumbs = append(crumbs, BreadcrumbItem{Label: crumb})
		s.render(w, "backups_job_wizard", sess, pageData{Title: title, Active: "backups-jobs", Body: body,
			Subtitle:    "Source, schedule and retention, storage and protection, then verification. Test the connection first.",
			Breadcrumbs: crumbs})
	}
}

// ---- job view enrichment (policy / connection names, human schedule, warnings) ----

// enrichJobViews fills the display-only fields of the job views: schedule text, policy and connection names,
// legacy flag and the warnings a policy edit or an unsupported setting produced (recomputed from the stored
// job, so they stay visible after the save response is gone).
func (s *Server) enrichJobViews(ctx context.Context, views []backupJobView) {
	if s.db == nil || len(views) == 0 {
		return
	}
	pols := map[string]backup.Policy{}
	if ps, err := s.db.st.ListBackupPolicies(ctx); err == nil {
		for _, p := range ps {
			pols[p.ID] = p
		}
	}
	conns := map[string]string{}
	if cs, err := s.db.st.ListBackupConnections(ctx); err == nil {
		for _, c := range cs {
			conns[c.ID] = c.Name
		}
	}
	for i := range views {
		j := views[i].Job
		views[i].ScheduleText = j.EffectiveSchedule().Human()
		if j.ConnectionID != "" {
			views[i].ConnectionName = conns[j.ConnectionID]
			if views[i].ConnectionName == "" {
				views[i].ConnectionName = j.ConnectionID
			}
		}
		views[i].Legacy = !j.IsCustom() && j.ConnectionID == ""
		var tm *runner.ToolMissingError
		if err := jobToolError(j); errors.As(err, &tm) {
			views[i].ToolError, views[i].ToolsMissing = tm.Error(), tm.Tools()
		}
		var warns []string
		if w := views[i].Warning; w != "" {
			warns = append(warns, w)
		}
		if p, ok := pols[j.PolicyID]; ok {
			views[i].PolicyName = p.Name
			if _, pw := backup.ApplyPolicy(j, p); len(pw) > 0 {
				warns = append(warns, pw...)
			}
		}
		sort.Strings(warns)
		views[i].Warnings = warns
	}
}

// ---- legacy job -> connection ----

// apiBackupJobConvertConnection turns the inline connection of a legacy built-in job into a saved
// connection (environment-mode: the job already names its password variable) and points the job at it.
// Admin only (the fail-closed RBAC default for a write route); audited; no secret is read or returned.
func (s *Server) apiBackupJobConvertConnection(w http.ResponseWriter, r *http.Request, sess *Session) {
	if !s.needDB(w) {
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if r.ContentLength != 0 && !decodeStrict(w, r, &req) {
		return
	}
	j, ok, err := s.db.st.GetBackupJob(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup job")
		return
	}
	if !ok {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	if j.IsCustom() {
		writeErr(w, http.StatusBadRequest, "custom jobs do not use connections")
		return
	}
	if j.ConnectionID != "" {
		writeErr(w, http.StatusConflict, "job already uses connection "+j.ConnectionID)
		return
	}
	engine := j.Engine()
	switch {
	case j.Method == backup.MethodMariaDBDump:
		engine = backup.ConnMariaDB
	case engine == backup.EngineMySQL:
		engine = backup.ConnMySQL
	}
	existing, err := s.db.st.ListBackupConnections(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read backup connections")
		return
	}
	port := j.Conn.Port
	if port == 0 {
		port = backup.DefaultPort(engine)
	}
	var conn backup.Connection
	reused := false
	for _, c := range existing {
		if c.SecretMode == backup.SecretEnv && c.Engine == engine && c.Host == j.Conn.Host && c.Port == port && c.User == j.Conn.User &&
			c.PasswordEnv == j.Conn.PasswordEnv && c.URIEnv == j.Conn.URIEnv {
			conn, reused = c, true
			break
		}
	}
	if !reused {
		id, err := randomConnID()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "could not create a connection id")
			return
		}
		name := strings.TrimSpace(req.Name)
		if name == "" {
			name = j.Source
		}
		if name == "" {
			name = fmt.Sprintf("%s-%s", engine, j.Conn.Host)
		}
		taken := map[string]bool{}
		for _, c := range existing {
			taken[c.Name] = true
		}
		base := name
		for n := 2; taken[name]; n++ {
			name = fmt.Sprintf("%s-%d", base, n)
		}
		conn = backup.Connection{ID: id, Name: name, Engine: engine, Host: j.Conn.Host, Port: port, User: j.Conn.User,
			SecretMode: backup.SecretEnv, PasswordEnv: j.Conn.PasswordEnv, URIEnv: j.Conn.URIEnv, CreatedBy: sess.Username}
		if err := conn.Validate(); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	nj, err := backup.ApplyConnection(j, conn)
	if err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	nj.ConnectionID = conn.ID
	if !s.auditBackupOp(w, r, "backup_job.convert_connection", map[string]any{"job": j.ID, "connection": conn.ID, "name": conn.Name, "reused": reused}) {
		return
	}
	if !reused {
		if conn, err = s.db.st.CreateBackupConnection(r.Context(), conn); err != nil {
			code := http.StatusInternalServerError
			if errors.Is(err, backup.ErrConflict) {
				code = http.StatusConflict
			}
			writeErr(w, code, "could not create the connection: "+err.Error())
			return
		}
	}
	if changed, err := s.db.st.UpdateBackupJob(r.Context(), nj); err != nil || !changed {
		writeErr(w, http.StatusInternalServerError, "could not update the backup job")
		return
	}
	// The effective connection is unchanged (same host, user and variable names), so the running scheduler
	// needs no restart.
	writeJSON(w, http.StatusOK, map[string]any{"job_id": j.ID, "connection": conn, "reused": reused})
}
