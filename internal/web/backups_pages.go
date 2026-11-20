package web

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// backupsJobsPageBody is the template body for /backups/jobs.
type backupsJobsPageBody struct {
	Jobs          []backupJobView
	FilterQ       string
	Engine        string
	Status        string
	Repo          string
	Policy        string
	Policies      []backup.Policy
	Repos         []backup.Repo
	Methods       []string
	MethodOptions []backupMethodOption
	CanWrite      bool
	IsAdmin       bool
	TotalCount    int
}

// backupsJobDetailPageBody is the template body for /backups/jobs/{id}.
type backupsJobDetailPageBody struct {
	Job        backupJobView
	ActiveTab  string
	Runs       []backupRunView
	RunsCursor string
	RunsPaged  bool
	Chart30    template.HTML
	Backups    []backup.Record
	Audit      []store.AuditRow
	CanWrite   bool
	IsAdmin    bool
}

type backupRunView struct {
	backup.Run
	RemediationHint string
}

// backupsCatalogPageBody is the template body for /backups/catalog.
type backupsCatalogPageBody struct {
	Backups    []backup.Record
	NextCursor string
	Paged      bool
	Source     string
	Engine     string
	Kind       string
	Status     string
	Repo       string
	Repos      []backup.Repo
	Sources    []string
	CanWrite   bool
	IsAdmin    bool
}

// backupsCatalogDetailPageBody is the template body for /backups/catalog/{id}.
type backupsCatalogDetailPageBody struct {
	Backup   backup.Record
	Checks   []backup.CheckResult
	CanWrite bool
	IsAdmin  bool
}

// backupsRestorePageBody is the template body for /backups/restore.
type backupsRestorePageBody struct {
	SelectedBackup string
	SelectedSource string
	Backups        []backup.Record
	Sources        []string
	CanWrite       bool
	IsAdmin        bool
}

func (s *Server) pageBackupsJobs(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil || s.db.st == nil {
		http.Error(w, "backup service unavailable: configure metadata database", http.StatusServiceUnavailable)
		return
	}
	allJobs, err := s.backupJobViews(r.Context())
	if err != nil {
		s.log.Error("list backup jobs", "error", err)
	}
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
	engine := r.URL.Query().Get("engine")
	status := r.URL.Query().Get("status")
	repo := r.URL.Query().Get("repo")
	policyFilter := r.URL.Query().Get("policy")

	var filtered []backupJobView
	for _, j := range allJobs {
		if q != "" && !strings.Contains(strings.ToLower(j.Job.Name), q) && !strings.Contains(strings.ToLower(j.Job.Source), q) {
			continue
		}
		if engine != "" && j.Job.Engine() != engine {
			continue
		}
		if status == "enabled" && !j.Job.Enabled {
			continue
		}
		if status == "paused" && j.Job.Enabled {
			continue
		}
		if status == "failing" && j.Job.ConsecutiveFailures == 0 {
			continue
		}
		if status == "missed" && j.Job.LastRunStatus != backup.RunStatusMissed {
			continue
		}
		if policyFilter != "" && j.Job.PolicyID != policyFilter {
			continue
		}
		if repo != "" && j.Job.Bucket != repo {
			continue
		}
		runs, _ := s.db.st.ListBackupRuns(r.Context(), store.BackupRunFilter{JobID: j.Job.ID, Limit: 10})
		j.Last10Runs = runs
		filtered = append(filtered, j)
	}

	repos, _ := s.db.st.ListBackupRepos(r.Context())
	policies, _ := s.db.st.ListBackupPolicies(r.Context())
	methods := []string{backup.MethodCustom, backup.MethodMySQLDump, backup.MethodMariaDBDump, backup.MethodMydumper,
		backup.MethodPGDump, backup.MethodPGBaseBackup, backup.MethodMongodump}
	var methodOptions []backupMethodOption
	for _, m := range methods {
		if eng := backup.MethodEngine(m); eng != "" {
			methodOptions = append(methodOptions, backupMethodOption{Value: m, Engine: eng})
		}
	}

	canWrite := sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator)
	isAdmin := sess != nil && sess.Role == store.RoleAdmin

	body := backupsJobsPageBody{
		Jobs:          filtered,
		FilterQ:       q,
		Engine:        engine,
		Status:        status,
		Repo:          repo,
		Policy:        policyFilter,
		Policies:      policies,
		Repos:         repos,
		Methods:       methods,
		MethodOptions: methodOptions,
		CanWrite:      canWrite,
		IsAdmin:       isAdmin,
		TotalCount:    len(filtered),
	}
	s.render(w, "backups_jobs", sess, pageData{Title: "Backup Jobs", Active: "backups-jobs", Refresh: 10, Body: body})
}

func (s *Server) pageBackupsJobDetail(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil || s.db.st == nil {
		http.Error(w, "backup service unavailable: configure metadata database", http.StatusServiceUnavailable)
		return
	}
	id := r.PathValue("id")
	job, found, err := s.db.st.GetBackupJob(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	jobView := backupJobView{Job: job}
	if s.jobs != nil {
		if st, ok := s.jobs.Get(job.ID); ok {
			jobView.Process = &st
		}
	}
	if err := backup.ValidateJob(job); err != nil {
		jobView.Warning = err.Error()
	}
	if err := backup.ValidatePITR(job); err != nil && jobView.Warning == "" {
		jobView.Warning = err.Error()
	}
	one := []backupJobView{jobView}
	s.enrichJobViews(r.Context(), one)
	jobView = one[0]

	tab := r.URL.Query().Get("tab")
	if tab == "" {
		tab = "overview"
	}

	limit := 30
	runFilter := store.BackupRunFilter{
		JobID: id,
		Limit: limit,
	}
	var runsPaged bool
	if cur := r.URL.Query().Get("cursor"); cur != "" {
		if c, err := backup.ParseCursor(cur); err == nil {
			runFilter.After = &c
			runsPaged = true
		}
	}
	runs, _ := s.db.st.ListBackupRuns(r.Context(), runFilter)
	var runViews []backupRunView
	for _, run := range runs {
		runViews = append(runViews, backupRunView{
			Run:             run,
			RemediationHint: run.Remediation(),
		})
	}
	nextCursor := backup.RunNextCursor(runs, limit)

	backups, _ := s.db.st.ListBackups(r.Context(), backup.Filter{JobID: id, Limit: 50})

	allAudit, _ := s.db.st.ListAudit(r.Context(), 100)
	var jobAudit []store.AuditRow
	for _, a := range allAudit {
		if a.Pipeline == backupJobPipeline(id) || strings.Contains(string(a.Data), id) {
			jobAudit = append(jobAudit, a)
		}
	}

	chart := JobRuns30Chart(runs, 600, 160)

	canWrite := sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator)
	isAdmin := sess != nil && sess.Role == store.RoleAdmin

	body := backupsJobDetailPageBody{
		Job:        jobView,
		ActiveTab:  tab,
		Runs:       runViews,
		RunsCursor: nextCursor,
		RunsPaged:  runsPaged,
		Chart30:    chart,
		Backups:    backups,
		Audit:      jobAudit,
		CanWrite:   canWrite,
		IsAdmin:    isAdmin,
	}
	s.render(w, "backups_job_detail", sess, pageData{Title: job.Name, Active: "backups-jobs", Refresh: 10, Body: body})
}

func (s *Server) pageBackupsCatalog(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.backups == nil {
		http.Error(w, "backup catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	limit := 50
	f := backup.Filter{Limit: limit}
	source := r.URL.Query().Get("source")
	engine := r.URL.Query().Get("engine")
	kind := r.URL.Query().Get("kind")
	status := r.URL.Query().Get("status")
	repo := r.URL.Query().Get("repo")

	f.Source = source
	f.Engine = engine
	f.Kind = kind
	f.Status = backup.Status(status)
	f.Repository = repo

	var paged bool
	if v := r.URL.Query().Get("cursor"); v != "" {
		c, err := backup.ParseCursor(v)
		if err == nil {
			f.After = &c
			paged = true
		}
	}
	items, err := s.backups.List(r.Context(), f)
	if err != nil {
		s.log.Error("list backup catalog", "error", err)
	}
	nextCursor := backup.NextCursor(items, f.Limit)
	repos, _ := s.db.st.ListBackupRepos(r.Context())

	canWrite := sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator)
	isAdmin := sess != nil && sess.Role == store.RoleAdmin

	body := backupsCatalogPageBody{
		Backups:    items,
		NextCursor: nextCursor,
		Paged:      paged,
		Source:     source,
		Engine:     engine,
		Kind:       kind,
		Status:     status,
		Repo:       repo,
		Repos:      repos,
		CanWrite:   canWrite,
		IsAdmin:    isAdmin,
	}
	s.render(w, "backups_catalog", sess, pageData{Title: "Restore Points", Active: "backups-catalog", Refresh: 10, Body: body})
}

func (s *Server) pageBackupsCatalogDetail(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.backups == nil {
		http.Error(w, "backup catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	id := r.PathValue("id")
	rec, found, err := s.backups.Get(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	checks, _ := s.backups.Checks(r.Context(), id, 20)

	canWrite := sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator)
	isAdmin := sess != nil && sess.Role == store.RoleAdmin

	body := backupsCatalogDetailPageBody{
		Backup:   rec,
		Checks:   checks,
		CanWrite: canWrite,
		IsAdmin:  isAdmin,
	}
	s.render(w, "backups_catalog_detail", sess, pageData{Title: rec.ID, Active: "backups-catalog", Body: body})
}

func (s *Server) pageBackupsRestore(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.backups == nil {
		http.Error(w, "backup catalog unavailable", http.StatusServiceUnavailable)
		return
	}
	selectedBackup := r.URL.Query().Get("backup")
	selectedSource := r.URL.Query().Get("source")
	backups, _ := s.backups.List(r.Context(), backup.Filter{Limit: 100})

	sourcesMap := map[string]bool{}
	var sources []string
	for _, b := range backups {
		if !sourcesMap[b.Source] && b.Source != "" {
			sourcesMap[b.Source] = true
			sources = append(sources, b.Source)
		}
	}

	canWrite := sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator)
	isAdmin := sess != nil && sess.Role == store.RoleAdmin

	body := backupsRestorePageBody{
		SelectedBackup: selectedBackup,
		SelectedSource: selectedSource,
		Backups:        backups,
		Sources:        sources,
		CanWrite:       canWrite,
		IsAdmin:        isAdmin,
	}
	s.render(w, "backups_restore", sess, pageData{Title: "Restore", Active: "backups-restore", Body: body})
}

func (s *Server) apiBackupJobRuns(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.db == nil || s.db.st == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup metadata database unavailable")
		return
	}
	id := r.PathValue("id")
	if _, found, err := s.db.st.GetBackupJob(r.Context(), id); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	} else if !found {
		writeErr(w, http.StatusNotFound, "backup job not found")
		return
	}
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}
	filter := store.BackupRunFilter{
		JobID: id,
		Limit: limit,
	}
	if cur := r.URL.Query().Get("cursor"); cur != "" {
		if c, err := backup.ParseCursor(cur); err == nil {
			filter.After = &c
		}
	}
	runs, err := s.db.st.ListBackupRuns(r.Context(), filter)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	type runView struct {
		backup.Run
		RemediationHint string `json:"remediation_hint,omitempty"`
	}
	views := make([]runView, len(runs))
	for i, run := range runs {
		views[i] = runView{
			Run:             run,
			RemediationHint: run.Remediation(),
		}
	}
	next := backup.RunNextCursor(runs, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"runs":        views,
		"next_cursor": next,
	})
}

func (s *Server) apiBackupRunCancel(w http.ResponseWriter, r *http.Request, sess *Session) {
	if s.db == nil || s.db.st == nil {
		writeErr(w, http.StatusServiceUnavailable, "backup metadata database unavailable")
		return
	}
	id := r.PathValue("id")
	run, found, err := s.db.st.GetBackupRun(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup run not found")
		return
	}
	if run.Status != string(backup.RunStatusRunning) {
		writeErr(w, http.StatusConflict, fmt.Sprintf("backup run is not running (status: %s)", run.Status))
		return
	}
	var ownedPID int
	if pidVal, ok := s.ownedBackupProcs.Load(run.ProcessID); ok {
		ownedPID = pidVal.(int)
	} else if pidVal, ok := s.ownedBackupProcs.Load(run.ID); ok {
		ownedPID = pidVal.(int)
	} else {
		s.ownedBackupProcs.Range(func(k, v any) bool {
			if strconv.Itoa(v.(int)) == run.ProcessID {
				ownedPID = v.(int)
				return false
			}
			return true
		})
	}
	if ownedPID == 0 {
		writeErr(w, http.StatusConflict, "backup run is owned by another instance or not running on this host")
		return
	}
	// Cancelling kills the job's scheduler process, which the reconciler restarts while the job is
	// enabled. Move the due time to the next slot first, or the restarted scheduler would see the run
	// still due and start the same backup again at once.
	if job, ok, err := s.db.st.GetBackupJob(r.Context(), run.JobID); err == nil && ok && job.Enabled {
		now := s.now().UTC()
		next := now.Add(time.Duration(job.EverySeconds) * time.Second)
		if job.Schedule != nil && job.Schedule.Kind != backup.ScheduleInterval {
			if n, err := job.Schedule.Next(now, now); err == nil {
				next = n
			}
		}
		if moved, err := s.db.st.SetBackupJobNextRun(r.Context(), job.ID, job.UpdatedAt, next); err != nil || !moved {
			writeErr(w, http.StatusConflict, "backup job changed concurrently; retry the cancel")
			return
		}
	}
	if syscall.Kill(ownedPID, 0) == nil {
		if err := syscall.Kill(-ownedPID, syscall.SIGTERM); err != nil {
			_ = syscall.Kill(ownedPID, syscall.SIGTERM)
		}
	}
	if s.jobs != nil {
		_ = s.jobs.Cancel(run.ProcessID)
	}
	now := time.Now().UTC()
	dur := now.Sub(run.StartedAt)
	_, _, _ = s.db.st.FinishBackupRun(r.Context(), run.ID, now, string(backup.RunStatusCancelled), run.BytesRead, run.BytesWritten, dur, run.BackupID, backup.ErrCodeCancelled, "cancelled by operator")
	_, _ = s.db.st.FinishBackupJobRun(r.Context(), run.JobID, run.StartedAt, backup.RunStatusCancelled)
	if s.audit != nil {
		_ = s.audit.Log(AuditEntry{
			IP:       s.clientIP(r),
			User:     sess.Username,
			Action:   "backup_run.cancel",
			Pipeline: run.JobID,
			Result:   "ok",
			Detail:   map[string]any{"run_id": run.ID},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "run_id": run.ID, "status": backup.RunStatusCancelled})
}
