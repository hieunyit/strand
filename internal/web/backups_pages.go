package web

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/runner"
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
	Protection   backupJobProtection
	LatestBackup *backup.Record
	RPO          backup.RPOStatus
	LastSuccess  *time.Time
	Job          backupJobView
	ActiveTab    string
	Runs         []backupRunView
	RunsCursor   string
	RunsPaged    bool
	RunProcess   string
	Chart30      template.HTML
	Backups      []backup.Record
	Audit        []store.AuditRow
	CanWrite     bool
	IsAdmin      bool
}

type backupRunView struct {
	backup.Run
	RemediationHint string
	LogURL          string
}

// backupsCatalogPageBody is the template body for /backups/catalog.
type backupsCatalogPageBody struct {
	Backups         []backup.Record
	NextCursor      string
	Paged           bool
	Source          string
	Engine          string
	Kind            string
	Status          string
	Repo            string
	Repos           []backup.Repo
	From            string
	To              string
	LegalHold       string
	RecoverySources []catalogRecoverySource
	CanWrite        bool
	IsAdmin         bool
}

// backupsCatalogDetailPageBody is the template body for /backups/catalog/{id}.
type backupsCatalogDetailPageBody struct {
	Backup   backup.Record
	Checks   []backup.CheckResult
	Repos    []backup.Repo
	Copies   []backup.Record
	CanWrite bool
	IsAdmin  bool
	// Run is the job run that produced the record (its error for a failed record), when it can be found.
	Run *backup.Run
	// CanDelete: the record is not complete, the user may delete it and nothing blocks it (hold, lock,
	// a still running writer); DeleteBlocked explains a block to an operator.
	CanDelete     bool
	DeleteBlocked string
}

type catalogRecoverySource struct {
	Name       string
	Latest     *backup.Record
	Window     runner.Window
	RestoreURL string
	HasLogs    bool
	LogBreaks  int
	Records    []backup.Record
}

func catalogRecoverySources(items []backup.Record) []catalogRecoverySource {
	bySource := make(map[string][]backup.Record)
	for _, item := range items {
		bySource[item.Source] = append(bySource[item.Source], item)
	}
	out := make([]catalogRecoverySource, 0, len(bySource))
	for name, records := range bySource {
		row := catalogRecoverySource{Name: name, Records: records, Window: runner.RecoveryWindow(records)}
		row.LogBreaks = catalogLogBreaks(records)
		for _, record := range records {
			if record.Kind == backup.KindLog {
				row.HasLogs = row.HasLogs || record.Status == backup.StatusComplete
				continue
			}
			if record.Status == backup.StatusComplete && runner.RequireStandaloneBackup(record) == nil &&
				(row.Latest == nil || runner.ConsistentTime(record).After(runner.ConsistentTime(*row.Latest))) {
				latest := record
				row.Latest = &latest
			}
		}
		if row.Window.Continuous && row.Window.End != nil {
			row.RestoreURL = "/backups/restore?" + (url.Values{
				"source": {name}, "target_time": {row.Window.End.Format(time.RFC3339)},
			}).Encode()
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func catalogLogBreaks(records []backup.Record) int {
	type lineage struct{ engine, method, job, identity, timeline string }
	groups := make(map[lineage][]backup.Record)
	for _, record := range records {
		identity := record.Details["source_identity"]
		if record.Kind == backup.KindLog && record.Status == backup.StatusComplete && record.CopyOf == "" &&
			identity != "" && record.JobID != "" && record.StartTime != nil && record.EndTime != nil {
			key := lineage{record.Engine, record.Method, record.JobID, identity, record.Details["timeline"]}
			groups[key] = append(groups[key], record)
		}
	}
	breaks := 0
	for _, logs := range groups {
		sort.Slice(logs, func(i, j int) bool { return logs[i].StartTime.Before(*logs[j].StartTime) })
		for i := 1; i < len(logs); i++ {
			if !runner.Contiguous(logs[i-1], logs[i]) {
				breaks++
			}
		}
	}
	return breaks
}

type restoreSourceInfo struct {
	Source         string          `json:"source"`
	Engine         string          `json:"engine"`
	WindowStart    *time.Time      `json:"window_start,omitempty"`
	WindowEnd      *time.Time      `json:"window_end,omitempty"`
	Continuous     bool            `json:"continuous"`
	VerifyOnReplay bool            `json:"verify_on_replay,omitempty"`
	BaseBackups    []backup.Record `json:"base_backups"`
	LogsCount      int             `json:"logs_count"`
}

type restoreConnView struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Engine string `json:"engine"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	User   string `json:"user"`
}

// backupsRestorePageBody is the template body for /backups/restore.
type backupsRestorePageBody struct {
	// SelectedError: the backup in ?backup= cannot be restored (unknown or not complete); it is not preselected.
	SelectedError      string
	SelectedBackup     string
	SelectedSource     string
	SelectedTargetTime string
	Backups            []backup.Record
	Sources            []string
	Connections        []restoreConnView
	SourceInfos        []restoreSourceInfo
	WizardJSON         template.JS
	CanWrite           bool
	IsAdmin            bool
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
	processID := r.URL.Query().Get("process")
	if tab != "runs" || !JobIDRE.MatchString(processID) {
		processID = ""
	}
	runFilter := store.BackupRunFilter{
		ProcessID: processID,
		JobID:     id,
		Limit:     limit,
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
		view := backupRunView{Run: run, RemediationHint: run.Remediation()}
		if s.jobs != nil && JobIDRE.MatchString(run.ProcessID) {
			if process, ok := s.jobs.Get(run.ProcessID); ok &&
				(process.Pipeline == backupJobPipeline(id) || process.Pipeline == backupJobRunPipeline(id)) {
				view.LogURL = "/jobs/" + run.ProcessID
			}
		}
		runViews = append(runViews, view)
	}
	nextCursor := backup.RunNextCursor(runs, limit)

	backups, _ := s.db.st.ListBackups(r.Context(), backup.Filter{JobID: id, Limit: 50})
	var latest *backup.Record
	var lastSuccess *time.Time
	completed, _ := s.db.st.ListBackups(r.Context(), backup.Filter{JobID: id, Status: backup.StatusComplete, NotKind: backup.KindLog, Limit: 50})
	for _, b := range completed {
		if latest == nil || runner.ConsistentTime(b).After(runner.ConsistentTime(*latest)) {
			record := b
			latest = &record
		}
	}
	if job.LastRunStatus == backup.RunStatusSuccess {
		lastSuccess = job.RunFinishedAt
	}
	if lastSuccess == nil {
		successes, _ := s.db.st.ListBackupRuns(r.Context(), store.BackupRunFilter{JobID: id, Status: string(backup.RunStatusSuccess), Limit: 1})
		if len(successes) > 0 {
			lastSuccess = successes[0].FinishedAt
		}
	}
	rpoInput := backup.RPOInput{Source: job.Source, JobID: id, JobName: job.Name, Engine: job.Engine(), PolicyID: job.PolicyID}
	if latest != nil {
		at := runner.ConsistentTime(*latest)
		rpoInput.LastBackup = &at
	}
	if job.PolicyID != "" {
		if policy, ok, err := s.db.st.GetBackupPolicy(r.Context(), job.PolicyID); ok && err == nil {
			rpoInput.TargetMinutes, rpoInput.RTOMinutes = policy.RPOMinutes, policy.RTOMinutes
		}
	}
	if job.LogArchive {
		logs, _ := s.db.st.ListBackups(r.Context(), backup.Filter{JobID: id, Status: backup.StatusComplete, Kind: backup.KindLog, Limit: 50})
		window := runner.RecoveryWindow(append(completed, logs...))
		if window.Continuous {
			rpoInput.PITR, rpoInput.LastLogEnd = true, window.End
		}
	}

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
		Protection:   jobEncryption(job),
		LatestBackup: latest, LastSuccess: lastSuccess, RPO: backup.ComputeRPO(s.now(), rpoInput),
		Job:        jobView,
		ActiveTab:  tab,
		Runs:       runViews,
		RunsCursor: nextCursor,
		RunsPaged:  runsPaged,
		RunProcess: processID,
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
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	legalHold := r.URL.Query().Get("legal_hold")

	f.Source = source
	f.Engine = engine
	f.Kind = kind
	f.Status = backup.Status(status)
	f.Repository = repo
	if legalHold == "1" {
		t := true
		f.LegalHold = &t
	} else if legalHold == "0" {
		fVal := false
		f.LegalHold = &fVal
	}

	parseDate := func(str string, endOfDay bool) *time.Time {
		str = strings.TrimSpace(str)
		if str == "" {
			return nil
		}
		for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02"} {
			if t, err := time.ParseInLocation(layout, str, webTimeZone); err == nil {
				if endOfDay && layout == "2006-01-02" {
					t = t.Add(24*time.Hour - time.Nanosecond)
				}
				return &t
			}
		}
		return nil
	}

	if t := parseDate(from, false); t != nil {
		f.CreatedAfter = t
	}
	if t := parseDate(to, true); t != nil {
		f.CreatedBefore = t
	}

	var paged bool
	if v := r.URL.Query().Get("cursor"); v != "" {
		c, err := backup.ParseCursor(v)
		if err != nil {
			http.Error(w, "invalid cursor", http.StatusBadRequest)
			return
		}
		f.After = &c
		paged = true
	}
	items, err := s.backups.List(r.Context(), f)
	if err != nil {
		s.log.Error("list backup catalog", "error", err)
	}

	nextCursor := backup.NextCursor(items, f.Limit)
	var repos []backup.Repo
	if s.db != nil && s.db.st != nil {
		repos, _ = s.db.st.ListBackupRepos(r.Context())
	}

	canWrite := sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator)
	isAdmin := sess != nil && sess.Role == store.RoleAdmin

	body := backupsCatalogPageBody{
		Backups:         items,
		NextCursor:      nextCursor,
		Paged:           paged,
		Source:          source,
		Engine:          engine,
		Kind:            kind,
		Status:          status,
		Repo:            repo,
		Repos:           repos,
		From:            from,
		To:              to,
		LegalHold:       legalHold,
		RecoverySources: catalogRecoverySources(items),
		CanWrite:        canWrite,
		IsAdmin:         isAdmin,
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
	var repos []backup.Repo
	if s.db != nil && s.db.st != nil {
		repos, _ = s.db.st.ListBackupRepos(r.Context())
	}
	var copies []backup.Record
	if all, err := s.backups.List(r.Context(), backup.Filter{Limit: 100}); err == nil {
		for _, b := range all {
			if b.CopyOf == id || (b.ParentID == id && b.Kind == backup.KindIncremental) {
				copies = append(copies, b)
			}
		}
	}

	canWrite := sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator)
	isAdmin := sess != nil && sess.Role == store.RoleAdmin

	body := backupsCatalogDetailPageBody{
		Backup:   rec,
		Checks:   checks,
		Repos:    repos,
		Copies:   copies,
		CanWrite: canWrite,
		IsAdmin:  isAdmin,
	}
	if rec.Status == backup.StatusFailed {
		body.Run = s.recordRun(r.Context(), rec)
	}
	if rec.Status != backup.StatusComplete && canWrite && s.db != nil {
		var alive bool
		var why string
		if backup.InProgress(rec.Status) {
			alive, why = s.backupWriterRunning(r.Context(), rec)
		}
		if err := backup.DeletableErr(rec, s.now()); err != nil {
			body.DeleteBlocked = catalogMsg(err)
		} else if alive {
			body.DeleteBlocked = why
		} else {
			body.CanDelete = true
		}
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
	var selectedTargetTime string
	if selectedSource != "" {
		if target, err := time.Parse(time.RFC3339, r.URL.Query().Get("target_time")); err == nil {
			selectedTargetTime = target.In(webTimeZone).Format("2006-01-02T15:04:05")
		}
	}
	var selectedError string
	if selectedBackup != "" {
		if rec, found, err := s.backups.Get(r.Context(), selectedBackup); err != nil || !found {
			selectedError, selectedBackup = "restore point "+selectedBackup+" was not found in the catalog", ""
		} else if err := backup.RequireComplete(rec, "restored"); err != nil {
			selectedError, selectedBackup = catalogMsg(err), ""
		} else if err := runner.RequireStandaloneBackup(rec); err != nil {
			selectedError, selectedBackup = err.Error(), ""
		}
	}
	backups, _ := s.backups.ListAll(r.Context(), backup.Filter{Status: backup.StatusComplete}, 1000)

	sourcesMap := map[string]bool{}
	var sources []string
	for _, b := range backups {
		if !sourcesMap[b.Source] && b.Source != "" {
			sourcesMap[b.Source] = true
			sources = append(sources, b.Source)
		}
	}
	if s.db != nil && s.db.st != nil {
		if jobs, err := s.db.st.ListBackupJobs(r.Context()); err == nil {
			for _, j := range jobs {
				if j.Source != "" && !sourcesMap[j.Source] {
					sourcesMap[j.Source] = true
					sources = append(sources, j.Source)
				}
			}
		}
	}
	sort.Strings(sources)

	// Embed objects only for the first N backups (N=10) so the embedded wizard JSON does not blow up.
	// Subsequent backups fetch details on demand via GET /api/v1/backups/{id}.
	const maxEmbedObjects = 10
	embedBackups := make([]backup.Record, 0, len(backups))
	for _, b := range backups {
		if runner.RequireStandaloneBackup(b) != nil {
			continue
		}
		if len(embedBackups) >= maxEmbedObjects && b.Details["objects"] != "" {
			bCopy := b
			bCopy.Details = make(map[string]string, len(b.Details))
			for k, v := range b.Details {
				if k != "objects" {
					bCopy.Details[k] = v
				}
			}
			b = bCopy
		}
		embedBackups = append(embedBackups, b)
	}

	var sourceInfos []restoreSourceInfo
	for _, src := range sources {
		win, _ := s.recoveryWindow(r.Context(), src)
		var baseBackups []backup.Record
		logsCount := 0
		var engine string
		for _, b := range embedBackups {
			if b.Source == src {
				if engine == "" && b.Engine != "" {
					engine = b.Engine
				}
				if b.Kind == backup.KindLog {
					logsCount++
				} else {
					baseBackups = append(baseBackups, b)
				}
			}
		}
		sourceInfos = append(sourceInfos, restoreSourceInfo{
			Source:         src,
			Engine:         engine,
			WindowStart:    win.Start,
			WindowEnd:      win.End,
			Continuous:     win.Continuous,
			VerifyOnReplay: win.VerifyOnReplay,
			BaseBackups:    baseBackups,
			LogsCount:      logsCount,
		})
	}

	var conns []restoreConnView
	if s.db != nil && s.db.st != nil {
		dbConns, _ := s.db.st.ListBackupConnections(r.Context())
		for _, c := range dbConns {
			conns = append(conns, restoreConnView{
				ID:     c.ID,
				Name:   c.Name,
				Engine: c.Engine,
				Host:   c.Host,
				Port:   c.Port,
				User:   c.User,
			})
		}
	}

	wizData := map[string]any{
		"sources":         sourceInfos,
		"connections":     conns,
		"backups":         embedBackups,
		"selected_backup": selectedBackup,
		"selected_source": selectedSource,
	}
	wizBytes, _ := json.Marshal(wizData)

	canWrite := sess != nil && (sess.Role == store.RoleAdmin || sess.Role == store.RoleOperator)
	isAdmin := sess != nil && sess.Role == store.RoleAdmin

	body := backupsRestorePageBody{
		SelectedError:      selectedError,
		SelectedBackup:     selectedBackup,
		SelectedSource:     selectedSource,
		SelectedTargetTime: selectedTargetTime,
		Backups:            backups,
		Sources:            sources,
		Connections:        conns,
		SourceInfos:        sourceInfos,
		WizardJSON:         template.JS(wizBytes),
		CanWrite:           canWrite,
		IsAdmin:            isAdmin,
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
	pid, owned := s.ownedBackupProcs.Load(run.ProcessID)
	if !owned || s.jobs == nil || !JobIDRE.MatchString(run.ProcessID) {
		writeErr(w, http.StatusConflict, "backup run is owned by another instance or not running on this host")
		return
	}
	process, ok := s.jobs.Get(run.ProcessID)
	if !ok || !process.Running || process.PID != pid.(int) ||
		(process.Pipeline != backupJobPipeline(run.JobID) && process.Pipeline != backupJobRunPipeline(run.JobID)) {
		writeErr(w, http.StatusConflict, "backup run process is no longer active for this job")
		return
	}
	if runner, ok := s.jobs.(*FileJobRunner); ok && !jobProcessMatches(process.PID, process.ID, runner.bin, runner.logPath(process.ID)) {
		writeErr(w, http.StatusConflict, "backup run process identity could not be verified")
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
	if err := s.jobs.Cancel(run.ProcessID); err != nil {
		writeErr(w, http.StatusConflict, "backup run could not be cancelled: "+err.Error())
		return
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
