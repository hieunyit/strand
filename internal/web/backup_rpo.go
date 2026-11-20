package web

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/runner"
	"strand/internal/web/store"
)

// RPO / SLA and compliance. current_rpo = now - latest restore point of a source: the last archived log
// segment when the job archives logs (PITR), otherwise the newest complete backup (backup.ComputeRPO). The
// target is the policy's rpo_minutes. A source over its target raises an rpo_breach notification once per
// episode (re-sent every rpoRenotify while it lasts), is exported as strand_backup_rpo_seconds{source} and is
// listed in the compliance report.

const rpoRenotify = 6 * time.Hour

func (s *Server) backupRPORoutes(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/backup-rpo", s.auth(true, s.apiBackupRPO))
	mux.Handle("GET /api/v1/backup-reports/compliance", s.auth(true, s.apiBackupCompliance))
	mux.Handle("GET /api/v1/backup-reports/compliance.csv", s.auth(true, s.apiBackupComplianceCSV))
	mux.Handle("GET /api/v1/backup-activity", s.auth(true, s.apiBackupActivity))
}

// rpoStatuses computes the RPO of every source (one per source; a source with several jobs uses the first
// enabled one).
func (s *Server) rpoStatuses(ctx context.Context) ([]backup.RPOStatus, map[string]backup.Job, error) {
	sums, err := s.db.st.BackupSummaries(ctx)
	if err != nil {
		return nil, nil, err
	}
	jobs, err := s.db.st.ListBackupJobs(ctx)
	if err != nil {
		return nil, nil, err
	}
	policies := map[string]backup.Policy{}
	if ps, err := s.db.st.ListBackupPolicies(ctx); err == nil {
		for _, p := range ps {
			policies[p.ID] = p
		}
	}
	sumBy := map[string]backup.SourceSummary{}
	for _, sm := range sums {
		sumBy[sm.Source] = sm
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	jobBy := map[string]backup.Job{}
	for _, j := range jobs {
		if j.Source == "" {
			continue
		}
		if cur, ok := jobBy[j.Source]; !ok || (!cur.Enabled && j.Enabled) {
			jobBy[j.Source] = j
		}
	}
	now := s.now().UTC()
	var out []backup.RPOStatus
	seen := map[string]bool{}
	build := func(source string) {
		in := backup.RPOInput{Source: source}
		if j, ok := jobBy[source]; ok {
			in.JobID, in.JobName, in.Engine, in.PolicyID = j.ID, j.Name, j.Engine(), j.PolicyID
			in.PITR = j.LogArchive && !j.IsCustom()
			if p, ok := policies[j.PolicyID]; ok {
				in.TargetMinutes, in.RTOMinutes = p.RPOMinutes, p.RTOMinutes
			}
		}
		if sm, ok := sumBy[source]; ok {
			if in.Engine == "" {
				in.Engine = sm.Engine
			}
			if sm.LastSuccess != nil {
				t := sm.LastSuccess.CreatedAt
				if sm.LastSuccess.StartTime != nil {
					t = *sm.LastSuccess.StartTime
				}
				in.LastBackup = &t
			}
			in.LastLogEnd = sm.LogEnd
		}
		out = append(out, backup.ComputeRPO(now, in))
	}
	for src := range jobBy {
		build(src)
		seen[src] = true
	}
	for _, sm := range sums {
		if !seen[sm.Source] {
			build(sm.Source)
		}
	}
	backup.SortRPO(out)
	return out, jobBy, nil
}

func (s *Server) apiBackupRPO(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	st, _, err := s.rpoStatuses(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		return
	}
	if st == nil {
		st = []backup.RPOStatus{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"sources": st, "generated_at": s.now().UTC()})
}

// writeRPOMetrics appends the RPO gauges to the Prometheus output.
func (s *Server) writeRPOMetrics(ctx context.Context, b *strings.Builder, gauge func(name, help string)) {
	st, _, err := s.rpoStatuses(ctx)
	if err != nil {
		return
	}
	gauge("strand_backup_rpo_seconds", "Current RPO per source: now minus the latest restore point (last log segment for PITR sources, else the last backup).")
	for _, x := range st {
		if x.CurrentSecs >= 0 {
			fmt.Fprintf(b, "strand_backup_rpo_seconds{source=\"%s\"} %d\n", promLabel(x.Source), x.CurrentSecs)
		}
	}
	gauge("strand_backup_rpo_target_seconds", "RPO target of the source's policy (rpo_minutes).")
	for _, x := range st {
		if x.TargetMinutes > 0 {
			fmt.Fprintf(b, "strand_backup_rpo_target_seconds{source=\"%s\"} %d\n", promLabel(x.Source), x.TargetMinutes*60)
		}
	}
	gauge("strand_backup_rpo_breach", "1 when the source is over its RPO target, 0 otherwise.")
	for _, x := range st {
		if x.TargetMinutes > 0 {
			v := 0
			if x.Breach {
				v = 1
			}
			fmt.Fprintf(b, "strand_backup_rpo_breach{source=\"%s\"} %d\n", promLabel(x.Source), v)
		}
	}
}

// rpoTick (maintenance, active instance): notify new breaches of enabled jobs once per episode and close the
// episodes that recovered.
func (s *Server) rpoTick(ctx context.Context, now time.Time) {
	st, jobBy, err := s.rpoStatuses(ctx)
	if err != nil {
		return
	}
	breaching := map[string]bool{}
	for _, x := range st {
		if !x.Breach {
			continue
		}
		breaching[x.Source] = true
		if j, ok := jobBy[x.Source]; ok && !j.Enabled {
			continue // a paused job is reported (metric, report) but does not page anyone
		}
		claimed, err := s.db.st.ClaimBackupRPOBreach(ctx, x.Source, now, rpoRenotify)
		if err != nil || !claimed || !s.notifyConfigured(ctx) {
			continue
		}
		msg := fmt.Sprintf("source %q is over its RPO: %s since the last restore point (%s), target %d min",
			x.Source, (time.Duration(x.CurrentSecs) * time.Second).Round(time.Second), x.Basis, x.TargetMinutes)
		go s.notifyBackupEvent(runner.Event{Kind: backup.EventRPOBreach, Job: x.JobID, JobName: x.JobName, Source: x.Source, Message: msg, At: now}, x.PolicyID)
	}
	if open, err := s.db.st.ListBackupRPOBreaches(ctx); err == nil {
		for _, b := range open {
			if !breaching[b.Source] {
				_ = s.db.st.ClearBackupRPOBreach(ctx, b.Source)
			}
		}
	}
}

// ---- compliance report ----

type complianceRow struct {
	Source            string     `json:"source"`
	JobID             string     `json:"job_id,omitempty"`
	JobName           string     `json:"job_name,omitempty"`
	Engine            string     `json:"engine,omitempty"`
	PolicyID          string     `json:"policy_id,omitempty"`
	PITR              bool       `json:"pitr"`
	RPOTargetMinutes  int        `json:"rpo_target_minutes"`
	RTOMinutes        int        `json:"rto_minutes"`
	CurrentRPOSeconds int64      `json:"current_rpo_seconds"` // -1: no restore point yet
	RPOBasis          string     `json:"rpo_basis"`
	RPOBreach         bool       `json:"rpo_breach"`
	Runs              int        `json:"runs"` // in [from, to): succeeded + failed (cancelled and running runs are not counted)
	Succeeded         int        `json:"succeeded"`
	Failed            int        `json:"failed"` // failed, partial and missed
	SuccessRate       *float64   `json:"success_rate_pct"`
	SuccessRate7d     *float64   `json:"success_rate_7d_pct"`
	SuccessRate30d    *float64   `json:"success_rate_30d_pct"`
	LastRestorePoint  *time.Time `json:"last_restore_point,omitempty"`
	LastTestRestoreAt *time.Time `json:"last_test_restore_at,omitempty"`
	LastTestRestore   string     `json:"last_test_restore_status,omitempty"`
}

type complianceReport struct {
	GeneratedAt     time.Time       `json:"generated_at"`
	From            time.Time       `json:"from"`
	To              time.Time       `json:"to"`
	Sources         int             `json:"sources"`
	RPOBreaching    int             `json:"sources_breaching_rpo"`
	SuccessRate7d   *float64        `json:"success_rate_7d_pct"`
	SuccessRate30d  *float64        `json:"success_rate_30d_pct"`
	LastTestRestore *time.Time      `json:"last_test_restore_at,omitempty"`
	Rows            []complianceRow `json:"rows"`
}

type runTally struct{ ok, bad int }

func tally(cs []store.RunCount) map[string]runTally {
	out := map[string]runTally{}
	for _, c := range cs {
		t := out[c.JobID]
		switch c.Status {
		case backup.RunStatusSuccess:
			t.ok += c.N
		case string(backup.RunStatusFailed), backup.RunStatusPartial, string(backup.RunStatusMissed):
			t.bad += c.N
		}
		out[c.JobID] = t
	}
	return out
}

func pct(t runTally) *float64 {
	if t.ok+t.bad == 0 {
		return nil
	}
	v := float64(t.ok) * 100 / float64(t.ok+t.bad)
	v = float64(int(v*10+0.5)) / 10
	return &v
}

func (s *Server) buildCompliance(ctx context.Context, from, to time.Time) (complianceReport, error) {
	rep := complianceReport{GeneratedAt: s.now().UTC(), From: from, To: to, Rows: []complianceRow{}}
	st, _, err := s.rpoStatuses(ctx)
	if err != nil {
		return rep, err
	}
	win, err := s.db.st.BackupRunCounts(ctx, from, to)
	if err != nil {
		return rep, err
	}
	d7, _ := s.db.st.BackupRunCounts(ctx, to.Add(-7*24*time.Hour), to)
	d30, _ := s.db.st.BackupRunCounts(ctx, to.Add(-30*24*time.Hour), to)
	tw, t7, t30 := tally(win), tally(d7), tally(d30)
	// Newest test-restore per source.
	type tr struct {
		at     time.Time
		status string
	}
	tests := map[string]tr{}
	checks, _ := s.backups.Checks(ctx, "", 200)
	srcOf := map[string]string{}
	for _, c := range checks {
		if c.Kind != "test_restore" {
			continue
		}
		src, ok := srcOf[c.BackupID]
		if !ok {
			if rec, found, _ := s.backups.Get(ctx, c.BackupID); found {
				src = rec.Source
			}
			srcOf[c.BackupID] = src
		}
		if src == "" {
			continue
		}
		if cur, ok := tests[src]; !ok || c.FinishedAt.After(cur.at) {
			tests[src] = tr{c.FinishedAt, c.Status}
		}
	}
	var all7, all30 runTally
	for _, x := range st {
		row := complianceRow{Source: x.Source, JobID: x.JobID, JobName: x.JobName, Engine: x.Engine, PolicyID: x.PolicyID, PITR: x.PITR,
			RPOTargetMinutes: x.TargetMinutes, RTOMinutes: x.RTOMinutes, CurrentRPOSeconds: x.CurrentSecs, RPOBasis: x.Basis,
			RPOBreach: x.Breach, LastRestorePoint: x.LastPoint}
		if x.JobID != "" {
			w := tw[x.JobID]
			row.Runs, row.Succeeded, row.Failed = w.ok+w.bad, w.ok, w.bad
			row.SuccessRate, row.SuccessRate7d, row.SuccessRate30d = pct(w), pct(t7[x.JobID]), pct(t30[x.JobID])
			all7.ok, all7.bad = all7.ok+t7[x.JobID].ok, all7.bad+t7[x.JobID].bad
			all30.ok, all30.bad = all30.ok+t30[x.JobID].ok, all30.bad+t30[x.JobID].bad
		}
		if t, ok := tests[x.Source]; ok {
			at := t.at
			row.LastTestRestoreAt, row.LastTestRestore = &at, t.status
			if rep.LastTestRestore == nil || at.After(*rep.LastTestRestore) {
				rep.LastTestRestore = &at
			}
		}
		if x.Breach {
			rep.RPOBreaching++
		}
		rep.Rows = append(rep.Rows, row)
	}
	rep.Sources = len(rep.Rows)
	rep.SuccessRate7d, rep.SuccessRate30d = pct(all7), pct(all30)
	return rep, nil
}

// reportWindow reads from/to (default: the last 30 days up to now; at most 366 days).
func (s *Server) reportWindow(w http.ResponseWriter, r *http.Request) (from, to time.Time, ok bool) {
	to = s.now().UTC()
	if v := r.URL.Query().Get("to"); v != "" {
		t, good := parseTimeParam(w, v, "to")
		if !good {
			return
		}
		to = *t
		if len(v) == 10 { // a date: through the end of that day
			to = to.Add(24 * time.Hour)
		}
	}
	from = to.Add(-30 * 24 * time.Hour)
	if v := r.URL.Query().Get("from"); v != "" {
		t, good := parseTimeParam(w, v, "from")
		if !good {
			return
		}
		from = *t
	}
	if !from.Before(to) || to.Sub(from) > 366*24*time.Hour {
		writeErr(w, http.StatusBadRequest, "from must be before to, at most 366 days apart")
		return
	}
	return from, to, true
}

func (s *Server) apiBackupCompliance(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	from, to, ok := s.reportWindow(w, r)
	if !ok {
		return
	}
	rep, err := s.buildCompliance(r.Context(), from, to)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not build the compliance report")
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// csvText makes a text cell safe for spreadsheets: a cell that starts with = + - @ (or a tab/CR, which Excel
// also trims before evaluating) is prefixed with an apostrophe so it is shown, not executed.
func csvText(s string) string {
	if s != "" && strings.ContainsRune("=+-@\t\r", rune(s[0])) {
		return "'" + s
	}
	return s
}

func csvPct(p *float64) string {
	if p == nil {
		return ""
	}
	return strconv.FormatFloat(*p, 'f', 1, 64)
}

func csvTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func (s *Server) apiBackupComplianceCSV(w http.ResponseWriter, r *http.Request, sess *Session) {
	if !s.needDB(w) {
		return
	}
	from, to, ok := s.reportWindow(w, r)
	if !ok {
		return
	}
	rep, err := s.buildCompliance(r.Context(), from, to)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not build the compliance report")
		return
	}
	name := fmt.Sprintf("strand-backup-compliance-%s_%s.csv", from.Format("20060102"), to.Format("20060102"))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	w.Header().Set("Cache-Control", "no-store")
	// UTF-8 BOM: Excel opens the file as UTF-8 instead of the legacy code page.
	_, _ = w.Write([]byte("\xEF\xBB\xBF"))
	cw := csv.NewWriter(w)
	cw.UseCRLF = true
	_ = cw.Write([]string{"source", "job_id", "job_name", "engine", "policy_id", "pitr", "rpo_target_minutes", "rto_minutes",
		"current_rpo_minutes", "rpo_basis", "rpo_breach", "runs", "succeeded", "failed", "success_rate_pct",
		"success_rate_7d_pct", "success_rate_30d_pct", "last_restore_point", "last_test_restore_at", "last_test_restore_status",
		"window_from", "window_to"})
	yes := func(b bool) string {
		if b {
			return "yes"
		}
		return "no"
	}
	for _, x := range rep.Rows {
		rpo := ""
		if x.CurrentRPOSeconds >= 0 {
			rpo = strconv.FormatFloat(float64(x.CurrentRPOSeconds)/60, 'f', 1, 64)
		}
		_ = cw.Write([]string{csvText(x.Source), csvText(x.JobID), csvText(x.JobName), csvText(x.Engine), csvText(x.PolicyID), yes(x.PITR),
			strconv.Itoa(x.RPOTargetMinutes), strconv.Itoa(x.RTOMinutes), rpo, x.RPOBasis, yes(x.RPOBreach), strconv.Itoa(x.Runs),
			strconv.Itoa(x.Succeeded), strconv.Itoa(x.Failed), csvPct(x.SuccessRate), csvPct(x.SuccessRate7d), csvPct(x.SuccessRate30d),
			csvTime(x.LastRestorePoint), csvTime(x.LastTestRestoreAt), csvText(x.LastTestRestore), rep.From.Format(time.RFC3339), rep.To.Format(time.RFC3339)})
	}
	cw.Flush()
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: sess.Username, Action: "backup.report_export", Result: "ok",
		Detail: map[string]any{"report": "compliance", "from": rep.From, "to": rep.To, "rows": len(rep.Rows)}})
}
