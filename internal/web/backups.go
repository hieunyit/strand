package web

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// AttentionItem represents an issue on the Overview dashboard requiring operator attention.
type AttentionItem struct {
	JobID    string
	JobName  string
	Engine   string
	Reason   string
	Severity string // "warn", "bad"
	LogURL   string
	EditURL  string
}

type backupsPageBody struct {
	Backups    []backup.Record
	Message    string
	NextCursor string
	Paged      bool
	Jobs       []backupJobView
	Sources    []dashboardRow
	Repos      []backup.Repo
	Checks     []backup.CheckResult
	Methods    []string

	// MethodOptions are the built-in methods with their engine, so the job form lists only the methods
	// of the engine the operator picked.
	MethodOptions []backupMethodOption

	// Overview stat tiles and banners (spec 4.1)
	StatProtectedSources string
	StatSuccessRate      string
	StatFailingJobs      int
	StatStorageUsed      string
	StatLastTestRestore  string
	StatRPOBreaches      string
	RPOBreachCount       int
	LastTestRestoreOK    bool
	Chart14Day           template.HTML
	IsStandby            bool
	NoRepo               bool
	NoKey                bool
	NeedsAttention       []AttentionItem
}

// backupMethodOption is one built-in backup method in the job form.
type backupMethodOption struct {
	Value  string
	Engine string
}

// dashboardRow is one source on the dashboard, preformatted for the template.
type dashboardRow struct {
	recoveryView
	Age       string
	Window    string
	Spark     string // SVG polyline points of the recent sizes
	LastSize  string
	LastCheck string
}

func fmtAge(sec int64) string {
	d := time.Duration(sec) * time.Second
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", sec)
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

func backupSparkPoints(sizes []int64) string {
	if len(sizes) < 2 {
		return ""
	}
	var max int64 = 1
	for _, s := range sizes {
		if s > max {
			max = s
		}
	}
	pts := make([]string, len(sizes))
	for i, s := range sizes {
		x := float64(i) * 80 / float64(len(sizes)-1)
		y := 18 - float64(s)*16/float64(max)
		pts[i] = fmt.Sprintf("%.1f,%.1f", x, y)
	}
	return strings.Join(pts, " ")
}

func (s *Server) dashboardRows(ctx context.Context) []dashboardRow {
	views, err := s.backupDashboard(ctx)
	if err != nil {
		return nil
	}
	out := make([]dashboardRow, 0, len(views))
	for _, v := range views {
		r := dashboardRow{recoveryView: v, Window: "-", Spark: backupSparkPoints(v.RecentSizes), LastSize: "-", LastCheck: "-"}
		if v.LastSuccess != nil {
			r.Age = fmtAge(v.AgeSeconds)
			r.LastSize = fmtBytes(float64(v.LastSuccess.Bytes))
			if v.LastSuccess.CheckedAt != nil {
				r.LastCheck = v.LastSuccess.CheckStatus + " " + v.LastSuccess.CheckedAt.UTC().Format("2006-01-02 15:04")
			}
		}
		if v.WindowStart != nil && v.WindowEnd != nil {
			kind := "restore points"
			if win, err := s.recoveryWindow(ctx, v.Source); err == nil && win.Continuous {
				kind = "point-in-time"
			}
			r.Window = v.WindowStart.UTC().Format("2006-01-02 15:04") + " → " + v.WindowEnd.UTC().Format("2006-01-02 15:04:05") + " UTC (" + kind + ")"
		}
		out = append(out, r)
	}
	return out
}

const backupsPageSize = 200

func (s *Server) pageBackups(w http.ResponseWriter, r *http.Request, sess *Session) {
	b := backupsPageBody{}
	if s.backups == nil {
		b.Message = "Central backup catalog is unavailable: configure the web metadata database."
	} else {
		f := backup.Filter{Limit: backupsPageSize}
		if v := r.URL.Query().Get("cursor"); v != "" {
			c, err := backup.ParseCursor(v)
			if err != nil {
				http.Error(w, "invalid cursor", http.StatusBadRequest)
				return
			}
			f.After, b.Paged = &c, true
		}
		items, err := s.backups.List(r.Context(), f)
		if err != nil {
			s.log.Error("list backup catalog", "error", err)
			b.Message = "Could not read the backup catalog."
		} else {
			b.Backups = items
			b.NextCursor = backup.NextCursor(items, f.Limit)
		}
		b.Jobs, err = s.backupJobViews(r.Context())
		if err != nil {
			b.Message = "Could not read backup jobs."
		}
		if !b.Paged {
			b.Sources = s.dashboardRows(r.Context())
			b.Repos, _ = s.db.st.ListBackupRepos(r.Context())
			b.Checks, _ = s.backups.Checks(r.Context(), "", 20)

			h := s.backupHealthFrom(r.Context(), b.Jobs, b.Sources, b.Checks, b.Repos)
			b.StatProtectedSources = h.ProtectedText()
			b.IsStandby = !s.isActive()
			b.NoRepo, b.NoKey = h.NoRepo, h.NoKey
			b.StatFailingJobs, b.NeedsAttention = h.Failing, h.Attention
			b.StatLastTestRestore, b.LastTestRestoreOK = h.LastTestRestore, h.LastTestRestoreOK
			b.RPOBreachCount = h.RPOBreaches
			b.StatRPOBreaches = "-"
			if h.RPOKnown {
				b.StatRPOBreaches = fmt.Sprintf("%d", h.RPOBreaches)
			}

			now := time.Now().UTC()
			since24h := now.Add(-24 * time.Hour)
			allRuns, _ := s.db.st.ListBackupRuns(r.Context(), store.BackupRunFilter{Limit: 2000})
			succ24h := 0
			total24h := 0
			// Only finished runs count: a running or cancelled run is neither a success nor a failure.
			finishedBad := func(st string) bool {
				return st == string(backup.RunStatusFailed) || st == backup.RunStatusPartial || st == string(backup.RunStatusMissed)
			}
			for _, run := range allRuns {
				if run.StartedAt.After(since24h) && (run.Status == backup.RunStatusSuccess || finishedBad(run.Status)) {
					total24h++
					if run.Status == backup.RunStatusSuccess {
						succ24h++
					}
				}
			}
			if total24h > 0 {
				b.StatSuccessRate = fmt.Sprintf("%d%%", int(float64(succ24h)/float64(total24h)*100))
			} else {
				b.StatSuccessRate = "-" // no run in 24 h is not a 100% success rate
			}

			// Total over the whole catalog (per repository), not just the page of backups shown below.
			var totalStorage int64
			if usage, err := s.db.st.BackupRepoUsage(r.Context()); err == nil {
				for _, u := range usage {
					totalStorage += u.Bytes
				}
			}
			b.StatStorageUsed = fmtBytes(float64(totalStorage))

			days := make([]BackupDayStat, 14)
			for i := 0; i < 14; i++ {
				d := now.AddDate(0, 0, -13+i).Truncate(24 * time.Hour)
				days[i] = BackupDayStat{Date: d}
			}
			for _, run := range allRuns {
				runDate := run.StartedAt.UTC().Truncate(24 * time.Hour)
				for i := range days {
					if days[i].Date.Equal(runDate) {
						if run.Status == backup.RunStatusSuccess {
							days[i].Success++
						} else if finishedBad(run.Status) {
							days[i].Failed++
						}
						days[i].Bytes += run.BytesWritten
						break
					}
				}
			}
			b.Chart14Day = Backup14DayChart(days, 640, 180)
		}
		b.Methods = []string{backup.MethodCustom, backup.MethodMySQLDump, backup.MethodMariaDBDump, backup.MethodMydumper,
			backup.MethodPGDump, backup.MethodPGBaseBackup, backup.MethodMongodump}
		for _, m := range b.Methods {
			if engine := backup.MethodEngine(m); engine != "" {
				b.MethodOptions = append(b.MethodOptions, backupMethodOption{Value: m, Engine: engine})
			}
		}
	}
	s.render(w, "backups", sess, pageData{Title: "Backups", Active: "backups", Refresh: 10, Body: b})
}

func (s *Server) apiBackups(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.backups == nil {
		writeErr(w, http.StatusServiceUnavailable, "central backup catalog requires the metadata database")
		return
	}
	f := backup.Filter{Source: r.URL.Query().Get("source"), Engine: r.URL.Query().Get("engine"), Status: backup.Status(r.URL.Query().Get("status")), Limit: 200}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > backup.MaxListLimit {
			writeErr(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		f.Limit = n
	}
	var err error
	if v := r.URL.Query().Get("created_after"); v != "" {
		t, parseErr := time.Parse(time.RFC3339Nano, v)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, "created_after must be RFC3339")
			return
		}
		f.CreatedAfter = &t
	}
	if v := r.URL.Query().Get("created_before"); v != "" {
		t, parseErr := time.Parse(time.RFC3339Nano, v)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, "created_before must be RFC3339")
			return
		}
		f.CreatedBefore = &t
	}
	if v := r.URL.Query().Get("cursor"); v != "" {
		c, parseErr := backup.ParseCursor(v)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, "cursor is malformed")
			return
		}
		f.After = &c
	}
	items, err := s.backups.List(r.Context(), f)
	if err != nil {
		if errors.Is(err, backup.ErrInvalid) {
			writeErr(w, http.StatusBadRequest, err.Error())
		} else {
			s.log.Error("list backup catalog", "error", err)
			writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		}
		return
	}
	out := map[string]any{"backups": items, "limit": f.Limit}
	if next := backup.NextCursor(items, f.Limit); next != "" {
		out["next_cursor"] = next
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) apiBackup(w http.ResponseWriter, r *http.Request, _ *Session) {
	rc, found, ok := s.lookupBackup(w, r)
	if !ok {
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup not found")
		return
	}
	writeJSON(w, http.StatusOK, rc)
}

func (s *Server) apiBackupStatus(w http.ResponseWriter, r *http.Request, _ *Session) {
	rc, found, ok := s.lookupBackup(w, r)
	if !ok {
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": rc.ID, "status": rc.Status, "created_at": rc.CreatedAt,
		"verified_at": rc.VerifiedAt, "expires_at": rc.ExpiresAt,
	})
}

func (s *Server) lookupBackup(w http.ResponseWriter, r *http.Request) (backup.Record, bool, bool) {
	if s.backups == nil {
		writeErr(w, http.StatusServiceUnavailable, "central backup catalog requires the metadata database")
		return backup.Record{}, false, false
	}
	rc, found, err := s.backups.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, backup.ErrInvalid) {
			writeErr(w, http.StatusBadRequest, err.Error())
		} else {
			s.log.Error("get backup catalog record", "error", err)
			writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		}
		return backup.Record{}, false, false
	}
	return rc, found, true
}
