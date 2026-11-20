package web

import (
	"context"
	"fmt"
	"time"

	"strand/internal/backup"
)

// backupHealth answers "is every source backed up on time and restorable?" for the Backup overview and the
// console home page, from one computation so both always agree.
type backupHealth struct {
	Available         bool // the metadata database / catalog is configured
	Protected, Total  int  // sources with a recent successful backup / all sources
	Failing           int  // jobs failing consecutively
	RPOBreaches       int
	RPOKnown          bool
	LastTestRestore   string // "ok, 3h ago" | "never"
	LastTestRestoreOK bool
	NoRepo, NoKey     bool
	Attention         []AttentionItem
}

func (h backupHealth) ProtectedText() string { return fmt.Sprintf("%d/%d", h.Protected, h.Total) }

// backupHealthFrom computes the health from data the caller already loaded (the Backup page lists the
// same jobs, sources, checks and repositories).
func (s *Server) backupHealthFrom(ctx context.Context, jobs []backupJobView, sources []dashboardRow, checks []backup.CheckResult, repos []backup.Repo) backupHealth {
	h := backupHealth{Available: true, Total: len(sources), NoRepo: len(repos) == 0, LastTestRestore: "never"}
	for _, src := range sources {
		if src.LastSuccess != nil && !src.Stale {
			h.Protected++
		}
	}
	for _, v := range jobs {
		j := v.Job
		if j.ConsecutiveFailures == 0 {
			continue
		}
		h.Failing++
		h.Attention = append(h.Attention, AttentionItem{JobID: j.ID, JobName: j.Name, Engine: j.Engine(), Severity: "bad",
			Reason:  fmt.Sprintf("%d consecutive failures (%s)", j.ConsecutiveFailures, j.LastRunStatus),
			LogURL:  "/backups/jobs/" + j.ID + "?tab=runs",
			EditURL: "/backups/jobs/" + j.ID + "?tab=settings"})
	}
	for _, c := range checks { // newest first
		if c.Kind == "test_restore" {
			h.LastTestRestore = c.Status + ", " + fmtRel(c.FinishedAt, time.Now())
			h.LastTestRestoreOK = c.Status == "ok" || c.Status == "success" || c.Status == "passed"
			break
		}
	}
	if st, _, err := s.rpoStatuses(ctx); err == nil {
		h.RPOKnown = true
		for _, x := range st {
			if !x.Breach {
				continue
			}
			h.RPOBreaches++
			name := x.JobName
			if name == "" {
				name = x.Source
			}
			h.Attention = append(h.Attention, AttentionItem{JobID: x.JobID, JobName: name, Engine: x.Engine, Severity: "bad",
				Reason:  fmt.Sprintf("RPO breach: last restore point %s ago (target %d min)", fmtDur(float64(x.CurrentSecs)), x.TargetMinutes),
				LogURL:  "/backups/jobs/" + x.JobID + "?tab=runs",
				EditURL: "/backups/jobs/" + x.JobID + "?tab=settings"})
		}
	}
	// Backup encryption keys (STRAND_BACKUP_ENCRYPTION_KEYS), not the web's own secret key.
	if kinfo, err := s.keysInfo(ctx); err == nil {
		h.NoKey = !kinfo.Configured
		for _, kw := range kinfo.Warnings {
			h.Attention = append(h.Attention, AttentionItem{JobName: "Encryption key " + kw.KeyID, Engine: "-", Severity: "bad",
				Reason: kw.Message, LogURL: "/backups/keys", EditURL: "/backups/keys"})
		}
	}
	return h
}

// backupHealth loads what it needs; for pages that do not list backups themselves (the console home).
func (s *Server) backupHealth(ctx context.Context) backupHealth {
	if s.db == nil || s.backups == nil {
		return backupHealth{}
	}
	jobs, _ := s.backupJobViews(ctx)
	repos, _ := s.db.st.ListBackupRepos(ctx)
	checks, _ := s.backups.Checks(ctx, "", 20)
	return s.backupHealthFrom(ctx, jobs, s.dashboardRows(ctx), checks, repos)
}
