package web

import (
	"context"
	"testing"
	"time"

	"strand/internal/backup"
)

// markRunning registers a live scheduler and Run now process of the job with the fake runner.
func (e *cpEnv) markRunning(jobID string) {
	e.rec.backupStopTestRunner.mu.Lock()
	defer e.rec.backupStopTestRunner.mu.Unlock()
	e.rec.jobs["sched-"+jobID] = JobStatus{ID: "sched-" + jobID, Pipeline: backupJobPipeline(jobID), Kind: "backup-schedule", Running: true}
	e.rec.jobs["run-"+jobID] = JobStatus{ID: "run-" + jobID, Pipeline: backupJobRunPipeline(jobID), Kind: "backup-run", Running: true}
}

func (e *cpEnv) stillRunning(t *testing.T, jobID string) {
	t.Helper()
	for _, id := range []string{"sched-" + jobID, "run-" + jobID} {
		if p, _ := e.rec.Get(id); !p.Running {
			t.Fatalf("process %s was cancelled", id)
		}
	}
}

// A policy or connection edit lets the running backup finish (new settings apply from the next run) and
// keeps the job's due time when the schedule did not change.
func TestPolicyAndConnectionEditsDoNotCancelRunningBackups(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	ctx := context.Background()
	cid := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	job := decode(t, a.must("POST", "/api/v1/backup-jobs", policyJobBody("following", cid, "policy-silver", map[string]any{"enabled": true}), 201))["job"].(map[string]any)
	id := job["id"].(string)
	cur, _, _ := e.st.GetBackupJob(ctx, id)
	due := e.clock.Now().Add(5 * time.Minute).UTC() // e.g. a retry back-off, not the schedule's next occurrence
	if ok, _ := e.st.SetBackupJobNextRun(ctx, id, cur.UpdatedAt, due); !ok {
		t.Fatal("set next run")
	}
	e.markRunning(id)

	silver := decode(t, a.must("GET", "/api/v1/backup-policies/policy-silver", nil, 200))["policy"].(map[string]any)
	silver["window_minutes"] = 30
	a.must("PUT", "/api/v1/backup-policies/policy-silver", silver, 200)
	e.stillRunning(t, id)
	got, _, _ := e.st.GetBackupJob(ctx, id)
	if got.WindowMinutes != 30 {
		t.Fatalf("job not refreshed: window %d", got.WindowMinutes)
	}
	if got.NextRunAt == nil || !got.NextRunAt.Equal(due) {
		t.Fatalf("policy edit moved the due time: %v, want %s", got.NextRunAt, due)
	}

	upd := storedConn("prod")
	upd["host"] = "db2.internal"
	a.must("PUT", "/api/v1/backup-connections/"+cid, upd, 200)
	e.stillRunning(t, id)
	if got, _, _ = e.st.GetBackupJob(ctx, id); got.Conn.Host != "db2.internal" || got.NextRunAt == nil || !got.NextRunAt.Equal(due) {
		t.Fatalf("after connection edit: host %q next %v", got.Conn.Host, got.NextRunAt)
	}
}

// Editing an idle interval job keeps its due time unless the schedule changes; a schedule change is
// counted from the last run (interval) or follows the new schedule (daily), never "now" by default.
func TestJobEditKeepsDueTimeUnlessScheduleChanges(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	ctx := context.Background()
	cid := connID(t, a.must("POST", "/api/v1/backup-connections", storedConn("prod"), 201))
	b := mysqlJobBody("hourly", cid)
	b["enabled"] = true
	id := decode(t, a.must("POST", "/api/v1/backup-jobs", b, 201))["job"].(map[string]any)["id"].(string)
	now := e.clock.Now().UTC()
	cur, _, _ := e.st.GetBackupJob(ctx, id)
	started, _, _ := e.st.BeginBackupJobRun(ctx, id, cur.UpdatedAt)
	if ok, _ := e.st.FinishBackupJobRun(ctx, id, started, backup.RunStatusSucceeded); !ok {
		t.Fatal("seed last run")
	}
	cur, _, _ = e.st.GetBackupJob(ctx, id)
	finished, due := *cur.RunFinishedAt, now.Add(40*time.Minute)
	if ok, _ := e.st.SetBackupJobNextRun(ctx, id, cur.UpdatedAt, due); !ok {
		t.Fatal("seed due time")
	}

	b["name"] = "hourly-renamed"
	a.must("PUT", "/api/v1/backup-jobs/"+id, b, 200)
	got, _, _ := e.st.GetBackupJob(ctx, id)
	if got.Name != "hourly-renamed" || got.NextRunAt == nil || !got.NextRunAt.Equal(due) {
		t.Fatalf("rename moved the due time: %v, want %s", got.NextRunAt, due)
	}

	b["every_seconds"] = 7200
	a.must("PUT", "/api/v1/backup-jobs/"+id, b, 200)
	got, _, _ = e.st.GetBackupJob(ctx, id)
	if want := finished.Add(2 * time.Hour); got.NextRunAt == nil || !got.NextRunAt.Equal(want) {
		t.Fatalf("new interval: next %v, want %s (last run + 2h)", got.NextRunAt, want)
	}

	delete(b, "every_seconds")
	b["schedule"] = map[string]any{"kind": "daily", "at": "02:00", "timezone": "UTC"}
	a.must("PUT", "/api/v1/backup-jobs/"+id, b, 200)
	got, _, _ = e.st.GetBackupJob(ctx, id)
	if want := time.Date(now.Year(), now.Month(), now.Day()+1, 2, 0, 0, 0, time.UTC); got.NextRunAt == nil || !got.NextRunAt.Equal(want) {
		t.Fatalf("new daily schedule: next %v, want %s", got.NextRunAt, want)
	}
}
