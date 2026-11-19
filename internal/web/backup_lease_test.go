package web

import (
	"context"
	"net/http"
	"syscall"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func runningSchedulers(e *testEnv, jobID string) []JobStatus {
	var out []JobStatus
	for _, j := range e.srv.jobs.List(backupJobPipeline(jobID)) {
		if j.Running && j.Kind == "backup-schedule" {
			out = append(out, j)
		}
	}
	return out
}

// Backup schedules are singleton work: only the ACTIVE instance runs scheduler processes; a standby runs
// none and refuses backup-job writes like every other write; on takeover the new active starts them and
// the old one stops its own when it steps down.
func TestBackupSchedulersFollowTheLease(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		ctx := context.Background()
		_, script := backupCommandDir(t)
		st := mk()
		j := backup.Job{ID: "bjob-lease", Name: "nightly", Command: script, EverySeconds: 3600,
			ArtifactTemplate: "/var/backups/{id}.sql", ManifestTemplate: "/var/backups/{id}.json", Bucket: "central-backups", Enabled: true}
		if err := st.CreateBackupJob(ctx, j); err != nil {
			t.Fatal(err)
		}
		ttl := 900 * time.Millisecond
		withTTL := func(o *Options) { o.LeaseTTL = ttl }
		holdOn := func(e *testEnv, _ *Config) { e.hold(true) } // fake strand stays running like a scheduler
		e1 := dbEnv(t, st, holdOn, withTTL)
		t.Cleanup(func() { e1.hold(false) })
		if e1.srv.role() != RoleActive {
			t.Fatal("first instance not active")
		}
		waitFor(t, "active instance starts the scheduler", func() bool { return len(runningSchedulers(e1, j.ID)) == 1 })
		first := runningSchedulers(e1, j.ID)[0].ID
		firstPID := runningSchedulers(e1, j.ID)[0].PID
		if firstPID <= 0 {
			t.Fatal("scheduler has no pid")
		}

		e2 := dbEnv(t, st, holdOn, withTTL)
		t.Cleanup(func() { e2.hold(false) })
		if e2.srv.role() != RoleStandby {
			t.Fatal("second instance not standby")
		}
		// The standby started nothing (its job list mirrors the active's history: still exactly one).
		if got := runningSchedulers(e2, j.ID); len(got) != 1 || got[0].ID != first {
			t.Fatalf("standby schedulers = %+v", got)
		}
		if _, err := e2.srv.startBackupJob(j); err == nil {
			t.Fatal("standby started a backup scheduler")
		}
		e2.srv.reconcileBackupSchedule(backupJobPipeline(j.ID)) // returns at once on a standby
		if rows, _ := st.ListJobs(ctx); countPipeline(rows, backupJobPipeline(j.ID)) != 1 {
			t.Fatalf("scheduler processes recorded = %d, want 1", countPipeline(rows, backupJobPipeline(j.ID)))
		}
		// Backup-job writes on a standby: 503 + X-Strand-Role like every other write.
		c2, csrf2 := e2.loginOK()
		for _, req := range []struct{ method, path, body string }{
			{"POST", "/api/v1/backup-jobs", jobJSON(script, "/var/backups/s-{id}.sql")},
			{"POST", "/api/v1/backup-jobs/" + j.ID + "/run", ""},
			{"POST", "/api/v1/backup-jobs/" + j.ID + "/stop", ""},
			{"DELETE", "/api/v1/backup-jobs/" + j.ID, ""},
		} {
			resp, _ := e2.do(c2, req.method, req.path, req.body, map[string]string{csrfHeader: csrf2, "Content-Type": "application/json"})
			if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("X-Strand-Role") != "standby" {
				t.Fatalf("%s %s on standby = %d", req.method, req.path, resp.StatusCode)
			}
		}
		if resp, _ := e2.do(c2, "GET", "/api/v1/backup-jobs", "", nil); resp.StatusCode != 200 {
			t.Fatalf("standby read = %d", resp.StatusCode)
		}

		// The active instance stops renewing: the standby takes over and starts the scheduler; the old
		// instance learns it lost the lease on its next round and stops its own scheduler.
		time.Sleep(ttl + 200*time.Millisecond)
		if !e2.srv.leaseStep(ctx, false) {
			t.Fatal("standby did not take over")
		}
		waitFor(t, "new active starts the scheduler", func() bool {
			got := runningSchedulers(e2, j.ID)
			return len(got) == 1 && got[0].ID != first
		})
		if e1.srv.leaseStep(ctx, true) {
			t.Fatal("old instance still active after the takeover")
		}
		newPID := 0
		for _, jj := range runningSchedulers(e2, j.ID) {
			newPID = jj.PID
		}
		waitFor(t, "old active kills its scheduler process on step-down", func() bool {
			return syscall.Kill(firstPID, 0) != nil
		})
		// Step-down must only stop processes this instance started: the new active's scheduler (same
		// pipeline name, visible in the shared job list) must survive.
		time.Sleep(500 * time.Millisecond)
		if newPID <= 0 || syscall.Kill(newPID, 0) != nil {
			t.Fatalf("step-down of the old instance killed the NEW active's scheduler (pid %d)", newPID)
		}
		if _, err := e1.srv.startBackupJob(j); err == nil {
			t.Fatal("stepped-down instance started a scheduler")
		}
	})
}

func countPipeline(rows []store.Job, pipeline string) int {
	n := 0
	for _, r := range rows {
		if r.Pipeline == pipeline {
			n++
		}
	}
	return n
}
