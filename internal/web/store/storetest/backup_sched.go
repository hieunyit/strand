package storetest

import (
	"context"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// runBackupJobRunSchedule: Begin, Finish and Recover keep a cron/daily job on its schedule (not
// every_seconds after the clock), and a cancelled run does not push the due time an interval away.
func runBackupJobRunSchedule(t *testing.T, s store.Store) {
	ctx := context.Background()
	onDaily := func(what string, at *time.Time, after time.Time) {
		t.Helper()
		if at == nil {
			t.Fatalf("%s: no next run", what)
		}
		u := at.UTC()
		if u.Hour() != 2 || u.Minute() != 0 || u.Second() != 0 || !u.After(after) || u.Sub(after) > 24*time.Hour {
			t.Fatalf("%s: next run %s, want the first 02:00 UTC after %s", what, u, after.UTC())
		}
	}
	daily := cfgJob("bjob-daily", "daily")
	daily.Schedule = &backup.Schedule{Kind: backup.ScheduleDaily, At: "02:00", Timezone: "UTC"}
	daily.EverySeconds = daily.Schedule.NominalSeconds()
	must(t, s.CreateBackupJob(ctx, daily))
	cur, _, _ := s.GetBackupJob(ctx, daily.ID)

	started, ok, err := s.BeginBackupJobRun(ctx, daily.ID, cur.UpdatedAt)
	if err != nil || !ok {
		t.Fatalf("begin: %v %v", ok, err)
	}
	running, _, _ := s.GetBackupJob(ctx, daily.ID)
	onDaily("begin", running.NextRunAt, started)

	// A crashed run recovered hours later: the job stays on 02:00 instead of drifting to the recovery time.
	if ok, err := s.RecoverBackupJobRun(ctx, daily.ID, running.UpdatedAt); err != nil || !ok {
		t.Fatalf("recover: %v %v", ok, err)
	}
	rec, _, _ := s.GetBackupJob(ctx, daily.ID)
	if rec.LastRunStatus != backup.RunStatusInterrupted {
		t.Fatalf("recovered = %+v", rec)
	}
	onDaily("recover", rec.NextRunAt, *rec.RunFinishedAt)

	started, _, _ = s.BeginBackupJobRun(ctx, daily.ID, rec.UpdatedAt)
	if ok, err := s.FinishBackupJobRun(ctx, daily.ID, started, backup.RunStatusSucceeded); err != nil || !ok {
		t.Fatalf("finish: %v %v", ok, err)
	}
	done, _, _ := s.GetBackupJob(ctx, daily.ID)
	onDaily("finish", done.NextRunAt, *done.RunFinishedAt)

	// A weekday cron never lands on a weekend after recovery.
	cron := cfgJob("bjob-cron", "weekdays")
	cron.Schedule = &backup.Schedule{Kind: backup.ScheduleCron, Cron: "0 3 * * 1-5", Timezone: "UTC"}
	cron.EverySeconds = cron.Schedule.NominalSeconds()
	must(t, s.CreateBackupJob(ctx, cron))
	cc, _, _ := s.GetBackupJob(ctx, cron.ID)
	if _, ok, _ := s.BeginBackupJobRun(ctx, cron.ID, cc.UpdatedAt); !ok {
		t.Fatal("begin cron")
	}
	cc, _, _ = s.GetBackupJob(ctx, cron.ID)
	if ok, err := s.RecoverBackupJobRun(ctx, cron.ID, cc.UpdatedAt); err != nil || !ok {
		t.Fatalf("recover cron: %v %v", ok, err)
	}
	cc, _, _ = s.GetBackupJob(ctx, cron.ID)
	want, _ := cron.Schedule.Next(*cc.RunFinishedAt, *cc.RunFinishedAt)
	if cc.NextRunAt == nil || !cc.NextRunAt.Equal(want) {
		t.Fatalf("recovered cron next run = %v, want %s", cc.NextRunAt, want)
	}

	// An edit during a run sets the due time; the cancelled run keeps it instead of pushing it an interval.
	iv := cfgJob("bjob-iv", "hourly")
	iv.EverySeconds = 3600
	must(t, s.CreateBackupJob(ctx, iv))
	ic, _, _ := s.GetBackupJob(ctx, iv.ID)
	started, ok, _ = s.BeginBackupJobRun(ctx, iv.ID, ic.UpdatedAt)
	if !ok {
		t.Fatal("begin interval")
	}
	ic, _, _ = s.GetBackupJob(ctx, iv.ID)
	due := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Millisecond)
	ic.Name, ic.NextRunAt = "hourly-renamed", &due
	if ok, err := s.UpdateBackupJob(ctx, ic); err != nil || !ok {
		t.Fatalf("edit: %v %v", ok, err)
	}
	// An edit leaves the run state alone.
	if ed, _, _ := s.GetBackupJob(ctx, iv.ID); ed.LastRunStatus != backup.RunStatusRunning || ed.RunStartedAt == nil || !ed.RunStartedAt.Equal(started) {
		t.Fatalf("edit changed the run state: %+v", ed)
	}
	if ok, err := s.FinishBackupJobRun(ctx, iv.ID, started, backup.RunStatusCancelled); err != nil || !ok {
		t.Fatalf("finish cancelled: %v %v", ok, err)
	}
	ic, _, _ = s.GetBackupJob(ctx, iv.ID)
	if ic.LastRunStatus != backup.RunStatusCancelled || ic.NextRunAt == nil || !ic.NextRunAt.Equal(due) {
		t.Fatalf("cancelled run moved the due time: next %v, want %s (%s)", ic.NextRunAt, due, ic.LastRunStatus)
	}
	// A disabled job has no due time after any finish.
	started, _, _ = s.BeginBackupJobRun(ctx, iv.ID, ic.UpdatedAt)
	ic, _, _ = s.GetBackupJob(ctx, iv.ID)
	ic.Enabled, ic.NextRunAt = false, nil
	if ok, _ := s.UpdateBackupJob(ctx, ic); !ok {
		t.Fatal("disable")
	}
	if ok, _ := s.FinishBackupJobRun(ctx, iv.ID, started, backup.RunStatusCancelled); !ok {
		t.Fatal("finish disabled")
	}
	if ic, _, _ = s.GetBackupJob(ctx, iv.ID); ic.NextRunAt != nil {
		t.Fatalf("disabled job got a due time: %v", ic.NextRunAt)
	}
}

// runBackupStaleRunRecovery: a run left "running" by a dead process is recovered for a disabled job too,
// but never while a live process holds the job's execution lock.
func runBackupStaleRunRecovery(t *testing.T, s store.Store) {
	ctx := context.Background()
	j := cfgJob("bjob-stale", "stale")
	j.EverySeconds = 3600
	must(t, s.CreateBackupJob(ctx, j))
	cur, _, _ := s.GetBackupJob(ctx, j.ID)
	if _, ok, _ := s.BeginBackupJobRun(ctx, j.ID, cur.UpdatedAt); !ok {
		t.Fatal("begin")
	}
	cur, _, _ = s.GetBackupJob(ctx, j.ID)
	cur.Enabled, cur.NextRunAt = false, nil
	if ok, _ := s.UpdateBackupJob(ctx, cur); !ok {
		t.Fatal("disable")
	}

	held, release := make(chan struct{}), make(chan struct{})
	lockDone := make(chan error, 1)
	go func() {
		lockDone <- s.Locked(ctx, store.BackupJobExecutionLock(j.ID), func(context.Context) error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	if got, err := s.TryLocked(ctx, store.BackupJobExecutionLock(j.ID), func(context.Context) error {
		t.Error("TryLocked ran fn while the lock was held")
		return nil
	}); err != nil || got {
		t.Fatalf("TryLocked on a held lock = %v %v", got, err)
	}
	if ok, err := store.RecoverStaleBackupJobRun(ctx, s, j.ID); err != nil || ok {
		t.Fatalf("recovered a run whose process holds the lock: %v %v", ok, err)
	}
	if live, _, _ := s.GetBackupJob(ctx, j.ID); live.LastRunStatus != backup.RunStatusRunning {
		t.Fatalf("live run changed: %+v", live)
	}
	close(release)
	if err := <-lockDone; err != nil {
		t.Fatal(err)
	}

	if ok, err := store.RecoverStaleBackupJobRun(ctx, s, j.ID); err != nil || !ok {
		t.Fatalf("recover stale run of a disabled job: %v %v", ok, err)
	}
	got, _, _ := s.GetBackupJob(ctx, j.ID)
	if got.LastRunStatus != backup.RunStatusInterrupted || got.RunFinishedAt == nil || got.NextRunAt != nil {
		t.Fatalf("recovered = %+v", got)
	}
	if ok, err := store.RecoverStaleBackupJobRun(ctx, s, j.ID); err != nil || ok {
		t.Fatalf("second recovery = %v %v", ok, err)
	}
	if ran, err := s.TryLocked(ctx, store.BackupJobExecutionLock(j.ID), func(context.Context) error { return nil }); err != nil || !ran {
		t.Fatalf("TryLocked on a free lock = %v %v", ran, err)
	}
}
