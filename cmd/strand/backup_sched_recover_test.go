package main

import (
	"context"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// A run left "running" by a crashed process is recovered by the scheduler onto the job's schedule: a
// weekday 03:00 cron stays at 03:00 on a weekday instead of moving to the recovery time + every_seconds
// (which drifted daily jobs and let a weekday cron fire on a Saturday).
func TestSchedulerRecoveryKeepsCronSchedule(t *testing.T) {
	fastSlots(t)
	st := store.NewMem()
	ctx := context.Background()
	j := schedJob("crashed", "h1")
	j.Schedule = &backup.Schedule{Kind: backup.ScheduleCron, Cron: "0 3 * * 1-5", Timezone: "UTC"}
	j.EverySeconds = j.Schedule.NominalSeconds()
	cur := createDue(t, st, j, time.Now().UTC().Add(-time.Hour))
	if _, ok, err := st.BeginBackupJobRun(ctx, j.ID, cur.UpdatedAt); err != nil || !ok {
		t.Fatalf("begin: %v %v", ok, err)
	}
	stop := runLoop(t, st, j.ID, schedulerOptions{limits: concurrencyLimits{2, 1}, run: trackedRun(st, func(backup.Job) {})})
	waitFor(t, "recovered run", func() bool {
		g, _, _ := st.GetBackupJob(ctx, j.ID)
		return g.LastRunStatus == backup.RunStatusInterrupted
	})
	stop()
	g, _, _ := st.GetBackupJob(ctx, j.ID)
	want, _ := j.Schedule.Next(*g.RunFinishedAt, *g.RunFinishedAt)
	if g.NextRunAt == nil || !g.NextRunAt.Equal(want) || g.NextRunAt.Hour() != 3 || g.NextRunAt.Weekday() == time.Saturday || g.NextRunAt.Weekday() == time.Sunday {
		t.Fatalf("next run after recovery = %v, want %s", g.NextRunAt, want)
	}
}
