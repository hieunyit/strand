package main

import (
	"context"
	"errors"
	"testing"

	"github.com/spf13/cobra"

	"strand/internal/backup"
	"strand/internal/backup/runner"
	"strand/internal/web/store"
)

// `strand backup schedule --job-id X [--once]` (Run now and the scheduler) refuses a job whose native tool is
// missing before it records a run, opens the repository or creates a catalog record.
func TestBuiltinScheduleMissingToolRecordsNothing(t *testing.T) {
	t.Setenv(runner.EnvToolsDir, t.TempDir())
	ctx := context.Background()
	st := store.NewMem()
	job := backup.Job{ID: "bjob-notool", Name: "notool", EverySeconds: 3600, Enabled: true,
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "src", Repository: "repo1",
			Conn: backup.ConnSpec{Host: "db", Databases: []string{"app"}}}}
	if err := st.CreateBackupJob(ctx, job); err != nil {
		t.Fatal(err)
	}
	for _, once := range []bool{true, false} {
		cmd := &cobra.Command{}
		err := runManagedBuiltinSchedule(ctx, cmd, st, job.ID, once)
		var tm *runner.ToolMissingError
		if !errors.As(err, &tm) {
			t.Fatalf("once=%v: err = %v, want a tool_missing error", once, err)
		}
	}
	runs, err := st.ListBackupRuns(ctx, store.BackupRunFilter{JobID: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 0 {
		t.Fatalf("runs recorded: %+v", runs)
	}
	recs, err := st.ListBackups(ctx, backup.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("catalog records created: %+v", recs)
	}
}
