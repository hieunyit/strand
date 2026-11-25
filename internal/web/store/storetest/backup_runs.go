package storetest

import (
	"context"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

func runBackupRuns(t *testing.T, s store.Store) {
	ctx := context.Background()

	// Setup a parent job
	job := backup.Job{
		ID:           "bjob-runtest-1",
		Name:         "Run Test Job",
		EverySeconds: 3600,
		Bucket:       "central-backups",
		JobOptions: backup.JobOptions{
			Method: backup.MethodMySQLDump,
			Source: "mysql-source",
			Conn: backup.ConnSpec{
				Host:        "127.0.0.1",
				Port:        3306,
				User:        "root",
				PasswordEnv: "PW",
				Databases:   []string{"app"},
			},
		},
	}
	if err := s.CreateBackupJob(ctx, job); err != nil {
		t.Fatalf("CreateBackupJob: %v", err)
	}

	t0 := time.Now().UTC().Truncate(time.Microsecond)
	run1 := backup.Run{
		ID:        "run-001",
		JobID:     job.ID,
		Trigger:   backup.TriggerSchedule,
		StartedAt: t0,
		Status:    string(backup.RunStatusRunning),
		ProcessID: "proc-1",
		Attempt:   1,
	}

	// 1. CreateBackupRun
	if err := s.CreateBackupRun(ctx, run1); err != nil {
		t.Fatalf("CreateBackupRun: %v", err)
	}

	// Duplicate ID fails
	if err := s.CreateBackupRun(ctx, run1); err == nil {
		t.Fatal("expected duplicate run to fail")
	}

	// 2. GetBackupRun
	got, ok, err := s.GetBackupRun(ctx, "run-001")
	if err != nil || !ok {
		t.Fatalf("GetBackupRun: ok=%v err=%v", ok, err)
	}
	if got.ID != "run-001" || got.JobID != job.ID || got.Status != string(backup.RunStatusRunning) || got.ProcessID != "proc-1" {
		t.Fatalf("unexpected run: %+v", got)
	}

	// 3. FinishBackupRun
	t1 := t0.Add(5 * time.Second)
	dur := 5 * time.Second
	finished, ok, err := s.FinishBackupRun(ctx, "run-001", t1, backup.RunStatusSuccess, 1024, 512, dur, "bkp-123", "", "")
	if err != nil || !ok {
		t.Fatalf("FinishBackupRun: ok=%v err=%v", ok, err)
	}
	if finished.Status != backup.RunStatusSuccess || finished.BytesRead != 1024 || finished.BytesWritten != 512 || finished.Duration != dur || finished.BackupID != "bkp-123" {
		t.Fatalf("unexpected finished run: %+v", finished)
	}

	// 4. UpdateBackupRun
	finished.ErrorMessage = "some error"
	finished.ErrorCode = backup.ErrCodeToolFailed
	if ok, err := s.UpdateBackupRun(ctx, finished); err != nil || !ok {
		t.Fatalf("UpdateBackupRun: ok=%v err=%v", ok, err)
	}
	gotAfterUpdate, ok, err := s.GetBackupRun(ctx, "run-001")
	if err != nil || !ok || gotAfterUpdate.ErrorCode != backup.ErrCodeToolFailed {
		t.Fatalf("expected updated error code: %+v", gotAfterUpdate)
	}

	// 5. Create multiple runs and test ListBackupRuns with cursor pagination
	for i := 2; i <= 5; i++ {
		r := backup.Run{
			ID:        "run-00" + string(rune('0'+i)),
			JobID:     job.ID,
			Trigger:   backup.TriggerManual,
			StartedAt: t0.Add(time.Duration(i) * time.Minute),
			Status:    backup.RunStatusSuccess,
			Attempt:   1,
		}
		if err := s.CreateBackupRun(ctx, r); err != nil {
			t.Fatalf("CreateBackupRun %d: %v", i, err)
		}
	}

	// List page 1 (limit 2)
	p1, err := s.ListBackupRuns(ctx, store.BackupRunFilter{JobID: job.ID, Limit: 2})
	if err != nil || len(p1) != 2 {
		t.Fatalf("ListBackupRuns page 1: len=%d err=%v", len(p1), err)
	}
	// Ordered by started_at DESC, so run 5 is first, run 4 is second
	if p1[0].StartedAt.Before(p1[1].StartedAt) {
		t.Fatalf("expected descending order: %+v, %+v", p1[0], p1[1])
	}

	// List page 2 with cursor
	cToken := backup.RunNextCursor(p1, 2)
	if cToken == "" {
		t.Fatal("expected non-empty cursor token")
	}
	cursor, err := backup.ParseCursor(cToken)
	if err != nil {
		t.Fatalf("ParseCursor: %v", err)
	}
	p2, err := s.ListBackupRuns(ctx, store.BackupRunFilter{JobID: job.ID, Limit: 2, After: &cursor})
	if err != nil || len(p2) != 2 {
		t.Fatalf("ListBackupRuns page 2: len=%d err=%v", len(p2), err)
	}
	if p2[0].ID == p1[0].ID || p2[0].ID == p1[1].ID {
		t.Fatalf("page 2 contains items from page 1: %+v", p2)
	}

	filtered, err := s.ListBackupRuns(ctx, store.BackupRunFilter{JobID: job.ID, Status: backup.RunStatusSuccess, ProcessID: "proc-1", Limit: 1})
	if err != nil || len(filtered) != 1 || filtered[0].ID != run1.ID {
		t.Fatalf("ListBackupRuns for process/status = %+v, %v", filtered, err)
	}

	// 6. Cascade delete: deleting the job deletes its runs
	if ok, err := s.DeleteBackupJob(ctx, job.ID); err != nil || !ok {
		t.Fatalf("DeleteBackupJob: ok=%v err=%v", ok, err)
	}
	remaining, err := s.ListBackupRuns(ctx, store.BackupRunFilter{JobID: job.ID, Limit: 10})
	if err != nil || len(remaining) != 0 {
		t.Fatalf("expected 0 runs after job delete, got %d", len(remaining))
	}
}
