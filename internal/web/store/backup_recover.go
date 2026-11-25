package store

import (
	"context"

	"strand/internal/backup"
)

// BackupJobExecutionLock names the lock every process holds while it runs a job (scheduled run, Run now,
// custom wrapper): from BeginBackupJobRun until FinishBackupJobRun. A PostgreSQL advisory lock dies with its
// session, so a crashed process no longer holds it.
func BackupJobExecutionLock(jobID string) string { return "backup-job-execution:" + jobID }

// RecoverStaleBackupJobRun marks a run of the job that is recorded as running but has no live process
// interrupted (RecoverBackupJobRun), whether the job is enabled or not. Liveness is the execution lock: when
// another process (on any instance) holds it the run may be alive and nothing changes. It reports whether
// a run was recovered.
func RecoverStaleBackupJobRun(ctx context.Context, st Store, jobID string) (bool, error) {
	j, ok, err := st.GetBackupJob(ctx, jobID)
	if err != nil || !ok || !staleCandidate(j) {
		return false, err
	}
	recovered := false
	_, err = st.TryLocked(ctx, BackupJobExecutionLock(jobID), func(ctx context.Context) error {
		j, ok, err := st.GetBackupJob(ctx, jobID)
		if err != nil || !ok || !staleCandidate(j) {
			return err
		}
		recovered, err = st.RecoverBackupJobRun(ctx, jobID, j.UpdatedAt)
		return err
	})
	return recovered, err
}

func staleCandidate(j backup.Job) bool {
	return j.LastRunStatus == backup.RunStatusRunning && j.RunStartedAt != nil && j.RunFinishedAt == nil
}
