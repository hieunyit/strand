ALTER TABLE backup_jobs ADD COLUMN run_started_at timestamptz;
ALTER TABLE backup_jobs ADD COLUMN run_finished_at timestamptz;
ALTER TABLE backup_jobs ADD COLUMN last_run_status text NOT NULL DEFAULT ''
    CHECK (last_run_status IN ('', 'running', 'succeeded', 'failed', 'cancelled', 'interrupted'));
ALTER TABLE backup_jobs ADD CONSTRAINT backup_jobs_run_finished_check
    CHECK (run_finished_at IS NULL OR (run_started_at IS NOT NULL AND run_finished_at >= run_started_at));
