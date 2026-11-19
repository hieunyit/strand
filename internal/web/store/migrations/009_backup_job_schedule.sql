ALTER TABLE backup_jobs ADD COLUMN next_run_at timestamptz;
ALTER TABLE backup_jobs ADD COLUMN cleanup_local boolean NOT NULL DEFAULT false;
UPDATE backup_jobs SET next_run_at = now() WHERE enabled;
