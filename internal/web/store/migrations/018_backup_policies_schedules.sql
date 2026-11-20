-- Backup policies (schedule + retention + storage + protection + verification), the "missed" run status
-- (a run not started within window_minutes) and the cross-process concurrency slots of the schedulers.
CREATE TABLE backup_policies (
    id          text        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'),
    name        text        NOT NULL UNIQUE,
    description text        NOT NULL DEFAULT '',
    builtin     boolean     NOT NULL DEFAULT false,
    spec        jsonb       NOT NULL DEFAULT '{}',
    created_by  text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE backup_jobs DROP CONSTRAINT backup_jobs_last_run_status_check;
ALTER TABLE backup_jobs ADD CONSTRAINT backup_jobs_last_run_status_check
    CHECK (last_run_status IN ('', 'running', 'succeeded', 'failed', 'cancelled', 'interrupted', 'missed'));

-- One row per running backup (STRAND_BACKUP_MAX_CONCURRENT / max_per_host). Rows expire when not renewed.
CREATE TABLE backup_run_slots (
    job_id     text        PRIMARY KEY,
    host       text        NOT NULL DEFAULT '',
    acquired_at timestamptz NOT NULL DEFAULT now(),
    renewed_at  timestamptz NOT NULL DEFAULT now()
);
