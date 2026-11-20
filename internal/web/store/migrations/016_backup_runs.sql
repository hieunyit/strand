-- Execution history of backup jobs.
CREATE TABLE backup_runs (
    id            text        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'),
    job_id        text        NOT NULL REFERENCES backup_jobs (id) ON DELETE CASCADE,
    trigger       text        NOT NULL CHECK (trigger IN ('schedule', 'manual', 'retry', 'api')),
    started_at    timestamptz NOT NULL,
    finished_at   timestamptz,
    status        text        NOT NULL CHECK (status IN ('running', 'success', 'failed', 'cancelled', 'missed', 'partial')),
    bytes_read    bigint      NOT NULL DEFAULT 0 CHECK (bytes_read >= 0),
    bytes_written bigint      NOT NULL DEFAULT 0 CHECK (bytes_written >= 0),
    duration      bigint      NOT NULL DEFAULT 0 CHECK (duration >= 0),
    backup_id     text        NOT NULL DEFAULT '',
    error_code    text        NOT NULL DEFAULT '' CHECK (error_code IN ('', 'auth_failed', 'network', 'permission', 'disk_full', 'repo_unreachable', 'tool_missing', 'tool_failed', 'timeout', 'cancelled')),
    error_message text        NOT NULL DEFAULT '',
    process_id    text        NOT NULL DEFAULT '',
    attempt       integer     NOT NULL DEFAULT 1 CHECK (attempt >= 1)
);
CREATE INDEX backup_runs_job_started_idx ON backup_runs (job_id, started_at DESC, id DESC);
CREATE INDEX backup_runs_status_idx ON backup_runs (status);
