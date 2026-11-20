-- Managed backups: built-in methods, incremental chains, log segments (PITR), client-side encryption and
-- compression metadata, Object Lock, verification results, multiple repositories, retry policy.
ALTER TABLE backups DROP CONSTRAINT backups_engine_check;
ALTER TABLE backups ADD CONSTRAINT backups_engine_check CHECK (engine IN ('mysql', 'postgres', 'mongodb'));
ALTER TABLE backups
    ADD COLUMN job_id        text        NOT NULL DEFAULT '',
    ADD COLUMN repository    text        NOT NULL DEFAULT '',
    ADD COLUMN method        text        NOT NULL DEFAULT '',
    ADD COLUMN kind          text        NOT NULL DEFAULT 'full' CHECK (kind IN ('full', 'incremental', 'log')),
    ADD COLUMN parent_id     text        NOT NULL DEFAULT '',
    ADD COLUMN copy_of       text        NOT NULL DEFAULT '',
    ADD COLUMN format        text        NOT NULL DEFAULT 'raw' CHECK (format IN ('raw', 'sbk1')),
    ADD COLUMN compression   text        NOT NULL DEFAULT '' CHECK (compression IN ('', 'zstd')),
    ADD COLUMN key_id        text        NOT NULL DEFAULT '',
    ADD COLUMN plain_bytes   bigint      NOT NULL DEFAULT 0 CHECK (plain_bytes >= 0),
    ADD COLUMN plain_sha256  text        NOT NULL DEFAULT '' CHECK (plain_sha256 = '' OR plain_sha256 ~ '^[a-f0-9]{64}$'),
    ADD COLUMN oplog_ts      text        NOT NULL DEFAULT '',
    ADD COLUMN end_position  jsonb       NOT NULL DEFAULT '{}',
    ADD COLUMN start_time    timestamptz,
    ADD COLUMN end_time      timestamptz,
    ADD COLUMN version_id    text        NOT NULL DEFAULT '',
    ADD COLUMN lock_mode     text        NOT NULL DEFAULT '' CHECK (lock_mode IN ('', 'GOVERNANCE', 'COMPLIANCE')),
    ADD COLUMN retain_until  timestamptz,
    ADD COLUMN checked_at    timestamptz,
    ADD COLUMN check_status  text        NOT NULL DEFAULT '' CHECK (check_status IN ('', 'ok', 'failed')),
    ADD COLUMN check_detail  text        NOT NULL DEFAULT '',
    ADD COLUMN expired_at    timestamptz,
    ADD COLUMN details       jsonb       NOT NULL DEFAULT '{}',
    ADD CONSTRAINT backups_incremental_parent CHECK (kind <> 'incremental' OR parent_id <> ''),
    ADD CONSTRAINT backups_lock_pair CHECK ((lock_mode = '') = (retain_until IS NULL)),
    ADD CONSTRAINT backups_oplog_engine CHECK (engine = 'mongodb' OR oplog_ts = ''),
    ADD CONSTRAINT backups_mongo_position CHECK (engine <> 'mongodb' OR (gtid_set = '' AND lsn = '' AND binlog_file = '' AND binlog_pos = 0)),
    ADD CONSTRAINT backups_expired_at CHECK (expired_at IS NULL OR status = 'expired');
CREATE INDEX backups_job_created_idx ON backups (job_id, created_at DESC, id DESC) WHERE job_id <> '';
CREATE INDEX backups_source_kind_idx ON backups (source, kind, created_at DESC);
CREATE INDEX backups_parent_idx ON backups (parent_id) WHERE parent_id <> '';

-- Verification and test-restore results (append-only history; the latest is mirrored on backups).
CREATE TABLE backup_checks (
    id          bigserial   PRIMARY KEY,
    backup_id   text        NOT NULL REFERENCES backups (id) ON DELETE CASCADE,
    kind        text        NOT NULL CHECK (kind ~ '^[a-z][a-z0-9_-]{0,63}$'),
    status      text        NOT NULL CHECK (status IN ('ok', 'failed')),
    started_at  timestamptz NOT NULL,
    finished_at timestamptz NOT NULL CHECK (finished_at >= started_at),
    detail      text        NOT NULL DEFAULT '',
    metrics     jsonb       NOT NULL DEFAULT '{}'
);
CREATE INDEX backup_checks_backup_idx ON backup_checks (backup_id, id DESC);
CREATE INDEX backup_checks_recent_idx ON backup_checks (id DESC);

-- Backup repositories. Credentials and the endpoint are NOT stored: env_prefix names the environment
-- variables of the Web service (<prefix>ENDPOINT, <prefix>ACCESS_KEY, ...).
CREATE TABLE backup_repositories (
    id          text        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'),
    name        text        NOT NULL UNIQUE,
    env_prefix  text        NOT NULL CHECK (env_prefix ~ '^STRAND_BACKUP_(S3|REPO_[A-Z0-9]{1,32})_$'),
    bucket      text        NOT NULL,
    prefix      text        NOT NULL DEFAULT '',
    lock_mode   text        NOT NULL DEFAULT '' CHECK (lock_mode IN ('', 'GOVERNANCE', 'COMPLIANCE')),
    lock_days   integer     NOT NULL DEFAULT 0 CHECK (lock_days BETWEEN 0 AND 36500),
    retention   jsonb       NOT NULL DEFAULT '{}',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    CHECK ((lock_mode = '') = (lock_days = 0))
);

-- Jobs: built-in method options (JSON), retry policy, notification bookkeeping.
ALTER TABLE backup_jobs
    ADD COLUMN options              jsonb       NOT NULL DEFAULT '{}',
    ADD COLUMN retry_max            integer     NOT NULL DEFAULT 0 CHECK (retry_max BETWEEN 0 AND 20),
    ADD COLUMN retry_backoff_seconds integer    NOT NULL DEFAULT 0 CHECK (retry_backoff_seconds BETWEEN 0 AND 86400),
    ADD COLUMN consecutive_failures integer     NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    ADD COLUMN notified_run_at      timestamptz,
    ADD COLUMN missed_notified_at   timestamptz,
    ADD COLUMN last_verify_at       timestamptz,
    ADD COLUMN last_prune_at        timestamptz;
ALTER TABLE backup_jobs ALTER COLUMN command SET DEFAULT '';
ALTER TABLE backup_jobs ALTER COLUMN artifact_template SET DEFAULT '';
ALTER TABLE backup_jobs ALTER COLUMN manifest_template SET DEFAULT '';
