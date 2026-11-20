-- Repositories manageable from the console (endpoint/region/keys sealed with store.Box, AAD = repository id),
-- notification channels (destination sealed the same way) and rules routing events to channels.
ALTER TABLE backup_repositories DROP CONSTRAINT backup_repositories_env_prefix_check;
ALTER TABLE backup_repositories
    ADD COLUMN kind                 text    NOT NULL DEFAULT 's3' CHECK (kind IN ('s3', 'azure', 'gcs')),
    ADD COLUMN secret_mode          text    NOT NULL DEFAULT 'env' CHECK (secret_mode IN ('env', 'stored')),
    ADD COLUMN endpoint             text    NOT NULL DEFAULT '',
    ADD COLUMN region               text    NOT NULL DEFAULT '',
    ADD COLUMN allow_insecure_local boolean NOT NULL DEFAULT false,
    ADD COLUMN access_key_sealed    bytea,
    ADD COLUMN secret_key_sealed    bytea,
    ADD COLUMN last_test_at         timestamptz,
    ADD COLUMN last_test_status     text    NOT NULL DEFAULT '',
    ADD COLUMN last_test_detail     text    NOT NULL DEFAULT '',
    ADD COLUMN last_test_steps      jsonb   NOT NULL DEFAULT '[]';
ALTER TABLE backup_repositories ADD CONSTRAINT backup_repositories_env_prefix_check
    CHECK (env_prefix = '' OR env_prefix ~ '^STRAND_BACKUP_(S3|REPO_[A-Z0-9]{1,32})_$');
ALTER TABLE backup_repositories ADD CONSTRAINT backup_repositories_mode_fields_check
    CHECK ((secret_mode = 'env' AND env_prefix <> '' AND access_key_sealed IS NULL AND secret_key_sealed IS NULL)
        OR (secret_mode = 'stored' AND env_prefix = ''));

CREATE TABLE backup_notify_channels (
    id               text        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'),
    name             text        NOT NULL UNIQUE,
    kind             text        NOT NULL CHECK (kind IN ('webhook', 'slack', 'teams', 'email')),
    enabled          boolean     NOT NULL DEFAULT true,
    smtp_addr        text        NOT NULL DEFAULT '',
    mail_from        text        NOT NULL DEFAULT '',
    mail_to          jsonb       NOT NULL DEFAULT '[]',
    smtp_user        text        NOT NULL DEFAULT '',
    secret_sealed    bytea,
    created_by       text        NOT NULL DEFAULT '',
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    last_test_at     timestamptz,
    last_test_status text        NOT NULL DEFAULT '',
    last_test_detail text        NOT NULL DEFAULT ''
);

CREATE TABLE backup_notify_rules (
    id         text        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'),
    event      text        NOT NULL CHECK (event IN ('failure', 'missed', 'verify_failed', 'success', 'rpo_breach', 'repo_unreachable')),
    scope_kind text        NOT NULL CHECK (scope_kind IN ('all', 'policy', 'job')),
    scope_id   text        NOT NULL DEFAULT '',
    channel_id text        NOT NULL REFERENCES backup_notify_channels (id) ON DELETE CASCADE,
    enabled    boolean     NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((scope_kind = 'all') = (scope_id = '')),
    UNIQUE (event, scope_kind, scope_id, channel_id)
);
CREATE INDEX backup_notify_rules_channel_idx ON backup_notify_rules (channel_id);
