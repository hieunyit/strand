-- Central backup catalog. Payloads remain in object storage; this stores immutable metadata and lifecycle state.
CREATE TABLE backups (
    id           text        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'),
    source       text        NOT NULL,
    engine       text        NOT NULL CHECK (engine IN ('mysql', 'postgres')),
    tool         text        NOT NULL CHECK (tool ~ '^[a-z][a-z0-9_-]{0,63}$'),
    tool_version text        NOT NULL DEFAULT '',
    bucket       text        NOT NULL CHECK (bucket ~ '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$' AND bucket !~ '\.\.|\.\-|\-\.'),
    object_key   text        NOT NULL CHECK (object_key <> '' AND left(object_key, 1) <> '/'),
    bytes        bigint      NOT NULL CHECK (bytes >= 0),
    sha256       text        NOT NULL DEFAULT '' CHECK (sha256 = '' OR sha256 ~ '^[A-Fa-f0-9]{64}$'),
    gtid_set     text        NOT NULL DEFAULT '',
    lsn          text        NOT NULL DEFAULT '',
    binlog_file  text        NOT NULL DEFAULT '',
    binlog_pos   bigint      NOT NULL DEFAULT 0 CHECK (binlog_pos >= 0),
    manifest_uri text        NOT NULL DEFAULT '',
    status       text        NOT NULL CHECK (status IN ('queued', 'uploading', 'verifying', 'complete', 'failed', 'expired')),
    created_at   timestamptz NOT NULL,
    verified_at  timestamptz,
    expires_at   timestamptz,
    legal_hold   boolean     NOT NULL DEFAULT false,
    UNIQUE (bucket, object_key),
    CHECK (engine <> 'postgres' OR (gtid_set = '' AND binlog_file = '' AND binlog_pos = 0)),
    CHECK (engine <> 'mysql' OR lsn = ''),
    CHECK (NOT legal_hold OR status <> 'expired'),
    CHECK (verified_at IS NULL OR verified_at >= created_at),
    CHECK (expires_at IS NULL OR expires_at >= created_at),
    CHECK (status <> 'complete' OR (verified_at IS NOT NULL AND sha256 <> '' AND manifest_uri <> ''))
);
CREATE INDEX backups_source_created_idx ON backups (source, created_at DESC);
CREATE INDEX backups_status_created_idx ON backups (status, created_at DESC);
CREATE INDEX backups_expiry_idx ON backups (expires_at) WHERE expires_at IS NOT NULL;
