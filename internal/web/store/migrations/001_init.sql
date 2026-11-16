-- Strand web metadata store, schema v1.

-- Single admin account (id is pinned to 1): bcrypt password hash and sha256 of the static API token.
CREATE TABLE admin (
    id              smallint    PRIMARY KEY CHECK (id = 1),
    password_hash   text        NOT NULL DEFAULT '',
    api_token_hash  text        NOT NULL DEFAULT '',
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- Login sessions. Only sha256(session id) is stored, never the cookie value.
CREATE TABLE sessions (
    id_hash    text        PRIMARY KEY,
    csrf       text        NOT NULL,
    ip         text        NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL,
    last_seen  timestamptz NOT NULL
);
CREATE INDEX sessions_last_seen_idx ON sessions (last_seen);

-- Registry of pipelines created from the web UI/API. config_yaml (and env_enc, AES-GCM, only when
-- STRAND_WEB_SECRET_KEY is set) are the source of truth; files are materialised from them.
CREATE TABLE pipelines (
    name         text        PRIMARY KEY,
    config_path  text        NOT NULL,
    env_path     text        NOT NULL DEFAULT '',
    systemd_unit text        NOT NULL DEFAULT '',
    log_file     text        NOT NULL DEFAULT '',
    pid_file     text        NOT NULL DEFAULT '',
    config_yaml  text,
    env_enc      bytea,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Desired run state of any pipeline (declared, scanned or created): the web restarts 'running' ones on boot.
CREATE TABLE pipeline_desired (
    name       text        PRIMARY KEY,
    desired    text        NOT NULL CHECK (desired IN ('running', 'stopped')),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Job history (output stays in <state_dir>/jobs/<id>.log).
CREATE TABLE jobs (
    id         text        PRIMARY KEY,
    pipeline   text        NOT NULL,
    started_at timestamptz NOT NULL,
    running    boolean     NOT NULL,
    data       jsonb       NOT NULL
);
CREATE INDEX jobs_pipeline_started_idx ON jobs (pipeline, started_at DESC);

CREATE TABLE wizards (
    kind       text        NOT NULL,
    pipeline   text        NOT NULL,
    data       jsonb       NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, pipeline)
);

CREATE TABLE audit_log (
    id       bigserial   PRIMARY KEY,
    ts       timestamptz NOT NULL,
    ip       text        NOT NULL DEFAULT '',
    action   text        NOT NULL,
    pipeline text        NOT NULL DEFAULT '',
    result   text        NOT NULL DEFAULT '',
    data     jsonb       NOT NULL
);
CREATE INDEX audit_log_ts_idx ON audit_log (ts);
CREATE INDEX audit_log_pipeline_idx ON audit_log (pipeline, ts);

CREATE TABLE web_meta (
    key        text        PRIMARY KEY,
    value      text        NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
