CREATE TABLE backup_jobs (
    id                 text        PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$'),
    name               text        NOT NULL UNIQUE,
    command            text        NOT NULL,
    every_seconds      integer     NOT NULL CHECK (every_seconds BETWEEN 60 AND 31536000),
    artifact_template  text        NOT NULL,
    manifest_template  text        NOT NULL,
    bucket             text        NOT NULL,
    prefix             text        NOT NULL DEFAULT '',
    enabled            boolean     NOT NULL DEFAULT true,
    tunnel_ssh         text        NOT NULL DEFAULT '',
    tunnel_user        text        NOT NULL DEFAULT '',
    tunnel_identity_file text      NOT NULL DEFAULT '',
    tunnel_known_hosts text        NOT NULL DEFAULT '',
    tunnel_remote      text        NOT NULL DEFAULT '',
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);
