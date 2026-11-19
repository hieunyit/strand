-- Several `strand web` processes may share one metadata database (two containers, blue/green). Exactly one is
-- ACTIVE: it holds the lease row below and runs the singleton work (autostart / desired state, observed stops,
-- metrics history writes and maintenance, job recovery, pipeline discovery writes, audit archiving). The others
-- are STANDBY: they serve the UI and API read-only and take over when the lease expires (holder stopped, or
-- cut off from the database for longer than the TTL). Expiry is judged with the database clock only.
-- epoch increments at every change of holder (fencing token, shown in /healthz and the audit log).
CREATE TABLE web_lease (
    id          smallint    PRIMARY KEY CHECK (id = 1),
    holder      text        NOT NULL,
    info        jsonb       NOT NULL DEFAULT '{}',
    epoch       bigint      NOT NULL DEFAULT 1,
    acquired_at timestamptz NOT NULL,
    renewed_at  timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL
);

-- Heartbeat of every running web instance (Settings page, /healthz): who is out there and in which role.
-- Rows not seen for a day are removed by the heartbeat itself.
CREATE TABLE web_instances (
    id         text        PRIMARY KEY,
    info       jsonb       NOT NULL DEFAULT '{}',
    role       text        NOT NULL DEFAULT 'standby',
    started_at timestamptz NOT NULL DEFAULT now(),
    last_seen  timestamptz NOT NULL DEFAULT now()
);
