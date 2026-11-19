-- Metrics history of the sampler (one row per pipeline per sample interval, default 5 s), so charts survive a
-- web restart and can show more than the 1 h kept in RAM. Raw samples are kept 24 h; older data lives in
-- 1-minute averages (lag also as the per-minute maximum) for 7 days. Maintenance runs hourly in the web.
CREATE TABLE metric_samples (
    pipeline    text             NOT NULL,
    ts          timestamptz      NOT NULL,
    state       text             NOT NULL DEFAULT '',
    lag         double precision NOT NULL DEFAULT 0,
    throughput  double precision NOT NULL DEFAULT 0,
    apply_rate  double precision NOT NULL DEFAULT 0,
    rows_rate   double precision NOT NULL DEFAULT 0,
    snap_rate   double precision NOT NULL DEFAULT 0,
    holes       double precision NOT NULL DEFAULT 0,
    writeset    double precision NOT NULL DEFAULT 0,
    spool_bytes double precision NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, ts)
);
CREATE INDEX metric_samples_ts_idx ON metric_samples (ts);

CREATE TABLE metric_samples_1m (
    pipeline    text             NOT NULL,
    ts          timestamptz      NOT NULL,
    state       text             NOT NULL DEFAULT '',
    lag         double precision NOT NULL DEFAULT 0,
    lag_max     double precision NOT NULL DEFAULT 0,
    throughput  double precision NOT NULL DEFAULT 0,
    apply_rate  double precision NOT NULL DEFAULT 0,
    rows_rate   double precision NOT NULL DEFAULT 0,
    snap_rate   double precision NOT NULL DEFAULT 0,
    holes       double precision NOT NULL DEFAULT 0,
    writeset    double precision NOT NULL DEFAULT 0,
    spool_bytes double precision NOT NULL DEFAULT 0,
    PRIMARY KEY (pipeline, ts)
);
CREATE INDEX metric_samples_1m_ts_idx ON metric_samples_1m (ts);
