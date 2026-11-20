-- RPO breach episodes: one row per source currently over its RPO target. The row makes rpo_breach fire once
-- per episode (and again only after the re-notify interval) across web instances; it is deleted on recovery.
CREATE TABLE backup_rpo_breaches (
    source      text        PRIMARY KEY,
    since       timestamptz NOT NULL DEFAULT now(),
    notified_at timestamptz NOT NULL DEFAULT now()
);
