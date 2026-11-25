-- Restore pins: a restore, test restore or verification pins the backups it reads (base and log segments)
-- so that retention, possibly running in another process, does not delete them mid-read. A pin lapses at
-- expires_at (database clock) unless its holder renews it, so a crashed holder cannot block retention.
CREATE TABLE backup_pins (
    backup_id  text        NOT NULL REFERENCES backups(id) ON DELETE CASCADE,
    holder     text        NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (backup_id, holder)
);
CREATE INDEX backup_pins_holder_idx ON backup_pins (holder);
