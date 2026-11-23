ALTER TABLE backups
    DROP CONSTRAINT backups_status_check,
    ADD CONSTRAINT backups_status_check
        CHECK (status IN ('queued', 'uploading', 'verifying', 'complete', 'failed', 'deleting', 'expired')),
    ADD CONSTRAINT backups_deletion_no_hold
        CHECK (status <> 'deleting' OR NOT legal_hold);
