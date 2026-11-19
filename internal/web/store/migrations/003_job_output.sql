-- Job output (stdout+stderr of `strand verify|promote|...` started from the web), stored in chunks keyed by
-- byte offset. The job runner ships the spool file <state_dir>/jobs/<id>.log into this table while the job
-- runs and removes the spool when the job ends. Deleting the job row deletes its output.
CREATE TABLE job_output (
    job_id text   NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    off    bigint NOT NULL CHECK (off >= 0),
    data   bytea  NOT NULL,
    PRIMARY KEY (job_id, off)
);
