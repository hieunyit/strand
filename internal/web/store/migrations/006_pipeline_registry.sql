-- DB mode registers every pipeline in the pipelines table, not only those created from the web: pipelines
-- declared in web.yaml (`pipelines:`) and files found by `scan` are discovered into it (origin), so their config
-- and desired state live in the database too. missing_since flags a declared/scanned pipeline whose web.yaml
-- entry or file has gone (it is kept, never deleted silently). conflicts lists on-disk edits that were set aside
-- because the database copy (source of truth) won: [{"file": "config|env", "path": "<aside copy>", "at": ts}].
ALTER TABLE pipelines ADD COLUMN origin text NOT NULL DEFAULT 'created' CHECK (origin IN ('created', 'web.yaml', 'scan'));
ALTER TABLE pipelines ADD COLUMN missing_since timestamptz;
ALTER TABLE pipelines ADD COLUMN conflicts jsonb NOT NULL DEFAULT '[]';
