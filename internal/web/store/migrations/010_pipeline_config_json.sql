-- New Web-managed pipelines store a typed JSON configuration in PostgreSQL.
-- config_yaml remains temporarily for import/upgrade of file-managed pipelines.
ALTER TABLE pipelines ADD COLUMN config_json jsonb;

CREATE INDEX pipelines_config_json_present_idx ON pipelines (name) WHERE config_json IS NOT NULL;
