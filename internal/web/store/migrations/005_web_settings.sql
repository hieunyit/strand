-- Web settings edited on the Settings page (DB mode). A row overrides the web.yaml value of the same key;
-- deleting the row ("reset") falls back to web.yaml / the default. Environment variables win over both.
-- Bootstrap settings (listen, TLS, database, state_dir) are never stored here.
CREATE TABLE web_settings (
    key        text        PRIMARY KEY,
    value      jsonb       NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
