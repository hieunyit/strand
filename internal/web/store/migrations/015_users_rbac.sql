-- Users and role-based access control (RBAC): admin, operator, viewer.
-- Per-user API tokens and session user binding.

CREATE TABLE users (
    id                   bigserial   PRIMARY KEY,
    username             text        NOT NULL CHECK (username ~ '^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$'),
    display_name         text        NOT NULL DEFAULT '',
    password_hash        text        NOT NULL DEFAULT '',
    role                 text        NOT NULL CHECK (role IN ('admin', 'operator', 'viewer')),
    disabled             boolean     NOT NULL DEFAULT false,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    last_login_at        timestamptz,
    must_change_password boolean     NOT NULL DEFAULT false,
    failed_login_count   integer     NOT NULL DEFAULT 0 CHECK (failed_login_count >= 0),
    locked_until         timestamptz,
    last_failed_at       timestamptz
);
CREATE UNIQUE INDEX users_username_lower_idx ON users (lower(username));

CREATE TABLE user_tokens (
    id           bigserial   PRIMARY KEY,
    user_id      bigint      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name         text        NOT NULL DEFAULT '',
    token_hash   text        NOT NULL UNIQUE CHECK (token_hash ~ '^[a-f0-9]{64}$'),
    token_prefix text        NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at   timestamptz
);
CREATE INDEX user_tokens_user_idx ON user_tokens (user_id);
CREATE INDEX user_tokens_hash_idx ON user_tokens (token_hash) WHERE revoked_at IS NULL;

ALTER TABLE sessions
    ADD COLUMN user_id  bigint REFERENCES users (id) ON DELETE CASCADE,
    ADD COLUMN username text NOT NULL DEFAULT '';
CREATE INDEX sessions_user_id_idx ON sessions (user_id);

-- Idempotent upgrade of existing single admin credentials:
-- If admin table has password_hash, create admin user if it does not already exist.
INSERT INTO users (username, display_name, password_hash, role, must_change_password)
SELECT 'admin', 'Administrator', password_hash, 'admin', false
FROM admin
WHERE id = 1 AND password_hash <> ''
ON CONFLICT DO NOTHING;

-- Attach existing sessions to admin user if migrated
UPDATE sessions
SET user_id = (SELECT id FROM users WHERE lower(username) = 'admin'), username = 'admin'
WHERE user_id IS NULL AND EXISTS (SELECT 1 FROM users WHERE lower(username) = 'admin');
