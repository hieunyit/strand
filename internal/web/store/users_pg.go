package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func scanUser(row interface {
	Scan(dest ...any) error
}) (User, error) {
	var u User
	var roleStr string
	err := row.Scan(
		&u.ID,
		&u.Username,
		&u.DisplayName,
		&u.PasswordHash,
		&roleStr,
		&u.Disabled,
		&u.CreatedAt,
		&u.UpdatedAt,
		&u.LastLoginAt,
		&u.MustChangePassword,
		&u.FailedLoginCount,
		&u.LockedUntil,
		&u.LastFailedAt,
		&u.Language,
	)
	if err != nil {
		return User{}, err
	}
	u.Role = Role(roleStr)
	return u, nil
}

func (p *PG) GetUser(ctx context.Context, username string) (User, bool, error) {
	row := p.db.QueryRowContext(ctx, `SELECT id, username, display_name, password_hash, role, disabled, created_at, updated_at,
		last_login_at, must_change_password, failed_login_count, locked_until, last_failed_at, language
		FROM users WHERE lower(username) = lower($1)`, username)
	u, err := scanUser(row)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, false, nil
	}
	return u, err == nil, p.wrap(err)
}

func (p *PG) GetUserByID(ctx context.Context, id int64) (User, bool, error) {
	row := p.db.QueryRowContext(ctx, `SELECT id, username, display_name, password_hash, role, disabled, created_at, updated_at,
		last_login_at, must_change_password, failed_login_count, locked_until, last_failed_at, language
		FROM users WHERE id = $1`, id)
	u, err := scanUser(row)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, false, nil
	}
	return u, err == nil, p.wrap(err)
}

func (p *PG) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, username, display_name, password_hash, role, disabled, created_at, updated_at,
		last_login_at, must_change_password, failed_login_count, locked_until, last_failed_at, language
		FROM users ORDER BY lower(username) ASC`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()

	var out []User
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, u)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) CreateUser(ctx context.Context, u User) (User, error) {
	if u.Role == "" {
		u.Role = RoleViewer
	}
	row := p.db.QueryRowContext(ctx, `INSERT INTO users (username, display_name, password_hash, role, disabled, must_change_password, language)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, username, display_name, password_hash, role, disabled, created_at, updated_at,
		          last_login_at, must_change_password, failed_login_count, locked_until, last_failed_at, language`,
		u.Username, u.DisplayName, u.PasswordHash, string(u.Role), u.Disabled, u.MustChangePassword, u.Language)
	created, err := scanUser(row)
	return created, p.wrap(err)
}

func (p *PG) UpdateUser(ctx context.Context, u User) error {
	_, err := p.db.ExecContext(ctx, `UPDATE users
		SET display_name = $2, role = $3, disabled = $4, language = $5, updated_at = now()
		WHERE id = $1`, u.ID, u.DisplayName, string(u.Role), u.Disabled, u.Language)
	return p.wrap(err)
}

func (p *PG) SetUserLanguage(ctx context.Context, id int64, lang string) error {
	_, err := p.db.ExecContext(ctx, `UPDATE users SET language = $2, updated_at = now() WHERE id = $1`, id, lang)
	return p.wrap(err)
}

func (p *PG) SetUserPassword(ctx context.Context, id int64, passwordHash string, mustChange bool) error {
	_, err := p.db.ExecContext(ctx, `UPDATE users
		SET password_hash = $2, must_change_password = $3, updated_at = now()
		WHERE id = $1`, id, passwordHash, mustChange)
	return p.wrap(err)
}

func (p *PG) RecordLoginSuccess(ctx context.Context, id int64, at time.Time) error {
	_, err := p.db.ExecContext(ctx, `UPDATE users
		SET last_login_at = $2, failed_login_count = 0, locked_until = NULL, updated_at = now()
		WHERE id = $1`, id, at)
	return p.wrap(err)
}

func (p *PG) RecordLoginFailure(ctx context.Context, id int64, count int, lockedUntil *time.Time, at time.Time) error {
	_, err := p.db.ExecContext(ctx, `UPDATE users
		SET failed_login_count = $2, locked_until = $3, last_failed_at = $4, updated_at = now()
		WHERE id = $1`, id, count, lockedUntil, at)
	return p.wrap(err)
}

func (p *PG) ResetLoginFailures(ctx context.Context, id int64) error {
	_, err := p.db.ExecContext(ctx, `UPDATE users
		SET failed_login_count = 0, locked_until = NULL, updated_at = now()
		WHERE id = $1`, id)
	return p.wrap(err)
}

func (p *PG) DeleteUser(ctx context.Context, id int64) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM users WHERE id = $1`, id)
	return p.wrap(err)
}

func (p *PG) PurgeUserSessions(ctx context.Context, userID int64) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM sessions WHERE user_id = $1`, userID)
	return p.wrap(err)
}

func (p *PG) PurgeOtherUserSessions(ctx context.Context, userID int64, keepIDHash string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM sessions WHERE user_id = $1 AND id_hash <> $2`, userID, keepIDHash)
	return p.wrap(err)
}

func (p *PG) ListUserSessions(ctx context.Context, userID int64) ([]Session, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id_hash, csrf, ip, created_at, last_seen, user_id, username
		FROM sessions WHERE user_id = $1 ORDER BY last_seen DESC`, userID)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()

	var out []Session
	for rows.Next() {
		var s Session
		var uid sql.NullInt64
		if err := rows.Scan(&s.IDHash, &s.CSRF, &s.IP, &s.Created, &s.LastSeen, &uid, &s.Username); err != nil {
			return nil, p.wrap(err)
		}
		if uid.Valid {
			s.UserID = uid.Int64
		}
		out = append(out, s)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) CreateUserToken(ctx context.Context, tok UserToken) (UserToken, error) {
	var created UserToken
	err := p.db.QueryRowContext(ctx, `INSERT INTO user_tokens (user_id, name, token_hash, token_prefix)
		VALUES ($1, $2, $3, $4)
		RETURNING id, user_id, name, token_hash, token_prefix, created_at, last_used_at, revoked_at`,
		tok.UserID, tok.Name, tok.TokenHash, tok.TokenPrefix).
		Scan(&created.ID, &created.UserID, &created.Name, &created.TokenHash, &created.TokenPrefix, &created.CreatedAt, &created.LastUsedAt, &created.RevokedAt)
	return created, p.wrap(err)
}

func (p *PG) ListUserTokens(ctx context.Context, userID int64) ([]UserToken, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, user_id, name, token_hash, token_prefix, created_at, last_used_at, revoked_at
		FROM user_tokens WHERE user_id = $1 ORDER BY id DESC`, userID)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()

	var out []UserToken
	for rows.Next() {
		var t UserToken
		if err := rows.Scan(&t.ID, &t.UserID, &t.Name, &t.TokenHash, &t.TokenPrefix, &t.CreatedAt, &t.LastUsedAt, &t.RevokedAt); err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, t)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) RevokeUserToken(ctx context.Context, id int64, userID int64) error {
	_, err := p.db.ExecContext(ctx, `UPDATE user_tokens SET revoked_at = now() WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL`, id, userID)
	return p.wrap(err)
}

func (p *PG) AuthenticateToken(ctx context.Context, tokenHash string) (User, UserToken, bool, error) {
	var u User
	var roleStr string
	var tok UserToken
	err := p.db.QueryRowContext(ctx, `SELECT u.id, u.username, u.display_name, u.password_hash, u.role, u.disabled, u.created_at, u.updated_at,
		       u.last_login_at, u.must_change_password, u.failed_login_count, u.locked_until, u.last_failed_at,
		       t.id, t.user_id, t.name, t.token_hash, t.token_prefix, t.created_at, t.last_used_at, t.revoked_at
		FROM user_tokens t
		JOIN users u ON u.id = t.user_id
		WHERE t.token_hash = $1 AND t.revoked_at IS NULL AND u.disabled = false`, tokenHash).
		Scan(
			&u.ID, &u.Username, &u.DisplayName, &u.PasswordHash, &roleStr, &u.Disabled, &u.CreatedAt, &u.UpdatedAt,
			&u.LastLoginAt, &u.MustChangePassword, &u.FailedLoginCount, &u.LockedUntil, &u.LastFailedAt,
			&tok.ID, &tok.UserID, &tok.Name, &tok.TokenHash, &tok.TokenPrefix, &tok.CreatedAt, &tok.LastUsedAt, &tok.RevokedAt,
		)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, UserToken{}, false, nil
	}
	if err != nil {
		return User{}, UserToken{}, false, p.wrap(err)
	}
	u.Role = Role(roleStr)
	now := time.Now()
	_, _ = p.db.ExecContext(ctx, `UPDATE user_tokens SET last_used_at = $2 WHERE id = $1`, tok.ID, now)
	tok.LastUsedAt = &now
	return u, tok, true, nil
}
