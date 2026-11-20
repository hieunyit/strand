package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

func (m *Mem) GetUser(ctx context.Context, username string) (User, bool, error) {
	if err := m.enter(); err != nil {
		return User{}, false, err
	}
	defer m.mu.Unlock()

	for _, u := range m.users {
		if strings.EqualFold(u.Username, username) {
			return u, true, nil
		}
	}
	return User{}, false, nil
}

func (m *Mem) GetUserByID(ctx context.Context, id int64) (User, bool, error) {
	if err := m.enter(); err != nil {
		return User{}, false, err
	}
	defer m.mu.Unlock()

	u, ok := m.users[id]
	return u, ok, nil
}

func (m *Mem) ListUsers(ctx context.Context) ([]User, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()

	out := make([]User, 0, len(m.users))
	for _, u := range m.users {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i].Username) < strings.ToLower(out[j].Username)
	})
	return out, nil
}

func (m *Mem) CreateUser(ctx context.Context, u User) (User, error) {
	if err := m.enter(); err != nil {
		return User{}, err
	}
	defer m.mu.Unlock()

	for _, existing := range m.users {
		if strings.EqualFold(existing.Username, u.Username) {
			return User{}, fmt.Errorf("user %q already exists", u.Username)
		}
	}
	m.userSeq++
	u.ID = m.userSeq
	now := time.Now()
	u.CreatedAt = now
	u.UpdatedAt = now
	if u.Role == "" {
		u.Role = RoleViewer
	}
	m.users[u.ID] = u
	return u, nil
}

func (m *Mem) UpdateUser(ctx context.Context, u User) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	cur, ok := m.users[u.ID]
	if !ok {
		return errors.New("user not found")
	}
	cur.DisplayName = u.DisplayName
	cur.Role = u.Role
	cur.Disabled = u.Disabled
	cur.Language = u.Language
	cur.UpdatedAt = time.Now()
	m.users[u.ID] = cur
	return nil
}

func (m *Mem) SetUserLanguage(ctx context.Context, id int64, lang string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	cur, ok := m.users[id]
	if !ok {
		return errors.New("user not found")
	}
	cur.Language = lang
	cur.UpdatedAt = time.Now()
	m.users[id] = cur
	return nil
}

func (m *Mem) SetUserPassword(ctx context.Context, id int64, passwordHash string, mustChange bool) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	cur, ok := m.users[id]
	if !ok {
		return errors.New("user not found")
	}
	cur.PasswordHash = passwordHash
	cur.MustChangePassword = mustChange
	cur.UpdatedAt = time.Now()
	m.users[id] = cur
	return nil
}

func (m *Mem) RecordLoginSuccess(ctx context.Context, id int64, at time.Time) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	cur, ok := m.users[id]
	if !ok {
		return nil
	}
	cur.LastLoginAt = &at
	cur.FailedLoginCount = 0
	cur.LockedUntil = nil
	cur.UpdatedAt = at
	m.users[id] = cur
	return nil
}

func (m *Mem) RecordLoginFailure(ctx context.Context, id int64, count int, lockedUntil *time.Time, at time.Time) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	cur, ok := m.users[id]
	if !ok {
		return nil
	}
	cur.FailedLoginCount = count
	cur.LockedUntil = lockedUntil
	cur.LastFailedAt = &at
	cur.UpdatedAt = at
	m.users[id] = cur
	return nil
}

func (m *Mem) ResetLoginFailures(ctx context.Context, id int64) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	cur, ok := m.users[id]
	if !ok {
		return nil
	}
	cur.FailedLoginCount = 0
	cur.LockedUntil = nil
	cur.UpdatedAt = time.Now()
	m.users[id] = cur
	return nil
}

func (m *Mem) DeleteUser(ctx context.Context, id int64) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	delete(m.users, id)
	for k, s := range m.sessions {
		if s.UserID == id {
			delete(m.sessions, k)
		}
	}
	for k, tok := range m.tokens {
		if tok.UserID == id {
			delete(m.tokens, k)
		}
	}
	return nil
}

func (m *Mem) PurgeUserSessions(ctx context.Context, userID int64) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	for k, s := range m.sessions {
		if s.UserID == userID {
			delete(m.sessions, k)
		}
	}
	return nil
}

func (m *Mem) PurgeOtherUserSessions(ctx context.Context, userID int64, keepIDHash string) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	for k, s := range m.sessions {
		if s.UserID == userID && s.IDHash != keepIDHash {
			delete(m.sessions, k)
		}
	}
	return nil
}

func (m *Mem) ListUserSessions(ctx context.Context, userID int64) ([]Session, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()

	var out []Session
	for _, s := range m.sessions {
		if s.UserID == userID {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	return out, nil
}

func (m *Mem) CreateUserToken(ctx context.Context, tok UserToken) (UserToken, error) {
	if err := m.enter(); err != nil {
		return UserToken{}, err
	}
	defer m.mu.Unlock()

	for _, existing := range m.tokens {
		if existing.TokenHash == tok.TokenHash {
			return UserToken{}, errors.New("token hash collision")
		}
	}
	m.tokenSeq++
	tok.ID = m.tokenSeq
	tok.CreatedAt = time.Now()
	m.tokens[tok.ID] = tok
	return tok, nil
}

func (m *Mem) ListUserTokens(ctx context.Context, userID int64) ([]UserToken, error) {
	if err := m.enter(); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()

	var out []UserToken
	for _, tok := range m.tokens {
		if tok.UserID == userID {
			out = append(out, tok)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID > out[j].ID
	})
	return out, nil
}

func (m *Mem) RevokeUserToken(ctx context.Context, id int64, userID int64) error {
	if err := m.enter(); err != nil {
		return err
	}
	defer m.mu.Unlock()

	tok, ok := m.tokens[id]
	if ok && tok.UserID == userID && tok.RevokedAt == nil {
		now := time.Now()
		tok.RevokedAt = &now
		m.tokens[id] = tok
	}
	return nil
}

func (m *Mem) AuthenticateToken(ctx context.Context, tokenHash string) (User, UserToken, bool, error) {
	if err := m.enter(); err != nil {
		return User{}, UserToken{}, false, err
	}
	defer m.mu.Unlock()

	for _, tok := range m.tokens {
		if tok.TokenHash == tokenHash && tok.RevokedAt == nil {
			u, ok := m.users[tok.UserID]
			if !ok || u.Disabled {
				return User{}, UserToken{}, false, nil
			}
			now := time.Now()
			tok.LastUsedAt = &now
			m.tokens[tok.ID] = tok
			return u, tok, true, nil
		}
	}
	return User{}, UserToken{}, false, nil
}
