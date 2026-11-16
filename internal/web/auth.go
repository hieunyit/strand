package web

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"sync"
	"time"

	"strand/internal/web/store"
)

const (
	SessionIdleTimeout = 30 * time.Minute
	SessionMaxAge      = 12 * time.Hour
	loginWindow        = time.Minute
	loginMaxFailures   = 5
	maxLockout         = 15 * time.Minute
)

// randToken trả n byte ngẫu nhiên mã hóa base64 URL (256 bit khi n = 32).
func randToken(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func hashKey(tok string) string {
	h := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(h[:])
}

// HashAPIToken trả sha256 hex của token tĩnh (ghi vào api_token_hash).
func HashAPIToken(tok string) string { return hashKey(tok) }

// NewAPIToken sinh token tĩnh ngẫu nhiên 256 bit.
func NewAPIToken() string { return "strand_" + randToken(32) }

func constEq(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

// Session là một phiên đăng nhập trong RAM.
type Session struct {
	CSRF     string
	Created  time.Time
	LastSeen time.Time
	IP       string
}

// SessionStore giữ session trong RAM (khóa là sha256 của token trong cookie).
type SessionStore struct {
	mu  sync.Mutex
	m   map[string]*Session
	now func() time.Time
	// db: session lưu trong PostgreSQL (chỉ sha256 của id); phiên sống qua lần khởi động lại web.
	db *metaDB
}

// NewSessionStore: now = nil dùng time.Now.
func NewSessionStore(now func() time.Time) *SessionStore {
	if now == nil {
		now = time.Now
	}
	return &SessionStore{m: map[string]*Session{}, now: now}
}

// Create tạo session mới, trả token cookie (256 bit) và session.
func (s *SessionStore) Create(ip string) (string, *Session) {
	tok := randToken(32)
	t := s.now()
	sess := &Session{CSRF: randToken(32), Created: t, LastSeen: t, IP: ip}
	if s.db != nil {
		ctx, cancel := dbCtx()
		defer cancel()
		_ = s.db.st.PurgeSessions(ctx, t.Add(-SessionIdleTimeout), t.Add(-SessionMaxAge))
		if err := s.db.st.PutSession(ctx, store.Session{IDHash: hashKey(tok), CSRF: sess.CSRF, IP: ip, Created: t, LastSeen: t}); err != nil {
			s.db.log.Error("store session failed", "err", err)
			return "", nil
		}
		return tok, sess
	}
	s.mu.Lock()
	s.gcLocked(t)
	s.m[hashKey(tok)] = sess
	s.mu.Unlock()
	return tok, sess
}

// Get trả session còn hạn (và gia hạn idle), hoặc nil. Quá 30 phút không dùng hoặc quá 12 giờ: xóa.
func (s *SessionStore) Get(tok string) *Session {
	if tok == "" {
		return nil
	}
	k := hashKey(tok)
	t := s.now()
	if s.db != nil {
		return s.getDB(k, t)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := s.m[k]
	if sess == nil {
		return nil
	}
	if t.Sub(sess.LastSeen) > SessionIdleTimeout || t.Sub(sess.Created) > SessionMaxAge {
		delete(s.m, k)
		return nil
	}
	sess.LastSeen = t
	c := *sess
	return &c
}

func (s *SessionStore) getDB(k string, t time.Time) *Session {
	ctx, cancel := dbCtx()
	defer cancel()
	row, ok, err := s.db.st.GetSession(ctx, k)
	if err != nil {
		s.db.log.Warn("read session failed", "err", err)
		return nil
	}
	if !ok {
		return nil
	}
	if t.Sub(row.LastSeen) > SessionIdleTimeout || t.Sub(row.Created) > SessionMaxAge {
		_ = s.db.st.DeleteSession(ctx, k)
		return nil
	}
	if t.Sub(row.LastSeen) >= time.Second { // gia hạn idle; bỏ qua ghi dày đặc
		_ = s.db.st.TouchSession(ctx, k, t)
	}
	return &Session{CSRF: row.CSRF, Created: row.Created, LastSeen: t, IP: row.IP}
}

// Delete hủy session (đăng xuất).
func (s *SessionStore) Delete(tok string) {
	if s.db != nil {
		ctx, cancel := dbCtx()
		defer cancel()
		_ = s.db.st.DeleteSession(ctx, hashKey(tok))
		return
	}
	s.mu.Lock()
	delete(s.m, hashKey(tok))
	s.mu.Unlock()
}

// Len: số session đang giữ (test).
func (s *SessionStore) Len() int {
	if s.db != nil {
		ctx, cancel := dbCtx()
		defer cancel()
		n, _ := s.db.st.CountSessions(ctx)
		return n
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}

func (s *SessionStore) gcLocked(t time.Time) {
	for k, v := range s.m {
		if t.Sub(v.LastSeen) > SessionIdleTimeout || t.Sub(v.Created) > SessionMaxAge {
			delete(s.m, k)
		}
	}
}

// LoginLimiter: tối đa 5 lần sai/phút/IP; sau đó khóa, thời gian khóa tăng gấp đôi mỗi lần sai tiếp
// (1 phút, 2, 4, ... tối đa 15 phút). Đăng nhập đúng xóa bộ đếm của IP.
type LoginLimiter struct {
	mu  sync.Mutex
	m   map[string]*ipState
	now func() time.Time
}

type ipState struct {
	fails     []time.Time
	lockUntil time.Time
	strikes   int
}

func NewLoginLimiter(now func() time.Time) *LoginLimiter {
	if now == nil {
		now = time.Now
	}
	return &LoginLimiter{m: map[string]*ipState{}, now: now}
}

// Allow báo IP có được thử đăng nhập không; nếu không, trả thời gian phải chờ.
func (l *LoginLimiter) Allow(ip string) (bool, time.Duration) {
	t := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.m[ip]
	if st != nil && t.Before(st.lockUntil) {
		return false, st.lockUntil.Sub(t)
	}
	return true, 0
}

// Fail ghi một lần sai; từ lần thứ 5 trong cửa sổ 1 phút thì khóa (và khóa lâu hơn nếu lặp lại).
func (l *LoginLimiter) Fail(ip string) {
	t := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.m[ip]
	if st == nil {
		st = &ipState{}
		l.m[ip] = st
	}
	kept := st.fails[:0]
	for _, f := range st.fails {
		if t.Sub(f) < loginWindow {
			kept = append(kept, f)
		}
	}
	st.fails = append(kept, t)
	if len(st.fails) >= loginMaxFailures {
		d := loginWindow << st.strikes
		if d > maxLockout || d <= 0 {
			d = maxLockout
		}
		st.lockUntil = t.Add(d)
		st.strikes++
		st.fails = nil
	}
}

// Success xóa bộ đếm của IP.
func (l *LoginLimiter) Success(ip string) {
	l.mu.Lock()
	delete(l.m, ip)
	l.mu.Unlock()
}
