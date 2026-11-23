package web

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"

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

var dummyHash []byte
var dummyHashOnce sync.Once

func getDummyHash() []byte {
	dummyHashOnce.Do(func() {
		h, _ := bcrypt.GenerateFromPassword([]byte("strand-timing-safe-dummy-hash-pad"), BcryptCost)
		dummyHash = h
	})
	return dummyHash
}

func constEq(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

// Session là một phiên đăng nhập.
type Session struct {
	CSRF     string
	Created  time.Time
	LastSeen time.Time
	IP       string
	UserID   int64
	Username string
	Role     store.Role
}

// SessionStore giữ session trong PostgreSQL (khóa là sha256 của token trong cookie).
type SessionStore struct {
	now func() time.Time
	// db: session lưu trong PostgreSQL (chỉ sha256 của id); phiên sống qua lần khởi động lại web.
	db *metaDB
}

// NewSessionStore: now = nil dùng time.Now.
func NewSessionStore(now func() time.Time) *SessionStore {
	if now == nil {
		now = time.Now
	}
	return &SessionStore{now: now}
}

// Create tạo session mới (mặc định admin).
func (s *SessionStore) Create(ip string) (string, *Session) {
	ctx, cancel := dbCtx()
	defer cancel()
	u, ok, err := s.db.st.GetUser(ctx, "admin")
	if err != nil || !ok || u.Disabled || !u.Role.Valid() {
		return "", nil
	}
	return s.CreateUserSession(ip, u.ID, u.Username, u.Role)
}

// CreateUserSession tạo session mới gắn với user id và role.
func (s *SessionStore) CreateUserSession(ip string, userID int64, username string, role store.Role) (string, *Session) {
	tok := randToken(32)
	t := s.now()
	sess := &Session{CSRF: randToken(32), Created: t, LastSeen: t, IP: ip, UserID: userID, Username: username, Role: role}
	ctx, cancel := dbCtx()
	defer cancel()
	_ = s.db.st.PurgeSessions(ctx, t.Add(-SessionIdleTimeout), t.Add(-SessionMaxAge))
	if err := s.db.st.PutSession(ctx, store.Session{
		IDHash:   hashKey(tok),
		CSRF:     sess.CSRF,
		IP:       ip,
		Created:  t,
		LastSeen: t,
		UserID:   userID,
		Username: username,
	}); err != nil {
		s.db.log.Error("store session failed", "err", err)
		return "", nil
	}
	return tok, sess
}

// Get trả session còn hạn (và gia hạn idle), hoặc nil. Quá 30 phút không dùng hoặc quá 12 giờ: xóa.
// Nếu tài khoản bị vô hiệu hóa hoặc xóa, session bị hủy ngay.
func (s *SessionStore) Get(tok string) *Session {
	if tok == "" {
		return nil
	}
	k, t := hashKey(tok), s.now()
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

	// Migration 015 binds legacy sessions to the migrated admin. Any remaining
	// unbound session must sign in again.
	if row.UserID <= 0 {
		_ = s.db.st.DeleteSession(ctx, k)
		return nil
	}
	u, ok, err := s.db.st.GetUserByID(ctx, row.UserID)
	if err != nil {
		s.db.log.Warn("read session user failed", "err", err)
		return nil
	}
	if !ok || u.Disabled || !u.Role.Valid() {
		_ = s.db.st.DeleteSession(ctx, k)
		return nil
	}

	if t.Sub(row.LastSeen) >= time.Second { // gia hạn idle; bỏ qua ghi dày đặc
		_ = s.db.st.TouchSession(ctx, k, t)
	}
	return &Session{CSRF: row.CSRF, Created: row.Created, LastSeen: t, IP: row.IP, UserID: row.UserID, Username: u.Username, Role: u.Role}
}

// Delete hủy session (đăng xuất).
func (s *SessionStore) Delete(tok string) {
	ctx, cancel := dbCtx()
	defer cancel()
	_ = s.db.st.DeleteSession(ctx, hashKey(tok))
}

// Len: số session đang giữ (test).
func (s *SessionStore) Len() int {
	ctx, cancel := dbCtx()
	defer cancel()
	n, _ := s.db.st.CountSessions(ctx)
	return n
}

// LoginLimiter: tối đa 5 lần sai/phút/IP; sau đó khóa, thời gian khóa tăng gấp đôi mỗi lần sai tiếp
// (1 phút, 2, 4, ... tối đa 15 phút). Đăng nhập đúng xóa bộ đếm của IP.
type LoginLimiter struct {
	mu        sync.Mutex
	m         map[string]*ipState
	now       func() time.Time
	nextSweep time.Time
}

type ipState struct {
	fails     []time.Time
	lockUntil time.Time
	strikes   int
	lastFail  time.Time
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
	l.expire(t)
	st := l.m[ip]
	if st == nil && len(l.m) >= 10000 {
		return false, loginWindow
	}
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
	l.expire(t)
	st := l.m[ip]
	if st == nil {
		if len(l.m) >= 10000 {
			return
		}
		st = &ipState{}
		l.m[ip] = st
	}
	st.lastFail = t
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

// expire runs under mu and bounds both idle retention and sweep frequency.
func (l *LoginLimiter) expire(now time.Time) {
	if now.Before(l.nextSweep) {
		return
	}
	for ip, st := range l.m {
		if !now.Before(st.lockUntil) && now.Sub(st.lastFail) >= maxLockout {
			delete(l.m, ip)
		}
	}
	l.nextSweep = now.Add(loginWindow)
}
