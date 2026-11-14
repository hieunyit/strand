package web

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

var pagePaths = []string{"/", "/pipelines", "/pipelines/orders", "/pipelines/orders/tables", "/pipelines/orders/checkpoint",
	"/pipelines/orders/errors", "/pipelines/orders/config", "/pipelines/orders/logs", "/pipelines/nope"}

var apiPaths = []string{"/api/v1/session", "/api/v1/overview", "/api/v1/pipelines", "/api/v1/pipelines/orders",
	"/api/v1/pipelines/orders/status", "/api/v1/pipelines/orders/tables", "/api/v1/pipelines/orders/checkpoint",
	"/api/v1/pipelines/orders/errors", "/api/v1/pipelines/orders/config", "/api/v1/pipelines/orders/logs",
	"/api/v1/pipelines/orders/series", "/api/v1/unknown", "/api/v1/pipelines/nope"}

func TestAuthRequiredOnEveryRoute(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	for _, p := range pagePaths {
		resp, _ := e.do(c, "GET", p, "", nil)
		if resp.StatusCode != http.StatusFound || !strings.HasPrefix(resp.Header.Get("Location"), "/login") {
			t.Errorf("GET %s without session: status %d location %q, want 302 to /login", p, resp.StatusCode, resp.Header.Get("Location"))
		}
	}
	for _, p := range apiPaths {
		resp, body := e.do(c, "GET", p, "", nil)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET %s without session: status %d, want 401", p, resp.StatusCode)
		}
		if strings.Contains(body, "orders") {
			t.Errorf("GET %s leaked data without auth: %s", p, body)
		}
	}
	for _, p := range []string{"/api/v1/logout", "/logout", "/api/v1/pipelines/orders/actions/start"} {
		resp, _ := e.do(c, "POST", p, "{}", nil)
		if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusFound {
			t.Errorf("POST %s without session: status %d, want 401/302", p, resp.StatusCode)
		}
	}
	// Token sai cũng bị từ chối.
	resp, _ := e.do(c, "GET", "/api/v1/overview", "", map[string]string{"Authorization": "Bearer nonsense"})
	if resp.StatusCode != 401 {
		t.Errorf("bad bearer: %d", resp.StatusCode)
	}
	// Trang đăng nhập và static là công khai.
	if resp, _ := e.do(c, "GET", "/login", "", nil); resp.StatusCode != 200 {
		t.Errorf("/login: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/static/app.css", "", nil); resp.StatusCode != 200 {
		t.Errorf("/static/app.css: %d", resp.StatusCode)
	}
}

func TestLoginThenPagesAndAPI(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c := e.client()
	if resp, _ := e.login(c, testPassword); resp.StatusCode != 200 {
		t.Fatalf("login failed: %d", resp.StatusCode)
	}
	for _, p := range pagePaths[:len(pagePaths)-1] {
		resp, body := e.do(c, "GET", p, "", nil)
		if resp.StatusCode != 200 || !strings.Contains(body, "Strand") {
			t.Errorf("GET %s: status %d", p, resp.StatusCode)
		}
	}
	if resp, _ := e.do(c, "GET", "/pipelines/nope", "", nil); resp.StatusCode != 404 {
		t.Errorf("unknown pipeline page: %d", resp.StatusCode)
	}
	for _, p := range apiPaths[:len(apiPaths)-2] {
		resp, body := e.do(c, "GET", p, "", nil)
		if resp.StatusCode != 200 || !json.Valid([]byte(body)) {
			t.Errorf("GET %s: status %d body %q", p, resp.StatusCode, body)
		}
	}
	for _, p := range apiPaths[len(apiPaths)-2:] {
		if resp, _ := e.do(c, "GET", p, "", nil); resp.StatusCode != 404 {
			t.Errorf("GET %s: %d, want 404", p, resp.StatusCode)
		}
	}
}

func TestCSRFMissingIs403(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	// Đăng nhập không có token CSRF (cookie đăng nhập có nhưng header/trường thiếu).
	e.do(c, "GET", "/login", "", nil)
	resp, _ := e.do(c, "POST", "/api/v1/login", `{"password":"`+testPassword+`"}`, nil)
	if resp.StatusCode != 403 {
		t.Fatalf("login without CSRF: %d, want 403", resp.StatusCode)
	}
	resp, _ = e.do(c, "POST", "/login", "password="+testPassword, map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if resp.StatusCode != 403 {
		t.Fatalf("form login without CSRF: %d, want 403", resp.StatusCode)
	}
	// Sai token cũng 403.
	resp, _ = e.do(c, "POST", "/api/v1/login", `{"password":"`+testPassword+`"}`, map[string]string{csrfHeader: "wrong"})
	if resp.StatusCode != 403 {
		t.Fatalf("login with wrong CSRF: %d", resp.StatusCode)
	}

	_, csrf := e.login(c, testPassword)
	if csrf == "" {
		t.Fatal("no csrf from login")
	}
	for _, p := range []string{"/api/v1/logout", "/logout", "/api/v1/pipelines/orders/actions/anything"} {
		resp, _ := e.do(c, "POST", p, "{}", nil)
		if resp.StatusCode != 403 {
			t.Errorf("POST %s without CSRF: %d, want 403", p, resp.StatusCode)
		}
		resp, _ = e.do(c, "POST", p, "{}", map[string]string{csrfHeader: "bad"})
		if resp.StatusCode != 403 {
			t.Errorf("POST %s with bad CSRF: %d, want 403", p, resp.StatusCode)
		}
	}
	// Session vẫn sống sau các lần bị từ chối; với token đúng thì logout được.
	resp, _ = e.do(c, "POST", "/api/v1/logout", "", map[string]string{csrfHeader: csrf})
	if resp.StatusCode != 200 {
		t.Fatalf("logout with CSRF: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/api/v1/overview", "", nil); resp.StatusCode != 401 {
		t.Fatalf("session must be gone after logout: %d", resp.StatusCode)
	}
}

func TestFormLoginRedirectsAndSafeNext(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	_, body := e.do(c, "GET", "/login?next=/pipelines/orders", "", nil)
	if !strings.Contains(body, `name="next" value="/pipelines/orders"`) {
		t.Fatalf("next not carried: %s", body)
	}
	var lc string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}
	form := "password=" + testPassword + "&csrf=" + lc + "&next=//evil.example"
	resp, _ := e.do(c, "POST", "/login", strings.ReplaceAll(form, " ", "+"), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/" {
		t.Fatalf("unsafe next must fall back to /: %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
}

func TestLoginRateLimit(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	for i := 0; i < 5; i++ {
		resp, _ := e.login(c, "wrong password")
		if resp.StatusCode != 401 {
			t.Fatalf("attempt %d: %d, want 401", i+1, resp.StatusCode)
		}
	}
	// Lần 6 bị chặn dù mật khẩu đúng.
	resp, _ := e.login(c, testPassword)
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("6th attempt: %d (Retry-After %q), want 429", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	// Chờ hết một phút: được thử lại.
	e.clock.Advance(61 * time.Second)
	if resp, _ := e.login(c, "wrong again"); resp.StatusCode != 401 {
		t.Fatalf("after lockout expiry: %d, want 401", resp.StatusCode)
	}
	// Backoff tăng dần: 4 lần sai nữa thì khóa lần 2 dài hơn (2 phút).
	for i := 0; i < 4; i++ {
		e.login(c, "wrong")
	}
	e.clock.Advance(61 * time.Second)
	if resp, _ := e.login(c, testPassword); resp.StatusCode != 429 {
		t.Fatalf("second lockout must last longer than 1 minute, got %d", resp.StatusCode)
	}
	e.clock.Advance(60 * time.Second)
	if resp, _ := e.login(c, testPassword); resp.StatusCode != 200 {
		t.Fatalf("after second lockout: %d", resp.StatusCode)
	}
	// Audit ghi các lần bị từ chối.
	var denied, limited int
	for _, l := range e.auditLines() {
		if l["action"] == "login" && l["result"] == "denied" {
			denied++
			if d, _ := l["detail"].(map[string]any); d["reason"] == "rate_limited" {
				limited++
			}
		}
	}
	if denied < 6 || limited < 2 {
		t.Fatalf("audit: denied=%d limited=%d", denied, limited)
	}
}

func TestLimiterIsPerIP(t *testing.T) {
	now := time.Now()
	l := NewLoginLimiter(func() time.Time { return now })
	for i := 0; i < 5; i++ {
		l.Fail("1.1.1.1")
	}
	if ok, _ := l.Allow("1.1.1.1"); ok {
		t.Fatal("1.1.1.1 must be locked")
	}
	if ok, _ := l.Allow("2.2.2.2"); !ok {
		t.Fatal("other IP must not be affected")
	}
	// Lỗi cách nhau hơn một phút không cộng dồn.
	l2 := NewLoginLimiter(func() time.Time { return now })
	for i := 0; i < 8; i++ {
		l2.Fail("3.3.3.3")
		now = now.Add(20 * time.Second)
		if ok, _ := l2.Allow("3.3.3.3"); !ok {
			t.Fatalf("3 failures per minute must not lock (i=%d)", i)
		}
	}
}

func TestSessionExpiry(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	e.login(c, testPassword)
	// Idle 29 phút: còn sống và được gia hạn.
	e.clock.Advance(29 * time.Minute)
	if resp, _ := e.do(c, "GET", "/api/v1/overview", "", nil); resp.StatusCode != 200 {
		t.Fatalf("29 min idle: %d", resp.StatusCode)
	}
	e.clock.Advance(29 * time.Minute)
	if resp, _ := e.do(c, "GET", "/api/v1/overview", "", nil); resp.StatusCode != 200 {
		t.Fatalf("activity must extend the idle timer: %d", resp.StatusCode)
	}
	// Quá 30 phút không dùng: hết hạn.
	e.clock.Advance(31 * time.Minute)
	if resp, _ := e.do(c, "GET", "/api/v1/overview", "", nil); resp.StatusCode != 401 {
		t.Fatalf("idle 31 min: %d, want 401", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/pipelines", "", nil); resp.StatusCode != 302 {
		t.Fatalf("page after expiry: %d, want 302", resp.StatusCode)
	}
}

func TestSessionMaxAge(t *testing.T) {
	now := time.Now()
	s := NewSessionStore(func() time.Time { return now })
	tok, _ := s.Create("1.2.3.4")
	for i := 0; i < 25; i++ { // hoạt động liên tục mỗi 29 phút
		now = now.Add(29 * time.Minute)
		if s.Get(tok) == nil {
			if now.Sub(now.Add(-time.Duration(i+1)*29*time.Minute)) <= SessionMaxAge {
				t.Fatalf("expired early at step %d", i)
			}
			return
		}
	}
	t.Fatal("session survived beyond the 12 h maximum")
}

func TestSessionTokenIs256BitAndHashedInMemory(t *testing.T) {
	s := NewSessionStore(nil)
	tok, sess := s.Create("x")
	if len(tok) < 43 || len(sess.CSRF) < 43 { // 32 byte base64url = 43 ký tự
		t.Fatalf("token too short: %d / %d", len(tok), len(sess.CSRF))
	}
	if _, ok := s.m[tok]; ok {
		t.Fatal("raw token must not be a map key")
	}
	t2, _ := s.Create("x")
	if t2 == tok {
		t.Fatal("tokens must differ")
	}
}

func TestSecurityHeadersAndCookieFlags(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	resp, _ := e.do(c, "GET", "/login", "", nil)
	h := resp.Header
	if h.Get("Content-Security-Policy") == "" || !strings.HasPrefix(h.Get("Content-Security-Policy"), "default-src 'self'") {
		t.Errorf("CSP: %q", h.Get("Content-Security-Policy"))
	}
	if h.Get("X-Frame-Options") != "DENY" || h.Get("Referrer-Policy") != "no-referrer" || h.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("headers: %v", h)
	}
	// Mọi phản hồi (kể cả 302/401) mang header bảo mật.
	resp, _ = e.do(c, "GET", "/api/v1/overview", "", nil)
	if resp.Header.Get("X-Frame-Options") != "DENY" {
		t.Error("401 response lacks security headers")
	}
	e.login(c, testPassword)
	var sc *http.Cookie
	resp, _ = e.do(c, "GET", "/login", "", nil)
	_ = resp
	for _, ck := range e.cookiesFromLogin(t) {
		if ck.Name == sessionCookie {
			sc = ck
		}
	}
	if sc == nil {
		t.Fatal("no session cookie")
	}
	if !sc.HttpOnly || sc.SameSite != http.SameSiteStrictMode || sc.Path != "/" {
		t.Errorf("cookie flags: %+v", sc)
	}
	if sc.Secure {
		t.Error("plain HTTP loopback server must not set Secure")
	}
}

// cookiesFromLogin đăng nhập bằng một client mới và trả các Set-Cookie của phản hồi đăng nhập.
func (e *testEnv) cookiesFromLogin(t *testing.T) []*http.Cookie {
	c := e.client()
	resp, _ := e.login(c, testPassword)
	return resp.Cookies()
}

func TestSecureCookieWhenConfigured(t *testing.T) {
	e := newEnv(t, func(e *testEnv, c *Config) { c.SecureCookies = true })
	cs := e.cookiesFromLogin(t)
	found := false
	for _, ck := range cs {
		if ck.Name == sessionCookie {
			found = true
			if !ck.Secure {
				t.Error("secure_cookies must set Secure")
			}
		}
	}
	if !found {
		t.Fatal("no session cookie")
	}
}

func TestBearerAPIToken(t *testing.T) {
	tok := NewAPIToken()
	e := newEnv(t, func(e *testEnv, c *Config) { c.APITokenHash = HashAPIToken(tok) })
	c := e.client()
	hdr := map[string]string{"Authorization": "Bearer " + tok}
	if resp, _ := e.do(c, "GET", "/api/v1/overview", "", hdr); resp.StatusCode != 200 {
		t.Fatalf("token GET: %d", resp.StatusCode)
	}
	// Token tĩnh không dùng cookie nên không cần CSRF: tới được action (ở đây 404 vì chưa đăng ký action nào).
	if resp, _ := e.do(c, "POST", "/api/v1/pipelines/orders/actions/none", "{}", hdr); resp.StatusCode != 404 {
		t.Fatalf("token POST: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/api/v1/overview", "", map[string]string{"Authorization": "Bearer " + tok + "x"}); resp.StatusCode != 401 {
		t.Fatalf("wrong token: %d", resp.StatusCode)
	}
	// Không đặt api_token_hash thì token nào cũng bị từ chối.
	e2 := newEnv(t, nil)
	if resp, _ := e2.do(e2.client(), "GET", "/api/v1/overview", "", hdr); resp.StatusCode != 401 {
		t.Fatalf("token with none configured: %d", resp.StatusCode)
	}
}

func TestActionRegistryExtensionPoint(t *testing.T) {
	e := newEnv(t, nil)
	ran := 0
	e.srv.Actions().Register(Action{Name: "ping", ConfirmName: true, Run: func(ctx context.Context, s *Server, r ActionRequest) (ActionResult, error) {
		ran++
		zero := 0
		return ActionResult{Message: "pong " + r.Pipeline.Name, ExitCode: &zero}, nil
	}})
	c := e.client()
	_, csrf := e.login(c, testPassword)
	h := map[string]string{csrfHeader: csrf}
	if resp, _ := e.do(c, "POST", "/api/v1/pipelines/orders/actions/ping", `{}`, h); resp.StatusCode != 400 || ran != 0 {
		t.Fatalf("missing confirm_name must be rejected: %d", resp.StatusCode)
	}
	resp, body := e.do(c, "POST", "/api/v1/pipelines/orders/actions/ping", `{"confirm_name":"orders"}`, h)
	if resp.StatusCode != 200 || ran != 1 || !strings.Contains(body, "pong orders") {
		t.Fatalf("action: %d %s", resp.StatusCode, body)
	}
	if resp, _ := e.do(c, "POST", "/api/v1/pipelines/nope/actions/ping", `{}`, h); resp.StatusCode != 404 {
		t.Fatalf("unknown pipeline: %d", resp.StatusCode)
	}
	// Audit: dòng started + ok, có pipeline, args, exit_code.
	var started, ok bool
	for _, l := range e.auditLines() {
		if l["action"] == "ping" {
			if l["pipeline"] != "orders" {
				t.Errorf("pipeline missing: %v", l)
			}
			switch l["result"] {
			case "started":
				started = true
			case "ok":
				ok = true
				if l["exit_code"] != float64(0) {
					t.Errorf("exit_code: %v", l)
				}
			}
		}
	}
	if !started || !ok {
		t.Fatalf("audit lacks started/ok lines: %v", e.auditLines())
	}
}

func TestAuditLogFormatAndMode(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	e.login(c, "wrong password")
	_, csrf := e.login(c, testPassword)
	e.do(c, "GET", "/api/v1/pipelines/orders/config", "", nil)
	e.do(c, "POST", "/api/v1/logout", "", map[string]string{csrfHeader: csrf})
	e.do(c, "POST", "/api/v1/logout", "", nil) // không có session: không ghi gì thêm hợp lệ
	st, err := os.Stat(e.srv.Audit().Path())
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("audit mode %v, want 0600", st.Mode().Perm())
	}
	var actions []string
	for _, l := range e.auditLines() {
		if l["time"] == nil || l["ip"] == nil || l["action"] == nil || l["result"] == nil {
			t.Errorf("audit line lacks required fields: %v", l)
		}
		actions = append(actions, l["action"].(string)+":"+l["result"].(string))
	}
	want := []string{"login:denied", "login:ok", "config.view:ok", "logout:ok"}
	if strings.Join(actions, ",") != strings.Join(want, ",") {
		t.Fatalf("audit actions %v, want %v", actions, want)
	}
	first := e.auditLines()[0]
	if first["ip"] != "127.0.0.1" {
		t.Errorf("ip %v", first["ip"])
	}
}

func TestSecretMaskingInConfigAndErrors(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	for _, p := range []string{"/pipelines/orders/config", "/api/v1/pipelines/orders/config", "/pipelines/orders/errors", "/api/v1/pipelines/orders/errors"} {
		_, body := e.do(c, "GET", p, "", nil)
		for _, secret := range []string{"SuperSecretSrc", "tgtSecretPw", "LeakyPass"} {
			if strings.Contains(body, secret) {
				t.Errorf("%s leaks %q", p, secret)
			}
		}
	}
	_, body := e.do(c, "GET", "/api/v1/pipelines/orders/config", "", nil)
	if !strings.Contains(body, "********") || !strings.Contains(body, "DST_PW") || !strings.Contains(body, "10.0.0.1") {
		t.Errorf("masked config should keep host and env var name: %s", body)
	}
	// Khóa không phải bí mật (host, user) còn nguyên.
	if !strings.Contains(body, "strand") {
		t.Errorf("non-secrets must remain: %s", body)
	}
	// Cảnh báo khóa chưa có tác dụng được hiển thị (tables.mask).
	if !strings.Contains(body, "tables.mask") {
		t.Errorf("unimplemented key warning missing: %s", body)
	}
}

func TestMaskConfigYAMLAndSecrets(t *testing.T) {
	raw := "# top comment\nsource:\n  dsn: mysql://u:p4ss@host:3306/db\n  password: ${SRC_PW}\n  api_token: abc\n  password_env: SRC_PW\n  password_file: /x\n" +
		"  alt: \"user:pw@tcp(h:3306)/d\"\n"
	out, err := MaskConfigYAML([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"p4ss", "abc", "user:pw@"} {
		if strings.Contains(out, leak) {
			t.Errorf("leak %q in:\n%s", leak, out)
		}
	}
	for _, keep := range []string{"# top comment", "${SRC_PW}", "password_env: SRC_PW", "/x", "host:3306"} {
		if !strings.Contains(out, keep) {
			t.Errorf("lost %q in:\n%s", keep, out)
		}
	}
	for in, want := range map[string]string{
		"dial postgres://u:secret@h/db failed": "dial postgres://u:********@h/db failed",
		"x password=hunter2 y":                 "x password=******** y",
		"nothing here":                         "nothing here",
	} {
		if got := MaskSecrets(in); got != want {
			t.Errorf("MaskSecrets(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNameWhitelist(t *testing.T) {
	good := []string{"orders", "a", "orders-sync_2", "A.b-c", strings.Repeat("a", 63)}
	bad := []string{"", "-x", ".x", "a b", "a/b", "../etc", "a..b", "a;rm", "a$(x)", "a\nb", "é", strings.Repeat("a", 64), "a%2fb"}
	for _, n := range good {
		if !ValidName(n) {
			t.Errorf("%q should be valid", n)
		}
	}
	for _, n := range bad {
		if ValidName(n) {
			t.Errorf("%q should be invalid", n)
		}
	}
	for _, u := range []string{"strand@x.service", "strand@x", "my-unit.service"} {
		if !validUnit(u) {
			t.Errorf("unit %q should be valid", u)
		}
	}
	for _, u := range []string{"", "-x", "a b", "a;b", "a/b", "--now"} {
		if validUnit(u) {
			t.Errorf("unit %q should be invalid", u)
		}
	}
	cfg := &Config{PasswordHash: "x", Listen: "127.0.0.1:1", Pipelines: []PipelineEntry{{Name: "bad name", Config: "/x"}}}
	cfg.PasswordHash = mustHash(t)
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "invalid name") {
		t.Fatalf("invalid name must be rejected: %v", err)
	}
	cfg.Pipelines = []PipelineEntry{{Name: "a", Config: "/x"}, {Name: "a", Config: "/y"}}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate must be rejected: %v", err)
	}
	cfg.Pipelines = []PipelineEntry{{Name: "a", Config: "/x", SystemdUnit: "-x;y"}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("bad unit must be rejected")
	}
}

func mustHash(t *testing.T) string {
	h, err := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

func TestWhitelistAtHTTPLayer(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	_, csrf := e.login(c, testPassword)
	for _, name := range []string{"..%2fetc", "a%3Bb", "%2e%2e", "orders%00", "x%20y", "$(id)"} {
		for _, p := range []string{"/api/v1/pipelines/" + name, "/api/v1/pipelines/" + name + "/logs", "/pipelines/" + name} {
			resp, _ := e.do(c, "GET", p, "", nil)
			if resp.StatusCode != 404 && resp.StatusCode != 400 {
				t.Errorf("GET %s: %d, want 404", p, resp.StatusCode)
			}
		}
		resp, _ := e.do(c, "POST", "/api/v1/pipelines/"+name+"/actions/x", "{}", map[string]string{csrfHeader: csrf})
		if resp.StatusCode != 404 && resp.StatusCode != 400 {
			t.Errorf("POST action on %s: %d", name, resp.StatusCode)
		}
	}
	if e.srv.Registry().Get("../x") != nil || e.srv.Registry().Get("") != nil {
		t.Fatal("Get must apply the whitelist")
	}
}

func TestDiscoveryFromWebYAMLAndScan(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o640); err != nil {
			t.Fatal(err)
		}
		return p
	}
	mini := func(n string) string {
		return "name: " + n + "\nsource: {kind: mysql}\ntarget: {kind: mysql}\nobservability: {admin_socket: /nonexistent/" + n + ".sock}\n"
	}
	write("alpha.yaml", mini("alpha"))
	write("alpha.env", "DB_PW=x\n")
	write("beta.yaml", mini("beta"))
	write("bad name.yaml", mini("bad")) // tên file không hợp lệ: bỏ và cảnh báo
	write("zeta.yaml", mini("zeta"))
	explicit := write("custom.yaml", mini("custom"))
	cmd := &fakeCmd{out: map[string]string{
		"systemctl show -p LoadState --value strand@alpha.service": "loaded\n",
		"systemctl show -p LoadState --value strand@beta.service":  "not-found\n",
		"systemctl show -p LoadState --value strand@zeta.service":  "loaded\n",
	}}
	cfg := &Config{Listen: "127.0.0.1:1", Scan: []string{filepath.Join(dir, "*.yaml")}, Pipelines: []PipelineEntry{
		{Name: "custom", Config: explicit, EnvFile: "/etc/x.env", SystemdUnit: "my-custom"},
		{Name: "zeta", Config: filepath.Join(dir, "zeta.yaml")}, // khai báo tường minh thắng scan: không systemd => nền
	}}
	r := NewRegistry(cfg, cmd)
	r.Refresh(t.Context())
	var names []string
	for _, p := range r.List() {
		names = append(names, p.Name)
	}
	if strings.Join(names, ",") != "alpha,beta,custom,zeta" {
		t.Fatalf("discovered %v", names)
	}
	a := r.Get("alpha")
	if a.Mode() != ModeSystemd || a.SystemdUnit != "strand@alpha.service" || a.Origin != "scan" || a.EnvFile != filepath.Join(dir, "alpha.env") {
		t.Errorf("alpha: %+v", a)
	}
	if a.Cfg == nil || a.AdminSocket() != "/nonexistent/alpha.sock" || a.Pair() != "mysql → mysql" {
		t.Errorf("alpha cfg: %+v", a)
	}
	if b := r.Get("beta"); b.Mode() != ModeBackground || b.EnvFile != "" {
		t.Errorf("beta (no unit): %+v", b)
	}
	if c := r.Get("custom"); c.Mode() != ModeSystemd || c.SystemdUnit != "my-custom.service" || c.Origin != "web.yaml" || c.EnvFile != "/etc/x.env" {
		t.Errorf("custom: %+v", c)
	}
	if z := r.Get("zeta"); z.Mode() != ModeBackground || z.Origin != "web.yaml" {
		t.Errorf("zeta explicit must win over scan: %+v", z)
	}
	if w := r.Warnings(); len(w) != 1 || !strings.Contains(w[0], "bad name.yaml") {
		t.Errorf("warnings %v", w)
	}
	if r.Get("bad name") != nil {
		t.Error("invalid name must not be registered")
	}
	// Cache UnitExists: lần Refresh thứ hai không gọi lại systemctl cho cùng unit.
	before := len(cmd.calls)
	r.Refresh(t.Context())
	if len(cmd.calls) != before {
		t.Errorf("unit lookups should be cached, extra calls: %v", cmd.calls[before:])
	}
}

func TestBackgroundPathsAndRuntime(t *testing.T) {
	dir := t.TempDir()
	p := &Pipeline{Name: "x"}
	p.Cfg = nil
	if l, pid := p.BgPaths(); l != "strand.log" || pid != "strand.pid" {
		t.Errorf("defaults %s %s", l, pid)
	}
	cfgPath := filepath.Join(dir, "x.yaml")
	if err := os.WriteFile(cfgPath, []byte("name: x\nsource: {kind: mysql}\ntarget: {kind: mysql}\nresources: {spool: {path: "+dir+"/sp}}\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	r := NewRegistry(&Config{Pipelines: []PipelineEntry{{Name: "x", Config: cfgPath}}}, &fakeCmd{})
	r.Refresh(t.Context())
	x := r.Get("x")
	l, pid := x.BgPaths()
	if l != filepath.Join(dir, "sp", "strand.log") || pid != filepath.Join(dir, "sp", "strand.pid") {
		t.Errorf("spool paths %s %s", l, pid)
	}
	if rt := r.Runtime(t.Context(), x); rt.Running || rt.Mode != ModeBackground || rt.ActiveState != "inactive" {
		t.Errorf("no pid file: %+v", rt)
	}
	// pid file trỏ vào tiến trình không phải `strand ... sync` (chính test này): không coi là đang chạy.
	if err := os.MkdirAll(filepath.Dir(pid), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pid, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if rt := r.Runtime(t.Context(), x); rt.Running {
		t.Errorf("foreign pid must not count as running: %+v", rt)
	}
	if err := os.WriteFile(pid, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if rt := r.Runtime(t.Context(), x); rt.Error == "" {
		t.Errorf("corrupt pid file must be reported: %+v", rt)
	}
	// Log từ file.
	if err := os.WriteFile(l, []byte("a\nb\nc\nd\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	lines, err := r.Logs(t.Context(), x, 2)
	if err != nil || strings.Join(lines, ",") != "c,d" {
		t.Errorf("logs %v %v", lines, err)
	}
}

func TestSystemdRuntimeAndLogs(t *testing.T) {
	cmd := &fakeCmd{out: map[string]string{
		"systemctl show strand@o.service":                             "ActiveState=failed\nSubState=failed\nExecMainStatus=5\nExecMainPID=0\nActiveEnterTimestamp=\n",
		"journalctl -u strand@o.service -n 3 --no-pager -o short-iso": "line1\nline2\nline3\n",
	}}
	r := NewRegistry(&Config{}, cmd)
	p := &Pipeline{Name: "o", SystemdUnit: "strand@o.service"}
	rt := r.Runtime(t.Context(), p)
	if rt.Running || rt.ActiveState != "failed" || rt.ExitCode == nil || *rt.ExitCode != 5 || rt.Mode != ModeSystemd {
		t.Fatalf("runtime %+v", rt)
	}
	if st := DeriveState(nil, rt); st != StateFailed {
		t.Fatalf("state %s", st)
	}
	lines, err := r.Logs(t.Context(), p, 3)
	if err != nil || len(lines) != 3 || lines[2] != "line3" {
		t.Fatalf("logs %v %v", lines, err)
	}
	cmd.out["systemctl show strand@o.service"] = "ActiveState=active\nSubState=running\nExecMainStatus=0\nExecMainPID=4242\nActiveEnterTimestamp=Sun 2026-11-15 10:00:00 UTC\n"
	rt = r.Runtime(t.Context(), p)
	if !rt.Running || rt.PID != 4242 || rt.ExitCode != nil {
		t.Fatalf("running %+v", rt)
	}
	if DeriveState(nil, rt) != StateUnreachable {
		t.Fatal("running process without admin answer must be unreachable")
	}
	if DeriveState(nil, Runtime{ActiveState: "inactive"}) != StateStopped {
		t.Fatal("inactive must be stopped")
	}
}

func TestReadEnvFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "x.env")
	if err := os.WriteFile(p, []byte("# c\n\nA=1\nexport B=\"two words\"\nC='q'\nD=a=b\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEnvFile(p)
	if err != nil || strings.Join(got, "|") != "A=1|B=two words|C=q|D=a=b" {
		t.Fatalf("%v %v", got, err)
	}
	if err := os.WriteFile(p, []byte("not a pair\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEnvFile(p); err == nil {
		t.Fatal("malformed line must fail")
	}
}

func TestConfigValidateStartConditions(t *testing.T) {
	cfg := &Config{Listen: "127.0.0.1:8080"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "password") {
		t.Fatalf("must refuse without a password: %v", err)
	}
	cfg.PasswordHash = "plaintext-not-a-hash"
	if err := cfg.Validate(); err == nil {
		t.Fatal("must reject a non-bcrypt value")
	}
	cfg.PasswordHash = mustHash(t)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("loopback without TLS is fine: %v", err)
	}
	for _, addr := range []string{"0.0.0.0:8080", ":8080", "10.1.2.3:8080", "[::]:8080"} {
		cfg.Listen = addr
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "TLS") {
			t.Errorf("%s without TLS must be refused: %v", addr, err)
		}
	}
	for _, addr := range []string{"127.0.0.1:1", "localhost:1", "[::1]:1"} {
		cfg.Listen = addr
		if err := cfg.Validate(); err != nil {
			t.Errorf("%s: %v", addr, err)
		}
	}
	cfg.Listen = "0.0.0.0:8443"
	cfg.TLSCert, cfg.TLSKey = "c.pem", "k.pem"
	if err := cfg.Validate(); err != nil {
		t.Errorf("TLS configured: %v", err)
	}
	cfg.TLSKey = ""
	if err := cfg.Validate(); err == nil {
		t.Error("cert without key must be refused")
	}
	// New() refuses too.
	if _, err := New(Options{Config: &Config{Listen: "127.0.0.1:1", StateDir: t.TempDir()}}); err == nil {
		t.Error("New must refuse without a password")
	}
}

func TestSetConfigKeyKeepsFileAndMode(t *testing.T) {
	p := filepath.Join(t.TempDir(), "web.yaml")
	orig := "# my web config\nlisten: 127.0.0.1:9000\nscan:\n  - /etc/strand/*.yaml\n"
	if err := os.WriteFile(p, []byte(orig), 0o644); err != nil {
		t.Fatal(err)
	}
	h, err := HashPassword("longenough1")
	if err != nil {
		t.Fatal(err)
	}
	if err := SetConfigKey(p, "password_hash", h); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(p)
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v, want 0600", st.Mode().Perm())
	}
	b, _ := os.ReadFile(p)
	if !strings.Contains(string(b), "# my web config") || !strings.Contains(string(b), "/etc/strand/*.yaml") {
		t.Fatalf("lost content:\n%s", b)
	}
	c, err := LoadConfig(p)
	if err != nil || c.Listen != "127.0.0.1:9000" || bcrypt.CompareHashAndPassword([]byte(c.PasswordHash), []byte("longenough1")) != nil {
		t.Fatalf("reload: %+v %v", c, err)
	}
	// Đổi mật khẩu: ghi đè, không nhân đôi khóa.
	h2, _ := HashPassword("anotherpass9")
	if err := SetConfigKey(p, "password_hash", h2); err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile(p)
	if strings.Count(string(b), "password_hash") != 1 {
		t.Fatalf("duplicate key:\n%s", b)
	}
	// File chưa có: tạo mới 0600.
	np := filepath.Join(t.TempDir(), "new.yaml")
	if err := SetConfigKey(np, "password_hash", h); err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(np); st.Mode().Perm() != 0o600 {
		t.Fatalf("new file mode %v", st.Mode().Perm())
	}
	if _, err := HashPassword("short"); err == nil {
		t.Fatal("short password must be refused")
	}
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing file")
	}
	if err := os.WriteFile(p, []byte("bogus_key: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(p); err == nil {
		t.Fatal("unknown keys must be rejected")
	}
}
