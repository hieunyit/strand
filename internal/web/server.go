package web

import (
	"context"
	"crypto/tls"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/web/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*
var staticFS embed.FS

const (
	sessionCookie = "strand_session"
	loginCookie   = "strand_login"
	csrfHeader    = "X-CSRF-Token"
	csrfField     = "csrf"
)

// Server là Web UI + REST API.
type Server struct {
	cfg      *Config
	reg      *Registry
	samp     *Sampler
	sessions *SessionStore
	limiter  *LoginLimiter
	audit    *AuditLog
	actions  *ActionRegistry
	jobs     JobRunner // FileJobRunner (M3); test có thể thay bằng SetJobRunner
	log      *slog.Logger
	now      func() time.Time
	secure   bool // đặt cờ Secure cho cookie (TLS hoặc secure_cookies)
	tls      bool
	version  string
	db       *metaDB // nil = chế độ file

	pages   map[string]*template.Template
	handler http.Handler
}

// Options của New.
type Options struct {
	Config  *Config
	Version string
	Logger  *slog.Logger
	// Commander thay systemctl/journalctl (test). nil: chạy lệnh thật.
	Commander Commander
	// Now thay đồng hồ (test).
	Now func() time.Time
	// Store thay kho PostgreSQL (test dùng store.Mem). nil + Config.Database.DSN rỗng: chế độ file.
	Store store.Store
	// SecretKey thay STRAND_WEB_SECRET_KEY (test).
	SecretKey string
	// PreviousSecretKey thay STRAND_WEB_SECRET_KEY_PREVIOUS (test).
	PreviousSecretKey string
}

// New dựng Server (chưa lắng nghe, chưa chạy sampler). Trả lỗi nếu cấu hình không đủ điều kiện khởi động.
func New(o Options) (*Server, error) {
	if err := o.Config.validate(o.Store != nil || o.Config.DBEnabled()); err != nil {
		return nil, err
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	var db *metaDB
	if o.Store == nil && o.Config.DBEnabled() {
		st, err := OpenStore(context.Background(), o.Config, o.Logger)
		if err != nil {
			return nil, fmt.Errorf("metadata database: %w", err)
		}
		o.Store = st
	}
	if o.Store != nil {
		secret := o.SecretKey
		if secret == "" {
			secret = strings.TrimSpace(os.Getenv(EnvSecretKey))
		}
		var err error
		if db, err = newMetaDB(o.Store, secret, o.Logger); err != nil {
			return nil, err
		}
		db.previous = o.PreviousSecretKey
		if db.previous == "" {
			db.previous = strings.TrimSpace(os.Getenv(EnvSecretKeyPrevious))
		}
	}
	audit, err := OpenAuditLog(o.Config.StateDir, o.Now)
	if err != nil {
		return nil, err
	}
	if db != nil {
		audit.useDB(db, o.Config.Database.Audit)
	}
	reg := NewRegistry(o.Config, o.Commander)
	reg.now = o.Now
	reg.db = db
	iv := time.Duration(o.Config.SampleIntervalSec) * time.Second
	s := &Server{
		cfg: o.Config, reg: reg, samp: NewSampler(reg, iv),
		sessions: NewSessionStore(o.Now), limiter: NewLoginLimiter(o.Now), audit: audit,
		actions: NewActionRegistry(), log: o.Logger, now: o.Now, version: o.Version,
		tls:    o.Config.TLSCert != "",
		secure: o.Config.TLSCert != "" || o.Config.SecureCookies,
	}
	s.samp.Now = o.Now
	s.db = db
	s.sessions.db = db
	if db != nil {
		if err := s.initDB(); err != nil {
			return nil, err
		}
	}
	var jst store.Store
	if db != nil {
		jst = db.st
	}
	jr, err := NewJobRunner(filepath.Join(o.Config.StateDir, "jobs"), s.strandBinary(), o.Now, jst)
	if err != nil {
		return nil, err
	}
	jr.OnEnd = s.auditJobEnd
	s.jobs = jr
	s.registerOps()
	s.registerWizards()
	s.registerConfigEditOps()
	if s.pages, err = parsePages(); err != nil {
		return nil, err
	}
	reg.Refresh(context.Background())
	if db != nil {
		s.preloadSeries()
		s.samp.OnSample = db.metrics.add
	}
	s.handler = s.routes()
	return s, nil
}

// initDB: kiểm tài khoản admin trong DB, nhập trạng thái file một lần, khôi phục file pipeline từ DB.
func (s *Server) initDB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := s.db.checkSecretKey(ctx, s.db.previous); err != nil {
		return err
	}
	sum, err := s.importFiles(ctx)
	if err != nil {
		return fmt.Errorf("import file state into database: %w", err)
	}
	if sum != nil {
		s.log.Info("imported existing file state into the database (first start with a database)",
			"admin_password", sum.Admin, "api_token", sum.Token, "pipelines", sum.Pipelines, "jobs", sum.Jobs,
			"wizards", sum.Wizards, "audit_lines", sum.Audit)
		_ = s.audit.Log(AuditEntry{Action: "state.import", Result: "ok", Detail: map[string]any{
			"pipelines": sum.Pipelines, "jobs": sum.Jobs, "wizards": sum.Wizards, "audit_lines": sum.Audit}})
	}
	pw, _ := s.db.adminHashes()
	if pw == "" {
		return errors.New("no admin password in the database: run `strand web passwd --config <web.yaml>` (or `strand web init`) first")
	}
	s.materializeAll()
	return nil
}

// Handler trả http.Handler đầy đủ (middleware + route), dùng cho httptest.
func (s *Server) Handler() http.Handler { return s.handler }

// Sampler cho phép test/M3 truy cập bộ lấy mẫu.
func (s *Server) Sampler() *Sampler { return s.samp }

// Registry trả danh sách pipeline.
func (s *Server) Registry() *Registry { return s.reg }

// Audit trả audit log (M3 ghi mọi thao tác qua đây).
func (s *Server) Audit() *AuditLog { return s.audit }

// Actions trả registry thao tác (điểm mở rộng M3-M5).
func (s *Server) Actions() *ActionRegistry { return s.actions }

// SetJobRunner gắn bộ chạy tiến trình con (M3).
func (s *Server) SetJobRunner(j JobRunner) { s.jobs = j }

// ListenAndServe chạy sampler và HTTP(S) server cho tới khi ctx hủy.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve phục vụ trên ln.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.samp.Run(ctx)
	go s.ResumeDesired(ctx)
	go s.runMetricsHistory(ctx)
	srv := &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	if s.tls {
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	errc := make(chan error, 1)
	go func() {
		if s.tls {
			errc <- srv.ServeTLS(ln, s.cfg.TLSCert, s.cfg.TLSKey)
		} else {
			errc <- srv.Serve(ln)
		}
	}()
	select {
	case <-ctx.Done():
		sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
		if s.db != nil && s.cfg.ShutdownStopPipelines {
			s.stopForShutdown()
		}
		_ = s.audit.Close()
		if s.db != nil {
			s.flushMetrics()
			_ = s.db.st.Close()
		}
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(sub)))

	// Đăng nhập / đăng xuất (không cần session).
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /login", s.handleLoginPage)
	mux.HandleFunc("POST /login", s.handleLoginForm)
	mux.HandleFunc("POST /api/v1/login", s.handleLoginAPI)
	mux.Handle("POST /logout", s.auth(false, s.handleLogoutForm))
	mux.Handle("POST /api/v1/logout", s.auth(true, s.handleLogoutAPI))

	// Trang.
	mux.Handle("GET /{$}", s.auth(false, s.pageOverview))
	mux.Handle("GET /pipelines", s.auth(false, s.pagePipelines))
	mux.Handle("GET /pipelines/new", s.auth(false, s.pageNewPipeline))
	mux.Handle("POST /pipelines/new", s.auth(false, s.apiPipelineCreate))
	mux.Handle("GET /jobs", s.auth(false, s.pageJobs))
	mux.Handle("GET /jobs/{id}", s.auth(false, s.pageJob))
	for _, tab := range []string{"", "tables", "checkpoint", "errors", "config", "logs", "jobs"} {
		tab := tab
		p := "GET /pipelines/{name}"
		if tab != "" {
			p += "/" + tab
		}
		mux.Handle(p, s.auth(false, func(w http.ResponseWriter, r *http.Request, sess *Session) {
			s.pageDetail(w, r, sess, tab)
		}))
	}

	// REST API.
	mux.Handle("GET /api/v1/session", s.auth(true, s.apiSession))
	mux.Handle("GET /api/v1/overview", s.auth(true, s.apiOverview))
	mux.Handle("GET /api/v1/pipelines", s.auth(true, s.apiPipelines))
	mux.Handle("POST /api/v1/pipelines", s.auth(true, s.apiPipelineCreate))
	mux.Handle("GET /api/v1/pipelines/{name}", s.auth(true, s.apiPipeline))
	mux.Handle("POST /api/v1/pipelines/{name}/config/validate", s.auth(true, s.apiConfigValidate))
	mux.Handle("POST /api/v1/pipelines/{name}/config", s.auth(true, s.apiConfigSave))
	mux.Handle("GET /api/v1/pipelines/{name}/tuning", s.auth(true, s.apiTuningGet))
	mux.Handle("POST /api/v1/pipelines/{name}/tuning", s.auth(true, s.apiTuningSave))
	mux.Handle("GET /api/v1/pipelines/{name}/env", s.auth(true, s.apiEnvGet))
	mux.Handle("POST /api/v1/pipelines/{name}/env", s.auth(true, s.apiEnvSave))
	for _, tab := range []string{"status", "tables", "checkpoint", "errors", "config", "logs", "series"} {
		tab := tab
		mux.Handle("GET /api/v1/pipelines/{name}/"+tab, s.auth(true, func(w http.ResponseWriter, r *http.Request, sess *Session) {
			s.apiPipelineTab(w, r, tab)
		}))
	}
	mux.Handle("GET /api/v1/pipelines/{name}/source-tables", s.auth(true, s.apiSourceTables))
	mux.Handle("GET /api/v1/pipelines/{name}/jobs", s.auth(true, s.apiPipelineJobs))
	mux.Handle("GET /api/v1/jobs", s.auth(true, s.apiJobs))
	mux.Handle("GET /api/v1/jobs/{id}", s.auth(true, s.apiJob))
	mux.Handle("GET /api/v1/jobs/{id}/output", s.auth(true, s.apiJobOutput))
	mux.Handle("POST /api/v1/pipelines/{name}/actions/{action}", s.auth(true, s.apiAction))
	s.wizardRoutes(mux)
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		// Mọi đường dẫn /api/ không khớp: vẫn đòi đăng nhập trước khi tiết lộ gì (404 sau 401).
		s.auth(true, func(w http.ResponseWriter, r *http.Request, _ *Session) {
			writeErr(w, http.StatusNotFound, "not found")
		}).ServeHTTP(w, r)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.auth(false, func(w http.ResponseWriter, r *http.Request, _ *Session) {
			http.NotFound(w, r)
		}).ServeHTTP(w, r)
	})
	return s.secureHeaders(mux)
}

// secureHeaders thêm header bảo mật vào mọi phản hồi.
func (s *Server) secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Content-Type-Options", "nosniff")
		if !strings.HasPrefix(r.URL.Path, "/static/") {
			h.Set("Cache-Control", "no-store")
		}
		if s.secure {
			h.Set("Strict-Transport-Security", "max-age=31536000")
		}
		next.ServeHTTP(w, r)
	})
}

type authedHandler func(w http.ResponseWriter, r *http.Request, sess *Session)

// auth đòi xác thực: session cookie (kèm CSRF cho phương thức ghi) hoặc Bearer token tĩnh.
// api=true: trả 401 JSON; ngược lại chuyển về /login (302).
func (s *Server) auth(api bool, next authedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tok := bearer(r); tok != "" {
			if s.apiTokenHash() != "" && constEq(hashKey(tok), s.apiTokenHash()) {
				// Token tĩnh không dùng cookie nên không cần CSRF.
				next(w, r, &Session{CSRF: "", IP: s.clientIP(r)})
				return
			}
			s.deny(w, r, api)
			return
		}
		var sess *Session
		if c, err := r.Cookie(sessionCookie); err == nil {
			sess = s.sessions.Get(c.Value)
		}
		if sess == nil {
			s.deny(w, r, api)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
			got := r.Header.Get(csrfHeader)
			if got == "" {
				_ = r.ParseForm()
				got = r.PostForm.Get(csrfField)
			}
			if got == "" || !constEq(got, sess.CSRF) {
				s.auditDenied(r, "csrf_rejected")
				writeErr(w, http.StatusForbidden, "missing or invalid CSRF token")
				return
			}
		}
		next(w, r, sess)
	})
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

func (s *Server) deny(w http.ResponseWriter, r *http.Request, api bool) {
	if api {
		w.Header().Set("WWW-Authenticate", `Bearer realm="strand"`)
		writeErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	next := r.URL.RequestURI()
	target := "/login"
	if r.Method == http.MethodGet && safeNext(next) && next != "/" {
		target += "?next=" + url.QueryEscape(next)
	}
	http.Redirect(w, r, target, http.StatusFound)
}

func safeNext(n string) bool {
	return strings.HasPrefix(n, "/") && !strings.HasPrefix(n, "//") && !strings.Contains(n, "\\")
}

func (s *Server) clientIP(r *http.Request) string {
	if s.cfg.TrustForwardedFor {
		if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
			first, _, _ := strings.Cut(xf, ",")
			if ip := net.ParseIP(strings.TrimSpace(first)); ip != nil {
				return ip.String()
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) auditDenied(r *http.Request, action string) {
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: action, Result: "denied",
		Args: []string{r.Method, r.URL.Path}})
}

// ---- đăng nhập ----

func (s *Server) setCookie(w http.ResponseWriter, name, val string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: val, Path: "/", HttpOnly: true, Secure: s.secure,
		SameSite: http.SameSiteStrictMode, MaxAge: maxAge})
}

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil && s.sessions.Get(c.Value) != nil {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	s.renderLogin(w, r, http.StatusOK, "")
}

func (s *Server) renderLogin(w http.ResponseWriter, r *http.Request, status int, msg string) {
	tok := randToken(24)
	s.setCookie(w, loginCookie, tok, loginCookieMaxAge)
	next := r.URL.Query().Get("next")
	if r.Method == http.MethodPost {
		next = r.FormValue("next")
	}
	if !safeNext(next) {
		next = ""
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_ = s.pages["login"].ExecuteTemplate(w, "login", map[string]any{"CSRF": tok, "Error": msg, "Next": next, "Version": s.version})
}

// loginCookieMaxAge: thời hạn cookie CSRF của trang đăng nhập (giây). Để trang đăng nhập mở lâu vẫn dùng được.
const loginCookieMaxAge = 3600

// loginCSRFReason trả "" nếu request đăng nhập qua kiểm CSRF, ngược lại lý do (ghi audit).
// Hai cách được chấp nhận:
//  1. double-submit: token trong form/header khớp cookie strand_login (cách cũ; client không phải trình duyệt
//     như curl dùng cách này);
//  2. trình duyệt xác nhận cùng origin: Sec-Fetch-Site: same-origin, hoặc Origin trùng host của request.
//     Trình duyệt luôn gửi các header này với POST và trang khác không giả được, nên đây là kiểm CSRF chuẩn
//     (OWASP "verifying origin with standard headers"). Cách này cứu trường hợp cookie strand_login không về
//     tới server hoặc là cookie cũ: mở trang đăng nhập ở tab khác, trang để quá lâu, trình duyệt giữ cookie
//     Secure cùng tên từ lần vào qua HTTPS (HTTP không ghi đè được), chặn cookie theo IP...
//
// Request cross-site (Sec-Fetch-Site: cross-site / same-site, hoặc Origin khác host) luôn bị từ chối nếu
// không có double-submit hợp lệ.
func loginCSRFReason(r *http.Request) string {
	got := r.Header.Get(csrfHeader)
	if got == "" {
		got = r.PostForm.Get(csrfField)
	}
	c, cerr := r.Cookie(loginCookie)
	if cerr == nil && got != "" && constEq(got, c.Value) {
		return ""
	}
	sfs := r.Header.Get("Sec-Fetch-Site")
	origin := r.Header.Get("Origin")
	switch {
	case sfs == "same-origin":
		return ""
	case sfs != "" && sfs != "none":
		return "cross_site_" + sfs
	case origin != "" && origin != "null":
		if u, err := url.Parse(origin); err == nil && u.Host != "" && strings.EqualFold(u.Host, r.Host) {
			return ""
		}
		return "origin_mismatch"
	}
	switch {
	case cerr != nil:
		return "no_login_cookie"
	case got == "":
		return "no_token"
	default:
		return "token_mismatch"
	}
}

// checkLogin kiểm CSRF của form đăng nhập (cookie ↔ trường/ header), giới hạn tốc độ rồi mật khẩu.
// Trả (status, thông báo); status 0 = thành công.
func (s *Server) checkLogin(r *http.Request, password string) (int, string) {
	ip := s.clientIP(r)
	if reason := loginCSRFReason(r); reason != "" {
		_ = s.audit.Log(AuditEntry{IP: ip, Action: "login_csrf_rejected", Result: "denied",
			Args: []string{r.Method, r.URL.Path}, Detail: map[string]any{"reason": reason}})
		return http.StatusForbidden, "missing or invalid CSRF token; reload the page and try again"
	}
	if ok, wait := s.limiter.Allow(ip); !ok {
		_ = s.audit.Log(AuditEntry{IP: ip, Action: "login", Result: "denied", Detail: map[string]any{"reason": "rate_limited", "retry_after_s": int(wait.Seconds()) + 1}})
		return http.StatusTooManyRequests, "too many failed attempts; try again in " + strconv.Itoa(int(wait.Seconds())+1) + " s"
	}
	if bcrypt.CompareHashAndPassword([]byte(s.passwordHash()), []byte(password)) != nil {
		s.limiter.Fail(ip)
		_ = s.audit.Log(AuditEntry{IP: ip, Action: "login", Result: "denied", Detail: map[string]any{"reason": "bad_password"}})
		return http.StatusUnauthorized, "wrong password"
	}
	s.limiter.Success(ip)
	return 0, ""
}

func (s *Server) startSession(w http.ResponseWriter, r *http.Request) *Session {
	tok, sess := s.sessions.Create(s.clientIP(r))
	if sess == nil {
		return nil
	}
	s.setCookie(w, sessionCookie, tok, int(SessionMaxAge.Seconds()))
	s.setCookie(w, loginCookie, "", -1)
	_ = s.audit.Log(AuditEntry{IP: sess.IP, Action: "login", Result: "ok"})
	return sess
}

func (s *Server) handleLoginForm(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		writeErr(w, http.StatusBadRequest, "bad form")
		return
	}
	status, msg := s.checkLogin(r, r.PostForm.Get("password"))
	if status != 0 {
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "60")
		}
		s.renderLogin(w, r, status, msg)
		return
	}
	if s.startSession(w, r) == nil {
		s.renderLogin(w, r, http.StatusServiceUnavailable, "session store unavailable; try again shortly")
		return
	}
	next := r.PostForm.Get("next")
	if !safeNext(next) {
		next = "/"
	}
	http.Redirect(w, r, next, http.StatusSeeOther)
}

func (s *Server) handleLoginAPI(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var body struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "expected JSON {\"password\": \"...\"}")
		return
	}
	status, msg := s.checkLogin(r, body.Password)
	if status != 0 {
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "60")
		}
		writeErr(w, status, msg)
		return
	}
	sess := s.startSession(w, r)
	if sess == nil {
		writeErr(w, http.StatusServiceUnavailable, "session store unavailable; try again shortly")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"csrf": sess.CSRF, "idle_timeout_s": int(SessionIdleTimeout.Seconds()), "max_age_s": int(SessionMaxAge.Seconds())})
}

func (s *Server) doLogout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.sessions.Delete(c.Value)
	}
	s.setCookie(w, sessionCookie, "", -1)
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "logout"})
}

func (s *Server) handleLogoutForm(w http.ResponseWriter, r *http.Request, _ *Session) {
	s.doLogout(w, r)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) handleLogoutAPI(w http.ResponseWriter, r *http.Request, _ *Session) {
	s.doLogout(w, r)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ---- tiện ích phản hồi ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (s *Server) String() string { return fmt.Sprintf("strand web on %s", s.cfg.Listen) }

// auditJobEnd ghi dòng audit khi job kết thúc: result ok khi exit 0, error khi khác 0 hoặc không quan sát được.
func (s *Server) auditJobEnd(j JobStatus) {
	res := "error"
	if j.ExitCode != nil && *j.ExitCode == 0 {
		res = "ok"
	}
	detail := map[string]any{"job_id": j.ID, "kind": j.Kind}
	if j.Cancelled {
		detail["cancelled"] = true
	}
	if j.Error != "" {
		detail["error"] = j.Error
	}
	_ = s.audit.Log(AuditEntry{Action: "job.end", Pipeline: j.Pipeline, Args: append([]string{j.Kind}, j.Args...), Result: res, ExitCode: j.ExitCode, Detail: detail})
}
