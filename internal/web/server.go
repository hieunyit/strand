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
	"net/smtp"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/backup"
	"strand/internal/backup/conncheck"
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
	// ownedBackupProcs: job runner id → pid của tiến trình backup (scheduler/Run now) do instance NÀY khởi
	// động; dùng khi mất quyền active (stopBackupSchedulers).
	ownedBackupProcs  sync.Map
	configEditMu      sync.Mutex
	sourceTablesMu    sync.Mutex
	sourceTablesCache map[string]sourceTablesCacheEntry
	cfg               *Config
	reg               *Registry
	samp              *Sampler
	sessions          *SessionStore
	limiter           *LoginLimiter
	audit             *AuditLog
	actions           *ActionRegistry
	jobs              JobRunner // FileJobRunner (M3); test có thể thay bằng SetJobRunner
	log               *slog.Logger
	now               func() time.Time
	tls               bool
	live              liveSettings // cài đặt hiệu lực (trang Settings)
	version           string
	db                *metaDB      // kho metadata PostgreSQL (Mem trong test); luôn khác nil
	stops             *stopTracker // dừng do web thực hiện (phân biệt với dừng ngoài web)
	lease             *leaseMgr    // chế độ DB: lease instance active (lease.go)
	desired           desiredCache
	backups           *backup.Catalog
	checker           conncheck.Checker                                                          // connection tests (nil: live)
	notifyHTTP        *http.Client                                                               // notification deliveries (nil: default client)
	notifyMail        func(addr string, a smtp.Auth, from string, to []string, msg []byte) error // nil: smtp.SendMail

	pages   map[string]*template.Template
	handler http.Handler

	tunnelMu        sync.Mutex
	tunnels         map[string]activeTunnel
	tunnelCtx       context.Context
	tunnelCancel    context.CancelFunc
	archiverMu      sync.Mutex
	archiverRunning bool
	archiverDir     string
	archiverJobID   string

	// Luồng sống (stream.go): hub phát sự kiện của sampler; delta nhớ phần đã phát; streams đếm kết nối SSE.
	hub          *liveHub
	delta        liveDelta
	streams      atomic.Int32
	sseHeartbeat time.Duration
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
	// Store thay kho PostgreSQL (test dùng store.Mem). nil: mở Config.Database.DSN (bắt buộc).
	Store store.Store
	// SecretKey thay STRAND_WEB_SECRET_KEY (test).
	SecretKey string
	// PreviousSecretKey thay STRAND_WEB_SECRET_KEY_PREVIOUS (test).
	PreviousSecretKey string
	// LeaseTTL thay thời hạn lease instance active (test; mặc định 30 s hoặc STRAND_WEB_LEASE_TTL).
	LeaseTTL time.Duration
	// ConnChecker thay bộ kiểm tra kết nối backup (test dùng bản giả). nil: kiểm tra thật.
	ConnChecker conncheck.Checker
	// NotifyHTTPClient / NotifySendMail replace the notification transports (tests).
	NotifyHTTPClient *http.Client
	NotifySendMail   func(addr string, a smtp.Auth, from string, to []string, msg []byte) error
}

// New dựng Server (chưa lắng nghe, chưa chạy sampler). Trả lỗi nếu cấu hình không đủ điều kiện khởi động.
func New(o Options) (*Server, error) {
	if err := o.Config.Validate(); err != nil {
		return nil, err
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Store == nil && !o.Config.DBEnabled() {
		return nil, errors.New(ErrNoDatabase)
	}
	secret := o.SecretKey
	if secret == "" {
		secret = strings.TrimSpace(os.Getenv(EnvSecretKey))
	}
	if secret == "" {
		return nil, errors.New(ErrNoSecretKey)
	}
	if o.Store == nil {
		st, err := OpenStore(context.Background(), o.Config, o.Logger)
		if err != nil {
			return nil, fmt.Errorf("metadata database: %w", err)
		}
		o.Store = st
	}
	db, err := newMetaDB(o.Store, secret, o.Logger)
	if err != nil {
		return nil, err
	}
	db.previous = o.PreviousSecretKey
	if db.previous == "" {
		db.previous = strings.TrimSpace(os.Getenv(EnvSecretKeyPrevious))
	}
	audit, err := OpenAuditLog(o.Config.StateDir, o.Now)
	if err != nil {
		return nil, err
	}
	audit.useDB(db, o.Config.Database.Audit)
	reg := NewRegistry(o.Config, o.Commander)
	reg.now = o.Now
	reg.db = db
	tunnelCtx, tunnelCancel := context.WithCancel(context.Background())
	s := &Server{
		cfg: o.Config, reg: reg,
		sourceTablesCache: make(map[string]sourceTablesCacheEntry),
		sessions:          NewSessionStore(o.Now), limiter: NewLoginLimiter(o.Now), audit: audit,
		actions: NewActionRegistry(), log: o.Logger, now: o.Now, version: o.Version,
		tls:     o.Config.TLSCert != "",
		tunnels: make(map[string]activeTunnel), tunnelCtx: tunnelCtx, tunnelCancel: tunnelCancel,
	}
	s.db = db
	s.checker = o.ConnChecker
	s.notifyHTTP, s.notifyMail = o.NotifyHTTPClient, o.NotifySendMail
	s.backups, err = backup.NewCatalog(db.st)
	if err != nil {
		return nil, err
	}
	s.stops = newStopTracker()
	s.sessions.db = db
	// Cài đặt hiệu lực (env > DB > web.yaml > mặc định), trước sampler vì chu kỳ lấy mẫu là một cài đặt.
	if err := s.loadSettings(true); err != nil {
		return nil, fmt.Errorf("load settings from the metadata database: %w", err)
	}
	reg.scanFn = func() []string { return s.settingList("scan") }
	audit.rotateBytes = func() int64 { return int64(s.settingInt("audit_rotate_mb")) << 20 }
	reg.writable = s.isActive
	iv, _ := s.live.get("sample_interval_sec").(int)
	s.samp = NewSampler(reg, time.Duration(iv)*time.Second)
	s.samp.Now = o.Now
	s.hub = newLiveHub()
	s.delta = liveDelta{tables: map[string]map[string]string{}, events: map[string]map[string]bool{}}
	s.sseHeartbeat = 15 * time.Second
	s.samp.OnSample = s.onSample
	s.samp.OnRound = s.onRound
	if err := s.initDB(); err != nil {
		return nil, err
	}
	{
		sctx, scancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := s.seedBackupPolicies(sctx); err != nil {
			o.Logger.Warn("seed built-in backup policies failed (retried on the next start)", "error", err)
		}
		scancel()
	}
	// Nhiều instance trên một DB: chỉ instance giữ lease là active; không lấy được thì chạy standby (chỉ đọc).
	s.lease = newLeaseMgr(o.Config.Listen, o.Version, leaseTTL(o))
	s.leaseTick()
	if !s.isActive() {
		o.Logger.Warn("another strand web instance holds the active lease: this instance runs as STANDBY (read-only) and takes over when the lease expires",
			"active", s.leaseInfo().Active)
	}
	jr, err := newJobRunner(filepath.Join(o.Config.StateDir, "jobs"), s.strandBinary(), o.Now, db.st, !s.isActive())
	if err != nil {
		return nil, err
	}
	jr.OnEnd = s.auditJobEnd
	s.jobs = jr
	if err := s.resumeBackupJobs(context.Background()); err != nil {
		s.log.Error("resume scheduled backup jobs", "error", err)
	}
	s.registerOps()
	s.registerWizards()
	s.registerConfigEditOps()
	s.registerRegistryOps()
	s.registerCLIOps()
	if s.pages, err = parsePages(); err != nil {
		return nil, err
	}
	reg.Refresh(context.Background())
	s.preloadSeries()
	s.refreshDesired()
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
	s.db.invalidateAdminCache()
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
	s.ensureAdminUser()
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
	defer func() {
		s.closeTunnels()
		if s.tunnelCancel != nil {
			s.tunnelCancel()
		}
	}()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.samp.Run(ctx)
	go s.ResumeDesired(ctx)
	go s.runMetricsHistory(ctx)
	go s.runLease(ctx)
	go s.runAuditMaintenance(ctx)
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
		if s.settingBool("shutdown_stop_pipelines") && s.isActive() {
			s.stopForShutdown()
		}
		s.flushMetrics()
		s.releaseLease() // standby tiếp quản ngay, không chờ hết TTL
		_ = s.audit.Close()
		_ = s.db.st.Close()
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
	mux.Handle("GET /settings", s.auth(false, s.pageSettings))
	mux.Handle("POST /settings", s.auth(false, s.pageSettings))
	mux.Handle("GET /api/v1/settings", s.auth(true, s.apiSettingsGet))
	mux.Handle("POST /api/v1/settings", s.auth(true, s.apiSettingsSave))

	// RBAC: admin-only read
	mux.Handle("GET /settings/db", s.auth(false, s.pageDBAdmin))
	mux.Handle("POST /settings/db/migrate", s.auth(false, s.pageDBMigrate))
	mux.Handle("POST /settings/db/export", s.auth(false, s.pageDBExport))

	mux.Handle("GET /help/schema", s.auth(false, s.pageSchemaHelp))

	// RBAC: admin-only read
	mux.Handle("GET /tools/spool-inspect", s.auth(false, s.pageSpoolInspect))
	mux.Handle("POST /tools/spool-inspect", s.auth(false, s.pageSpoolInspect))
	mux.Handle("POST /api/v1/tools/spool-inspect", s.auth(true, s.apiSpoolInspect))

	// RBAC: admin-only read
	mux.Handle("GET /archive", s.auth(false, s.pageArchive))
	mux.Handle("POST /archive/start", s.auth(false, s.pageArchiveStart))
	mux.Handle("POST /archive/stop", s.auth(false, s.pageArchiveStop))
	mux.Handle("POST /api/v1/archive/start", s.auth(true, s.apiArchiveStart))
	mux.Handle("POST /api/v1/archive/stop", s.auth(true, s.apiArchiveStop))

	// RBAC: admin-only read
	mux.Handle("GET /tunnels", s.auth(false, s.pageTunnel))
	mux.Handle("POST /tunnels", s.auth(false, s.pageTunnelOpen))
	mux.Handle("POST /tunnels/stop", s.auth(false, s.pageTunnelStop))
	mux.Handle("POST /api/v1/tunnels", s.auth(true, s.apiTunnelOpen))
	mux.Handle("POST /api/v1/tunnels/{id}/stop", s.auth(true, s.apiTunnelStop))

	// Users management (admin only)
	mux.Handle("GET /users", s.auth(false, s.pageUsers))
	mux.Handle("POST /users/create", s.auth(false, s.handleUserCreateForm))
	mux.Handle("POST /users/disable", s.auth(false, s.handleUserDisableForm))
	mux.Handle("POST /users/enable", s.auth(false, s.handleUserEnableForm))
	mux.Handle("POST /users/reset-password", s.auth(false, s.handleUserResetPasswordForm))
	mux.Handle("POST /users/tokens/revoke", s.auth(false, s.handleUserRevokeTokensForm))
	mux.Handle("POST /users/delete", s.auth(false, s.handleUserDeleteForm))
	mux.Handle("GET /api/v1/users", s.auth(true, s.apiUsersList))
	mux.Handle("POST /api/v1/users", s.auth(true, s.apiUserCreate))
	mux.Handle("GET /api/v1/users/{username}", s.auth(true, s.apiUserGet))
	mux.Handle("POST /api/v1/users/{username}/role", s.auth(true, s.apiUserSetRole))
	mux.Handle("POST /api/v1/users/{username}/disable", s.auth(true, s.apiUserDisable))
	mux.Handle("POST /api/v1/users/{username}/enable", s.auth(true, s.apiUserEnable))
	mux.Handle("POST /api/v1/users/{username}/reset-password", s.auth(true, s.apiUserResetPassword))
	mux.Handle("POST /api/v1/users/{username}/tokens/revoke", s.auth(true, s.apiUserRevokeTokens))
	mux.Handle("DELETE /api/v1/users/{username}", s.auth(true, s.apiUserDelete))

	// My account (all authenticated users)
	mux.Handle("GET /account", s.auth(false, s.pageAccount))
	mux.Handle("POST /account/password", s.auth(false, s.handleAccountPasswordForm))
	mux.Handle("POST /account/tokens/create", s.auth(false, s.handleAccountTokenCreateForm))
	mux.Handle("POST /account/tokens/revoke", s.auth(false, s.handleAccountTokenRevokeForm))
	mux.Handle("POST /account/sessions/signout-others", s.auth(false, s.handleAccountSignoutOthersForm))
	mux.Handle("GET /api/v1/account", s.auth(true, s.apiAccountGet))
	mux.Handle("POST /api/v1/account/password", s.auth(true, s.apiAccountChangePassword))
	mux.Handle("GET /api/v1/account/tokens", s.auth(true, s.apiAccountTokensList))
	mux.Handle("POST /api/v1/account/tokens", s.auth(true, s.apiAccountTokenCreate))
	mux.Handle("DELETE /api/v1/account/tokens/{id}", s.auth(true, s.apiAccountTokenRevoke))
	mux.Handle("GET /api/v1/account/sessions", s.auth(true, s.apiAccountSessionsList))
	mux.Handle("POST /api/v1/account/sessions/signout-others", s.auth(true, s.apiAccountSignoutOthers))
	mux.Handle("GET /jobs/{id}", s.auth(false, s.pageJob))

	// RBAC: admin-only read
	mux.Handle("GET /api/v1/jobs/{id}/download", s.auth(true, s.apiJobDownload))
	mux.Handle("GET /backups", s.auth(false, s.pageBackups))
	mux.Handle("GET /backups/jobs", s.auth(false, s.pageBackupsJobs))
	mux.Handle("GET /backups/jobs/{id}", s.auth(false, s.pageBackupsJobDetail))
	mux.Handle("GET /backups/catalog", s.auth(false, s.pageBackupsCatalog))
	mux.Handle("GET /backups/catalog/{id}", s.auth(false, s.pageBackupsCatalogDetail))
	mux.Handle("GET /backups/restore", s.auth(false, s.pageBackupsRestore))

	s.backupConnectionRoutes(mux)
	s.backupPolicyRoutes(mux)
	s.backupUIRoutes(mux)
	mux.Handle("GET /api/v1/backup-jobs", s.auth(true, s.apiBackupJobs))
	mux.Handle("GET /api/v1/backup-jobs/{id}", s.auth(true, s.apiBackupJobGet))
	mux.Handle("GET /api/v1/backup-jobs/{id}/runs", s.auth(true, s.apiBackupJobRuns))
	mux.Handle("POST /api/v1/backup-jobs", s.auth(true, s.apiBackupJobCreate))
	mux.Handle("PUT /api/v1/backup-jobs/{id}", s.auth(true, s.apiBackupJobUpdate))
	mux.Handle("DELETE /api/v1/backup-jobs/{id}", s.auth(true, s.apiBackupJobDelete))
	mux.Handle("POST /api/v1/backup-jobs/{id}/start", s.auth(true, s.apiBackupJobStart))
	mux.Handle("POST /api/v1/backup-jobs/{id}/stop", s.auth(true, s.apiBackupJobStop))
	mux.Handle("POST /api/v1/backup-jobs/{id}/pause", s.auth(true, s.apiBackupJobStop))
	mux.Handle("POST /api/v1/backup-jobs/{id}/resume", s.auth(true, s.apiBackupJobStart))
	mux.Handle("POST /api/v1/backup-jobs/{id}/run", s.auth(true, s.apiBackupJobRun))
	mux.Handle("POST /api/v1/backup-runs/{id}/cancel", s.auth(true, s.apiBackupRunCancel))
	for _, tab := range []string{"", "tables", "checkpoint", "errors", "config", "logs", "jobs", "schema", "objects"} {
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
	mux.Handle("GET /api/v1/stream", s.auth(true, s.apiStream))
	mux.Handle("GET /api/v1/live", s.auth(true, s.apiLive))
	mux.Handle("GET /api/v1/pipelines", s.auth(true, s.apiPipelines))
	mux.Handle("POST /api/v1/pipelines", s.auth(true, s.apiPipelineCreate))
	mux.Handle("POST /api/v1/source/databases", s.auth(true, s.apiSourceDatabases))
	mux.Handle("GET /api/v1/pipelines/{name}", s.auth(true, s.apiPipeline))
	mux.Handle("DELETE /api/v1/pipelines/{name}", s.auth(true, s.apiPipelineDelete))
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
	mux.Handle("GET /api/v1/pipelines/{name}/config/download", s.auth(true, s.apiConfigDownload))
	mux.Handle("GET /pipelines/{name}/config.yaml", s.auth(false, s.apiConfigDownload))
	mux.Handle("GET /mcp", s.auth(false, s.pageMCP))
	mux.Handle("GET /api/v1/mcp", s.auth(true, s.apiMCP))
	mux.Handle("GET /api/v1/pipelines/{name}/jobs", s.auth(true, s.apiPipelineJobs))
	mux.Handle("GET /api/v1/jobs", s.auth(true, s.apiJobs))
	mux.Handle("GET /api/v1/jobs/{id}", s.auth(true, s.apiJob))
	mux.Handle("GET /api/v1/jobs/{id}/output", s.auth(true, s.apiJobOutput))
	s.backupProRoutes(mux)
	mux.Handle("GET /api/v1/backups", s.auth(true, s.apiBackups))
	mux.Handle("GET /api/v1/backups/{id}", s.auth(true, s.apiBackup))
	mux.Handle("GET /api/v1/backups/{id}/status", s.auth(true, s.apiBackupStatus))
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

// secureCookies: cờ Secure cho cookie (web phục vụ TLS, hoặc cài đặt secure_cookies sau reverse proxy TLS).
func (s *Server) secureCookies() bool { return s.tls || s.settingBool("secure_cookies") }

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
		if s.secureCookies() {
			h.Set("Strict-Transport-Security", "max-age=31536000")
		}
		next.ServeHTTP(w, r)
	})
}

type authedHandler func(w http.ResponseWriter, r *http.Request, sess *Session)

type contextKey string

const userCtxKey contextKey = "strand_user"

func (s *Server) ensureAdminUser() {
	if s.db == nil || s.db.st == nil {
		return
	}
	ctx, cancel := dbCtx()
	defer cancel()
	_, found, err := s.db.st.GetUser(ctx, "admin")
	if err == nil && !found {
		if pwHash := s.passwordHash(); pwHash != "" {
			_, _ = s.db.st.CreateUser(ctx, store.User{
				Username:           "admin",
				DisplayName:        "Administrator",
				PasswordHash:       pwHash,
				Role:               store.RoleAdmin,
				MustChangePassword: false,
			})
		}
	}
}

// auth đòi xác thực: session cookie (kèm CSRF cho phương thức ghi) hoặc Bearer token tĩnh.
// api=true: trả 401 JSON; ngược lại chuyển về /login (302).
func (s *Server) auth(api bool, next authedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tok := bearer(r); tok != "" {
			// First, check scoped user API tokens
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			u, _, ok, err := s.db.st.AuthenticateToken(ctx, hashKey(tok))
			cancel()
			if err == nil && ok {
				if u.Disabled {
					s.deny(w, r, api)
					return
				}
				sess := &Session{
					UserID:   u.ID,
					Username: u.Username,
					Role:     u.Role,
					CSRF:     "",
					IP:       s.clientIP(r),
				}
				required := RequiredPermissionForRequest(r)
				if !RoleAllows(sess.Role, required) {
					s.auditDenied(r, sess.Username, "forbidden_insufficient_role")
					s.denyForbidden(w, r, api, required)
					return
				}
				if s.standbyGuard(w, r) {
					return
				}
				r = r.WithContext(context.WithValue(r.Context(), userCtxKey, sess.Username))
				next(w, r, sess)
				return
			}

			// Fallback: legacy global token (treated as admin with deprecation note)
			if s.apiTokenHash() != "" && constEq(hashKey(tok), s.apiTokenHash()) {
				s.log.Warn("legacy global API token used; deprecation notice: migrate to per-user API tokens")
				w.Header().Set("Deprecation", "true")
				w.Header().Set("Warning", `299 - "Legacy API token is deprecated; migrate to per-user API tokens"`)
				sess := &Session{
					UserID:   0,
					Username: "admin",
					Role:     store.RoleAdmin,
					CSRF:     "",
					IP:       s.clientIP(r),
				}
				required := RequiredPermissionForRequest(r)
				if !RoleAllows(sess.Role, required) {
					s.auditDenied(r, sess.Username, "forbidden_insufficient_role")
					s.denyForbidden(w, r, api, required)
					return
				}
				if s.standbyGuard(w, r) {
					return
				}
				r = r.WithContext(context.WithValue(r.Context(), userCtxKey, sess.Username))
				next(w, r, sess)
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
				s.auditDenied(r, sess.Username, "csrf_rejected")
				writeErr(w, http.StatusForbidden, "missing or invalid CSRF token")
				return
			}
		}
		required := RequiredPermissionForRequest(r)
		if !RoleAllows(sess.Role, required) {
			s.auditDenied(r, sess.Username, "forbidden_insufficient_role")
			s.denyForbidden(w, r, api, required)
			return
		}
		if s.standbyGuard(w, r) {
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), userCtxKey, sess.Username))
		next(w, r, sess)
	})
}

func (s *Server) denyForbidden(w http.ResponseWriter, r *http.Request, api bool, perm Permission) {
	if api {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "forbidden: requires " + string(perm) + " role"})
		return
	}
	RenderForbiddenHTML(w, perm)
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
	if s.settingBool("trust_forwarded_for") {
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

func (s *Server) auditDenied(r *http.Request, user, action string) {
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: user, Action: action, Result: "denied",
		Args: []string{r.Method, r.URL.Path}})
}

// ---- đăng nhập ----

func (s *Server) setCookie(w http.ResponseWriter, name, val string, maxAge int) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: val, Path: "/", HttpOnly: true, Secure: s.secureCookies(),
		SameSite: http.SameSiteStrictMode, MaxAge: maxAge})
}

func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil && s.sessions.Get(c.Value) != nil {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	s.renderLogin(w, r, http.StatusOK, "", "", false, "")
}

func (s *Server) renderLogin(w http.ResponseWriter, r *http.Request, status int, msg, username string, mustChange bool, currentPassword string) {
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
	_ = s.pages["login"].ExecuteTemplate(w, "login", map[string]any{
		"CSRF":            tok,
		"Error":           msg,
		"Next":            next,
		"Version":         s.version,
		"Username":        username,
		"MustChange":      mustChange,
		"CurrentPassword": currentPassword,
	})
}

// loginCookieMaxAge: thời hạn cookie CSRF của trang đăng nhập (giây). Để trang đăng nhập mở lâu vẫn dùng được.
const loginCookieMaxAge = 3600

// loginCSRFReason trả "" nếu request đăng nhập qua kiểm CSRF, ngược lại lý do (ghi audit).
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

// checkLogin kiểm CSRF của form đăng nhập (cookie ↔ trường/ header), giới hạn tốc độ (IP + user), và mật khẩu.
// Trả (status, thông báo, user, retryAfterSec); status 0 = thành công.
func (s *Server) checkLogin(r *http.Request, username, password string) (int, string, *store.User, int) {
	ip := s.clientIP(r)
	if reason := loginCSRFReason(r); reason != "" {
		_ = s.audit.Log(AuditEntry{IP: ip, User: username, Action: "login_csrf_rejected", Result: "denied",
			Args: []string{r.Method, r.URL.Path}, Detail: map[string]any{"reason": reason}})
		return http.StatusForbidden, "missing or invalid CSRF token; reload the page and try again", nil, 0
	}
	if ok, wait := s.limiter.Allow(ip); !ok {
		retrySec := int(wait.Seconds()) + 1
		_ = s.audit.Log(AuditEntry{IP: ip, User: username, Action: "login", Result: "denied", Detail: map[string]any{"reason": "rate_limited", "retry_after_s": retrySec}})
		return http.StatusTooManyRequests, fmt.Sprintf("too many failed attempts; try again in %d s", retrySec), nil, retrySec
	}

	ctx, cancel := dbCtx()
	defer cancel()
	now := s.now()

	u, ok, err := s.db.st.GetUser(ctx, username)
	if err != nil || !ok {
		// Timing mitigation: run dummy bcrypt compare so execution time is indistinguishable
		_ = bcrypt.CompareHashAndPassword(getDummyHash(), []byte(password))
		s.limiter.Fail(ip)
		_ = s.audit.Log(AuditEntry{IP: ip, User: username, Action: "login", Result: "denied", Detail: map[string]any{"reason": "bad_credentials"}})
		return http.StatusUnauthorized, "invalid username or password", nil, 0
	}

	// Check if account lockout has expired: reset failure count for new window
	if u.LockedUntil != nil {
		if u.LockedUntil.After(now) {
			retrySec := int(u.LockedUntil.Sub(now).Seconds()) + 1
			_ = bcrypt.CompareHashAndPassword(getDummyHash(), []byte(password))
			_ = s.audit.Log(AuditEntry{IP: ip, User: u.Username, Action: "login", Result: "denied", Detail: map[string]any{"reason": "account_locked", "retry_after_s": retrySec}})
			return http.StatusTooManyRequests, fmt.Sprintf("account is temporarily locked; try again in %d s", retrySec), nil, retrySec
		}
		u.FailedLoginCount = 0
	}

	// Check if account is disabled
	if u.Disabled {
		_ = bcrypt.CompareHashAndPassword(getDummyHash(), []byte(password))
		s.limiter.Fail(ip)
		_ = s.audit.Log(AuditEntry{IP: ip, User: u.Username, Action: "login", Result: "denied", Detail: map[string]any{"reason": "account_disabled"}})
		return http.StatusUnauthorized, "invalid username or password", nil, 0
	}

	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) != nil {
		s.limiter.Fail(ip)
		newCount := u.FailedLoginCount + 1
		var lockedUntil *time.Time
		if newCount >= MaxFailedLogins {
			strikes := (newCount - MaxFailedLogins) / MaxFailedLogins
			if strikes > 4 {
				strikes = 4
			}
			d := loginWindow << strikes
			if d > maxLockout || d <= 0 {
				d = maxLockout
			}
			t := now.Add(d)
			lockedUntil = &t
		}
		_ = s.db.st.RecordLoginFailure(ctx, u.ID, newCount, lockedUntil, now)
		_ = s.audit.Log(AuditEntry{IP: ip, User: u.Username, Action: "login", Result: "denied", Detail: map[string]any{"reason": "bad_password"}})
		return http.StatusUnauthorized, "invalid username or password", nil, 0
	}

	s.limiter.Success(ip)
	_ = s.db.st.RecordLoginSuccess(ctx, u.ID, now)
	return 0, "", &u, 0
}

func (s *Server) startSession(w http.ResponseWriter, r *http.Request, u *store.User) *Session {
	// Session rotation: clear old session if any
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.sessions.Delete(c.Value)
	}
	tok, sess := s.sessions.CreateUserSession(s.clientIP(r), u.ID, u.Username, u.Role)
	if sess == nil {
		return nil
	}
	s.setCookie(w, sessionCookie, tok, int(SessionMaxAge.Seconds()))
	s.setCookie(w, loginCookie, "", -1)
	_ = s.audit.Log(AuditEntry{IP: sess.IP, User: u.Username, Action: "login", Result: "ok"})
	return sess
}

func (s *Server) handleLoginForm(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		writeErr(w, http.StatusBadRequest, "bad form")
		return
	}
	username := strings.TrimSpace(r.PostForm.Get("username"))
	if username == "" {
		username = "admin"
	}
	if r.PostForm.Get("must_change") == "1" {
		currentPassword := r.PostForm.Get("current_password")
		newPassword := r.PostForm.Get("new_password")
		confirmPassword := r.PostForm.Get("confirm_password")

		status, msg, u, retrySec := s.checkLogin(r, username, currentPassword)
		if status != 0 {
			if retrySec > 0 {
				w.Header().Set("Retry-After", strconv.Itoa(retrySec))
			}
			s.renderLogin(w, r, status, msg, username, false, "")
			return
		}
		if newPassword != confirmPassword {
			s.renderLogin(w, r, http.StatusBadRequest, "New passwords do not match", username, true, currentPassword)
			return
		}
		if err := ValidatePassword(username, newPassword); err != nil {
			s.renderLogin(w, r, http.StatusBadRequest, err.Error(), username, true, currentPassword)
			return
		}
		h, err := HashPassword(newPassword)
		if err != nil {
			s.renderLogin(w, r, http.StatusInternalServerError, err.Error(), username, true, currentPassword)
			return
		}
		ctx, cancel := dbCtx()
		defer cancel()
		if err := s.db.st.SetUserPassword(ctx, u.ID, h, false); err != nil {
			s.renderLogin(w, r, http.StatusInternalServerError, "Failed to update password", username, true, currentPassword)
			return
		}
		_ = s.db.st.PurgeUserSessions(ctx, u.ID)
		if s.startSession(w, r, u) == nil {
			s.renderLogin(w, r, http.StatusServiceUnavailable, "session store unavailable; try again shortly", username, false, "")
			return
		}
		next := r.PostForm.Get("next")
		if !safeNext(next) {
			next = "/"
		}
		http.Redirect(w, r, next, http.StatusSeeOther)
		return
	}

	password := r.PostForm.Get("password")
	status, msg, u, retrySec := s.checkLogin(r, username, password)
	if status != 0 {
		if retrySec > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retrySec))
		}
		s.renderLogin(w, r, status, msg, username, false, "")
		return
	}
	if u.MustChangePassword {
		s.renderLogin(w, r, http.StatusOK, "", username, true, password)
		return
	}
	if s.startSession(w, r, u) == nil {
		s.renderLogin(w, r, http.StatusServiceUnavailable, "session store unavailable; try again shortly", username, false, "")
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
		Username    string `json:"username"`
		Password    string `json:"password"`
		NewPassword string `json:"new_password,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "expected JSON {\"username\": \"...\", \"password\": \"...\"}")
		return
	}
	username := strings.TrimSpace(body.Username)
	if username == "" {
		username = "admin"
	}
	status, msg, u, retrySec := s.checkLogin(r, username, body.Password)
	if status != 0 {
		if retrySec > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retrySec))
		}
		writeErr(w, status, msg)
		return
	}
	if u.MustChangePassword {
		if body.NewPassword == "" {
			writeJSON(w, http.StatusForbidden, map[string]any{
				"error":                "password change required on first login",
				"must_change_password": true,
			})
			return
		}
		if err := ValidatePassword(username, body.NewPassword); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		h, err := HashPassword(body.NewPassword)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		ctx, cancel := dbCtx()
		defer cancel()
		if err := s.db.st.SetUserPassword(ctx, u.ID, h, false); err != nil {
			writeErr(w, http.StatusInternalServerError, "failed to update password")
			return
		}
		_ = s.db.st.PurgeUserSessions(ctx, u.ID)
	}

	sess := s.startSession(w, r, u)
	if sess == nil {
		writeErr(w, http.StatusServiceUnavailable, "session store unavailable; try again shortly")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"csrf":           sess.CSRF,
		"username":       u.Username,
		"role":           string(u.Role),
		"idle_timeout_s": int(SessionIdleTimeout.Seconds()),
		"max_age_s":      int(SessionMaxAge.Seconds()),
	})
}

func (s *Server) doLogout(w http.ResponseWriter, r *http.Request) {
	username := ""
	if c, err := r.Cookie(sessionCookie); err == nil {
		if sess := s.sessions.Get(c.Value); sess != nil {
			username = sess.Username
		}
		s.sessions.Delete(c.Value)
	}
	s.setCookie(w, sessionCookie, "", -1)
	_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), User: username, Action: "logout", Result: "ok"})
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
	if j.Kind == "backup-schedule" && !j.Cancelled {
		go s.reconcileBackupSchedule(j.Pipeline)
	}
}
