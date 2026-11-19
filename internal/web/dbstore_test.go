package web

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/testutil"
	"strand/internal/web/store"
)

func TestMain(m *testing.M) {
	code := m.Run()
	testutil.TerminatePostgres()
	os.Exit(code)
}

// storeKinds chạy f với kho Mem (luôn) và PostgreSQL thật (testcontainers): cùng kịch bản, hai cài đặt.
func storeKinds(t *testing.T, f func(t *testing.T, mk func() store.Store)) {
	t.Run("mem", func(t *testing.T) {
		f(t, func() store.Store { return store.NewMem() })
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := testutil.NewPostgresDSN(t)
		f(t, func() store.Store {
			p, err := store.Open(context.Background(), dsn, store.OpenOptions{MaxWait: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			return p
		})
	})
}

// dbEnv dựng server chế độ DB: admin nằm trong kho (không có password_hash trong Config).
func dbEnv(t *testing.T, st store.Store, mutate func(e *testEnv, c *Config), opt func(o *Options)) *testEnv {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := st.Admin(context.Background()); !ok {
		if err := st.SetPasswordHash(context.Background(), string(h)); err != nil {
			t.Fatal(err)
		}
	}
	return newEnvWith(t, func(e *testEnv, c *Config) {
		c.PasswordHash = ""
		if mutate != nil {
			mutate(e, c)
		}
	}, func(o *Options) {
		o.Store = st
		if opt != nil {
			opt(o)
		}
	})
}

// restart dựng server mới từ cùng cấu hình + cùng kho (mô phỏng khởi động lại tiến trình web).
func (e *testEnv) restart() *testEnv {
	e.t.Helper()
	c2 := *e.srv.cfg
	srv, err := New(Options{Config: &c2, Version: "test", Commander: e.cmd, Now: e.clock.Now, Store: e.srv.db.st, SecretKey: e.srv.db.secret})
	if err != nil {
		e.t.Fatal(err)
	}
	e2 := *e
	e2.srv = srv
	e2.ts = httptest.NewServer(srv.Handler())
	e.t.Cleanup(e2.ts.Close)
	return &e2
}

func TestDBSessionSurvivesRestart(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		st := mk()
		e := dbEnv(t, st, nil, nil)
		c, csrf := e.loginOK()
		if resp, _ := e.do(c, "GET", "/api/v1/session", "", nil); resp.StatusCode != 200 {
			t.Fatalf("session before restart: %d", resp.StatusCode)
		}
		e2 := e.restart()
		resp, body := e2.do(c, "GET", "/api/v1/session", "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("session after web restart: %d %s (user was logged out)", resp.StatusCode, body)
		}
		// CSRF của session cũ vẫn hợp lệ trên server mới.
		if code, _ := e2.act(c, csrf, "orders", "stop", nil); code == http.StatusForbidden || code == http.StatusUnauthorized {
			t.Fatalf("write after restart rejected: %d", code)
		}
		// Chỉ sha256 của cookie nằm trong DB, không có giá trị cookie.
		var cookie string
		for _, ck := range c.Jar.Cookies(nil) {
			if ck.Name == sessionCookie {
				cookie = ck.Value
			}
		}
		ctx := context.Background()
		if _, ok, _ := st.GetSession(ctx, cookie); ok {
			t.Fatal("raw session id is stored in the database")
		}
		if _, ok, _ := st.GetSession(ctx, hashKey(cookie)); !ok {
			t.Fatal("hashed session id missing from the database")
		}
		// Logout xóa phiên trong DB; idle timeout vẫn áp dụng.
		if resp, _ := e2.do(c, "POST", "/api/v1/logout", "", map[string]string{csrfHeader: csrf}); resp.StatusCode != 200 {
			t.Fatalf("logout: %d", resp.StatusCode)
		}
		if n, _ := st.CountSessions(ctx); n != 0 {
			t.Fatalf("%d sessions left after logout", n)
		}
	})
}

func TestDBSessionIdleAndMaxAgeRules(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	c, _ := e.loginOK()
	e.clock.Advance(SessionIdleTimeout + time.Minute)
	e2 := e.restart()
	if resp, _ := e2.do(c, "GET", "/api/v1/session", "", nil); resp.StatusCode != 401 {
		t.Fatalf("idle session accepted after restart: %d", resp.StatusCode)
	}
	// Dùng đều đặn thì gia hạn idle nhưng không vượt max age.
	c, _ = e.loginOK()
	for i := 0; i < 40; i++ {
		e.clock.Advance(20 * time.Minute)
		if resp, _ := e.do(c, "GET", "/api/v1/session", "", nil); resp.StatusCode != 200 && e.clock.Now().Sub(time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)) < SessionMaxAge {
			t.Fatalf("active session dropped early at step %d: %d", i, resp.StatusCode)
		}
	}
	if resp, _ := e.do(c, "GET", "/api/v1/session", "", nil); resp.StatusCode != 401 {
		t.Fatalf("session older than max age still valid: %d", resp.StatusCode)
	}
}

// Contrast: chế độ file (hành vi cũ) mất phiên khi web khởi động lại.
func TestFileModeSessionDoesNotSurviveRestart(t *testing.T) {
	e := newEnv(t, nil)
	c, _ := e.loginOK()
	c2 := *e.srv.cfg
	srv, err := New(Options{Config: &c2, Version: "test", Commander: e.cmd, Now: e.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()
	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/session", nil)
	for _, ck := range c.Jar.Cookies(nil) {
		req.AddCookie(ck)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("file mode unexpectedly kept the session: %d", resp.StatusCode)
	}
}

func TestDesiredStateRestartsPipelines(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		st := mk()
		e := dbEnv(t, st, nil, nil)
		c, csrf := e.loginOK()
		if code, body := e.act(c, csrf, "orders", "start", nil); code != 200 {
			t.Fatalf("start: %d %v", code, body)
		}
		d, _ := st.Desired(context.Background())
		if d["orders"] != store.DesiredRunning {
			t.Fatalf("desired after start = %v", d)
		}
		syncCalls := func(en *testEnv) int {
			n := 0
			for _, cl := range en.fakeCalls() {
				if len(cl.Args) > 0 && cl.Args[0] == "sync" {
					n++
				}
			}
			return n
		}
		if syncCalls(e) != 1 {
			t.Fatalf("calls after start: %v", e.fakeCalls())
		}
		// Web/container khởi động lại: tiến trình pipeline không còn (fake strand không để pid file) -> chạy lại.
		e2 := e.restart()
		got := e2.srv.ResumeDesired(context.Background())
		if len(got) != 1 || got[0] != "orders" || syncCalls(e2) != 2 {
			t.Fatalf("ResumeDesired = %v, sync calls = %d, calls=%v", got, syncCalls(e2), e2.fakeCalls())
		}
		if a := e2.auditFor("autostart"); len(a) != 2 || a[1]["result"] != "ok" {
			t.Errorf("autostart audit: %v", a)
		}
		// Dừng có chủ đích: lần khởi động sau KHÔNG chạy lại.
		c2, csrf2 := e2.loginOK()
		if code, _ := e2.act(c2, csrf2, "orders", "stop", nil); code != 200 {
			t.Fatalf("stop: %d", code)
		}
		d, _ = st.Desired(context.Background())
		if d["orders"] != store.DesiredStopped {
			t.Fatalf("desired after stop = %v", d)
		}
		e3 := e2.restart()
		before := syncCalls(e3)
		if got := e3.srv.ResumeDesired(context.Background()); len(got) != 0 || syncCalls(e3) != before {
			t.Fatalf("stopped pipeline was restarted: %v", got)
		}
	})
}

func TestResumeDesiredFileModeIsNoop(t *testing.T) {
	e := newEnv(t, nil)
	if got := e.srv.ResumeDesired(context.Background()); got != nil {
		t.Fatalf("file mode autostart: %v", got)
	}
}

func TestInitAndTokenWriteDatabase(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		st := mk()
		dir := t.TempDir()
		cfgPath := filepath.Join(dir, "web.yaml")
		var so, se strings.Builder
		res, err := RunInit(InitOptions{ConfigPath: cfgPath, Listen: "127.0.0.1:8080", StateDir: filepath.Join(dir, "state"),
			Scan: []string{"/nonexistent/*.yaml"}, Password: "first-pass-1", NewToken: true, Store: st, IfNoAdmin: true, Quiet: true,
			Stdout: &so, Stderr: &se})
		if err != nil {
			t.Fatal(err)
		}
		a, ok, _ := st.Admin(context.Background())
		if !ok || bcrypt.CompareHashAndPassword([]byte(a.PasswordHash), []byte("first-pass-1")) != nil {
			t.Fatalf("password hash not in the database: %+v", a)
		}
		if a.APITokenHash != HashAPIToken(res.Token) || res.Token == "" {
			t.Fatalf("token hash not in the database")
		}
		b, _ := os.ReadFile(cfgPath)
		if strings.Contains(string(b), "\npassword_hash:") || strings.Contains(string(b), "\napi_token_hash:") {
			t.Fatalf("hashes leaked into web.yaml in DB mode:\n%s", b)
		}
		// Chạy lại với --if-no-admin: giữ admin cũ, không in mật khẩu sinh ra.
		so.Reset()
		if _, err := RunInit(InitOptions{ConfigPath: cfgPath, Listen: "127.0.0.1:8080", StateDir: filepath.Join(dir, "state"),
			Scan: []string{"/nonexistent/*.yaml"}, Password: "generated-pw-2", PrintPassword: true, NewToken: true, Store: st, IfNoAdmin: true, Quiet: true,
			Stdout: &so, Stderr: &se}); err != nil {
			t.Fatal(err)
		}
		a2, _, _ := st.Admin(context.Background())
		if a2 != a {
			t.Fatal("second init --if-no-admin changed the admin account")
		}
		if strings.Contains(so.String(), "generated-pw-2") {
			t.Fatalf("ignored generated password was printed: %s", so.String())
		}
		// Server dùng hash trong DB: đăng nhập được, token Bearer dùng được.
		cfg := &Config{StrandBinary: "x", Listen: "127.0.0.1:0", StateDir: filepath.Join(dir, "state")}
		srv, err := New(Options{Config: cfg, Store: st, Version: "t"})
		if err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()
		req, _ := http.NewRequest("GET", ts.URL+"/api/v1/pipelines", nil)
		req.Header.Set("Authorization", "Bearer "+res.Token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("bearer token from DB: %v %v", resp, err)
		}
		resp.Body.Close()
		// `passwd` sau đó (SetPasswordHash) có hiệu lực với server mới.
		h, _ := HashPassword("changed-pass-3")
		_ = st.SetPasswordHash(context.Background(), h)
		srv2, err := New(Options{Config: &Config{StrandBinary: "x", Listen: "127.0.0.1:0", StateDir: filepath.Join(dir, "state")}, Store: st, Version: "t"})
		if err != nil {
			t.Fatal(err)
		}
		if bcrypt.CompareHashAndPassword([]byte(srv2.passwordHash()), []byte("changed-pass-3")) != nil {
			t.Fatal("server does not use the password hash from the database")
		}
	})
}

func TestDBModeRequiresAdmin(t *testing.T) {
	_, err := New(Options{Config: &Config{StrandBinary: "x", Listen: "127.0.0.1:0", StateDir: t.TempDir()}, Store: store.NewMem()})
	if err == nil || !strings.Contains(err.Error(), "no admin password") {
		t.Fatalf("err = %v, want 'no admin password'", err)
	}
}

func TestImportFilesIntoDatabaseOnce(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		st := mk()
		dir := t.TempDir()
		state := filepath.Join(dir, "state")
		must := func(err error) {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
		}
		cfgDir := filepath.Join(dir, "cfg")
		must(os.MkdirAll(cfgDir, 0o755))
		must(os.WriteFile(filepath.Join(cfgDir, "legacy.yaml"), []byte("version: 1\nname: legacy\n"), 0o640))
		must(os.WriteFile(filepath.Join(cfgDir, "legacy.env"), []byte("SRC_PW=hunter2\n"), 0o600))
		must(os.MkdirAll(filepath.Join(state, "jobs"), 0o750))
		must(os.MkdirAll(filepath.Join(state, "wizards"), 0o750))
		must(os.WriteFile(filepath.Join(state, CreatedFile),
			[]byte(`{"version":1,"pipelines":[{"name":"legacy","config":"`+filepath.Join(cfgDir, "legacy.yaml")+`","env_file":"`+filepath.Join(cfgDir, "legacy.env")+`"}]}`), 0o600))
		jid := "20261101T100000Z-deadbeef"
		must(os.WriteFile(filepath.Join(state, "jobs", jid+".json"),
			[]byte(`{"id":"`+jid+`","pipeline":"legacy","kind":"verify","args":["verify"],"started":"2026-11-01T10:00:00Z","running":false,"exit_code":0}`), 0o600))
		must(os.WriteFile(filepath.Join(state, "wizards", "repair-legacy.json"),
			[]byte(`{"kind":"repair","pipeline":"legacy","step":"done"}`), 0o600))
		must(os.WriteFile(filepath.Join(state, "audit.log"),
			[]byte(`{"time":"2026-11-01T09:00:00Z","ip":"1.1.1.1","action":"login","result":"ok"}`+"\n"+
				`{"time":"2026-11-01T09:05:00Z","action":"start","pipeline":"legacy","result":"ok"}`+"\n"), 0o600))
		hash, _ := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
		newCfg := func() *Config {
			return &Config{StrandBinary: "x", Listen: "127.0.0.1:0", StateDir: state, PasswordHash: string(hash), APITokenHash: HashAPIToken("tok")}
		}
		var logs strings.Builder
		srv, err := New(Options{Config: newCfg(), Store: st, SecretKey: "0123456789abcdef0123456789abcdef", Logger: slogTo(&logs)})
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		a, ok, _ := st.Admin(ctx)
		if !ok || a.PasswordHash != string(hash) || a.APITokenHash != HashAPIToken("tok") {
			t.Fatalf("web.yaml hashes not imported: %+v", a)
		}
		p, ok, _ := st.GetPipeline(ctx, "legacy")
		if !ok || !p.HasConfig || !strings.Contains(p.ConfigYAML, "name: legacy") || len(p.EnvEnc) == 0 || strings.Contains(string(p.EnvEnc), "hunter2") {
			t.Fatalf("pipelines.json not imported (env must be sealed): %+v", p)
		}
		if jobs, _ := st.ListJobs(ctx); len(jobs) != 1 || jobs[0].ID != jid {
			t.Fatalf("jobs = %+v", jobs)
		}
		if j, ok := srv.jobs.Get(jid); !ok || j.Pipeline != "legacy" {
			t.Fatalf("imported job not served by the runner: %v", j)
		}
		if _, ok, _ := st.GetWizard(ctx, "repair", "legacy"); !ok {
			t.Fatal("wizard not imported")
		}
		rows, _ := st.ListAudit(ctx, 100)
		imported := 0
		for _, r := range rows {
			if r.Time.Year() == 2026 && r.Time.Month() == 11 && r.Time.Day() == 1 {
				imported++
			}
		}
		if imported != 2 {
			t.Fatalf("imported %d audit lines, want 2 (rows: %d)", imported, len(rows))
		}
		for _, want := range []string{"imported existing file state", "pipelines=1", "jobs=1", "wizards=1", "audit_lines=2"} {
			if !strings.Contains(logs.String(), want) {
				t.Errorf("log lacks %q:\n%s", want, logs.String())
			}
		}
		// Lần khởi động thứ hai: không nhập lại, không nhân đôi.
		n1 := len(rows)
		logs.Reset()
		if _, err := New(Options{Config: newCfg(), Store: st, SecretKey: "0123456789abcdef0123456789abcdef", Logger: slogTo(&logs)}); err != nil {
			t.Fatal(err)
		}
		rows2, _ := st.ListAudit(ctx, 100)
		if len(rows2) != n1 {
			t.Fatalf("audit rows %d -> %d after second start: import was not idempotent", n1, len(rows2))
		}
		if strings.Contains(logs.String(), "imported existing file state") {
			t.Errorf("second start imported again:\n%s", logs.String())
		}
	})
}

func TestCreatedPipelineSourceOfTruthIsDatabase(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		st := mk()
		cfgDir := t.TempDir()
		e := dbEnv(t, st, func(e *testEnv, c *Config) { c.ConfigDir = cfgDir; c.Scan = nil },
			func(o *Options) { o.SecretKey = "0123456789abcdef0123456789abcdef" })
		c, csrf := e.loginOK()
		e.setExit(0)
		if code, _, _, out := createNP(t, e, c, csrf, newOpts("dbpipe", "SrcSecretPw", "DstSecretPw")); code != 200 {
			t.Fatalf("create: %d %s", code, out)
		}
		ctx := context.Background()
		row, ok, _ := st.GetPipeline(ctx, "dbpipe")
		if !ok || !row.HasConfig || !strings.Contains(row.ConfigYAML, "name: dbpipe") {
			t.Fatalf("pipeline not in the database: %+v", row)
		}
		if len(row.EnvEnc) == 0 || strings.Contains(string(row.EnvEnc), "SrcSecretPw") {
			t.Fatalf("env must be stored sealed, got %q", row.EnvEnc)
		}
		if _, err := os.Stat(filepath.Join(state(e), CreatedFile)); err == nil {
			t.Fatal("pipelines.json written in DB mode")
		}
		// Mất file (volume cấu hình mới): web khởi động lại dựng lại từ DB với đúng quyền.
		yamlPath, envPath := filepath.Join(cfgDir, "dbpipe.yaml"), filepath.Join(cfgDir, "dbpipe.env")
		wantYAML, _ := os.ReadFile(yamlPath)
		must0(t, os.Remove(yamlPath))
		must0(t, os.Remove(envPath))
		e2 := e.restart()
		got, err := os.ReadFile(yamlPath)
		if err != nil || string(got) != string(wantYAML) {
			t.Fatalf("config not materialised from DB: %v", err)
		}
		if fi, _ := os.Stat(yamlPath); fi.Mode().Perm() != 0o640 {
			t.Errorf("config mode %o, want 0640", fi.Mode().Perm())
		}
		envB, err := os.ReadFile(envPath)
		if err != nil || !strings.Contains(string(envB), "SrcSecretPw") {
			t.Fatalf("env not materialised: %v %q", err, envB)
		}
		if fi, _ := os.Stat(envPath); fi.Mode().Perm() != 0o600 {
			t.Errorf("env mode %o, want 0600", fi.Mode().Perm())
		}
		if e2.srv.reg.Get("dbpipe") == nil {
			t.Fatal("pipeline missing after restart")
		}
		// Sửa file bằng tay rồi chạy validate/start: DB thắng (nguồn sự thật).
		must0(t, os.WriteFile(yamlPath, []byte("# hand edit\n"), 0o640))
		e2.srv.materialize("dbpipe")
		if b, _ := os.ReadFile(yamlPath); string(b) != string(wantYAML) {
			t.Fatalf("hand-edited config was not restored from the database: %q", b)
		}
		// Sửa qua API ghi vào DB.
		c2, csrf2 := e2.loginOK()
		newYAML := string(wantYAML) + "\n# edited via api\n"
		body, _ := json.Marshal(map[string]any{"yaml": newYAML})
		resp, out := e2.do(c2, "POST", "/api/v1/pipelines/dbpipe/config", string(body), map[string]string{csrfHeader: csrf2, "Content-Type": "application/json"})
		if resp.StatusCode != 200 {
			t.Fatalf("config save: %d %s", resp.StatusCode, out)
		}
		row, _, _ = st.GetPipeline(ctx, "dbpipe")
		if !strings.Contains(row.ConfigYAML, "# edited via api") {
			t.Fatalf("config edit not stored in the database")
		}
	})
}

func TestEnvStaysOnDiskWithoutSecretKey(t *testing.T) {
	st := store.NewMem()
	cfgDir := t.TempDir()
	t.Setenv(EnvSecretKey, "")
	e := dbEnv(t, st, func(e *testEnv, c *Config) { c.ConfigDir = cfgDir; c.Scan = nil }, nil)
	c, csrf := e.loginOK()
	e.setExit(0)
	if code, _, _, out := createNP(t, e, c, csrf, newOpts("nokey", "SrcSecretPw", "DstSecretPw")); code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	row, ok, _ := st.GetPipeline(context.Background(), "nokey")
	if !ok || !row.HasConfig {
		t.Fatalf("pipeline row missing: %+v", row)
	}
	if len(row.EnvEnc) != 0 {
		t.Fatal("env stored in the database although STRAND_WEB_SECRET_KEY is not set")
	}
	if b, err := os.ReadFile(filepath.Join(cfgDir, "nokey.env")); err != nil || !strings.Contains(string(b), "SrcSecretPw") {
		t.Fatalf("env must stay on disk: %v", err)
	}
	// Mật khẩu không bao giờ nằm trong bất kỳ giá trị nào của kho (config yaml, audit).
	rows, _ := st.ListAudit(context.Background(), 100)
	for _, r := range rows {
		if strings.Contains(string(r.Data), "SrcSecretPw") {
			t.Fatalf("secret in audit row: %s", r.Data)
		}
	}
}

func TestAuditSinkModes(t *testing.T) {
	for _, mode := range []string{AuditDB, AuditFile, AuditBoth} {
		t.Run(mode, func(t *testing.T) {
			st := store.NewMem()
			e := dbEnv(t, st, func(e *testEnv, c *Config) { c.Database.Audit = mode }, nil)
			e.loginOK()
			rows, _ := st.ListAudit(context.Background(), 10)
			fileData, _ := os.ReadFile(e.srv.Audit().Path())
			inDB, inFile := len(rows) > 0 && rows[0].Action == "login", strings.Contains(string(fileData), `"login"`)
			wantDB, wantFile := mode != AuditFile, mode != AuditDB
			if inDB != wantDB || inFile != wantFile {
				t.Fatalf("mode %s: in DB %v (want %v), in file %v (want %v)", mode, inDB, wantDB, inFile, wantFile)
			}
		})
	}
}

func TestAuditFailsClosedWhenDatabaseDown(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()
	st.SetDown(true)
	defer st.SetDown(false)
	if err := e.srv.Audit().Log(AuditEntry{Action: "x"}); err == nil {
		t.Fatal("audit write succeeded with the database down")
	}
	_ = c
	_ = csrf
}

func TestHealthz(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	resp, body := e.do(e.client(), "GET", "/healthz", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `"ok": true`) {
		t.Fatalf("healthz: %d %s", resp.StatusCode, body)
	}
	st.SetDown(true)
	defer st.SetDown(false)
	resp, body = e.do(e.client(), "GET", "/healthz", "", nil)
	if resp.StatusCode != 503 || !strings.Contains(body, `"ok": false`) {
		t.Fatalf("healthz with DB down: %d %s", resp.StatusCode, body)
	}
	// File mode: 200, không có DB.
	f := newEnv(t, nil)
	resp, body = f.do(f.client(), "GET", "/healthz", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `"file"`) {
		t.Fatalf("healthz file mode: %d %s", resp.StatusCode, body)
	}
}

func TestWizardAndJobsInDatabase(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()
	e.setExit(0)
	if code, body := e.act(c, csrf, "orders", "verify", map[string]string{"mode": "full"}); code != 200 {
		t.Fatalf("verify: %d %v", code, body)
	}
	id, _ := (func() (string, bool) {
		for _, j := range e.srv.jobs.List("orders") {
			return j.ID, true
		}
		return "", false
	})()
	waitFor(t, "job row in the database", func() bool {
		rows, _ := st.ListJobs(context.Background())
		for _, r := range rows {
			if r.ID == id && !r.Running {
				return true
			}
		}
		return false
	})
	if _, err := os.Stat(filepath.Join(state(e), "jobs", id+".json")); err == nil {
		t.Fatal("job .json written in DB mode")
	}
	// Web khởi động lại: lịch sử job đọc từ DB.
	e2 := e.restart()
	if j, ok := e2.srv.jobs.Get(id); !ok || j.Kind != "verify" {
		t.Fatalf("job history lost across restart: %+v %v", j, ok)
	}
}

func slogTo(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func state(e *testEnv) string { return e.srv.cfg.StateDir }

func TestConfigViewAndSaveFromDBWhenDiskFileMissing(t *testing.T) {
	st := store.NewMem()
	cfgDir := t.TempDir()
	t.Setenv(EnvSecretKey, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	e := dbEnv(t, st, func(e *testEnv, c *Config) { c.ConfigDir = cfgDir; c.Scan = nil }, nil)
	c, csrf := e.loginOK()
	e.setExit(0)

	// Tạo pipeline qua wizard (được lưu vào DB và disk)
	if code, _, _, out := createNP(t, e, c, csrf, newOpts("dbcfg", "SrcSecretPw", "DstSecretPw")); code != 200 {
		t.Fatalf("create pipeline failed: %d %s", code, out)
	}

	p := e.srv.reg.Get("dbcfg")
	if p == nil {
		t.Fatal("pipeline dbcfg not found")
	}

	// Xóa file config trên đĩa để giả lập trường hợp file chưa kịp ghi hoặc chạy trên node mới
	_ = os.Remove(p.ConfigPath)

	// configView phải khôi phục từ DB và không trả về lỗi
	cv := e.srv.configView(p)
	if cv.Error != "" {
		t.Fatalf("configView returned error: %s", cv.Error)
	}
	if !strings.Contains(cv.YAML, "name: dbcfg") {
		t.Fatalf("configView YAML missing expected content: %s", cv.YAML)
	}

	// Sửa config qua API với masked password:
	editedYAML := cv.YAML + "\n# test comment added\n"
	body, _ := json.Marshal(map[string]any{"yaml": editedYAML, "force": true})
	resp, out := e.do(c, "POST", "/api/v1/pipelines/dbcfg/config", string(body), map[string]string{
		csrfHeader: csrf, "Content-Type": "application/json",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("save config: status %d, body: %s", resp.StatusCode, out)
	}

	// Kiểm tra file trên đĩa đã được tạo lại và DB đã cập nhật
	fileBytes, err := os.ReadFile(p.ConfigPath)
	if err != nil {
		t.Fatalf("file not recreated: %v", err)
	}
	if !strings.Contains(string(fileBytes), "# test comment added") {
		t.Fatalf("file content does not contain comment: %s", fileBytes)
	}

	ctx := context.Background()
	row, ok, err := st.GetPipeline(ctx, "dbcfg")
	if err != nil || !ok {
		t.Fatalf("pipeline not found in store: %v", err)
	}
	if !strings.Contains(row.ConfigYAML, "# test comment added") {
		t.Fatalf("store does not contain updated config YAML: %s", row.ConfigYAML)
	}
}

func TestPersistPipelineFilesCreatesRowForWebYamlPipeline(t *testing.T) {
	st := store.NewMem()
	cfgDir := t.TempDir()
	yamlFile := filepath.Join(cfgDir, "orders.yaml")
	must0(t, os.WriteFile(yamlFile, []byte("name: orders\n"), 0o640))

	e := dbEnv(t, st, func(e *testEnv, c *Config) {
		c.Pipelines = []PipelineEntry{{Name: "orders", Config: yamlFile}}
		c.Scan = nil
	}, nil)

	p := e.srv.reg.Get("orders")
	if p == nil {
		t.Fatal("pipeline orders not found in registry")
	}

	// Chế độ DB đăng ký pipeline khai báo trong web.yaml ngay khi quét (registry trong DB); xóa dòng để kiểm
	// persistPipelineFiles tự tạo lại dòng khi chưa có.
	ctx := context.Background()
	must0(t, st.DeletePipeline(ctx, "orders"))

	// Gọi persistPipelineFiles (như khi người dùng lưu config hoặc tạo)
	if err := e.srv.persistPipelineFiles(p); err != nil {
		t.Fatalf("persistPipelineFiles failed: %v", err)
	}

	// Giờ đây pipeline đã được ghi vào DB
	row, ok, err := st.GetPipeline(ctx, "orders")
	if err != nil || !ok {
		t.Fatalf("orders not persisted to store: %v", err)
	}
	if row.ConfigYAML != "name: orders\n" {
		t.Fatalf("unexpected ConfigYAML in store: %q", row.ConfigYAML)
	}
}

func TestTuningAndEnvViewSaveWhenDiskFileMissing(t *testing.T) {
	st := store.NewMem()
	cfgDir := t.TempDir()
	t.Setenv(EnvSecretKey, "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	e := dbEnv(t, st, func(e *testEnv, c *Config) { c.ConfigDir = cfgDir; c.Scan = nil }, nil)
	c, csrf := e.loginOK()
	e.setExit(0)

	// Tạo pipeline qua wizard
	if code, _, _, out := createNP(t, e, c, csrf, newOpts("tuneenv", "MySrcSecret", "MyDstSecret")); code != 200 {
		t.Fatalf("create pipeline failed: %d %s", code, out)
	}

	p := e.srv.reg.Get("tuneenv")
	if p == nil {
		t.Fatal("pipeline tuneenv not found")
	}

	// Xóa cả file config và file .env trên đĩa để mô phỏng môi trường DB-first
	_ = os.Remove(p.ConfigPath)
	_ = os.Remove(p.EnvFile)

	// 1. tuningView phải đọc được từ DB
	tv := e.srv.tuningView(p)
	if tv.Error != "" {
		t.Fatalf("tuningView returned error: %s", tv.Error)
	}

	// 2. getEnvView phải đọc được từ DB
	ev := e.srv.getEnvView(p)
	if ev.Error != "" || !ev.Exists {
		t.Fatalf("getEnvView failed: exists=%v, err=%s", ev.Exists, ev.Error)
	}
	if ev.Entries["SRC_PW"] != maskStr {
		t.Fatalf("SRC_PW should be masked, got: %s", ev.Entries["SRC_PW"])
	}

	// 3. saveTuning với force: true thành công và lưu vào DB
	tBody, _ := json.Marshal(map[string]any{"chunk_rows": "8888", "force": true})
	resp, out := e.do(c, "POST", "/api/v1/pipelines/tuneenv/tuning", string(tBody), map[string]string{
		csrfHeader: csrf, "Content-Type": "application/json",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("save tuning: %d %s", resp.StatusCode, out)
	}

	// 4. saveEnv gửi lại masked password không được ghi '********' vào file
	eBody, _ := json.Marshal(map[string]any{
		"entries": map[string]string{
			"SRC_PW":  maskStr, // giữ nguyên mật khẩu cũ từ DB
			"NEW_KEY": "newval",
		},
	})
	resp, out = e.do(c, "POST", "/api/v1/pipelines/tuneenv/env", string(eBody), map[string]string{
		csrfHeader: csrf, "Content-Type": "application/json",
	})
	if resp.StatusCode != 200 {
		t.Fatalf("save env: %d %s", resp.StatusCode, out)
	}

	// Đọc lại file .env vừa materialize và kiểm tra
	envBytes, err := os.ReadFile(p.EnvFile)
	if err != nil {
		t.Fatalf("read env file: %v", err)
	}
	if strings.Contains(string(envBytes), "********") {
		t.Fatalf(".env file contains masked stars '********'")
	}
	if !strings.Contains(string(envBytes), "MySrcSecret") {
		t.Fatalf(".env file lost original secret, content:\n%s", envBytes)
	}
	if !strings.Contains(string(envBytes), "NEW_KEY=newval") {
		t.Fatalf(".env file missing NEW_KEY, content:\n%s", envBytes)
	}
}

func must0(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
