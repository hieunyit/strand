package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"strand/internal/admin"
	"strand/internal/web/store"
)

const testPassword = "correct horse battery"

// testSecretKey là STRAND_WEB_SECRET_KEY của các test (bắt buộc với web PostgreSQL-only).
const testSecretKey = "0123456789abcdef0123456789abcdef"

// fakeCmd là Commander giả: khớp theo chuỗi "name arg arg" (tiền tố).
type fakeCmd struct {
	mu    sync.Mutex
	out   map[string]string
	fail  map[string]int // tiền tố lệnh -> exit code (trả lỗi có ExitCode())
	calls []string
}

// fakeExit giả *exec.ExitError: có ExitCode().
type fakeExit struct{ code int }

func (e fakeExit) Error() string { return fmt.Sprintf("exit status %d", e.code) }
func (e fakeExit) ExitCode() int { return e.code }

// setFail / setOut change the fake's answers while background goroutines (sampler, reconcile) may be running it.
func (f *fakeCmd) setFail(m map[string]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = m
}

func (f *fakeCmd) setOut(prefix, out string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.out == nil {
		f.out = map[string]string{}
	}
	f.out[prefix] = out
}

// callsWith trả các lệnh đã chạy bắt đầu bằng prefix.
func (f *fakeCmd) callsWith(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeCmd) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	line := name + " " + strings.Join(args, " ")
	f.mu.Lock()
	f.calls = append(f.calls, line)
	fail, outs := make(map[string]int, len(f.fail)), make(map[string]string, len(f.out))
	for k, v := range f.fail {
		fail[k] = v
	}
	for k, v := range f.out {
		outs[k] = v
	}
	f.mu.Unlock()
	for prefix, code := range fail {
		if strings.HasPrefix(line, prefix) {
			return []byte("Failed: Access denied"), fakeExit{code}
		}
	}
	for prefix, out := range outs {
		if strings.HasPrefix(line, prefix) {
			return []byte(out), nil
		}
	}
	return []byte("Unit not found"), fmt.Errorf("exit status 1")
}

// mockPipeline cài admin socket giả với đủ lệnh cũ và mới.
type mockPipeline struct {
	events []admin.EventRecord
	tables []admin.TableInfo // nil: the three default tables
}

func (m *mockPipeline) Top(ctx context.Context) (*admin.TopInfo, error) {
	return &admin.TopInfo{PipelineID: "orders", State: "synced", LagSeconds: 0.25, Throughput: 1234, Zone: "green"}, nil
}
func (m *mockPipeline) Pause(ctx context.Context) (*admin.PauseResult, error) { return nil, nil }
func (m *mockPipeline) Resume(ctx context.Context, so bool) (*admin.ResumeResult, error) {
	return nil, nil
}
func (m *mockPipeline) ThrottleSet(ctx context.Context, r admin.Request) (*admin.ThrottleResult, error) {
	return nil, nil
}
func (m *mockPipeline) CheckpointShow(ctx context.Context) (*admin.CheckpointResult, error) {
	return &admin.CheckpointResult{PipelineID: "orders", Epoch: 4, Applied: "uuid-1234:1-99", UpdatedAt: time.Now()}, nil
}
func (m *mockPipeline) CheckpointRewind(ctx context.Context, to string) (*admin.CheckpointResult, error) {
	return nil, nil
}
func (m *mockPipeline) Status(ctx context.Context) (*admin.StatusInfo, error) {
	return &admin.StatusInfo{PipelineID: "orders", State: "synced", Epoch: 4, StartedAt: time.Now().Add(-time.Hour), UptimeSeconds: 3600,
		StartPosition: "uuid-1234:1-50", SourceKind: "mysql", TargetKind: "postgres", CommitOrder: "strict", Workers: 8,
		AppliedTxns: 777, AppliedRows: 5555, Governor: admin.GovernorInfo{Algorithm: "grace-v2", Zone: "Green", BackgroundRate: 321}}, nil
}
func (m *mockPipeline) Tables(ctx context.Context) ([]admin.TableInfo, error) {
	if m.tables != nil {
		return m.tables, nil
	}
	return []admin.TableInfo{
		{Table: "app.users", State: "done", ProgressPct: 100, PK: []string{"id"}},
		{Table: "app.orders", State: "running", Ranges: map[string]int{"Done": 2, "Pending": 6}, RowsEstimate: 1000000, RowsCopied: 250000, ProgressPct: 25, ETASeconds: 90, PK: []string{"id"}},
		{Table: "app.codes", State: "pending", GenericPK: true, PK: []string{"code"}},
	}, nil
}
func (m *mockPipeline) Events(ctx context.Context) ([]admin.EventRecord, error) {
	return m.events, nil
}

// testEnv gom mọi thứ cho một test HTTP.
type testEnv struct {
	t     *testing.T
	srv   *Server
	ts    *httptest.Server
	cmd   *fakeCmd
	clock *fakeClock
	dir   string
	sock  string
	mock  *recPipeline
	bin   string // strand giả (script sh); xem newFakeStrand
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

const pipelineYAML = `version: 1
name: orders
source:
  kind: mysql
  dsn: "mysql://strand:SuperSecretSrc@10.0.0.1:3306/app"
target:
  kind: postgres
  host: 10.0.0.2
  user: strand
  password: tgtSecretPw
  password_env: DST_PW
  database: app
tables:
  include: ["app.*"]
  mask:
    app.users.phone: sha256
resources:
  spool:
    path: %s
observability:
  admin_socket: %s
`

// newEnv dựng server: pipeline "orders" (admin socket giả đang chạy, chế độ nền) và tùy chọn thêm.
func newEnv(t *testing.T, mutate func(e *testEnv, c *Config)) *testEnv {
	t.Helper()
	return newEnvWith(t, mutate, nil)
}

// newEnvWith như newEnv; opt chỉnh Options của web.New (ví dụ gắn Store).
func newEnvWith(t *testing.T, mutate func(e *testEnv, c *Config), opt func(o *Options)) *testEnv {
	t.Helper()
	e := &testEnv{t: t, cmd: &fakeCmd{out: map[string]string{}}, clock: &fakeClock{t: time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)}}
	e.dir = t.TempDir()
	sockDir, err := os.MkdirTemp("", "sw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	e.sock = filepath.Join(sockDir, "a.sock")
	e.mock = &recPipeline{mockPipeline: &mockPipeline{events: []admin.EventRecord{
		{Time: time.Now(), Level: "WARN", Message: "snapshot slow", Attrs: map[string]string{"table": "app.orders"}},
		{Time: time.Now(), Level: "ERROR", Message: "dial mysql://u:LeakyPass@h/db failed"},
	}}}
	asrv := admin.NewServer(e.sock, e.mock, nil)
	if err := asrv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = asrv.Close() })

	spool := filepath.Join(e.dir, "spool")
	if err := os.MkdirAll(spool, 0o755); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(e.dir, "orders.yaml")
	if err := os.WriteFile(cfgPath, []byte(fmt.Sprintf(pipelineYAML, spool, e.sock)), 0o640); err != nil {
		t.Fatal(err)
	}
	// pid file của pipeline nền: trỏ vào tiến trình test này không phải "strand ... sync" => coi như dừng.
	hash, err := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	e.bin = newFakeStrand(t, e.dir)
	cfg := &Config{
		StrandBinary: e.bin,
		Listen:       "127.0.0.1:0", PasswordHash: string(hash), StateDir: filepath.Join(e.dir, "state"),
		Pipelines: []PipelineEntry{{Name: "orders", Config: cfgPath}},
	}
	if mutate != nil {
		mutate(e, cfg)
	}
	// Tests do not run Serve, so nothing renews the lease: with the 30 s default a test that runs longer (common
	// under -race) loses it and every write gets 503 "standby". Lease tests set their own TTL through opt.
	o := Options{Config: cfg, Version: "test", Commander: e.cmd, Now: e.clock.Now, LeaseTTL: testLeaseTTL}
	if opt != nil {
		opt(&o)
	}
	// Web chỉ chạy với kho metadata: test mặc định dùng kho Mem (admin nhập từ password_hash của Config
	// ở lần khởi động đầu, như bản cài cũ chuyển sang DB) và một khóa bí mật cố định.
	if o.Store == nil && !cfg.DBEnabled() {
		o.Store = store.NewMem()
	}
	if o.SecretKey == "" && os.Getenv(EnvSecretKey) == "" {
		o.SecretKey = testSecretKey
	}
	srv, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	e.srv = srv
	t.Cleanup(func() {
		_ = e.srv.Close()
		e.hold(false)
		if jr, ok := e.srv.jobs.(*FileJobRunner); ok {
			for _, j := range jr.List("") {
				if j.Running {
					_ = jr.Cancel(j.ID)
				}
			}
			deadline := time.Now().Add(200 * time.Millisecond)
			for time.Now().Before(deadline) {
				allStopped := true
				for _, j := range jr.List("") {
					if j.Running {
						allStopped = false
						break
					}
				}
				if allStopped {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			for _, j := range jr.List("") {
				if j.Running && j.PID > 0 {
					_ = syscall.Kill(-j.PID, syscall.SIGKILL)
					_ = syscall.Kill(j.PID, syscall.SIGKILL)
				}
			}
		}
	})
	e.ts = httptest.NewServer(srv.Handler())
	t.Cleanup(e.ts.Close)
	return e
}

// client trả http.Client có cookie jar, không tự theo redirect.
func (e *testEnv) client() *http.Client {
	jar := &simpleJar{m: map[string]*http.Cookie{}}
	return &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type simpleJar struct {
	mu sync.Mutex
	m  map[string]*http.Cookie
}

func (j *simpleJar) SetCookies(u *url.URL, cs []*http.Cookie) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, c := range cs {
		if c.MaxAge < 0 || c.Value == "" {
			delete(j.m, c.Name)
			continue
		}
		j.m[c.Name] = c
	}
}
func (j *simpleJar) Cookies(u *url.URL) []*http.Cookie {
	j.mu.Lock()
	defer j.mu.Unlock()
	var out []*http.Cookie
	for _, c := range j.m {
		out = append(out, c)
	}
	return out
}

func (e *testEnv) do(c *http.Client, method, path string, body string, hdr map[string]string) (*http.Response, string) {
	e.t.Helper()
	req, err := http.NewRequest(method, e.ts.URL+path, strings.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

// login đăng nhập bằng API JSON (lấy cookie đăng nhập từ GET /login) và trả CSRF token của session.
func (e *testEnv) login(c *http.Client, password string) (*http.Response, string) {
	e.t.Helper()
	e.do(c, "GET", "/login", "", nil)
	var lc string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}
	resp, body := e.do(c, "POST", "/api/v1/login", `{"password":"`+password+`"}`, map[string]string{csrfHeader: lc, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		return resp, ""
	}
	var raw map[string]any
	_ = json.Unmarshal([]byte(body), &raw)
	tok, _ := raw["csrf"].(string)
	return resp, tok
}

func (e *testEnv) sampleNow() { e.srv.Sampler().SampleOnce(context.Background()) }

func (e *testEnv) auditLines() []map[string]any {
	e.t.Helper()
	b, err := os.ReadFile(e.srv.Audit().Path())
	if err != nil {
		e.t.Fatal(err)
	}
	var out []map[string]any
	for _, ln := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if ln == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			e.t.Fatalf("audit line is not JSON: %q", ln)
		}
		out = append(out, m)
	}
	return out
}

// writePipelineFile ghi file cấu hình/.env của pipeline rồi lưu vào DB (như thao tác "Adopt disk version"):
// DB là nguồn sự thật, sửa file trên đĩa mà không đưa vào DB thì lần vật chất hóa sau đặt nó sang bên.
func (e *testEnv) writePipelineFile(name, path, data string, perm os.FileMode) {
	e.t.Helper()
	if err := os.WriteFile(path, []byte(data), perm); err != nil {
		e.t.Fatal(err)
	}
	e.srv.reg.Refresh(context.Background())
	p := e.srv.reg.Get(name)
	if p == nil {
		e.t.Fatalf("pipeline %s not registered", name)
	}
	if strings.HasSuffix(path, ".env") {
		p.EnvFile = path
	}
	if err := e.srv.persistPipelineFiles(p); err != nil {
		e.t.Fatal(err)
	}
	e.srv.reg.Refresh(context.Background())
}

// testRegistry: Registry trên kho Mem riêng (danh sách pipeline luôn nằm trong kho metadata).
func testRegistry(c *Config, cmd Commander) *Registry {
	r := NewRegistry(c, cmd)
	db, err := newMetaDB(store.NewMem(), testSecretKey, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		panic(err)
	}
	r.db = db
	return r
}

// testLeaseTTL is the lease of test servers (see newEnvWith).
const testLeaseTTL = time.Hour
