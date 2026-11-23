package web

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"strand/internal/config"
)

func TestNewPipelineAuthAndCSRF(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	// 1. Unauthenticated -> 302 or 401
	anon := e.client()
	resp, _ := e.do(anon, "GET", "/pipelines/new", "", nil)
	if resp.StatusCode != http.StatusFound {
		t.Errorf("GET /pipelines/new unauthenticated: %d, want 302", resp.StatusCode)
	}

	resp, _ = e.do(anon, "POST", "/api/v1/pipelines", `{"name":"test-pipe"}`, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST /api/v1/pipelines unauthenticated: %d, want 401", resp.StatusCode)
	}

	// 2. Authenticated without CSRF -> 403
	resp, _ = e.do(c, "POST", "/api/v1/pipelines", `{"name":"test-pipe"}`, map[string]string{"Content-Type": "application/json"})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("POST /api/v1/pipelines without CSRF: %d, want 403", resp.StatusCode)
	}

	// 3. Authenticated with wrong CSRF -> 403
	resp, _ = e.do(c, "POST", "/api/v1/pipelines", `{"name":"test-pipe"}`, map[string]string{csrfHeader: csrf + "bad", "Content-Type": "application/json"})
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("POST /api/v1/pipelines with wrong CSRF: %d, want 403", resp.StatusCode)
	}
}

func TestNewPipelineCreation(t *testing.T) {
	customCfgDir := t.TempDir()
	e := opsEnv(t, false, func(c *Config) {
		c.ConfigDir = customCfgDir
	})
	c, csrf := e.loginOK()
	e.setExit(0)

	opts := NewPipelineOpts{
		Name:           "store-sync",
		From:           "mysql",
		To:             "postgres",
		SourceHost:     "10.0.0.11",
		SourcePort:     3306,
		TargetHost:     "10.0.0.21",
		TargetPort:     5432,
		SourceUser:     "strand_src",
		TargetUser:     "strand_tgt",
		SourcePassword: "SecretSrcPassword123",
		TargetPassword: "SecretTgtPassword456",
		Database:       "store",
		CommitOrder:    "relaxed",
	}
	body, _ := json.Marshal(opts)

	resp, out := e.do(c, "POST", "/api/v1/pipelines", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("create pipeline failed (%d): %s", resp.StatusCode, out)
	}

	var res CreatePipelineResult
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !res.OK || res.Pipeline != "store-sync" {
		t.Fatalf("unexpected result: %+v", res)
	}

	// 1. Kiểm tra file cấu hình YAML được tạo
	yamlPath := filepath.Join(customCfgDir, "store-sync.yaml")
	yamlBytes, err := os.ReadFile(yamlPath)
	if err != nil {
		t.Fatalf("config file not created at %s: %v", yamlPath, err)
	}
	yamlContent := string(yamlBytes)
	if !strings.Contains(yamlContent, "name: store-sync") {
		t.Errorf("yaml missing pipeline name: %s", yamlContent)
	}
	if !strings.Contains(yamlContent, "password_env: "+pipelineSourcePassword) {
		t.Errorf("yaml missing password_env: %s", yamlContent)
	}
	// Bí mật KHÔNG ĐƯỢC nằm trong YAML
	if strings.Contains(yamlContent, "SecretSrcPassword123") || strings.Contains(yamlContent, "SecretTgtPassword456") {
		t.Fatalf("passwords leaked into YAML config: %s", yamlContent)
	}

	envPath := filepath.Join(customCfgDir, "store-sync.env")
	assertNoEnvFile := func() {
		t.Helper()
		if _, err := os.Stat(envPath); !os.IsNotExist(err) {
			t.Fatalf("expected no plaintext credentials file: %v", err)
		}
	}
	assertNoEnvFile()
	row, found, err := e.srv.db.st.GetPipeline(t.Context(), opts.Name)
	if err != nil || !found || row.EnvPath != "" || res.EnvPath != "" || len(row.EnvEnc) == 0 ||
		strings.Contains(string(row.EnvEnc), opts.SourcePassword) || strings.Contains(string(row.EnvEnc), opts.TargetPassword) {
		t.Fatalf("expected database-only encrypted credentials: found=%v err=%v", found, err)
	}

	// 3. Pipeline đã được nạp vào Registry
	p := e.srv.Registry().Get("store-sync")
	if p == nil {
		t.Fatalf("store-sync pipeline not found in registry")
	}
	if p.CfgErr != "" || p.Cfg == nil {
		t.Fatalf("registry could not load pipeline credentials: %s", p.CfgErr)
	}
	env, err := p.Env()
	if err != nil || !slices.Contains(env, pipelineSourcePassword+"="+opts.SourcePassword) ||
		!slices.Contains(env, pipelineTargetDSN+"=postgres://strand_tgt:"+opts.TargetPassword+"@10.0.0.21:5432/store?sslmode=prefer") {
		t.Fatal("pipeline process did not receive stored source credentials")
	}
	resp, out = e.do(c, "GET", "/api/v1/pipelines/store-sync/env", "", nil)
	var view EnvView
	if resp.StatusCode != 200 || json.Unmarshal([]byte(out), &view) != nil || !view.Database ||
		view.Entries[pipelineSourcePassword] != maskStr || view.Entries[pipelineTargetDSN] != maskStr {
		t.Fatalf("credentials view returned status %d and exposed or omitted a credential", resp.StatusCode)
	}
	const replacement = `new-src:p@ss"$`
	edit, _ := json.Marshal(map[string]any{"entries": map[string]string{
		pipelineSourcePassword: replacement, pipelineTargetDSN: maskStr,
	}})
	resp, out = e.do(c, "POST", "/api/v1/pipelines/store-sync/env", string(edit),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("edit credentials: %d %s", resp.StatusCode, out)
	}
	e.srv.Registry().Refresh(t.Context())
	p = e.srv.Registry().Get(opts.Name)
	env, err = p.Env()
	if err != nil || !slices.Contains(env, pipelineSourcePassword+"="+replacement) ||
		!slices.Contains(env, pipelineTargetDSN+"=postgres://strand_tgt:"+opts.TargetPassword+"@10.0.0.21:5432/store?sslmode=prefer") {
		t.Fatal("credential edit did not replace the source password and preserve the target DSN")
	}
	assertNoEnvFile()
	if p.Mode() != ModeBackground {
		t.Errorf("new pipeline mode %s, want background", p.Mode())
	}

	// 4. Fake strand đã được gọi validate và plan
	calls := e.fakeCalls()
	var valCalled, planCalled bool
	for _, cl := range calls {
		if len(cl.Args) >= 3 && cl.Args[0] == "validate" && cl.Args[1] == "-c" && cl.Args[2] == yamlPath {
			valCalled = true
		}
		if len(cl.Args) >= 3 && cl.Args[0] == "plan" && cl.Args[1] == "-c" && cl.Args[2] == yamlPath {
			planCalled = true
		}
	}
	if !valCalled || !planCalled {
		t.Errorf("expected validate and plan to be called: val=%v plan=%v calls=%v", valCalled, planCalled, calls)
	}

	// 5. Nút Start (dùng action start sẵn có ở chế độ background)
	code, startOut := e.act(c, csrf, "store-sync", "start", nil)
	if code != 200 {
		t.Errorf("start store-sync: %d %v", code, startOut)
	}
	startCalls := e.fakeCalls()
	lastCall := startCalls[len(startCalls)-1]
	if len(lastCall.Args) < 4 || lastCall.Args[0] != "sync" || lastCall.Args[3] != "--background" {
		t.Errorf("start store-sync did not run sync --background: %v", lastCall.Args)
	}
}

func TestNewPipelineDuplicateRejected(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	// "orders" already exists
	body, _ := json.Marshal(NewPipelineOpts{
		Name: "orders",
		From: "mysql",
		To:   "postgres",
	})
	resp, out := e.do(c, "POST", "/api/v1/pipelines", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate pipeline name returned %d, want 409. Out: %s", resp.StatusCode, out)
	}
}

func TestNewPipelineOverwriteUnstarted(t *testing.T) {
	customCfgDir := t.TempDir()
	e := opsEnv(t, false, func(c *Config) {
		c.ConfigDir = customCfgDir
	})
	c, csrf := e.loginOK()
	e.setExit(0)

	opts := NewPipelineOpts{
		Name:           "retry-pipe",
		From:           "mysql",
		To:             "postgres",
		SourceHost:     "10.0.0.1",
		SourcePort:     3306,
		TargetHost:     "10.0.0.2",
		TargetPort:     5432,
		SourceUser:     "user1",
		SourcePassword: "pw1",
		TargetUser:     "user2",
		TargetPassword: "pw2",
		Database:       "app",
	}

	body, _ := json.Marshal(opts)
	resp, out := e.do(c, "POST", "/api/v1/pipelines", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first creation: %d %s", resp.StatusCode, out)
	}

	// Tạo lại mà không có overwrite -> 409 Conflict
	opts.SourceHost = "10.0.0.99"
	body, _ = json.Marshal(opts)
	resp, out = e.do(c, "POST", "/api/v1/pipelines", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 Conflict without overwrite, got %d %s", resp.StatusCode, out)
	}

	// Tạo lại kèm overwrite: true -> thành công 200 và cập nhật cấu hình mới
	opts.Overwrite = true
	body, _ = json.Marshal(opts)
	resp, out = e.do(c, "POST", "/api/v1/pipelines", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK with overwrite, got %d %s", resp.StatusCode, out)
	}

	// Kiểm tra file trên đĩa đã được cập nhật host mới
	yamlContent, err := os.ReadFile(filepath.Join(customCfgDir, "retry-pipe.yaml"))
	if err != nil {
		t.Fatalf("read yaml: %v", err)
	}
	if !strings.Contains(string(yamlContent), "10.0.0.99") {
		t.Fatalf("expected updated host 10.0.0.99 in yaml, got:\n%s", yamlContent)
	}
}

func TestNewPipelinePathTraversalBlocked(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	badNames := []string{
		"../../bad",
		"/etc/passwd",
		"foo/bar",
		"has space",
		"invalid!@#",
		"-starts-with-dash",
	}
	for _, bad := range badNames {
		body, _ := json.Marshal(NewPipelineOpts{
			Name: bad,
			From: "mysql",
			To:   "postgres",
		})
		resp, _ := e.do(c, "POST", "/api/v1/pipelines", string(body),
			map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("invalid name %q returned %d, want 400", bad, resp.StatusCode)
		}
	}
}

func TestNewPipelineConfigDirFallback(t *testing.T) {
	e := opsEnv(t, false, nil)
	cDir := e.srv.ConfigDir()
	if cDir == "" {
		t.Fatal("ConfigDir() returned empty")
	}
	// Since /etc/strand is typically not writable in unprivileged test, it falls back to state_dir/pipelines
	if cDir != "/etc/strand" && !strings.Contains(cDir, "pipelines") {
		t.Errorf("unexpected ConfigDir: %s", cDir)
	}
}

// createNP gửi POST /api/v1/pipelines (JSON) và trả mã HTTP, kết quả giải mã, nội dung YAML đã sinh.
func createNP(t *testing.T, e *testEnv, c *http.Client, csrf string, opts NewPipelineOpts) (int, CreatePipelineResult, string, string) {
	t.Helper()
	body, _ := json.Marshal(opts)
	resp, out := e.do(c, "POST", "/api/v1/pipelines", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	return decodeNP(t, e, resp.StatusCode, out)
}

func createNPForm(t *testing.T, e *testEnv, c *http.Client, csrf string, v url.Values) (int, CreatePipelineResult, string, string) {
	t.Helper()
	resp, out := e.do(c, "POST", "/api/v1/pipelines", v.Encode(),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/x-www-form-urlencoded"})
	return decodeNP(t, e, resp.StatusCode, out)
}

func decodeNP(t *testing.T, e *testEnv, code int, out string) (int, CreatePipelineResult, string, string) {
	t.Helper()
	var res CreatePipelineResult
	yamlText := ""
	if code == 200 {
		if err := json.Unmarshal([]byte(out), &res); err != nil {
			t.Fatalf("unmarshal: %v (%s)", err, out)
		}
		b, err := os.ReadFile(res.ConfigPath)
		if err != nil {
			t.Fatalf("read generated yaml: %v", err)
		}
		yamlText = string(b)
	}
	return code, res, yamlText, out
}

func npEnv(t *testing.T) (*testEnv, *http.Client, string) {
	t.Helper()
	dir := t.TempDir()
	e := opsEnv(t, false, func(c *Config) { c.ConfigDir = dir })
	c, csrf := e.loginOK()
	e.setExit(0)
	return e, c, csrf
}

// Form của trình duyệt phải đọc đủ mọi trường (trước đây source_port/target_port/workers/exclude bị bỏ qua).
func TestNewPipelineFormParsesAllFields(t *testing.T) {
	e, c, csrf := npEnv(t)
	code, _, y, out := createNPForm(t, e, c, csrf, url.Values{
		"name": {"f1"}, "from": {"mysql"}, "to": {"mysql"},
		"source_host": {"10.0.0.1"}, "source_port": {"33061"},
		"target_host": {"10.0.0.2"}, "target_port": {"33062"},
		"databases": {"shop, crm"}, "exclude": {"shop.log_*, crm.tmp"}, "workers": {"4"},
		"server_id": {"51000"}, "iops_soft": {"100"}, "metrics": {"127.0.0.1:19464"},
	})
	if code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	for _, want := range []string{"port: 33061", "port: 33062", "max: 4", `exclude: ["shop.log_*", "crm.tmp"]`,
		`include: ["shop.*", "crm.*"]`, "server_id: 51000", "soft: 100", `metrics: "127.0.0.1:19464"`} {
		if !strings.Contains(y, want) {
			t.Errorf("yaml thiếu %q:\n%s", want, y)
		}
	}
	if strings.Contains(y, "port: 3306\n") {
		t.Errorf("yaml không được chứa port mặc định 3306:\n%s", y)
	}
}

func TestNewPipelineFormInvalidIntegerRejected(t *testing.T) {
	e, c, csrf := npEnv(t)
	for _, f := range []string{"source_port", "target_port", "workers"} {
		code, _, _, out := createNPForm(t, e, c, csrf, url.Values{
			"name": {"bad-" + strings.ReplaceAll(f, "_", "-")}, "from": {"mysql"}, "to": {"mysql"}, f: {"abc"}})
		if code != 400 || !strings.Contains(out, f) {
			t.Errorf("%s=abc: want 400 nêu tên trường, got %d %s", f, code, out)
		}
	}
}

// JSON `databases` phải được dùng để sinh include (trước đây bị bỏ qua → app.*).
func TestNewPipelineJSONDatabases(t *testing.T) {
	e, c, csrf := npEnv(t)
	code, _, y, out := createNP(t, e, c, csrf, NewPipelineOpts{Name: "j1", From: "mysql", To: "mysql", Databases: []string{"shop", "crm"}})
	if code != 200 {
		t.Fatalf("%d %s", code, out)
	}
	if !strings.Contains(y, `include: ["shop.*", "crm.*"]`) || strings.Contains(y, "app.*") {
		t.Errorf("include sai:\n%s", y)
	}
	// `database` (số ít) vẫn hoạt động.
	code, _, y, out = createNP(t, e, c, csrf, NewPipelineOpts{Name: "j2", From: "mysql", To: "mysql", Database: "shop"})
	if code != 200 || !strings.Contains(y, `include: ["shop.*"]`) {
		t.Errorf("database số ít: %d %s\n%s", code, out, y)
	}
}

func TestNewPipelineKeyless(t *testing.T) {
	e, c, csrf := npEnv(t)
	resp, page := e.do(c, "GET", "/pipelines/new", "", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(page, `name="keyless"`) ||
		!strings.Contains(page, `value="full_row"`) || !strings.Contains(page, "REPLICA IDENTITY FULL") || !strings.Contains(page, "Start position of the restored copy") ||
		strings.Contains(page, `name="source_password_env"`) || strings.Contains(page, `name="target_password_env"`) {
		t.Fatalf("expected the keyless choice and prerequisites in the wizard, got %d", resp.StatusCode)
	}

	for _, tt := range []struct {
		name, engine, keyless    string
		status                   int
		useForm, wantFullRowYAML bool
	}{
		{name: "default", engine: "mysql", status: 200},
		{name: "explicit-off", engine: "mysql", keyless: "off", status: 200},
		{name: "full-row-json", engine: "mysql", keyless: "full_row", status: 200, wantFullRowYAML: true},
		{name: "full-row-form", engine: "postgres", keyless: "full_row", status: 200, useForm: true, wantFullRowYAML: true},
		{name: "unknown-mode", engine: "mysql", keyless: "anything", status: 400},
		{name: "mongo-rejects-full-row", engine: "mongodb", keyless: "full_row", status: 400},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var code int
			var y, body string
			if tt.useForm {
				code, _, y, body = createNPForm(t, e, c, csrf, url.Values{
					"name": {tt.name}, "from": {tt.engine}, "to": {tt.engine}, "database": {"shop"},
					"keyless": {tt.keyless}, "start_at": {"0/1ABCDEF"},
				})
			} else {
				code, _, y, body = createNP(t, e, c, csrf, NewPipelineOpts{
					Name: tt.name, From: tt.engine, To: tt.engine, Database: "shop", Keyless: tt.keyless,
				})
			}
			if code != tt.status {
				t.Fatalf("create: got %d, want %d: %s", code, tt.status, body)
			}
			if code == http.StatusBadRequest {
				var result struct {
					Fields map[string]string `json:"fields"`
				}
				if err := json.Unmarshal([]byte(body), &result); err != nil || result.Fields["keyless"] == "" {
					t.Fatalf("expected a keyless field error, got %s", body)
				}
			}
			if got := strings.Contains(y, "\n  keyless: full_row\n"); got != tt.wantFullRowYAML {
				t.Fatalf("keyless YAML present = %v, want %v:\n%s", got, tt.wantFullRowYAML, y)
			}
			if tt.useForm && !strings.Contains(y, "  start_at: \"0/1ABCDEF\"") {
				t.Fatalf("restored copy start position missing:\n%s", y)
			}
		})
	}
}

// target.database (nơi chứa _strand_*) phải tách khỏi database dữ liệu; mặc định "strand".
func TestNewPipelineTargetMetaDatabase(t *testing.T) {
	e, c, csrf := npEnv(t)
	_, _, y, _ := createNP(t, e, c, csrf, NewPipelineOpts{Name: "m1", From: "mysql", To: "mysql", Database: "shop"})
	if !strings.Contains(y, "  database: strand") || strings.Contains(y, "database: shop") {
		t.Errorf("target.database phải là strand:\n%s", y)
	}
	_, _, y, _ = createNP(t, e, c, csrf, NewPipelineOpts{Name: "m2", From: "mysql", To: "mysql", Database: "shop", TargetDatabase: "meta_db"})
	if !strings.Contains(y, "  database: meta_db") {
		t.Errorf("target_database không được áp dụng:\n%s", y)
	}
	code, _, _, out := createNP(t, e, c, csrf, NewPipelineOpts{Name: "m3", From: "mysql", To: "mysql", TargetDatabase: "a; DROP"})
	if code != 400 {
		t.Errorf("target_database lạ phải 400, got %d %s", code, out)
	}
}

// validate lỗi: giữ file nhưng ok=false và thông báo rõ.
func TestNewPipelineValidateFailureNotOK(t *testing.T) {
	e, c, csrf := npEnv(t)
	e.setExit(2)
	code, res, _, out := createNP(t, e, c, csrf, NewPipelineOpts{Name: "v1", From: "mysql", To: "mysql"})
	if code != 200 {
		t.Fatalf("%d %s", code, out)
	}
	if res.OK {
		t.Errorf("validate exit 2 nhưng ok=true: %+v", res)
	}
	if !strings.Contains(res.Message, "validation failed") || strings.Contains(res.Message, "created successfully") {
		t.Errorf("message chưa rõ: %q", res.Message)
	}
	if res.Validate.ExitCode != 2 {
		t.Errorf("validate.exit_code=%d", res.Validate.ExitCode)
	}
	if _, err := os.Stat(res.ConfigPath); err != nil {
		t.Errorf("file cấu hình phải được giữ lại: %v", err)
	}
	if e.srv.Registry().Get("v1") == nil {
		t.Errorf("pipeline vẫn phải được đăng ký")
	}
}

func TestDefaultAdminSocketFitsLimit(t *testing.T) {
	short := defaultAdminSocket("/var/lib/strand/p/spool", "p", "/var/lib/strand")
	if short != "/var/lib/strand/p/spool/admin.sock" {
		t.Errorf("spool ngắn phải giữ <spool>/admin.sock, got %s", short)
	}
	long := "/var/lib/strand/" + strings.Repeat("x", 120) + "/spool"
	got := defaultAdminSocket(long, "p", "/var/lib/strand")
	if len(got) > config.MaxUnixSocketPath() {
		t.Errorf("socket %q dài %d > giới hạn", got, len(got))
	}
	// Ngay cả khi state_dir cũng dài thì vẫn phải nằm trong giới hạn.
	got = defaultAdminSocket(long, "p", long)
	if len(got) > config.MaxUnixSocketPath() {
		t.Errorf("socket %q dài %d > giới hạn", got, len(got))
	}
}

func TestNewPipelineLongSpoolSocketFits(t *testing.T) {
	e, c, csrf := npEnv(t)
	long := filepath.Join(t.TempDir(), strings.Repeat("s", 110))
	code, res, y, out := createNP(t, e, c, csrf, NewPipelineOpts{Name: "s1", From: "mysql", To: "mysql", SpoolDir: long})
	if code != 200 {
		t.Fatalf("%d %s", code, out)
	}
	for _, l := range strings.Split(y, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(l), "admin_socket: "); ok && len(v) > config.MaxUnixSocketPath() {
			t.Errorf("admin_socket quá dài (%d): %s", len(v), v)
		}
	}
	_ = res
}

// Mỗi pipeline một cổng metrics riêng; cổng do người dùng đặt được tôn trọng và không được trùng.
func TestNewPipelineMetricsUniquePort(t *testing.T) {
	e, c, csrf := npEnv(t)
	_, _, y1, _ := createNP(t, e, c, csrf, NewPipelineOpts{Name: "p1", From: "mysql", To: "mysql"})
	_, _, y2, _ := createNP(t, e, c, csrf, NewPipelineOpts{Name: "p2", From: "mysql", To: "mysql"})
	m := func(y string) string {
		for _, l := range strings.Split(y, "\n") {
			if v, ok := strings.CutPrefix(strings.TrimSpace(l), "metrics: "); ok {
				return v
			}
		}
		return ""
	}
	if m(y1) == "" || m(y1) == m(y2) {
		t.Errorf("cổng metrics trùng: %q vs %q", m(y1), m(y2))
	}
	// Trường metrics được tôn trọng.
	_, _, y3, _ := createNP(t, e, c, csrf, NewPipelineOpts{Name: "p3", From: "mysql", To: "mysql", Metrics: "127.0.0.1:19999"})
	if m(y3) != `"127.0.0.1:19999"` {
		t.Errorf("metrics không được tôn trọng: %q", m(y3))
	}
	// Trùng cổng với pipeline khác → 409.
	code, _, _, out := createNP(t, e, c, csrf, NewPipelineOpts{Name: "p4", From: "mysql", To: "mysql", Metrics: "127.0.0.1:19999"})
	if code != 409 {
		t.Errorf("metrics trùng phải 409, got %d %s", code, out)
	}
}

// Trang tạo pipeline phải có ô nhập database metadata của target.
func TestNewPipelinePageHasTargetDatabaseInput(t *testing.T) {
	e, c, _ := npEnv(t)
	resp, out := e.do(c, "GET", "/pipelines/new", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(out, `name="target_database"`) {
		t.Errorf("thiếu input target_database (%d)", resp.StatusCode)
	}
}

// Target PG: database đích chọn riêng (target_pg_database), mặc định cùng tên database nguồn; cặp engine
// không hỗ trợ bị từ chối 400 ngay khi tạo.
func TestNewPipelineTargetPGDatabaseAndPairs(t *testing.T) {
	e, c, csrf := npEnv(t)
	for _, tc := range []struct {
		name, from, database, target, sourcePath, targetPath string
	}{
		{"p1", "mysql", "shop", "", "", "/shop?sslmode=prefer"},
		{"p2", "mysql", "shop", "warehouse", "", "/warehouse?sslmode=prefer"},
		{"p3", "pg", "src_db", "dst_db", "/src_db?sslmode=prefer", "/dst_db?sslmode=prefer"},
	} {
		code, _, _, out := createNP(t, e, c, csrf, NewPipelineOpts{
			Name: tc.name, From: tc.from, To: "postgres", Database: tc.database, TargetPGDatabase: tc.target,
		})
		p := e.srv.Registry().Get(tc.name)
		if code != 200 || p == nil || p.Cfg == nil {
			t.Fatalf("%s: create failed with status %d: %s", tc.name, code, out)
		}
		env, err := p.Env()
		if err != nil {
			t.Fatal(err)
		}
		values := make(map[string]string, len(env))
		for _, l := range env {
			k, v, _ := strings.Cut(l, "=")
			values[k] = v
		}
		cfg, err := config.LoadYAMLStrictWithEnv(p.dbYAML, values)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(cfg.Source.DSN, tc.sourcePath) || !strings.HasSuffix(cfg.Target.DSN, tc.targetPath) {
			t.Errorf("%s: configured source or target database does not match", tc.name)
		}
	}

	for _, o := range []NewPipelineOpts{
		{Name: "bad1", From: "postgres", To: "mysql", Database: "x"},
		{Name: "bad2", From: "mariadb", To: "postgres", Database: "x"},
		{Name: "bad3", From: "mysql", To: "postgres", Database: "x", TargetPGDatabase: "a b"},
		{Name: "bad4", From: "mysql", To: "postgres", Database: "*"},
	} {
		if code, _, _, out := createNP(t, e, c, csrf, o); code != 400 {
			t.Errorf("%s: want 400, got %d %s", o.Name, code, out)
		}
	}
}

// Trang form: ô PG/MySQL được đánh dấu theo cặp engine (JS ẩn/disable ô không liên quan), có ô database PG đích.
func TestNewPipelinePageConditionalFields(t *testing.T) {
	e, c, _ := npEnv(t)
	_, body := e.do(c, "GET", "/pipelines/new", "", nil)
	for _, want := range []string{`name="target_pg_database"`, `data-when="dst:postgres"`, `data-when="dst:mysql"`, `data-when="src:postgres"`, `data-when="src:mysql,mariadb"`, `id="np-preview"`, `data-from="mysql postgres"`} {
		if !strings.Contains(body, want) {
			t.Errorf("form missing %s", want)
		}
	}
}
