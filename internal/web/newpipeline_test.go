package web

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	if !strings.Contains(yamlContent, "password_env: SRC_PW") {
		t.Errorf("yaml missing password_env: %s", yamlContent)
	}
	// Bí mật KHÔNG ĐƯỢC nằm trong YAML
	if strings.Contains(yamlContent, "SecretSrcPassword123") || strings.Contains(yamlContent, "SecretTgtPassword456") {
		t.Fatalf("passwords leaked into YAML config: %s", yamlContent)
	}

	// 2. Kiểm tra file .env với mode 0600
	envPath := filepath.Join(customCfgDir, "store-sync.env")
	envBytes, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatalf("env file not created at %s: %v", envPath, err)
	}
	fi, err := os.Stat(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("env file permissions %o, want 0600", fi.Mode().Perm())
	}
	envContent := string(envBytes)
	if !strings.Contains(envContent, "SecretSrcPassword123") || !strings.Contains(envContent, "SecretTgtPassword456") {
		t.Errorf("env file missing passwords: %s", envContent)
	}

	// 3. Pipeline đã được nạp vào Registry
	p := e.srv.Registry().Get("store-sync")
	if p == nil {
		t.Fatalf("store-sync pipeline not found in registry")
	}
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
