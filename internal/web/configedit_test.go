package web

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigEditAuthAndCSRF(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	// 1. Unauthenticated -> 401
	anon := e.client()
	routes := []struct {
		method, path, body string
	}{
		{"POST", "/api/v1/pipelines/orders/config/validate", `{"yaml": "name: orders"}`},
		{"POST", "/api/v1/pipelines/orders/config", `{"yaml": "name: orders"}`},
		{"GET", "/api/v1/pipelines/orders/env", ""},
		{"POST", "/api/v1/pipelines/orders/env", `{"entries": {"A": "B"}}`},
	}
	for _, r := range routes {
		resp, _ := e.do(anon, r.method, r.path, r.body, nil)
		if resp.StatusCode != 401 {
			t.Errorf("%s %s unauthenticated: %d, want 401", r.method, r.path, resp.StatusCode)
		}
	}

	// 2. Authenticated without CSRF -> 403 for POST
	for _, r := range routes {
		if r.method == "POST" {
			resp, _ := e.do(c, r.method, r.path, r.body, map[string]string{"Content-Type": "application/json"})
			if resp.StatusCode != 403 {
				t.Errorf("%s %s without CSRF: %d, want 403", r.method, r.path, resp.StatusCode)
			}
			resp, _ = e.do(c, r.method, r.path, r.body, map[string]string{csrfHeader: csrf + "bad", "Content-Type": "application/json"})
			if resp.StatusCode != 403 {
				t.Errorf("%s %s with wrong CSRF: %d, want 403", r.method, r.path, resp.StatusCode)
			}
		}
	}

	// 3. Actions registered
	for _, act := range []string{"config-validate", "config-save", "env-save"} {
		if _, ok := e.srv.Actions().Get(act); !ok {
			t.Errorf("action %s not registered in ActionRegistry", act)
		}
	}
}

func TestConfigValidateAndDiff(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	cfgPath := filepath.Join(e.dir, "orders.yaml")
	origBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	modified := string(origBytes) + "\n# added test comment line\n"
	body, _ := json.Marshal(map[string]any{"yaml": modified})

	resp, out := e.do(c, "POST", "/api/v1/pipelines/orders/config/validate", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("config validate: %d %s", resp.StatusCode, out)
	}

	var res ConfigValidateResult
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("unmarshal validate result: %v", err)
	}
	if !res.Valid || res.ExitCode != 0 {
		t.Errorf("expected valid=true exit=0, got valid=%v exit=%d output=%s", res.Valid, res.ExitCode, res.Output)
	}
	if !strings.Contains(res.Diff, "+# added test comment line") {
		t.Errorf("diff does not contain added line: %s", res.Diff)
	}
}

func TestConfigSaveValidationFailureDoesNotOverwrite(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	cfgPath := filepath.Join(e.dir, "orders.yaml")
	origBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	// Fake strand exits with code 3 on validate failure
	e.setExit(3)

	modified := string(origBytes) + "\n# bad change that fails validation\n"
	body, _ := json.Marshal(map[string]any{"yaml": modified})

	resp, out := e.do(c, "POST", "/api/v1/pipelines/orders/config", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("save invalid config: got status %d, want 502. Body: %s", resp.StatusCode, out)
	}

	// Config file must NOT be overwritten
	afterBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(afterBytes) != string(origBytes) {
		t.Errorf("file was overwritten despite validation failure!")
	}

	// Audit must have recorded error
	edits := e.auditFor("config.edit")
	if len(edits) == 0 {
		t.Fatalf("no audit log for config.edit")
	}
	last := edits[len(edits)-1]
	if last["result"] != "error" {
		t.Errorf("audit result %v, want error", last["result"])
	}
}

func TestConfigSaveAtomicAndBackup(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	cfgPath := filepath.Join(e.dir, "orders.yaml")
	origBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	e.setExit(0)
	modified := string(origBytes) + "\n# safe valid change\n"
	body, _ := json.Marshal(map[string]any{"yaml": modified, "restart": true})

	resp, out := e.do(c, "POST", "/api/v1/pipelines/orders/config", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("save config: %d %s", resp.StatusCode, out)
	}

	var res ConfigSaveResult
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !res.OK {
		t.Errorf("expected ok=true, got %v", res)
	}

	// File was updated
	afterBytes, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(afterBytes), "# safe valid change") {
		t.Errorf("config file was not updated with new content")
	}

	// Backup file was created
	if res.Backup == "" {
		t.Fatalf("no backup path returned in result")
	}
	bakBytes, err := os.ReadFile(res.Backup)
	if err != nil {
		t.Fatalf("read backup file %s: %v", res.Backup, err)
	}
	if string(bakBytes) != string(origBytes) {
		t.Errorf("backup file content does not match original content")
	}

	// Audit was recorded
	edits := e.auditFor("config.edit")
	if len(edits) == 0 {
		t.Fatalf("no audit log for config.edit")
	}
	last := edits[len(edits)-1]
	if last["result"] != "ok" {
		t.Errorf("audit result %v, want ok", last["result"])
	}
}

func TestConfigSecretsNeverLeakedAndWriteOnly(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	// 1. Check YAML masking via API
	resp, out := e.do(c, "GET", "/api/v1/pipelines/orders/config", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("get config: %d %s", resp.StatusCode, out)
	}
	if strings.Contains(out, "SuperSecretSrc") || strings.Contains(out, "tgtSecretPw") {
		t.Fatalf("passwords leaked in config view: %s", out)
	}
	if !strings.Contains(out, "********") {
		t.Errorf("secrets not masked with ******** in config view: %s", out)
	}

	// 2. Check .env editing and masking
	envPath := filepath.Join(e.dir, "orders.env")
	if err := os.WriteFile(envPath, []byte("export SRC_PW=\"original-secret\"\nexport OTHER_FLAG=\"enabled\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	resp, out = e.do(c, "GET", "/api/v1/pipelines/orders/env", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("get env: %d %s", resp.StatusCode, out)
	}
	if strings.Contains(out, "original-secret") {
		t.Fatalf("original secret leaked in env view: %s", out)
	}
	var ev EnvView
	_ = json.Unmarshal([]byte(out), &ev)
	if ev.Entries["SRC_PW"] != "********" {
		t.Errorf("SRC_PW value not masked: %v", ev.Entries["SRC_PW"])
	}

	// 3. Save .env keeping '********' -> original secret retained
	saveReq, _ := json.Marshal(map[string]any{
		"entries": map[string]string{
			"SRC_PW":     "********",
			"OTHER_FLAG": "updated",
		},
	})
	resp, out = e.do(c, "POST", "/api/v1/pipelines/orders/env", string(saveReq),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("save env: %d %s", resp.StatusCode, out)
	}

	envBytes, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(envBytes), "original-secret") {
		t.Errorf("original secret was lost when saving '********': %s", string(envBytes))
	}
	if !strings.Contains(string(envBytes), "updated") {
		t.Errorf("OTHER_FLAG was not updated: %s", string(envBytes))
	}

	// Check file permissions of .env (strictly 0600)
	fi, err := os.Stat(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf(".env permissions %o, want 0600", fi.Mode().Perm())
	}

	// 4. Update secret to a new value
	saveReq2, _ := json.Marshal(map[string]any{
		"entries": map[string]string{
			"SRC_PW": "new-secret-value",
		},
	})
	resp, _ = e.do(c, "POST", "/api/v1/pipelines/orders/env", string(saveReq2),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("save env 2: %d", resp.StatusCode)
	}
	envBytes2, _ := os.ReadFile(envPath)
	if !strings.Contains(string(envBytes2), "new-secret-value") {
		t.Errorf("new secret was not written to .env: %s", string(envBytes2))
	}
}

func TestConfigPathTraversalBlocked(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	badNames := []string{
		"..%2fetc",
		"%2e%2e",
		"orders%00",
		"x%20y",
		"$(id)",
	}
	for _, bad := range badNames {
		resp, _ := e.do(c, "POST", "/api/v1/pipelines/"+bad+"/config", `{"yaml":"x"}`,
			map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
		if resp.StatusCode != 400 && resp.StatusCode != 404 {
			t.Errorf("path traversal name %q returned %d, want 400 or 404", bad, resp.StatusCode)
		}
	}
}

const fmtYAML = `# giữ nguyên chú thích
version: 1
name: orders

source: { kind: mysql, dsn: "mysql://strand:SuperSecretSrc@10.0.0.1:3306/app" }   # flow-style


target:
  kind: postgres
  host: 10.0.0.2
  password: tgtSecretPw   # bí mật
  password_env: DST_PW
  database: app
tables:
  include: ["app.*"]
observability: { metrics: "127.0.0.1:9464" }
`

func writeFmtYAML(t *testing.T, e *testEnv) string {
	t.Helper()
	cfgPath := filepath.Join(e.dir, "orders.yaml")
	if err := os.WriteFile(cfgPath, []byte(fmtYAML), 0o640); err != nil {
		t.Fatal(err)
	}
	return cfgPath
}

func getCfgYAML(t *testing.T, e *testEnv, c *http.Client) string {
	t.Helper()
	resp, out := e.do(c, "GET", "/api/v1/pipelines/orders/config", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("get config: %d %s", resp.StatusCode, out)
	}
	var v struct {
		YAML string `json:"yaml"`
	}
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatal(err)
	}
	return v.YAML
}

func saveCfg(e *testEnv, c *http.Client, csrf, yamlText string) (int, string) {
	body, _ := json.Marshal(map[string]any{"yaml": yamlText})
	resp, out := e.do(c, "POST", "/api/v1/pipelines/orders/config", string(body),
		map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	return resp.StatusCode, out
}

// GET config trả đúng văn bản thô (dòng trống, flow-style, chú thích), chỉ che giá trị bí mật.
func TestConfigViewKeepsRawFormatting(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, _ := e.loginOK()
	writeFmtYAML(t, e)
	got := getCfgYAML(t, e, c)
	for _, leak := range []string{"SuperSecretSrc", "tgtSecretPw"} {
		if strings.Contains(got, leak) {
			t.Fatalf("leak %q:\n%s", leak, got)
		}
	}
	want := strings.NewReplacer("SuperSecretSrc", "********", "tgtSecretPw", `"********"`).Replace(fmtYAML)
	if got != want {
		t.Errorf("văn bản không được định dạng lại.\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// Lưu lại văn bản đã che (kèm sửa nhỏ) phải giữ bí mật gốc và định dạng gốc; diff chỉ chứa dòng đã sửa.
func TestConfigSaveMaskedTextKeepsSecretsAndFormat(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	cfgPath := writeFmtYAML(t, e)
	e.setExit(0)
	edited := strings.Replace(getCfgYAML(t, e, c), "host: 10.0.0.2", "host: 10.0.0.9", 1)
	if code, out := saveCfg(e, c, csrf, edited); code != 200 {
		t.Fatalf("save: %d %s", code, out)
	}
	after, _ := os.ReadFile(cfgPath)
	want := strings.Replace(fmtYAML, "host: 10.0.0.2", "host: 10.0.0.9", 1)
	if string(after) != want {
		t.Errorf("file sau khi lưu khác kỳ vọng.\n--- got ---\n%s\n--- want ---\n%s", after, want)
	}
	if strings.Contains(string(after), "********") {
		t.Errorf("'********' bị ghi vào file")
	}
}

// Không khôi phục được '********' (dòng bí mật không có trong file gốc) => từ chối, file không đổi.
func TestConfigSaveRejectsUnrestorableMask(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	cfgPath := writeFmtYAML(t, e)
	e.setExit(0)
	edited := strings.Replace(getCfgYAML(t, e, c), "  database: app\n", "  database: app\n  token: ********\n", 1)
	code, out := saveCfg(e, c, csrf, edited)
	if code != 400 || !strings.Contains(out, "********") {
		t.Errorf("want 400, got %d %s", code, out)
	}
	after, _ := os.ReadFile(cfgPath)
	if string(after) != fmtYAML {
		t.Errorf("file bị đổi dù bị từ chối")
	}
}
