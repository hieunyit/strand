package web

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/web/store"
)

// doctorReportJSON: what `strand doctor --json` prints (findings.Report); one message carries a DSN password
// the web layer must mask even though doctor already redacts registered secrets.
const doctorReportJSON = `{"pipeline":"orders","ok":false,"summary":{"errors":1,"warnings":1,"info":0},"findings":[
{"severity":"error","code":"source_binlog_format","object":"source","message":"binlog_format is STATEMENT on mysql://u:LeakyPw@h/db","fix":"SET GLOBAL binlog_format = ROW"},
{"severity":"warning","code":"target_table_not_empty","object":"app.orders","message":"target table app.orders has rows","fix":""}]}`

// doctorScript writes a fake strand binary: argv goes to calls.log, log lines to stderr, body to stdout,
// then it exits with code.
func doctorScript(t *testing.T, dir, body string, code int, sleep string) string {
	t.Helper()
	bin := filepath.Join(dir, "fakedoctor")
	if err := os.WriteFile(filepath.Join(dir, "doctor-out.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nd=$(dirname \"$0\")\n{ echo CALL; for a in \"$@\"; do echo \"$a\"; done; echo \"ENV=$STRAND_TEST_VAR\"; } >> \"$d/calls.log\"\n" +
		"echo 'time=x level=INFO msg=\"doctor start\"' >&2\n"
	if sleep != "" {
		script += "sleep " + sleep + "\n"
	}
	script += "cat \"$d/doctor-out.json\"\nexit " + itoa(code) + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func doctorEnv(t *testing.T, body string, code int, sleep string) *testEnv {
	t.Helper()
	return newEnv(t, func(e *testEnv, c *Config) {
		envf := filepath.Join(e.dir, "orders.env")
		if err := os.WriteFile(envf, []byte("STRAND_TEST_VAR=from-env-file\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		c.Pipelines[0].EnvFile = envf
		e.bin = doctorScript(t, e.dir, body, code, sleep)
		c.StrandBinary = e.bin
	})
}

func postDoctor(t *testing.T, e *testEnv, c *http.Client, csrf, name string) (*http.Response, string) {
	t.Helper()
	return e.do(c, "POST", "/api/v1/pipelines/"+name+"/doctor", "", map[string]string{csrfHeader: csrf})
}

func TestDoctorAPIListsFindings(t *testing.T) {
	e := doctorEnv(t, doctorReportJSON, 3, "")
	c, csrf := e.loginOK()
	resp, out := postDoctor(t, e, c, csrf, "orders")
	if resp.StatusCode != 200 {
		t.Fatalf("doctor: %d %s", resp.StatusCode, out)
	}
	if strings.Contains(out, "LeakyPw") {
		t.Fatalf("doctor result leaks a password: %s", out)
	}
	var res DoctorResult
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("decode: %v: %s", err, out)
	}
	if res.OK || res.ExitCode != 3 || res.Summary.Errors != 1 || res.Summary.Warnings != 1 || len(res.Findings) != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	if f := res.Findings[0]; f.Code != "source_binlog_format" || f.Fix != "SET GLOBAL binlog_format = ROW" || !strings.Contains(f.Message, "binlog_format is STATEMENT") {
		t.Fatalf("first finding: %+v", f)
	}
	calls := e.fakeCalls()
	if len(calls) != 1 {
		t.Fatalf("want one strand call, got %d", len(calls))
	}
	if got := strings.Join(calls[0].Args, " "); got != "doctor -c "+filepath.Join(e.dir, "orders.yaml")+" --json" {
		t.Fatalf("argv = %q", got)
	}
	if calls[0].Env != "from-env-file" {
		t.Fatalf("doctor did not get the pipeline env file: %q", calls[0].Env)
	}
	a := e.auditFor("pipeline.doctor")
	if len(a) != 1 || a[0]["result"] != "ok" || a[0]["pipeline"] != "orders" {
		t.Fatalf("audit: %v", a)
	}
	if d, _ := a[0]["detail"].(map[string]any); d == nil || d["errors"] != float64(1) || d["warnings"] != float64(1) {
		t.Fatalf("audit detail: %v", a[0]["detail"])
	}
}

func TestDoctorAPICleanReport(t *testing.T) {
	e := doctorEnv(t, `{"pipeline":"orders","ok":true,"summary":{"errors":0,"warnings":0,"info":0},"findings":[]}`, 0, "")
	c, csrf := e.loginOK()
	resp, out := postDoctor(t, e, c, csrf, "orders")
	var res DoctorResult
	if resp.StatusCode != 200 || json.Unmarshal([]byte(out), &res) != nil || !res.OK || res.ExitCode != 0 || res.Findings == nil || len(res.Findings) != 0 {
		t.Fatalf("clean doctor: %d %s", resp.StatusCode, out)
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal([]byte(out), &raw) != nil || string(raw["findings"]) != "[]" {
		t.Fatalf("findings must be an empty list, not null: %s", out)
	}
}

// Output that is not a report (old binary without doctor, crash) is an error with the masked output, not an
// empty "no problems" result.
func TestDoctorAPIBadOutput(t *testing.T) {
	e := doctorEnv(t, "Error: unknown command \"doctor\" for \"strand\" mysql://u:LeakyPw@h/db\n", 1, "")
	c, csrf := e.loginOK()
	resp, out := postDoctor(t, e, c, csrf, "orders")
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(out, "unknown command") || !strings.Contains(out, "did not return a report") {
		t.Fatalf("bad output: want 502 with the output, got %d %s", resp.StatusCode, out)
	}
	if strings.Contains(out, "LeakyPw") {
		t.Fatalf("error leaks a password: %s", out)
	}
	if a := e.auditFor("pipeline.doctor"); len(a) != 1 || a[0]["result"] != "error" {
		t.Fatalf("audit: %v", a)
	}
}

func TestDoctorAPITimeout(t *testing.T) {
	old := doctorTimeout
	doctorTimeout = 300 * time.Millisecond
	t.Cleanup(func() { doctorTimeout = old })
	e := doctorEnv(t, doctorReportJSON, 0, "30")
	c, csrf := e.loginOK()
	start := time.Now()
	resp, out := postDoctor(t, e, c, csrf, "orders")
	if el := time.Since(start); el > 10*time.Second {
		t.Fatalf("doctor request took %s after the deadline", el)
	}
	var res DoctorResult
	if resp.StatusCode != 200 || json.Unmarshal([]byte(out), &res) != nil {
		t.Fatalf("timeout: %d %s", resp.StatusCode, out)
	}
	if !res.TimedOut || res.OK || res.Summary.Errors != 1 || len(res.Findings) != 1 || res.Findings[0].Code != "doctor_timeout" {
		t.Fatalf("timeout result: %+v", res)
	}
}

func TestDoctorAPIUnknownPipelineAndRBAC(t *testing.T) {
	e := doctorEnv(t, doctorReportJSON, 3, "")
	users := setupRBACUsers(t, e)
	if resp, out := postDoctor(t, e, users[store.RoleAdmin].client, users[store.RoleAdmin].csrf, "nope"); resp.StatusCode != 404 {
		t.Fatalf("unknown pipeline: %d %s", resp.StatusCode, out)
	}
	for _, role := range []store.Role{store.RoleOperator, store.RoleViewer} {
		if resp, out := postDoctor(t, e, users[role].client, users[role].csrf, "orders"); resp.StatusCode != 403 {
			t.Fatalf("%s: want 403, got %d %s", role, resp.StatusCode, out)
		}
	}
	if n := len(e.fakeCalls()); n != 0 {
		t.Fatalf("doctor ran %d time(s) for refused requests", n)
	}
	if resp, out := postDoctor(t, e, users[store.RoleAdmin].client, users[store.RoleAdmin].csrf, "orders"); resp.StatusCode != 200 {
		t.Fatalf("admin: %d %s", resp.StatusCode, out)
	}
}

// The Jobs tab has the doctor card and the pipeline head a Doctor link (stopped pipeline) for admins only.
func TestDoctorControlsOnPipelinePage(t *testing.T) {
	e := doctorEnv(t, doctorReportJSON, 3, "")
	users := setupRBACUsers(t, e)
	resp, page := e.do(users[store.RoleAdmin].client, "GET", "/pipelines/orders/jobs", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("jobs tab: %d", resp.StatusCode)
	}
	for _, want := range []string{`id="doctor"`, `data-doctor-run="orders"`, `data-doctor-result="orders"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("admin jobs tab lacks %s", want)
		}
	}
	_, graph := e.do(users[store.RoleAdmin].client, "GET", "/pipelines/orders", "", nil)
	if !strings.Contains(graph, `href="/pipelines/orders/jobs?doctor=1#doctor"`) {
		t.Fatalf("pipeline head lacks the Doctor link for a stopped pipeline")
	}
	for _, role := range []store.Role{store.RoleOperator, store.RoleViewer} {
		_, p := e.do(users[role].client, "GET", "/pipelines/orders/jobs", "", nil)
		if strings.Contains(p, "data-doctor-run") || strings.Contains(p, "jobs?doctor=1") {
			t.Fatalf("%s sees doctor controls it cannot use", role)
		}
	}
}
