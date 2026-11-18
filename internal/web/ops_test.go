package web

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// Mỗi action: chưa đăng nhập → 401, thiếu CSRF → 403, mọi thứ còn lại thuộc các test bên dưới.
func TestOpsActionsRequireAuthAndCSRF(t *testing.T) {
	e := opsEnv(t, true, nil)
	e.sampleNow()
	names := []string{ActStart, ActStop, ActRestart, ActEnable, ActDisable, ActPause, ActPauseSnapshot, ActResume, ActResumeSnapOnly,
		ActThrottle, ActSnapshotAdd, ActCheckpointShow, ActRewind, ActVerify, ActJobCancel, ActDelete}
	for _, n := range names {
		if _, ok := e.srv.Actions().Get(n); !ok {
			t.Errorf("action %s not registered", n)
		}
		anon := e.client()
		resp, _ := e.do(anon, "POST", "/api/v1/pipelines/orders/actions/"+n, "{}", nil)
		if resp.StatusCode != 401 {
			t.Errorf("%s unauthenticated: %d, want 401", n, resp.StatusCode)
		}
	}
	c, csrf := e.loginOK()
	for _, n := range names {
		resp, _ := e.do(c, "POST", "/api/v1/pipelines/orders/actions/"+n, "{}", map[string]string{"Content-Type": "application/json"})
		if resp.StatusCode != 403 {
			t.Errorf("%s without CSRF: %d, want 403", n, resp.StatusCode)
		}
		resp, _ = e.do(c, "POST", "/api/v1/pipelines/orders/actions/"+n, "{}", map[string]string{csrfHeader: csrf + "x"})
		if resp.StatusCode != 403 {
			t.Errorf("%s with wrong CSRF: %d, want 403", n, resp.StatusCode)
		}
	}
	// Nothing ran.
	if got := e.cmd.callsWith("systemctl start"); len(got) != 0 {
		t.Errorf("systemctl ran without auth: %v", got)
	}
	if len(e.mock.Calls()) != 0 || len(e.fakeCalls()) != 0 {
		t.Errorf("a backend ran without auth: %v %v", e.mock.Calls(), e.fakeCalls())
	}
	// Bearer token (no CSRF) is accepted.
	if code, _ := e.act(c, csrf, "nope", ActPause, nil); code != 404 {
		t.Errorf("unknown pipeline: %d", code)
	}
}

func TestOpsSystemdLifecycle(t *testing.T) {
	e := opsEnv(t, true, nil)
	e.cmd.out["systemctl "] = ""
	c, csrf := e.loginOK()
	for _, verb := range []string{"start", "stop", "restart", "enable", "disable"} {
		code, body := e.act(c, csrf, "orders", verb, nil)
		if code != 200 {
			t.Fatalf("%s: %d %v", verb, code, body)
		}
		want := "systemctl " + verb + " strand@orders.service"
		got := e.cmd.callsWith("systemctl " + verb + " ")
		if len(got) != 1 || got[0] != want {
			t.Errorf("%s: commands %v, want exactly [%q]", verb, got, want)
		}
		if body["exit_code"] != float64(0) {
			t.Errorf("%s: exit_code %v", verb, body["exit_code"])
		}
		a := e.auditFor(verb)
		if len(a) != 2 || a[0]["result"] != "started" || a[1]["result"] != "ok" || a[1]["exit_code"] != float64(0) || a[1]["pipeline"] != "orders" {
			t.Errorf("%s: audit %v", verb, a)
		}
	}
	if len(e.fakeCalls()) != 0 {
		t.Errorf("systemd mode must not exec strand: %v", e.fakeCalls())
	}
	// Failure: exit code is relayed and audited as error.
	e.cmd.fail = map[string]int{"systemctl stop": 5}
	code, body := e.act(c, csrf, "orders", "stop", nil)
	if code != 502 || !strings.Contains(body["error"].(string), "Access denied") {
		t.Fatalf("stop failure: %d %v", code, body)
	}
	a := e.auditFor("stop")
	last := a[len(a)-1]
	if last["result"] != "error" || last["exit_code"] != float64(5) {
		t.Errorf("failed stop audit: %v", last)
	}
}

func TestOpsBackgroundLifecycle(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	cfg := filepath.Join(e.dir, "orders.yaml")

	if code, body := e.act(c, csrf, "orders", "start", nil); code != 200 {
		t.Fatalf("start: %d %v", code, body)
	}
	if code, _ := e.act(c, csrf, "orders", "stop", nil); code != 200 {
		t.Fatalf("stop: %d", code)
	}
	if code, _ := e.act(c, csrf, "orders", "restart", nil); code != 200 {
		t.Fatalf("restart: %d", code)
	}
	calls := e.fakeCalls()
	want := [][]string{
		{"sync", "-c", cfg, "--background"},
		{"stop", "-c", cfg},
		{"stop", "-c", cfg}, {"sync", "-c", cfg, "--background"},
	}
	if len(calls) != len(want) {
		t.Fatalf("calls %v, want %v", calls, want)
	}
	for i, w := range want {
		if !reflect.DeepEqual(calls[i].Args, w) {
			t.Errorf("call %d argv %v, want %v", i, calls[i].Args, w)
		}
		if calls[i].Env != "from-env-file" {
			t.Errorf("call %d: env_file not applied (STRAND_TEST_VAR=%q)", i, calls[i].Env)
		}
	}
	if got := e.cmd.callsWith("systemctl"); len(got) != 0 {
		t.Errorf("background mode must not call systemctl: %v", got)
	}
	// enable/disable are hidden/refused in background mode.
	for _, v := range []string{"enable", "disable"} {
		if code, _ := e.act(c, csrf, "orders", v, nil); code != 400 {
			t.Errorf("%s in background mode: %d, want 400", v, code)
		}
	}
	// A non-zero exit of strand is a 502 with the exit code audited.
	e.setExit(3)
	code, body := e.act(c, csrf, "orders", "start", nil)
	if code != 502 || body["error"] == nil {
		t.Fatalf("start failure: %d %v", code, body)
	}
	a := e.auditFor("start")
	if last := a[len(a)-1]; last["result"] != "error" || last["exit_code"] != float64(3) {
		t.Errorf("audit of failed start: %v", last)
	}
	// Restart aborts when the stop step fails (does not start a second instance).
	before := len(e.fakeCalls())
	if code, _ := e.act(c, csrf, "orders", "restart", nil); code != 502 {
		t.Errorf("restart with failing stop: %d", code)
	}
	if n := len(e.fakeCalls()) - before; n != 1 {
		t.Errorf("restart with failing stop ran %d commands, want 1", n)
	}
}

func TestOpsAdminSocketActions(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	for _, tc := range []struct {
		action string
		body   map[string]string
		call   string
	}{
		{ActPause, nil, "pause"},
		{ActPauseSnapshot, nil, "pause_snapshot"},
		{ActResume, nil, "resume snapshot_only=false"},
		{ActResumeSnapOnly, nil, "resume snapshot_only=true"},
		{ActThrottle, map[string]string{"source_iops": "5000"}, "throttle src=5000 tgt=0 soft=true"},
		{ActSnapshotAdd, map[string]string{"table": "app.extra"}, "snapshot_add app.extra"},
		{ActSnapshotAdd, map[string]string{"tables": "app.extra, app.more"}, "snapshot_add app.extra"},
	} {
		before := len(e.mock.Calls())
		code, body := e.act(c, csrf, "orders", tc.action, tc.body)
		if code != 200 {
			t.Fatalf("%s: %d %v", tc.action, code, body)
		}
		got := e.mock.Calls()[before:]
		if len(got) == 0 || got[0] != tc.call {
			t.Errorf("%s: socket calls %v, want first %q", tc.action, got, tc.call)
		}
	}
	// Multiple tables are added one by one, in order.
	calls := e.mock.Calls()
	if calls[len(calls)-1] != "snapshot_add app.more" {
		t.Errorf("last socket call %q", calls[len(calls)-1])
	}
	for _, a := range []string{ActPause, ActPauseSnapshot, ActResume, ActResumeSnapOnly, ActThrottle, ActSnapshotAdd} {
		al := e.auditFor(a)
		if len(al) < 2 || al[0]["result"] != "started" || al[1]["result"] != "ok" {
			t.Errorf("%s audit: %v", a, al)
		}
	}
	if code, body := e.act(c, csrf, "orders", ActCheckpointShow, nil); code != 200 || !strings.Contains(toJSON(body), "uuid-1234:1-99") {
		t.Errorf("checkpoint-show: %d %v", code, body)
	}
	// Socket-side failure is relayed (502) and audited as error.
	e.mock.fail = "table app.extra is already being snapshotted"
	code, body := e.act(c, csrf, "orders", ActSnapshotAdd, map[string]string{"table": "app.extra"})
	if code != 502 || !strings.Contains(body["error"].(string), "already being snapshotted") {
		t.Errorf("socket failure: %d %v", code, body)
	}
	al := e.auditFor(ActSnapshotAdd)
	if al[len(al)-1]["result"] != "error" {
		t.Errorf("audit of failure: %v", al[len(al)-1])
	}
}

func TestOpsInputValidationNeverReachesBackends(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	bad := []struct {
		action string
		body   map[string]string
	}{
		{ActThrottle, nil},
		{ActThrottle, map[string]string{"source_iops": "abc"}},
		{ActThrottle, map[string]string{"source_iops": "0"}},
		{ActThrottle, map[string]string{"source_iops": "-5"}},
		{ActThrottle, map[string]string{"source_iops": "99999999999"}},
		{ActThrottle, map[string]string{"source_iops": "1; reboot"}},
		{ActSnapshotAdd, nil},
		{ActSnapshotAdd, map[string]string{"table": "users"}},
		{ActSnapshotAdd, map[string]string{"table": "app.users; DROP TABLE x"}},
		{ActSnapshotAdd, map[string]string{"table": "../etc.passwd"}},
		{ActSnapshotAdd, map[string]string{"tables": "app.ok,app.$(id) x"}},
		{ActSnapshotAdd, map[string]string{"table": "-rf.x"}},
		{ActVerify, map[string]string{"mode": "repair"}},
		{ActVerify, map[string]string{"mode": "sample"}},
		{ActVerify, map[string]string{"mode": "sample", "table": "app.users", "rate": "2"}},
		{ActVerify, map[string]string{"mode": "sample", "table": "app.users", "rate": "0"}},
		{ActVerify, map[string]string{"mode": "full", "rate": "1e3"}},
		{ActVerify, map[string]string{"mode": "full", "rate": "--repair=fix"}},
		{ActVerify, map[string]string{"mode": "full", "table": "app.users --repair fix"}},
		{ActVerify, map[string]string{"mode": "full", "table": "-x.y"}},
		{ActVerify, map[string]string{"mode": "delta", "table": "app.users"}},
		{ActVerify, map[string]string{"mode": "delta", "rate": "5"}},
		{ActVerify, map[string]string{"mode": "sample", "table": "app.users", "resume": "true"}},
		{ActVerify, map[string]string{"mode": "full", "resume": "maybe"}},
		{ActJobCancel, map[string]string{"job_id": "../../etc/passwd"}},
	}
	for _, tc := range bad {
		if code, body := e.act(c, csrf, "orders", tc.action, tc.body); code != 400 {
			t.Errorf("%s %v: %d %v, want 400", tc.action, tc.body, code, body)
		}
	}
	// Rewind: wrong confirm_name first, then a bad position (confirm is right).
	for _, body := range []map[string]string{
		{"to": "uuid:1-5"},
		{"to": "uuid:1-5", "confirm_name": "other"},
		{"to": "uuid:1-5", "confirm_name": "ORDERS"},
	} {
		if code, _ := e.act(c, csrf, "orders", ActRewind, body); code != 400 {
			t.Errorf("rewind %v: %d, want 400", body, code)
		}
	}
	for _, to := range []string{"", "uuid:1-5; drop", "a'b", "$(id)", strings.Repeat("1", 9000)} {
		if code, _ := e.act(c, csrf, "orders", ActRewind, map[string]string{"to": to, "confirm_name": "orders"}); code != 400 {
			t.Errorf("rewind to %q: %d, want 400", to, code)
		}
	}
	if len(e.mock.Calls()) != 0 || len(e.fakeCalls()) != 0 || len(e.srv.jobs.List("")) != 0 {
		t.Errorf("a rejected request reached a backend: socket %v exec %v", e.mock.Calls(), e.fakeCalls())
	}
}

func TestOpsRewind(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	code, body := e.act(c, csrf, "orders", ActRewind, map[string]string{"to": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-77", "confirm_name": "orders"})
	if code != 200 {
		t.Fatalf("rewind: %d %v", code, body)
	}
	if got := e.mock.Calls(); len(got) != 1 || got[0] != "rewind 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-77" {
		t.Errorf("socket calls %v", got)
	}
	al := e.auditFor(ActRewind)
	if len(al) != 2 || al[1]["result"] != "ok" {
		t.Errorf("audit %v", al)
	}
	// The wrong-name attempt is rejected before any audit line "started" would be needed: nothing ran.
	n := len(e.mock.Calls())
	if code, _ := e.act(c, csrf, "orders", ActRewind, map[string]string{"to": "x:1", "confirm_name": "wrong"}); code != 400 {
		t.Errorf("wrong confirm_name: %d", code)
	}
	if len(e.mock.Calls()) != n {
		t.Errorf("rewind ran with a wrong confirm_name")
	}
}

func TestOpsNoSocketConfigured(t *testing.T) {
	e := opsEnv(t, false, nil)
	e.srv.reg.Get("orders").Cfg.Observability.AdminSocket = ""
	c, csrf := e.loginOK()
	if code, body := e.act(c, csrf, "orders", ActPause, nil); code != 400 || !strings.Contains(body["error"].(string), "admin_socket") {
		t.Errorf("pause without socket: %d %v", code, body)
	}
}

func TestOpsSourceTablesEndpoint(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, _ := e.loginOK()
	resp, body := e.do(c, "GET", "/api/v1/pipelines/orders/source-tables", "", nil)
	var v SourceTablesView
	if resp.StatusCode != 200 || json.Unmarshal([]byte(body), &v) != nil || len(v.Tables) != 3 || !v.Tables[0].InPipeline || v.Tables[1].Table != "app.extra" {
		t.Fatalf("source-tables: %d %s", resp.StatusCode, body)
	}
	anon := e.client()
	if resp, _ := e.do(anon, "GET", "/api/v1/pipelines/orders/source-tables", "", nil); resp.StatusCode != 401 {
		t.Errorf("unauthenticated: %d", resp.StatusCode)
	}
}

func TestOpsPagesRenderControls(t *testing.T) {
	e := opsEnv(t, true, nil)
	e.sampleNow()
	c, _ := e.loginOK()
	has := func(path string, wants ...string) string {
		t.Helper()
		resp, body := e.do(c, "GET", path, "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("GET %s: %d", path, resp.StatusCode)
		}
		for _, w := range wants {
			if !strings.Contains(body, w) {
				t.Errorf("GET %s: missing %q", path, w)
			}
		}
		// CSP default-src 'self': no inline script, handler or style.
		for _, bad := range []string{"onclick=", "onsubmit=", " style=", "<script>"} {
			if strings.Contains(body, bad) {
				t.Errorf("GET %s: contains inline %q (blocked by the CSP)", path, bad)
			}
		}
		return body
	}
	has("/pipelines/orders", `data-action="restart"`, `data-action="stop"`, `data-action="enable"`, `data-action="disable"`, `data-action="pause"`, `data-action="throttle"`, `data-confirm=`)
	has("/pipelines/orders/tables", `data-action="snapshot-add"`, `value="app.extra"`, `data-action="pause-snapshot"`, `data-action="resume-snapshot-only"`)
	body := has("/pipelines/orders/tables")
	if strings.Contains(body, `value="app.users"`) {
		t.Errorf("tables already in the pipeline must not be offered for snapshot add")
	}
	has("/pipelines/orders/jobs", `data-action="verify"`, `Job history`)
	has("/jobs", `Jobs`)
	// Rewind form only for MySQL -> MySQL: the fixture is mysql -> postgres.
	body = has("/pipelines/orders/checkpoint")
	if strings.Contains(body, `data-action="checkpoint-rewind"`) {
		t.Errorf("rewind offered for a MySQL -> PostgreSQL pipeline")
	}
	// Background mode: no enable/disable.
	e2 := opsEnv(t, false, nil)
	e2.sampleNow()
	c2, _ := e2.loginOK()
	_, body = e2.do(c2, "GET", "/pipelines/orders", "", nil)
	if strings.Contains(body, `data-action="enable"`) || strings.Contains(body, `data-action="disable"`) {
		t.Errorf("enable/disable shown in background mode")
	}
	// Unauthenticated pages redirect.
	for _, p := range []string{"/jobs", "/jobs/20261115T100000Z-deadbeef", "/pipelines/orders/jobs"} {
		if resp, _ := e.do(e.client(), "GET", p, "", nil); resp.StatusCode != http.StatusFound {
			t.Errorf("GET %s without session: %d", p, resp.StatusCode)
		}
	}
}

func toJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

// Bị polkit từ chối: enable/disable chỉ dẫn chạy sudo systemctl trên máy chủ; start/stop/restart chỉ dẫn cài quy tắc polkit.
func TestOpsSystemdDeniedHints(t *testing.T) {
	e := opsEnv(t, true, nil)
	e.cmd.out["systemctl "] = ""
	c, csrf := e.loginOK()
	e.cmd.fail = map[string]int{"systemctl enable": 1, "systemctl disable": 1, "systemctl start": 1}
	for verb, want := range map[string]string{
		"enable":  "sudo systemctl enable strand@orders.service",
		"disable": "sudo systemctl disable strand@orders.service",
		"start":   "50-strand.rules",
	} {
		code, body := e.act(c, csrf, "orders", verb, nil)
		msg, _ := body["error"].(string)
		if code != 502 || !strings.Contains(msg, want) {
			t.Errorf("%s: %d %q, want 502 containing %q", verb, code, msg, want)
		}
	}
}

func TestOpsDeletePipeline(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()

	// Pipeline requires confirmation to delete
	code, body := e.act(c, csrf, "orders", ActDelete, nil)
	if code != 400 {
		t.Fatalf("delete without confirmation: got %d, want 400: %v", code, body)
	}

	// Delete with confirmation
	code, body = e.act(c, csrf, "orders", ActDelete, map[string]string{"confirm_name": "orders"})
	if code != 200 {
		t.Fatalf("delete with confirmation: got %d, want 200: %v", code, body)
	}

	// Pipeline should be gone from registry
	if p := e.srv.Registry().Get("orders"); p != nil {
		t.Fatalf("orders still in registry after delete")
	}

	// Config file should be deleted
	cfgPath := filepath.Join(e.dir, "orders.yaml")
	if _, err := os.Stat(cfgPath); !os.IsNotExist(err) {
		t.Fatalf("orders.yaml still exists after delete")
	}
}
