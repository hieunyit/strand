package web

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	logStart  = `{"time":"2026-11-15T21:00:00Z","level":"INFO","msg":"pipeline lock acquired","pipeline":"bg"}`
	logRun    = `{"time":"2026-11-15T21:00:01Z","level":"INFO","msg":"pipeline running","pipeline":"bg","workers":4}`
	logFailed = `{"time":"2026-11-15T21:29:50Z","level":"ERROR","msg":"pipeline failed","pipeline":"bg","epoch":4,"error":"worker 2: failed to begin transaction: dial tcp 127.0.0.1:33062: connect: connection refused password=Zz9secret"}`
	logClosed = "2026/11/15 21:29:50 INFO syncer is closing...\n[mysql] 2026/11/15 21:29:50 connection.go:49: unexpected EOF"
	logErrCLI = "Error: worker 2: failed to begin transaction: dial tcp 127.0.0.1:33062: connect: connection refused"
	logClean  = `{"time":"2026-11-15T21:40:00Z","level":"INFO","msg":"pipeline stopped cleanly","pipeline":"bg"}`
)

// bgEnv dựng một pipeline chế độ nền (không có tiến trình) với nội dung log cho trước.
func bgEnv(t *testing.T, logLines ...string) *testEnv {
	t.Helper()
	e := newEnv(t, func(e *testEnv, c *Config) {
		dir := t.TempDir()
		lp := filepath.Join(dir, "strand.log")
		if err := os.WriteFile(lp, []byte(strings.Join(logLines, "\n")+"\n"), 0o640); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, "bg.yaml")
		if err := os.WriteFile(p, []byte("name: bg\nsource: {kind: mysql}\ntarget: {kind: mysql}\nobservability: {admin_socket: "+dir+"/none.sock}\n"), 0o640); err != nil {
			t.Fatal(err)
		}
		c.Pipelines = append(c.Pipelines, PipelineEntry{Name: "bg", Config: p, LogFile: lp, PidFile: filepath.Join(dir, "strand.pid")})
	})
	e.sampleNow()
	return e
}

func TestBackgroundFailureWithoutErrorCodeIsFailed(t *testing.T) {
	cases := []struct {
		name  string
		lines []string
		state string
		code  int // 0 = không có last_exit
	}{
		{"untagged failure (record + CLI line)", []string{logStart, logRun, logFailed, logClosed, logErrCLI}, StateFailed, 1},
		{"untagged failure, record only", []string{logStart, logRun, logFailed}, StateFailed, 1},
		{"untagged failure, CLI line only", []string{logStart, logErrCLI}, StateFailed, 1},
		{"tagged failure", []string{logStart, "Error [binlog_config_invalid]: log_bin is OFF"}, StateFailed, 3},
		{"clean stop", []string{logStart, logRun, logClean}, StateStopped, 0},
		{"old failure then clean run and stop", []string{logStart, logFailed, logErrCLI, logStart, logRun, logClean}, StateStopped, 0},
		{"old failure then new run killed", []string{logStart, logFailed, logErrCLI, logStart, logRun}, StateStopped, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := bgEnv(t, tc.lines...)
			c := e.client()
			e.login(c, testPassword)
			if st := e.srv.Sampler().Latest("bg").State; st != tc.state {
				t.Fatalf("state %s, want %s", st, tc.state)
			}
			var pv struct {
				Pipeline PipelineView `json:"pipeline"`
			}
			_, body := e.do(c, "GET", "/api/v1/pipelines/bg", "", nil)
			if err := json.Unmarshal([]byte(body), &pv); err != nil {
				t.Fatal(err, body)
			}
			var ev ErrorsView
			_, body = e.do(c, "GET", "/api/v1/pipelines/bg/errors", "", nil)
			if err := json.Unmarshal([]byte(body), &ev); err != nil {
				t.Fatal(err, body)
			}
			if tc.code == 0 {
				if pv.Pipeline.LastExit != nil || ev.LastExit != nil {
					t.Fatalf("unexpected last_exit: %s", body)
				}
				return
			}
			if pv.Pipeline.LastExit == nil || pv.Pipeline.LastExit.Code != tc.code || ev.LastExit == nil || ev.LastExit.Code != tc.code || ev.ExitFrom != "log" {
				t.Fatalf("last_exit want %d: view=%+v errors=%s", tc.code, pv.Pipeline.LastExit, body)
			}
		})
	}
}

func TestLogEventsKeepAttributesAndCLIErrorLine(t *testing.T) {
	e := bgEnv(t, logStart,
		`{"time":"t1","level":"WARN","msg":"slow apply","pipeline":"bg","epoch":4,"reason":"lock wait","hint":"raise timeout","table":"shop.orders","n":3}`,
		logFailed, logClosed, logErrCLI, "Error: something only the CLI printed")
	c := e.client()
	e.login(c, testPassword)
	var ev ErrorsView
	_, body := e.do(c, "GET", "/api/v1/pipelines/bg/errors", "", nil)
	if err := json.Unmarshal([]byte(body), &ev); err != nil {
		t.Fatal(err, body)
	}
	all := ""
	for _, x := range ev.FromLog {
		all += x.Message + "\n"
	}
	for _, want := range []string{"slow apply  hint=raise timeout  n=3  reason=lock wait  table=shop.orders", "connection refused", "Error: something only the CLI printed"} {
		if !strings.Contains(all, want) {
			t.Errorf("log_events lacks %q:\n%s", want, all)
		}
	}
	if strings.Contains(all, "Zz9secret") {
		t.Errorf("secret not masked:\n%s", all)
	}
	if strings.Contains(all, "pipeline=") || strings.Contains(all, "epoch=") {
		t.Errorf("standard keys leaked:\n%s", all)
	}
	// dòng Error: trùng lý do với bản ghi "pipeline failed" không lặp lại
	if n := strings.Count(all, "connection refused"); n != 1 {
		t.Errorf("duplicate failure reason (%d):\n%s", n, all)
	}
}

func TestParseLogEventsTextAttrs(t *testing.T) {
	ev := ParseLogEvents([]string{`time=t3 level=ERROR msg="c d" error="x y" table=a.b pipeline=p epoch=2`}, 10)
	if len(ev) != 1 || ev[0].Message != "c d  error=x y  table=a.b" {
		t.Fatalf("%+v", ev)
	}
}

func TestOverviewCountsHaveEveryState(t *testing.T) {
	e := bgEnv(t, logStart)
	c := e.client()
	e.login(c, testPassword)
	var ov struct {
		Counts map[string]int `json:"counts"`
	}
	_, body := e.do(c, "GET", "/api/v1/overview", "", nil)
	if err := json.Unmarshal([]byte(body), &ov); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"synced", "bootstrapping", "paused", "starting", "failed", "stopped", "unreachable", "unknown"} {
		if _, ok := ov.Counts[s]; !ok {
			t.Errorf("counts lacks %q: %s", s, body)
		}
	}
}

func TestFailedBackgroundPipelinePagesShowStateAndReason(t *testing.T) {
	e := bgEnv(t, logStart, logRun, logFailed, logClosed, logErrCLI)
	c := e.client()
	e.login(c, testPassword)
	for _, path := range []string{"/", "/pipelines"} {
		_, body := e.do(c, "GET", path, "", nil)
		if !strings.Contains(body, `badge st-failed" data-bind-badge="state">failed<`) {
			t.Errorf("%s: no failed badge: %s", path, body)
		}
	}
	_, body := e.do(c, "GET", "/pipelines/bg", "", nil)
	for _, want := range []string{`badge big st-failed" data-bind-badge="state">failed<`, "Last exit code", "Unclassified or transient error", "Reason:", "connection refused"} {
		if !strings.Contains(body, want) {
			t.Errorf("detail header lacks %q: %s", want, body)
		}
	}
	if strings.Contains(body, "Zz9secret") {
		t.Error("secret leaked in header")
	}
	_, body = e.do(c, "GET", "/pipelines/bg/errors", "", nil)
	for _, want := range []string{"Last exit code 1", "What to do", "connection refused", "pipeline failed", "Recent log entries", "Live events are not available"} {
		if !strings.Contains(body, want) {
			t.Errorf("errors tab lacks %q: %s", want, body)
		}
	}
	if strings.Contains(body, `aria-label="Live metrics"`) || !strings.Contains(body, `<tr class="empty-row" hidden`) {
		t.Error("failed pipeline should hide unavailable metrics and the live empty-state message")
	}
}

// Chế độ nền không có systemd khởi động lại: last_exit không được hứa "Systemd restarts it automatically"
// (auto_restart=false) ở header, tab Errors và API — trước đây exit 1 luôn ghi như vậy.
func TestBackgroundFailureDoesNotPromiseSystemdRestart(t *testing.T) {
	e := bgEnv(t, logStart, logRun, logFailed, logClosed, logErrCLI)
	c := e.client()
	e.login(c, testPassword)
	for _, path := range []string{"/pipelines/bg", "/pipelines/bg/errors"} {
		_, body := e.do(c, "GET", path, "", nil)
		if strings.Contains(body, "Systemd restarts") {
			t.Errorf("%s promises a systemd restart in background mode", path)
		}
		if !strings.Contains(body, "press Start") {
			t.Errorf("%s lacks the background-mode action", path)
		}
	}
	for _, path := range []string{"/api/v1/pipelines/bg", "/api/v1/pipelines/bg/errors"} {
		_, body := e.do(c, "GET", path, "", nil)
		if strings.Contains(body, `"auto_restart":true`) || strings.Contains(body, `"auto_restart": true`) {
			t.Errorf("%s: auto_restart true in background mode: %s", path, body)
		}
	}
}
