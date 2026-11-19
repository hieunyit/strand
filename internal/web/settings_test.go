package web

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/web/store"
)

func settingOf(t *testing.T, body string, key string) SettingView {
	t.Helper()
	var out struct {
		Settings []SettingView `json:"settings"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	for _, s := range out.Settings {
		if s.Key == key {
			return s
		}
	}
	t.Fatalf("setting %s missing: %s", key, body)
	return SettingView{}
}

func TestSettingsDatabaseMode(t *testing.T) {
	st := store.NewMem()
	scanDir := t.TempDir()
	e := dbEnv(t, st, nil, nil)
	c, csrf := e.loginOK()
	h := map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}

	_, body := e.do(c, "GET", "/api/v1/settings", "", nil)
	if s := settingOf(t, body, "scan"); s.Source != SrcDefault || !s.Editable {
		t.Fatalf("scan before: %+v", s)
	}
	// New scan glob: applies immediately, the discovered pipeline appears.
	src, _ := os.ReadFile(e.srv.reg.Get("orders").ConfigPath)
	must0(t, os.WriteFile(filepath.Join(scanDir, "scanned.yaml"), src, 0o640))
	glob := filepath.Join(scanDir, "*.yaml")
	resp, body := e.do(c, "POST", "/api/v1/settings", `{"scan": ["`+glob+`"], "trust_forwarded_for": true}`, h)
	if resp.StatusCode != 200 {
		t.Fatalf("save: %d %s", resp.StatusCode, body)
	}
	if s := settingOf(t, body, "scan"); s.Source != SrcDatabase || len(s.Value.([]any)) != 1 {
		t.Fatalf("scan after: %+v", s)
	}
	if e.srv.reg.Get("scanned") == nil {
		t.Fatal("scan setting did not apply")
	}
	if !e.srv.settingBool("trust_forwarded_for") {
		t.Fatal("trust_forwarded_for did not apply")
	}
	// Audit: one row per changed key.
	rows, _ := st.ListAudit(context.Background(), 50)
	n := 0
	for _, r := range rows {
		if r.Action == "settings.edit" && r.Result == "ok" {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("settings.edit audit rows = %d, want 2", n)
	}
	// Validation.
	for _, bad := range []string{`{"scan": ["relative/*.yaml"]}`, `{"sample_interval_sec": 0}`, `{"config_dir": "x/../y"}`, `{"listen": "0.0.0.0:1"}`, `{"secure_cookies": "maybe"}`} {
		if resp, body := e.do(c, "POST", "/api/v1/settings", bad, h); resp.StatusCode != 400 {
			t.Errorf("%s: %d %s, want 400", bad, resp.StatusCode, body)
		}
	}
	// Restart-only setting: saved, flagged pending, running value unchanged.
	resp, body = e.do(c, "POST", "/api/v1/settings", `{"sample_interval_sec": 10}`, h)
	if s := settingOf(t, body, "sample_interval_sec"); resp.StatusCode != 200 || !s.PendingRestart {
		t.Fatalf("sample interval: %d %+v", resp.StatusCode, s)
	}
	// Survives a web restart; the restart applies the interval.
	e2 := e.restart()
	if e2.srv.reg.Get("scanned") == nil || e2.srv.samp.interval.Seconds() != 10 {
		t.Fatalf("settings lost across restart (interval %v)", e2.srv.samp.interval)
	}
	// Reset: back to the default, row removed.
	c2, csrf2 := e2.loginOK()
	resp, body = e2.do(c2, "POST", "/api/v1/settings", `{"scan": null}`, map[string]string{csrfHeader: csrf2, "Content-Type": "application/json"})
	if s := settingOf(t, body, "scan"); resp.StatusCode != 200 || s.Source != SrcDefault {
		t.Fatalf("reset: %d %+v", resp.StatusCode, s)
	}
	if m, _ := st.Settings(context.Background()); m["scan"] != "" {
		t.Fatal("reset left the row")
	}
	// Environment variable wins and locks the key.
	t.Setenv("STRAND_WEB_SECURE_COOKIES", "true")
	e3 := e2.restart()
	c3, csrf3 := e3.loginOK()
	_, body = e3.do(c3, "GET", "/api/v1/settings", "", nil)
	if s := settingOf(t, body, "secure_cookies"); s.Source != SrcEnv || s.Editable || s.Value != true {
		t.Fatalf("env override: %+v", s)
	}
	if resp, _ := e3.do(c3, "POST", "/api/v1/settings", `{"secure_cookies": false}`, map[string]string{csrfHeader: csrf3, "Content-Type": "application/json"}); resp.StatusCode != 409 {
		t.Fatalf("editing an env-set key: %d, want 409", resp.StatusCode)
	}
	// HTML page: form posts with CSRF, shows sources.
	resp, page := e3.do(c3, "GET", "/settings", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(page, "STRAND_WEB_SECURE_COOKIES") || !strings.Contains(page, `name="csrf"`) {
		t.Fatalf("settings page: %d", resp.StatusCode)
	}
	form := url.Values{"csrf": {csrf3}, "config_dir": {"/srv/strand"}, "scan": {""}, "trust_forwarded_for": {"true"}, "shutdown_stop_pipelines": {"false"}, "sample_interval_sec": {"10"}}
	resp, page = e3.do(c3, "POST", "/settings", form.Encode(), map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
	if resp.StatusCode != 200 || !strings.Contains(page, "Saved: config_dir.") {
		t.Fatalf("form save: %d %s", resp.StatusCode, page)
	}
	if e3.srv.ConfigDir() != "/srv/strand" {
		t.Fatalf("config_dir = %s", e3.srv.ConfigDir())
	}
	// No CSRF: rejected.
	if resp, _ := e3.do(c3, "POST", "/settings", "config_dir=/x", map[string]string{"Content-Type": "application/x-www-form-urlencoded"}); resp.StatusCode != 403 {
		t.Fatalf("settings POST without CSRF: %d", resp.StatusCode)
	}
}
