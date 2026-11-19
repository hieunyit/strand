package web

import (
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentTuningPreservesIndependentUpdates(t *testing.T) {
	e := opsEnv(t, false, nil)
	p := e.srv.reg.Get("orders")
	// Make validation long enough for concurrent requests to overlap their reads.
	// Only the first validation needs to block; all saves use the same fixture.
	e.hold(true)
	t.Cleanup(func() { e.hold(false) })
	updates := map[string]any{"workers": "4", "chunk_rows": "12345", "algorithm": "static", "lag_slo": "17s", "replica_lag_max": "31s"}
	start := make(chan struct{})
	errs := make(chan error, len(updates))
	var wg sync.WaitGroup
	for key, value := range updates {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := e.srv.saveTuning(t.Context(), p, map[string]any{key: value}, false, false, false, "test")
			errs <- err
		}()
	}
	close(start)
	// Wait until the first validator is running, then keep it blocked while the
	// remaining requests arrive. Their independent changes must all survive.
	deadline := time.Now().Add(5 * time.Second)
	for len(e.fakeCalls()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(e.fakeCalls()) == 0 {
		e.hold(false)
		wg.Wait()
		t.Fatal("validator did not start")
	}
	time.Sleep(100 * time.Millisecond)
	e.hold(false)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	view := e.srv.tuningView(p)
	for _, field := range view.Fields {
		if want, ok := updates[field.Key]; ok && field.Value != want {
			t.Errorf("concurrent update lost for %s: got %q want %q", field.Key, field.Value, want)
		}
	}
}

func TestTuningSaveRejectsOversizedBody(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	// Whitespace is valid in a duration string and is trimmed by validation.
	// Without a body limit this request succeeds after allocating the whole body.
	body := `{"lag_slo":"` + strings.Repeat(" ", 1<<20) + `17s"}`
	resp, out := e.do(c, "POST", "/api/v1/pipelines/orders/tuning", body, tuningHdr(csrf))
	if resp.StatusCode != 400 {
		t.Fatalf("oversized tuning request: status=%d body=%s", resp.StatusCode, out)
	}
}

var tuningHdr = func(csrf string) map[string]string {
	return map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}
}

func writeTuningBase(t *testing.T, e *testEnv, extra string) string {
	t.Helper()
	p := filepath.Join(e.dir, "orders.yaml")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	e.writePipelineFile("orders", p, "# keep me: header comment\n"+string(b)+extra, 0o640)
	return p
}

func TestTuningGetCurrentAndDefaults(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, _ := e.loginOK()
	writeTuningBase(t, e, "bootstrap:\n  logical: { chunk_rows: 10000, windows: [\"22:00-06:00\", \"12:00-13:00\"] }\ngovernor:\n  algorithm: grace-v2\n  breaker: { replica_lag: { max: 30s } }\n")
	resp, out := e.do(c, "GET", "/api/v1/pipelines/orders/tuning", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("get: %d %s", resp.StatusCode, out)
	}
	var v TuningView
	if err := json.Unmarshal([]byte(out), &v); err != nil {
		t.Fatal(err)
	}
	got := map[string]TuningField{}
	for _, f := range v.Fields {
		got[f.Key] = f
	}
	for key, want := range map[string]string{"chunk_rows": "10000", "windows": "22:00-06:00, 12:00-13:00", "algorithm": "grace-v2", "replica_lag_max": "30s"} {
		if !got[key].Set || got[key].Value != want {
			t.Errorf("%s: got %+v, want value %q set", key, got[key], want)
		}
	}
	for key, def := range map[string]string{"iops_soft": "600", "lag_slo": "5s", "breaker_max_ratio": "off", "workers": "8"} {
		if got[key].Set || got[key].Value != "" || got[key].Default != def {
			t.Errorf("%s: got %+v, want unset with default %q", key, got[key], def)
		}
	}
	sql := 0
	for _, d := range tuningDefs {
		if !d.Mongo {
			sql++
		}
	}
	if len(v.Fields) != sql {
		t.Errorf("fields = %d, want the %d SQL-pipeline fields", len(v.Fields), sql)
	}
}

func TestTuningPostUpdatesPreservingCommentsAndRemovesKeys(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	e.setExit(0)
	path := writeTuningBase(t, e, "bootstrap:\n  logical: { chunk_rows: 10000 }  # inline note\ngovernor:\n  algorithm: onoff\n  lag_slo: 5s\n")
	orig, _ := os.ReadFile(path)

	b, _ := json.Marshal(map[string]any{
		"iops_soft": 300, "iops_hard": "900", "chunk_rows": "auto", "windows": []string{"22:00-06:00"},
		"algorithm": "grace-v2", "lag_slo": "", "breaker_max_ratio": 3.5, "replica_lag_max": "30s", "workers": 4,
	})
	resp, out := e.do(c, "POST", "/api/v1/pipelines/orders/tuning", string(b), tuningHdr(csrf))
	if resp.StatusCode != 200 {
		t.Fatalf("post: %d %s", resp.StatusCode, out)
	}
	var res tuningResult
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatal(err)
	}
	if !res.OK || res.Backup == "" || res.Diff == "" || len(res.Changed) != 9 {
		t.Fatalf("result: %+v", res)
	}
	bak, err := os.ReadFile(res.Backup)
	if err != nil || string(bak) != string(orig) {
		t.Errorf("backup missing or different: %v", err)
	}
	after, _ := os.ReadFile(path)
	s := string(after)
	for _, want := range []string{"# keep me: header comment", "# inline note", "SuperSecretSrc", "mask:", "app.users.phone: sha256",
		"soft: 300", "hard: 900", "chunk_rows: auto", `"22:00-06:00"`, "algorithm: grace-v2", "max_ratio: 3.5", "max: 30s", "max: 4"} {
		if !strings.Contains(s, want) {
			t.Errorf("result lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "lag_slo") {
		t.Errorf("lag_slo should be removed:\n%s", s)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o640 {
		t.Errorf("mode = %v", fi.Mode().Perm())
	}
	if a := e.auditFor("config.tuning"); len(a) != 1 || a[0]["result"] != "ok" {
		t.Errorf("audit config.tuning: %v", a)
	}
	if a := e.auditFor("config.edit"); len(a) == 0 {
		t.Errorf("config.edit audit missing")
	}

	// Xóa các khóa: ô rỗng = về mặc định, mapping cha rỗng bị dọn.
	b, _ = json.Marshal(map[string]any{"iops_soft": "", "iops_hard": "", "chunk_rows": "", "windows": "", "algorithm": "",
		"breaker_max_ratio": "", "replica_lag_max": "", "workers": ""})
	resp, out = e.do(c, "POST", "/api/v1/pipelines/orders/tuning", string(b), tuningHdr(csrf))
	if resp.StatusCode != 200 {
		t.Fatalf("post clear: %d %s", resp.StatusCode, out)
	}
	after, _ = os.ReadFile(path)
	s = string(after)
	for _, gone := range []string{"soft:", "hard:", "chunk_rows", "windows", "algorithm", "max_ratio", "breaker", "workers", "iops"} {
		if strings.Contains(s, gone) {
			t.Errorf("%q should be gone:\n%s", gone, s)
		}
	}
	if !strings.Contains(s, "# keep me: header comment") || !strings.Contains(s, "app.users.phone: sha256") {
		t.Errorf("comments/other keys lost:\n%s", s)
	}
	// Không thay đổi gì: không ghi thêm backup.
	resp, out = e.do(c, "POST", "/api/v1/pipelines/orders/tuning", `{"iops_soft": ""}`, tuningHdr(csrf))
	if resp.StatusCode != 200 || !strings.Contains(out, "No changes") {
		t.Errorf("noop: %d %s", resp.StatusCode, out)
	}
}

func TestTuningPostValidationErrors(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	e.setExit(0)
	path := writeTuningBase(t, e, "")
	orig, _ := os.ReadFile(path)
	bad := map[string]map[string]any{
		"iops_soft":         {"iops_soft": "abc"},
		"iops_hard":         {"iops_soft": 500, "iops_hard": 100},
		"chunk_rows":        {"chunk_rows": "-5"},
		"windows":           {"windows": "25:00-06:00"},
		"algorithm":         {"algorithm": "turbo"},
		"lag_slo":           {"lag_slo": "fast"},
		"breaker_max_ratio": {"breaker_max_ratio": 0.5},
		"replica_lag_max":   {"replica_lag_max": "-3s"},
		"workers":           {"workers": 0},
		"nope":              {"nope": 1},
	}
	for field, body := range bad {
		b, _ := json.Marshal(body)
		resp, out := e.do(c, "POST", "/api/v1/pipelines/orders/tuning", string(b), tuningHdr(csrf))
		if resp.StatusCode != 400 || !strings.Contains(out, field) {
			t.Errorf("%s: status %d body %s, want 400 naming the field", field, resp.StatusCode, out)
		}
		after, _ := os.ReadFile(path)
		if string(after) != string(orig) {
			t.Fatalf("%s: file changed despite 400", field)
		}
	}
	if m, _ := filepath.Glob(path + ".bak.*"); len(m) != 0 {
		t.Errorf("backup created on 400: %v", m)
	}
}

func TestTuningValidateFailureNotWrittenAndDryRun(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, csrf := e.loginOK()
	path := writeTuningBase(t, e, "")
	orig, _ := os.ReadFile(path)

	e.setExit(3)
	resp, out := e.do(c, "POST", "/api/v1/pipelines/orders/tuning", `{"iops_soft": 100}`, tuningHdr(csrf))
	if resp.StatusCode != 502 {
		t.Fatalf("status %d %s, want 502", resp.StatusCode, out)
	}
	if after, _ := os.ReadFile(path); string(after) != string(orig) {
		t.Errorf("file written despite failed validate")
	}
	if a := e.auditFor("config.tuning"); len(a) != 0 {
		t.Errorf("config.tuning audited on failure: %v", a)
	}

	e.setExit(0)
	resp, out = e.do(c, "POST", "/api/v1/pipelines/orders/tuning", `{"iops_soft": 100, "dry_run": true}`, tuningHdr(csrf))
	if resp.StatusCode != 200 || !strings.Contains(out, "+      soft: 100") {
		t.Fatalf("dry run: %d %s", resp.StatusCode, out)
	}
	if after, _ := os.ReadFile(path); string(after) != string(orig) {
		t.Errorf("dry run wrote the file")
	}
}

func TestTuningAuthAndCSRF(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, _ := e.loginOK()
	if resp, _ := e.do(e.client(), "GET", "/api/v1/pipelines/orders/tuning", "", nil); resp.StatusCode != 401 {
		t.Errorf("anon GET: %d", resp.StatusCode)
	}
	if resp, _ := e.do(e.client(), "POST", "/api/v1/pipelines/orders/tuning", `{}`, nil); resp.StatusCode != 401 {
		t.Errorf("anon POST: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "POST", "/api/v1/pipelines/orders/tuning", `{"iops_soft": 5}`, map[string]string{"Content-Type": "application/json"}); resp.StatusCode != 403 {
		t.Errorf("no CSRF: %d", resp.StatusCode)
	}
}

func TestTuningCardMarkup(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, _ := e.loginOK()
	writeTuningBase(t, e, "governor:\n  algorithm: grace-v2\n")
	_, page := e.do(c, "GET", "/pipelines/orders/config", "", nil)
	for _, want := range []string{`id="tuning-card"`, `name="iops_soft"`, `placeholder="default: 600"`, `name="chunk_rows"`, `name="windows"`,
		`name="algorithm"`, `<option value="grace-v2" selected>`, `name="breaker_max_ratio"`, `name="replica_lag_max"`, `name="lag_slo"`,
		`id="tuning-restart"`, `/pipelines/orders#throttle`, "takes effect after a restart"} {
		if !strings.Contains(strings.ToLower(page), strings.ToLower(want)) {
			t.Errorf("config tab lacks %q", want)
		}
	}
	// bootstrap.logical.max_parallel đã thi hành (snapshot song song): có trên Tuning card.
	if !strings.Contains(page, `name="max_parallel"`) || !strings.Contains(page, "Tables in parallel") {
		t.Errorf("tuning card lacks max_parallel (Tables in parallel)")
	}
}

func TestNewPipelineTuningFields(t *testing.T) {
	e, c, csrf := npEnv(t)
	code, _, y, out := createNPForm(t, e, c, csrf, url.Values{
		"name": {"tun1"}, "from": {"mariadb"}, "to": {"mysql"},
		"source_host": {"10.0.0.1"}, "target_host": {"10.0.0.2"}, "databases": {"shop"},
		"iops_soft": {"250"}, "iops_hard": {"500"}, "chunk_rows": {"2000"}, "windows": {"22:00-06:00, 01:00-02:00"},
		"governor_algorithm": {"grace-v2"}, "lag_slo": {"10s"}, "breaker_max_ratio": {"3"}, "replica_lag_max": {"30s"},
		"metrics": {"127.0.0.1:19470"}, "max_parallel": {"4"},
	})
	if code != 200 {
		t.Fatalf("create: %d %s", code, out)
	}
	for _, want := range []string{"soft: 250, hard: 500", "chunk_rows: 2000, max_parallel: 4", `windows: ["22:00-06:00", "01:00-02:00"]`,
		"algorithm: grace-v2", "lag_slo: 10s", "max_ratio: 3", "max: 30s"} {
		if !strings.Contains(y, want) {
			t.Errorf("yaml lacks %q:\n%s", want, y)
		}
	}
	// Chỉ đặt hard (không soft) vẫn ra YAML hợp lệ, không có "soft: 0".
	_, _, y2, _ := createNPForm(t, e, c, csrf, url.Values{
		"name": {"tun2"}, "from": {"mysql"}, "to": {"mysql"}, "source_host": {"10.0.0.1"}, "target_host": {"10.0.0.2"},
		"databases": {"shop"}, "iops_hard": {"800"}, "metrics": {"127.0.0.1:19471"},
	})
	if strings.Contains(y2, "soft:") || !strings.Contains(y2, "hard: 800") {
		t.Errorf("hard-only yaml:\n%s", y2)
	}
	for name, v := range map[string]url.Values{
		"governor_algorithm": {"governor_algorithm": {"turbo"}},
		"windows":            {"windows": {"noon"}},
		"lag_slo":            {"lag_slo": {"soon"}},
		"breaker_max_ratio":  {"breaker_max_ratio": {"0.5"}},
		"iops_hard":          {"iops_soft": {"500"}, "iops_hard": {"100"}},
		"max_parallel":       {"max_parallel": {"33"}},
	} {
		base := url.Values{"name": {"bad-" + strings.ReplaceAll(name, "_", "-")}, "from": {"mysql"}, "to": {"mysql"},
			"source_host": {"10.0.0.1"}, "target_host": {"10.0.0.2"}, "databases": {"shop"}}
		for k, x := range v {
			base[k] = x
		}
		code, _, _, out := createNPForm(t, e, c, csrf, base)
		if code != 400 || !strings.Contains(out, name) {
			t.Errorf("%s: %d %s, want 400", name, code, out)
		}
	}
}

func TestNewPipelinePageHasTuningSection(t *testing.T) {
	e := opsEnv(t, false, nil)
	c, _ := e.loginOK()
	_, page := e.do(c, "GET", "/pipelines/new", "", nil)
	for _, want := range []string{"Snapshot speed", `name="iops_soft"`, `name="iops_hard"`, `name="chunk_rows"`, `name="max_parallel"`, `name="windows"`,
		`name="governor_algorithm"`, `name="breaker_max_ratio"`, `name="replica_lag_max"`, `placeholder="default: 600"`} {
		if !strings.Contains(page, want) {
			t.Errorf("new pipeline page lacks %q", want)
		}
	}
}
