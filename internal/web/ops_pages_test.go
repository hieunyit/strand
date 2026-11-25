package web

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"

	"strand/internal/admin"
)

// CSP is "default-src 'self'" (no 'unsafe-inline'): browsers ignore style="..." attributes, so none may be used.
func TestNoInlineStyles(t *testing.T) {
	inline := regexp.MustCompile(`(?i)\sstyle\s*=`)
	err := fs.WalkDir(templateFS, "templates", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, _ := templateFS.ReadFile(p)
		if inline.Match(b) {
			t.Errorf("%s contains an inline style attribute (blocked by the CSP); use a class in app.css", p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	js, err := staticFS.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	if regexp.MustCompile(`style\s*=\s*\\?["']|setAttribute\(\s*['"]style`).Match(js) {
		t.Error("app.js builds style= strings or sets the style attribute (blocked by the CSP)")
	}
}

func TestPageTitlesPerTab(t *testing.T) {
	e := newEnv(t, nil)
	e.ordersMySQLTarget()
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	for path, want := range map[string]string{
		"/":                            "Overview \u00b7 Strand",
		"/pipelines":                   "Pipelines \u00b7 Strand",
		"/jobs":                        "Jobs \u00b7 Strand",
		"/pipelines/orders":            "orders \u00b7 Graph \u00b7 Strand",
		"/pipelines/orders/tables":     "orders \u00b7 Tables \u00b7 Strand",
		"/pipelines/orders/checkpoint": "orders \u00b7 Checkpoint \u00b7 Strand",
		"/pipelines/orders/errors":     "orders \u00b7 Errors \u00b7 Strand",
		"/pipelines/orders/config":     "orders \u00b7 Config \u00b7 Strand",
		"/pipelines/orders/logs":       "orders \u00b7 Logs \u00b7 Strand",
		"/pipelines/orders/jobs":       "orders \u00b7 Jobs \u00b7 Strand",
		"/pipelines/orders/repair":     "orders \u00b7 Repair \u00b7 Strand",
		"/pipelines/orders/promote":    "orders \u00b7 Promote \u00b7 Strand",
	} {
		resp, body := e.do(c, "GET", path, "", nil)
		if resp.StatusCode != 200 {
			t.Errorf("%s: status %d", path, resp.StatusCode)
			continue
		}
		if !strings.Contains(body, "<title>"+want+"</title>") {
			t.Errorf("%s: title not %q", path, want)
		}
	}
}

func TestPagesShowLastUpdatedAndState(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	// Live pages distinguish render time from incoming telemetry. Static pages keep "Data as of".
	for path, refresh := range map[string]string{"/": "Last update", "/pipelines/orders": "Last update",
		"/pipelines/orders/config": "Last update", "/jobs": "refreshes every 5 s"} {
		_, body := e.do(c, "GET", path, "", nil)
		if !regexp.MustCompile(`(Data as of|Page rendered|page rendered) <time datetime="[^"]+">\d\d:\d\d:\d\d [^<]+</time>`).MatchString(body) || !strings.Contains(body, refresh) {
			t.Errorf("%s: no last-updated marker (%q)", path, refresh)
		}
	}
	_, body := e.do(c, "GET", "/pipelines/orders", "", nil)
	for _, want := range []string{`badge big st-synced" data-bind-badge="state">synced<`, "sampled <b", `aria-current="page"`, `data-live="orders"`} {
		if !strings.Contains(body, want) {
			t.Errorf("detail lacks %q", want)
		}
	}
	// Running pipelines show the action that applies to their current state.
	if !strings.Contains(body, `data-action="pause"`) || strings.Contains(body, `data-action="resume"`) {
		t.Error("synced pipeline should offer Pause and hide Resume")
	}
}

func TestSortViewsFailedFirst(t *testing.T) {
	top := func(l float64) *admin.TopInfo { return &admin.TopInfo{LagSeconds: l} }
	vs := []PipelineView{
		{Name: "a-ok", State: StateSynced, Top: top(1)},
		{Name: "b-slow", State: StateSynced, Top: top(90)},
		{Name: "c-stopped", State: StateStopped},
		{Name: "d-paused", State: StatePaused, Top: top(5)},
		{Name: "e-unreach", State: StateUnreachable},
		{Name: "z-failed", State: StateFailed},
	}
	sortViews(vs)
	var got []string
	for _, v := range vs {
		got = append(got, v.Name)
	}
	if want := "z-failed e-unreach d-paused c-stopped b-slow a-ok"; strings.Join(got, " ") != want {
		t.Fatalf("order %v, want %s", got, want)
	}
}

func TestOperatorFormatting(t *testing.T) {
	for in, want := range map[float64]string{0.4: "400 ms", 12.34: "12.3 s", 75: "1m 15s", 3725: "1h 02m 05s", 90000: "1d 1h 00m"} {
		if got := fmtSecs(in); got != want {
			t.Errorf("fmtSecs(%v) = %q, want %q", in, got, want)
		}
	}
	if got := fmtNum(1234567); got != "1,234,567" {
		t.Errorf("fmtNum = %q", got)
	}
	if got := fmtNum(12.34); got != "12.3" {
		t.Errorf("fmtNum small = %q", got)
	}
	if got := fmtInt(-1234); got != "-1,234" {
		t.Errorf("fmtInt = %q", got)
	}
	r := stateReason(PipelineView{State: StateFailed, Runtime: Runtime{LastError: "connection refused"}})
	if r != "connection refused" {
		t.Errorf("reason %q", r)
	}
	if stateReason(PipelineView{State: StateSynced}) != "" {
		t.Error("healthy pipeline has no reason")
	}
}
