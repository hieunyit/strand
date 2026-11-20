package web

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"strand/internal/backup/conncheck"
	"strand/internal/web/store"
)

// ---- state map ----

func TestStateMapSpec(t *testing.T) {
	// SPEC 3.1: success/ok/complete -> success; running/pending -> info; warning/stale/partial/missed -> warning;
	// failed/error/rpo_breach -> danger; paused/disabled/expired -> neutral.
	want := map[string]string{
		"success": ToneSuccess, "ok": ToneSuccess, "complete": ToneSuccess, "synced": ToneSuccess,
		"running": ToneInfo, "pending": ToneInfo, "bootstrapping": ToneInfo, "starting": ToneInfo,
		"warning": ToneWarning, "stale": ToneWarning, "partial": ToneWarning, "missed": ToneWarning,
		"failed": ToneDanger, "error": ToneDanger, "rpo_breach": ToneDanger, "unreachable": ToneDanger,
		"paused": ToneNeutral, "disabled": ToneNeutral, "expired": ToneNeutral, "stopped": ToneNeutral, "unknown": ToneNeutral,
		"": ToneNeutral, "something-new": ToneNeutral,
	}
	for in, tone := range want {
		if got := stateTone(in); got != tone {
			t.Errorf("stateTone(%q) = %q, want %q", in, got, tone)
		}
	}
	if stateTone(" FAILED ") != ToneDanger || stateClass("Synced") != "st-synced" || stateClass("whatever") != "st-neutral" {
		t.Errorf("normalisation: %q %q %q", stateTone(" FAILED "), stateClass("Synced"), stateClass("whatever"))
	}
	// Jobs map through the same table.
	zero, one := 0, 1
	for j, cls := range map[string]JobStatus{
		"st-running": {Running: true}, "st-cancelling": {Running: true, Cancelled: true}, "st-success": {ExitCode: &zero},
		"st-failed": {ExitCode: &one}, "st-cancelled": {Cancelled: true}, "st-unknown": {},
	} {
		if got := jobClass(cls); got != j {
			t.Errorf("jobClass(%+v) = %q, want %q", cls, got, j)
		}
	}
}

// app.css carries one rule per tone; every key of the Go map must sit in the rule of its tone and in no other.
func TestStateMapMatchesCSS(t *testing.T) {
	b, err := os.ReadFile("static/app.css")
	if err != nil {
		t.Fatal(err)
	}
	ruleRE := regexp.MustCompile(`(?m)^([^{\n]+)\{[^}\n]*\}\s*/\* tone:(\w+) \*/`)
	inTone := map[string]map[string]bool{}
	for _, m := range ruleRE.FindAllStringSubmatch(string(b), -1) {
		set := map[string]bool{}
		for _, sel := range strings.Split(m[1], ",") {
			if sel = strings.TrimSpace(sel); strings.HasPrefix(sel, ".st-") {
				set[strings.TrimPrefix(sel, ".st-")] = true
			}
		}
		inTone[m[2]] = set
	}
	for _, tone := range []string{ToneSuccess, ToneInfo, ToneWarning, ToneDanger, ToneNeutral} {
		if len(inTone[tone]) == 0 {
			t.Fatalf("app.css has no `/* tone:%s */` rule", tone)
		}
	}
	for state, tone := range stateTones {
		for other, set := range inTone {
			if has := set[state]; has != (other == tone) {
				t.Errorf("state %q (tone %s): present in the %s rule = %v", state, tone, other, has)
			}
		}
	}
	for tone, set := range inTone {
		for state := range set {
			if stateTones[state] != tone {
				t.Errorf("app.css lists .st-%s under tone %s but state_map.go says %q", state, tone, stateTones[state])
			}
		}
	}
}

func TestFmtDur(t *testing.T) {
	for in, want := range map[float64]string{0: "-", 0.4: "400 ms", 42: "42 s", 245: "4m 05s", 3900: "1h 05m", 183600: "2d 03h"} {
		if got := fmtDur(in); got != want {
			t.Errorf("fmtDur(%v) = %q, want %q", in, got, want)
		}
	}
}

// ---- role matrix ----

// roleSession logs in as a new user with the role (database mode) and returns the client and CSRF token.
func roleSession(t *testing.T, e *testEnv, st store.Store, role store.Role) (*http.Client, string) {
	t.Helper()
	pw := "SecurePassword123!"
	h, err := HashPassword(pw)
	if err != nil {
		t.Fatal(err)
	}
	name := "u_" + string(role)
	if _, err := st.CreateUser(context.Background(), store.User{Username: name, PasswordHash: h, Role: role, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	c := e.client()
	e.do(c, "GET", "/login", "", nil)
	var lc string
	for _, ck := range c.Jar.Cookies(nil) {
		if ck.Name == loginCookie {
			lc = ck.Value
		}
	}
	resp, body := e.do(c, "POST", "/api/v1/login", `{"username":"`+name+`","password":"`+pw+`"}`, map[string]string{csrfHeader: lc, "Content-Type": "application/json"})
	if resp.StatusCode != 200 {
		t.Fatalf("login %s: %d %s", name, resp.StatusCode, body)
	}
	var raw map[string]any
	_ = json.Unmarshal([]byte(body), &raw)
	csrf, _ := raw["csrf"].(string)
	return c, csrf
}

type pipePage struct {
	path       string
	needsWrite []string // markers only roles with write rights may see
}

func TestPipelinePagesRoleMatrix(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, func(o *Options) { o.ConnChecker = &fakeChecker{result: conncheck.Result{OK: true}} })
	e.sampleNow()
	admin, acsrf := e.loginOK()
	e.hold(true)
	if code, body := e.act(admin, acsrf, "orders", ActVerify, map[string]string{"mode": "full"}); code != 200 {
		t.Fatalf("start job: %d %v", code, body)
	}
	oper, _ := roleSession(t, e, st, store.RoleOperator)
	view, _ := roleSession(t, e, st, store.RoleViewer)

	// Markers of controls, by the minimum role that may use them.
	operatorOnly := []string{`data-action="start"`, `data-action="stop"`, `data-action="job-cancel"`, `data-bulk="start"`, `class="rowcheck"`, `data-action="verify"`, `data-action="pause"`, `data-action="throttle"`}
	adminOnly := []string{`data-action="delete"`, `href="/pipelines/new"`, `id="cfg-btn-toggle"`, `data-action="schema-diff"`, `data-action="objects-list"`, `data-action="rewind"`, `data-action="checkpoint-rewind"`, `id="env-edit-form"`, `id="tuning-form"`}
	pages := []string{
		"/", "/pipelines", "/jobs", "/pipelines/orders", "/pipelines/orders/tables", "/pipelines/orders/checkpoint", "/pipelines/orders/errors",
		"/pipelines/orders/config", "/pipelines/orders/logs", "/pipelines/orders/jobs", "/pipelines/orders/schema", "/pipelines/orders/objects",
		"/pipelines/orders/repair", "/pipelines/orders/promote", "/pipelines/orders/seed",
	}
	has := func(body string, markers []string) []string {
		var out []string
		for _, m := range markers {
			if strings.Contains(body, m) {
				out = append(out, m)
			}
		}
		return out
	}
	seenOp, seenAdmin := map[string]bool{}, map[string]bool{}
	for _, p := range pages {
		for role, c := range map[string]*http.Client{"admin": admin, "operator": oper, "viewer": view} {
			resp, body := e.do(c, "GET", p, "", nil)
			if resp.StatusCode != 200 {
				t.Errorf("%s GET %s: %d", role, p, resp.StatusCode)
				continue
			}
			op, ad := has(body, operatorOnly), has(body, adminOnly)
			for _, m := range op {
				seenOp[m] = true
			}
			for _, m := range ad {
				seenAdmin[m] = true
			}
			switch role {
			case "viewer":
				if len(op)+len(ad) > 0 {
					t.Errorf("viewer sees write controls on %s: %v %v", p, op, ad)
				}
			case "operator":
				if len(ad) > 0 {
					t.Errorf("operator sees admin controls on %s: %v", p, ad)
				}
			}
			// Every page keeps the shared skeleton, in every role.
			for _, want := range []string{`class="breadcrumb"`, `class="pagehead"`, "<h1>"} {
				if !strings.Contains(body, want) {
					t.Errorf("%s %s lacks %s", role, p, want)
				}
			}
			if n := strings.Count(body, `<table`); n != strings.Count(body, `<caption`) {
				t.Errorf("%s %s: %d tables but %d captions", role, p, n, strings.Count(body, `<caption`))
			}
			if strings.Count(between(body, `class="pagehead-actions"`, `</header>`), `btn primary`) > 1 {
				t.Errorf("%s %s: more than one primary action in the page header", role, p)
			}
		}
	}
	// The matrix must actually have exercised the controls for the roles that own them.
	for _, m := range []string{`data-action="start"`, `data-action="stop"`, `data-action="job-cancel"`, `data-bulk="start"`, `class="rowcheck"`, `data-action="verify"`, `data-action="throttle"`} {
		if !seenOp[m] {
			t.Errorf("no page showed %s to admin/operator", m)
		}
	}
	for _, m := range []string{`data-action="delete"`, `href="/pipelines/new"`, `id="cfg-btn-toggle"`, `data-action="schema-diff"`, `data-action="objects-list"`, `id="env-edit-form"`, `id="tuning-form"`} {
		if !seenAdmin[m] {
			t.Errorf("no page showed %s to admin", m)
		}
	}
	// Operators may start and stop but not delete.
	_, ob := e.do(oper, "GET", "/pipelines/orders", "", nil)
	if !strings.Contains(ob, `data-action="start"`) || strings.Contains(ob, `data-action="delete"`) {
		t.Error("operator detail page: want start, no delete")
	}
	e.hold(false)
}

func between(s, a, b string) string {
	i := strings.Index(s, a)
	if i < 0 {
		return ""
	}
	s = s[i:]
	if j := strings.Index(s, b); j >= 0 {
		return s[:j]
	}
	return s
}

// ---- page structure ----

func TestPipelineListFilterBulkAndFormatting(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c, _ := e.loginOK()
	_, body := e.do(c, "GET", "/pipelines", "", nil)
	for _, want := range []string{
		`class="filterbar"`, `id="pl-filter"`, `id="pl-kind"`, `id="pl-state"`, `id="pl-clear"`, `class="bulkbar"`, `data-bulk="start"`, `data-bulk="stop"`,
		`id="pl-selall"`, `class="rowcheck"`, `data-kinds="mysql postgres"`, `<caption class="sr-only">`, `class="rowmenu"`, `aria-label="Actions for orders"`,
		`<option value="mysql">mysql</option>`, `<option value="postgres">postgres</option>`, `data-retype="orders"`, "0.25 s", `class="btn primary" href="/pipelines/new"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/pipelines lacks %q", want)
		}
	}
}

func TestPipelineDetailLayoutAndTiles(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c, _ := e.loginOK()
	for _, p := range []string{"/pipelines/orders", "/pipelines/orders/tables", "/pipelines/orders/logs"} {
		_, body := e.do(c, "GET", p, "", nil)
		for _, want := range []string{`class="detail-layout"`, `class="detail-main"`, `<dl class="props">`, "<dt>Config revision</dt>", "<dt>Source</dt>", "<dt>Managed by</dt>", `aria-label="Pipeline properties"`} {
			if !strings.Contains(body, want) {
				t.Errorf("%s lacks %q", p, want)
			}
		}
	}
	_, body := e.do(c, "GET", "/pipelines/orders", "", nil)
	// Live metrics are stat tiles that keep the data-bind names live.js updates.
	for _, want := range []string{`class="kpis" aria-label="Live metrics"`, `class="kpi"`, `data-bind="lag"`, `data-bind="rows"`, `data-bind="txn"`, `data-bind="uptime"`, `data-bind="ckpt"`, `data-bind-badge="state"`} {
		if !strings.Contains(body, want) {
			t.Errorf("detail lacks %q", want)
		}
	}
	if strings.Contains(body, `class="metrics"`) {
		t.Error("old .metrics strip still rendered")
	}
}

func TestJobsPageFiltersDurationAndCancel(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	e.sampleNow()
	admin, acsrf := e.loginOK()
	e.hold(true)
	if code, body := e.act(admin, acsrf, "orders", ActVerify, map[string]string{"mode": "full"}); code != 200 {
		t.Fatalf("start job: %d %v", code, body)
	}
	oper, _ := roleSession(t, e, st, store.RoleOperator)
	view, _ := roleSession(t, e, st, store.RoleViewer)
	_, ob := e.do(oper, "GET", "/jobs", "", nil)
	for _, want := range []string{`class="filterbar"`, `id="pl-kind"`, `id="pl-state"`, `id="pl-pipeline"`, `<option value="orders">orders</option>`, `<option value="verify">verify</option>`,
		`id="jobs-table" data-filterable`, `<th scope="col" class="r">Duration</th>`, `data-state="running"`, `data-action="job-cancel"`, `class="badge st-running"`} {
		if !strings.Contains(ob, want) {
			t.Errorf("operator /jobs lacks %q", want)
		}
	}
	_, vb := e.do(view, "GET", "/jobs", "", nil)
	if strings.Contains(vb, `data-action="job-cancel"`) || !strings.Contains(vb, `data-state="running"`) {
		t.Error("viewer /jobs: want the running job without a Cancel button")
	}
	e.hold(false)
}

// ---- Test connection ----

func TestPipelineConnectionTest(t *testing.T) {
	st := store.NewMem()
	fake := &fakeChecker{result: conncheck.Result{OK: true, Version: "8.4.0", MissingPrivileges: []string{}, Databases: []string{"app"}, Checks: []conncheck.Check{{Name: "login", Status: "ok"}}}}
	e := dbEnv(t, st, nil, func(o *Options) { o.ConnChecker = fake })
	admin, acsrf := e.loginOK()
	oper, ocsrf := roleSession(t, e, st, store.RoleOperator)
	view, vcsrf := roleSession(t, e, st, store.RoleViewer)
	post := func(c *http.Client, csrf, body string) (int, string) {
		resp, b := e.do(c, "POST", "/api/v1/pipeline-connections/test", body, map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
		return resp.StatusCode, b
	}
	const secret = "np-canary-p4ss-77"
	ok := `{"role":"source","engine":"mysql","host":"db1","port":3306,"user":"strand","password":"` + secret + `"}`
	for name, c := range map[string][2]any{"admin": {admin, acsrf}, "operator": {oper, ocsrf}} {
		code, out := post(c[0].(*http.Client), c[1].(string), ok)
		if code != 200 || !regexp.MustCompile(`"ok":\s*true`).MatchString(out) || strings.Contains(out, secret) {
			t.Errorf("%s: %d %s", name, code, out)
		}
	}
	if got := fake.last(); got.Password != secret || got.Host != "db1" || got.Engine != "mysql" {
		t.Errorf("checker target %+v", got)
	}
	if code, _ := post(view, vcsrf, ok); code != http.StatusForbidden {
		t.Errorf("viewer: %d, want 403", code)
	}
	if code, _ := post(e.client(), "", ok); code != http.StatusUnauthorized && code != http.StatusForbidden && code != http.StatusFound {
		t.Errorf("anonymous: %d", code)
	}
	// Mongo takes a URI; its secret is not echoed or audited either.
	uri := "mongodb://u:" + secret + "@m1:27017/?replicaSet=rs0"
	if code, out := post(oper, ocsrf, `{"role":"target","engine":"mongodb","uri":"`+uri+`"}`); code != 200 || strings.Contains(out, secret) {
		t.Errorf("mongo: %d %s", code, out)
	}
	if got := fake.last(); got.URI != uri {
		t.Errorf("mongo target %+v", got)
	}
	for _, bad := range []string{`{"role":"x","engine":"mysql","host":"h","user":"u"}`, `{"role":"source","engine":"oracle","host":"h","user":"u"}`,
		`{"role":"source","engine":"mysql","host":"","user":"u"}`, `{"role":"source","engine":"mysql","host":"h","user":"u","port":70000}`,
		`{"role":"source","engine":"mongodb"}`, `{"role":"source","engine":"mysql","host":"h","user":"u","extra":1}`} {
		if code, _ := post(oper, ocsrf, bad); code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", bad, code)
		}
	}
	// Audited, without any secret.
	raw, err := os.ReadFile(e.srv.Audit().Path())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "pipeline.test_connection") || strings.Contains(string(raw), secret) {
		t.Errorf("audit log: want pipeline.test_connection and no secret:\n%s", raw)
	}
}

// ---- create errors carry the field ----

func TestCreateErrorsNameTheField(t *testing.T) {
	for msg, want := range map[string]string{
		`invalid pipeline name "a b" (allowed: letters)`:                           "name",
		`pipeline "x" already exists in registry (check 'Overwrite' to update it)`: "name",
		"iops_soft: 5 is not an integer between 1 and 10000000":                    "iops_soft",
		"windows: bad window":                                "windows",
		`target_database "a-b" is not a plain database name`: "target_database",
		"PostgreSQL can only be copied to PostgreSQL (postgres -> mysql is not supported)": "to",
		"something unrelated": "",
	} {
		if got := createErrField(msg); got != want {
			t.Errorf("createErrField(%q) = %q, want %q", msg, got, want)
		}
	}
	e := newEnv(t, nil)
	c, csrf := e.loginOK()
	resp, body := e.do(c, "POST", "/api/v1/pipelines", `{"name":"bad name"}`, map[string]string{csrfHeader: csrf, "Content-Type": "application/json"})
	var out struct {
		Error  string            `json:"error"`
		Fields map[string]string `json:"fields"`
	}
	_ = json.Unmarshal([]byte(body), &out)
	if resp.StatusCode != 400 || out.Fields["name"] == "" || out.Error == "" {
		t.Errorf("create with a bad name: %d %s", resp.StatusCode, body)
	}
}

// ---- new pipeline wizard markup ----

func TestNewPipelineWizardMarkup(t *testing.T) {
	e := newEnv(t, nil)
	c, _ := e.loginOK()
	_, body := e.do(c, "GET", "/pipelines/new", "", nil)
	for _, want := range []string{
		`<ol class="stepper" id="np-stepper"`, `aria-current="step"`, `data-step="1"`, `data-step="2"`, `data-step="3"`, `data-step="4"`,
		`id="np-test-source"`, `id="np-test-target"`, `id="np-summary"`, `id="np-review"`, `data-guard`, `id="btn-create-pipeline"`,
		`id="np-back"`, `id="np-next"`, `/static/pipelines.js`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("wizard lacks %q", want)
		}
	}
	// Every step has the four labelled stages in order.
	i1, i2, i3, i4 := strings.Index(body, ">Source<"), strings.Index(body, ">Target<"), strings.Index(body, ">Tables &amp; options<"), strings.Index(body, ">Review<")
	if !(i1 > 0 && i1 < i2 && i2 < i3 && i3 < i4) {
		t.Errorf("stepper order: %d %d %d %d", i1, i2, i3, i4)
	}
}

func TestToastAndDialogContract(t *testing.T) {
	b, err := os.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(b)
	// SPEC 3.1: a success toast hides after 4 s, an error stays until it is closed; the close button is labelled.
	for _, want := range []string{"}, 4000)", "Dismiss notification", "if (ok) { toastTimer"} {
		if !strings.Contains(js, want) {
			t.Errorf("app.js lacks %q", want)
		}
	}
}
