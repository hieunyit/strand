package web

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
)

func tabLinks(body string) []string {
	navigation := between(body, `aria-label="Pipeline sections">`, `</nav>`) + between(body, `id="pipeline-tab-help"`, `</details>`)
	var out []string
	for _, k := range []string{"graph", "tables", "checkpoint", "errors", "config", "logs", "jobs", "schema", "objects", "repair", "promote", "seed"} {
		href := `href="/pipelines/orders/` + k + `"`
		if k == "graph" {
			href = `href="/pipelines/orders"`
		}
		if strings.Contains(navigation, href) {
			out = append(out, k)
		}
	}
	return out
}

// Pipeline tabs follow the source and target: MySQL -> PostgreSQL has no Objects or
// Repair (strand objects and verify --repair fix refuse it), MySQL -> MySQL has every
// tab, a MongoDB pipeline only the tabs that work for it. A hidden tab's URL goes to the
// pipeline, and the operations guide lists only the shown tabs.
func TestPipelineTabsFollowThePipelineKind(t *testing.T) {
	e := opsEnv(t, false, nil)
	wizFake(t, e)
	e.sampleNow()
	c, csrf := e.loginOK()
	get := func(path string) (int, string) {
		resp, body := e.do(c, "GET", path, "", nil)
		return resp.StatusCode, body
	}
	_, body := get("/pipelines/orders")
	if got := strings.Join(tabLinks(body), ","); got != "graph,tables,checkpoint,errors,config,logs,jobs,schema,promote,seed" {
		t.Errorf("mysql -> postgres tabs: %s", got)
	}
	nc := &http.Client{Jar: c.Jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for _, path := range []string{"/pipelines/orders/objects", "/pipelines/orders/repair"} {
		resp, _ := e.do(nc, "GET", path, "", nil)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/pipelines/orders" {
			t.Errorf("%s: %d %q, want a redirect to the pipeline", path, resp.StatusCode, resp.Header.Get("Location"))
		}
	}

	e.ordersMySQLTarget()
	_, body = get("/pipelines/orders")
	if got := strings.Join(tabLinks(body), ","); got != "graph,tables,checkpoint,errors,config,logs,jobs,schema,objects,repair,promote,seed" {
		t.Errorf("mysql -> mysql tabs: %s", got)
	}
	for _, want := range []string{"Assembler", "Scheduler", "Holes and writeset", `data-chart="spool"`, "<dt>Governor</dt>", `id="throttle"`} {
		if !strings.Contains(body, want) {
			t.Errorf("SQL graph tab lacks %q", want)
		}
	}

	p := e.srv.reg.Get("orders")
	mongo := fmt.Sprintf(`version: 1
name: orders
source: { kind: mongodb, dsn: "mongodb://s1:27017/?replicaSet=rs0", databases: [shop, billing] }
target: { kind: mongodb, dsn: "mongodb://t1:27017/?replicaSet=rs1" }
tables: { include: ["shop.*", "billing.*"] }
resources: { spool: { path: %s } }
observability: { admin_socket: %s }
`, p.Cfg.Resources.Spool.Path, p.Cfg.Observability.AdminSocket)
	e.writePipelineFile("orders", p.ConfigPath, mongo, 0o640)
	if p := e.srv.reg.Get("orders"); p.CfgErr != "" {
		t.Fatalf("mongo config: %s", p.CfgErr)
	}
	_, body = get("/pipelines/orders")
	if got := strings.Join(tabLinks(body), ","); got != "graph,tables,checkpoint,errors,config,logs,jobs" {
		t.Errorf("mongodb tabs: %s", got)
	}
	for _, gone := range []string{"Preview table definitions", "List MySQL views", "MySQL-to-MySQL pipeline", "remote clone"} {
		if strings.Contains(body, gone) {
			t.Errorf("operations guide of a MongoDB pipeline mentions %q", gone)
		}
	}
	if !strings.Contains(body, "change stream resume token") {
		t.Errorf("operations guide lacks the MongoDB checkpoint help")
	}
	// Graph tab: the MongoDB data path (copy workers, change stream) without the SQL
	// stages, governor, throttle (refused by MongoDB pipelines) and holes/spool charts.
	for _, gone := range []string{"Assembler", "Scheduler", "Holes and writeset", `data-chart="spool"`, "<dt>Governor</dt>", `id="throttle"`} {
		if strings.Contains(body, gone) {
			t.Errorf("MongoDB graph tab shows %q", gone)
		}
	}
	for _, want := range []string{"Change stream", "Copy x", `data-chart="rate"`, `data-chart="lag"`} {
		if !strings.Contains(body, want) {
			t.Errorf("MongoDB graph tab lacks %q", want)
		}
	}
	for _, path := range []string{"/pipelines/orders/schema", "/pipelines/orders/objects", "/pipelines/orders/promote", "/pipelines/orders/seed", "/pipelines/orders/repair"} {
		resp, _ := e.do(nc, "GET", path, "", nil)
		if resp.StatusCode != http.StatusSeeOther {
			t.Errorf("%s on a MongoDB pipeline: %d", path, resp.StatusCode)
		}
	}
	code, jobs := get("/pipelines/orders/jobs")
	if code != 200 {
		t.Fatalf("jobs: %d", code)
	}
	for _, gone := range []string{`value="delta"`, `name="resume"`, "schema.table", "Approve a schema change", "Preview DDL"} {
		if strings.Contains(jobs, gone) {
			t.Errorf("MongoDB jobs tab shows %q", gone)
		}
	}
	for _, want := range []string{"database.collection", "Sample size", `data-do="plan"`} {
		if !strings.Contains(jobs, want) {
			t.Errorf("MongoDB jobs tab lacks %q", want)
		}
	}
	if _, cp := get("/pipelines/orders/checkpoint"); strings.Contains(cp, "Rewind is available") {
		t.Error("MongoDB checkpoint tab mentions MySQL rewind")
	}

	// Verify of a MongoDB pipeline: every collection, or one database.collection; the
	// sample size is a document count.
	for _, tc := range []struct {
		args map[string]string
		code int
		want string
	}{
		{map[string]string{"mode": "full"}, 200, "--mode full"},
		{map[string]string{"mode": "sample", "rate": "500"}, 200, "--mode sample --rate=500"},
		{map[string]string{"mode": "full", "table": "billing.invoices.2026"}, 200, "--table=billing.invoices.2026"},
		{map[string]string{"mode": "delta"}, 400, ""},
		{map[string]string{"mode": "full", "rate": "500"}, 400, ""},
		{map[string]string{"mode": "sample", "rate": "0.1"}, 400, ""},
		{map[string]string{"mode": "full", "resume": "true"}, 400, ""},
		{map[string]string{"mode": "full", "table": "invoices"}, 400, ""},
	} {
		before := len(e.fakeCalls())
		code, out := e.act(c, csrf, "orders", ActVerify, tc.args)
		if code != tc.code {
			t.Errorf("%v: %d %v", tc.args, code, out)
			continue
		}
		if tc.code != 200 {
			continue
		}
		waitFor(t, "verify job", func() bool { return len(e.fakeCalls()) > before })
		args := strings.Join(e.fakeCalls()[before].Args, " ")
		if !strings.Contains(args, tc.want) {
			t.Errorf("%v: argv %q lacks %q", tc.args, args, tc.want)
		}
		waitFor(t, "verify job end", func() bool {
			for _, j := range e.srv.jobs.List("orders") {
				if j.Running {
					return false
				}
			}
			return true
		})
	}
}

// After a successful create the console opens the new pipeline (and shows the notice
// there) instead of staying on the New pipeline page.
func TestCreatePipelineOpensThePipeline(t *testing.T) {
	js, err := os.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	s := string(js)
	for _, want := range []string{"window.location.assign('/pipelines/' + encodeURIComponent(res.j.pipeline))", "sessionStorage.setItem('strand-flash'", "showFlash();"} {
		if !strings.Contains(s, want) {
			t.Errorf("app.js lacks %q", want)
		}
	}
}
