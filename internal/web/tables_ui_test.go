package web

import (
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

func TestTablesViewOrderAndAggregates(t *testing.T) {
	v := TablesView{Tables: []admin.TableInfo{
		{Table: "z.done1", State: "done", RowsEstimate: 100},
		{Table: "b.pend", State: "pending", RowsEstimate: 50},
		{Table: "a.run2", State: "running", RowsEstimate: 1000, RowsCopied: 400, ProgressPct: 40},
		{Table: "a.done0", State: "done", RowsEstimate: 300},
		{Table: "a.pend", State: "pending"},
		{Table: "a.run1", State: "running", RowsEstimate: 10, RowsCopied: 20}, // estimate below copied: copied wins
	}}
	v.finish()
	var got []string
	for _, x := range v.Tables {
		got = append(got, x.Table)
	}
	if want := "a.run1 a.run2 a.pend b.pend a.done0 z.done1"; strings.Join(got, " ") != want {
		t.Fatalf("order %v, want %s", got, want)
	}
	if v.Done != 2 || v.Running != 2 || v.Pending != 2 || v.Total != 6 {
		t.Fatalf("counts %+v", v)
	}
	// done tables count as fully copied: 400 + 20 + 100 + 300 = 820 of 1000+20+50+100+300 = 1470
	if v.RowsCopied != 820 || v.RowsEstimate != 1470 {
		t.Fatalf("rows %d / %d", v.RowsCopied, v.RowsEstimate)
	}
	if v.TablesPct < 33.3 || v.TablesPct > 33.4 || v.RowsPct < 55.7 || v.RowsPct > 55.9 {
		t.Fatalf("pct tables %.2f rows %.2f", v.TablesPct, v.RowsPct)
	}
	empty := TablesView{}
	empty.finish()
	if empty.Total != 0 || empty.TablesPct != 0 || empty.RowsPct != 0 {
		t.Fatalf("empty %+v", empty)
	}
}

func TestKeyKindLabels(t *testing.T) {
	for _, c := range []struct {
		in         admin.TableInfo
		kind, text string
	}{
		{admin.TableInfo{PK: []string{"id"}, Key: "PRIMARY (id)"}, "pk", "PK"},
		{admin.TableInfo{PK: []string{"id"}}, "pk", "PK"},
		{admin.TableInfo{PK: []string{"a", "b"}, Key: "UNIQUE uk (a, b)"}, "unique", "UNIQUE"},
		{admin.TableInfo{Key: "full row (keyless)"}, "keyless", "KEYLESS"},
		{admin.TableInfo{}, "unknown", "?"},
	} {
		if k := keyKind(c.in); k.Kind != c.kind || k.Label != c.text {
			t.Errorf("%+v: got %+v, want %s/%s", c.in, k, c.kind, c.text)
		}
	}
	if k := keyKind(admin.TableInfo{PK: []string{"code"}, GenericPK: true}); !strings.Contains(k.Detail, "generic key") {
		t.Errorf("generic key not shown: %+v", k)
	}
}

func TestTablesTabMarkup(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	resp, body := e.do(c, "GET", "/pipelines/orders/tables", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	for _, want := range []string{
		`id="snapshot-summary"`, `id="snap-tables"`, `1 <small>of 3 done`, `id="tables-table"`,
		`data-tfilter="all"`, `data-tfilter="running"`, `data-tfilter="pending"`, `data-tfilter="done"`,
		`id="tb-filter"`, `id="tb-sort"`, `class="keytag pk"`, `data-state="running"`,
		`aria-label="Tables done: 33.3%"`, `class="trow is-running"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("tables tab lacks %q", want)
		}
	}
	// running first, then pending, then done
	i, j, k := strings.Index(body, `data-table="app.orders"`), strings.Index(body, `data-table="app.codes"`), strings.Index(body, `data-table="app.users"`)
	if !(i > 0 && i < j && j < k) {
		t.Errorf("row order must be running, pending, done: %d %d %d", i, j, k)
	}
	// the done row exposes its state for the filter and no invented "finished at" column exists
	if strings.Contains(body, "Finished at") {
		t.Error("the admin API has no finish time: the page must not invent one")
	}
}

func TestHelpersFmtRelAndEngines(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	for in, want := range map[time.Duration]string{5 * time.Second: "5 s ago", 5 * time.Minute: "5 min ago", 3 * time.Hour: "3 h ago", 72 * time.Hour: "3 d ago"} {
		if got := fmtRel(now.Add(-in), now); got != want {
			t.Errorf("%v: %q, want %q", in, got, want)
		}
	}
	if fmtRel(time.Time{}, now) != "-" {
		t.Error("zero time")
	}
	e := engines("mysql → postgres")
	if e[0].Label != "MySQL" || e[1].Label != "PostgreSQL" || e[1].Class != "postgres" {
		t.Errorf("engines %+v", e)
	}
}

func TestOverviewAttentionAndKPIs(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	_, body := e.do(c, "GET", "/", "", nil)
	for _, want := range []string{`class="kpis"`, "Needs attention", "Total rows/s", "Everything is running normally", `/static/icons.svg#i-overview`} {
		if !strings.Contains(body, want) {
			t.Errorf("overview lacks %q", want)
		}
	}
	b := overviewBody{Views: []PipelineView{
		{Name: "a", State: StateFailed},
		{Name: "b", State: StateSynced},
		{Name: "c", State: StatePaused},
		{Name: "d", State: StateSynced, Top: &admin.TopInfo{BreakerOpen: true, TablesDone: 2, TablesTotal: 5}},
		{Name: "e", State: StateBootstrapping, Top: &admin.TopInfo{TablesDone: 1, TablesTotal: 5}},
	}}
	b.summarize()
	if len(b.Attn) != 3 || b.Healthy != 1 || b.TblDone != 3 || b.TblTotal != 10 || b.TblPct != 30 {
		t.Fatalf("summary %+v", b)
	}
}

func TestThemeAssetsServed(t *testing.T) {
	e := newEnv(t, nil)
	c := e.client()
	for _, p := range []string{"/static/icons.svg", "/static/theme.js", "/static/app.css"} {
		resp, body := e.do(c, "GET", p, "", nil)
		if resp.StatusCode != 200 || len(body) < 50 {
			t.Errorf("%s: %d (%d bytes)", p, resp.StatusCode, len(body))
		}
	}
	_, css := e.do(c, "GET", "/static/app.css", "", nil)
	for _, want := range []string{"prefers-color-scheme: dark", `[data-theme="dark"]`, "--accent"} {
		if !strings.Contains(css, want) {
			t.Errorf("css lacks %q", want)
		}
	}
}
