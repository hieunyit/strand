package web

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
	"strand/internal/web/store"
)

// The per-table history: never-changed tables have no entry, the first sample is a baseline, a pipeline restart
// (counters back to 0) never gives a negative rate, buckets are capped and freed, the number of tables is capped.
func TestTableHistoryBoundsAndRestart(t *testing.T) {
	h := newPipeHist()
	t0 := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	tbl := func(name string, n int64, at time.Time) admin.TableInfo {
		ti := admin.TableInfo{Table: name, State: "done", Updates: n}
		if n > 0 {
			ti.LastChangeAt, ti.LastSourceAt = tp(at), tp(at.Add(-2*time.Second))
		}
		return ti
	}
	// First sample: 500 changes from before the Web watched: baseline only.
	h.update([]admin.TableInfo{tbl("a.busy", 500, t0.Add(-time.Minute)), tbl("a.never", 0, t0)}, t0)
	if n, s := h.histTracked(); n != 1 || s != 0 {
		t.Fatalf("after the baseline: %d entries, %d series (want 1, 0)", n, s)
	}
	// 30 minutes of 10 changes per minute, then the pipeline restarts (counter 300 -> 4), then 10 more minutes.
	total := int64(500)
	for m := 1; m <= 40; m++ {
		now := t0.Add(time.Duration(m) * time.Minute)
		switch {
		case m == 31:
			total = 4 // restarted process: counts again from 0
		default:
			total += 10
		}
		h.update([]admin.TableInfo{tbl("a.busy", total, now), tbl("a.never", 0, now)}, now)
	}
	pts, _ := h.tableHistory("a.busy", t0.Add(40*time.Minute))
	if len(pts) != 41 { // since the Web started (t0), one point per minute
		t.Fatalf("points: %d, want 41", len(pts))
	}
	for _, p := range pts {
		if p.Changes < 0 || p.Changes > 10 {
			t.Fatalf("point %v: %d changes (negative or over the real rate)", p.T, p.Changes)
		}
		if p.T.After(t0) && p.T.Before(t0.Add(40*time.Minute)) && p.Changes == 0 {
			t.Fatalf("missing changes at %v", p.T)
		}
	}
	if got := pts[31].Changes; got != 4 { // minute 31: the restart counts the 4 changes since it, not -306
		t.Fatalf("restart minute: %d changes, want 4", got)
	}
	if pts[0].Changes != 0 || pts[40].MaxDelay == nil || *pts[40].MaxDelay != 2 {
		t.Fatalf("baseline minute %d changes; sync delay of the last minute: %v", pts[0].Changes, pts[40].MaxDelay)
	}
	if pts, _ := h.tableHistory("a.never", t0.Add(40*time.Minute)); len(pts) != 41 || pts[5].Changes != 0 {
		t.Fatalf("never-changed table history: %+v", pts)
	}
	if n, s := h.histTracked(); n != 1 || s != 1 {
		t.Fatalf("a never-changed table got an entry: %d entries, %d series", n, s)
	}
	// Two hours later (no new change, more than the bucket window): the buckets are freed, the entry stays.
	now := t0.Add(3 * time.Hour)
	h.update([]admin.TableInfo{tbl("a.busy", total, t0.Add(40*time.Minute))}, now)
	if n, s := h.histTracked(); n != 1 || s != 0 {
		t.Fatalf("idle table keeps its buckets: %d entries, %d series", n, s)
	}
	if pts, _ := h.tableHistory("a.busy", now); len(pts) != histBuckets {
		t.Fatalf("window capped at %d points, got %d", histBuckets, len(pts))
	}
	// A table that left the list loses its entry.
	h.update([]admin.TableInfo{tbl("a.other", 0, now)}, now.Add(time.Minute))
	if n, _ := h.histTracked(); n != 0 {
		t.Fatalf("removed table kept: %d entries", n)
	}
	// At most histMaxTables tables get an entry.
	many := make([]admin.TableInfo, histMaxTables+50)
	for i := range many {
		many[i] = tbl(fmt.Sprintf("s.t%05d", i), 1, now)
	}
	h.update(many, now.Add(2*time.Minute))
	if n, _ := h.histTracked(); n != histMaxTables {
		t.Fatalf("entries: %d, want the cap %d", n, histMaxTables)
	}
}

// quietScenario feeds h one sample per minute for two hours: a.steady changes every minute and stops at
// minute 100, a.busy never stops, a.burst changes for 5 minutes only, a.copying changes but is in its initial
// copy, a.never never changes. It returns the quiet tables at each minute after 100.
func quietScenario(h *pipeHist, t0 time.Time) map[int]map[string]bool {
	out := map[int]map[string]bool{}
	var steady, busy, burst, copying int64
	var steadyAt, busyAt, burstAt, copyAt time.Time
	for m := 0; m <= 140; m++ {
		now := t0.Add(time.Duration(m) * time.Minute)
		if m < 100 {
			steady, steadyAt = steady+3, now
			copying, copyAt = copying+3, now
		}
		busy, busyAt = busy+5, now
		if m >= 20 && m < 25 {
			burst, burstAt = burst+50, now
		}
		at := func(n int64, t time.Time) *time.Time {
			if n == 0 {
				return nil
			}
			return tp(t)
		}
		q := h.update([]admin.TableInfo{
			{Table: "a.steady", State: "done", Inserts: steady, LastChangeAt: at(steady, steadyAt)},
			{Table: "a.busy", State: "done", Updates: busy, LastChangeAt: at(busy, busyAt)},
			{Table: "a.burst", State: "done", Deletes: burst, LastChangeAt: at(burst, burstAt)},
			{Table: "a.copying", State: "running", Inserts: copying, LastChangeAt: at(copying, copyAt)},
			{Table: "a.never", State: "done"},
		}, now)
		out[m] = q
	}
	return out
}

// The quiet rule flags a table that changed regularly and stopped, after max(10 × usual gap, 15 min); never a
// table that never changed, changed only in a burst, is still busy, or is being copied.
func TestQuietTableRule(t *testing.T) {
	t0 := time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC)
	h := newPipeHist()
	q := quietScenario(h, t0)
	for m := 0; m <= 114; m++ {
		if len(q[m]) != 0 {
			t.Fatalf("minute %d: quiet too early: %v", m, q[m])
		}
	}
	// a.steady: usual gap 60 s -> threshold max(600 s, 15 min) = 15 min after its last change (minute 99).
	for m := 115; m <= 140; m++ {
		if len(q[m]) != 1 || !q[m]["a.steady"] {
			t.Fatalf("minute %d: quiet = %v, want only a.steady", m, q[m])
		}
	}
	_, qi := h.tableHistory("a.steady", t0.Add(140*time.Minute))
	if !qi.Regular || qi.ActiveWindows < quietMinWindows || qi.UsualGapS != 60 || qi.QuietAfterS != quietFloor.Seconds() || qi.IdleS != 41*60 {
		t.Fatalf("quiet details: %+v", qi)
	}
	if _, qb := h.tableHistory("a.burst", t0.Add(140*time.Minute)); qb.Regular || qb.ActiveWindows > 1 {
		t.Fatalf("a burst counts as regular: %+v", qb)
	}
	// A pipeline restart (counters back to 0, last change carried) does not make a busy table quiet.
	h2 := newPipeHist()
	quietScenario(h2, t0)
	now := t0.Add(141 * time.Minute)
	last := t0.Add(140 * time.Minute)
	if q := h2.update([]admin.TableInfo{{Table: "a.busy", State: "done", LastChangeAt: tp(last)}}, now); q["a.busy"] {
		t.Fatal("restart flagged the busy table")
	}
	if q := h2.update([]admin.TableInfo{{Table: "a.busy", State: "done", Updates: 2, LastChangeAt: tp(now)}}, now.Add(time.Minute)); len(q) != 0 {
		t.Fatalf("after restart: %v", q)
	}
}

// quietEnv runs the quiet scenario through the real sampler and returns the env at minute 120 (a.steady quiet).
func quietEnv(t *testing.T, extra ...admin.TableInfo) *testEnv {
	t.Helper()
	e := newEnv(t, nil)
	var steady, busy int64
	var steadyAt, busyAt time.Time
	for m := 0; m <= 120; m++ {
		now := e.clock.Now()
		if m < 100 {
			steady, steadyAt = steady+3, now
		}
		busy, busyAt = busy+5, now
		ts := []admin.TableInfo{
			{Table: "a.steady", State: "done", Inserts: steady, LastChangeAt: tp(steadyAt), PK: []string{"id"}},
			{Table: "a.busy", State: "done", Updates: busy, LastChangeAt: tp(busyAt), PK: []string{"id"}},
			{Table: "a.never", State: "done", PK: []string{"id"}},
		}
		e.mock.mockPipeline.tables = append(ts, extra...)
		e.sampleNow()
		e.clock.Advance(time.Minute)
	}
	return e
}

// The Tables tab: quiet badge, summary count, the quiet option of the Last sync filter (inline and paged), the
// table name links to its page and the Export CSV link.
func TestTablesQuietFilterAndLinks(t *testing.T) {
	e := quietEnv(t)
	c := e.client()
	e.login(c, testPassword)
	_, body := e.do(c, "GET", "/pipelines/orders/tables", "", nil)
	for _, want := range []string{
		`<option value="quiet" title="Quiet: the table changed regularly`, `>Quiet (unusually idle)</option>`,
		`data-bind="tables_quiet">1</b>`, `data-table="a.steady"`, `data-quiet="1"`, `data-tb="quiet"`,
		`<a href="/pipelines/orders/tables/a.steady">a.steady</a>`, `id="tb-csv" href="/pipelines/orders/tables.csv"`,
		`data-csv-base="/pipelines/orders/tables.csv"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("tables tab lacks %q", want)
		}
	}
	if strings.Count(body, `data-quiet="1"`) != 1 || strings.Count(body, `data-tb="quiet"`) != 1 {
		t.Errorf("only a.steady is quiet")
	}
	// Paged (server) filter.
	_, body = e.do(c, "GET", "/pipelines/orders/tables?tsince=quiet", "", nil)
	if !strings.Contains(body, `<option value="quiet" title="`) || !strings.Contains(body, `selected>Quiet (unusually idle)</option>`) ||
		!strings.Contains(body, `data-table="a.steady"`) || strings.Contains(body, `data-table="a.busy"`) || strings.Contains(body, `data-table="a.never"`) {
		t.Errorf("paged quiet filter wrong:\n%s", body)
	}
	if !strings.Contains(body, `id="tb-csv" href="/pipelines/orders/tables.csv?tsince=quiet"`) {
		t.Errorf("CSV link does not carry the filter")
	}
	var tv TablesView
	_, api := e.do(c, "GET", "/api/v1/pipelines/orders/tables?since=quiet", "", nil)
	if err := json.Unmarshal([]byte(api), &tv); err != nil || tv.Matching != 1 || tv.Quiet != 1 || !tv.Tables[0].Quiet || tv.Tables[0].Table != "a.steady" {
		t.Fatalf("API since=quiet: %v %s", err, api)
	}
	// The inline list filters in the browser: app.js knows the value.
	js, err := os.ReadFile("static/app.js")
	if err != nil || !strings.Contains(string(js), `if (f === 'quiet') { return r.getAttribute('data-quiet') === '1'; }`) {
		t.Fatalf("app.js lacks the quiet filter: %v", err)
	}
	// Live rows carry the flag.
	p := e.srv.reg.Get("orders")
	r := e.srv.liveRecord(p, e.srv.samp.Latest("orders"), nil, time.Now())
	e.srv.fullDelta(p, &r, e.srv.samp.Latest("orders"), true)
	found := false
	for _, row := range r.TableRows {
		if row.Table == "a.steady" {
			found = row.Quiet
		} else if row.Quiet {
			t.Errorf("live row %s quiet", row.Table)
		}
	}
	if !found || r.F["tables_quiet"] != "1" || r.Tables.Quiet != 1 {
		t.Fatalf("live record: quiet row %v, f=%q, count %d", found, r.F["tables_quiet"], r.Tables.Quiet)
	}
}

// The table page and its API: 200 with the chart and the activity, 404 for an unknown table or pipeline, odd
// names escaped (HTML and URL), events that mention the table, the verify form for operators only.
func TestTableDetailPageAndAPI(t *testing.T) {
	odd := `a.b"c<x>`
	e := quietEnv(t, admin.TableInfo{Table: odd, State: "done", Inserts: 7, LastChangeAt: tp(time.Date(2026, 11, 15, 11, 0, 0, 0, time.UTC))},
		admin.TableInfo{Table: "db.with/slash", State: "running", ProgressPct: 40, RowsCopied: 40, RowsEstimate: 100})
	e.mock.mockPipeline.events = []admin.EventRecord{
		{Time: e.clock.Now(), Level: "WARN", Message: "slow apply on a.steady", Attrs: map[string]string{"password": "hunter2"}},
		{Time: e.clock.Now(), Level: "ERROR", Message: "duplicate key in `a`.`busy`"},
	}
	e.sampleNow()
	users := setupRBACUsers(t, e)
	ac := users[store.RoleAdmin].client
	resp, body := e.do(ac, "GET", "/pipelines/orders/tables/a.steady", "", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("table page: %d", resp.StatusCode)
	}
	for _, want := range []string{
		`<h2 class="mono wrap">a.steady</h2>`, `id="tbl-quiet"`, `<svg class="linechart"`, `Changes per minute`, `Sync delay`,
		`slow apply on a.steady`, `data-action="verify"`, `<input type="hidden" name="table" value="a.steady">`,
		`href="/pipelines/orders/tables"`, `No change for 22m 00s; this table usually changes every 1m 00s and counts as quiet after 15m 00s.`, `class="breadcrumb"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("table page lacks %q", want)
		}
	}
	if strings.Contains(body, "hunter2") || strings.Contains(body, "duplicate key") || strings.Contains(body, ` style="`) {
		t.Errorf("table page shows a secret, another table's event or an inline style")
	}
	// Viewer: the page, no verify form. Operator: the form.
	_, vb := e.do(users[store.RoleViewer].client, "GET", "/pipelines/orders/tables/a.busy", "", nil)
	if !strings.Contains(vb, `<h2 class="mono wrap">a.busy</h2>`) || strings.Contains(vb, `data-action="verify"`) || !strings.Contains(vb, "duplicate key") {
		t.Errorf("viewer page wrong")
	}
	if _, ob := e.do(users[store.RoleOperator].client, "GET", "/pipelines/orders/tables/a.busy", "", nil); !strings.Contains(ob, `data-action="verify"`) {
		t.Errorf("operator has no verify form")
	}
	// Odd names: escaped in the link of the list, on the page and in the JSON.
	_, list := e.do(ac, "GET", "/pipelines/orders/tables", "", nil)
	esc := "/pipelines/orders/tables/" + url.PathEscape(odd)
	if !strings.Contains(list, `href="`+esc+`"`) || strings.Contains(list, `a.b"c<x>`) {
		t.Fatalf("odd name link: want %q", esc)
	}
	resp, body = e.do(ac, "GET", esc, "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `a.b&#34;c&lt;x&gt;`) || strings.Contains(body, `a.b"c<x>`) ||
		strings.Contains(body, `data-action="verify"`) || !strings.Contains(body, "The verify job does not accept this name") {
		t.Fatalf("odd name page: %d", resp.StatusCode)
	}
	resp, body = e.do(ac, "GET", "/api/v1/pipelines/orders/tables/"+url.PathEscape(odd), "", nil)
	var dv TableDetailView
	if err := json.Unmarshal([]byte(body), &dv); resp.StatusCode != 200 || err != nil || dv.Table.Table != odd || dv.Table.Inserts != 7 {
		t.Fatalf("odd name API: %d %v %s", resp.StatusCode, err, body)
	}
	if resp, body = e.do(ac, "GET", "/pipelines/orders/tables/"+url.PathEscape("db.with/slash"), "", nil); resp.StatusCode != 200 || !strings.Contains(body, "db.with/slash") {
		t.Fatalf("name with a slash: %d", resp.StatusCode)
	}
	// API of a quiet table: history, quiet details, activity.
	resp, body = e.do(ac, "GET", "/api/v1/pipelines/orders/tables/a.steady", "", nil)
	if err := json.Unmarshal([]byte(body), &dv); resp.StatusCode != 200 || err != nil {
		t.Fatalf("API: %d %v", resp.StatusCode, err)
	}
	if !dv.Table.Quiet || !dv.Quiet.Quiet || !dv.Quiet.Regular || dv.HistoryBucketS != 60 || len(dv.History) != histBuckets ||
		dv.Changes != 300 || len(dv.Events) != 1 || strings.Contains(dv.Events[0].Message, "hunter2") {
		t.Fatalf("API detail: %+v", dv)
	}
	var sum int64
	for _, p := range dv.History {
		if p.Changes < 0 {
			t.Fatalf("negative point %+v", p)
		}
		sum += p.Changes
	}
	if sum != dv.HistoryChanges || sum == 0 {
		t.Fatalf("history sum %d vs %d", sum, dv.HistoryChanges)
	}
	// 404s.
	for _, path := range []string{"/pipelines/orders/tables/a.nope", "/api/v1/pipelines/orders/tables/a.nope",
		"/pipelines/nope/tables/a.steady", "/api/v1/pipelines/nope/tables/a.steady"} {
		if resp, _ := e.do(ac, "GET", path, "", nil); resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: %d, want 404", path, resp.StatusCode)
		}
	}
	// Signed out: the page asks for a login, the API refuses.
	anon := e.client()
	if resp, _ := e.do(anon, "GET", "/pipelines/orders/tables/a.steady", "", nil); resp.StatusCode != http.StatusSeeOther && resp.StatusCode != http.StatusFound {
		t.Errorf("anonymous page: %d", resp.StatusCode)
	}
	if resp, _ := e.do(anon, "GET", "/api/v1/pipelines/orders/tables/a.steady", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("anonymous API: %d", resp.StatusCode)
	}
}

// The CSV export: every matching table (across pages), the filters and the sort of the view, a header row,
// formula injection neutralised, an attachment with a safe file name.
func TestTablesCSVExport(t *testing.T) {
	e := newEnv(t, nil)
	now := e.clock.Now()
	ts := manyTables(450)
	ts = append(ts,
		admin.TableInfo{Table: "=cmd|' /C calc'!A0", State: "done", Inserts: 1, LastChangeAt: tp(now.Add(-time.Minute)), LastSourceAt: tp(now.Add(-61 * time.Second)), Key: "@key"},
		admin.TableInfo{Table: "+x.y", State: "done"}, admin.TableInfo{Table: "-x.z", State: "done"}, admin.TableInfo{Table: "@x.w", State: "done"},
		admin.TableInfo{Table: "x.\"quoted\",comma", State: "done"})
	e.mock.mockPipeline.tables = ts
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	get := func(q string) ([][]string, *http.Response) {
		t.Helper()
		resp, body := e.do(c, "GET", "/pipelines/orders/tables.csv"+q, "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("csv %s: %d %s", q, resp.StatusCode, body)
		}
		recs, err := csv.NewReader(strings.NewReader(body)).ReadAll()
		if err != nil {
			t.Fatalf("csv %s: %v", q, err)
		}
		return recs, resp
	}
	recs, resp := get("")
	if strings.Join(recs[0], ",") != "table,state,progress_pct,rows_copied,rows_estimate,last_change_at,last_source_at,inserts,updates,deletes,changes,eta_seconds,key,quiet" {
		t.Fatalf("header: %v", recs[0])
	}
	if len(recs) != 1+455 {
		t.Fatalf("rows: %d, want every table (455)", len(recs)-1)
	}
	if ct, cd := resp.Header.Get("Content-Type"), resp.Header.Get("Content-Disposition"); !strings.HasPrefix(ct, "text/csv") ||
		!strings.HasPrefix(cd, `attachment; filename="strand-orders-tables-`) || !strings.HasSuffix(cd, `.csv"`) {
		t.Fatalf("headers: %q %q", ct, cd)
	}
	byName := map[string][]string{}
	for _, r := range recs[1:] {
		byName[r[0]] = r
	}
	for _, want := range []string{`'=cmd|' /C calc'!A0`, `'+x.y`, `'-x.z`, `'@x.w`, `x."quoted",comma`} {
		if byName[want] == nil {
			t.Errorf("CSV lacks the (escaped) table %q", want)
		}
	}
	if r := byName[`'=cmd|' /C calc'!A0`]; r != nil && (r[5] != now.Add(-time.Minute).UTC().Format(time.RFC3339) || r[7] != "1" || r[10] != "1" || r[12] != "'@key" || r[13] != "false") {
		t.Errorf("CSV row: %v", r)
	}
	// Filters and sort of the paged view, all matching rows (the page holds 100).
	recs, _ = get("?tstate=pending&tsort=name&tdir=desc&tpage=2")
	pending := 0
	for _, x := range ts {
		if tblState(x) == "pending" {
			pending++
		}
	}
	if len(recs)-1 != pending || pending <= tablesPageSize {
		t.Fatalf("pending rows: %d, want %d (> one page)", len(recs)-1, pending)
	}
	for i := 2; i < len(recs); i++ {
		if recs[i][1] != "pending" || recs[i-1][0] < recs[i][0] {
			t.Fatalf("filter/sort not applied at row %d: %v after %v", i, recs[i], recs[i-1])
		}
	}
	recs, _ = get("?tq=number_0004&tsince=none")
	if len(recs)-1 != 10 {
		t.Fatalf("search: %d rows", len(recs)-1)
	}
	if resp, _ := e.do(e.client(), "GET", "/pipelines/orders/tables.csv", "", nil); resp.StatusCode == 200 {
		t.Fatalf("anonymous CSV: %d", resp.StatusCode)
	}
	if resp, _ := e.do(c, "GET", "/pipelines/nope/tables.csv", "", nil); resp.StatusCode != 404 {
		t.Fatalf("unknown pipeline CSV: %d", resp.StatusCode)
	}
	if got := csvSafe("\t=1"); got != "'\t=1" {
		t.Fatalf("tab prefix: %q", got)
	}
}
