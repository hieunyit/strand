package web

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

func tp(t time.Time) *time.Time { return &t }

// The Since (last CDC change) and Rows filters and the last/changes sorts of the paged Tables list.
func TestTablesQuerySinceRowsAndSort(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	base := TablesView{At: now, Tables: []admin.TableInfo{
		{Table: "a.hot", State: "done", RowsEstimate: 50_000_000, Inserts: 10, Updates: 5, LastChangeAt: tp(now.Add(-30 * time.Second))},
		{Table: "a.warm", State: "done", RowsEstimate: 20_000, Updates: 100, LastChangeAt: tp(now.Add(-40 * time.Minute))},
		{Table: "a.cold", State: "done", RowsEstimate: 0, Deletes: 1, LastChangeAt: tp(now.Add(-3 * 24 * time.Hour))},
		{Table: "a.never", State: "done", RowsEstimate: 1_500_000},
		{Table: "a.copying", State: "running", RowsEstimate: 9_000, RowsCopied: 3_000},
	}}
	base.finish()
	if base.Active5m != 1 || base.Active1h != 2 || base.WithChange != 3 || base.Changes != 116 || !base.LastChange.Equal(now.Add(-30*time.Second)) {
		t.Fatalf("activity aggregates: 5m=%d 1h=%d with=%d changes=%d last=%v", base.Active5m, base.Active1h, base.WithChange, base.Changes, base.LastChange)
	}
	names := func(q TableQuery) string {
		v := base
		v.applyPage(q)
		var out []string
		for _, x := range v.Tables {
			out = append(out, strings.TrimPrefix(x.Table, "a."))
		}
		return strings.Join(out, " ")
	}
	for _, c := range []struct {
		q    TableQuery
		want string
	}{
		{TableQuery{Since: "5m", Sort: "name"}, "hot"},
		{TableQuery{Since: "1h", Sort: "name"}, "hot warm"},
		{TableQuery{Since: "7d", Sort: "name"}, "cold hot warm"},
		{TableQuery{Since: "none", Sort: "name"}, "copying never"},
		{TableQuery{Since: "idle24h", Sort: "name"}, "cold copying never"},
		{TableQuery{Rows: "1000000-", Sort: "name"}, "hot never"},
		{TableQuery{Rows: "0-0"}, "cold"},
		{TableQuery{Rows: "10000-999999"}, "warm"},
		{TableQuery{Rows: "1,000-10,000"}, "copying"},
		{TableQuery{Since: "24h", Rows: "-100000", Sort: "name"}, "warm"},
		{TableQuery{Sort: "last"}, "hot warm cold copying never"},
		{TableQuery{Sort: "changes"}, "warm hot cold copying never"},
		{TableQuery{Since: "bogus", Rows: "x-y", Sort: "name"}, "cold copying hot never warm"},
	} {
		if got := names(c.q); got != c.want {
			t.Errorf("%+v: got %q, want %q", c.q, got, c.want)
		}
	}
	q := normTableQuery(TableQuery{Since: "bogus", Rows: "9-1"})
	if q.Since != "" || q.Rows != "" {
		t.Fatalf("invalid filters kept: %+v", q)
	}
	if q := normTableQuery(TableQuery{Rows: " 1,000 - "}); q.Rows != "1000-" {
		t.Fatalf("rows normalised to %q", q.Rows)
	}
}

// The page and the API take the new filters; the page shows the Last sync and Changes columns.
func TestTablesTabActivityColumnsAndFilters(t *testing.T) {
	e := newEnv(t, nil)
	now := e.clock.Now()
	e.mock.mockPipeline.tables = []admin.TableInfo{
		{Table: "app.users", State: "done", RowsEstimate: 2_000_000, PK: []string{"id"}, Inserts: 3, Updates: 4, Deletes: 1,
			LastChangeAt: tp(now.Add(-90 * time.Second)), LastSourceAt: tp(now.Add(-92 * time.Second))},
		{Table: "app.codes", State: "done", RowsEstimate: 10, PK: []string{"code"}},
	}
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	_, body := e.do(c, "GET", "/pipelines/orders/tables", "", nil)
	for _, want := range []string{
		`<th scope="col" class="nowrap">Last sync</th>`, `<th scope="col" class="r">Changes</th>`, `id="tb-since"`, `id="tb-rows"`,
		`<option value="last">Last sync (newest)</option>`, `data-last="` + fmt.Sprint(now.Add(-90*time.Second).UnixMilli()) + `"`, `data-changes="8"`,
		`data-ago="` + fmt.Sprint(now.Add(-90*time.Second).UnixMilli()) + `"`, `>1 min ago</time>`, `title="3 inserts, 4 updates, 1 deletes`,
		`data-last="-1"`, `id="tables-activity"`, `data-bind="tables_active5m">1</b>`, `~2,000,000`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("tables tab lacks %q", want)
		}
	}
	_, body = e.do(c, "GET", "/pipelines/orders/tables?tsince=5m&trows=1000000-", "", nil)
	if !strings.Contains(body, `data-paged="1"`) || !strings.Contains(body, `data-table="app.users"`) || strings.Contains(body, `data-table="app.codes"`) ||
		!strings.Contains(body, `<option value="5m" selected>`) || !strings.Contains(body, `<option value="1000000-" selected>`) {
		t.Errorf("paged filter page wrong:\n%s", body)
	}
	_, api := e.do(c, "GET", "/api/v1/pipelines/orders/tables?since=none", "", nil)
	var tv TablesView
	if err := json.Unmarshal([]byte(api), &tv); err != nil || tv.Matching != 1 || tv.Tables[0].Table != "app.codes" {
		t.Fatalf("API since=none: %v %+v", err, tv)
	}
	_, api = e.do(c, "GET", "/api/v1/pipelines/orders/tables?sort=last", "", nil)
	if err := json.Unmarshal([]byte(api), &tv); err != nil || tv.Tables[0].Table != "app.users" || tv.Tables[0].LastChangeAt == nil || tv.Tables[0].Inserts != 3 {
		t.Fatalf("API sort=last: %v %+v", err, tv.Tables)
	}
}

// A restart of the pipeline process empties its counters: the Web keeps the last change time it saw.
func TestTablesLastChangeSurvivesPipelineRestart(t *testing.T) {
	e := newEnv(t, nil)
	at := e.clock.Now().Add(-time.Hour)
	e.mock.mockPipeline.tables = []admin.TableInfo{{Table: "app.users", State: "done", Updates: 9, LastChangeAt: tp(at)}, {Table: "app.codes", State: "done"}}
	e.sampleNow()
	e.mock.mockPipeline.tables = []admin.TableInfo{{Table: "app.users", State: "done"}, {Table: "app.codes", State: "done"}}
	e.sampleNow()
	tabs := e.srv.samp.Latest("orders").Tables
	if tabs[0].LastChangeAt == nil || !tabs[0].LastChangeAt.Equal(at) || tabs[0].Updates != 0 || tabs[1].LastChangeAt != nil {
		t.Fatalf("after restart: %+v", tabs)
	}
	newer := e.clock.Now()
	e.mock.mockPipeline.tables = []admin.TableInfo{{Table: "app.users", State: "done", Inserts: 1, LastChangeAt: tp(newer)}, {Table: "app.codes", State: "done"}}
	e.sampleNow()
	if tabs = e.srv.samp.Latest("orders").Tables; !tabs[0].LastChangeAt.Equal(newer) {
		t.Fatalf("newer change not taken: %v", tabs[0].LastChangeAt)
	}
}

// In a big list, CDC activity alone never forces a re-render: changed rows are sent (the most recent first,
// at most tablesInlineMax) and the page updates them in place.
func TestTablesLiveDeltaActivityOnly(t *testing.T) {
	e := newEnv(t, nil)
	ts := manyTables(2000)
	e.mock.mockPipeline.tables = ts
	e.srv.samp.OnSample = nil
	ctx := context.Background()
	e.srv.samp.SampleOnce(ctx)
	p := e.srv.reg.Get("orders")
	rec := func() LiveRecord {
		r := e.srv.liveRecord(p, e.srv.samp.Latest("orders"), nil, time.Now())
		e.srv.fullDelta(p, &r, e.srv.samp.Latest("orders"), false)
		return r
	}
	rec()
	now := time.Now()
	ts2 := append([]admin.TableInfo(nil), ts...)
	for i := 0; i < 500; i++ {
		ts2[i].Updates = int64(i + 1)
		ts2[i].LastChangeAt = tp(now.Add(time.Duration(i) * time.Millisecond))
	}
	e.mock.mockPipeline.tables = ts2
	e.srv.samp.SampleOnce(ctx)
	r := rec()
	if r.TablesChanged || r.TablesFull || len(r.TableRows) != tablesInlineMax {
		t.Fatalf("activity delta: changed=%v full=%v rows=%d", r.TablesChanged, r.TablesFull, len(r.TableRows))
	}
	if r.TableRows[0].Table != ts2[499].Table || r.TableRows[0].Changes != 500 || r.TableRows[0].LastMs != ts2[499].LastChangeAt.UnixMilli() ||
		r.TableRows[0].F["chg"] != "500" || r.TableRows[0].F["last"] == "" {
		t.Fatalf("newest change first: %+v", r.TableRows[0])
	}
	if r.F["tables_active5m"] != "500" || r.F["tables_changes"] == "" {
		t.Fatalf("summary binds: %v %v", r.F["tables_active5m"], r.F["tables_changes"])
	}
	// A snapshot change of one row is still sent along with activity rows.
	ts3 := append([]admin.TableInfo(nil), ts2...)
	ts3[1000].ProgressPct = 99
	ts3[3].Updates++
	e.mock.mockPipeline.tables = ts3
	e.srv.samp.SampleOnce(ctx)
	r = rec()
	got := map[string]bool{}
	for _, row := range r.TableRows {
		got[row.Table] = true
	}
	if r.TablesChanged || len(r.TableRows) != 2 || !got[ts3[1000].Table] || !got[ts3[3].Table] {
		t.Fatalf("mixed delta: changed=%v rows=%v", r.TablesChanged, got)
	}
}
