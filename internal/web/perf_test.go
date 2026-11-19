package web

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

func manyTables(n int) []admin.TableInfo {
	var ts []admin.TableInfo
	for i := 0; i < n; i++ {
		st := "done"
		switch {
		case i%500 == 7: // a handful copying in parallel
			st = "running"
		case i%3 == 1:
			st = "pending"
		}
		ts = append(ts, admin.TableInfo{Table: fmt.Sprintf("schema_%02d.a_rather_long_table_name_number_%05d", i%40, i), State: st,
			RowsEstimate: int64(i) * 1_000_003, RowsCopied: int64(i) * 500_000, ProgressPct: 50, PK: []string{"id"}})
	}
	return ts
}

// Regression: with 2,000 tables the Tables tab was ~2.4 MB (every row rendered, re-fetched on each structural
// refresh) and the live snapshot carried every row too. Above 200 tables the list is now paged server-side:
// page < 200 KB and < 100 ms of server time at 2,000 and 10,000 tables; the live snapshot carries only the
// running tables.
func TestTablesTabScales(t *testing.T) {
	for _, n := range []int{2000, 10000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			e := newEnv(t, nil)
			e.mock.mockPipeline.tables = manyTables(n)
			e.srv.samp.SampleOnce(context.Background())
			c, _ := e.loginOK()
			var took []time.Duration
			var body string
			for i := 0; i < 7; i++ {
				t0 := time.Now()
				resp, b := e.do(c, "GET", "/pipelines/orders/tables", "", nil)
				took = append(took, time.Since(t0))
				if resp.StatusCode != 200 {
					t.Fatalf("tables tab: %d", resp.StatusCode)
				}
				body = b
			}
			sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
			med := took[len(took)/2]
			t.Logf("tables tab with %d tables: median %v, worst %v, %d KiB", n, med, took[len(took)-1], len(body)/1024)
			if len(body) > 200<<10 {
				t.Fatalf("tables tab is %d KiB with %d tables (want < 200 KiB)", len(body)/1024, n)
			}
			if med > 100*time.Millisecond*raceSlowdown {
				t.Fatalf("tables tab took %v (median) with %d tables (want < 100 ms)", med, n)
			}
			if !strings.Contains(body, fmt.Sprintf(`data-bind="tables_total">%d<`, n)) || !strings.Contains(body, `id="runlist" data-paged="1"`) ||
				!strings.Contains(body, fmt.Sprintf("1&ndash;%d of %d tables", tablesPageSize, n)) || !strings.Contains(body, `rel="next"`) {
				t.Fatal("summary, running list or pager missing")
			}
			// Running tables always shown (copying now), whatever page.
			if !strings.Contains(body, `data-run="schema_07.a_rather_long_table_name_number_00007"`) {
				t.Fatal("running table missing from Copying now")
			}
			// Server-side search + filter + sort + page.
			_, b := e.do(c, "GET", "/pipelines/orders/tables?tq=number_0199&tstate=pending&tsort=name", "", nil)
			if strings.Count(b, `<tr class="trow`) != 4 || !strings.Contains(b, "number_01990") || !strings.Contains(b, "number_01999") || strings.Contains(b, "number_01991") {
				t.Fatalf("search/filter: %d rows", strings.Count(b, `<tr class="trow`))
			}
			_, b = e.do(c, "GET", fmt.Sprintf("/pipelines/orders/tables?tpage=%d", n/tablesPageSize), "", nil)
			if !strings.Contains(b, fmt.Sprintf("Page %d of %d", n/tablesPageSize, n/tablesPageSize)) || strings.Contains(b, `rel="next"`) {
				t.Fatal("last page")
			}
			// Live snapshot: running tables only, not the whole list.
			_, lv := e.do(c, "GET", "/api/v1/live?pipeline=orders", "", nil)
			if len(lv) > 100<<10 || strings.Contains(lv, `"tables_full": true`) {
				t.Fatalf("live snapshot is %d KiB with %d tables", len(lv)/1024, n)
			}
			// API: whole list by default (compatibility), paged on request.
			_, api := e.do(c, "GET", "/api/v1/pipelines/orders/tables?state=running&per_page=3&page=1", "", nil)
			var tv TablesView
			if err := json.Unmarshal([]byte(api), &tv); err != nil || !tv.Paged || len(tv.Tables) != 3 || tv.Matching != (n+492)/500 || tv.Total != n {
				t.Fatalf("paged API: %+v %v", tv.Query, err)
			}
		})
	}
}

// Live deltas of a big list: rows that changed only; a list change asks for a re-render, never sends all rows.
func TestTablesLiveDeltaBounded(t *testing.T) {
	e := newEnv(t, nil)
	ts := manyTables(2000)
	e.mock.mockPipeline.tables = ts
	e.srv.samp.OnSample = nil // the test drives the deltas itself
	ctx := context.Background()
	e.srv.samp.SampleOnce(ctx)
	p := e.srv.reg.Get("orders")
	rec := func() LiveRecord {
		r := e.srv.liveRecord(p, e.srv.samp.Latest("orders"), nil, time.Now())
		e.srv.fullDelta(p, &r, e.srv.samp.Latest("orders"), false)
		return r
	}
	full := e.srv.liveRecord(p, e.srv.samp.Latest("orders"), nil, time.Now())
	e.srv.fullDelta(p, &full, e.srv.samp.Latest("orders"), true) // snapshot: running tables only
	if full.TablesFull || len(full.TableRows) != 4 {
		t.Fatalf("snapshot: full=%v rows=%d", full.TablesFull, len(full.TableRows))
	}
	if r := rec(); r.TablesFull || r.TablesChanged || len(r.TableRows) != 4 { // first delta: running tables only
		t.Fatalf("first delta: full=%v rows=%d", r.TablesFull, len(r.TableRows))
	}
	r := rec() // nothing changed since
	if r.TablesFull || r.TablesChanged || len(r.TableRows) != 0 {
		t.Fatalf("idle delta: full=%v rows=%d", r.TablesFull, len(r.TableRows))
	}
	ts2 := append([]admin.TableInfo(nil), ts...)
	ts2[7].ProgressPct = 77
	e.mock.mockPipeline.tables = ts2
	e.srv.samp.SampleOnce(ctx)
	if r = rec(); len(r.TableRows) != 1 || r.TableRows[0].Table != ts[7].Table || r.TablesChanged {
		t.Fatalf("delta: %d rows changed=%v", len(r.TableRows), r.TablesChanged)
	}
	e.mock.mockPipeline.tables = append(ts2, admin.TableInfo{Table: "app.new", State: "pending"})
	e.srv.samp.SampleOnce(ctx)
	if r = rec(); !r.TablesChanged || len(r.TableRows) != 0 || r.TablesFull {
		t.Fatalf("list change: changed=%v rows=%d", r.TablesChanged, len(r.TableRows))
	}
}
