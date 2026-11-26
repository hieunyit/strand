package web

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

// Every sortable column sorts both ways; tables without a value (no ETA, no change) stay last either way.
func TestTablesQuerySortDirection(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	base := TablesView{At: now, Tables: []admin.TableInfo{
		{Table: "a.hot", State: "done", RowsEstimate: 50_000, ProgressPct: 100, Updates: 5, LastChangeAt: tp(now.Add(-30 * time.Second))},
		{Table: "a.warm", State: "done", RowsEstimate: 20_000, ProgressPct: 100, Updates: 100, LastChangeAt: tp(now.Add(-40 * time.Minute))},
		{Table: "a.never", State: "pending", RowsEstimate: 1_500_000},
		{Table: "a.copying", State: "running", RowsEstimate: 9_000, RowsCopied: 3_000, ProgressPct: 33, ETASeconds: 60},
		{Table: "a.slow", State: "running", RowsEstimate: 90_000, RowsCopied: 1_000, ProgressPct: 1, ETASeconds: 900},
	}}
	base.finish()
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
		{TableQuery{Sort: "name"}, "copying hot never slow warm"},
		{TableQuery{Sort: "name", Dir: "desc"}, "warm slow never hot copying"},
		{TableQuery{Sort: "rows"}, "never slow hot warm copying"},
		{TableQuery{Sort: "rows", Dir: "asc"}, "copying warm hot slow never"},
		{TableQuery{Sort: "last"}, "hot warm copying slow never"},
		{TableQuery{Sort: "last", Dir: "asc"}, "warm hot copying slow never"},
		{TableQuery{Sort: "changes", Dir: "asc"}, "copying slow never hot warm"},
		{TableQuery{Sort: "eta"}, "copying slow never hot warm"},
		{TableQuery{Sort: "eta", Dir: "desc"}, "slow copying never hot warm"},
		{TableQuery{Sort: "pct", Dir: "asc"}, "never slow copying hot warm"},
		{TableQuery{Sort: "ord", Dir: "desc"}, "warm hot never slow copying"},
		{TableQuery{Sort: "name", Dir: "bogus"}, "copying hot never slow warm"},
	} {
		if got := names(c.q); got != c.want {
			t.Errorf("%+v: got %q, want %q", c.q, got, c.want)
		}
	}
	if q := normTableQuery(TableQuery{Sort: "changes"}); q.Dir != "desc" {
		t.Fatalf("default dir of changes: %q", q.Dir)
	}
}

// Header links of the paged list: the active column flips direction, another column starts in its natural one;
// the default direction is never written into the URL.
func TestTablesSortHrefAndAria(t *testing.T) {
	base := "/pipelines/orders/tables"
	q := normTableQuery(TableQuery{Q: "ord", Sort: "rows", Page: 3})
	for _, c := range []struct{ got, want string }{
		{tablesSortHref(base, &q, "rows"), base + "?tdir=asc&tq=ord&tsort=rows"},
		{tablesSortHref(base, &q, "name"), base + "?tq=ord&tsort=name"},
		{tablesSortHref(base, &q, "ord"), base + "?tq=ord"},
		{tablesAriaSort(&q, "rows"), "descending"},
		{tablesAriaSort(&q, "name"), "none"},
		{tablesAriaSort(nil, "ord"), "ascending"},
	} {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
	asc := normTableQuery(TableQuery{Sort: "rows", Dir: "asc"})
	if got := tablesSortHref(base, &asc, "rows"); got != base+"?tsort=rows" {
		t.Errorf("flip back to the natural direction: %q", got)
	}
	if got := tablesHref(base, &asc, "tpage", "2"); got != base+"?tdir=asc&tpage=2&tsort=rows" {
		t.Errorf("pager keeps the direction: %q", got)
	}
}

func TestTablesChangesDetail(t *testing.T) {
	for _, c := range []struct {
		t    admin.TableInfo
		want string
	}{
		{admin.TableInfo{}, ""},
		{admin.TableInfo{Updates: 1_234}, "1.2K upd"},
		{admin.TableInfo{Inserts: 12, Updates: 56_789, Deletes: 3}, "12 ins · 56K upd · 3 del"},
		{admin.TableInfo{Inserts: 7_890_000, Deletes: 2_500_000_000}, "7.8M ins · 2.5B del"},
	} {
		if got := tblChangesDetail(c.t); got != c.want {
			t.Errorf("%+v: got %q, want %q", c.t, got, c.want)
		}
	}
}

// The page renders sortable headers (buttons inline, links when paged) with aria-sort, the Changes split,
// and the API takes dir.
func TestTablesTabSortHeaders(t *testing.T) {
	e := newEnv(t, nil)
	now := e.clock.Now()
	e.mock.mockPipeline.tables = []admin.TableInfo{
		{Table: "app.users", State: "done", RowsEstimate: 2_000_000, PK: []string{"id"}, Inserts: 3, Updates: 1_500, Deletes: 1,
			LastChangeAt: tp(now.Add(-90 * time.Second))},
		{Table: "app.codes", State: "done", RowsEstimate: 10, PK: []string{"code"}},
	}
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	_, body := e.do(c, "GET", "/pipelines/orders/tables", "", nil)
	for _, want := range []string{
		`aria-sort="ascending"><button type="button" class="sortbtn" data-tsort="ord"`,
		`aria-sort="none"><button type="button" class="sortbtn" data-tsort="changes"`,
		`data-tb="chgd">3 ins · 1.5K upd · 1 del</span>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("inline tables tab lacks %q", want)
		}
	}
	_, body = e.do(c, "GET", "/pipelines/orders/tables?tsort=rows&tdir=asc", "", nil)
	for _, want := range []string{
		`aria-sort="ascending"><a class="sortbtn" data-tsort="rows" href="/pipelines/orders/tables?tsort=rows"`,
		`<input type="hidden" name="tdir" id="tb-pdir" value="asc">`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("paged tables tab lacks %q", want)
		}
	}
	if i, j := strings.Index(body, `data-table="app.codes"`), strings.Index(body, `data-table="app.users"`); i < 0 || j < 0 || i > j {
		t.Errorf("rows asc: codes (10 rows) must come before users")
	}
	_, api := e.do(c, "GET", "/api/v1/pipelines/orders/tables?sort=rows&dir=asc", "", nil)
	var tv TablesView
	if err := json.Unmarshal([]byte(api), &tv); err != nil || len(tv.Tables) != 2 || tv.Tables[0].Table != "app.codes" || tv.Query.Dir != "asc" {
		t.Fatalf("API sort=rows&dir=asc: %v %+v", err, tv)
	}
}
