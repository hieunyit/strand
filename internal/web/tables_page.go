package web

import (
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/admin"
)

// Tables tab at scale (thousands of tables). Up to tablesInlineMax tables the page carries every row and the
// browser filters/sorts them (as before). Above it the page is PAGED: the server applies the search, the status
// filter and the sort and renders one page of tablesPageSize rows; the summary, the counters and the "copying
// now" list (running tables, capped) are always shown. The live stream (stream.go) then sends only the rows
// that changed, never the full list, and asks the page to re-render (structural refresh) when the list itself
// changed or a visible row changed state.
const (
	tablesInlineMax = 200
	tablesPageSize  = 100
	tablesPageMax   = 500
	runningListMax  = 50
)

// TableQuery is the server-side view of the Tables list (query parameters tq, tstate, tsince, trows, tsort, tdir,
// tpage on the page; q, state, since, rows, sort, dir, page, per_page on the API).
type TableQuery struct {
	Q     string `json:"q,omitempty"`
	State string `json:"state,omitempty"` // all | running | pending | done
	// Since filters by the last CDC change: a window (5m, 15m, 1h, 6h, 24h, 7d: changed within it), "idle24h"
	// (no change in the last 24 hours, or none recorded), "none" (no change recorded) or "quiet" (flagged by the
	// quiet-table rule: used to change regularly, no change for unusually long; table_history.go). Empty: no filter.
	Since string `json:"since,omitempty"`
	// Rows filters by the row count (estimate) as "min-max", either side optional ("1000000-", "-9999", "0-0").
	Rows string `json:"rows,omitempty"`
	Sort string `json:"sort,omitempty"` // ord | name | pct | rows | eta | last | changes
	// Dir is the sort direction, asc | desc. Empty or invalid: the natural direction of the sort (sortDefaultDir).
	// Tables without a value (no ETA, no change recorded) stay at the end in both directions.
	Dir     string `json:"dir,omitempty"`
	Page    int    `json:"page"`
	PerPage int    `json:"per_page"`
}

// sortDefaultDir: the direction a sort starts with (largest/newest first for the measures, A-Z otherwise).
func sortDefaultDir(sort string) string {
	switch sort {
	case "pct", "rows", "last", "changes":
		return "desc"
	}
	return "asc"
}

// sinceWindows are the "changed within" choices of the Since filter.
var sinceWindows = map[string]time.Duration{"5m": 5 * time.Minute, "15m": 15 * time.Minute, "1h": time.Hour,
	"6h": 6 * time.Hour, "24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour}

// parseRowsRange parses "min-max" (either side may be empty). ok=false: no filter.
func parseRowsRange(s string) (lo, hi int64, ok bool) {
	a, b, found := strings.Cut(strings.ReplaceAll(strings.TrimSpace(s), ",", ""), "-")
	if !found {
		return 0, 0, false
	}
	a, b = strings.TrimSpace(a), strings.TrimSpace(b)
	lo, hi = 0, math.MaxInt64
	if a != "" {
		n, err := strconv.ParseInt(a, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, false
		}
		lo = n
	}
	if b != "" {
		n, err := strconv.ParseInt(b, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, false
		}
		hi = n
	}
	if a == "" && b == "" || lo > hi {
		return 0, 0, false
	}
	return lo, hi, true
}

// matchSince reports whether a table passes the Since filter at now.
func matchSince(t *admin.TableInfo, since string, now time.Time) bool {
	last := t.LastChangeAt
	switch since {
	case "":
		return true
	case "none":
		return last == nil
	case "idle24h":
		return last == nil || now.Sub(*last) > 24*time.Hour
	case "quiet":
		return t.Quiet
	}
	w, ok := sinceWindows[since]
	if !ok {
		return true
	}
	return last != nil && now.Sub(*last) <= w
}

func normTableQuery(q TableQuery) TableQuery {
	q.Q = strings.TrimSpace(q.Q)
	switch q.State {
	case "running", "pending", "done":
	default:
		q.State = "all"
	}
	switch q.Sort {
	case "name", "pct", "rows", "eta", "last", "changes":
	default:
		q.Sort = "ord"
	}
	if q.Dir != "asc" && q.Dir != "desc" {
		q.Dir = sortDefaultDir(q.Sort)
	}
	if _, ok := sinceWindows[q.Since]; !ok && q.Since != "none" && q.Since != "idle24h" && q.Since != "quiet" {
		q.Since = ""
	}
	if lo, hi, ok := parseRowsRange(q.Rows); ok {
		q.Rows = strconv.FormatInt(lo, 10) + "-"
		if hi != math.MaxInt64 {
			q.Rows += strconv.FormatInt(hi, 10)
		}
	} else {
		q.Rows = ""
	}
	if q.PerPage <= 0 {
		q.PerPage = tablesPageSize
	}
	if q.PerPage > tablesPageMax {
		q.PerPage = tablesPageMax
	}
	if q.Page < 1 {
		q.Page = 1
	}
	return q
}

func tableQueryFrom(v url.Values, prefix string) TableQuery {
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	return normTableQuery(TableQuery{Q: v.Get(prefix + "q"), State: v.Get(prefix + "state"), Since: v.Get(prefix + "since"),
		Rows: v.Get(prefix + "rows"), Sort: v.Get(prefix + "sort"), Dir: v.Get(prefix + "dir"),
		Page: atoi(v.Get(prefix + "page")), PerPage: atoi(v.Get(prefix + "per_page"))})
}

// matching returns every table that passes the search and the filters of q, in the order of q (a new slice;
// v.Tables is not modified). v.Tables must be finish()ed (status order, the tie-breaker of every sort).
func (v *TablesView) matching(q TableQuery) []admin.TableInfo {
	needle := strings.ToLower(q.Q)
	now := v.At
	if now.IsZero() {
		now = time.Now()
	}
	minRows, maxRows, rowsOn := parseRowsRange(q.Rows)
	match := v.Tables[:0:0]
	for i := range v.Tables {
		t := &v.Tables[i]
		if (q.State == "all" || tblState(*t) == q.State) && (needle == "" || strings.Contains(strings.ToLower(t.Table), needle)) &&
			matchSince(t, q.Since, now) && (!rowsOn || tblEst(*t) >= minRows && tblEst(*t) <= maxRows) {
			match = append(match, *t)
		}
	}
	ord := make(map[string]int, len(match))
	for i, t := range match {
		ord[t.Table] = i
	}
	// sort.Slice with the status order (ord) as tie-breaker: a total order, so no (slow) stable sort is needed.
	// key returns the ascending value and ok=false for "no value", which sorts last in both directions.
	desc := q.Dir == "desc"
	by := func(key func(t *admin.TableInfo) (float64, bool)) {
		sort.Slice(match, func(i, j int) bool {
			x, xok := key(&match[i])
			y, yok := key(&match[j])
			if xok != yok {
				return xok
			}
			if x != y {
				return (x < y) != desc
			}
			return ord[match[i].Table] < ord[match[j].Table]
		})
	}
	switch q.Sort {
	case "ord":
		if desc {
			by(func(t *admin.TableInfo) (float64, bool) { return float64(ord[t.Table]), true })
		}
	case "name":
		sort.Slice(match, func(i, j int) bool { return (match[i].Table < match[j].Table) != desc })
	case "pct":
		by(func(t *admin.TableInfo) (float64, bool) { return t.ProgressPct, true })
	case "rows":
		by(func(t *admin.TableInfo) (float64, bool) { return float64(tblEst(*t)), true })
	case "eta":
		by(func(t *admin.TableInfo) (float64, bool) { return t.ETASeconds, t.ETASeconds > 0 })
	case "last":
		by(func(t *admin.TableInfo) (float64, bool) {
			if t.LastChangeAt == nil {
				return 0, false
			}
			return float64(t.LastChangeAt.UnixNano()), true
		})
	case "changes":
		by(func(t *admin.TableInfo) (float64, bool) { return float64(t.Changes()), true })
	}
	return match
}

// applyPage keeps the tables of the requested page (v.Tables must be finish()ed: status order). Counters and
// aggregates stay those of the whole list.
func (v *TablesView) applyPage(q TableQuery) {
	q = normTableQuery(q)
	v.Paged, v.Query = true, &q
	match := v.matching(q)
	v.Matching = len(match)
	v.Pages = (v.Matching + q.PerPage - 1) / q.PerPage
	if v.Pages == 0 {
		v.Pages = 1
	}
	if q.Page > v.Pages {
		q.Page = v.Pages
		v.Query.Page = q.Page
	}
	lo := (q.Page - 1) * q.PerPage
	hi := lo + q.PerPage
	if hi > len(match) {
		hi = len(match)
	}
	v.First = lo + 1
	if v.Matching == 0 {
		v.First = 0
	}
	v.Last = hi
	v.Tables = append([]admin.TableInfo(nil), match[lo:hi]...)
}

// tblState: the status group of a table (anything not running/done is pending, as in the counters).
func tblState(t admin.TableInfo) string {
	switch t.State {
	case "running", "done":
		return t.State
	}
	return "pending"
}

// runningOf: the running tables (for "Copying now"), at most runningListMax.
func (v *TablesView) runningOf(all []admin.TableInfo) {
	for _, t := range all {
		if t.State != "running" {
			continue
		}
		if len(v.RunningRows) < runningListMax {
			v.RunningRows = append(v.RunningRows, t)
		} else {
			v.RunningMore++
		}
	}
}

// tablesPageView: the Tables tab of the page. Paged above tablesInlineMax tables (or when a query is given).
func (s *Server) tablesPageView(p *Pipeline, r *http.Request) TablesView {
	v := tablesFromSnap(s.samp.Latest(p.Name))
	v.runningOf(v.Tables)
	qv := r.URL.Query()
	if v.Total > tablesInlineMax || qv.Get("tq") != "" || qv.Get("tstate") != "" || qv.Get("tsince") != "" || qv.Get("trows") != "" ||
		qv.Get("tsort") != "" || qv.Get("tdir") != "" || qv.Get("tpage") != "" {
		v.applyPage(tableQueryFrom(qv, "t"))
	}
	return v
}

// tablesAPIView: GET /api/v1/pipelines/{name}/tables. Without query parameters the whole list (as before);
// with any of q, state, since, rows, sort, dir, page, per_page one page.
func (s *Server) tablesAPIView(p *Pipeline, r *http.Request) TablesView {
	v := tablesFromSnap(s.samp.Latest(p.Name))
	qv := r.URL.Query()
	for _, k := range []string{"q", "state", "since", "rows", "sort", "dir", "page", "per_page"} {
		if qv.Has(k) {
			v.applyPage(tableQueryFrom(qv, ""))
			break
		}
	}
	return v
}

// tablesHref: link to the Tables tab with the current query and one parameter changed.
func tablesHref(base string, q *TableQuery, key, val string) string {
	v := url.Values{}
	if q != nil {
		if q.Q != "" {
			v.Set("tq", q.Q)
		}
		if q.State != "all" {
			v.Set("tstate", q.State)
		}
		if q.Since != "" {
			v.Set("tsince", q.Since)
		}
		if q.Rows != "" {
			v.Set("trows", q.Rows)
		}
		if q.Sort != "ord" {
			v.Set("tsort", q.Sort)
		}
		if q.Dir != "" && q.Dir != sortDefaultDir(q.Sort) {
			v.Set("tdir", q.Dir)
		}
		if q.Page > 1 {
			v.Set("tpage", strconv.Itoa(q.Page))
		}
	}
	if key == "tsort" { // a new sort starts in its natural direction
		v.Del("tdir")
	}
	if val == "" || (key == "tstate" && val == "all") || (key == "tsort" && val == "ord") || (key == "tpage" && val == "1") ||
		(key == "tdir" && q != nil && val == sortDefaultDir(q.Sort)) {
		v.Del(key)
	} else {
		v.Set(key, val)
	}
	if key != "tpage" {
		v.Del("tpage")
	}
	if len(v) == 0 {
		return base
	}
	return base + "?" + v.Encode()
}

// tablesSortHref: the link of a sortable column header of the paged list. The active column flips its direction;
// another column starts in its natural direction.
func tablesSortHref(base string, q *TableQuery, sort string) string {
	if q == nil || q.Sort != sort {
		return tablesHref(base, q, "tsort", sort)
	}
	c := *q
	c.Page = 1
	c.Dir = "asc"
	if q.Dir == "asc" {
		c.Dir = "desc"
	}
	return tablesHref(base, &c, "tdir", c.Dir)
}

// tablesAriaSort: aria-sort of a column header ("ascending", "descending" or "none"). Without a query (inline
// list) the page starts sorted by status; the browser keeps it current afterwards.
func tablesAriaSort(q *TableQuery, sort string) string {
	cur, dir := "ord", "asc"
	if q != nil {
		cur, dir = q.Sort, q.Dir
	}
	if cur != sort {
		return "none"
	}
	if dir == "desc" {
		return "descending"
	}
	return "ascending"
}
