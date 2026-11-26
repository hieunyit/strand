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

// TableQuery is the server-side view of the Tables list (query parameters tq, tstate, tsince, trows, tsort, tpage
// on the page; q, state, since, rows, sort, page, per_page on the API).
type TableQuery struct {
	Q     string `json:"q,omitempty"`
	State string `json:"state,omitempty"` // all | running | pending | done
	// Since filters by the last CDC change: a window (5m, 15m, 1h, 6h, 24h, 7d: changed within it), "idle24h"
	// (no change in the last 24 hours, or none recorded) or "none" (no change recorded). Empty: no filter.
	Since string `json:"since,omitempty"`
	// Rows filters by the row count (estimate) as "min-max", either side optional ("1000000-", "-9999", "0-0").
	Rows    string `json:"rows,omitempty"`
	Sort    string `json:"sort,omitempty"` // ord | name | pct | rows | eta | last | changes
	Page    int    `json:"page"`
	PerPage int    `json:"per_page"`
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
	if _, ok := sinceWindows[q.Since]; !ok && q.Since != "none" && q.Since != "idle24h" {
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
		Rows: v.Get(prefix + "rows"), Sort: v.Get(prefix + "sort"),
		Page: atoi(v.Get(prefix + "page")), PerPage: atoi(v.Get(prefix + "per_page"))})
}

// applyPage keeps the tables of the requested page (v.Tables must be finish()ed: status order). Counters and
// aggregates stay those of the whole list.
func (v *TablesView) applyPage(q TableQuery) {
	q = normTableQuery(q)
	v.Paged, v.Query = true, &q
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
	by := func(key func(t *admin.TableInfo) float64) {
		sort.Slice(match, func(i, j int) bool {
			x, y := key(&match[i]), key(&match[j])
			if x != y {
				return x < y
			}
			return ord[match[i].Table] < ord[match[j].Table]
		})
	}
	switch q.Sort {
	case "name":
		sort.Slice(match, func(i, j int) bool { return match[i].Table < match[j].Table })
	case "pct":
		by(func(t *admin.TableInfo) float64 { return -t.ProgressPct })
	case "rows":
		by(func(t *admin.TableInfo) float64 { return -float64(tblEst(*t)) })
	case "eta":
		by(func(t *admin.TableInfo) float64 {
			if t.ETASeconds > 0 {
				return t.ETASeconds
			}
			return 1e18
		})
	case "last": // most recently changed first; never changed last
		by(func(t *admin.TableInfo) float64 {
			if t.LastChangeAt != nil {
				return -float64(t.LastChangeAt.UnixNano())
			}
			return math.MaxFloat64
		})
	case "changes":
		by(func(t *admin.TableInfo) float64 { return -float64(t.Changes()) })
	}
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
		qv.Get("tsort") != "" || qv.Get("tpage") != "" {
		v.applyPage(tableQueryFrom(qv, "t"))
	}
	return v
}

// tablesAPIView: GET /api/v1/pipelines/{name}/tables. Without query parameters the whole list (as before);
// with any of q, state, sort, page, per_page one page.
func (s *Server) tablesAPIView(p *Pipeline, r *http.Request) TablesView {
	v := tablesFromSnap(s.samp.Latest(p.Name))
	qv := r.URL.Query()
	for _, k := range []string{"q", "state", "since", "rows", "sort", "page", "per_page"} {
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
		if q.Page > 1 {
			v.Set("tpage", strconv.Itoa(q.Page))
		}
	}
	if val == "" || (key == "tstate" && val == "all") || (key == "tsort" && val == "ord") || (key == "tpage" && val == "1") {
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
