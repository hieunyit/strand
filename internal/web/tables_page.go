package web

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

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

// TableQuery is the server-side view of the Tables list (query parameters tq, tstate, tsort, tpage on the page;
// q, state, sort, page, per_page on the API).
type TableQuery struct {
	Q       string `json:"q,omitempty"`
	State   string `json:"state,omitempty"` // all | running | pending | done
	Sort    string `json:"sort,omitempty"`  // ord | name | pct | rows | eta
	Page    int    `json:"page"`
	PerPage int    `json:"per_page"`
}

func normTableQuery(q TableQuery) TableQuery {
	q.Q = strings.TrimSpace(q.Q)
	switch q.State {
	case "running", "pending", "done":
	default:
		q.State = "all"
	}
	switch q.Sort {
	case "name", "pct", "rows", "eta":
	default:
		q.Sort = "ord"
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
	return normTableQuery(TableQuery{Q: v.Get(prefix + "q"), State: v.Get(prefix + "state"), Sort: v.Get(prefix + "sort"),
		Page: atoi(v.Get(prefix + "page")), PerPage: atoi(v.Get(prefix + "per_page"))})
}

// applyPage keeps the tables of the requested page (v.Tables must be finish()ed: status order). Counters and
// aggregates stay those of the whole list.
func (v *TablesView) applyPage(q TableQuery) {
	q = normTableQuery(q)
	v.Paged, v.Query = true, &q
	needle := strings.ToLower(q.Q)
	match := v.Tables[:0:0]
	for _, t := range v.Tables {
		if (q.State == "all" || tblState(t) == q.State) && (needle == "" || strings.Contains(strings.ToLower(t.Table), needle)) {
			match = append(match, t)
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
	if v.Total > tablesInlineMax || qv.Get("tq") != "" || qv.Get("tstate") != "" || qv.Get("tsort") != "" || qv.Get("tpage") != "" {
		v.applyPage(tableQueryFrom(qv, "t"))
	}
	return v
}

// tablesAPIView: GET /api/v1/pipelines/{name}/tables. Without query parameters the whole list (as before);
// with any of q, state, sort, page, per_page one page.
func (s *Server) tablesAPIView(p *Pipeline, r *http.Request) TablesView {
	v := tablesFromSnap(s.samp.Latest(p.Name))
	qv := r.URL.Query()
	for _, k := range []string{"q", "state", "sort", "page", "per_page"} {
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
