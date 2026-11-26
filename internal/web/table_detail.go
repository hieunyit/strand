package web

import (
	"encoding/csv"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"strand/internal/admin"
)

// Table page (GET /pipelines/{name}/tables/{table}), its JSON (GET /api/v1/pipelines/{name}/tables/{table}) and
// the CSV export of the Tables list (GET /pipelines/{name}/tables.csv). Everything is read from the sampler's
// memory (latest sample + per-table history): no request calls the admin socket.

// tableEventsMax: newest events mentioning the table shown on its page.
const tableEventsMax = 20

// TableDetailView is the data of the table page and of its API.
type TableDetailView struct {
	Pipeline  string          `json:"pipeline"`
	Table     admin.TableInfo `json:"table"`
	SampledAt time.Time       `json:"sampled_at"`
	Stale     bool            `json:"stale,omitempty"`
	// ApplyDelayS: last_change_at - last_source_at of the newest change (seconds), when both are known.
	ApplyDelayS *float64 `json:"apply_delay_seconds,omitempty"`
	Changes     int64    `json:"changes"`
	// History: one point per minute (history_bucket_seconds) for up to the last hour, since the Web started
	// watching the pipeline; HistoryChanges is their sum.
	HistoryBucketS int              `json:"history_bucket_seconds"`
	History        []TableHistPoint `json:"history"`
	HistoryChanges int64            `json:"history_changes"`
	Quiet          TableQuietInfo   `json:"quiet"`
	// Events: the newest warnings/errors of the pipeline that mention the table (secrets masked).
	Events []LogEvent `json:"events"`

	// Page only.
	Error        string        `json:"-"`
	Mongo        bool          `json:"-"`
	Key          KeyKind       `json:"-"`
	ChangesChart template.HTML `json:"-"`
	DelayChart   template.HTML `json:"-"`
	TablesHref   string        `json:"-"`
	CanVerify    bool          `json:"-"` // operator, job runner present and a name the verify action accepts
	JobActive    *JobStatus    `json:"-"`
	VerifyNote   string        `json:"-"` // why there is no verify button (operator only)
}

// tableDetail builds the view of one table. found=false: the list is available and the table is not in it.
func (s *Server) tableDetail(p *Pipeline, table string, now time.Time) (v TableDetailView, found bool) {
	v = TableDetailView{Pipeline: p.Name, Table: admin.TableInfo{Table: table}, HistoryBucketS: int(histBucket / time.Second),
		History: []TableHistPoint{}, Events: []LogEvent{}, TablesHref: "/pipelines/" + p.Name + "/tables"}
	v.Mongo = p.Cfg != nil && p.Cfg.IsMongoDB()
	sn := s.samp.Latest(p.Name)
	tv := tablesFromSnap(sn)
	if tv.Error != "" {
		v.Error = tv.Error
		return v, true
	}
	found = false
	for i := range tv.Tables {
		if tv.Tables[i].Table == table {
			v.Table, found = tv.Tables[i], true
			break
		}
	}
	if !found {
		return v, false
	}
	v.SampledAt, v.Stale = sn.At, sn.Stale
	t := v.Table
	v.Changes = t.Changes()
	v.Key = keyKind(t)
	if t.LastChangeAt != nil && t.LastSourceAt != nil {
		d := maxf(0, t.LastChangeAt.Sub(*t.LastSourceAt).Seconds())
		v.ApplyDelayS = &d
	}
	ref := sn.At
	if ref.IsZero() {
		ref = now
	}
	v.History, v.Quiet = s.samp.TableHistory(p.Name, table, ref)
	if v.History == nil {
		v.History = []TableHistPoint{}
	}
	v.Quiet.Quiet = t.Quiet
	chg := make([]Point, 0, len(v.History))
	var delay []Point
	for _, h := range v.History {
		v.HistoryChanges += h.Changes
		chg = append(chg, Point{T: h.T, V: float64(h.Changes)})
		if h.MaxDelay != nil {
			delay = append(delay, Point{T: h.T, V: *h.MaxDelay})
		}
	}
	v.ChangesChart = LineChart("Changes per minute", "rows", []ChartSeries{{Name: "changes", Class: "s1", Points: chg}}, 560, 190)
	v.DelayChart = LineChart("Sync delay", "seconds, largest per minute", []ChartSeries{{Name: "delay", Class: "s2", Points: delay}}, 560, 190)
	v.Events = tableEvents(sn.Events, table)
	return v, true
}

// tableEvents: the newest events (at most tableEventsMax, newest first) whose message names the table, as
// "db.table" or quoted ("`db`.`table`", "\"db\".\"table\"").
func tableEvents(ev []admin.EventRecord, table string) []LogEvent {
	needles := []string{table}
	if db, tb, ok := strings.Cut(table, "."); ok {
		needles = append(needles, "`"+db+"`.`"+tb+"`", `"`+db+`"."`+tb+`"`)
	}
	out := []LogEvent{}
	all := EventsToLog(ev)
	for i := len(all) - 1; i >= 0 && len(out) < tableEventsMax; i-- {
		for _, n := range needles {
			if strings.Contains(all[i].Message, n) {
				e := all[i]
				e.Message, e.Raw = MaskSecrets(e.Message), ""
				out = append(out, e)
				break
			}
		}
	}
	return out
}

// verifyTableRE: the names the verify action takes for one table (actions_ops.go).
func verifyTableOK(mongo bool, table string) bool {
	if mongo {
		return mongoCollRE.MatchString(table)
	}
	return tableRE.MatchString(table)
}

// tableHref: the page of one table (the name is one escaped path segment: dots, quotes, slashes are safe).
func tableHref(pipeline, table string) string {
	return "/pipelines/" + pipeline + "/tables/" + url.PathEscape(table)
}

func (s *Server) pageTableDetail(w http.ResponseWriter, r *http.Request, sess *Session) {
	p := s.reg.Get(r.PathValue("name"))
	if p == nil {
		http.NotFound(w, r)
		return
	}
	table := r.PathValue("table")
	v, found := s.tableDetail(p, table, s.now())
	if !found {
		s.renderError(w, r, sess, http.StatusNotFound, "This pipeline has no table "+strconv.Quote(table)+".")
		return
	}
	if RoleAllows(sess.Role, PermOperator) {
		switch {
		case s.jobRunner() == nil:
			v.VerifyNote = "The job runner is not configured."
		case !verifyTableOK(v.Mongo, table):
			v.VerifyNote = "The verify job does not accept this name (expected database.table)."
		default:
			v.CanVerify = true
			for _, j := range s.jobRunner().List(p.Name) {
				if j.Running {
					jj := j
					v.JobActive = &jj
					break
				}
			}
		}
	}
	crumbs := []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Pipelines", URL: "/pipelines"},
		{Label: p.Name, URL: "/pipelines/" + p.Name}, {Label: "Tables", URL: v.TablesHref}, {Label: table}}
	if v.Mongo {
		crumbs[3].Label = "Collections"
	}
	s.render(w, "table_detail", sess, pageData{Title: table, DocTitle: table + " · " + p.Name, Subtitle: "Table of pipeline " + p.Name,
		Active: p.Name, Breadcrumbs: crumbs, Refresh: int(s.samp.Interval() / time.Second), Body: v})
}

func (s *Server) apiTableDetail(w http.ResponseWriter, r *http.Request, _ *Session) {
	p := s.lookup(w, r)
	if p == nil {
		return
	}
	v, found := s.tableDetail(p, r.PathValue("table"), s.now())
	switch {
	case !found:
		writeErr(w, http.StatusNotFound, "unknown table")
	case v.Error != "":
		writeErr(w, http.StatusServiceUnavailable, v.Error)
	default:
		writeJSON(w, http.StatusOK, v)
	}
}

// csvSafe neutralises spreadsheet formulas: a cell starting with = + - @ (or a tab / carriage return, which some
// spreadsheets skip before reading a formula) gets a leading single quote. Table names come from the database.
func csvSafe(s string) string {
	if s != "" && strings.ContainsRune("=+-@\t\r", rune(s[0])) {
		return "'" + s
	}
	return s
}

var unsafeFileChars = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// pageTablesCSV: GET /pipelines/{name}/tables.csv with the filters and the sort of the Tables tab (tq, tstate,
// tsince, trows, tsort, tdir): every matching table, not one page.
func (s *Server) pageTablesCSV(w http.ResponseWriter, r *http.Request, _ *Session) {
	p := s.reg.Get(r.PathValue("name"))
	if p == nil {
		http.NotFound(w, r)
		return
	}
	tv := tablesFromSnap(s.samp.Latest(p.Name))
	if tv.Error != "" {
		http.Error(w, "table status is not available: "+tv.Error, http.StatusServiceUnavailable)
		return
	}
	rows := tv.matching(tableQueryFrom(r.URL.Query(), "t"))
	name := "strand-" + strings.Trim(unsafeFileChars.ReplaceAllString(p.Name, "_"), "._-") + "-tables-" + s.now().UTC().Format("20060102T150405Z") + ".csv"
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"table", "state", "progress_pct", "rows_copied", "rows_estimate", "last_change_at", "last_source_at",
		"inserts", "updates", "deletes", "changes", "eta_seconds", "key", "quiet"})
	ts := func(t *time.Time) string {
		if t == nil {
			return ""
		}
		return t.UTC().Format(time.RFC3339)
	}
	for _, t := range rows {
		eta := ""
		if t.ETASeconds > 0 && t.State != "done" {
			eta = strconv.FormatFloat(t.ETASeconds, 'f', 0, 64)
		}
		rec := []string{t.Table, t.State, strconv.FormatFloat(t.ProgressPct, 'f', 2, 64), strconv.FormatInt(tblCopied(t), 10),
			strconv.FormatInt(tblEst(t), 10), ts(t.LastChangeAt), ts(t.LastSourceAt), strconv.FormatInt(t.Inserts, 10),
			strconv.FormatInt(t.Updates, 10), strconv.FormatInt(t.Deletes, 10), strconv.FormatInt(t.Changes(), 10), eta, t.Key,
			strconv.FormatBool(t.Quiet)}
		for i := range rec {
			rec[i] = csvSafe(rec[i])
		}
		if err := cw.Write(rec); err != nil {
			return
		}
	}
	cw.Flush()
}

// tablesCSVHref: the CSV export link with the current filters and sort (never the page).
func tablesCSVHref(pipeline string, q *TableQuery) string {
	h := tablesHref("/pipelines/"+pipeline+"/tables.csv", q, "tpage", "1")
	return h
}
