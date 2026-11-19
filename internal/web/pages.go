package web

import (
	"bytes"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/admin"
	"strand/internal/config"
)

func unimplementedWarn(k string) string { return config.UnimplementedWarning(k) }

var pageNames = []string{"overview", "pipelines", "detail", "jobs", "job", "wizard", "new_pipeline", "settings"}

func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"stateClass": func(s string) string { return "st-" + s },
		"secs":       fmtSecs,
		"lag":        fmtLag,
		"governor":   fmtGovernor,
		"tblRows":    tblRowsText,
		"tblETA":     tblETAText,
		"num":        fmtNum,
		"int64":      func(v int64) string { return fmtInt(v) },
		"clock": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			return t.Local().Format("15:04:05 MST")
		},
		"stateReason": stateReason,
		"lagSort":     lagSort,
		"stateSev":    stateSev,
		"lagClass":    lagClass,
		"bytes":       fmtBytes,
		"ts": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			return t.Local().Format("2006-01-02 15:04:05 MST")
		},
		"pct":       func(v float64) string { return fmt.Sprintf("%.1f%%", v) },
		"bar":       ProgressBar,
		"bigbar":    BigProgressBar,
		"ranges":    fmtRanges,
		"lower":     strings.ToLower,
		"ptrInt":    func(p *int) int { return *p },
		"deref":     func(p *float64) float64 { return *p },
		"stateKey":  func(s string) string { return s },
		"icon":      icon,
		"tabIcon":   tabIcon,
		"engines":   engines,
		"keyKind":   keyKind,
		"tblCopied": tblCopied,
		"tblEst":    tblEst,
		"tablesOf":  tablesOf,
		"rel":       func(t time.Time) string { return fmtRel(t, time.Now()) },
		"join":      strings.Join,
		"jobState":  jobState,
		"jobClass":  jobClass,
		"jobDur":    jobDur,
		"deref2":    func(t *time.Time) time.Time { return *t },
		"jobsCtx": func(j []JobStatus, showPipeline bool) struct {
			Jobs         []JobStatus
			ShowPipeline bool
		} {
			return struct {
				Jobs         []JobStatus
				ShowPipeline bool
			}{j, showPipeline}
		},
	}
}

func parsePages() (map[string]*template.Template, error) {
	out := map[string]*template.Template{}
	for _, n := range pageNames {
		t, err := template.New(n).Funcs(templateFuncs()).ParseFS(templateFS, "templates/base.html", "templates/"+n+".html")
		if err != nil {
			return nil, fmt.Errorf("parse template %s: %w", n, err)
		}
		out[n] = t
	}
	t, err := template.New("login").Funcs(templateFuncs()).ParseFS(templateFS, "templates/login.html")
	if err != nil {
		return nil, err
	}
	out["login"] = t
	return out, nil
}

func jobState(j JobStatus) string {
	switch {
	case j.Running && j.Cancelled:
		return "cancelling"
	case j.Running:
		return "running"
	case j.Cancelled:
		return "cancelled"
	case j.Interrupted:
		return "interrupted"
	case j.ExitCode == nil:
		return "unknown exit"
	case *j.ExitCode == 0:
		return "ok"
	}
	return "exit " + strconv.Itoa(*j.ExitCode)
}

func jobClass(j JobStatus) string {
	switch {
	case j.Running:
		return "st-bootstrapping"
	case j.ExitCode != nil && *j.ExitCode == 0 && !j.Cancelled:
		return "st-synced"
	case j.ExitCode == nil && !j.Cancelled:
		return "st-paused"
	}
	return "st-failed"
}

func jobDur(j JobStatus) string {
	end := time.Now()
	if j.Ended != nil {
		end = *j.Ended
	}
	return fmtSecs(end.Sub(j.Started).Seconds())
}

func fmtSecs(v float64) string {
	switch {
	case v <= 0 || math.IsNaN(v):
		return "-"
	case v < 1:
		return fmt.Sprintf("%.0f ms", v*1000)
	case v < 60:
		return fmt.Sprintf("%.1f s", v)
	}
	d := time.Duration(v * float64(time.Second)).Round(time.Second)
	days, h, m, s := int(d/(24*time.Hour)), int(d/time.Hour)%24, int(d/time.Minute)%60, int(d/time.Second)%60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh %02dm", days, h, m)
	case h > 0:
		return fmt.Sprintf("%dh %02dm %02ds", h, m, s)
	}
	return fmt.Sprintf("%dm %02ds", m, s)
}

// fmtLag formats a replication lag with stable units so the digits do not jump between ms and s:
// "0.25 s" below 10 s, "12.3 s" below a minute, then "4m 05s" and "2h 03m".
func fmtLag(v float64) string {
	switch {
	case math.IsNaN(v) || v <= 0:
		return "0.00 s"
	case v < 10:
		return fmt.Sprintf("%.2f s", v)
	case v < 60:
		return fmt.Sprintf("%.1f s", v)
	}
	d := time.Duration(v * float64(time.Second)).Round(time.Second)
	h, m, sec := int(d/time.Hour), int(d/time.Minute)%60, int(d/time.Second)%60
	if h > 0 {
		return fmt.Sprintf("%dh %02dm", h, m)
	}
	return fmt.Sprintf("%dm %02ds", m, sec)
}

// groupDigits inserts thousands separators into a plain integer string.
func groupDigits(s string) string {
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

func fmtInt(v int64) string { return groupDigits(strconv.FormatInt(v, 10)) }

// fmtNum formats a rate or count for tables: thousands separators, one decimal below 100.
func fmtNum(v float64) string {
	switch {
	case math.IsNaN(v) || math.IsInf(v, 0):
		return "-"
	case math.Abs(v) >= 100 || v == 0:
		return groupDigits(strconv.FormatFloat(math.Round(v), 'f', 0, 64))
	}
	return strconv.FormatFloat(math.Round(v*10)/10, 'f', 1, 64)
}

// stateSev orders states for operators: the ones that need attention come first.
func stateSev(s string) int {
	switch s {
	case StateFailed:
		return 0
	case StateUnreachable:
		return 1
	case StateUnknown:
		return 2
	case StatePaused:
		return 3
	case StateStarting:
		return 4
	case StateBootstrapping:
		return 5
	case StateStopped:
		return 6
	}
	return 7 // synced
}

// lagSort is the numeric sort key of the lag column (-1 when there is no reading).
func lagSort(v PipelineView) float64 {
	if v.Top == nil {
		return -1
	}
	return math.Max(v.Top.LagSeconds, 0)
}

// lagClass colours the lag (thresholds LagWarnSeconds / LagBadSeconds with hysteresis, computed by the sampler).
func lagClass(v PipelineView) string {
	if v.Top == nil || v.LagLevel == "" {
		return ""
	}
	return "lag-" + v.LagLevel
}

// fmtGovernor is the governor line of the Run card (also sent live).
func fmtGovernor(g admin.GovernorInfo) string {
	s := g.Algorithm
	if g.Zone != "" {
		s += ", zone " + g.Zone
	}
	s += ", rate " + fmtNum(g.BackgroundRate) + "/s background, pressure " + fmt.Sprintf("%.2f", g.Pressure)
	if g.BreakerOpen {
		s += ", breaker open: " + g.BreakerReason
	}
	return s
}

// stateReason says why a pipeline is not healthy (empty when there is nothing to add).
func stateReason(v PipelineView) string {
	switch v.State {
	case StateFailed:
		switch {
		case v.ConfigErr != "":
			return "config error: " + v.ConfigErr
		case v.Top != nil && v.Top.BreakerOpen && v.Top.BreakerReason != "":
			return "breaker open: " + v.Top.BreakerReason
		case v.Runtime.LastError != "":
			return v.Runtime.LastError
		case v.LastExit != nil:
			return fmt.Sprintf("exit %d: %s", v.LastExit.Code, v.LastExit.Meaning)
		}
		return "the pipeline reported a failure"
	case StateUnreachable:
		return "process is running but the admin socket does not answer"
	case StatePaused:
		if v.Top != nil && v.Top.BreakerOpen && v.Top.BreakerReason != "" {
			return "breaker open: " + v.Top.BreakerReason
		}
		return "replication is paused (use Resume)"
	case StateUnknown:
		if v.ConfigErr != "" {
			return "config error: " + v.ConfigErr
		}
		return "no sample yet"
	}
	if v.Top != nil && v.Top.BreakerOpen {
		return "breaker open: " + v.Top.BreakerReason
	}
	return ""
}

func fmtBytes(v float64) string {
	const k = 1024.0
	switch {
	case v < k:
		return fmt.Sprintf("%.0f B", v)
	case v < k*k:
		return fmt.Sprintf("%.1f KiB", v/k)
	case v < k*k*k:
		return fmt.Sprintf("%.1f MiB", v/(k*k))
	}
	return fmt.Sprintf("%.2f GiB", v/(k*k*k))
}

func fmtRanges(m map[string]int) string {
	if len(m) == 0 {
		return "-"
	}
	var parts []string
	for _, k := range sortedKeys(m) {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	return strings.Join(parts, ", ")
}

type pageData struct {
	Title    string
	DocTitle string    // <title> text; defaults to Title
	AsOf     time.Time // when the page was rendered (shown as "data as of")
	Active   string    // overview | pipelines | tên pipeline
	CSRF     string
	Nav      []NavItem
	Version  string
	Host     string // host name shown in the top bar
	Refresh  int    // giây; 0 = không tự làm mới toàn trang
	// Live: trang nhận số liệu qua SSE ("all" hoặc tên pipeline); "" = trang tĩnh. Interval: chu kỳ của sampler.
	Live     string
	LiveLogs bool
	Interval int
	Body     any
}

func (s *Server) render(w http.ResponseWriter, name string, sess *Session, d pageData) {
	d.CSRF, d.Nav, d.Version = sess.CSRF, s.navItems(), s.version
	d.Host = hostName()
	d.Interval = int(s.samp.Interval().Seconds())
	if d.AsOf.IsZero() {
		d.AsOf = s.now()
	}
	var buf bytes.Buffer
	if err := s.pages[name].ExecuteTemplate(&buf, "base", d); err != nil {
		s.log.Error("render page", "page", name, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(buf.Bytes())
}

type overviewBody struct {
	Overview Overview
	Healthy  int            // pipelines in the synced state
	Attn     []PipelineView // failed, unreachable, paused, unknown or breaker open (worst first)
	TblDone  int64          // tables done and total across all pipelines (snapshot)
	TblTotal int64
	TblPct   float64
	States   []string
	Views    []PipelineView
	Sparks   map[string]template.HTML
	Warnings []string
}

func (s *Server) sparkFor(name string) template.HTML {
	smp := s.samp.Series(name)
	if n := len(smp); n > sparkPoints {
		smp = smp[n-sparkPoints:] // 5 phút gần nhất
	}
	vals := make([]float64, len(smp))
	for i, x := range smp {
		vals[i] = x.Lag
	}
	return Sparkline(vals, 90, 22)
}

// sparkPoints is the number of lag samples in the overview sparkline (5 min at the default interval).
const sparkPoints = 60

// sortViews puts the pipelines that need attention first (failed, unreachable, ...), then by lag, then by name.
func sortViews(vs []PipelineView) {
	sort.SliceStable(vs, func(i, j int) bool {
		a, b := vs[i], vs[j]
		if sa, sb := stateSev(a.State), stateSev(b.State); sa != sb {
			return sa < sb
		}
		if la, lb := lagSort(a), lagSort(b); la != lb {
			return la > lb
		}
		return a.Name < b.Name
	})
}

// summarize fills the aggregates of the overview from the (already sorted) views.
func (b *overviewBody) summarize() {
	for _, v := range b.Views {
		if needsAttention(v) {
			b.Attn = append(b.Attn, v)
		} else if v.State == StateSynced {
			b.Healthy++
		}
		if tp := tablesOf(v); tp.Total > 0 {
			b.TblDone += tp.Done
			b.TblTotal += tp.Total
		}
	}
	if b.TblTotal > 0 {
		b.TblPct = float64(b.TblDone) * 100 / float64(b.TblTotal)
	}
}

// needsAttention: failed, unreachable, unknown, paused, breaker open or a config error.
func needsAttention(v PipelineView) bool {
	return stateSev(v.State) <= 3 || (v.Top != nil && v.Top.BreakerOpen) || v.ConfigErr != ""
}

func (s *Server) pageOverview(w http.ResponseWriter, r *http.Request, sess *Session) {
	b := overviewBody{Overview: s.samp.BuildOverview(), States: AllStates, Sparks: map[string]template.HTML{}, Warnings: s.reg.Warnings()}
	for _, p := range s.reg.List() {
		b.Views = append(b.Views, s.view(p))
		b.Sparks[p.Name] = s.sparkFor(p.Name)
	}
	sortViews(b.Views)
	b.summarize()
	s.render(w, "overview", sess, pageData{Title: "Overview", Active: "overview", Live: "all", Body: b})
}

func (s *Server) pagePipelines(w http.ResponseWriter, r *http.Request, sess *Session) {
	b := overviewBody{Overview: s.samp.BuildOverview(), States: AllStates, Sparks: map[string]template.HTML{}, Warnings: s.reg.Warnings()}
	for _, p := range s.reg.List() {
		b.Views = append(b.Views, s.view(p))
		b.Sparks[p.Name] = s.sparkFor(p.Name)
	}
	sortViews(b.Views)
	s.render(w, "pipelines", sess, pageData{Title: "Pipelines", Active: "pipelines", Live: "all", Body: b})
}

// detailBody là dữ liệu của trang chi tiết; chỉ trường của tab đang xem được điền.
type detailBody struct {
	Tab        string
	View       PipelineView
	Tabs       []tabLink
	Status     StatusView
	Graph      template.HTML
	Charts     []chartCard
	HasHistory bool // chế độ DB: lịch sử số liệu quá 1 giờ (6 h, 24 h, 7 d) trong DB
	Metrics    map[string]float64
	Tables     TablesView
	Checkpoint CheckpointView
	Errors     ErrorsView
	Config     ConfigView
	Tuning     TuningView
	Env        EnvView
	Logs       LogsView
	Follow     bool
	Jobs       []JobStatus
	Src        SourceTablesView
	SrcLoaded  bool // Src có sẵn trong cache; không thì trình duyệt tải khi người dùng bấm (không gọi socket khi render)
	Ctl        Controls
	Head       HeadInfo
}

// Controls quyết định nút nào hiện ở trang chi tiết (chỉ là gợi ý UI: API vẫn kiểm lại).
type Controls struct {
	Running   bool // tiến trình đang chạy
	Systemd   bool
	Reachable bool // admin socket trả lời
	Paused    bool
	CanRewind bool // MySQL → MySQL và socket trả lời (CLI chỉ cho phép loại này)
	CfgOK     bool
	JobActive *JobStatus // job đang chạy của pipeline này (nếu có)
}

func (s *Server) controls(p *Pipeline, v PipelineView) Controls {
	c := Controls{Running: v.Runtime.Running || v.Reachable, Systemd: p.Mode() == ModeSystemd, Reachable: v.Reachable,
		Paused: v.State == StatePaused, CfgOK: p.CfgErr == ""}
	c.CanRewind = v.Reachable && p.Cfg != nil && p.Cfg.Source.Kind == "mysql" && p.Cfg.Target.Kind == "mysql"
	if s.jobs != nil {
		for _, j := range s.jobs.List(p.Name) {
			if j.Running {
				jj := j
				c.JobActive = &jj
				break
			}
		}
	}
	return c
}

var tabLabels = map[string]string{"graph": "Graph", "tables": "Tables", "checkpoint": "Checkpoint", "errors": "Errors", "config": "Config", "logs": "Logs", "jobs": "Jobs", "repair": "Repair", "promote": "Promote"}

func tabIcon(key string) string {
	if i, ok := map[string]string{"graph": "graph", "tables": "table", "checkpoint": "flag", "errors": "alert", "config": "config", "logs": "logs", "jobs": "jobs", "repair": "repair", "promote": "promote"}[key]; ok {
		return i
	}
	return "pipelines"
}

type tabLink struct{ Key, Label, Href string }

func (s *Server) pageDetail(w http.ResponseWriter, r *http.Request, sess *Session, tab string) {
	p := s.reg.Get(r.PathValue("name"))
	if p == nil {
		http.NotFound(w, r)
		return
	}
	if tab == "" {
		tab = "graph"
	}
	base := "/pipelines/" + p.Name
	b := detailBody{Tab: tab, View: s.view(p)}
	b.Ctl = s.controls(p, b.View)
	for _, t := range [][2]string{{"graph", "Graph"}, {"tables", "Tables"}, {"checkpoint", "Checkpoint"}, {"errors", "Errors"}, {"config", "Config"}, {"logs", "Logs"}, {"jobs", "Jobs"}, {"repair", "Repair"}, {"promote", "Promote"}} {
		href := base
		if t[0] != "graph" {
			href += "/" + t[0]
		}
		b.Tabs = append(b.Tabs, tabLink{Key: t[0], Label: t[1], Href: href})
	}
	// Mọi số liệu đọc từ bộ nhớ của sampler: render trang không bao giờ gọi admin socket.
	ctx := r.Context()
	b.Head = s.headInfo(ctx, p)
	live, liveLogs := p.Name, false
	switch tab {
	case "graph":
		b.Status = s.statusView(ctx, p)
		if sn := s.samp.Latest(p.Name); sn != nil {
			b.Metrics = sn.Metrics
		}
		b.Graph = PipelineGraph(s.graphNodes(b.View, b.Status.Status, b.Metrics))
		b.HasHistory = s.db != nil
		b.Charts = s.charts(p.Name)
	case "tables":
		b.Tables = s.tablesView(ctx, p)
		if b.Ctl.Running {
			b.Src, b.SrcLoaded = s.cachedSourceTables(p)
		}
	case "jobs":
		b.Jobs = s.listJobs(p.Name)
		if b.Ctl.Running {
			b.Src, b.SrcLoaded = s.cachedSourceTables(p)
		}
	case "checkpoint":
		b.Checkpoint = s.checkpointView(ctx, p)
		b.Status = s.statusView(ctx, p)
	case "errors":
		b.Errors = s.errorsView(ctx, p)
	case "config":
		_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "config.view", Pipeline: p.Name})
		b.Config = s.configView(p)
		b.Tuning = s.tuningView(p)
		b.Env = s.getEnvView(p)
	case "logs":
		n, _ := strconv.Atoi(r.URL.Query().Get("lines"))
		b.Logs = s.logsView(ctx, p, n)
		b.Follow = r.URL.Query().Get("follow") != "0" // theo dõi đuôi log trực tiếp, mặc định bật
		liveLogs = true
	}
	s.render(w, "detail", sess, pageData{Title: p.Name, DocTitle: p.Name + " \u00b7 " + tabLabels[tab], Active: p.Name, Live: live, LiveLogs: liveLogs, Body: b})
}

// chartCard is one chart of the Graph tab; Key names it for the live redraw in app.js.
type chartCard struct {
	Key  string
	HTML template.HTML
}

func (s *Server) charts(name string) []chartCard {
	smp := s.samp.Series(name)
	pick := func(f func(Sample) float64) []Point {
		pts := make([]Point, len(smp))
		for i, x := range smp {
			pts[i] = Point{T: x.T, V: f(x)}
		}
		return pts
	}
	// Keys and series must match CHARTS in app.js (the live redraw).
	return []chartCard{
		{"lag", LineChart("Replication lag", "seconds", []ChartSeries{{Name: "lag", Class: "s1", Points: pick(func(x Sample) float64 { return x.Lag })}}, 560, 190)},
		{"rate", LineChart("Throughput", "rows/s", []ChartSeries{
			{Name: "applied, 30 s avg", Class: "s1", Points: pick(func(x Sample) float64 { return x.Rows })},
			{Name: "snapshot, 30 s avg", Class: "s2", Points: pick(func(x Sample) float64 { return x.SnapRows })},
		}, 560, 190)},
		{"holes", LineChart("Holes and writeset", "count", []ChartSeries{
			{Name: "holes", Class: "s3", Points: pick(func(x Sample) float64 { return x.Holes })},
			{Name: "writeset entries", Class: "s2", Points: pick(func(x Sample) float64 { return x.Writeset })},
		}, 560, 190)},
		{"spool", LineChart("Spool", "bytes", []ChartSeries{{Name: "spool bytes", Class: "s4", Points: pick(func(x Sample) float64 { return x.SpoolBytes })}}, 560, 190)},
	}
}

func mval(m map[string]float64, k string) string {
	if v, ok := m[k]; ok {
		return FormatValue(v)
	}
	return "-"
}

// graphNodes dựng sơ đồ Source → Assembler → Scheduler → Writer×N → Target với số liệu có sẵn.
func (s *Server) graphNodes(v PipelineView, st *admin.StatusInfo, m map[string]float64) []GraphNode {
	cls := "idle"
	switch v.State {
	case StateSynced:
		cls = "ok"
	case StateBootstrapping, StatePaused, StateStarting:
		cls = "warn"
	case StateFailed:
		cls = "bad"
	}
	srcKind, dstKind := "?", "?"
	if parts := strings.SplitN(v.Pair, " → ", 2); len(parts) == 2 {
		srcKind, dstKind = parts[0], parts[1]
	}
	lag, rate := "-", "-"
	if v.Top != nil {
		lag, rate = fmtLag(v.Top.LagSeconds), FormatValue(v.Rates.Rows)+" rows/s"
	}
	workers, order, zone, txns, rows := "-", "-", "-", "-", "-"
	if st != nil {
		workers = strconv.Itoa(st.Workers)
		order = st.CommitOrder
		zone = st.Governor.Zone
		if zone == "" {
			zone = st.Governor.Algorithm
		}
		txns, rows = FormatValue(float64(st.AppliedTxns)), FormatValue(float64(st.AppliedRows))
	}
	return []GraphNode{
		{Title: "Source", Class: cls, Lines: []string{srcKind, "lag " + lag, "state " + v.State}, Binds: []string{"", "g_lag", "g_state"}},
		{Title: "Assembler", Class: cls, Lines: []string{"pending x-range " + mval(m, "strand_pending_cross_range"), "spool " + fmtBytesOrDash(m, "strand_spool_bytes")}, Binds: []string{"g_xrange", "g_spool"}},
		{Title: "Scheduler", Class: cls, Lines: []string{"holes " + mval(m, "strand_holes"), "writeset " + mval(m, "strand_writeset_entries"), "order " + order}, Binds: []string{"g_holes", "g_ws", ""}},
		{Title: "Writer x" + workers, Class: cls, Lines: []string{rate, "governor " + zone}, Binds: []string{"g_rate", "g_zone"}},
		{Title: "Target", Class: cls, Lines: []string{dstKind, "txns " + txns, "rows " + rows}, Binds: []string{"", "g_txns", "g_rows"}},
	}
}

func fmtBytesOrDash(m map[string]float64, k string) string {
	if v, ok := m[k]; ok {
		return fmtBytes(v)
	}
	return "-"
}
