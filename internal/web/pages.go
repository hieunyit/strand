package web

import (
	"bytes"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"strand/internal/admin"
	"strand/internal/config"
)

func unimplementedWarn(k string) string { return config.UnimplementedWarning(k) }

var pageNames = []string{"overview", "pipelines", "detail", "jobs", "job"}

func templateFuncs() template.FuncMap {
	return template.FuncMap{
		"stateClass": func(s string) string { return "st-" + s },
		"secs":       fmtSecs,
		"num":        func(v float64) string { return FormatValue(v) },
		"int64":      func(v int64) string { return FormatValue(float64(v)) },
		"bytes":      fmtBytes,
		"ts": func(t time.Time) string {
			if t.IsZero() {
				return "-"
			}
			return t.Local().Format("2006-01-02 15:04:05")
		},
		"pct":      func(v float64) string { return fmt.Sprintf("%.1f%%", v) },
		"bar":      ProgressBar,
		"ranges":   fmtRanges,
		"lower":    strings.ToLower,
		"ptrInt":   func(p *int) int { return *p },
		"deref":    func(p *float64) float64 { return *p },
		"stateKey": func(s string) string { return s },
		"join":     strings.Join,
		"jobState": jobState,
		"jobClass": jobClass,
		"jobDur":   jobDur,
		"deref2":   func(t *time.Time) time.Time { return *t },
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
	return time.Duration(v * float64(time.Second)).Round(time.Second).String()
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
	Title   string
	Active  string // overview | pipelines | tên pipeline
	CSRF    string
	Nav     []NavItem
	Version string
	Refresh int // giây; 0 = không tự làm mới
	Body    any
}

func (s *Server) render(w http.ResponseWriter, name string, sess *Session, d pageData) {
	d.CSRF, d.Nav, d.Version = sess.CSRF, s.navItems(), s.version
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
	States   []string
	Views    []PipelineView
	Sparks   map[string]template.HTML
	Warnings []string
}

func (s *Server) sparkFor(name string) template.HTML {
	smp := s.samp.Series(name)
	if n := len(smp); n > 60 {
		smp = smp[n-60:] // 5 phút gần nhất
	}
	vals := make([]float64, len(smp))
	for i, x := range smp {
		vals[i] = x.Lag
	}
	return Sparkline(vals, 90, 22)
}

func (s *Server) pageOverview(w http.ResponseWriter, r *http.Request, sess *Session) {
	b := overviewBody{Overview: s.samp.BuildOverview(), States: AllStates, Sparks: map[string]template.HTML{}, Warnings: s.reg.Warnings()}
	for _, p := range s.reg.List() {
		b.Views = append(b.Views, s.view(p))
		b.Sparks[p.Name] = s.sparkFor(p.Name)
	}
	s.render(w, "overview", sess, pageData{Title: "Overview", Active: "overview", Refresh: 5, Body: b})
}

func (s *Server) pagePipelines(w http.ResponseWriter, r *http.Request, sess *Session) {
	b := overviewBody{Overview: s.samp.BuildOverview(), States: AllStates, Sparks: map[string]template.HTML{}, Warnings: s.reg.Warnings()}
	for _, p := range s.reg.List() {
		b.Views = append(b.Views, s.view(p))
		b.Sparks[p.Name] = s.sparkFor(p.Name)
	}
	s.render(w, "pipelines", sess, pageData{Title: "Pipelines", Active: "pipelines", Refresh: 5, Body: b})
}

// detailBody là dữ liệu của trang chi tiết; chỉ trường của tab đang xem được điền.
type detailBody struct {
	Tab        string
	View       PipelineView
	Tabs       []tabLink
	Status     StatusView
	Graph      template.HTML
	Charts     []template.HTML
	Metrics    map[string]float64
	Tables     TablesView
	Checkpoint CheckpointView
	Errors     ErrorsView
	Config     ConfigView
	Logs       LogsView
	Follow     bool
	Jobs       []JobStatus
	Src        SourceTablesView
	Ctl        Controls
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
	for _, t := range [][2]string{{"graph", "Graph"}, {"tables", "Tables"}, {"checkpoint", "Checkpoint"}, {"errors", "Errors"}, {"config", "Config"}, {"logs", "Logs"}, {"jobs", "Jobs"}} {
		href := base
		if t[0] != "graph" {
			href += "/" + t[0]
		}
		b.Tabs = append(b.Tabs, tabLink{Key: t[0], Label: t[1], Href: href})
	}
	ctx := r.Context()
	refresh := 5
	switch tab {
	case "graph":
		b.Status = s.statusView(ctx, p)
		if sn := s.samp.Latest(p.Name); sn != nil {
			b.Metrics = sn.Metrics
		}
		b.Graph = PipelineGraph(s.graphNodes(b.View, b.Status.Status, b.Metrics))
		b.Charts = s.charts(p.Name)
	case "tables":
		b.Tables = s.tablesView(ctx, p)
		if b.Ctl.Reachable {
			b.Src = s.sourceTablesView(ctx, p)
		}
	case "jobs":
		b.Jobs = s.listJobs(p.Name)
		if b.Ctl.Reachable {
			b.Src = s.sourceTablesView(ctx, p)
		}
	case "checkpoint":
		b.Checkpoint = s.checkpointView(ctx, p)
		b.Status = s.statusView(ctx, p)
	case "errors":
		b.Errors = s.errorsView(ctx, p)
	case "config":
		_ = s.audit.Log(AuditEntry{IP: s.clientIP(r), Action: "config.view", Pipeline: p.Name})
		b.Config = s.configView(p)
		refresh = 0
	case "logs":
		n, _ := strconv.Atoi(r.URL.Query().Get("lines"))
		b.Logs = s.logsView(ctx, p, n)
		b.Follow = r.URL.Query().Get("follow") == "1"
		refresh = 0
		if b.Follow {
			refresh = 5
		}
	}
	s.render(w, "detail", sess, pageData{Title: p.Name, Active: p.Name, Refresh: refresh, Body: b})
}

func (s *Server) charts(name string) []template.HTML {
	smp := s.samp.Series(name)
	pick := func(f func(Sample) float64) []Point {
		pts := make([]Point, len(smp))
		for i, x := range smp {
			pts[i] = Point{T: x.T, V: f(x)}
		}
		return pts
	}
	return []template.HTML{
		LineChart("Lag", "seconds", []ChartSeries{{Name: "lag", Class: "s1", Points: pick(func(x Sample) float64 { return x.Lag })}}, 520, 150),
		LineChart("Throughput", "rows/s", []ChartSeries{
			{Name: "stream (top)", Class: "s1", Points: pick(func(x Sample) float64 { return x.Throughput })},
			{Name: "applied (metrics)", Class: "s2", Points: pick(func(x Sample) float64 { return x.ApplyRate })},
		}, 520, 150),
		LineChart("Holes and writeset", "count", []ChartSeries{
			{Name: "holes", Class: "s3", Points: pick(func(x Sample) float64 { return x.Holes })},
			{Name: "writeset entries", Class: "s2", Points: pick(func(x Sample) float64 { return x.Writeset })},
		}, 520, 150),
		LineChart("Spool", "bytes", []ChartSeries{{Name: "spool bytes", Class: "s4", Points: pick(func(x Sample) float64 { return x.SpoolBytes })}}, 520, 150),
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
		lag, rate = fmtSecs(v.Top.LagSeconds), FormatValue(v.Top.Throughput)+" rows/s"
		if v.Top.LagSeconds <= 0 {
			lag = "0 s"
		}
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
		{Title: "Source", Class: cls, Lines: []string{srcKind, "lag " + lag, "state " + v.State}},
		{Title: "Assembler", Class: cls, Lines: []string{"pending x-range " + mval(m, "strand_pending_cross_range"), "spool " + fmtBytesOrDash(m, "strand_spool_bytes")}},
		{Title: "Scheduler", Class: cls, Lines: []string{"holes " + mval(m, "strand_holes"), "writeset " + mval(m, "strand_writeset_entries"), "order " + order}},
		{Title: "Writer x" + workers, Class: cls, Lines: []string{rate, "governor " + zone}},
		{Title: "Target", Class: cls, Lines: []string{dstKind, "txns " + txns, "rows " + rows}},
	}
}

func fmtBytesOrDash(m map[string]float64, k string) string {
	if v, ok := m[k]; ok {
		return fmtBytes(v)
	}
	return "-"
}
