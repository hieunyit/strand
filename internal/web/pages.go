package web

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"strand/internal/admin"
	"strand/internal/backup"
	"strand/internal/config"
	"strand/internal/spool"
	"strand/internal/tunnel"
	"strand/internal/web/store"
)

func unimplementedWarn(k string) string { return config.UnimplementedWarning(k) }

var pageNames = []string{
	"overview", "pipelines", "detail", "jobs", "job", "backups", "backups_jobs", "backups_job_detail",
	"backups_catalog", "backups_catalog_detail", "backups_restore", "wizard", "new_pipeline", "settings",
	"mcp", "schema", "spool_inspect", "archive", "tunnel", "db_admin", "error", "users", "account",
	"backups_repositories", "backups_repository_detail", "backups_notifications", "backups_keys", "backups_activity", "backups_reports",
}

func templateFuncs() template.FuncMap {
	return templateFuncsForLang(LangEN)
}

func templateFuncsForLang(lang string) template.FuncMap {
	return template.FuncMap{
		"t": func(key string, optLang ...string) string {
			l := lang
			if len(optLang) > 0 && optLang[0] != "" {
				l = optLang[0]
			}
			return T(l, key)
		},
		"kindsOf":       kindsOf,
		"retentionText": retentionWords,
		"dur":           fmtDur,
		"stateClass":    stateClass,
		"stateTone":     stateTone,
		"secs":          fmtSecs,
		"lag":           fmtLag,
		"governor":      fmtGovernor,
		"tblRows":       tblRowsText,
		"tblETA":        tblETAText,
		"num":           fmtNum,
		"int64":         func(v int64) string { return fmtInt(v) },
		"clock": func(t time.Time) string {
			return formatWebTime(t, "15:04:05 MST")
		},
		"derefTime": func(t *time.Time) time.Time {
			if t == nil {
				return time.Time{}
			}
			return *t
		},
		"auditDetail": func(m map[string]any) string {
			if len(m) == 0 {
				return ""
			}
			b, _ := json.Marshal(maskAuditSecrets(m))
			return string(b)
		},
		"stateReason": stateReason,
		"lagSort":     lagSort,
		"stateSev":    stateSev,
		"lagClass":    lagClass,
		"bytes":       fmtBytes,
		"bytes64":     func(n int64) string { return fmtBytes(float64(n)) },
		"ts": func(t time.Time) string {
			return formatWebTime(t, "2006-01-02 15:04:05 MST")
		},
		"logTS":                     formatWebLogTime,
		"tsMinute":                  func(t time.Time) string { return formatWebTime(t, "2006-01-02 15:04 MST") },
		"pct":                       func(v float64) string { return fmt.Sprintf("%.1f%%", v) },
		"bar":                       ProgressBar,
		"bigbar":                    BigProgressBar,
		"ranges":                    fmtRanges,
		"lower":                     strings.ToLower,
		"ptrInt":                    func(p *int) int { return *p },
		"deref":                     func(p *float64) float64 { return *p },
		"i64f":                      func(v int64) float64 { return float64(v) },
		"stateKey":                  func(s string) string { return s },
		"icon":                      icon,
		"sectionHome":               sectionHome,
		"tabIcon":                   tabIcon,
		"engines":                   engines,
		"keyKind":                   keyKind,
		"tblCopied":                 tblCopied,
		"tblEst":                    tblEst,
		"tablesOf":                  tablesOf,
		"tablesHref":                tablesHref,
		"add":                       func(a, b int) int { return a + b },
		"rel":                       func(t time.Time) string { return fmtRel(t, time.Now()) },
		"join":                      strings.Join,
		"jobState":                  jobState,
		"jobClass":                  jobClass,
		"jobDur":                    jobDur,
		"deref2":                    func(t *time.Time) time.Time { return *t },
		"div":                       func(a, b int) int { return a / b },
		"isUnsupportedBackupMethod": backup.IsUnsupportedMethod,
		"backup14DayChart":          Backup14DayChart,
		"jobRuns30Chart":            JobRuns30Chart,
		"fmtDuration": func(d time.Duration) string {
			if d <= 0 {
				return "-"
			}
			if d < time.Second {
				return fmt.Sprintf("%dms", d.Milliseconds())
			}
			if d < time.Minute {
				return fmt.Sprintf("%.1fs", d.Seconds())
			}
			return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
		},
		"remediationHint": func(code string) string {
			return backup.RemediationHints[code]
		},
		"pipelineSSHFields": func(side string, profiles []sshProfileView) pipelineSSHFieldsView {
			return pipelineSSHFieldsView{Side: side, Profiles: profiles}
		},
		"jobsCtx": func(j []JobStatus, showPipeline, canOperate bool, origins ...map[string]jobOrigin) jobsTableCtx {
			v := jobsTableCtx{Jobs: j, ShowPipeline: showPipeline, CanOperate: canOperate}
			if len(origins) > 0 {
				v.Origins = origins[0]
			}
			return v
		},
		"jobStateKey": jobStateKey,
		"tabsCtx":     func(tabs []tabLink, active string) tabsData { return tabsData{tabs, active} },
		"rowCtx":      func(v PipelineView, op, adm bool) rowData { return rowData{v, op, adm} },
		"errState":    func(title, detail string) errStateData { return errStateData{title, detail} },
	}
}

// Small view structs passed to the shared partials of pipeline_parts.html.
type (
	jobsTableCtx struct {
		Jobs         []JobStatus
		ShowPipeline bool
		CanOperate   bool
		Origins      map[string]jobOrigin
	}
	tabsData struct {
		Tabs   []tabLink
		Active string
	}
	rowData struct {
		View                 PipelineView
		CanOperate, CanAdmin bool
	}
	errStateData struct{ Title, Detail string }
)

func parsePages() (map[string]*template.Template, error) {
	return parsePagesForLang(LangEN)
}

var (
	cachedPagesLang   = make(map[string]map[string]*template.Template)
	cachedPagesLangMu sync.Mutex
)

func parsePagesForLang(lang string) (map[string]*template.Template, error) {
	cachedPagesLangMu.Lock()
	defer cachedPagesLangMu.Unlock()
	if m, ok := cachedPagesLang[lang]; ok {
		return m, nil
	}
	out := map[string]*template.Template{}
	funcs := templateFuncsForLang(lang)
	for _, n := range pageNames {
		t, err := template.New(n).Funcs(funcs).ParseFS(templateFS, append(append([]string{"templates/base.html"}, partialTemplates...), "templates/pipeline_parts.html", "templates/"+n+".html")...)
		if err != nil {
			return nil, fmt.Errorf("parse template %s (%s): %w", n, lang, err)
		}
		out[n] = t
	}
	t, err := template.New("login").Funcs(funcs).ParseFS(templateFS, "templates/login.html")
	if err != nil {
		return nil, err
	}
	out["login"] = t
	cachedPagesLang[lang] = out
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
	case j.Running && j.Cancelled:
		return stateClass("cancelling")
	case j.Running:
		return stateClass("running")
	case j.Cancelled:
		return stateClass("cancelled")
	case j.Interrupted:
		return stateClass("interrupted")
	case j.ExitCode != nil && *j.ExitCode == 0:
		return stateClass("success")
	case j.ExitCode == nil:
		return stateClass("unknown")
	}
	return stateClass("failed")
}

func jobDur(j JobStatus) string {
	end := time.Now()
	if j.Ended != nil {
		end = *j.Ended
	}
	return fmtDur(end.Sub(j.Started).Seconds())
}

// fmtDur is the duration format of tables (SPEC 3.1): "42 s", "4m 05s", "1h 05m", "2d 03h".
func fmtDur(v float64) string {
	switch {
	case v <= 0 || math.IsNaN(v):
		return "-"
	case v < 1:
		return fmt.Sprintf("%.0f ms", v*1000)
	case v < 60:
		return fmt.Sprintf("%.0f s", v)
	}
	d := time.Duration(v * float64(time.Second)).Round(time.Second)
	days, h, m, sec := int(d/(24*time.Hour)), int(d/time.Hour)%24, int(d/time.Minute)%60, int(d/time.Second)%60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %02dh", days, h)
	case h > 0:
		return fmt.Sprintf("%dh %02dm", h, m)
	}
	return fmt.Sprintf("%dm %02ds", m, sec)
}

// configRevision is a short content hash of the stored pipeline config (the source of truth), so two
// operators can tell at a glance whether they look at the same configuration.
func configRevision(p *Pipeline) string {
	data := p.dbYAML
	if len(data) == 0 && p.ConfigPath != "" {
		data, _ = os.ReadFile(p.ConfigPath)
	}
	if len(data) == 0 {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])[:8]
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
	case StateStopped:
		if v.StoppedOutside {
			return "stopped outside the web (strand stop or a signal); it is not started automatically"
		}
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

type CurrentUserInfo struct {
	ID          int64
	Username    string
	DisplayName string
	Role        store.Role
}

type pageData struct {
	Lang         string // en | vi
	Title        string
	DocTitle     string // <title> text; defaults to Title
	Subtitle     string // Subtitle shown below Title in .pagehead
	Breadcrumbs  []BreadcrumbItem
	Actions      template.HTML
	SummaryStrip template.HTML
	AsOf         time.Time // when the page was rendered (shown as "data as of")
	Active       string    // overview | pipelines | tên pipeline
	CSRF         string
	Nav          []NavItem
	Version      string
	Host         string // host name shown in the top bar
	EnvName      string // environment chip label (setting environment_name); empty: the host name
	EnvTone      string // neutral | info | success | warning | danger
	Standby      string // non-empty on a standby instance (banner)
	Refresh      int    // giây; 0 = không tự làm mới toàn trang
	CurrentUser  CurrentUserInfo
	// Live: trang nhận số liệu qua SSE ("all" hoặc tên pipeline); "" = trang tĩnh. Interval: chu kỳ của sampler.
	Live     string
	LiveLogs bool
	Interval int
	// CanOperate / CanAdmin: what the signed-in role may do. Templates hide write controls from roles that
	// the API would refuse anyway (RBAC stays enforced server side in rbac.go).
	CanOperate bool
	CanAdmin   bool
	Body       any
	// Section groups related pages under one sidebar entry; SectionTabs are its pages the role may open,
	// shown as a tab strip above the content (set by render from Active).
	Section     string
	SectionTabs []SectionTab
}

// SectionTab is one page of a section (a link, not an in-page tab).
type SectionTab struct {
	Label  string // i18n key
	URL    string
	Icon   string
	Active bool
}

// sectionPage is a page that belongs to a section; Admin pages are listed for administrators only.
type sectionPage struct {
	Active, Label, URL, Icon string
	Admin                    bool
}

// sections groups the pages that share one sidebar entry. The first page a role may open is the entry's link.
var sections = map[string][]sectionPage{
	"tools": {
		{"archive", "nav.binlog_archive", "/archive", "logs", true},
		{"tunnels", "nav.ssh_tunnels", "/tunnels", "server", true},
		{"spool", "nav.spool_inspect", "/tools/spool-inspect", "search", true},
	},
	"backup-resources": {
		{"backups-connections", "nav.connections", "/backups/connections", "server", false},
		{"backups-repos", "nav.repositories", "/backups/repositories", "db", false},
		{"backups-policies", "nav.policies", "/backups/policies", "config", false},
		{"backups-keys", "nav.encryption_keys", "/backups/keys", "flag", true},
		{"backups-notify", "nav.notifications", "/backups/notifications", "bolt", true},
		{"backups-tools", "nav.backup_tools", "/backups/tools", "config", true},
	},
	"backup-reports": {
		{"backups-reports", "nav.reports", "/backups/reports", "graph", false},
		{"backups-activity", "nav.activity", "/backups/activity", "logs", true},
	},
	"system-settings": {
		{"settings", "nav.settings", "/settings", "config", true},
		{"db-admin", "nav.metadata_database", "/settings/db", "db", true},
		{"mcp", "nav.mcp", "/mcp", "bolt", false},
		{"schema-help", "nav.config_reference", "/help/schema", "table", false},
	},
}

// sectionOf returns the section of an Active page key.
func sectionOf(active string) string {
	for name, pages := range sections {
		for _, p := range pages {
			if p.Active == active {
				return name
			}
		}
	}
	return ""
}

// sectionTabs lists the pages of a section a role may open, marking the active one.
func sectionTabs(section, active string, admin bool) []SectionTab {
	var out []SectionTab
	for _, p := range sections[section] {
		if p.Admin && !admin {
			continue
		}
		out = append(out, SectionTab{Label: p.Label, URL: p.URL, Icon: p.Icon, Active: p.Active == active})
	}
	return out
}

// sectionHome is the sidebar link of a section for a role: its first page the role may open ("" = none).
func sectionHome(section string, admin bool) string {
	if t := sectionTabs(section, "", admin); len(t) > 0 {
		return t[0].URL
	}
	return ""
}

type BreadcrumbItem struct {
	Label string
	URL   string
}

func defaultBreadcrumbs(page, title, active string) []BreadcrumbItem {
	switch page {
	case "overview":
		return []BreadcrumbItem{{Label: "Home", URL: ""}}
	case "pipelines":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Pipelines", URL: ""}}
	case "new_pipeline":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Pipelines", URL: "/pipelines"}, {Label: "New pipeline", URL: ""}}
	case "jobs":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Jobs", URL: ""}}
	case "job":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Jobs", URL: "/jobs"}, {Label: title, URL: ""}}
	case "backups":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: ""}}
	case "backups_jobs":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Jobs", URL: ""}}
	case "backups_job_detail":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Jobs", URL: "/backups/jobs"}, {Label: title, URL: ""}}
	case "backups_catalog":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Restore points", URL: ""}}
	case "backups_catalog_detail":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Restore points", URL: "/backups/catalog"}, {Label: title, URL: ""}}
	case "backups_restore":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Backups", URL: "/backups"}, {Label: "Restore", URL: ""}}
	case "mcp":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "MCP", URL: ""}}
	case "settings":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Settings", URL: ""}}
	case "db_admin":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Settings", URL: "/settings"}, {Label: "Database Admin", URL: ""}}
	case "schema":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Help", URL: ""}, {Label: "Configuration Schema", URL: ""}}
	case "spool_inspect":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Tools", URL: ""}, {Label: "Spool Inspect", URL: ""}}
	case "archive":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Tools", URL: ""}, {Label: "Archive Binlog", URL: ""}}
	case "tunnel":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Tools", URL: ""}, {Label: "SSH Tunnels", URL: ""}}
	case "error":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Error", URL: ""}}
	case "detail":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Pipelines", URL: "/pipelines"}, {Label: active, URL: ""}}
	case "wizard":
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: "Pipelines", URL: "/pipelines"}, {Label: active, URL: "/pipelines/" + active}, {Label: title, URL: ""}}
	default:
		return []BreadcrumbItem{{Label: "Home", URL: "/"}, {Label: title, URL: ""}}
	}
}

func defaultSubtitle(page string) string {
	switch page {
	case "overview":
		return "Real-time health, lag, and sync status across all pipelines"
	case "pipelines":
		return "Manage replication pipelines, review state and performance"
	case "new_pipeline":
		return "Configure and register a new change data capture pipeline"
	case "jobs":
		return "Task execution history, verification runs, and background jobs"
	case "job":
		return "Job execution log and real-time process output"
	case "backups":
		return "Database backups, restore verification, and disaster recovery"
	case "mcp":
		return "Model Context Protocol read-only tools and sidecar integration"
	case "settings":
		return "Server configuration, security parameters, and maintenance"
	case "db_admin":
		return "Web console metadata database migration and backup operations"
	case "schema":
		return "Configuration schema reference and YAML validation specification"
	case "spool_inspect":
		return "Read-only sidecar inspection of on-disk Pebble spool buffers"
	case "archive":
		return "Binlog archiver status, continuous log preservation, and contents"
	case "tunnel":
		return "Secure SSH port forwarding tunnels for database connections"
	case "error":
		return "Requested resource or operation encountered an issue"
	default:
		return ""
	}
}

func (s *Server) render(w http.ResponseWriter, name string, sess *Session, d pageData) {
	if sess != nil {
		d.CSRF = sess.CSRF
		d.CurrentUser = CurrentUserInfo{
			ID:       sess.UserID,
			Username: sess.Username,
			Role:     sess.Role,
		}
		d.CanOperate, d.CanAdmin = RoleAllows(sess.Role, PermOperator), RoleAllows(sess.Role, PermAdmin)
	}
	d.Nav, d.Version = s.navItems(), s.version
	d.Standby = s.standbyMessage()
	d.Host = hostName()
	d.EnvName, d.EnvTone = s.settingStr("environment_name"), "neutral"
	if t := s.settingStr("environment_tone"); t != "" {
		if def := settingDefOf("environment_tone"); def != nil {
			for _, c := range def.Choices { // web.yaml values are not validated on load: only known tones become a class
				if c == t {
					d.EnvTone = t
				}
			}
		}
	}
	d.Interval = int(s.samp.Interval().Seconds())
	if d.AsOf.IsZero() {
		d.AsOf = s.now()
	}
	if d.Section = sectionOf(d.Active); d.Section != "" {
		if tabs := sectionTabs(d.Section, d.Active, d.CanAdmin); len(tabs) > 1 {
			d.SectionTabs = tabs
		}
	}
	lang := s.userLanguage(sess, nil)
	d.Lang = lang
	if len(d.Breadcrumbs) == 0 {
		d.Breadcrumbs = defaultBreadcrumbs(name, d.Title, d.Active)
	}
	if d.Subtitle == "" {
		d.Subtitle = defaultSubtitle(name)
	}
	var buf bytes.Buffer
	tmpl := s.pages[name]
	if s.pagesLang != nil && s.pagesLang[lang] != nil && s.pagesLang[lang][name] != nil {
		tmpl = s.pagesLang[lang][name]
	}
	if err := tmpl.ExecuteTemplate(&buf, "base", d); err != nil {
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
	Kinds    []string // engines in use (source or target), for the filter bar of the list page
	Bulk     bool     // list page: checkbox column, bulk bar and row menu
	Backup   backupHealth
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
	b := overviewBody{
		Overview: s.samp.BuildOverview(),
		States:   AllStates,
		Sparks:   map[string]template.HTML{},
		Warnings: s.reg.Warnings(),
		Backup:   s.backupHealth(r.Context()),
	}
	for _, p := range s.reg.List() {
		b.Views = append(b.Views, s.view(p))
		b.Sparks[p.Name] = s.sparkFor(p.Name)
	}
	sortViews(b.Views)
	b.summarize()
	b.Kinds = viewKinds(b.Views)
	s.render(w, "overview", sess, pageData{Title: "Overview", Active: "overview", Live: "all", Body: b})
}

func (s *Server) pagePipelines(w http.ResponseWriter, r *http.Request, sess *Session) {
	b := overviewBody{Overview: s.samp.BuildOverview(), States: AllStates, Sparks: map[string]template.HTML{}, Warnings: s.reg.Warnings()}
	for _, p := range s.reg.List() {
		b.Views = append(b.Views, s.view(p))
		b.Sparks[p.Name] = s.sparkFor(p.Name)
	}
	sortViews(b.Views)
	b.Kinds, b.Bulk = viewKinds(b.Views), true
	d := pageData{Title: "Pipelines", Active: "pipelines", Live: "all", Body: b}
	if sess != nil && RoleAllows(sess.Role, PermAdmin) { // creating a pipeline is an admin action
		d.Actions = template.HTML(`<a class="btn primary" href="/pipelines/new">` + string(icon("plus")) + `New pipeline</a>`)
	}
	s.render(w, "pipelines", sess, d)
}

// viewKinds lists the engines (source or target kind) the registered pipelines use, sorted.
func viewKinds(vs []PipelineView) []string {
	seen := map[string]bool{}
	for _, v := range vs {
		for _, k := range []string{v.SourceKind, v.TargetKind} {
			if k != "" {
				seen[k] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// detailSubtitle: the one-line description under the pipeline name.
func detailSubtitle(v PipelineView) string {
	if v.SourceKind == "" {
		return "Replication pipeline"
	}
	e := engines(v.Pair)
	return e[0].Label + " to " + e[1].Label + " replication, managed by " + v.Mode
}

// kindsOf is the space separated engine list of a row (data-kinds), matched by the Engine filter.
func kindsOf(v PipelineView) string {
	return strings.TrimSpace(v.SourceKind + " " + v.TargetKind)
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
	TablesBase string
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
	CanOperate bool   // operator or admin: start/stop/pause/... controls
	CanAdmin   bool   // admin: delete, config edit, rewind
	Revision   string // short content hash of the stored config (shown as "Config revision")
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

var tabLabels = map[string]string{
	"graph": "Graph", "tables": "Tables", "checkpoint": "Checkpoint", "errors": "Errors",
	"config": "Config", "logs": "Logs", "jobs": "Jobs", "schema": "Schema", "objects": "Objects",
	"repair": "Repair", "promote": "Promote", "seed": "Seed",
}

func tabIcon(key string) string {
	if i, ok := map[string]string{
		"graph": "graph", "tables": "table", "checkpoint": "flag", "errors": "alert",
		"config": "config", "logs": "logs", "jobs": "jobs", "schema": "config", "objects": "table",
		"repair": "repair", "promote": "promote", "seed": "db",
	}[key]; ok {
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
	b := detailBody{Tab: tab, View: s.view(p), Revision: configRevision(p)}
	b.CanOperate, b.CanAdmin = RoleAllows(sess.Role, PermOperator), RoleAllows(sess.Role, PermAdmin)
	b.Ctl = s.controls(p, b.View)
	for _, t := range [][2]string{
		{"graph", "Graph"}, {"tables", "Tables"}, {"checkpoint", "Checkpoint"},
		{"errors", "Errors"}, {"config", "Config"}, {"logs", "Logs"}, {"jobs", "Jobs"},
		{"schema", "Schema"}, {"objects", "Objects"},
		{"repair", "Repair"}, {"promote", "Promote"}, {"seed", "Seed"},
	} {
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
		b.HasHistory = true
		b.Charts = s.charts(p.Name)
	case "tables":
		b.Tables = s.tablesPageView(p, r)
		b.TablesBase = base + "/tables"
		if b.Ctl.Running {
			b.Src, b.SrcLoaded = s.cachedSourceTables(p)
		}
	case "jobs":
		b.Jobs = s.listJobs(p.Name)
		if b.Ctl.Running {
			b.Src, b.SrcLoaded = s.cachedSourceTables(p)
		}
	case "schema", "objects":
		b.Status = s.statusView(ctx, p)
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
	s.render(w, "detail", sess, pageData{Title: p.Name, DocTitle: p.Name + " \u00b7 " + tabLabels[tab], Subtitle: detailSubtitle(b.View), Active: p.Name, Live: live, LiveLogs: liveLogs, Body: b})
}

type spoolInspectReport = spool.InventoryReport

type spoolInspectView struct {
	Pipelines []string
	Pipeline  string
	SpoolDir  string
	Report    *spoolInspectReport
}

type archiveFileItem struct {
	Name      string
	Size      string
	ModTime   string
	IsPartial bool
}

type archiveView struct {
	Running   bool
	JobID     string
	Pipeline  string
	Pipelines []string
	Error     string
	FileCount int
	TotalSize string
	Files     []archiveFileItem
}

type tunnelItem struct {
	ID           string `json:"id"`
	LocalAddress string `json:"local_address"`
	Remote       string `json:"remote"`
	SSHAddress   string `json:"ssh_address"`
	SSHUser      string `json:"ssh_user"`
}

type activeTunnel struct {
	Item    tunnelItem
	Forward *tunnel.Forward
	Cancel  context.CancelFunc
}

type tunnelView struct {
	Profiles []sshProfileView
	Tunnels  []tunnelItem
}

type dbMigrateReport struct {
	Verified bool
	Summary  struct {
		Total    int
		New      int
		Present  int
		Conflict int
	}
	Problems []string
}

type dbAdminView struct {
	DryRun        bool
	MigrateReport *dbMigrateReport
	MigrateOutput string
	ExportOutput  string
	Error         string
}

func (s *Server) pageSchemaHelp(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.render(w, "schema", sess, pageData{
		Title:  "Configuration Schema",
		Active: "schema-help",
		Body: struct {
			JSONSchema string
		}{
			JSONSchema: config.JSONSchema,
		},
	})
}

func (s *Server) pageSpoolInspect(w http.ResponseWriter, r *http.Request, sess *Session) {
	_ = r.ParseForm()
	pipeline := r.FormValue("pipeline")
	var rep *spoolInspectReport
	spoolDir := ""

	if r.Method == http.MethodPost {
		if err := s.auditSensitive(r, "spool.inspect", map[string]any{"pipeline": pipeline}, "started"); err != nil {
			s.renderError(w, r, sess, http.StatusInternalServerError, err.Error())
			return
		}
		var err error
		rep, spoolDir, err = s.scanPipelineSpool(r.Context(), pipeline)
		if err != nil {
			_ = s.auditSensitive(r, "spool.inspect", map[string]any{"pipeline": pipeline, "error": err.Error()}, "error")
			s.renderError(w, r, sess, http.StatusBadRequest, err.Error())
			return
		}
		if err := s.auditSensitive(r, "spool.inspect", map[string]any{"pipeline": pipeline, "discovered_files": rep.DiscoveredFiles, "new_files": rep.NewFiles}, "ok"); err != nil {
			s.renderError(w, r, sess, http.StatusInternalServerError, err.Error())
			return
		}
	}

	s.render(w, "spool_inspect", sess, pageData{
		Title:  "Spool Inspect",
		Active: "spool",
		Body: &spoolInspectView{
			Pipelines: s.spoolPipelineNames(),
			Pipeline:  pipeline,
			SpoolDir:  spoolDir,
			Report:    rep,
		},
	})
}

func (s *Server) pageArchive(w http.ResponseWriter, r *http.Request, sess *Session) {
	job := s.latestArchiveJob()
	running, jobID, pipeline, dir := false, "", "", ""
	if job != nil {
		running, jobID, pipeline = job.Running, job.ID, job.Pipeline
		if len(job.Args) > 0 {
			dir = s.archiveOutputDir(job)
		}
	}

	var files []archiveFileItem
	var totalSize int64
	if dir != "" {
		if safeDir, err := s.validateArchiveDir(dir); err == nil {
			if entries, err := os.ReadDir(safeDir); err == nil {
				for _, e := range entries {
					if !e.IsDir() {
						info, err := e.Info()
						if err == nil {
							totalSize += info.Size()
							files = append(files, archiveFileItem{
								Name:      e.Name(),
								Size:      fmtBytes(float64(info.Size())),
								ModTime:   formatWebTime(info.ModTime(), "2006-01-02 15:04:05 MST"),
								IsPartial: strings.HasPrefix(e.Name(), ".partial-"),
							})
						}
					}
				}
			}
		}
	}

	s.render(w, "archive", sess, pageData{
		Title:  "Archive Binlog",
		Active: "archive",
		Body: &archiveView{
			Running:   running,
			JobID:     jobID,
			Pipeline:  pipeline,
			Pipelines: s.archivePipelineNames(),
			FileCount: len(files),
			TotalSize: fmtBytes(float64(totalSize)),
			Files:     files,
		},
	})
}

func (s *Server) pageTunnel(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.tunnelMu.Lock()
	list := make([]tunnelItem, 0, len(s.tunnels))
	for _, t := range s.tunnels {
		if t.Forward == nil {
			continue
		}
		select {
		case <-t.Forward.Done():
		default:
			list = append(list, t.Item)
		}
	}
	s.tunnelMu.Unlock()

	s.render(w, "tunnel", sess, pageData{
		Title:  "SSH Tunnels",
		Active: "tunnels",
		Body:   &tunnelView{Tunnels: list, Profiles: s.sshProfileViews(r.Context())},
	})
}

func (s *Server) pageDBAdmin(w http.ResponseWriter, r *http.Request, sess *Session) {
	s.render(w, "db_admin", sess, pageData{
		Title:  "Database Admin",
		Active: "db-admin",
		Body:   &dbAdminView{},
	})
}

func (s *Server) renderError(w http.ResponseWriter, r *http.Request, sess *Session, code int, msg string) {
	w.WriteHeader(code)
	s.render(w, "error", sess, pageData{
		Title:    fmt.Sprintf("Error %d", code),
		Subtitle: "An error occurred while processing your request",
		Body: struct {
			Code    int
			Message string
		}{Code: code, Message: msg},
	})
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

func (s *Server) userLanguage(sess *Session, r *http.Request) string {
	if r != nil {
		if q := r.URL.Query().Get("lang"); q == LangVI || q == LangEN {
			return q
		}
	}
	if sess != nil && sess.Username != "" {
		s.userLangMu.RLock()
		uLang := s.userLangs[sess.Username]
		s.userLangMu.RUnlock()
		if uLang == LangVI || uLang == LangEN {
			return uLang
		}
		if s.db != nil && s.db.st != nil {
			ctx, cancel := dbCtx()
			var u store.User
			var ok bool
			var err error
			if sess.UserID != 0 {
				u, ok, err = s.db.st.GetUserByID(ctx, sess.UserID)
			} else {
				u, ok, err = s.db.st.GetUser(ctx, sess.Username)
			}
			cancel()
			if err == nil && ok && (u.Language == LangVI || u.Language == LangEN) {
				s.userLangMu.Lock()
				if s.userLangs == nil {
					s.userLangs = make(map[string]string)
				}
				s.userLangs[sess.Username] = u.Language
				s.userLangMu.Unlock()
				return u.Language
			}
		}
	}
	if r != nil {
		if c, err := r.Cookie("strand_lang"); err == nil && (c.Value == LangVI || c.Value == LangEN) {
			return c.Value
		}
	}
	if def := s.settingStr("default_language"); def == LangVI || def == LangEN {
		return def
	}
	return LangEN
}

func (s *Server) loginLanguage(r *http.Request) string {
	if r != nil {
		if q := r.URL.Query().Get("lang"); q == LangVI || q == LangEN {
			return q
		}
		if c, err := r.Cookie("strand_lang"); err == nil && (c.Value == LangVI || c.Value == LangEN) {
			return c.Value
		}
	}
	if def := s.settingStr("default_language"); def == LangVI || def == LangEN {
		return def
	}
	return LangEN
}
