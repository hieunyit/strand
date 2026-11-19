package web

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"strand/internal/admin"
	"strand/internal/exitcode"
)

// Các hàm dưới đây gom dữ liệu cho cả trang HTML lẫn JSON /api/v1 để hai nơi luôn khớp nhau.

var errNoSocket = errors.New("pipeline has no observability.admin_socket in its config")

const adminTimeout = 4 * time.Second

// withAdmin gọi admin socket của pipeline với timeout.
func (s *Server) withAdmin(ctx context.Context, p *Pipeline, f func(ctx context.Context, c *admin.Client) error) error {
	return s.withAdminTimeout(ctx, p, adminTimeout, f)
}

func (s *Server) withAdminTimeout(ctx context.Context, p *Pipeline, timeout time.Duration, f func(ctx context.Context, c *admin.Client) error) error {
	sock := p.AdminSocket()
	if sock == "" {
		return errNoSocket
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return f(ctx, admin.NewClient(sock))
}

// PipelineView là tóm tắt một pipeline cho danh sách và đầu trang chi tiết.
type PipelineView struct {
	Name       string         `json:"name"`
	Pair       string         `json:"pair"`
	Mode       string         `json:"mode"`
	Origin     string         `json:"origin"`
	ConfigPath string         `json:"config_path"`
	State      string         `json:"state"`
	Reachable  bool           `json:"admin_reachable"`
	Runtime    Runtime        `json:"runtime"`
	Top        *admin.TopInfo `json:"top,omitempty"`
	LastExit   *ExitInfo      `json:"last_exit,omitempty"`
	ConfigErr  string         `json:"config_error,omitempty"`
	SampledAt  time.Time      `json:"sampled_at"`
	// Chế độ DB: entry web.yaml/file scan đã mất; bản sửa trên đĩa đã bị đặt sang bên.
	Missing   string         `json:"missing,omitempty"`
	Conflicts []DiskConflict `json:"disk_conflicts,omitempty"`
	// Live: số liệu của sampler (docs/WEB-API.md "Live stream").
	RawState string `json:"raw_state,omitempty"`
	// StoppedOutside: chế độ DB, pipeline nền đã dừng êm không do web (strand stop, SIGTERM): desired=stopped,
	// không tự khởi động lại.
	StoppedOutside bool      `json:"stopped_outside_web,omitempty"`
	Stale          bool      `json:"stale"`
	LastOK         time.Time `json:"last_ok"`
	PollErr        string    `json:"poll_error,omitempty"`
	Rates          Rates     `json:"rates"`
	LagLevel       string    `json:"lag_level"`
}

func (s *Server) view(p *Pipeline) PipelineView {
	v := PipelineView{Name: p.Name, Pair: p.Pair(), Mode: p.Mode(), Origin: p.Origin, ConfigPath: p.ConfigPath, ConfigErr: p.CfgErr, State: StateUnknown,
		Missing: p.Missing, Conflicts: p.Conflicts}
	if sn := s.samp.Latest(p.Name); sn != nil {
		v.State, v.Reachable, v.Runtime, v.Top, v.SampledAt = sn.State, sn.Reachable, sn.Runtime, sn.Top, sn.At
		v.RawState, v.Stale, v.LastOK, v.PollErr, v.Rates, v.LagLevel = sn.RawState, sn.Stale, sn.LastOK, sn.PollErr, sn.Rates, sn.LagLevel
	}
	if !v.Runtime.Running && v.Runtime.ExitCode != nil {
		e := explainExitFor(*v.Runtime.ExitCode, v.Mode)
		v.LastExit = &e
	}
	v.StoppedOutside = !v.Runtime.Running && s.stoppedOutside(p)
	return v
}

// Nav là một mục ở thanh trái.
type NavItem struct {
	Name  string
	State string
}

func (s *Server) navItems() []NavItem {
	var out []NavItem
	for _, p := range s.reg.List() {
		st := StateUnknown
		if sn := s.samp.Latest(p.Name); sn != nil {
			st = sn.State
		}
		out = append(out, NavItem{Name: p.Name, State: st})
	}
	return out
}

// StatusView: kết quả `status` của admin socket, hoặc lý do không có.
type StatusView struct {
	Status *admin.StatusInfo `json:"status,omitempty"`
	Error  string            `json:"error,omitempty"`
}

// errNoSample: sampler chưa lấy mẫu pipeline này lần nào (vừa khởi động hoặc vừa đăng ký).
const errNoSample = "no sample yet; the console polls every pipeline in the background"

// statusView đọc kết quả `status` từ mẫu mới nhất của sampler (không gọi admin socket).
func (s *Server) statusView(_ context.Context, p *Pipeline) StatusView {
	var v StatusView
	sn := s.samp.Latest(p.Name)
	switch {
	case sn == nil:
		v.Error = errNoSample
	case sn.Status != nil:
		v.Status = sn.Status
	default:
		v.Error = sn.StatusErr
	}
	return v
}

// notRunningMsg rút gọn lỗi socket thành lời dễ hiểu.
func notRunningMsg(err error) string {
	msg := err.Error()
	switch {
	case errors.Is(err, errNoSocket):
		return msg
	case strings.Contains(msg, "no such file"), strings.Contains(msg, "connection refused"):
		return "pipeline is not running (admin socket unreachable)"
	case strings.Contains(msg, "not supported"):
		return "this strand version does not support the command; upgrade the pipeline binary: " + msg
	}
	return msg
}

// TablesView: trạng thái snapshot từng bảng. Tables is ordered for operators: running first, then pending,
// then done (name order inside each group); the aggregates feed the progress summary on the Tables tab.
type TablesView struct {
	Tables  []admin.TableInfo `json:"tables"`
	Done    int               `json:"done"`
	Running int               `json:"running"`
	Pending int               `json:"pending"`
	Total   int               `json:"total"`
	// TablesPct is done/total (percent); RowsPct is copied/estimated rows over all tables (estimates from the source statistics).
	TablesPct    float64 `json:"tables_pct"`
	RowsCopied   int64   `json:"rows_copied"`
	RowsEstimate int64   `json:"rows_estimate"`
	RowsPct      float64 `json:"rows_pct"`
	Error        string  `json:"error,omitempty"`
	// Paged (tables_page.go): Tables is one page of the Matching tables (search/filter/sort applied by the server).
	Paged       bool              `json:"paged,omitempty"`
	Query       *TableQuery       `json:"query,omitempty"`
	Matching    int               `json:"matching,omitempty"`
	Pages       int               `json:"pages,omitempty"`
	First       int               `json:"first,omitempty"`
	Last        int               `json:"last,omitempty"`
	RunningRows []admin.TableInfo `json:"-"` // "Copying now" (page only), at most runningListMax
	RunningMore int               `json:"-"`
}

// tableRank orders the groups: running (0), pending (1), done (2).
func tableRank(state string) int {
	switch state {
	case "running":
		return 0
	case "done":
		return 2
	}
	return 1
}

// tblCopied is the number of rows already copied; a finished table counts as fully copied.
func tblCopied(t admin.TableInfo) int64 {
	if t.State == "done" && t.RowsEstimate > t.RowsCopied {
		return t.RowsEstimate
	}
	return t.RowsCopied
}

// tblEst is the row estimate of a table (never below what was copied).
func tblEst(t admin.TableInfo) int64 {
	if t.RowsCopied > t.RowsEstimate {
		return t.RowsCopied
	}
	return t.RowsEstimate
}

// tablesView đọc danh sách bảng từ mẫu mới nhất của sampler (không gọi admin socket).
func (s *Server) tablesView(_ context.Context, p *Pipeline) TablesView {
	return tablesFromSnap(s.samp.Latest(p.Name))
}

func tablesFromSnap(sn *Snap) TablesView {
	var v TablesView
	switch {
	case sn == nil:
		v.Error = errNoSample
		return v
	case sn.Tables == nil && sn.TablesErr != "":
		v.Error = sn.TablesErr
		return v
	}
	if c := sn.tables; c != nil {
		// Snap không đổi sau khi lưu: sắp xếp + tổng hợp một lần cho mọi người đọc (10.000 bảng: vài ms mỗi lần).
		// Tables của kết quả là dùng chung, CHỈ ĐỌC (applyPage tạo slice mới).
		c.once.Do(func() { c.v = buildTablesView(sn) })
		return c.v
	}
	return buildTablesView(sn)
}

// tablesCache: TablesView của một Snap, tính lười một lần.
type tablesCache struct {
	once sync.Once
	v    TablesView
}

func buildTablesView(sn *Snap) TablesView {
	var v TablesView
	v.Tables = append([]admin.TableInfo{}, sn.Tables...) // finish sắp xếp tại chỗ: không đụng Snap dùng chung
	v.finish()
	return v
}

// finish sorts the tables and computes the counts and aggregates.
func (v *TablesView) finish() {
	// sort.Slice (not stable): the key (status group, name) is total, and TableInfo is large: the stable sort's
	// rotations made a 10,000-table page cost ~70 ms.
	sort.Slice(v.Tables, func(i, j int) bool {
		a, b := &v.Tables[i], &v.Tables[j]
		if ra, rb := tableRank(a.State), tableRank(b.State); ra != rb {
			return ra < rb
		}
		return a.Table < b.Table
	})
	for _, t := range v.Tables {
		switch t.State {
		case "done":
			v.Done++
		case "running":
			v.Running++
		default:
			v.Pending++
		}
		v.RowsCopied += tblCopied(t)
		v.RowsEstimate += tblEst(t)
	}
	v.Total = len(v.Tables)
	if v.Total > 0 {
		v.TablesPct = float64(v.Done) * 100 / float64(v.Total)
	}
	if v.RowsEstimate > 0 {
		v.RowsPct = math.Min(100, float64(v.RowsCopied)*100/float64(v.RowsEstimate))
	}
}

// KeyKind describes how a table's rows are identified: PK, UNIQUE or KEYLESS (full-row match).
type KeyKind struct {
	Kind   string // pk | unique | keyless | unknown
	Label  string // PK | UNIQUE | KEYLESS | ?
	Detail string // columns, e.g. "id" or "a, b"
	Title  string // explanation for the tooltip
}

func keyKind(t admin.TableInfo) KeyKind {
	k := strings.ToLower(t.Key)
	cols := strings.Join(t.PK, ", ")
	switch {
	case strings.Contains(k, "keyless") || strings.Contains(k, "full row"):
		return KeyKind{"keyless", "KEYLESS", "full row", "No primary or unique key: rows are matched by their whole content (byte-exact), which is slower. A key makes the copy and the apply faster."}
	case strings.HasPrefix(k, "unique"):
		return KeyKind{"unique", "UNIQUE", cols, "Rows are identified by a NOT NULL unique key (" + t.Key + ")."}
	case strings.HasPrefix(k, "primary") || len(t.PK) > 0:
		d := cols
		if t.GenericPK {
			d += " (generic key)"
		}
		return KeyKind{"pk", "PK", d, "Rows are identified by the primary key."}
	}
	return KeyKind{"unknown", "?", "", "The key of this table is not known."}
}

// TablesProg: tables done / total and the percentage for a pipeline in the snapshot phase (from `top`).
type TablesProg struct {
	Done, Total int64
	Pct         float64
}

func tablesOf(v PipelineView) TablesProg {
	if v.Top == nil || v.Top.TablesTotal <= 0 {
		return TablesProg{}
	}
	done, total := v.Top.TablesDone, v.Top.TablesTotal
	return TablesProg{done, total, math.Min(100, float64(done)*100/float64(total))}
}

// icon renders an icon of the /static/icons.svg sprite.
func icon(name string) template.HTML {
	return template.HTML(`<svg class="ic" aria-hidden="true" focusable="false"><use href="/static/icons.svg#i-` + template.HTMLEscapeString(name) + `"/></svg>`)
}

// Engine is a database engine label with a CSS class for its colour.
type Engine struct{ Label, Class string }

func engineOf(kind string) Engine {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "mysql":
		return Engine{"MySQL", "mysql"}
	case "mariadb":
		return Engine{"MariaDB", "mariadb"}
	case "postgres", "postgresql":
		return Engine{"PostgreSQL", "postgres"}
	}
	return Engine{kind, "other"}
}

// engines splits "mysql → postgres" into source and target engines.
func engines(pair string) [2]Engine {
	if parts := strings.SplitN(pair, " → ", 2); len(parts) == 2 {
		return [2]Engine{engineOf(parts[0]), engineOf(parts[1])}
	}
	return [2]Engine{engineOf(pair), {}}
}

// fmtRel: "12 s ago", "5 min ago", "3 h ago".
func fmtRel(t, now time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := now.Sub(t)
	switch {
	case d < 0:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%d s ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%d min ago", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%d h ago", int(d.Hours()))
	}
	return fmt.Sprintf("%d d ago", int(d.Hours()/24))
}

func hostName() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "localhost"
	}
	return h
}

// HeadInfo: extra facts for the pipeline header (only while the admin socket answers).
type HeadInfo struct {
	Uptime  float64
	CkptAt  time.Time
	HasInfo bool
}

func (s *Server) headInfo(_ context.Context, p *Pipeline) HeadInfo {
	return headFromSnap(s.samp.Latest(p.Name))
}

func headFromSnap(sn *Snap) HeadInfo {
	var h HeadInfo
	if sn == nil || sn.Status == nil {
		return h
	}
	h.Uptime, h.HasInfo = sn.Status.UptimeSeconds, true
	if sn.Stale && !sn.LastOK.IsZero() {
		h.Uptime += sn.At.Sub(sn.LastOK).Seconds()
	}
	if sn.Checkpoint != nil {
		h.CkptAt = sn.Checkpoint.UpdatedAt
	}
	return h
}

// CheckpointView: vị trí đã apply, epoch, thời điểm cập nhật.
type CheckpointView struct {
	Checkpoint        *admin.CheckpointResult `json:"checkpoint,omitempty"`
	StartPosition     string                  `json:"start_position,omitempty"`
	CompactionLagSecs *float64                `json:"compaction_lag_seconds,omitempty"`
	Error             string                  `json:"error,omitempty"`
}

// checkpointView đọc checkpoint từ mẫu mới nhất của sampler (không gọi admin socket).
func (s *Server) checkpointView(_ context.Context, p *Pipeline) CheckpointView {
	var v CheckpointView
	sn := s.samp.Latest(p.Name)
	switch {
	case sn == nil:
		v.Error = errNoSample
		return v
	case sn.Checkpoint == nil:
		// Pipeline dừng: không đọc target (đắt và cần mật khẩu); chỉ báo rõ.
		msg := sn.CheckpointErr
		if msg == "" {
			msg = "no checkpoint reported"
		}
		v.Error = msg + "; the position is shown only while the pipeline runs"
		return v
	}
	v.Checkpoint = sn.Checkpoint
	if sn.Status != nil {
		v.StartPosition = sn.Status.StartPosition
	}
	if sn.Metrics != nil {
		if x, ok := sn.Metrics["strand_checkpoint_compaction_lag_seconds"]; ok {
			v.CompactionLagSecs = &x
		}
	}
	return v
}

// ErrorsView: lỗi/cảnh báo gần nhất và exit code lần dừng gần nhất.
type ErrorsView struct {
	Live      []LogEvent `json:"live_events"` // ring buffer 200 bản ghi của tiến trình đang chạy
	FromLog   []LogEvent `json:"log_events"`  // WARN/ERROR rút từ đuôi log (cũng cho pipeline đã dừng)
	LiveErr   string     `json:"live_error,omitempty"`
	LastExit  *ExitInfo  `json:"last_exit,omitempty"`
	ExitFrom  string     `json:"exit_from,omitempty"` // systemd | log (suy ra từ mã lỗi trong log)
	ErrorCode string     `json:"error_code,omitempty"`
}

// exitForErrorCode suy exit code từ mã lỗi snake_case (spec/02 §12) khi không có systemd để hỏi.
func exitForErrorCode(code string) (int, bool) {
	switch code {
	case exitcode.ErrBinlogConfigInvalid, exitcode.ErrMissingPK, exitcode.ErrCollationLooser, exitcode.ErrTypeUnmappable,
		exitcode.ErrInsufficientPrivilege, exitcode.ErrSeedInvariantFailed, exitcode.ErrPrereleaseTool,
		exitcode.ErrTargetNotTransactional, exitcode.ErrTableTargetChanged:
		return exitcode.Rejected, true
	case exitcode.ErrPipelineLocked, exitcode.ErrFenced, exitcode.ErrPipelinePaused:
		return exitcode.Fenced, true
	case exitcode.ErrLogGap, exitcode.ErrPrefetchExhausted, exitcode.ErrVerifyMismatch, exitcode.ErrVerifyIncomplete:
		return exitcode.DataError, true
	case exitcode.ErrPromoteNeedsDecision, exitcode.ErrSchemaChangePending:
		return exitcode.NeedsDecision, true
	case "invalid_syntax", "invalid_connection", "missing_config_path", "config_read_error", "missing_name":
		return exitcode.InvalidSyntax, true
	}
	return 0, false
}

func (s *Server) errorsView(ctx context.Context, p *Pipeline) ErrorsView {
	var v ErrorsView
	var ev []admin.EventRecord
	sn := s.samp.Latest(p.Name)
	switch {
	case sn == nil:
		v.LiveErr = errNoSample
	default:
		ev = sn.Events
		if ev == nil {
			v.LiveErr = sn.EventsErr
		}
	}
	v.Live = EventsToLog(ev)
	for i := range v.Live {
		v.Live[i].Message = MaskSecrets(v.Live[i].Message)
	}
	if lines, err := s.reg.Logs(ctx, p, 1000); err == nil {
		v.FromLog = ParseLogEvents(lines, 100)
		for i := range v.FromLog {
			v.FromLog[i].Message = MaskSecrets(v.FromLog[i].Message)
			v.FromLog[i].Raw = ""
		}
		for i := len(v.FromLog) - 1; i >= 0 && v.ErrorCode == ""; i-- {
			if v.FromLog[i].Level == "ERROR" {
				v.ErrorCode = ErrorCodeFromLine(v.FromLog[i].Message)
			}
		}
	}
	var rt Runtime
	if sn != nil {
		rt = sn.Runtime
	} else {
		rt = s.reg.Runtime(ctx, p)
	}
	if !rt.Running {
		switch {
		case rt.ExitCode != nil:
			e := explainExitFor(*rt.ExitCode, rt.Mode)
			v.LastExit, v.ExitFrom = &e, "systemd"
			if rt.Mode == ModeBackground {
				v.ExitFrom = "log"
			}
		case v.ErrorCode != "":
			if c, ok := exitForErrorCode(v.ErrorCode); ok {
				e := explainExitFor(c, p.Mode())
				v.LastExit, v.ExitFrom = &e, "log"
			}
		}
	}
	return v
}

// ConfigView: cấu hình pipeline, đã che bí mật.
type ConfigView struct {
	Path          string   `json:"path"`
	YAML          string   `json:"yaml,omitempty"`
	Unimplemented []string `json:"unimplemented_keys"`
	Error         string   `json:"error,omitempty"`
}

func (s *Server) configView(p *Pipeline) ConfigView {
	v := ConfigView{Path: p.ConfigPath, Unimplemented: []string{}}
	if s.db != nil {
		s.materialize(p.Name)
	}
	raw, err := os.ReadFile(p.ConfigPath)
	if err != nil && s.db != nil {
		ctx, cancel := dbCtx()
		row, ok, dberr := s.db.st.GetPipeline(ctx, p.Name)
		cancel()
		if dberr == nil && ok && row.HasConfig && len(row.ConfigYAML) > 0 {
			raw = []byte(row.ConfigYAML)
			err = nil
		}
	}
	if err != nil {
		v.Error = "cannot read config file: " + err.Error()
		return v
	}
	masked, err := MaskConfigText(raw)
	if err != nil {
		v.Error = "config is not valid YAML: " + err.Error()
		return v
	}
	v.YAML = masked
	if p.Cfg != nil {
		for _, k := range p.Cfg.UnimplementedKeys() {
			v.Unimplemented = append(v.Unimplemented, MaskSecrets(unimplementedWarn(k)))
		}
	} else if p.CfgErr != "" {
		v.Error = MaskSecrets(p.CfgErr)
	}
	return v
}

// LogsView: đuôi log.
type LogsView struct {
	Source string   `json:"source"` // journalctl strand@x | file /path
	Lines  []string `json:"lines"`
	Error  string   `json:"error,omitempty"`
}

func (s *Server) logsView(ctx context.Context, p *Pipeline, n int) LogsView {
	v := LogsView{Lines: []string{}}
	if p.SystemdUnit != "" {
		v.Source = "journalctl -u " + p.SystemdUnit
	} else {
		lp, _ := p.BgPaths()
		v.Source = "file " + lp
	}
	lines, err := s.reg.Logs(ctx, p, n)
	if err != nil {
		v.Error = err.Error()
		return v
	}
	for _, l := range lines {
		v.Lines = append(v.Lines, MaskSecrets(l))
	}
	return v
}

// SeriesView: các mẫu của khoảng range (mặc định 1 giờ gần nhất, từ RAM).
type SeriesView struct {
	Range           string   `json:"range"`
	IntervalSeconds int      `json:"interval_seconds"`
	Samples         []Sample `json:"samples"`
	Error           string   `json:"error,omitempty"`
}

func (s *Server) seriesView(p *Pipeline, rangeKey string) SeriesView {
	rng := chartRangeOf(rangeKey)
	v := SeriesView{Range: rng.Key, IntervalSeconds: int(s.samp.interval.Seconds())}
	smp, ok := s.seriesFor(p.Name, rng)
	if !ok {
		v.Range, v.Error = "1h", "ranges longer than 1h need the PostgreSQL metadata store"
		smp = s.samp.Series(p.Name)
	} else if rng.Step > 0 {
		v.IntervalSeconds = int(rng.Step.Seconds())
	}
	v.Samples = append([]Sample{}, smp...)
	return v
}
