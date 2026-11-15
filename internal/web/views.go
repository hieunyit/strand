package web

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"strand/internal/admin"
	"strand/internal/exitcode"
)

// Các hàm dưới đây gom dữ liệu cho cả trang HTML lẫn JSON /api/v1 để hai nơi luôn khớp nhau.

var errNoSocket = errors.New("pipeline has no observability.admin_socket in its config")

const adminTimeout = 4 * time.Second

// withAdmin gọi admin socket của pipeline với timeout.
func (s *Server) withAdmin(ctx context.Context, p *Pipeline, f func(ctx context.Context, c *admin.Client) error) error {
	sock := p.AdminSocket()
	if sock == "" {
		return errNoSocket
	}
	ctx, cancel := context.WithTimeout(ctx, adminTimeout)
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
}

func (s *Server) view(p *Pipeline) PipelineView {
	v := PipelineView{Name: p.Name, Pair: p.Pair(), Mode: p.Mode(), Origin: p.Origin, ConfigPath: p.ConfigPath, ConfigErr: p.CfgErr, State: StateUnknown}
	if sn := s.samp.Latest(p.Name); sn != nil {
		v.State, v.Reachable, v.Runtime, v.Top, v.SampledAt = sn.State, sn.Reachable, sn.Runtime, sn.Top, sn.At
	}
	if !v.Runtime.Running && v.Runtime.ExitCode != nil {
		e := explainExitFor(*v.Runtime.ExitCode, v.Mode)
		v.LastExit = &e
	}
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

func (s *Server) statusView(ctx context.Context, p *Pipeline) StatusView {
	var v StatusView
	err := s.withAdmin(ctx, p, func(ctx context.Context, c *admin.Client) (err error) {
		v.Status, err = c.Status(ctx)
		return
	})
	if err != nil {
		v.Error = notRunningMsg(err)
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

// TablesView: trạng thái snapshot từng bảng.
type TablesView struct {
	Tables  []admin.TableInfo `json:"tables"`
	Done    int               `json:"done"`
	Running int               `json:"running"`
	Pending int               `json:"pending"`
	Error   string            `json:"error,omitempty"`
}

func (s *Server) tablesView(ctx context.Context, p *Pipeline) TablesView {
	var v TablesView
	err := s.withAdmin(ctx, p, func(ctx context.Context, c *admin.Client) (err error) {
		v.Tables, err = c.Tables(ctx)
		return
	})
	if err != nil {
		v.Error = notRunningMsg(err)
		return v
	}
	for _, t := range v.Tables {
		switch t.State {
		case "done":
			v.Done++
		case "running":
			v.Running++
		default:
			v.Pending++
		}
	}
	return v
}

// CheckpointView: vị trí đã apply, epoch, thời điểm cập nhật.
type CheckpointView struct {
	Checkpoint        *admin.CheckpointResult `json:"checkpoint,omitempty"`
	StartPosition     string                  `json:"start_position,omitempty"`
	CompactionLagSecs *float64                `json:"compaction_lag_seconds,omitempty"`
	Error             string                  `json:"error,omitempty"`
}

func (s *Server) checkpointView(ctx context.Context, p *Pipeline) CheckpointView {
	var v CheckpointView
	err := s.withAdmin(ctx, p, func(ctx context.Context, c *admin.Client) (err error) {
		v.Checkpoint, err = c.CheckpointShow(ctx)
		return
	})
	if err != nil {
		// Pipeline dừng: không đọc target ở M2 (đắt và cần mật khẩu); chỉ báo rõ.
		v.Error = notRunningMsg(err) + "; the position is shown only while the pipeline runs"
		return v
	}
	if st := s.statusView(ctx, p); st.Status != nil {
		v.StartPosition = st.Status.StartPosition
	}
	if sn := s.samp.Latest(p.Name); sn != nil && sn.Metrics != nil {
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
	err := s.withAdmin(ctx, p, func(ctx context.Context, c *admin.Client) (err error) {
		ev, err = c.Events(ctx)
		return
	})
	if err != nil {
		v.LiveErr = notRunningMsg(err)
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
	rt := s.reg.Runtime(ctx, p)
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
	raw, err := os.ReadFile(p.ConfigPath)
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

// SeriesView: các mẫu 1 giờ gần nhất.
type SeriesView struct {
	IntervalSeconds int      `json:"interval_seconds"`
	Samples         []Sample `json:"samples"`
}

func (s *Server) seriesView(p *Pipeline) SeriesView {
	return SeriesView{IntervalSeconds: int(s.samp.interval.Seconds()), Samples: append([]Sample{}, s.samp.Series(p.Name)...)}
}
