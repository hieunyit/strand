package web

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"strand/internal/admin"
)

// Thao tác của M3 (docs/PLAN-WEB-UI.md §3). Mọi action đi qua apiAction: xác thực, CSRF, whitelist tên
// pipeline, confirm_name, audit "started" trước khi chạy. Ở đây chỉ kiểm tham số theo whitelist rồi gọi
// systemctl / lệnh `strand` (argv mảng, không shell) / admin socket. Điều kiện an toàn nằm ở CLI và pipeline,
// không cài lại ở đây.

// Tên action (dùng trong URL và audit).
const (
	ActStart          = "start"
	ActStop           = "stop"
	ActRestart        = "restart"
	ActEnable         = "enable"
	ActDisable        = "disable"
	ActPause          = "pause"
	ActPauseSnapshot  = "pause-snapshot"
	ActResume         = "resume"
	ActResumeSnapOnly = "resume-snapshot-only"
	ActThrottle       = "throttle"
	ActSnapshotAdd    = "snapshot-add"
	ActCheckpointShow = "checkpoint-show"
	ActRewind         = "checkpoint-rewind"
	ActVerify         = "verify"
	ActJobCancel      = "job-cancel"
)

var (
	// schema.table: chữ, số, _ $ -, mỗi phần tối đa 64 ký tự, không bắt đầu bằng '-'.
	tableRE = regexp.MustCompile(`^[\p{L}\p{N}_$][\p{L}\p{N}_$-]{0,63}\.[\p{L}\p{N}_$][\p{L}\p{N}_$-]{0,63}$`)
	// Vị trí rewind: GTID set (uuid:1-5:7-9,uuid2:...) hoặc LSN X/Y; CLI/pipeline kiểm tiếp ngữ nghĩa.
	positionRE = regexp.MustCompile(`^[0-9A-Za-z_:,/.\s-]+$`)
	uintRE     = regexp.MustCompile(`^[0-9]{1,9}$`)
	rateRE     = regexp.MustCompile(`^[0-9]{1,6}(\.[0-9]{1,6})?$`)
)

const maxTablesPerCall = 200

// strandBinary: binary dùng cho tiến trình con (web.yaml strand_binary, mặc định chính binary đang chạy).
func (s *Server) strandBinary() string {
	if s.cfg.StrandBinary != "" {
		return s.cfg.StrandBinary
	}
	if exe, err := os.Executable(); err == nil {
		return exe
	}
	return "strand"
}

func exitCodeOf(err error) *int {
	var ec interface{ ExitCode() int }
	if errors.As(err, &ec) {
		n := ec.ExitCode()
		return &n
	}
	return nil
}

func zeroOr(err error) *int {
	if err == nil {
		z := 0
		return &z
	}
	return exitCodeOf(err)
}

func trimOut(b []byte) string {
	s := strings.TrimSpace(MaskSecrets(string(b)))
	const max = 2000
	if len(s) > max {
		s = s[len(s)-max:]
	}
	return s
}

func (s *Server) registerOps() {
	a := s.actions
	for _, v := range []string{ActStart, ActStop, ActRestart} {
		v := v
		a.Register(Action{Name: v, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
			return srv.runLifecycle(ctx, req.Pipeline, v)
		}})
	}
	for _, v := range []string{ActEnable, ActDisable} {
		v := v
		a.Register(Action{Name: v, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
			p := req.Pipeline
			if p.Mode() != ModeSystemd {
				return ActionResult{}, BadRequest("%s is only available for systemd-managed pipelines", v)
			}
			return srv.systemctl(ctx, v, p)
		}})
	}
	a.Register(Action{Name: ActPause, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		var res *admin.PauseResult
		err := srv.adminCall(ctx, req.Pipeline, 30*time.Second, func(ctx context.Context, c *admin.Client) (e error) {
			res, e = c.Pause(ctx)
			return
		})
		if err != nil {
			return ActionResult{}, err
		}
		return ActionResult{Message: "pipeline paused; state " + res.State, Data: map[string]any{"state": res.State, "paused": res.Paused}}, nil
	}})
	a.Register(Action{Name: ActPauseSnapshot, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		var res *admin.PauseResult
		err := srv.adminCall(ctx, req.Pipeline, 30*time.Second, func(ctx context.Context, c *admin.Client) (e error) {
			res, e = c.PauseSnapshot(ctx)
			return
		})
		if err != nil {
			return ActionResult{}, err
		}
		return ActionResult{Message: "snapshot paused, stream keeps running; state " + res.State, Data: map[string]any{"state": res.State}}, nil
	}})
	for _, snapOnly := range []bool{false, true} {
		snapOnly := snapOnly
		name := ActResume
		if snapOnly {
			name = ActResumeSnapOnly
		}
		a.Register(Action{Name: name, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
			var res *admin.ResumeResult
			err := srv.adminCall(ctx, req.Pipeline, 30*time.Second, func(ctx context.Context, c *admin.Client) (e error) {
				res, e = c.Resume(ctx, snapOnly)
				return
			})
			if err != nil {
				return ActionResult{}, err
			}
			return ActionResult{Message: "pipeline resumed; state " + res.State, Data: map[string]any{"state": res.State, "snapshot_only": res.SnapshotOnly}}, nil
		}})
	}
	a.Register(Action{Name: ActThrottle, Run: srvThrottle})
	a.Register(Action{Name: ActSnapshotAdd, Run: srvSnapshotAdd})
	a.Register(Action{Name: ActCheckpointShow, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		var res *admin.CheckpointResult
		err := srv.adminCall(ctx, req.Pipeline, 15*time.Second, func(ctx context.Context, c *admin.Client) (e error) {
			res, e = c.CheckpointShow(ctx)
			return
		})
		if err != nil {
			return ActionResult{}, err
		}
		return ActionResult{Message: "checkpoint " + res.Applied, Data: map[string]any{"checkpoint": res}}, nil
	}})
	a.Register(Action{Name: ActRewind, ConfirmName: true, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		to := strings.TrimSpace(req.Args["to"])
		if to == "" || len(to) > 8192 || !positionRE.MatchString(to) {
			return ActionResult{}, BadRequest("to must be a GTID set or LSN (letters, digits, : , - / . and whitespace only)")
		}
		var res *admin.CheckpointResult
		err := srv.adminCall(ctx, req.Pipeline, 120*time.Second, func(ctx context.Context, c *admin.Client) (e error) {
			res, e = c.CheckpointRewind(ctx, to)
			return
		})
		if err != nil {
			return ActionResult{}, err
		}
		return ActionResult{Message: "checkpoint rewound to " + res.RewoundTo, Data: map[string]any{"checkpoint": res}}, nil
	}})
	a.Register(Action{Name: ActVerify, Run: srvVerify})
	a.Register(Action{Name: ActJobCancel, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		id := req.Args["job_id"]
		if !JobIDRE.MatchString(id) {
			return ActionResult{}, BadRequest("invalid job_id")
		}
		j, ok := srv.jobs.Get(id)
		if !ok || j.Pipeline != req.Pipeline.Name {
			return ActionResult{}, &ActionError{Status: 404, Msg: "unknown job for this pipeline"}
		}
		if err := srv.jobs.Cancel(id); err != nil {
			return ActionResult{}, Conflict("%v", err)
		}
		return ActionResult{Message: "cancel requested", JobID: id}, nil
	}})
}

// adminCall gọi admin socket của pipeline; lỗi chuyển thành ActionError 502 với lời dễ hiểu.
func (s *Server) adminCall(ctx context.Context, p *Pipeline, timeout time.Duration, f func(ctx context.Context, c *admin.Client) error) error {
	sock := p.AdminSocket()
	if sock == "" {
		return BadRequest("%v", errNoSocket)
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := f(cctx, admin.NewClient(sock)); err != nil {
		return &ActionError{Status: 502, Msg: notRunningMsg(err)}
	}
	return nil
}

func srvThrottle(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
	v := strings.TrimSpace(req.Args["source_iops"])
	if !uintRE.MatchString(v) {
		return ActionResult{}, BadRequest("source_iops must be a positive integer")
	}
	n, _ := strconv.Atoi(v)
	if n < 1 || n > 10_000_000 {
		return ActionResult{}, BadRequest("source_iops must be between 1 and 10000000")
	}
	var res *admin.ThrottleResult
	err := srv.adminCall(ctx, req.Pipeline, 15*time.Second, func(ctx context.Context, c *admin.Client) (e error) {
		// Chỉ soft limit đổi được khi đang chạy (target IOPS chưa được thi hành ở pipeline).
		res, e = c.ThrottleSet(ctx, n, 0, true)
		return
	})
	if err != nil {
		return ActionResult{}, err
	}
	return ActionResult{Message: fmt.Sprintf("soft source IOPS set to %d (hard limit %d unchanged)", res.SourceIOPSSoft, res.SourceIOPSHard),
		Data: map[string]any{"source_iops_soft": res.SourceIOPSSoft, "source_iops_hard": res.SourceIOPSHard}}, nil
}

// parseTables gom `table` và `tables` (phân tách bằng dấu phẩy) thành danh sách đã kiểm whitelist.
func parseTables(args map[string]string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, raw := range []string{args["table"], args["tables"]} {
		for _, t := range strings.Split(raw, ",") {
			t = strings.TrimSpace(t)
			if t == "" || seen[t] {
				continue
			}
			if !tableRE.MatchString(t) {
				return nil, BadRequest("invalid table %q: expected schema.table (letters, digits, _ $ -)", t)
			}
			seen[t] = true
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, BadRequest("table is required (schema.table)")
	}
	if len(out) > maxTablesPerCall {
		return nil, BadRequest("too many tables (max %d)", maxTablesPerCall)
	}
	return out, nil
}

func srvSnapshotAdd(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
	tables, err := parseTables(req.Args)
	if err != nil {
		return ActionResult{}, err
	}
	var done []string
	for _, t := range tables {
		t := t
		if err := srv.adminCall(ctx, req.Pipeline, 60*time.Second, func(ctx context.Context, c *admin.Client) error {
			return c.SnapshotAdd(ctx, t)
		}); err != nil {
			var ae *ActionError
			if errors.As(err, &ae) {
				ae.Msg = fmt.Sprintf("snapshot add %s: %s (started before the failure: %s)", t, ae.Msg, strings.Join(done, ", "))
			}
			return ActionResult{Data: map[string]any{"started": done}}, err
		}
		done = append(done, t)
	}
	return ActionResult{Message: "snapshot started for " + strings.Join(done, ", "), Data: map[string]any{"started": done}}, nil
}

func srvVerify(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
	p := req.Pipeline
	if srv.jobs == nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: "job runner not configured"}
	}
	mode := req.Args["mode"]
	argv := []string{"verify", "-c", p.ConfigPath, "--mode", mode}
	switch mode {
	case "sample", "full", "delta":
	default:
		return ActionResult{}, BadRequest("mode must be sample, full or delta")
	}
	if t := strings.TrimSpace(req.Args["table"]); t != "" {
		if mode == "delta" {
			return ActionResult{}, BadRequest("table does not apply to delta verify (it checks the keys touched by CDC)")
		}
		if !tableRE.MatchString(t) {
			return ActionResult{}, BadRequest("invalid table %q: expected schema.table", t)
		}
		argv = append(argv, "--table="+t)
	} else if mode == "sample" {
		return ActionResult{}, BadRequest("table is required for sample verify")
	}
	if r := strings.TrimSpace(req.Args["rate"]); r != "" {
		if mode == "delta" {
			return ActionResult{}, BadRequest("rate does not apply to delta verify")
		}
		f, err := strconv.ParseFloat(r, 64)
		max := 100000.0
		if mode == "sample" {
			max = 1
		}
		if !rateRE.MatchString(r) || err != nil || f <= 0 || f > max {
			return ActionResult{}, BadRequest("rate must be a number in (0, %g] for %s verify", max, mode)
		}
		argv = append(argv, "--rate="+r)
	}
	switch strings.ToLower(strings.TrimSpace(req.Args["resume"])) {
	case "", "false", "0", "no":
	case "true", "1", "yes", "on":
		if mode != "full" {
			return ActionResult{}, BadRequest("resume applies to full verify only")
		}
		argv = append(argv, "--resume")
	default:
		return ActionResult{}, BadRequest("resume must be true or false")
	}
	if mode == "delta" {
		if sock := p.AdminSocket(); sock != "" {
			argv = append(argv, "--socket="+sock)
		}
	}
	env, err := p.Env()
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: "env_file: " + err.Error()}
	}
	id, err := srv.jobs.Start(ctx, JobSpec{Pipeline: p.Name, Kind: "verify", Args: argv, Env: env})
	if err != nil {
		var run *ErrJobRunning
		if errors.As(err, &run) {
			return ActionResult{}, Conflict("%v", err)
		}
		return ActionResult{}, &ActionError{Status: 502, Msg: err.Error()}
	}
	return ActionResult{Message: "verify " + mode + " job started", JobID: id}, nil
}

// ---- start / stop / restart ----

func (s *Server) runLifecycle(ctx context.Context, p *Pipeline, verb string) (ActionResult, error) {
	if p.Mode() == ModeSystemd {
		return s.systemctl(ctx, verb, p)
	}
	if p.CfgErr != "" {
		return ActionResult{}, BadRequest("pipeline config is invalid: %s", p.CfgErr)
	}
	switch verb {
	case ActStart:
		return s.bgStart(ctx, p)
	case ActStop:
		return s.bgStop(ctx, p)
	default: // restart
		res, err := s.bgStop(ctx, p)
		if err != nil {
			return res, err
		}
		return s.bgStart(ctx, p)
	}
}

// systemctl start|stop|restart|enable|disable <unit>. Cần quyền của user đang chạy web (không dùng sudo/polkit).
func (s *Server) systemctl(ctx context.Context, verb string, p *Pipeline) (ActionResult, error) {
	out, err := s.reg.cmd.Run(withCmdTimeout(ctx, actionTimeout), "systemctl", verb, p.SystemdUnit)
	res := ActionResult{ExitCode: zeroOr(err)}
	if err != nil {
		msg := trimOut(out)
		if msg == "" {
			msg = err.Error()
		}
		return res, &ActionError{Status: 502, Msg: fmt.Sprintf("systemctl %s %s failed: %s (the web user needs permission to manage the unit; see docs/USAGE.md 7.15)", verb, p.SystemdUnit, msg)}
	}
	res.Message = fmt.Sprintf("systemctl %s %s: ok", verb, p.SystemdUnit)
	s.reg.invalidateUnit(p.SystemdUnit)
	return res, nil
}

func (r *Registry) invalidateUnit(unit string) {
	r.unitMu.Lock()
	delete(r.unitCache, unit)
	r.unitMu.Unlock()
}

// runStrand chạy `<strand_binary> <args...>` với môi trường của env_file của pipeline, argv mảng, không shell.
func (s *Server) runStrand(ctx context.Context, p *Pipeline, args ...string) (string, *int, error) {
	env, err := p.Env()
	if err != nil {
		return "", nil, &ActionError{Status: 500, Msg: "env_file: " + err.Error()}
	}
	cmd := exec.CommandContext(ctx, s.strandBinary(), args...)
	cmd.Env = append(os.Environ(), env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	runErr := cmd.Run()
	return trimOut(out.Bytes()), zeroOr(runErr), runErr
}

// bgStart: `strand sync -c <cfg> --background` (chế độ nền; tự tách khỏi web bằng setsid).
func (s *Server) bgStart(ctx context.Context, p *Pipeline) (ActionResult, error) {
	args := []string{"sync", "-c", p.ConfigPath, "--background"}
	if p.LogFile != "" {
		args = append(args, "--log-file="+p.LogFile)
	}
	if p.PidFile != "" {
		args = append(args, "--pid-file="+p.PidFile)
	}
	out, code, err := s.runStrand(ctx, p, args...)
	res := ActionResult{ExitCode: code, Message: out}
	if err != nil {
		var ae *ActionError
		if errors.As(err, &ae) {
			return res, err
		}
		return res, &ActionError{Status: 502, Msg: fmt.Sprintf("strand sync --background failed: %s", firstNonEmpty(out, err.Error()))}
	}
	return res, nil
}

// bgStop: `strand stop -c <cfg>` (SIGTERM, chờ ghi checkpoint; mặc định tối đa 150 s).
func (s *Server) bgStop(ctx context.Context, p *Pipeline) (ActionResult, error) {
	args := []string{"stop", "-c", p.ConfigPath}
	if p.PidFile != "" {
		args = append(args, "--pid-file="+p.PidFile)
	}
	out, code, err := s.runStrand(ctx, p, args...)
	res := ActionResult{ExitCode: code, Message: out}
	if err != nil {
		var ae *ActionError
		if errors.As(err, &ae) {
			return res, err
		}
		return res, &ActionError{Status: 502, Msg: fmt.Sprintf("strand stop failed: %s", firstNonEmpty(out, err.Error()))}
	}
	return res, nil
}

func firstNonEmpty(a ...string) string {
	for _, x := range a {
		if x != "" {
			return x
		}
	}
	return ""
}
