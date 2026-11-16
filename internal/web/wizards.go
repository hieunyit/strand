package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"strand/internal/admin"
)

// Wizard Repair và Promote (docs/PLAN-WEB-UI.md §3, M4).
//
// Mỗi bước là một action riêng do người dùng bấm; thứ tự bước được ép Ở SERVER theo trạng thái lưu tại
// <state_dir>/wizards/<kind>-<pipeline>.json (ghi nguyên tử, đọc lại từ đĩa ở mỗi request nên web khởi động
// lại vẫn tiếp tục đúng bước). Mọi việc nặng là tiến trình con `strand` qua JobRunner (argv mảng, env_file,
// một job mỗi pipeline). Điều kiện an toàn nằm ở CLI (verify --repair fix từ chối khi pipeline còn chạy,
// promote có fence + exit 10); wizard không cài lại logic đó. Bí mật (DSN) không bao giờ được lưu: người
// dùng nhập lại ở mỗi lần chạy, audit và JobStatus chỉ có bản đã che.

const (
	ActRepairBegin = "repair-begin"
	ActRepairStop  = "repair-stop"
	ActRepairRun   = "repair-run"
	ActRepairStart = "repair-start"
	ActRepairAbort = "repair-abort"

	ActPromotePlan     = "promote-plan"
	ActPromoteRun      = "promote-run"
	ActPromoteFailover = "promote-failover"
	ActPromoteAbort    = "promote-abort"
)

// Bước (giá trị lưu trong wizardState.Step).
const (
	// repair
	stRepWait   = "wait"   // bước 1: chờ lag; kế tiếp là bước 2 (Stop)
	stRepVerify = "verify" // đã dừng pipeline; kế tiếp là bước 3 (chạy verify --repair fix)
	stRepStart  = "start"  // verify xong mã 0; kế tiếp là bước 4 (Start)
	// promote
	stProPlan     = "plan"     // chưa có kế hoạch (dry-run) thành công
	stProPlanning = "planning" // job dry-run đang chạy
	stProReady    = "ready"    // dry-run ok; promote được phép
	stProRunning  = "running"  // job promote đang chạy
	stProDecision = "decision" // exit 10: chờ người vận hành quyết định
	stProFailed   = "failed"   // promote thoát khác 0 và 10: có thể chạy lại (CLI tiếp từ bước dở)
	// chung
	stDone    = "done"
	stAborted = "aborted"
)

type wizardEvent struct {
	Time   time.Time `json:"time"`
	Step   string    `json:"step"`
	Result string    `json:"result"`
	JobID  string    `json:"job_id,omitempty"`
	Exit   *int      `json:"exit_code,omitempty"`
}

type wizardState struct {
	Kind     string            `json:"kind"`
	Pipeline string            `json:"pipeline"`
	Step     string            `json:"step"`
	Created  time.Time         `json:"created"`
	Updated  time.Time         `json:"updated"`
	JobID    string            `json:"job_id,omitempty"`   // job của bước hiện tại
	JobDone  bool              `json:"job_done,omitempty"` // kết quả job đã được ghi vào state
	LastExit *int              `json:"last_exit,omitempty"`
	Failover bool              `json:"failover,omitempty"` // job promote gần nhất chạy với --source-fenced-externally
	Opts     map[string]string `json:"opts,omitempty"`     // tùy chọn không bí mật, điền sẵn vào form
	History  []wizardEvent     `json:"history,omitempty"`
}

func (st *wizardState) active() bool { return st != nil && st.Step != stDone && st.Step != stAborted }

func (st *wizardState) event(now time.Time, step, result, job string, exit *int) {
	st.History = append(st.History, wizardEvent{Time: now.UTC(), Step: step, Result: result, JobID: job, Exit: exit})
	st.Updated = now.UTC()
}

func (s *Server) wizPath(kind, pipeline string) string {
	return filepath.Join(s.cfg.StateDir, "wizards", kind+"-"+pipeline+".json")
}

// wizLoad đọc state từ đĩa (nil nếu chưa có).
func (s *Server) wizLoad(kind, pipeline string) (*wizardState, error) {
	if s.db != nil {
		ctx, cancel := dbCtx()
		defer cancel()
		b, ok, err := s.db.st.GetWizard(ctx, kind, pipeline)
		if err != nil || !ok {
			return nil, err
		}
		var st wizardState
		if err := json.Unmarshal(b, &st); err != nil {
			return nil, fmt.Errorf("wizard state %s-%s: %w", kind, pipeline, err)
		}
		return &st, nil
	}
	b, err := os.ReadFile(s.wizPath(kind, pipeline))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st wizardState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("wizard state %s: %w", s.wizPath(kind, pipeline), err)
	}
	return &st, nil
}

func (s *Server) wizSave(st *wizardState) error {
	if s.db != nil {
		b, err := json.MarshalIndent(st, "", "  ")
		if err != nil {
			return err
		}
		ctx, cancel := dbCtx()
		defer cancel()
		return s.db.st.PutWizard(ctx, st.Kind, st.Pipeline, b)
	}
	p := s.wizPath(st.Kind, st.Pipeline)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

var wizLocks sync.Map // "kind-pipeline" -> *sync.Mutex

// wizLock tuần tự hóa các action của cùng một wizard (kiểm trạng thái + chuyển bước phải nguyên tử).
func wizLock(kind, pipeline string) func() {
	v, _ := wizLocks.LoadOrStore(kind+"-"+pipeline, &sync.Mutex{})
	m := v.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

func jobSucceeded(j JobStatus) bool {
	return !j.Running && !j.Cancelled && j.ExitCode != nil && *j.ExitCode == 0
}

// syncWizard đồng bộ trạng thái wizard với kết quả job (job chạy độc lập với web). Gọi khi đã giữ khóa.
func (s *Server) syncWizard(st *wizardState) {
	if st == nil || st.JobDone || st.JobID == "" || s.jobs == nil {
		return
	}
	switch st.Step {
	case stRepVerify, stProPlanning, stProRunning:
	default:
		return
	}
	j, ok := s.jobs.Get(st.JobID)
	if ok && j.Running {
		return
	}
	var exit *int
	if ok {
		exit = j.ExitCode
	}
	st.JobDone, st.LastExit = true, exit
	prev := st.Step
	switch prev {
	case stRepVerify:
		if ok && jobSucceeded(j) {
			st.Step = stRepStart
		}
	case stProPlanning:
		st.Step = stProPlan
		if ok && jobSucceeded(j) {
			st.Step = stProReady
		}
	case stProRunning:
		switch {
		case ok && jobSucceeded(j):
			st.Step = stDone
		case ok && !j.Cancelled && exit != nil && *exit == 10:
			st.Step = stProDecision
		default:
			st.Step = stProFailed
		}
	}
	st.event(s.now(), prev, "job finished", st.JobID, exit)
	_ = s.wizSave(st)
}

// ---- tiện ích dùng chung ----

var (
	durRE    = regexp.MustCompile(`^[0-9]{1,5}(s|m|h)$`)
	identsRE = regexp.MustCompile(`^[A-Za-z0-9_.,$-]{1,200}$`)
)

func (s *Server) startWizardJob(ctx context.Context, p *Pipeline, kind string, args, display []string, extraEnv ...string) (string, error) {
	env, err := p.Env()
	if err != nil {
		return "", &ActionError{Status: 500, Msg: "env_file: " + err.Error()}
	}
	env = append(env, extraEnv...)
	id, err := s.jobs.Start(ctx, JobSpec{Pipeline: p.Name, Kind: kind, Args: args, DisplayArgs: display, Env: env})
	if err != nil {
		var run *ErrJobRunning
		if errors.As(err, &run) {
			return "", Conflict("%v", err)
		}
		return "", &ActionError{Status: 502, Msg: err.Error()}
	}
	return id, nil
}

// liveLag đọc lag hiện tại thẳng từ admin socket (không dùng mẫu 5 s đã cũ).
func (s *Server) liveLag(ctx context.Context, p *Pipeline) (float64, error) {
	var top *admin.TopInfo
	err := s.adminCall(ctx, p, 10*time.Second, func(ctx context.Context, c *admin.Client) (e error) {
		top, e = c.Top(ctx)
		return
	})
	if err != nil {
		return 0, err
	}
	return top.LagSeconds, nil
}

func (s *Server) isRunning(p *Pipeline) bool {
	v := s.view(p)
	return s.controls(p, v).Running
}

func (s *Server) registerWizards() {
	a := s.actions
	a.Register(Action{Name: ActRepairBegin, ConfirmName: true, Run: wizRepairBegin})
	a.Register(Action{Name: ActRepairStop, Run: wizRepairStop})
	a.Register(Action{Name: ActRepairRun, ConfirmName: true, Run: wizRepairRun})
	a.Register(Action{Name: ActRepairStart, Run: wizRepairStart})
	a.Register(Action{Name: ActRepairAbort, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		return srv.wizAbort(req, "repair")
	}})
	secret := []string{"target_dsn", "fence_dsn"}
	a.Register(Action{Name: ActPromotePlan, SecretArgs: secret, Run: wizPromotePlan})
	a.Register(Action{Name: ActPromoteRun, ConfirmName: true, SecretArgs: secret, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		return srv.wizPromoteRun(ctx, req, false)
	}})
	a.Register(Action{Name: ActPromoteFailover, ConfirmName: true, SecretArgs: secret, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		// Failover: hộp thoại thứ hai, gõ lại tên một lần nữa (confirm_name đã được apiAction kiểm).
		if req.Args["confirm_name2"] != req.Pipeline.Name {
			return ActionResult{}, BadRequest("failover needs the pipeline name typed a second time (confirm_name2)")
		}
		return srv.wizPromoteRun(ctx, req, true)
	}})
	a.Register(Action{Name: ActPromoteAbort, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		return srv.wizAbort(req, "promote")
	}})
}

func (s *Server) wizAbort(req ActionRequest, kind string) (ActionResult, error) {
	p := req.Pipeline
	defer wizLock(kind, p.Name)()
	st, err := s.wizLoad(kind, p.Name)
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: err.Error()}
	}
	s.syncWizard(st)
	if !st.active() {
		return ActionResult{}, Conflict("no %s wizard in progress", kind)
	}
	if st.JobID != "" && s.jobs != nil {
		if j, ok := s.jobs.Get(st.JobID); ok && j.Running {
			return ActionResult{}, Conflict("a %s job is still running (%s); cancel it from the job page first", j.Kind, j.ID)
		}
	}
	prev := st.Step
	st.Step = stAborted
	st.event(s.now(), prev, "aborted", "", nil)
	if err := s.wizSave(st); err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: "save wizard state: " + err.Error()}
	}
	msg := "wizard closed; nothing was changed by closing it"
	if kind == "repair" && prev != stRepWait {
		msg = "wizard closed; the pipeline was stopped by this wizard and stays stopped: start it from the pipeline page when ready"
	}
	if kind == "promote" {
		msg = "wizard closed; promote progress stays in _strand_promote, the next promote run resumes it"
	}
	return ActionResult{Message: msg}, nil
}

// ---- Repair ----

func wizRepairBegin(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
	p := req.Pipeline
	if p.CfgErr != "" {
		return ActionResult{}, BadRequest("pipeline config is invalid: %s", p.CfgErr)
	}
	thr := 5.0
	opts := map[string]string{}
	if v := strings.TrimSpace(req.Args["lag_threshold"]); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if !rateRE.MatchString(v) || err != nil || f > 3600 {
			return ActionResult{}, BadRequest("lag_threshold must be a number of seconds between 0 and 3600")
		}
		thr = f
	}
	opts["lag_threshold"] = strconv.FormatFloat(thr, 'f', -1, 64)
	if t := strings.TrimSpace(req.Args["table"]); t != "" {
		if !tableRE.MatchString(t) {
			return ActionResult{}, BadRequest("invalid table %q: expected schema.table", t)
		}
		opts["table"] = t
	}
	defer wizLock("repair", p.Name)()
	st, err := srv.wizLoad("repair", p.Name)
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: err.Error()}
	}
	srv.syncWizard(st)
	if st.active() {
		return ActionResult{}, Conflict("a repair wizard is already in progress (step %s); finish or abort it first", st.Step)
	}
	now := srv.now().UTC()
	st = &wizardState{Kind: "repair", Pipeline: p.Name, Step: stRepWait, Created: now, Updated: now, Opts: opts}
	st.event(now, "begin", "started", "", nil)
	if err := srv.wizSave(st); err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: "save wizard state: " + err.Error()}
	}
	return ActionResult{Message: "repair wizard started: wait for the lag to fall below the threshold, then stop the pipeline", Data: map[string]any{"step": st.Step}}, nil
}

// wizRepairStop: bước 2. Chỉ khi lag đọc được và dưới ngưỡng (hoặc pipeline đã dừng), rồi Stop bằng đúng
// helper của nút Stop (systemctl hoặc `strand stop`).
func wizRepairStop(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
	p := req.Pipeline
	defer wizLock("repair", p.Name)()
	st, err := srv.wizLoad("repair", p.Name)
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: err.Error()}
	}
	srv.syncWizard(st)
	if !st.active() || st.Step != stRepWait {
		return ActionResult{}, Conflict("step 2 (stop) is not available now: wizard step is %q", stepOf(st))
	}
	var res ActionResult
	if srv.isRunning(p) {
		thr, _ := strconv.ParseFloat(st.Opts["lag_threshold"], 64)
		lag, lerr := srv.liveLag(ctx, p)
		if lerr != nil {
			return ActionResult{}, Conflict("cannot read the lag, so the pipeline is not stopped: %v", lerr)
		}
		if lag > thr {
			return ActionResult{}, Conflict("lag is %.1f s, above the threshold of %g s: wait until the pipeline has caught up", lag, thr)
		}
		res, err = srv.runLifecycle(ctx, p, ActStop)
		if err != nil {
			st.event(srv.now(), "stop", "error: "+err.Error(), "", res.ExitCode)
			_ = srv.wizSave(st)
			return res, err
		}
	} else {
		res.Message = "pipeline is already stopped"
	}
	st.Step = stRepVerify
	st.JobID, st.JobDone, st.LastExit = "", false, nil
	st.event(srv.now(), "stop", "ok", "", res.ExitCode)
	if err := srv.wizSave(st); err != nil {
		return res, &ActionError{Status: 500, Msg: "pipeline stopped but saving wizard state failed: " + err.Error()}
	}
	res.Data = map[string]any{"step": st.Step}
	return res, nil
}

func stepOf(st *wizardState) string {
	if st == nil {
		return "none"
	}
	return st.Step
}

// wizRepairRun: bước 3, `strand verify -c <cfg> --mode full --repair fix [--table=<t>]`.
func wizRepairRun(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
	p := req.Pipeline
	defer wizLock("repair", p.Name)()
	st, err := srv.wizLoad("repair", p.Name)
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: err.Error()}
	}
	srv.syncWizard(st)
	if !st.active() || st.Step != stRepVerify {
		return ActionResult{}, Conflict("step 3 (repair) needs step 2 (stop) to have succeeded: wizard step is %q", stepOf(st))
	}
	argv := []string{"verify", "-c", p.ConfigPath, "--mode", "full", "--repair", "fix"}
	if t := st.Opts["table"]; t != "" {
		argv = append(argv, "--table="+t)
	}
	id, err := srv.startWizardJob(ctx, p, "repair", argv, nil)
	if err != nil {
		return ActionResult{}, err
	}
	st.JobID, st.JobDone, st.LastExit = id, false, nil
	st.event(srv.now(), "repair", "job started", id, nil)
	if err := srv.wizSave(st); err != nil {
		return ActionResult{JobID: id}, &ActionError{Status: 500, Msg: "job started but saving wizard state failed: " + err.Error()}
	}
	return ActionResult{Message: "verify --repair fix job started", JobID: id}, nil
}

// wizRepairStart: bước 4. Chỉ khi job repair kết thúc mã 0.
func wizRepairStart(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
	p := req.Pipeline
	defer wizLock("repair", p.Name)()
	st, err := srv.wizLoad("repair", p.Name)
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: err.Error()}
	}
	srv.syncWizard(st)
	if !st.active() || st.Step != stRepStart {
		return ActionResult{}, Conflict("step 4 (start) needs the repair job to have finished with exit 0: wizard step is %q", stepOf(st))
	}
	res, err := srv.runLifecycle(ctx, p, ActStart)
	if err != nil {
		st.event(srv.now(), "start", "error: "+err.Error(), "", res.ExitCode)
		_ = srv.wizSave(st)
		return res, err
	}
	st.Step = stDone
	st.event(srv.now(), "start", "ok", "", res.ExitCode)
	if err := srv.wizSave(st); err != nil {
		return res, &ActionError{Status: 500, Msg: "pipeline started but saving wizard state failed: " + err.Error()}
	}
	res.Data = map[string]any{"step": st.Step}
	return res, nil
}

// ---- Promote ----

// promoteArgs dựng argv cho `strand promote`. display thay các DSN bằng <hidden>. opts là tùy chọn không bí mật.
func promoteArgs(p *Pipeline, a map[string]string, failover, dry bool) (argv, display, secretEnv []string, opts map[string]string, err error) {
	if p.Cfg == nil {
		return nil, nil, nil, nil, BadRequest("pipeline config is not readable: %s", p.CfgErr)
	}
	target := strings.TrimSpace(a["target_dsn"])
	fence := strings.TrimSpace(a["fence_dsn"])
	for _, d := range []struct{ n, v string }{{"target_dsn", target}, {"fence_dsn", fence}} {
		if len(d.v) > 4096 || strings.IndexFunc(d.v, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
			return nil, nil, nil, nil, BadRequest("%s has invalid characters", d.n)
		}
	}
	if target == "" {
		return nil, nil, nil, nil, BadRequest("target_dsn is required (the promote DSN of the target, e.g. the strand_admin user)")
	}
	if fence == "" && !failover {
		return nil, nil, nil, nil, BadRequest("fence_dsn is required: promote fences the old source through it. If the source is dead, run the promote and use the failover dialog when the CLI asks for a decision")
	}
	id := p.Cfg.Name
	if id == "" {
		id = p.Name
	}
	engine := "mysql"
	switch strings.ToLower(p.Cfg.Target.Kind) {
	case "postgres", "postgresql", "pg":
		engine = "pg"
	}
	argv = []string{"promote", "-c", p.ConfigPath, "--pipeline=" + id, "--engine=" + engine}
	display = append([]string(nil), argv...)
	// DSN (có thể kèm mật khẩu) đi qua biến môi trường của tiến trình con, không qua argv: argv đọc được
	// bằng ps bởi mọi user trên máy. Không ghi vào job record/audit.
	secretEnv = append(secretEnv, "STRAND_PROMOTE_TARGET_DSN="+target)
	display = append(display, "--target=<env>")
	if fence != "" {
		secretEnv = append(secretEnv, "STRAND_PROMOTE_FENCE_DSN="+fence)
		display = append(display, "--fence=<env>")
	}
	opts = map[string]string{}
	add := func(flag string, val string) {
		argv = append(argv, "--"+flag+"="+val)
		display = append(display, "--"+flag+"="+val)
		opts[strings.ReplaceAll(flag, "-", "_")] = val
	}
	for _, o := range []struct {
		key, flag string
		re        *regexp.Regexp
	}{
		{"catchup_timeout", "catchup-timeout", durRE}, {"fence_timeout", "fence-timeout", durRE},
		{"peak_rate", "peak-rate", rateRE}, {"sync_interval", "sync-interval", rateRE},
		{"app_role", "app-role", identsRE}, {"source_app_roles", "source-app-roles", identsRE}, {"schemas", "schemas", identsRE},
	} {
		v := strings.TrimSpace(a[o.key])
		if v == "" {
			continue
		}
		if !o.re.MatchString(v) {
			return nil, nil, nil, nil, BadRequest("invalid %s", o.key)
		}
		if engine != "pg" && (o.key == "app_role" || o.key == "schemas") {
			continue // chỉ có nghĩa với PostgreSQL
		}
		add(o.flag, v)
	}
	switch strings.ToLower(strings.TrimSpace(a["objects_post"])) {
	case "", "false", "0", "no":
	case "true", "1", "yes", "on":
		if engine != "mysql" {
			return nil, nil, nil, nil, BadRequest("objects_post applies to a MySQL target only")
		}
		argv, display = append(argv, "--objects-post"), append(display, "--objects-post")
		opts["objects_post"] = "true"
	default:
		return nil, nil, nil, nil, BadRequest("objects_post must be true or false")
	}
	if v := strings.TrimSpace(a["override_sequences"]); v != "" {
		var m map[string]int64
		if json.Unmarshal([]byte(v), &m) != nil || len(m) == 0 || len(v) > 8192 {
			return nil, nil, nil, nil, BadRequest(`override_sequences must be a JSON object of sequence name to integer, e.g. {"public.orders_id_seq": 123456}`)
		}
		b, _ := json.Marshal(m)
		argv, display = append(argv, "--override-sequences="+string(b)), append(display, "--override-sequences="+string(b))
	}
	if failover {
		argv, display = append(argv, "--source-fenced-externally"), append(display, "--source-fenced-externally")
	}
	if dry {
		argv, display = append(argv, "--dry-run"), append(display, "--dry-run")
	}
	return argv, display, secretEnv, opts, nil
}

// wizPromotePlan: đọc trạng thái promote hiện tại bằng `strand promote --dry-run` (CLI nạp _strand_promote ở chế độ
// chỉ đọc và in kế hoạch); không ghi gì vào target.
func wizPromotePlan(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
	p := req.Pipeline
	defer wizLock("promote", p.Name)()
	st, err := srv.wizLoad("promote", p.Name)
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: err.Error()}
	}
	srv.syncWizard(st)
	now := srv.now().UTC()
	switch {
	case st == nil || !st.active():
		st = &wizardState{Kind: "promote", Pipeline: p.Name, Step: stProPlan, Created: now, Updated: now}
	case st.Step == stProPlanning || st.Step == stProRunning:
		return ActionResult{}, Conflict("a promote job is running; wait for it to finish")
	}
	// Failover chỉ bàn tới sau exit 10: dry-run luôn là bản thường (không --source-fenced-externally).
	argv, display, secretEnv, opts, err := promoteArgs(p, req.Args, false, true)
	if err != nil {
		return ActionResult{}, err
	}
	id, err := srv.startWizardJob(ctx, p, "promote-plan", argv, display, secretEnv...)
	if err != nil {
		return ActionResult{}, err
	}
	st.Opts = opts
	prev := st.Step
	st.Step, st.JobID, st.JobDone, st.LastExit, st.Failover = stProPlanning, id, false, nil, false
	st.event(now, "plan", "job started (from "+prev+")", id, nil)
	if err := srv.wizSave(st); err != nil {
		return ActionResult{JobID: id}, &ActionError{Status: 500, Msg: "job started but saving wizard state failed: " + err.Error()}
	}
	return ActionResult{Message: "promote dry-run started", JobID: id}, nil
}

func (s *Server) wizPromoteRun(ctx context.Context, req ActionRequest, failover bool) (ActionResult, error) {
	p := req.Pipeline
	defer wizLock("promote", p.Name)()
	st, err := s.wizLoad("promote", p.Name)
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: err.Error()}
	}
	s.syncWizard(st)
	ok := st.active() && (st.Step == stProReady || st.Step == stProDecision || st.Step == stProFailed)
	if failover {
		ok = st.active() && st.Step == stProDecision
	}
	if !ok {
		if failover {
			return ActionResult{}, Conflict("failover is offered only after the CLI stopped with exit 10 (needs a decision): wizard step is %q", stepOf(st))
		}
		return ActionResult{}, Conflict("promote needs a successful dry-run plan first (step 1): wizard step is %q", stepOf(st))
	}
	argv, display, secretEnv, opts, err := promoteArgs(p, req.Args, failover, false)
	if err != nil {
		return ActionResult{}, err
	}
	kind := "promote"
	if failover {
		kind = "promote-failover"
	}
	id, err := s.startWizardJob(ctx, p, kind, argv, display, secretEnv...)
	if err != nil {
		return ActionResult{}, err
	}
	st.Opts = opts
	prev := st.Step
	st.Step, st.JobID, st.JobDone, st.LastExit, st.Failover = stProRunning, id, false, nil, failover
	st.event(s.now(), kind, "job started (from "+prev+")", id, nil)
	if err := s.wizSave(st); err != nil {
		return ActionResult{JobID: id}, &ActionError{Status: 500, Msg: "job started but saving wizard state failed: " + err.Error()}
	}
	return ActionResult{Message: kind + " job started", JobID: id}, nil
}

// ---- view (trang + REST) ----

type wizardStep struct {
	N      int    `json:"n"`
	Title  string `json:"title"`
	Status string `json:"status"` // todo | current | done | failed
	Note   string `json:"note,omitempty"`
}

type wizardView struct {
	Kind     string            `json:"kind"`
	Pipeline string            `json:"pipeline"`
	State    *wizardState      `json:"state"`
	Active   bool              `json:"active"`
	Step     string            `json:"step"`
	Steps    []wizardStep      `json:"steps"`
	Job      *JobStatus        `json:"job,omitempty"`
	Output   string            `json:"output_tail,omitempty"`
	Running  bool              `json:"pipeline_running"`
	Lag      *float64          `json:"lag_seconds,omitempty"`
	LagOK    bool              `json:"lag_ok"`
	LagNote  string            `json:"lag_note,omitempty"`
	Thr      float64           `json:"lag_threshold"`
	Decision string            `json:"decision,omitempty"` // phần CLI in khi exit 10, nguyên văn
	PlanInfo string            `json:"promote_state,omitempty"`
	Engine   string            `json:"engine,omitempty"`
	Opts     map[string]string `json:"opts"`
	Name     string            `json:"-"`
	View     PipelineView      `json:"-"`
	Ctl      Controls          `json:"-"`
	Head     HeadInfo          `json:"-"`
	Can      map[string]bool   `json:"can"` // action nào hợp lệ ở bước hiện tại (UI)
}

const decisionMarker = "=== PROMOTE NEEDS OPERATOR DECISION ==="

// jobTail đọc output của job, tối đa limit byte cuối.
func (s *Server) jobText(id string) string {
	var sb strings.Builder
	var off int64
	for i := 0; i < 16; i++ {
		o, ok := s.jobs.Output(id, off)
		if !ok {
			break
		}
		sb.WriteString(o.Data)
		if o.Offset == off {
			break
		}
		off = o.Offset
	}
	return sb.String()
}

func tail(s string, n int) string {
	if len(s) > n {
		s = "..." + s[len(s)-n:]
		s = strings.ToValidUTF8(s, "")
	}
	return s
}

// promoteStateFrom lấy trạng thái promote từ log JSON của dry-run ("resuming promote from interrupted step").
func promoteStateFrom(out string) string {
	for _, ln := range strings.Split(out, "\n") {
		if !strings.Contains(ln, "resuming promote") {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(ln), &m) != nil {
			continue
		}
		return fmt.Sprintf("a promote is in progress: current step %v, completed steps %v", m["current_step"], m["completed_steps"])
	}
	if strings.Contains(out, "Plan promote (dry-run): OK") {
		return "no promote in progress for this pipeline (fresh start)"
	}
	return ""
}

func (s *Server) wizardView(ctx context.Context, p *Pipeline, kind string) (wizardView, error) {
	unlock := wizLock(kind, p.Name)
	st, err := s.wizLoad(kind, p.Name)
	if err == nil {
		s.syncWizard(st)
	}
	unlock()
	v := wizardView{Kind: kind, Pipeline: p.Name, Name: p.Name, State: st, Active: st.active(), Opts: map[string]string{}, Can: map[string]bool{}}
	if err != nil {
		return v, err
	}
	v.Step = stepOf(st)
	v.View = s.view(p)
	v.Ctl = s.controls(p, v.View)
	if v.Ctl.Reachable {
		v.Head = s.headInfo(ctx, p)
	}
	v.Running = v.Ctl.Running
	if st != nil {
		for k, x := range st.Opts {
			v.Opts[k] = x
		}
	}
	if p.Cfg != nil {
		v.Engine = p.Cfg.Source.Kind + " to " + p.Cfg.Target.Kind
	}
	if st != nil && st.JobID != "" && s.jobs != nil {
		if j, ok := s.jobs.Get(st.JobID); ok {
			v.Job = &j
			text := s.jobText(j.ID)
			v.Output = tail(text, 6000)
			if kind == "promote" {
				if i := strings.LastIndex(text, decisionMarker); i >= 0 && st.Step == stProDecision {
					v.Decision = tail(strings.TrimSpace(text[i:]), 4000)
				}
				if j.Kind == "promote-plan" {
					v.PlanInfo = promoteStateFrom(text)
				}
			}
		}
	}
	if kind == "repair" {
		thr, _ := strconv.ParseFloat(v.Opts["lag_threshold"], 64)
		v.Thr = thr
		if v.Running {
			if lag, lerr := s.liveLag(ctx, p); lerr == nil {
				v.Lag = &lag
				v.LagOK = lag <= thr
			} else {
				v.LagNote = lerr.Error()
			}
		}
		s.repairSteps(&v)
	} else {
		s.promoteSteps(&v)
	}
	return v, nil
}

func (s *Server) repairSteps(v *wizardView) {
	step := stepOf(v.State)
	mk := func(n int, title string, done, cur bool, note string) wizardStep {
		ws := wizardStep{N: n, Title: title, Status: "todo", Note: note}
		if done {
			ws.Status = "done"
		} else if cur {
			ws.Status = "current"
		}
		return ws
	}
	after := func(ss ...string) bool {
		for _, x := range ss {
			if step == x {
				return true
			}
		}
		return false
	}
	v.Steps = []wizardStep{
		mk(1, "Wait until the lag is near 0", after(stRepVerify, stRepStart, stDone), step == stRepWait, ""),
		mk(2, "Stop the pipeline", after(stRepVerify, stRepStart, stDone), step == stRepWait, ""),
		mk(3, "Run verify --mode full --repair fix", after(stRepStart, stDone), step == stRepVerify, ""),
		mk(4, "Start the pipeline", step == stDone, step == stRepStart, ""),
	}
	// Bước 1 và 2 cùng một giai đoạn "wait": 1 hoàn thành khi sang 2.
	if step == stRepWait {
		v.Steps[0].Status = "current"
		v.Steps[1].Status = "todo"
	}
	if step == stRepVerify && v.State.JobID != "" && v.State.JobDone {
		v.Steps[2].Status = "failed"
		v.Steps[2].Note = "the last repair job did not finish with exit 0; read its output, then run the step again"
	}
	v.Can[ActRepairStop] = step == stRepWait && (!v.Running || (v.Lag != nil && v.LagOK))
	v.Can[ActRepairRun] = step == stRepVerify && !(v.Job != nil && v.Job.Running)
	v.Can[ActRepairStart] = step == stRepStart
}

func (s *Server) promoteSteps(v *wizardView) {
	step := stepOf(v.State)
	has := func(ss ...string) bool {
		for _, x := range ss {
			if step == x {
				return true
			}
		}
		return false
	}
	st := func(done, cur, fail bool) string {
		switch {
		case done:
			return "done"
		case fail:
			return "failed"
		case cur:
			return "current"
		}
		return "todo"
	}
	failedPlan := step == stProPlan && v.State != nil && v.State.JobDone
	v.Steps = []wizardStep{
		{N: 1, Title: "Check the current promote state (strand promote --dry-run)", Status: st(has(stProReady, stProRunning, stProDecision, stProFailed, stDone), has(stProPlan, stProPlanning), failedPlan)},
		{N: 2, Title: "Promote: fence the old source, hold the target read-only, catch up, sequences, open writes", Status: st(step == stDone, has(stProReady, stProRunning, stProFailed), false)},
		{N: 3, Title: "If the CLI needs a decision (exit 10): failover or sequence values", Status: st(false, step == stProDecision, false)},
		{N: 4, Title: "After promote: switch the application and stop the pipeline", Status: st(false, step == stDone, false)},
	}
	if v.State != nil && v.State.Step == stProFailed {
		v.Steps[1].Status, v.Steps[1].Note = "failed", "the CLI exited with an error; fix the cause and run again (it resumes from the unfinished step)"
	}
	busy := v.Job != nil && v.Job.Running
	v.Can[ActPromotePlan] = !busy && (!v.Active || has(stProPlan, stProReady, stProDecision, stProFailed))
	v.Can[ActPromoteRun] = !busy && has(stProReady, stProDecision, stProFailed)
	v.Can[ActPromoteFailover] = !busy && step == stProDecision
}

type wizardPage struct {
	wizardView
	Tabs []tabLink
}

func (s *Server) pageWizard(w http.ResponseWriter, r *http.Request, sess *Session, kind string) {
	p := s.reg.Get(r.PathValue("name"))
	if p == nil {
		http.NotFound(w, r)
		return
	}
	v, err := s.wizardView(r.Context(), p, kind)
	if err != nil {
		http.Error(w, "wizard state unreadable: "+err.Error(), http.StatusInternalServerError)
		return
	}
	base := "/pipelines/" + p.Name
	pg := wizardPage{wizardView: v}
	for _, t := range [][2]string{{"graph", "Graph"}, {"tables", "Tables"}, {"checkpoint", "Checkpoint"}, {"errors", "Errors"}, {"config", "Config"}, {"logs", "Logs"}, {"jobs", "Jobs"}, {"repair", "Repair"}, {"promote", "Promote"}} {
		href := base
		if t[0] != "graph" {
			href += "/" + t[0]
		}
		pg.Tabs = append(pg.Tabs, tabLink{Key: t[0], Label: t[1], Href: href})
	}
	refresh := 5
	if v.Job != nil && v.Job.Running {
		refresh = 2
	}
	s.render(w, "wizard", sess, pageData{Title: p.Name, DocTitle: p.Name + " \u00b7 " + tabLabels[pg.Kind], Active: p.Name, Refresh: refresh, Body: pg})
}

func (s *Server) wizardRoutes(mux *http.ServeMux) {
	for _, kind := range []string{"repair", "promote"} {
		kind := kind
		mux.Handle("GET /pipelines/{name}/"+kind, s.auth(false, func(w http.ResponseWriter, r *http.Request, sess *Session) {
			s.pageWizard(w, r, sess, kind)
		}))
		mux.Handle("GET /api/v1/pipelines/{name}/"+kind, s.auth(true, func(w http.ResponseWriter, r *http.Request, _ *Session) {
			p := s.lookup(w, r)
			if p == nil {
				return
			}
			v, err := s.wizardView(r.Context(), p, kind)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, v)
		}))
	}
}
