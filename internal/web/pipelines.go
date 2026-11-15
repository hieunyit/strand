package web

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"strand/internal/config"
	"strand/internal/exitcode"
)

// Commander chạy lệnh ngoài (systemctl, journalctl). Tách ra interface để test không cần systemd.
// Đối số luôn là mảng, không bao giờ qua shell.
type Commander interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

type execCommander struct{}

type cmdTimeoutKey struct{}

// withCmdTimeout nới giới hạn mặc định (10 s) của execCommander cho lệnh chạy lâu (systemctl stop chờ ghi checkpoint).
func withCmdTimeout(ctx context.Context, d time.Duration) context.Context {
	return context.WithValue(ctx, cmdTimeoutKey{}, d)
}

func (execCommander) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	d := 10 * time.Second
	if v, ok := ctx.Value(cmdTimeoutKey{}).(time.Duration); ok {
		d = v
	}
	ctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), "LC_ALL=C", "SYSTEMD_COLORS=0", "SYSTEMD_PAGER=")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err := cmd.Run()
	return out.Bytes(), err
}

// Mode: cách quản lý pipeline.
const (
	ModeSystemd    = "systemd"
	ModeBackground = "background"
)

// Pipeline là một pipeline đã đăng ký.
type Pipeline struct {
	Name        string
	ConfigPath  string
	EnvFile     string
	SystemdUnit string // rỗng = chế độ nền
	LogFile     string // ghi đè (chế độ nền)
	PidFile     string
	Origin      string // "web.yaml" | "scan"

	// Lấy từ file cấu hình pipeline (rỗng nếu đọc lỗi; xem CfgErr).
	Cfg    *config.Config
	CfgErr string
}

// Mode trả cách quản lý hiệu lực.
func (p *Pipeline) Mode() string {
	if p.SystemdUnit != "" {
		return ModeSystemd
	}
	return ModeBackground
}

// AdminSocket trả đường dẫn admin socket ("" nếu cấu hình không đặt).
func (p *Pipeline) AdminSocket() string {
	if p.Cfg == nil {
		return ""
	}
	return p.Cfg.Observability.AdminSocket
}

// MetricsURL trả URL /metrics ("" nếu không bật).
func (p *Pipeline) MetricsURL() string {
	if p.Cfg == nil || p.Cfg.Observability.Metrics == "" {
		return ""
	}
	a := p.Cfg.Observability.Metrics
	if strings.HasPrefix(a, ":") {
		a = "127.0.0.1" + a
	}
	if strings.HasPrefix(a, "0.0.0.0:") {
		a = "127.0.0.1:" + strings.TrimPrefix(a, "0.0.0.0:")
	}
	return "http://" + a + "/metrics"
}

// BgPaths trả file log và pid của chế độ nền; cùng quy tắc với `strand sync --background`
// (cmd/strand/background.go: bgPaths): mặc định nằm trong <resources.spool.path>.
func (p *Pipeline) BgPaths() (logPath, pidPath string) {
	dir := "."
	if p.Cfg != nil && p.Cfg.Resources.Spool.Path != "" {
		dir = p.Cfg.Resources.Spool.Path
	}
	logPath, pidPath = p.LogFile, p.PidFile
	if logPath == "" {
		logPath = filepath.Join(dir, "strand.log")
	}
	if pidPath == "" {
		pidPath = filepath.Join(dir, "strand.pid")
	}
	return
}

// Pair trả "mysql → postgres" từ cấu hình.
func (p *Pipeline) Pair() string {
	if p.Cfg == nil {
		return "?"
	}
	return p.Cfg.Source.Kind + " → " + p.Cfg.Target.Kind
}

// ReadEnvFile đọc file KEY=VALUE (bỏ dòng trống/chú thích, nhận tiền tố `export `, bỏ cặp nháy bao ngoài).
// Dùng nạp môi trường cho tiến trình con ở chế độ nền (M3).
func ReadEnvFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" || strings.ContainsAny(k, " \t") {
			return nil, fmt.Errorf("%s:%d: expected KEY=VALUE", path, n)
		}
		v = strings.TrimSpace(v)
		out = append(out, k+"="+parseEnvValue(v))
	}
	return out, sc.Err()
}

// Env trả môi trường của file env_file (rỗng nếu không khai báo).
func (p *Pipeline) Env() ([]string, error) {
	if p.EnvFile == "" {
		return nil, nil
	}
	return ReadEnvFile(p.EnvFile)
}

// Registry giữ danh sách pipeline phát hiện từ web.yaml (khai báo + glob scan).
type Registry struct {
	cfg *Config
	cmd Commander

	mu     sync.RWMutex
	byName map[string]*Pipeline
	order  []string
	warns  []string

	createdMu sync.Mutex

	unitMu    sync.Mutex
	unitCache map[string]unitCacheEntry
	now       func() time.Time
}

type unitCacheEntry struct {
	exists bool
	at     time.Time
}

// NewRegistry: cmd = nil dùng exec thật.
func NewRegistry(cfg *Config, cmd Commander) *Registry {
	if cmd == nil {
		cmd = execCommander{}
	}
	return &Registry{cfg: cfg, cmd: cmd, byName: map[string]*Pipeline{}, unitCache: map[string]unitCacheEntry{}, now: time.Now}
}

// UnitExists: systemd có biết unit này (LoadState=loaded). Cache 30 giây.
func (r *Registry) UnitExists(ctx context.Context, unit string) bool {
	r.unitMu.Lock()
	if e, ok := r.unitCache[unit]; ok && r.now().Sub(e.at) < 30*time.Second {
		r.unitMu.Unlock()
		return e.exists
	}
	r.unitMu.Unlock()
	out, err := r.cmd.Run(ctx, "systemctl", "show", "-p", "LoadState", "--value", unit)
	exists := err == nil && strings.TrimSpace(string(out)) == "loaded"
	r.unitMu.Lock()
	r.unitCache[unit] = unitCacheEntry{exists: exists, at: r.now()}
	r.unitMu.Unlock()
	return exists
}

// Refresh quét lại web.yaml + glob scan. Pipeline khai báo tường minh thắng pipeline từ scan cùng tên.
// Tên không qua whitelist bị bỏ và ghi vào Warnings.
func (r *Registry) Refresh(ctx context.Context) {
	byName := map[string]*Pipeline{}
	var warns []string
	for _, e := range r.cfg.Pipelines {
		if !ValidName(e.Name) {
			warns = append(warns, fmt.Sprintf("pipeline %q skipped: invalid name", e.Name))
			continue
		}
		unit := e.SystemdUnit
		if unit != "" && !strings.HasSuffix(unit, ".service") {
			unit += ".service"
		}
		byName[e.Name] = &Pipeline{Name: e.Name, ConfigPath: e.Config, EnvFile: e.EnvFile, SystemdUnit: unit,
			LogFile: e.LogFile, PidFile: e.PidFile, Origin: "web.yaml"}
	}
	// Pipeline tạo từ web: <state_dir>/pipelines.json. Entry tường minh trong web.yaml thắng cùng tên.
	created, cerr := r.loadCreated()
	if cerr != nil {
		warns = append(warns, cerr.Error())
	}
	for _, e := range created {
		if !ValidName(e.Name) || e.Config == "" {
			warns = append(warns, fmt.Sprintf("%s: entry %q skipped: invalid name or empty config", CreatedFile, e.Name))
			continue
		}
		if prev, dup := byName[e.Name]; dup {
			if prev.Origin == "web.yaml" {
				warns = append(warns, fmt.Sprintf("pipeline %q is in both web.yaml and %s: using the web.yaml entry", e.Name, r.createdPath()))
			}
			continue
		}
		byName[e.Name] = &Pipeline{Name: e.Name, ConfigPath: e.Config, EnvFile: e.EnvFile, SystemdUnit: e.SystemdUnit,
			LogFile: e.LogFile, PidFile: e.PidFile, Origin: "created"}
	}
	for _, pat := range r.cfg.Scan {
		matches, err := filepath.Glob(pat)
		if err != nil {
			warns = append(warns, fmt.Sprintf("scan %q: %v", pat, err))
			continue
		}
		sort.Strings(matches)
		for _, m := range matches {
			base := filepath.Base(m)
			// web.yaml thường nằm cạnh các file pipeline (/etc/strand): không phải pipeline.
			if abs, _ := filepath.Abs(m); (r.cfg.SourcePath != "" && abs == r.cfg.SourcePath) || base == "web.yaml" || base == "web.yml" {
				continue
			}
			name := strings.TrimSuffix(strings.TrimSuffix(base, ".yaml"), ".yml")
			if !ValidName(name) {
				warns = append(warns, fmt.Sprintf("scan: %s skipped: file name is not a valid pipeline name", m))
				continue
			}
			if _, dup := byName[name]; dup {
				continue
			}
			p := &Pipeline{Name: name, ConfigPath: m, Origin: "scan"}
			if env := filepath.Join(filepath.Dir(m), name+".env"); fileExists(env) {
				p.EnvFile = env
			}
			if unit := "strand@" + name + ".service"; r.UnitExists(ctx, unit) {
				p.SystemdUnit = unit
			}
			byName[name] = p
		}
	}
	names := make([]string, 0, len(byName))
	for n, p := range byName {
		names = append(names, n)
		if c, err := config.LoadFileForPaths(p.ConfigPath); err != nil {
			p.CfgErr = err.Error()
		} else {
			p.Cfg = c
		}
	}
	sort.Strings(names)
	r.mu.Lock()
	r.byName, r.order, r.warns = byName, names, warns
	r.mu.Unlock()
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }

// List trả pipeline theo tên.
func (r *Registry) List() []*Pipeline {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Pipeline, 0, len(r.order))
	for _, n := range r.order {
		out = append(out, r.byName[n])
	}
	return out
}

// Get trả pipeline theo tên (nil nếu tên sai whitelist hoặc không đăng ký).
func (r *Registry) Get(name string) *Pipeline {
	if !ValidName(name) {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byName[name]
}

// Warnings: các mục bị bỏ khi quét.
func (r *Registry) Warnings() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]string(nil), r.warns...)
}

// Runtime là trạng thái tiến trình độc lập với admin socket.
type Runtime struct {
	Mode        string `json:"mode"`
	Running     bool   `json:"running"`
	ActiveState string `json:"active_state,omitempty"` // systemd: active | inactive | failed | activating ...
	SubState    string `json:"sub_state,omitempty"`
	ExitCode    *int   `json:"exit_code,omitempty"` // systemd: ExecMainStatus của lần chạy gần nhất
	PID         int    `json:"pid,omitempty"`
	Since       string `json:"since,omitempty"`
	Error       string `json:"error,omitempty"`
	// LastError: chế độ nền, lý do lần chạy gần nhất thất bại (suy ra từ đuôi log; đã che bí mật).
	LastError string `json:"last_error,omitempty"`
}

// Runtime hỏi systemctl show (chế độ systemd) hoặc kiểm file pid (chế độ nền).
func (r *Registry) Runtime(ctx context.Context, p *Pipeline) Runtime {
	if p.SystemdUnit == "" {
		return bgRuntime(p)
	}
	rt := Runtime{Mode: ModeSystemd}
	out, err := r.cmd.Run(ctx, "systemctl", "show", p.SystemdUnit,
		"-p", "ActiveState", "-p", "SubState", "-p", "ExecMainStatus", "-p", "ExecMainPID", "-p", "ActiveEnterTimestamp")
	if err != nil {
		rt.Error = strings.TrimSpace(string(out))
		if rt.Error == "" {
			rt.Error = err.Error()
		}
		return rt
	}
	kv := ParseKV(string(out))
	rt.ActiveState, rt.SubState = kv["ActiveState"], kv["SubState"]
	rt.Running = rt.ActiveState == "active" || rt.ActiveState == "activating" || rt.ActiveState == "reloading"
	rt.PID, _ = strconv.Atoi(kv["ExecMainPID"])
	if rt.Running {
		rt.Since = kv["ActiveEnterTimestamp"]
	}
	if v, ok := kv["ExecMainStatus"]; ok && !rt.Running {
		if n, err := strconv.Atoi(v); err == nil {
			rt.ExitCode = &n
		}
	}
	return rt
}

// ParseKV phân tích đầu ra KEY=VALUE theo dòng của `systemctl show`.
func ParseKV(s string) map[string]string {
	m := map[string]string{}
	for _, line := range strings.Split(s, "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			m[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return m
}

func bgRuntime(p *Pipeline) Runtime {
	rt := Runtime{Mode: ModeBackground}
	_, pidPath := p.BgPaths()
	pid, ok, err := readPid(pidPath)
	if err != nil {
		rt.Error = err.Error()
		return rt
	}
	if ok && strandSyncAlive(pid) {
		rt.Running, rt.PID, rt.ActiveState = true, pid, "active"
	} else {
		rt.ActiveState = "inactive"
		bgInferExit(p, &rt)
	}
	return rt
}

// bgInferExit: tiến trình nền đã dừng — đọc đuôi log; nếu lần chạy cuối kết thúc bằng lỗi (dòng `Error...:` của CLI
// hoặc ERROR "pipeline failed" sau mốc khởi động gần nhất) thì coi là failed, exit code suy từ mã [code]
// (không có mã: exit 1 như cmd/strand/main.go). Dừng êm (SIGTERM, `strand stop`) vẫn là stopped.
func bgInferExit(p *Pipeline, rt *Runtime) {
	logPath, _ := p.BgPaths()
	lines, err := TailFile(logPath, 2000)
	if err != nil {
		return
	}
	o := ParseLogOutcome(lines)
	if !o.Failed {
		return
	}
	code, ok := exitForErrorCode(o.Code)
	if !ok {
		code = exitcode.Unclassified
	}
	rt.ActiveState, rt.ExitCode, rt.LastError = "failed", &code, MaskSecrets(o.Err)
}

func readPid(path string) (int, bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return 0, false, fmt.Errorf("pid file %s is corrupt", path)
	}
	return pid, true, nil
}

// strandSyncAlive: giống cmd/strand/background.go — tiến trình còn sống VÀ là `strand ... sync`
// (PID có thể đã được cấp lại cho tiến trình khác).
func strandSyncAlive(pid int) bool {
	if err := syscall.Kill(pid, 0); err != nil && !errors.Is(err, syscall.EPERM) {
		return false
	}
	cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return false
	}
	args := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
	if len(args) == 0 || !strings.Contains(filepath.Base(args[0]), "strand") {
		return false
	}
	for _, a := range args[1:] {
		if a == "sync" {
			return true
		}
	}
	return false
}

// Logs trả tối đa n dòng log cuối: journalctl (systemd) hoặc file log (chế độ nền).
func (r *Registry) Logs(ctx context.Context, p *Pipeline, n int) ([]string, error) {
	if n <= 0 || n > 2000 {
		n = 200
	}
	if p.SystemdUnit != "" {
		out, err := r.cmd.Run(ctx, "journalctl", "-u", p.SystemdUnit, "-n", strconv.Itoa(n), "--no-pager", "-o", "short-iso")
		if err != nil {
			return nil, fmt.Errorf("journalctl: %v: %s", err, strings.TrimSpace(string(out)))
		}
		return splitLines(string(out)), nil
	}
	logPath, _ := p.BgPaths()
	return TailFile(logPath, n)
}

func splitLines(s string) []string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// TailFile đọc tối đa n dòng cuối của file (chỉ đọc ~512 KiB cuối, không nạp cả file log lớn).
func TailFile(path string, n int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	const window = 512 << 10
	off := st.Size() - window
	if off < 0 {
		off = 0
	}
	buf := make([]byte, st.Size()-off)
	if _, err := f.ReadAt(buf, off); err != nil && len(buf) > 0 {
		return nil, err
	}
	lines := splitLines(string(buf))
	if off > 0 && len(lines) > 0 {
		lines = lines[1:] // dòng đầu có thể bị cắt giữa chừng
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines, nil
}
