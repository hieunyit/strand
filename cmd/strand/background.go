package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/config"
	"strand/internal/exitcode"
)

// Chạy nền (strand sync --background) và điều khiển tiến trình nền (strand stop / strand status).
// Tiến trình con là chính `strand sync` với cùng tham số (trừ --background), tách khỏi terminal (setsid),
// stdout/stderr ghi vào file log, PID ghi vào file pid. Con xóa file pid khi thoát.

const bgChildEnv = "STRAND_BACKGROUND_PIDFILE" // có giá trị = đang là tiến trình con chạy nền

var (
	syncBackground bool
	syncLogFile    string
	syncPidFile    string
	stopTimeout    time.Duration
)

// bgPaths trả đường dẫn file log và pid: cờ nếu có, ngược lại nằm trong thư mục spool của pipeline.
func bgPaths(cfg *config.Config, logFlag, pidFlag string) (logPath, pidPath string) {
	dir := cfg.Resources.Spool.Path
	if dir == "" {
		dir = "."
	}
	logPath, pidPath = logFlag, pidFlag
	if logPath == "" {
		logPath = filepath.Join(dir, "strand.log")
	}
	if pidPath == "" {
		pidPath = filepath.Join(dir, "strand.pid")
	}
	return logPath, pidPath
}

// readPid đọc PID trong file; ok=false nếu không có file.
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
		return 0, false, fmt.Errorf("pid file %s is corrupt: %q", path, strings.TrimSpace(string(b)))
	}
	return pid, true, nil
}

// strandSyncAlive: tiến trình pid còn sống VÀ đúng là `strand ... sync` (PID có thể đã bị hệ điều hành
// cấp lại cho tiến trình khác sau khi strand chết: không được gửi tín hiệu nhầm).
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

// childArgs: tham số cho tiến trình con = tham số hiện tại bỏ --background / -d.
func childArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, a := range args {
		switch {
		case a == "--background", a == "-d", strings.HasPrefix(a, "--background="):
			continue
		}
		out = append(out, a)
	}
	return out
}

// startBackground khởi tiến trình con chạy nền và chờ ngắn để bắt lỗi khởi động sớm (cấu hình, kết nối).
func startBackground(cmd *cobra.Command, cfg *config.Config) error {
	logPath, pidPath := bgPaths(cfg, syncLogFile, syncPidFile)
	if pid, ok, err := readPid(pidPath); err != nil {
		return err
	} else if ok && strandSyncAlive(pid) {
		return exitcode.New("already_running", exitcode.InvalidSyntax,
			fmt.Sprintf("pipeline %s is already running in the background (pid %d, pid file %s); stop it with: strand stop -c %s", cfg.Name, pid, pidPath, cfgPath), nil)
	}
	for _, p := range []string{logPath, pidPath} {
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			return err
		}
	}
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	defer logf.Close()
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		return err
	}
	defer devnull.Close()
	self, err := os.Executable()
	if err != nil {
		return err
	}
	absPid, err := filepath.Abs(pidPath)
	if err != nil {
		return err
	}
	start, _ := logf.Seek(0, 2) // vị trí cuối log trước khi con ghi (để in phần mới khi lỗi)
	child := exec.Command(self, childArgs(os.Args[1:])...)
	child.Stdin, child.Stdout, child.Stderr = devnull, logf, logf
	child.Env = append(os.Environ(), bgChildEnv+"="+absPid)
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // tách khỏi terminal: đóng SSH không giết strand
	if err := child.Start(); err != nil {
		return fmt.Errorf("start background strand: %w", err)
	}
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(child.Process.Pid)+"\n"), 0o640); err != nil {
		_ = child.Process.Signal(syscall.SIGTERM)
		return fmt.Errorf("write pid file: %w", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- child.Wait() }()
	select {
	case werr := <-exited:
		_ = os.Remove(pidPath)
		code := 1
		var ee *exec.ExitError
		if errors.As(werr, &ee) {
			code = ee.ExitCode()
		}
		tail := logTail(logPath, start, 20)
		return exitcode.New("background_start_failed", code,
			fmt.Sprintf("background strand exited during startup (exit %d); log %s:\n%s", code, logPath, tail), werr)
	case <-time.After(5 * time.Second):
	}
	fmt.Fprintf(cmd.OutOrStdout(), "strand sync started in the background: pipeline %s, pid %d\n  log:    %s\n  pid:    %s\n  status: strand status -c %s   (lag/progress: strand top -c %s)\n  stop:   strand stop -c %s\n",
		cfg.Name, child.Process.Pid, logPath, pidPath, cfgPath, cfgPath, cfgPath)
	return nil
}

// logTail trả tối đa n dòng cuối của phần log ghi từ vị trí from.
func logTail(path string, from int64, n int) string {
	b, err := os.ReadFile(path)
	if err != nil || from > int64(len(b)) {
		return ""
	}
	lines := strings.Split(strings.TrimRight(string(b[from:]), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// childPidFileCleanup: tiến trình con xóa file pid khi thoát (chỉ khi file vẫn trỏ tới chính nó).
func childPidFileCleanup() func() {
	path := os.Getenv(bgChildEnv)
	if path == "" {
		return func() {}
	}
	return func() {
		if pid, ok, _ := readPid(path); ok && pid == os.Getpid() {
			_ = os.Remove(path)
		}
	}
}

func loadBgConfig() (*config.Config, error) {
	if cfgPath == "" {
		return nil, exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing config file path (use -c or --config)", nil)
	}
	// Chỉ cần đường dẫn spool: không đòi mật khẩu trong môi trường như sync.
	return config.LoadFileForPaths(cfgPath)
}

func newStopCmd() *cobra.Command {
	var pidFlag string
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Dừng an toàn strand sync đang chạy nền (SIGTERM, chờ ghi checkpoint)",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadBgConfig()
			if err != nil {
				return err
			}
			_, pidPath := bgPaths(cfg, "", pidFlag)
			pid, ok, err := readPid(pidPath)
			if err != nil {
				return err
			}
			if !ok || !strandSyncAlive(pid) {
				if ok {
					_ = os.Remove(pidPath) // file pid cũ của tiến trình đã chết
				}
				fmt.Fprintf(cmd.OutOrStdout(), "pipeline %s is not running in the background (pid file %s)\n", cfg.Name, pidPath)
				return nil
			}
			if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
				return fmt.Errorf("signal pid %d: %w", pid, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "stopping pipeline %s (pid %d)...\n", cfg.Name, pid)
			deadline := time.Now().Add(stopTimeout)
			for time.Now().Before(deadline) {
				if !strandSyncAlive(pid) {
					_ = os.Remove(pidPath)
					fmt.Fprintf(cmd.OutOrStdout(), "stopped\n")
					return nil
				}
				time.Sleep(200 * time.Millisecond)
			}
			return exitcode.New("stop_timeout", 1, fmt.Sprintf("pid %d still running after %s (it may be flushing a large batch); check the log, run strand stop again, or kill -9 as a last resort (safe: it resumes from the last checkpoint)", pid, stopTimeout), nil)
		},
	}
	cmd.Flags().StringVar(&pidFlag, "pid-file", "", "File pid (mặc định <resources.spool.path>/strand.pid)")
	cmd.Flags().DurationVar(&stopTimeout, "timeout", 150*time.Second, "Thời gian chờ dừng tối đa")
	return cmd
}

func newStatusCmd() *cobra.Command {
	var pidFlag, logFlag string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Cho biết strand sync có đang chạy nền không, kèm vài dòng log cuối",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadBgConfig()
			if err != nil {
				return err
			}
			logPath, pidPath := bgPaths(cfg, logFlag, pidFlag)
			pid, ok, err := readPid(pidPath)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if ok && strandSyncAlive(pid) {
				fmt.Fprintf(out, "pipeline %s: running in the background (pid %d)\n", cfg.Name, pid)
			} else {
				fmt.Fprintf(out, "pipeline %s: not running in the background\n", cfg.Name)
			}
			fmt.Fprintf(out, "log: %s\n", logPath)
			if b, err := os.ReadFile(logPath); err == nil {
				lines := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
				if len(lines) > 5 {
					lines = lines[len(lines)-5:]
				}
				fmt.Fprintf(out, "last log lines:\n%s\n", bytes.Join(lines, []byte("\n")))
			}
			if !(ok && strandSyncAlive(pid)) {
				return exitcode.New("not_running", 1, "not running", nil)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&pidFlag, "pid-file", "", "File pid (mặc định <resources.spool.path>/strand.pid)")
	cmd.Flags().StringVar(&logFlag, "log-file", "", "File log (mặc định <resources.spool.path>/strand.log)")
	return cmd
}
