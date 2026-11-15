package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// FileJobRunner chạy `strand <lệnh>` thành tiến trình con và lưu lịch sử dưới <dir>:
//
//	<id>.json  JobStatus (ghi lại khi bắt đầu và khi kết thúc)
//	<id>.log   stdout+stderr của tiến trình con, ghi THẲNG vào file (không qua pipe của web)
//
// Hành vi khi web server chết hoặc khởi động lại:
//   - tiến trình con chạy trong process group riêng (Setpgid) và ghi output vào file nên không nhận SIGPIPE
//     và không bị giết theo web; nó tiếp tục chạy rồi init thu dọn (không có zombie của web, vì web đã chết).
//   - web khởi động lại đọc lại các job còn "running": nếu pid vẫn là tiến trình con đó thì nhận nuôi
//     (theo dõi bằng kill -0, không Wait được vì không còn là con), khi nó thoát thì đóng job nhưng mã thoát
//     không quan sát được (exit_code vắng). Nếu pid đã mất thì đánh dấu interrupted.
//   - một pipeline chỉ có một job chạy tại một thời điểm, kể cả job được nhận nuôi.
type FileJobRunner struct {
	dir string
	bin string
	now func() time.Time
	// OnEnd được gọi (không giữ khóa) khi job kết thúc: Server ghi audit "job.end".
	OnEnd func(JobStatus)
	// MaxHistory: số job đã xong giữ lại cho mỗi pipeline (mặc định 100).
	MaxHistory int
	// KillGrace: chờ SIGTERM trước khi SIGKILL khi hủy (mặc định 10 s).
	KillGrace time.Duration

	mu   sync.Mutex
	jobs map[string]*JobStatus
}

// ErrJobRunning: pipeline đã có job đang chạy.
type ErrJobRunning struct{ Pipeline, ID string }

func (e *ErrJobRunning) Error() string {
	return fmt.Sprintf("pipeline %s already has a running job (%s); wait for it to finish or cancel it", e.Pipeline, e.ID)
}

// JobIDRE là whitelist id job (dùng làm tên file và trong URL).
var JobIDRE = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{8}$`)

// NewFileJobRunner mở thư mục lịch sử (tạo nếu chưa có) và nạp lại job cũ.
func NewFileJobRunner(dir, bin string, now func() time.Time) (*FileJobRunner, error) {
	if now == nil {
		now = time.Now
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create jobs dir: %w", err)
	}
	r := &FileJobRunner{dir: dir, bin: bin, now: now, MaxHistory: 100, KillGrace: 10 * time.Second, jobs: map[string]*JobStatus{}}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var adopt []string
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var js JobStatus
		if json.Unmarshal(b, &js) != nil || !JobIDRE.MatchString(js.ID) || e.Name() != js.ID+".json" {
			continue
		}
		if js.Running {
			if js.PID > 0 && procIsBinary(js.PID, bin) {
				adopt = append(adopt, js.ID)
			} else {
				t := now()
				js.Running, js.Ended, js.Interrupted = false, &t, true
				js.Error = "web server restarted while the job was running and the process is gone; exit code not observed"
				r.persistLocked(&js)
			}
		}
		cp := js
		r.jobs[js.ID] = &cp
	}
	for _, id := range adopt {
		go r.adopt(id)
	}
	return r, nil
}

func (r *FileJobRunner) jsonPath(id string) string { return filepath.Join(r.dir, id+".json") }
func (r *FileJobRunner) logPath(id string) string  { return filepath.Join(r.dir, id+".log") }

// persistLocked ghi JobStatus nguyên tử (tmp + rename).
func (r *FileJobRunner) persistLocked(js *JobStatus) {
	b, err := json.MarshalIndent(js, "", "  ")
	if err != nil {
		return
	}
	tmp := r.jsonPath(js.ID) + ".tmp"
	if os.WriteFile(tmp, b, 0o600) == nil {
		_ = os.Rename(tmp, r.jsonPath(js.ID))
	}
}

func (r *FileJobRunner) runningLocked(pipeline string) *JobStatus {
	for _, j := range r.jobs {
		if j.Running && j.Pipeline == pipeline {
			return j
		}
	}
	return nil
}

func newJobID(t time.Time) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return t.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b[:])
}

// Start chạy `<bin> <args...>`; argv là mảng, không qua shell. Môi trường = môi trường của web + spec.Env.
func (r *FileJobRunner) Start(_ context.Context, spec JobSpec) (string, error) {
	if !ValidName(spec.Pipeline) || !ValidName(spec.Kind) {
		return "", errors.New("invalid job spec")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if j := r.runningLocked(spec.Pipeline); j != nil {
		return "", &ErrJobRunning{Pipeline: spec.Pipeline, ID: j.ID}
	}
	now := r.now()
	id := newJobID(now)
	logf, err := os.OpenFile(r.logPath(id), os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("create job log: %w", err)
	}
	defer logf.Close()
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		return "", err
	}
	defer devnull.Close()
	cmd := exec.Command(r.bin, spec.Args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = devnull, logf, logf
	cmd.Env = append(os.Environ(), spec.Env...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = os.Remove(r.logPath(id))
		return "", fmt.Errorf("start %s: %w", r.bin, err)
	}
	shown := spec.Args
	if spec.DisplayArgs != nil {
		shown = spec.DisplayArgs
	}
	js := &JobStatus{ID: id, Pipeline: spec.Pipeline, Kind: spec.Kind, Args: append([]string(nil), shown...),
		Started: now, Running: true, PID: cmd.Process.Pid}
	r.jobs[id] = js
	r.persistLocked(js)
	r.pruneLocked(spec.Pipeline)
	go r.wait(id, cmd)
	return id, nil
}

// wait thu hoạch tiến trình con (không để zombie) rồi ghi kết quả.
func (r *FileJobRunner) wait(id string, cmd *exec.Cmd) {
	err := cmd.Wait()
	code := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			code = ee.ExitCode() // -1 nếu chết vì tín hiệu
		} else {
			code = -1
		}
	}
	r.finish(id, &code, "")
}

// adopt theo dõi job còn chạy từ lần web trước (không phải tiến trình con của web này).
func (r *FileJobRunner) adopt(id string) {
	for {
		r.mu.Lock()
		js := r.jobs[id]
		var pid int
		if js != nil {
			pid = js.PID
		}
		r.mu.Unlock()
		if js == nil || !procIsBinary(pid, r.bin) {
			break
		}
		time.Sleep(time.Second)
	}
	r.finish(id, nil, "web server restarted while the job was running; exit code not observed")
}

func (r *FileJobRunner) finish(id string, code *int, note string) {
	r.mu.Lock()
	js := r.jobs[id]
	if js == nil || !js.Running {
		r.mu.Unlock()
		return
	}
	t := r.now()
	js.Running, js.Ended, js.ExitCode, js.Error = false, &t, code, note
	r.persistLocked(js)
	cp := *js
	cb := r.OnEnd
	r.mu.Unlock()
	if cb != nil {
		cb(cp)
	}
}

func (r *FileJobRunner) pruneLocked(pipeline string) {
	var done []*JobStatus
	for _, j := range r.jobs {
		if j.Pipeline == pipeline && !j.Running {
			done = append(done, j)
		}
	}
	if len(done) <= r.MaxHistory {
		return
	}
	sort.Slice(done, func(i, k int) bool { return done[i].Started.After(done[k].Started) })
	for _, j := range done[r.MaxHistory:] {
		_ = os.Remove(r.jsonPath(j.ID))
		_ = os.Remove(r.logPath(j.ID))
		delete(r.jobs, j.ID)
	}
}

// Get trả bản sao trạng thái job.
func (r *FileJobRunner) Get(id string) (JobStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	js, ok := r.jobs[id]
	if !ok {
		return JobStatus{}, false
	}
	return *js, true
}

// List: mới nhất trước.
func (r *FileJobRunner) List(pipeline string) []JobStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []JobStatus
	for _, j := range r.jobs {
		if pipeline == "" || j.Pipeline == pipeline {
			out = append(out, *j)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Started.After(out[k].Started) })
	return out
}

// Cancel gửi SIGTERM tới process group của job, SIGKILL sau KillGrace nếu còn sống.
func (r *FileJobRunner) Cancel(id string) error {
	r.mu.Lock()
	js, ok := r.jobs[id]
	if !ok {
		r.mu.Unlock()
		return errors.New("unknown job")
	}
	if !js.Running {
		r.mu.Unlock()
		return errors.New("job is not running")
	}
	js.Cancelled = true
	r.persistLocked(js)
	pid := js.PID
	r.mu.Unlock()
	if pid <= 0 {
		return errors.New("job has no pid")
	}
	if err := syscall.Kill(-pid, syscall.SIGTERM); err != nil {
		if err2 := syscall.Kill(pid, syscall.SIGTERM); err2 != nil {
			return err
		}
	}
	grace := r.KillGrace
	go func() {
		time.Sleep(grace)
		if j, ok := r.Get(id); ok && j.Running {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
	}()
	return nil
}

const maxOutputChunk = 256 << 10

// Output trả phần output từ offset. Khi job còn chạy chỉ trả đến dòng đầy đủ cuối cùng (không cắt giữa dòng,
// không cắt giữa ký tự UTF-8). Mật khẩu/DSN trong output bị che.
func (r *FileJobRunner) Output(id string, offset int64) (JobOutput, bool) {
	js, ok := r.Get(id)
	if !ok {
		return JobOutput{}, false
	}
	out := JobOutput{Offset: offset, Running: js.Running, ExitCode: js.ExitCode}
	f, err := os.Open(r.logPath(id))
	if err != nil {
		return out, true
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return out, true
	}
	if offset < 0 || offset > st.Size() {
		offset = st.Size()
		out.Offset = offset
	}
	n := st.Size() - offset
	if n > maxOutputChunk {
		n = maxOutputChunk
	}
	if n <= 0 {
		return out, true
	}
	buf := make([]byte, n)
	m, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return out, true
	}
	buf = buf[:m]
	// Trạng thái "đang chạy" lấy TRƯỚC khi đọc file: nếu job kết thúc giữa chừng, lần hỏi kế tiếp mới báo xong
	// và trả nốt phần đuôi, nên client không bao giờ dừng sớm mà bỏ sót output.
	if js.Running {
		cut := strings.LastIndexByte(string(buf), '\n')
		if cut < 0 {
			if len(buf) < maxOutputChunk {
				return out, true // chưa có dòng đầy đủ
			}
			cut = len(buf) - 1
		}
		buf = buf[:cut+1]
	}
	out.Offset = offset + int64(len(buf))
	out.Data = MaskSecrets(strings.ToValidUTF8(string(buf), "�"))
	return out, true
}

// procIsBinary: pid còn sống và đúng là tiến trình chạy bin (argv[0], hoặc argv[1] khi bin là script #!).
func procIsBinary(pid int, bin string) bool {
	if pid <= 0 {
		return false
	}
	if err := syscall.Kill(pid, 0); err != nil && !errors.Is(err, syscall.EPERM) {
		return false
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return false
	}
	args := strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
	for i := 0; i < len(args) && i < 2; i++ {
		if args[i] == bin {
			return true
		}
	}
	// zombie: /proc/<pid>/cmdline rỗng => coi như đã thoát.
	return false
}
