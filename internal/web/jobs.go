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

	"strand/internal/web/store"
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
	st         store.Store // nil: lịch sử trong <dir>/*.json
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
	return NewJobRunner(dir, bin, now, nil)
}

// NewJobRunner như NewFileJobRunner; st != nil thì lịch sử job và output nằm trong PostgreSQL (<id>.log chỉ là
// spool khi job chạy: được chuyển dần vào bảng job_output rồi xóa khi job xong).
func NewJobRunner(dir, bin string, now func() time.Time, st store.Store) (*FileJobRunner, error) {
	if now == nil {
		now = time.Now
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create jobs dir: %w", err)
	}
	r := &FileJobRunner{dir: dir, bin: bin, now: now, MaxHistory: 100, KillGrace: 10 * time.Second, jobs: map[string]*JobStatus{}, st: st}
	loaded, err := r.loadHistory()
	if err != nil {
		return nil, err
	}
	var adopt []string
	for _, js := range loaded {
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
	if st != nil {
		// Spool còn sót (job đang được nhận nuôi, job bị ngắt khi web dừng, hoặc output của bản web cũ chỉ ghi
		// file): chuyển nốt vào DB, tuần tự, nền.
		var pending []string
		for id, js := range r.jobs {
			switch {
			case !fileExists(r.logPath(id)):
			case js.Running:
				go r.shipLoop(id, true) // nhận nuôi: chạy tới khi job xong
			default:
				pending = append(pending, id)
			}
		}
		sort.Strings(pending)
		go func() {
			for _, id := range pending {
				// Job đã xong (ví dụ log của chế độ file trước khi chuyển sang DB): chép vào DB nhưng giữ
				// file, để còn quay lại chế độ file được.
				r.shipLoop(id, false)
			}
		}()
	}
	return r, nil
}

// loadHistory đọc lịch sử job từ DB (nếu có) hoặc thư mục <dir>/*.json.
func (r *FileJobRunner) loadHistory() ([]JobStatus, error) {
	var out []JobStatus
	if r.st != nil {
		ctx, cancel := dbCtx()
		defer cancel()
		rows, err := r.st.ListJobs(ctx)
		if err != nil {
			return nil, fmt.Errorf("load job history from database: %w", err)
		}
		for _, row := range rows {
			var js JobStatus
			if json.Unmarshal(row.Data, &js) == nil && JobIDRE.MatchString(js.ID) {
				out = append(out, js)
			}
		}
		return out, nil
	}
	ents, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(r.dir, e.Name()))
		if err != nil {
			continue
		}
		var js JobStatus
		if json.Unmarshal(b, &js) != nil || !JobIDRE.MatchString(js.ID) || e.Name() != js.ID+".json" {
			continue
		}
		out = append(out, js)
	}
	return out, nil
}

func (r *FileJobRunner) jsonPath(id string) string { return filepath.Join(r.dir, id+".json") }
func (r *FileJobRunner) logPath(id string) string  { return filepath.Join(r.dir, id+".log") }

// persistLocked ghi JobStatus nguyên tử (tmp + rename).
func (r *FileJobRunner) persistLocked(js *JobStatus) {
	b, err := json.MarshalIndent(js, "", "  ")
	if err != nil {
		return
	}
	if r.st != nil {
		ctx, cancel := dbCtx()
		defer cancel()
		_ = r.st.UpsertJob(ctx, store.Job{ID: js.ID, Pipeline: js.Pipeline, Started: js.Started, Running: js.Running, Data: b})
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
	if r.st != nil {
		go r.shipLoop(id, true)
	}
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
		if r.st != nil {
			ctx, cancel := dbCtx()
			_ = r.st.DeleteJob(ctx, j.ID)
			cancel()
		}
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
	var buf []byte
	f, err := os.Open(r.logPath(id))
	switch {
	case err == nil:
		// File spool còn (chế độ file, hoặc chế độ DB khi job đang chạy / chưa chuyển hết vào DB).
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
		buf = make([]byte, n)
		m, err := f.ReadAt(buf, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return out, true
		}
		buf = buf[:m]
	case r.st != nil:
		ctx, cancel := dbCtx()
		defer cancel()
		size, err := r.st.JobOutputSize(ctx, id)
		if err != nil {
			return out, true
		}
		if offset < 0 || offset > size {
			offset = size
			out.Offset = offset
		}
		if buf, err = r.st.ReadJobOutput(ctx, id, offset, maxOutputChunk); err != nil || len(buf) == 0 {
			return out, true
		}
	default:
		return out, true
	}
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

// ---- chế độ DB: chuyển output từ file spool vào bảng job_output ----

// MaxJobOutputDB: phần đầu output của một job được giữ trong DB; phần sau bị bỏ, kèm một dòng ghi chú.
const MaxJobOutputDB = 10 << 20

// shipEvery: chu kỳ chuyển output (biến để test rút ngắn).
var shipEvery = time.Second

// shipLoop chuyển <id>.log vào DB theo chu kỳ khi job chạy, lần cuối khi job xong, rồi xóa file spool. DB lỗi
// thì thử lại (file vẫn giữ nên Output đọc file, không mất gì); bỏ cuộc sau 10 phút kể từ khi job xong và để
// lại file.
func (r *FileJobRunner) shipLoop(id string, removeSpool bool) {
	var shipped int64 = -1
	var endedAt time.Time
	for {
		js, ok := r.Get(id)
		if !ok {
			return // job đã bị dọn (prune)
		}
		running := js.Running
		n, complete, err := r.shipOnce(id, shipped, !running)
		if err == nil {
			shipped = n
		}
		if !running {
			if err == nil && complete {
				if removeSpool {
					_ = os.Remove(r.logPath(id))
				}
				return
			}
			if endedAt.IsZero() {
				endedAt = time.Now()
			} else if time.Since(endedAt) > 10*time.Minute {
				return
			}
		}
		time.Sleep(shipEvery)
	}
}

// shipOnce gửi phần file chưa có trong DB (tối đa MaxJobOutputDB). final: job đã xong, thêm ghi chú nếu bị cắt.
// Trả số byte đã có trong DB và complete = toàn bộ (hoặc phần giữ lại + ghi chú) đã nằm trong DB.
func (r *FileJobRunner) shipOnce(id string, shipped int64, final bool) (int64, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if shipped < 0 {
		n, err := r.st.JobOutputSize(ctx, id)
		if err != nil {
			return shipped, false, err
		}
		shipped = n
	}
	f, err := os.Open(r.logPath(id))
	if err != nil {
		return shipped, errors.Is(err, os.ErrNotExist), nil
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return shipped, false, err
	}
	size := fi.Size()
	limit := size
	if limit > MaxJobOutputDB {
		limit = MaxJobOutputDB
	}
	buf := make([]byte, maxOutputChunk)
	for shipped < limit {
		want := limit - shipped
		if want > int64(len(buf)) {
			want = int64(len(buf))
		}
		m, err := f.ReadAt(buf[:want], shipped)
		if m == 0 {
			if err != nil && !errors.Is(err, io.EOF) {
				return shipped, false, err
			}
			break
		}
		if err := r.st.AppendJobOutput(ctx, id, shipped, buf[:m]); err != nil {
			return shipped, false, err
		}
		shipped += int64(m)
	}
	if !final {
		return shipped, false, nil
	}
	if size > MaxJobOutputDB && shipped == MaxJobOutputDB {
		note := fmt.Sprintf("\n[strand web: output truncated, %d bytes in total; only the first %d MB are kept in the database]\n", size, MaxJobOutputDB>>20)
		if err := r.st.AppendJobOutput(ctx, id, shipped, []byte(note)); err != nil {
			return shipped, false, err
		}
		shipped += int64(len(note))
	}
	return shipped, shipped >= limit, nil
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
