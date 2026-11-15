package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Điểm mở rộng (docs/PLAN-WEB-UI.md §3, §6). M3 đăng ký các action ở actions_ops.go.
//
// Mỗi thao tác ghi (start/stop/restart, pause/resume, throttle,
// snapshot add, rewind, verify, repair, promote, sửa cấu hình...) là một Action đăng ký ở đây. Đường
// POST /api/v1/pipelines/{name}/actions/{action} đã đi qua: xác thực, CSRF, whitelist tên pipeline,
// kiểm tên đăng ký, và dòng audit — action không phải tự làm lại các bước đó.

// ActionRequest là đầu vào đã được kiểm của một thao tác.
type ActionRequest struct {
	Pipeline *Pipeline
	Args     map[string]string // tham số từ JSON body (chuỗi), action tự kiểm theo whitelist
	IP       string
}

// ActionResult là kết quả trả về UI.
type ActionResult struct {
	Message  string         `json:"message,omitempty"`
	JobID    string         `json:"job_id,omitempty"` // thao tác dài: id để theo dõi qua JobRunner
	ExitCode *int           `json:"exit_code,omitempty"`
	Data     map[string]any `json:"data,omitempty"`
}

// Action là một thao tác có tên.
type Action struct {
	Name string
	// ConfirmName: UI bắt gõ lại tên pipeline (Rewind, Repair, Promote).
	ConfirmName bool
	// Run thực hiện thao tác. Lỗi trả về được audit với result=error. Đối số tiến trình con phải là mảng.
	Run func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error)
}

// ActionRegistry giữ các thao tác đã đăng ký.
type ActionRegistry struct {
	mu sync.RWMutex
	m  map[string]Action
}

func NewActionRegistry() *ActionRegistry { return &ActionRegistry{m: map[string]Action{}} }

// Register đăng ký action; tên phải qua whitelist (dùng trong URL và audit).
func (r *ActionRegistry) Register(a Action) {
	if !ValidName(a.Name) || a.Run == nil {
		panic("web: invalid action " + a.Name)
	}
	r.mu.Lock()
	r.m[a.Name] = a
	r.mu.Unlock()
}

// Get tìm action theo tên.
func (r *ActionRegistry) Get(name string) (Action, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.m[name]
	return a, ok
}

// Names liệt kê tên action đã đăng ký.
func (r *ActionRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.m))
	for n := range r.m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// JobSpec mô tả một tiến trình con dài hạn (verify, repair, promote, validate...).
type JobSpec struct {
	Pipeline string
	Kind     string   // tên thao tác, ví dụ "verify"
	Args     []string // argv của `strand`, không qua shell
	Env      []string // KEY=VALUE bổ sung (từ env_file)
}

// JobStatus là trạng thái một job (cũng là nội dung file <state_dir>/jobs/<id>.json).
type JobStatus struct {
	ID       string     `json:"id"`
	Pipeline string     `json:"pipeline"`
	Kind     string     `json:"kind"`
	Args     []string   `json:"args"`
	Started  time.Time  `json:"started"`
	Ended    *time.Time `json:"ended,omitempty"`
	Running  bool       `json:"running"`
	ExitCode *int       `json:"exit_code,omitempty"` // nil: chưa xong, hoặc web khởi động lại giữa chừng nên không quan sát được
	PID      int        `json:"pid,omitempty"`
	// Cancelled: người dùng bấm hủy. Interrupted: web server khởi động lại khi job đang chạy và tiến trình
	// con đã biến mất, mã thoát không quan sát được.
	Cancelled   bool   `json:"cancelled,omitempty"`
	Interrupted bool   `json:"interrupted,omitempty"`
	Error       string `json:"error,omitempty"`
}

// JobOutput là một đoạn output của job tính từ offset (byte) đã cho.
type JobOutput struct {
	Data     string `json:"data"`
	Offset   int64  `json:"offset"` // offset kế tiếp để hỏi
	Running  bool   `json:"running"`
	ExitCode *int   `json:"exit_code,omitempty"`
}

// JobRunner chạy `strand <lệnh>` thành tiến trình con và giữ lịch sử (bản cài đặt: FileJobRunner).
type JobRunner interface {
	// Start chạy job; từ chối (ErrJobRunning) nếu pipeline đã có job đang chạy. ctx chỉ dùng để kiểm lúc
	// khởi động: tiến trình con KHÔNG gắn với ctx của request.
	Start(ctx context.Context, spec JobSpec) (id string, err error)
	Get(id string) (JobStatus, bool)
	// List: mới nhất trước; pipeline rỗng = mọi pipeline.
	List(pipeline string) []JobStatus
	Cancel(id string) error
	Output(id string, offset int64) (JobOutput, bool)
}

// ActionError cho action chọn mã HTTP: 400 tham số sai, 409 xung đột trạng thái...
type ActionError struct {
	Status int
	Msg    string
}

func (e *ActionError) Error() string { return e.Msg }

// BadRequest, Conflict: tiện ích tạo ActionError.
func BadRequest(format string, a ...any) error {
	return &ActionError{Status: http.StatusBadRequest, Msg: fmt.Sprintf(format, a...)}
}
func Conflict(format string, a ...any) error {
	return &ActionError{Status: http.StatusConflict, Msg: fmt.Sprintf(format, a...)}
}

// actionTimeout: giới hạn tổng cho một action (systemctl stop / strand stop có thể chờ ghi checkpoint tới 150 s).
const actionTimeout = 170 * time.Second

// apiAction: POST /api/v1/pipelines/{name}/actions/{action}. Xác thực và CSRF đã xong ở middleware.
func (s *Server) apiAction(w http.ResponseWriter, r *http.Request, _ *Session) {
	name, act := r.PathValue("name"), r.PathValue("action")
	p := s.reg.Get(name)
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return
	}
	a, ok := s.actions.Get(act)
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown action")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	args := map[string]string{}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			writeErr(w, http.StatusBadRequest, "expected a JSON object of string values")
			return
		}
	}
	if a.ConfirmName && args["confirm_name"] != name {
		writeErr(w, http.StatusBadRequest, "confirm_name must repeat the pipeline name")
		return
	}
	argv := make([]string, 0, len(args))
	for _, k := range sortedKeys(args) {
		argv = append(argv, k+"="+args[k])
	}
	ip := s.clientIP(r)
	// Ghi audit TRƯỚC khi chạy: nếu không ghi được thì không chạy thao tác.
	if err := s.audit.Log(AuditEntry{IP: ip, Action: act, Pipeline: name, Args: argv, Result: "started"}); err != nil {
		writeErr(w, http.StatusInternalServerError, "audit log unavailable; refusing to run")
		return
	}
	// Thao tác chạy tách khỏi ctx của request: client ngắt kết nối giữa chừng không được làm hỏng
	// systemctl stop / strand stop đang dở; vẫn có hạn chót, và máy chủ nới deadline ghi cho request này.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(actionTimeout + 10*time.Second))
	rctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), actionTimeout)
	defer cancel()
	res, err := a.Run(rctx, s, ActionRequest{Pipeline: p, Args: args, IP: ip})
	entry := AuditEntry{IP: ip, Action: act, Pipeline: name, Args: argv, Result: "ok", ExitCode: res.ExitCode}
	if err != nil {
		entry.Result = "error"
		entry.Detail = map[string]any{"error": err.Error()}
	}
	_ = s.audit.Log(entry)
	if err != nil {
		status := http.StatusBadGateway
		var ae *ActionError
		if errors.As(err, &ae) {
			status = ae.Status
		}
		writeErr(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}
