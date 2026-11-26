package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"strand/internal/findings"
)

// Doctor trên web: POST /api/v1/pipelines/{name}/doctor chạy `strand doctor -c <cfg> --json` (preflight chỉ
// đọc, liệt kê MỌI vấn đề cùng lúc) và trả danh sách vấn đề. Quyền admin: không có trong writeRules nên mặc
// định fail-closed là admin, giống Validate và Plan (kết quả nêu tên bảng, constraint, quyền tài khoản DB).

// doctorTimeout: dưới WriteTimeout (60s) của server để client luôn nhận được câu trả lời. doctor tự giới hạn
// mỗi lần kết nối (doctor.PingTimeout = 15s mỗi phía).
var doctorTimeout = 50 * time.Second

// doctorOutputLimit: giới hạn stdout (JSON) đọc từ tiến trình doctor.
const doctorOutputLimit = 4 << 20

// DoctorResult là câu trả lời của API doctor.
type DoctorResult struct {
	Pipeline string `json:"pipeline"`
	// OK: không có vấn đề mức error (sync sẽ không bị chặn bởi preflight).
	OK       bool               `json:"ok"`
	Summary  findings.Summary   `json:"summary"`
	Findings []findings.Finding `json:"findings"`
	ExitCode int                `json:"exit_code"`
	// DurationMS: thời gian chạy doctor.
	DurationMS int64 `json:"duration_ms"`
	// TimedOut: doctor bị dừng vì vượt doctorTimeout (nguồn/đích không trả lời); Findings có một vấn đề
	// doctor_timeout.
	TimedOut bool `json:"timed_out,omitempty"`
}

// errDoctorOutput: doctor không in ra JSON đọc được (binary cũ không có lệnh doctor, crash...).
var errDoctorOutput = errors.New("doctor did not return a report")

// runDoctor chạy `strand doctor --json` cho pipeline p. stdout (JSON) tách khỏi stderr (log) để log không
// làm hỏng JSON. Mọi chuỗi trong kết quả qua MaskSecrets (doctor đã che mật khẩu DSN; đây là lớp thứ hai).
func (s *Server) runDoctor(ctx context.Context, p *Pipeline) (*DoctorResult, error) {
	if p == nil || p.ConfigPath == "" {
		return nil, &ActionError{Status: http.StatusBadRequest, Msg: "pipeline has no config file"}
	}
	s.materialize(p.Name) // chế độ DB: file cấu hình/.env được dựng lại từ DB nếu thiếu hoặc khác
	env, err := p.Env()
	if err != nil {
		return nil, &ActionError{Status: http.StatusInternalServerError, Msg: "env_file: " + err.Error()}
	}
	dctx, cancel := context.WithTimeout(ctx, doctorTimeout)
	defer cancel()
	start := time.Now()
	cmd := exec.CommandContext(dctx, s.strandBinary(), "doctor", "-c", p.ConfigPath, "--json")
	cmd.Env = append(os.Environ(), env...)
	// A child that outlives strand (ssh of a tunnel) keeps the pipes open: stop waiting for them shortly
	// after the deadline instead of hanging the request.
	cmd.WaitDelay = 2 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedBuffer{buf: &stdout, max: doctorOutputLimit}
	cmd.Stderr = &limitedBuffer{buf: &stderr, max: 64 << 10}
	runErr := cmd.Run()
	res := &DoctorResult{Pipeline: p.Name, DurationMS: time.Since(start).Milliseconds()}
	if code := zeroOr(runErr); code != nil {
		res.ExitCode = *code
	} else if runErr != nil {
		res.ExitCode = -1
	}
	if dctx.Err() == context.DeadlineExceeded {
		res.TimedOut = true
		res.Findings = []findings.Finding{{Severity: findings.Error, Code: "doctor_timeout", Object: "pipeline",
			Message: fmt.Sprintf("doctor stopped after %s: the source or target did not answer", doctorTimeout),
			Fix:     "check host, port, firewall and ssh_tunnel of the source and target"}}
		res.Summary.Errors = 1
		return res, nil
	}
	var rep findings.Report
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &rep); err != nil || (rep.Pipeline == "" && rep.Findings == nil) {
		detail := strings.TrimSpace(stderr.String() + "\n" + stdout.String())
		if detail == "" && runErr != nil {
			detail = runErr.Error()
		}
		return res, fmt.Errorf("%w (exit %d): %s", errDoctorOutput, res.ExitCode, tailString(MaskSecrets(detail), 2000))
	}
	res.OK, res.Summary = rep.OK, rep.Summary
	res.Findings = make([]findings.Finding, 0, len(rep.Findings))
	for _, f := range rep.Findings {
		f.Object, f.Message, f.Fix = MaskSecrets(f.Object), MaskSecrets(f.Message), MaskSecrets(f.Fix)
		res.Findings = append(res.Findings, f)
	}
	return res, nil
}

func (s *Server) apiDoctor(w http.ResponseWriter, r *http.Request, sess *Session) {
	name := r.PathValue("name")
	p := s.reg.Get(name)
	if p == nil {
		writeErr(w, http.StatusNotFound, "unknown pipeline")
		return
	}
	user := ""
	if sess != nil {
		user = sess.Username
	}
	entry := AuditEntry{IP: s.clientIP(r), User: user, Action: "pipeline.doctor", Pipeline: name}
	res, err := s.runDoctor(r.Context(), p)
	if err != nil {
		entry.Result, entry.Detail = "error", map[string]any{"error": err.Error()}
		if res != nil {
			entry.ExitCode = &res.ExitCode
		}
		_ = s.audit.Log(entry)
		var ae *ActionError
		if errors.As(err, &ae) {
			writeErr(w, ae.Status, ae.Msg)
			return
		}
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	entry.Result, entry.ExitCode = "ok", &res.ExitCode
	entry.Detail = map[string]any{"errors": res.Summary.Errors, "warnings": res.Summary.Warnings, "timed_out": res.TimedOut}
	_ = s.audit.Log(entry)
	writeJSON(w, http.StatusOK, res)
}

// limitedBuffer ghi tối đa max byte vào buf, phần thừa bị bỏ (tiến trình con không bị chặn).
type limitedBuffer struct {
	buf *bytes.Buffer
	max int
}

func (l *limitedBuffer) Write(b []byte) (int, error) {
	if room := l.max - l.buf.Len(); room > 0 {
		if len(b) > room {
			l.buf.Write(b[:room])
		} else {
			l.buf.Write(b)
		}
	}
	return len(b), nil
}

func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}
