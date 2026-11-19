package web

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
	"time"

	"strand/internal/admin"
	"strand/internal/exitcode"
)

// ExitInfo giải thích một exit code (bảng 8.3 của docs/USAGE.md).
type ExitInfo struct {
	Code    int    `json:"code"`
	Meaning string `json:"meaning"`
	Action  string `json:"action"`
	// Restarts: systemd tự khởi động lại (RestartPreventExitStatus loại 2 3 4 5 10).
	AutoRestart bool `json:"auto_restart"`
}

// ExplainExit trả ý nghĩa và cách xử lý của exit code.
func ExplainExit(code int) ExitInfo {
	switch code {
	case exitcode.Success:
		return ExitInfo{code, "Success, or stopped by a signal", "-", false}
	case exitcode.Unclassified:
		return ExitInfo{code, "Unclassified or transient error (lost connection, ...)", "Systemd restarts it automatically; read the log if it repeats", true}
	case exitcode.InvalidSyntax:
		return ExitInfo{code, "Invalid configuration or flag, unknown key", "Fix the configuration (see the Config tab), then start again", false}
	case exitcode.Rejected:
		return ExitInfo{code, "validate/plan rejected: binlog/GTID settings, missing primary key, unmappable type, collation, ...", "Fix what the message names (USAGE section 4, 5)", false}
	case exitcode.Fenced:
		return ExitInfo{code, "pipeline_locked or fenced: another instance with the same name is running or has replaced this one", "Make sure only one instance runs", false}
	case exitcode.DataError:
		return ExitInfo{code, "Data or log error: log_gap, prefetch_exhausted or verify_mismatch", "log_gap: restore the log from the archive or re-seed; verify_mismatch: run the repair procedure (USAGE 7.8)", false}
	case exitcode.NeedsDecision:
		return ExitInfo{code, "Stopped for an operator decision: schema_change_pending or promote_needs_decision", "Apply the DDL on the target and start again; or fence the source and re-run promote with --source-fenced-externally", false}
	case 137:
		return ExitInfo{code, "Killed (SIGKILL / OOM)", "Safe to start: it resumes from the last checkpoint", true}
	case 143:
		return ExitInfo{code, "Terminated (SIGTERM)", "-", false}
	}
	return ExitInfo{code, "Unknown exit code", "Read the log", false}
}

// explainExitFor như ExplainExit nhưng theo chế độ quản lý: chế độ nền không có systemd khởi động lại, nên
// không hứa auto_restart và chỉ cách Start lại.
func explainExitFor(code int, mode string) ExitInfo {
	e := ExplainExit(code)
	if mode == ModeBackground && e.AutoRestart {
		e.AutoRestart = false
		if code == exitcode.Unclassified {
			e.Action = "Background mode does not restart automatically: fix the cause shown in the reason / log, then press Start (it resumes from the last checkpoint)"
		}
	}
	return e
}

// ErrorCodeFromLine tìm mã lỗi dạng [code] (exitcode.Error.Error()) trong một dòng log.
var errCodeRE = regexp.MustCompile(`\[([a-z][a-z0-9_]+)\]`)

func ErrorCodeFromLine(line string) string {
	if m := errCodeRE.FindStringSubmatch(line); m != nil {
		return m[1]
	}
	return ""
}

// LogEvent là một dòng WARN/ERROR rút từ file log.
type LogEvent struct {
	Time    string `json:"time,omitempty"`
	Level   string `json:"level"`
	Message string `json:"message"`
	Raw     string `json:"raw,omitempty"`
}

var textLevelRE = regexp.MustCompile(`\blevel=(WARN|ERROR)\b`)
var textAnyLevelRE = regexp.MustCompile(`\blevel=(\S+)`)
var textMsgRE = regexp.MustCompile(`\bmsg=("(?:[^"\\]|\\.)*"|\S+)`)
var textTimeRE = regexp.MustCompile(`\btime=(\S+)`)
var textKVRE = regexp.MustCompile(`(?:^|\s)([A-Za-z_][A-Za-z0-9_.-]*)=("(?:[^"\\]|\\.)*"|\S*)`)

// cliErrRE khớp dòng lỗi cuối của CLI (cmd/strand/main.go): `Error: <msg>` hoặc `Error [code]: <msg>`.
var cliErrRE = regexp.MustCompile(`^Error(?: \[([a-z][a-z0-9_]+)\])?: (.+)$`)

// stdAttrs là các khóa không đưa vào phần thuộc tính của sự kiện.
var stdAttrs = map[string]bool{"time": true, "level": true, "msg": true, "pipeline": true, "epoch": true}

// logRec là một bản ghi slog (JSON hoặc text) đã tách.
type logRec struct {
	Time, Level, Msg string
	Attrs            map[string]string
}

// parseSlogLine tách một dòng log slog; ok=false nếu không phải bản ghi slog.
func parseSlogLine(ln string) (logRec, bool) {
	var r logRec
	if strings.HasPrefix(ln, "{") {
		var m map[string]any
		if json.Unmarshal([]byte(ln), &m) != nil {
			return r, false
		}
		r.Time, _ = m["time"].(string)
		r.Level, _ = m["level"].(string)
		r.Msg, _ = m["msg"].(string)
		for k, v := range m {
			if stdAttrs[k] {
				continue
			}
			if r.Attrs == nil {
				r.Attrs = map[string]string{}
			}
			switch x := v.(type) {
			case string:
				r.Attrs[k] = x
			case float64:
				r.Attrs[k] = strconv.FormatFloat(x, 'f', -1, 64)
			default:
				b, _ := json.Marshal(x)
				r.Attrs[k] = string(b)
			}
		}
		return r, true
	}
	lm := textAnyLevelRE.FindStringSubmatch(ln)
	if lm == nil {
		return r, false
	}
	r.Level, r.Msg = lm[1], ln
	if mm := textMsgRE.FindStringSubmatch(ln); mm != nil {
		r.Msg = unquoteVal(mm[1])
	}
	if tm := textTimeRE.FindStringSubmatch(ln); tm != nil {
		r.Time = tm[1]
	}
	for _, kv := range textKVRE.FindAllStringSubmatch(ln, -1) {
		if stdAttrs[kv[1]] {
			continue
		}
		if r.Attrs == nil {
			r.Attrs = map[string]string{}
		}
		r.Attrs[kv[1]] = unquoteVal(kv[2])
	}
	return r, true
}

func unquoteVal(v string) string {
	if len(v) >= 2 && v[0] == '"' {
		if u, err := strconv.Unquote(v); err == nil {
			return u
		}
		return strings.Trim(v, `"`)
	}
	return v
}

// withAttrs ghép message và thuộc tính theo cùng khuôn với sự kiện live (EventsToLog): "msg  k=v  k2=v2".
func withAttrs(msg string, attrs map[string]string) string {
	if len(attrs) == 0 {
		return msg
	}
	var sb strings.Builder
	sb.WriteString(msg)
	for _, k := range sortedKeys(attrs) {
		sb.WriteString("  " + k + "=" + attrs[k])
	}
	return sb.String()
}

// ParseLogEvents lọc các dòng WARN/ERROR từ log của strand (JSON hoặc text của slog), mới nhất ở cuối.
// Thuộc tính (error, reason, hint, table, ...) được ghép vào message như sự kiện live; dòng `Error: ...`
// cuối cùng của CLI (không phải bản ghi slog) cũng thành sự kiện ERROR nếu lý do chưa có trong sự kiện nào.
func ParseLogEvents(lines []string, max int) []LogEvent {
	var out []LogEvent
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		if m := cliErrRE.FindStringSubmatch(ln); m != nil {
			dup := false
			for _, e := range out {
				if e.Level == "ERROR" && strings.Contains(e.Message, m[2]) {
					dup = true
					break
				}
			}
			if !dup {
				out = append(out, LogEvent{Level: "ERROR", Message: ln, Raw: ln})
			}
			continue
		}
		r, ok := parseSlogLine(ln)
		if !ok || (r.Level != "WARN" && r.Level != "ERROR") {
			continue
		}
		out = append(out, LogEvent{Time: r.Time, Level: r.Level, Message: withAttrs(r.Msg, r.Attrs), Raw: ln})
	}
	if max > 0 && len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

// runMarkers đánh dấu một lần chạy mới bắt đầu: lỗi cũ hơn mốc này không còn là kết cục của lần chạy hiện tại.
var runMarkers = map[string]bool{"pipeline lock acquired": true, "source MySQL version": true, "pipeline running": true}

// cleanEndMarkers: lần chạy kết thúc êm.
var cleanEndMarkers = map[string]bool{"pipeline stopped cleanly": true}

// LogOutcome là kết cục của lần chạy gần nhất suy ra từ đuôi log (chế độ nền).
type LogOutcome struct {
	Failed bool
	Err    string // lý do (đã gồm [code] nếu có)
	Code   string // mã lỗi [code], rỗng nếu lỗi không gắn mã
	// Clean: lần chạy cuối kết thúc bằng "pipeline stopped cleanly" (exit 0: `strand stop`, SIGTERM); CleanAt là
	// thời điểm của dòng đó (zero nếu không đọc được). Bị giết (SIGKILL/OOM) thì không có dòng này: Clean=false.
	Clean   bool
	CleanAt time.Time
}

// ParseLogOutcome duyệt log từ cũ tới mới: mốc khởi động hoặc dừng êm xóa lỗi trước đó; ERROR "pipeline failed"
// hoặc dòng `Error...: ` cuối của CLI đặt lỗi. Chỉ lỗi còn lại ở cuối log mới tính (lỗi cũ rồi chạy+dừng êm thì không).
func ParseLogOutcome(lines []string) LogOutcome {
	var o LogOutcome
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if m := cliErrRE.FindStringSubmatch(ln); m != nil {
			o = LogOutcome{Failed: true, Err: m[2], Code: m[1]}
			if m[1] != "" {
				o.Err = "[" + m[1] + "] " + m[2]
			}
			continue
		}
		r, ok := parseSlogLine(ln)
		if !ok {
			continue
		}
		switch {
		case cleanEndMarkers[r.Msg]:
			o = LogOutcome{Clean: true}
			if t, err := time.Parse(time.RFC3339Nano, r.Time); err == nil {
				o.CleanAt = t
			}
		case runMarkers[r.Msg]:
			o = LogOutcome{}
		case r.Level == "ERROR" && r.Msg == "pipeline failed":
			e := r.Attrs["error"]
			o = LogOutcome{Failed: true, Err: e, Code: ErrorCodeFromLine(e)}
		}
	}
	return o
}

// EventsToLog chuyển bản ghi từ admin socket sang LogEvent.
func EventsToLog(ev []admin.EventRecord) []LogEvent {
	out := make([]LogEvent, 0, len(ev))
	for _, e := range ev {
		msg := withAttrs(e.Message, e.Attrs)
		out = append(out, LogEvent{Time: e.Time.Format(time.RFC3339), Level: e.Level, Message: msg})
	}
	return out
}
