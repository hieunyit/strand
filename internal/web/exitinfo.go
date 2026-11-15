package web

import (
	"encoding/json"
	"regexp"
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
var textMsgRE = regexp.MustCompile(`\bmsg=("(?:[^"\\]|\\.)*"|\S+)`)
var textTimeRE = regexp.MustCompile(`\btime=(\S+)`)

// ParseLogEvents lọc các dòng WARN/ERROR từ log của strand (JSON hoặc text của slog), mới nhất ở cuối.
func ParseLogEvents(lines []string, max int) []LogEvent {
	var out []LogEvent
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		if strings.HasPrefix(ln, "{") {
			var m map[string]any
			if json.Unmarshal([]byte(ln), &m) != nil {
				continue
			}
			lv, _ := m["level"].(string)
			if lv != "WARN" && lv != "ERROR" {
				continue
			}
			ts, _ := m["time"].(string)
			msg, _ := m["msg"].(string)
			out = append(out, LogEvent{Time: ts, Level: lv, Message: msg, Raw: ln})
			continue
		}
		if m := textLevelRE.FindStringSubmatch(ln); m != nil {
			ev := LogEvent{Level: m[1], Raw: ln, Message: ln}
			if mm := textMsgRE.FindStringSubmatch(ln); mm != nil {
				ev.Message = strings.Trim(mm[1], `"`)
			}
			if tm := textTimeRE.FindStringSubmatch(ln); tm != nil {
				ev.Time = tm[1]
			}
			out = append(out, ev)
		}
	}
	if max > 0 && len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

// EventsToLog chuyển bản ghi từ admin socket sang LogEvent.
func EventsToLog(ev []admin.EventRecord) []LogEvent {
	out := make([]LogEvent, 0, len(ev))
	for _, e := range ev {
		msg := e.Message
		if len(e.Attrs) > 0 {
			keys := sortedKeys(e.Attrs)
			var sb strings.Builder
			sb.WriteString(msg)
			for _, k := range keys {
				sb.WriteString("  " + k + "=" + e.Attrs[k])
			}
			msg = sb.String()
		}
		out = append(out, LogEvent{Time: e.Time.Format(time.RFC3339), Level: e.Level, Message: msg})
	}
	return out
}
