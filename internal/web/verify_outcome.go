package web

import (
	"regexp"
	"strconv"
	"strings"

	"strand/internal/exitcode"
)

// verifyOutcome giải thích một job verify thoát mã 5: phân biệt "chưa kết luận được" (verify_incomplete:
// bảng bị bỏ qua vì row đổi liên tục, không phải lệch dữ liệu) với "lệch thật" (verify_mismatch), và liệt kê
// bảng cần kiểm lại kèm lý do. Đọc từ output của CLI ("Error [code]: ..."), không đổi CLI.
type verifyOutcome struct {
	Status       string            `json:"status"` // incomplete | mismatch
	Title        string            `json:"title"`
	Explain      string            `json:"explain"`
	Tables       []verifySkipTable `json:"tables,omitempty"`
	NoMismatch   bool              `json:"no_mismatch"` // incomplete và mọi khóa đã kiểm đều khớp
	ScopeChanged bool              `json:"scope_changed,omitempty"`
	Message      string            `json:"message"`
	Mode         string            `json:"mode,omitempty"`
}

type verifySkipTable struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

var (
	verifyErrRE      = regexp.MustCompile(`(?m)^Error \[(verify_incomplete|verify_mismatch)\]: (.*)$`)
	verifyKeySkipRE  = regexp.MustCompile(`tables skipped by key verification: (.*?)(?:: \[|$)`)
	verifyEntryRE    = regexp.MustCompile(`([^\s,;:()]+) \(([^()]*)\)`)
	verifyHotRE      = regexp.MustCompile(`([^\s,;:()]+) has (\d+) unverifiable hot keys`)
	verifyKeylessRE  = regexp.MustCompile(`keyless table ([^\s,;:()]+) inconclusive: changing source \((\d+) attempts\)`)
	verifyByKeyRE    = regexp.MustCompile(`\d+ tables cannot be verified by key: ([^;]+)`)
	verifyTrackingRE = regexp.MustCompile(`\d+ tables have incomplete changed-key tracking \([^)]*\): ([^;]+)`)
	verifyMismRE     = regexp.MustCompile(`\b(\d+) mismatches\b`)
)

// parseVerifyOutcome trả nil khi job không phải verify thoát mã 5 hoặc output không có dòng lỗi verify.
func parseVerifyOutcome(j JobStatus, out string) *verifyOutcome {
	if j.Kind != "verify" || j.Running || j.ExitCode == nil || *j.ExitCode != exitcode.DataError {
		return nil
	}
	ms := verifyErrRE.FindAllStringSubmatch(out, -1)
	if len(ms) == 0 {
		return nil
	}
	m := ms[len(ms)-1]
	v := &verifyOutcome{Message: MaskSecrets(strings.TrimSpace(m[2])), Mode: argAfter(j.Args, "--mode")}
	msg := v.Message
	seen := map[string]bool{}
	add := func(name, reason string) {
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		v.Tables = append(v.Tables, verifySkipTable{Name: name, Reason: reason})
	}
	if k := verifyKeySkipRE.FindStringSubmatch(msg); k != nil {
		for _, e := range verifyEntryRE.FindAllStringSubmatch(k[1], -1) {
			add(e[1], e[2])
		}
	}
	for _, e := range verifyHotRE.FindAllStringSubmatch(msg, -1) {
		add(e[1], e[2]+" keys kept changing while they were compared")
	}
	for _, e := range verifyKeylessRE.FindAllStringSubmatch(msg, -1) {
		add(e[1], "no key; the source kept changing over "+e[2]+" count+checksum attempts")
	}
	for _, e := range verifyTrackingRE.FindAllStringSubmatch(msg, -1) {
		for _, n := range strings.Split(e[1], ",") {
			add(n, "changed-key tracking is incomplete (limit, encoding failure or implicit cascade)")
		}
	}
	for _, e := range verifyByKeyRE.FindAllStringSubmatch(msg, -1) {
		for _, n := range strings.Split(e[1], ",") {
			add(n, "cannot be verified by key")
		}
	}
	v.ScopeChanged = strings.Contains(msg, "table scope changed during full verify")
	if m[1] == exitcode.ErrVerifyMismatch {
		v.Status = "mismatch"
		v.Title = "Verify found rows that differ"
		v.Explain = "The target does not match the source for the rows listed in the message. Use the Repair wizard to copy them from the source."
		return v
	}
	v.Status = "incomplete"
	v.Title = "Verify could not finish: no difference found"
	v.Explain = "Some tables could not be compared, so the result is not a pass. This is not a data mismatch: " +
		"usually the rows kept changing while verify read them. Verify these tables again when writes are quieter, " +
		"or with sample or full verify, which compare by key range."
	if v.ScopeChanged {
		v.Explain += " The set of tables also changed while the full verify ran: run it again."
	}
	counts := verifyMismRE.FindAllStringSubmatch(out, -1)
	v.NoMismatch = len(counts) > 0
	for _, c := range counts {
		if n, _ := strconv.Atoi(c[1]); n != 0 {
			v.NoMismatch = false
		}
	}
	return v
}

// argAfter trả giá trị của cờ dạng "--flag value" hoặc "--flag=value".
func argAfter(args []string, flag string) string {
	for i, a := range args {
		if a == flag && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(a, flag+"=") {
			return strings.TrimPrefix(a, flag+"=")
		}
	}
	return ""
}
