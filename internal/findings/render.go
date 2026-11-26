package findings

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Summary đếm vấn đề theo mức độ.
type Summary struct {
	Errors   int `json:"errors"`
	Warnings int `json:"warnings"`
	Info     int `json:"info"`
}

// Report là kết quả của `strand doctor` (cũng là dạng JSON của --json).
type Report struct {
	Pipeline string    `json:"pipeline"`
	OK       bool      `json:"ok"` // true khi không có vấn đề mức error
	Summary  Summary   `json:"summary"`
	Findings []Finding `json:"findings"`
}

// NewReport dựng Report từ c.
func NewReport(pipeline string, c *Collector) Report {
	r := Report{Pipeline: pipeline, Findings: c.Findings()}
	if r.Findings == nil {
		r.Findings = []Finding{}
	}
	for _, f := range r.Findings {
		switch f.Severity {
		case Error:
			r.Summary.Errors++
		case Warning:
			r.Summary.Warnings++
		default:
			r.Summary.Info++
		}
	}
	r.OK = r.Summary.Errors == 0
	return r
}

// WriteJSON in r dạng JSON (thụt lề hai dấu cách, một dòng xuống cuối).
func WriteJSON(w io.Writer, r Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteHuman in r cho người đọc: nhóm theo mức độ, mỗi vấn đề một dòng "[mã] đối tượng: thông điệp",
// cách sửa thụt lề ở các dòng sau.
func WriteHuman(w io.Writer, r Report) {
	fmt.Fprintf(w, "strand doctor: pipeline %s: %d error(s), %d warning(s), %d info\n", r.Pipeline, r.Summary.Errors, r.Summary.Warnings, r.Summary.Info)
	groups := []struct {
		sev   Severity
		title string
	}{{Error, "ERRORS (sync refuses to start until these are fixed)"}, {Warning, "WARNINGS (sync runs, but at risk)"}, {Info, "INFO"}}
	for _, g := range groups {
		var items []Finding
		for _, f := range r.Findings {
			if f.Severity == g.sev {
				items = append(items, f)
			}
		}
		if len(items) == 0 {
			continue
		}
		fmt.Fprintf(w, "\n%s\n", g.title)
		for _, f := range items {
			obj := ""
			if f.Object != "" {
				obj = " " + f.Object + ":"
			}
			fmt.Fprintf(w, "  [%s]%s %s\n", f.Code, obj, indentRest(f.Message, "      "))
			if f.Fix != "" {
				fmt.Fprintf(w, "      fix: %s\n", indentRest(f.Fix, "           "))
			}
		}
	}
	if r.OK {
		fmt.Fprintln(w, "\nResult: OK, no blocking problem found")
	} else {
		fmt.Fprintf(w, "\nResult: FAILED, %d blocking problem(s)\n", r.Summary.Errors)
	}
}

// indentRest thụt lề mọi dòng sau dòng đầu của s.
func indentRest(s, indent string) string {
	return strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n"+indent)
}
