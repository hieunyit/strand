package findings

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"
)

func TestCollectorKeepsEveryProblemSortedAndDeduplicated(t *testing.T) {
	var nilC *Collector
	nilC.Add(Finding{Severity: Error, Code: "x"}) // nil collector: no panic, nothing kept
	if nilC.Findings() != nil || nilC.HasErrors() {
		t.Fatal("nil collector must be empty")
	}

	c := &Collector{}
	c.Info("i1", "source", "info first", "")
	c.Warn("w1", "target", "warning", "do this")
	c.Errorf("e1", "app.t", "error one", "fix one")
	c.Errorf("e1", "app.t", "error one", "another fix") // same problem again: kept once
	c.Errorf("e2", "app.u", "error two", "fix two")
	got := c.Findings()
	var codes []string
	for _, f := range got {
		codes = append(codes, f.Code)
	}
	if strings.Join(codes, ",") != "e1,e2,w1,i1" {
		t.Fatalf("order = %v, want errors, then warnings, then info, each in discovery order", codes)
	}
	if c.Count(Error) != 2 || c.Count(Warning) != 1 || c.Count(Info) != 1 || !c.HasErrors() {
		t.Fatalf("counts: %d %d %d", c.Count(Error), c.Count(Warning), c.Count(Info))
	}
	if !c.Has("e2", "app.u") || c.Has("e2", "app.t") {
		t.Fatal("Has must match code and object")
	}
}

func TestCollectorRedactsSecretsAndUnwrapsExitcodeErrors(t *testing.T) {
	c := &Collector{}
	c.AddSecret("s3cr3t-pw")
	c.AddSecret("ab") // too short to redact safely
	c.Err(Error, "source_unreachable", "source", exitcode.New("x", exitcode.Rejected, "login as u:s3cr3t-pw failed", errors.New("ab")), "check s3cr3t-pw")
	f := c.Findings()[0]
	if strings.Contains(f.Message+f.Fix, "s3cr3t-pw") {
		t.Fatalf("secret leaked: %+v", f)
	}
	if strings.HasPrefix(f.Message, "[x]") || f.Message != "login as u:xxxxx failed: ab" {
		t.Fatalf("message = %q, want the exitcode message without its code prefix", f.Message)
	}
	if Message(errors.New("plain")) != "plain" {
		t.Fatal("plain error message")
	}
}

func TestRenderHumanGroupsBySeverityAndJSONIsStable(t *testing.T) {
	c := &Collector{}
	c.Warn("w1", "", "slow", "")
	c.Errorf("source_privilege_missing", "source", "lacks REPLICATION SLAVE", "GRANT REPLICATION SLAVE ON *.* TO 'u'@'%';")
	c.Errorf("source_table_no_key", "app.t", "no key", "line one\nline two")
	r := NewReport("p1", c)
	if r.OK || r.Summary != (Summary{Errors: 2, Warnings: 1}) {
		t.Fatalf("report summary = %+v ok=%v", r.Summary, r.OK)
	}
	var h bytes.Buffer
	WriteHuman(&h, r)
	out := h.String()
	for _, want := range []string{
		"pipeline p1: 2 error(s), 1 warning(s), 0 info",
		"ERRORS",
		"  [source_privilege_missing] source: lacks REPLICATION SLAVE\n      fix: GRANT REPLICATION SLAVE ON *.* TO 'u'@'%';\n",
		"      fix: line one\n           line two\n",
		"WARNINGS",
		"  [w1] slow\n",
		"Result: FAILED, 2 blocking problem(s)",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("human output lacks %q:\n%s", want, out)
		}
	}
	if strings.Index(out, "ERRORS") > strings.Index(out, "WARNINGS") || strings.Contains(out, "INFO") {
		t.Errorf("groups out of order or empty group printed:\n%s", out)
	}

	var j bytes.Buffer
	if err := WriteJSON(&j, r); err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(j.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"pipeline", "ok", "summary", "findings"} {
		if _, ok := raw[k]; !ok {
			t.Errorf("JSON lacks %q: %s", k, j.String())
		}
	}
	first := raw["findings"].([]any)[0].(map[string]any)
	for _, k := range []string{"severity", "code", "object", "message", "fix"} {
		if _, ok := first[k]; !ok {
			t.Errorf("finding lacks %q: %v", k, first)
		}
	}

	clean := NewReport("p2", &Collector{})
	var cj, ch bytes.Buffer
	_ = WriteJSON(&cj, clean)
	WriteHuman(&ch, clean)
	if !clean.OK || !strings.Contains(cj.String(), `"findings": []`) || !strings.Contains(ch.String(), "Result: OK") {
		t.Fatalf("clean report: %s / %s", cj.String(), ch.String())
	}
}
