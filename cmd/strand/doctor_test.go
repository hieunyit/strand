package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/findings"
)

func writeDoctorConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "p.yaml")
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(body, "SPOOL", filepath.Join(dir, "spool"))), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Both sides unreachable: doctor names both in one run (exit 3), while validate stops at the source.
func TestDoctorReportsEveryUnreachableSideAndExitsRejected(t *testing.T) {
	t.Setenv("DOCTOR_TEST_PW", "Hunter2-secret")
	path := writeDoctorConfig(t, `version: 1
name: dr
source: { kind: mysql, dsn: "strand:${DOCTOR_TEST_PW}@tcp(127.0.0.1:1)/?timeout=2s" }
target: { kind: mysql, dsn: "strand:${DOCTOR_TEST_PW}@tcp(127.0.0.1:1)/meta?timeout=2s" }
tables: { include: ["app.*"] }
resources: { spool: { path: SPOOL } }
`)
	for _, asJSON := range []bool{false, true} {
		var out bytes.Buffer
		err := runDoctor(context.Background(), &out, path, asJSON)
		if got := exitcode.GetExitCode(err); got != exitcode.Rejected {
			t.Fatalf("json=%v: exit %d (%v), want %d", asJSON, got, err, exitcode.Rejected)
		}
		s := out.String()
		if !strings.Contains(s, "source_unreachable") || !strings.Contains(s, "target_unreachable") {
			t.Fatalf("json=%v: want both sides reported:\n%s", asJSON, s)
		}
		if strings.Contains(s, "Hunter2-secret") {
			t.Fatalf("password leaked:\n%s", s)
		}
		if asJSON {
			var r findings.Report
			if err := json.Unmarshal(out.Bytes(), &r); err != nil || r.OK || r.Summary.Errors != 2 || r.Pipeline != "dr" {
				t.Fatalf("JSON report: %v %+v", err, r)
			}
		}
	}

	// A/B: validate (fail-fast) reports only the first unreachable side.
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = path
	cmd := newValidateCmd()
	cmd.SetContext(context.Background())
	err := cmd.RunE(cmd, nil)
	if err == nil || strings.Contains(err.Error(), "meta") {
		t.Fatalf("validate: %v", err)
	}
}

func TestDoctorReportsAnUnloadableConfig(t *testing.T) {
	path := writeDoctorConfig(t, "version: 1\nname: [broken\n")
	var out bytes.Buffer
	err := runDoctor(context.Background(), &out, path, true)
	if err == nil || exitcode.GetExitCode(err) == exitcode.Success {
		t.Fatal("broken config must fail")
	}
	var r findings.Report
	if jerr := json.Unmarshal(out.Bytes(), &r); jerr != nil || len(r.Findings) != 1 || r.Findings[0].Code != "config_invalid" {
		t.Fatalf("report: %v %s", jerr, out.String())
	}
}

func TestSnake(t *testing.T) {
	if got := snake("Oplog window (hours)"); got != "oplog_window_hours" {
		t.Fatal(got)
	}
}
