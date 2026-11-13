package main

import (
	"fmt"
	"strings"
	"testing"
)

func goodRun(gov string, rep int, snap, p99, p2p float64) RunResult {
	return RunResult{Name: fmt.Sprintf("%s-%d", gov, rep), Governor: gov, Repeat: rep, Attempt: 1, OK: true,
		SnapshotSeconds: jf(snap), AppP99Ms: jf(p99), P2PLogRMedian: jf(p2p), P2PLogRMax: jf(p2p), FixtureManifest: "m"}
}

func fullSet(snapGrace, p99Grace, p2p float64) []RunResult {
	var rs []RunResult
	for i := 1; i <= 3; i++ {
		rs = append(rs, goodRun("static", i, 1000, 12, 0), goodRun("onoff", i, 1000, 10, 0), goodRun("grace-v2", i, snapGrace, p99Grace, p2p))
	}
	return rs
}

func TestDecideRule(t *testing.T) {
	cases := []struct {
		name         string
		runs         []RunResult
		status       string
		reasonSubstr string
	}{
		{"grace passes", fullSet(650, 10, 0.2), StatusGrace, ""},
		{"exactly 30% faster", fullSet(700, 10, 0.3), StatusGrace, ""},
		{"29% faster", fullSet(710, 10, 0.2), StatusOnoff, "< 30%"},
		{"p99 worse", fullSet(600, 10.01, 0.2), StatusOnoff, "p99"},
		{"p2p above 0.3", fullSet(600, 10, 0.31), StatusOnoff, "p2p"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, v := Decide(c.runs, 3)
			if v.Status != c.status || !strings.Contains(v.String(), c.reasonSubstr) {
				t.Fatalf("%s", v)
			}
		})
	}
}

func TestDecideHardLimitViolationBlocksGrace(t *testing.T) {
	rs := fullSet(500, 9, 0.1)
	rs[2].HardLimitExceed = 1 // grace-v2 vượt hard limit một mẫu
	_, v := Decide(rs, 3)
	if v.Status != StatusOnoff || !strings.Contains(v.String(), "hard limit") {
		t.Fatal(v)
	}
}

func TestDecideP2PUsesMaxNotOnlyMedian(t *testing.T) {
	rs := fullSet(500, 9, 0.1)
	for i := range rs {
		if rs[i].Governor == "grace-v2" && rs[i].Repeat == 2 {
			rs[i].P2PLogRMax = 0.45 // median 0.1 nhưng một cửa sổ 0.45
		}
	}
	if _, v := Decide(rs, 3); v.Status != StatusOnoff {
		t.Fatal(v)
	}
}

func TestDecideIncompleteIsNotAResult(t *testing.T) {
	mut := map[string]func(rs []RunResult) []RunResult{
		"missing governor": func(rs []RunResult) []RunResult {
			var o []RunResult
			for _, r := range rs {
				if r.Governor != "static" {
					o = append(o, r)
				}
			}
			return o
		},
		"two repeats only": func(rs []RunResult) []RunResult { return rs[:len(rs)-1] },
		"duplicate repeat id": func(rs []RunResult) []RunResult {
			rs[5].Repeat = 1 // grace-v2#2 mang id của #1, cùng attempt
			return rs
		},
		"evidence error": func(rs []RunResult) []RunResult { rs[5].Evidence = "telemetry thiếu"; return rs },
		"failed run":     func(rs []RunResult) []RunResult { rs[4].OK = false; rs[4].Error = "strand exit 1"; return rs },
		"fixture differs": func(rs []RunResult) []RunResult {
			rs[3].FixtureManifest = "other"
			return rs
		},
		"unknown governor": func(rs []RunResult) []RunResult { rs[0].Governor = "bogus"; return rs },
		"empty":            func(rs []RunResult) []RunResult { return nil },
	}
	for name, f := range mut {
		t.Run(name, func(t *testing.T) {
			_, v := Decide(f(fullSet(500, 9, 0.1)), 3)
			if v.Status != StatusIncomplete {
				t.Fatalf("must be incomplete, got %s", v)
			}
		})
	}
}

func TestDecideRerunReplacesFailedAttemptButIsRecorded(t *testing.T) {
	rs := fullSet(500, 9, 0.1)
	bad := goodRun("onoff", 2, 0, 0, 0)
	bad.OK, bad.Error, bad.Attempt = false, "strand exit 1", 1
	// thay lần 1 của onoff#2 bằng lần lỗi, thêm lần chạy bù attempt 2
	for i := range rs {
		if rs[i].Governor == "onoff" && rs[i].Repeat == 2 {
			rr := rs[i]
			rr.Attempt = 2
			rs[i] = bad
			rs = append(rs, rr)
			break
		}
	}
	_, v := Decide(rs, 3)
	if v.Status != StatusGrace || len(v.Notes) != 1 || !strings.Contains(v.Notes[0], "attempt 1 replaced by attempt 2") {
		t.Fatalf("%s notes=%v", v, v.Notes)
	}
	// nếu lần bù cũng lỗi → incomplete
	for i := range rs {
		if rs[i].Attempt == 2 {
			rs[i].OK, rs[i].Error = false, "again"
		}
	}
	if _, v := Decide(rs, 3); v.Status != StatusIncomplete {
		t.Fatalf("%s", v)
	}
}
