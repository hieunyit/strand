package main

import (
	"math"
	"strings"
	"testing"
)

func validReviewRuns() []runResult {
	var out []runResult
	for _, g := range []string{"static", "onoff", "grace-v2"} {
		for i := 1; i <= 3; i++ {
			secs := jf(1000)
			if g == "grace-v2" {
				secs = 600
			}
			out = append(out, runResult{Governor: g, Repeat: i, SnapshotSeconds: secs, AppP99Ms: 10, AppTxns: 1000, Samples: int(secs), ValidSamples: int(secs), P2PWindows: 1, P2PLogRMedian: 0.1, P2PLogRMax: 0.1, CompareOK: true, OK: true})
		}
	}
	return out
}

// Dữ liệu thiếu không được biến thành p2p=0 rồi khuyến nghị mặc định.
func TestReviewBenchRejectsIncompleteEvidence(t *testing.T) {
	for _, name := range []string{"no-windows", "nan", "app-errors", "too-few-runs", "missing-static", "no-valid-samples", "scrape-errors", "coverage-gap", "duplicate-repeat"} {
		t.Run(name, func(t *testing.T) {
			runs := validReviewRuns()
			switch name {
			case "no-windows":
				for i := range runs {
					runs[i].P2PWindows = 0
				}
			case "nan":
				for i := range runs {
					runs[i].P2PLogRMedian = jf(math.NaN())
				}
			case "app-errors":
				runs[0].AppErrors = 1
			case "too-few-runs":
				runs = append(runs[:1], runs[3], runs[6])
			case "missing-static":
				runs = runs[3:]
			case "no-valid-samples":
				runs[0].ValidSamples = 0
			case "scrape-errors":
				runs[0].InvalidSamples = 1
			case "coverage-gap":
				runs[0].ValidSamples = 100
			case "duplicate-repeat":
				runs[1].Repeat = runs[0].Repeat
			}
			_, decision := decide(runs)
			if strings.Contains(decision, "làm mặc định") {
				t.Fatalf("accepted incomplete evidence: %s", decision)
			}
		})
	}
}

func TestReviewBenchAcceptsCompleteEvidence(t *testing.T) {
	_, d := decide(validReviewRuns())
	if !strings.Contains(d, "làm mặc định") {
		t.Fatal(d)
	}
	runs := validReviewRuns()
	runs[8].P2PLogRMax = 0.4
	_, d = decide(runs)
	if strings.Contains(d, "làm mặc định") {
		t.Fatal("accepted an unstable window: " + d)
	}
}
