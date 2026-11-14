package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// calibSegment: đoạn sysbench không giới hạn tốc độ, 2 worker x n event cách nhau 100 ms (đủ dài để
// CalibrateFromEvents cắt 5 s mỗi đầu), stdout báo `ignored` lỗi đã thử lại.
func calibSegment(t *testing.T, n int, ignored int) SegmentRun {
	t.Helper()
	base := time.Unix(1_800_000_000, 0).UTC()
	dir := t.TempDir()
	prefix := filepath.Join(dir, "seg0000")
	for th := 0; th < 2; th++ {
		var b strings.Builder
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&b, "%d %d %d %d\n", th, i, base.Add(time.Duration(i)*100*time.Millisecond).UnixNano(), 1_000_000)
		}
		fmt.Fprintf(&b, "# STRAND_RAW_THREAD thread_id=%d count=%d errors=0\n", th, n)
		writeFile(t, fmt.Sprintf("%s.%d", prefix, th), b.String())
	}
	sr := SegmentRun{Threads: 2, Prefix: prefix, Stdout: prefix + ".stdout", Stderr: prefix + ".stderr"}
	writeFile(t, sr.Stdout, fmt.Sprintf("General statistics:\n    total number of events:              %d\n    ignored errors:                      %d      (0.01 per sec.)\n    reconnects:                          0      (0.00 per sec.)\n", 2*n, ignored))
	writeFile(t, sr.Stderr, fmt.Sprintf("STRAND_RAW_EVENTS count=%d errors=0 run_status=0\n", 2*n))
	return sr
}

// Ở bão hoà sysbench oltp luôn gặp vài deadlock (ignored errors 1–2 trên ~130k event); calibrate
// trước đây coi là lỗi nên không bao giờ chạy được (4/4 lần trên máy dev, 2026-11-14).
func TestCalibrationAcceptsFewIgnoredErrorsAtSaturation(t *testing.T) {
	o := Opts{Engine: "mysql", Clients: 2}
	c, err := calibrationFromSegment(o, calibSegment(t, 300, 2))
	if err != nil {
		t.Fatalf("calibration with 2 ignored errors in 600 events should pass: %v", err)
	}
	if c.IgnoredErrors != 2 || c.SaturationEPS <= 0 || c.High <= c.Low || c.Low < 1 {
		t.Fatalf("calibration %+v", c)
	}
}

func TestCalibrationRejectsManyIgnoredErrors(t *testing.T) {
	o := Opts{Engine: "mysql", Clients: 2}
	if _, err := calibrationFromSegment(o, calibSegment(t, 300, 7)); err == nil { // 7 > 1% of 600
		t.Fatal("7 ignored errors in 600 events must be rejected")
	}
}

func TestCalibrationStillRejectsReconnects(t *testing.T) {
	sr := calibSegment(t, 300, 0)
	writeFile(t, sr.Stdout, "total number of events: 600\nignored errors: 0 (0)\nreconnects: 1 (0.1 per sec.)\n")
	if _, err := calibrationFromSegment(Opts{Engine: "mysql", Clients: 2}, sr); err == nil {
		t.Fatal("reconnects must still fail calibration")
	}
}

// Lượt đo chính thức không được nới: cùng đoạn đó qua parser của run vẫn là lỗi.
func TestMeasuredSegmentStillRejectsIgnoredErrors(t *testing.T) {
	sr := calibSegment(t, 300, 1)
	if _, err := parseSegment(Opts{Engine: "mysql"})(sr); err == nil {
		t.Fatal("measured runs must reject ignored errors")
	}
}

// Chạy lại calibrate trên cùng --out: log thô cũ phải bị xoá, nếu không sysbench (O_EXCL) chết EEXIST.
func TestFreshCalibrateDirRemovesOldRawLogs(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "calibrate-raw")
	if err := freshCalibrateDir(dir); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(dir, "raw", "seg0000.0")
	writeFile(t, old, "stale")
	if err := freshCalibrateDir(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("stale raw log survived: %v", err)
	}
	if st, err := os.Stat(filepath.Join(dir, "raw")); err != nil || !st.IsDir() {
		t.Fatalf("raw dir missing: %v", err)
	}
}
