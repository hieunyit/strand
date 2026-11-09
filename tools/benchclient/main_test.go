package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPgbenchWindowUsesRawCompletionTimeAndNearestRank(t *testing.T) {
	base := time.Unix(1_800_000_000, 0).UTC()
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "pgbench.1"), filepath.Join(dir, "pgbench.2")}
	files := make([]*os.File, 2)
	for i, path := range paths {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		files[i] = f
	}
	for n := 1; n <= 100; n++ {
		at := base.Add(time.Duration(n) * time.Millisecond)
		fmt.Fprintf(files[n%2], "%d %d %d 0 %d %d 0\n", n%2, n, n*1000,
			at.Unix(), at.Nanosecond()/1000)
	}
	// Bắt đầu/kết thúc đều là biên [start,end); sự kiện ngoài khoảng không đổi p99.
	fmt.Fprintf(files[0], "0 101 9999999 0 %d 0\n", base.Add(-time.Second).Unix())
	fmt.Fprintf(files[0], "0 102 9999999 0 %d 0\n", base.Add(time.Second).Unix())
	fmt.Fprintf(files[0], "0 103 failed 0 %d 0\n", base.Add(time.Second).Unix())
	for _, f := range files {
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	got, err := parsePgbenchWindow(paths, base, base.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got.Transactions != 100 || got.P99Ms != 99 || got.FailedTotal != 1 || got.OfficialReady {
		t.Fatalf("raw window: %+v", got)
	}
}

func TestPgbenchRawLogRejectsMalformedAndEmptyWindow(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	p := filepath.Join(t.TempDir(), "pgbench.1")
	if err := os.WriteFile(p, []byte("0 1 1000 0 1800000000 1000000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parsePgbenchWindow([]string{p}, base, base.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "microseconds") {
		t.Fatalf("expected malformed timestamp, got %v", err)
	}
	if err := os.WriteFile(p, []byte("0 1 1000 0 1800000001 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parsePgbenchWindow([]string{p}, base, base.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "no successful") {
		t.Fatalf("expected empty window, got %v", err)
	}
}

func TestExternalClientPlanDoesNotClaimOfficialReadiness(t *testing.T) {
	pg, err := planClient("pgbench", "/owned/pgbench", "/owned/log", 16, 8, 10*time.Minute, 200)
	if err != nil {
		t.Fatal(err)
	}
	if !pg.RawLatencyAvailable || pg.OfficialReady || !strings.Contains(strings.Join(pg.Argv, " "), "--log-prefix=/owned/log") {
		t.Fatalf("pg plan: %+v", pg)
	}
	sys, err := planClient("sysbench", "/owned/sysbench", "/owned/log", 16, 8, 10*time.Minute, 200)
	if err != nil {
		t.Fatal(err)
	}
	if sys.RawLatencyAvailable || sys.OfficialReady || !strings.Contains(strings.Join(sys.Argv, " "), "oltp_read_write run") {
		t.Fatalf("sysbench plan: %+v", sys)
	}
	if _, err := planClient("pgbench", "pgbench", "/tmp/x", 0, 1, time.Minute, 100); err == nil {
		t.Fatal("zero clients accepted")
	}
	var out, bad bytes.Buffer
	if code := run([]string{"--backend", "sysbench", "--log-prefix", "/owned/log"}, &out, &bad); code != 0 || !strings.Contains(out.String(), `"official_ready": false`) {
		t.Fatalf("dry run code=%d out=%s stderr=%s", code, out.String(), bad.String())
	}
}
