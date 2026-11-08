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

func TestSysbenchRawWindowUsesCompletionAndRequiresFooters(t *testing.T) {
	base := time.Unix(1_800_000_000, 0).UTC()
	dir := t.TempDir()
	prefix := filepath.Join(dir, "raw")
	paths := []string{prefix + ".0", prefix + ".1"}
	for thread, path := range paths {
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i <= 50; i++ {
			lat := (thread*50 + i) * 1_000_000
			at := base.Add(time.Duration(i) * time.Millisecond)
			fmt.Fprintf(file, "%d %d %d %d\n", thread, i, at.UnixNano(), lat)
		}
		fmt.Fprintf(file, "# STRAND_RAW_THREAD thread_id=%d count=50 errors=0\n", thread)
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	stderrPath := filepath.Join(dir, "sysbench.stderr")
	if err := os.WriteFile(stderrPath, []byte("STRAND_RAW_EVENTS count=100 errors=0 run_status=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	count, err := parseSysbenchRawMarker(stderrPath)
	if err != nil || count != 100 {
		t.Fatalf("marker count=%d err=%v", count, err)
	}
	got, err := parseSysbenchRawWindow(paths, base, base.Add(time.Second), count, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got.Transactions != 100 || got.P99Ms != 99 || got.OfficialReady {
		t.Fatalf("raw window: %+v", got)
	}
	var out, bad bytes.Buffer
	code := run([]string{"--parse-sysbench-prefix", prefix, "--sysbench-stderr", stderrPath,
		"--sysbench-threads", "2", "--sysbench-exit-code", "0",
		"--snapshot-start", base.Format(time.RFC3339Nano),
		"--snapshot-end", base.Add(time.Second).Format(time.RFC3339Nano)}, &out, &bad)
	if code != 0 || !strings.Contains(out.String(), `"p99_ms": 99`) {
		t.Fatalf("CLI code=%d out=%s err=%s", code, out.String(), bad.String())
	}
	out.Reset()
	bad.Reset()
	code = run([]string{"--parse-sysbench-prefix", prefix, "--sysbench-stderr", stderrPath,
		"--sysbench-threads", "2", "--sysbench-exit-code", "1",
		"--snapshot-start", base.Format(time.RFC3339Nano),
		"--snapshot-end", base.Add(time.Second).Format(time.RFC3339Nano)}, &out, &bad)
	if code != 1 || !strings.Contains(bad.String(), "process failed") {
		t.Fatalf("failed sysbench process accepted: code=%d out=%s err=%s", code, out.String(), bad.String())
	}
	if err := os.WriteFile(paths[1], []byte("1 1 1800000000001000000 1000000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseSysbenchRawWindow(paths, base, base.Add(time.Second), count, 2); err == nil || !strings.Contains(err.Error(), "missing thread footer") {
		t.Fatalf("truncated file accepted: %v", err)
	}
}

func TestSysbenchRawRejectsMarkerErrorAndCountMismatch(t *testing.T) {
	base := time.Unix(1_800_000_000, 0)
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.0")
	if err := os.WriteFile(path, []byte("0 1 1800000000001000000 1000000\n# STRAND_RAW_THREAD thread_id=0 count=1 errors=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseSysbenchRawWindow([]string{path}, base, base.Add(time.Second), 2, 1); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("count mismatch accepted: %v", err)
	}
	stderrPath := filepath.Join(dir, "sysbench.stderr")
	if err := os.WriteFile(stderrPath, []byte("STRAND_RAW_EVENTS count=1 errors=1 run_status=0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseSysbenchRawMarker(stderrPath); err == nil {
		t.Fatal("write error marker accepted")
	}
	if err := os.WriteFile(stderrPath, []byte("STRAND_RAW_EVENTS count=1 errors=0 run_status=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseSysbenchRawMarker(stderrPath); err == nil {
		t.Fatal("failed workload marker accepted")
	}
}

func TestSysbenchRawWindowIsHalfOpenByCompletionTime(t *testing.T) {
	base := time.Unix(1_800_000_000, 0).UTC()
	path := filepath.Join(t.TempDir(), "raw.0")
	data := fmt.Sprintf("0 1 %d 1000000\n0 2 %d 2000000\n0 3 %d 999000000\n# STRAND_RAW_THREAD thread_id=0 count=3 errors=0\n",
		base.UnixNano(), base.Add(500*time.Millisecond).UnixNano(), base.Add(time.Second).UnixNano())
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := parseSysbenchRawWindow([]string{path}, base, base.Add(time.Second), 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Transactions != 2 || got.P99Ms != 2 {
		t.Fatalf("end boundary leaked into p99: %+v", got)
	}
}

func TestSysbenchRawRejectsClockStepBackwards(t *testing.T) {
	base := time.Unix(1_800_000_000, 0).UTC()
	path := filepath.Join(t.TempDir(), "raw.0")
	data := fmt.Sprintf("0 1 %d 1000000\n0 2 %d 2000000\n# STRAND_RAW_THREAD thread_id=0 count=2 errors=0\n",
		base.Add(2*time.Millisecond).UnixNano(), base.Add(time.Millisecond).UnixNano())
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseSysbenchRawWindow([]string{path}, base, base.Add(time.Second), 2, 1); err == nil ||
		!strings.Contains(err.Error(), "moved backwards") {
		t.Fatalf("realtime clock regression accepted: %v", err)
	}
}
