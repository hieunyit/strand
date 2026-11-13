package main

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"strand/internal/config"
)

func TestChildArgsDropsBackgroundFlag(t *testing.T) {
	got := childArgs([]string{"sync", "-c", "p.yaml", "-d", "--background", "--background=true", "--log-file", "x.log"})
	want := []string{"sync", "-c", "p.yaml", "--log-file", "x.log"}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestBgPathsDefaultToSpoolDir(t *testing.T) {
	cfg := &config.Config{}
	cfg.Resources.Spool.Path = "/var/lib/strand/x"
	l, p := bgPaths(cfg, "", "")
	if l != "/var/lib/strand/x/strand.log" || p != "/var/lib/strand/x/strand.pid" {
		t.Fatalf("%s %s", l, p)
	}
	if l, p = bgPaths(cfg, "a.log", "b.pid"); l != "a.log" || p != "b.pid" {
		t.Fatalf("%s %s", l, p)
	}
}

// PID của chính test (không phải "strand sync") không được coi là strand đang chạy: stop không được gửi
// SIGTERM nhầm cho tiến trình khác khi PID bị cấp lại.
func TestStrandSyncAliveRejectsOtherProcess(t *testing.T) {
	if strandSyncAlive(os.Getpid()) {
		t.Fatal("test process treated as strand sync")
	}
	if strandSyncAlive(1 << 30) {
		t.Fatal("nonexistent pid treated as alive")
	}
	f := filepath.Join(t.TempDir(), "s.pid")
	if err := os.WriteFile(f, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if pid, ok, err := readPid(f); err != nil || !ok || pid != os.Getpid() {
		t.Fatalf("readPid: %d %v %v", pid, ok, err)
	}
	if _, ok, err := readPid(filepath.Join(t.TempDir(), "none")); ok || err != nil {
		t.Fatalf("missing pid file: %v %v", ok, err)
	}
}
