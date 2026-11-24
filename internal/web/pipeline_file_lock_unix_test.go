//go:build unix

package web

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A config directory the Web user cannot create files in (root-owned
// /etc/strand with a group-writable config) must still allow saves: the lock
// falls back to a Web-owned directory and still serializes callers.
func TestPipelineLockFallsBackWhenConfigDirIsReadOnly(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Log("running as root: directory permissions do not apply; checking the fallback path directly")
	}
	dir := t.TempDir()
	cfg := filepath.Join(dir, "orders.yaml")
	if err := os.WriteFile(cfg, []byte("version: 1\n"), 0o660); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	old := pipelineLockFallbackDir
	pipelineLockFallbackDir = filepath.Join(t.TempDir(), "locks")
	t.Cleanup(func() { pipelineLockFallbackDir = old })

	unlock, err := lockPipelineFiles(cfg)
	if err != nil {
		t.Fatalf("lock with a read-only config directory: %v", err)
	}
	if os.Geteuid() != 0 {
		if _, err := os.Stat(cfg + ".strand.lock"); err == nil {
			t.Fatal("lock file was created in the read-only directory")
		}
		if entries, _ := os.ReadDir(pipelineLockFallbackDir); len(entries) != 1 {
			t.Fatalf("fallback lock files = %d, want 1", len(entries))
		}
	}
	acquired := make(chan func(), 1)
	go func() {
		u, err := lockPipelineFiles(cfg)
		if err != nil {
			t.Error(err)
			return
		}
		acquired <- u
	}()
	select {
	case <-acquired:
		t.Fatal("second lock acquired while the first is held")
	case <-time.After(200 * time.Millisecond):
	}
	unlock()
	select {
	case u := <-acquired:
		u()
	case <-time.After(2 * time.Second):
		t.Fatal("second lock not acquired after unlock")
	}
}
