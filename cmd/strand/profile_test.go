package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestProfile_Disabled(t *testing.T) {
	p, err := startProfiling(profileConfig{})
	if err != nil {
		t.Fatalf("expected nil error when profiling disabled, got: %v", err)
	}
	if p != nil {
		t.Fatalf("expected nil profiler when profiling disabled, got: %v", p)
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("expected nil error calling Stop on nil profiler, got: %v", err)
	}
}

func TestProfile_CPU(t *testing.T) {
	tmpDir := t.TempDir()
	cpuFile := filepath.Join(tmpDir, "cpu.pprof")

	p, err := startProfiling(profileConfig{
		cpuPath: cpuFile,
	})
	if err != nil {
		t.Fatalf("startProfiling failed: %v", err)
	}

	// Tạo một chút tải CPU để ghi nhận sample
	done := make(chan struct{})
	go func() {
		sum := 0
		for i := 0; i < 1_000_000; i++ {
			sum += i
		}
		_ = sum
		close(done)
	}()
	<-done
	time.Sleep(10 * time.Millisecond)

	if err := p.Stop(); err != nil {
		t.Fatalf("p.Stop() failed: %v", err)
	}

	info, err := os.Stat(cpuFile)
	if err != nil {
		t.Fatalf("stat cpu file failed: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected non-empty cpu profile file, got size 0")
	}
}

func TestProfile_Block(t *testing.T) {
	tmpDir := t.TempDir()
	blockFile := filepath.Join(tmpDir, "block.pprof")

	p, err := startProfiling(profileConfig{
		blockPath: blockFile,
		blockRate: 1, // sample all blocking events
	})
	if err != nil {
		t.Fatalf("startProfiling failed: %v", err)
	}

	// Tạo sự kiện blocking (khóa mutex)
	var mu sync.Mutex
	mu.Lock()
	go func() {
		time.Sleep(10 * time.Millisecond)
		mu.Unlock()
	}()
	time.Sleep(2 * time.Millisecond)
	mu.Lock()
	mu.Unlock()

	if err := p.Stop(); err != nil {
		t.Fatalf("p.Stop() failed: %v", err)
	}

	info, err := os.Stat(blockFile)
	if err != nil {
		t.Fatalf("stat block file failed: %v", err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected non-empty block profile file, got size 0")
	}
}

func TestProfile_NoUnexpectedOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	cpuFile := filepath.Join(tmpDir, "cpu.pprof")
	if err := os.WriteFile(cpuFile, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := startProfiling(profileConfig{
		cpuPath:        cpuFile,
		allowOverwrite: false,
	})
	if err == nil {
		t.Fatalf("expected error when file already exists and allowOverwrite is false")
	}

	// Thử tương tự với block profile
	blockFile := filepath.Join(tmpDir, "block.pprof")
	if err := os.WriteFile(blockFile, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = startProfiling(profileConfig{
		blockPath:      blockFile,
		allowOverwrite: false,
	})
	if err == nil {
		t.Fatalf("expected error when block profile file already exists and allowOverwrite is false")
	}
}

func TestProfile_AllowOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	cpuFile := filepath.Join(tmpDir, "cpu.pprof")
	if err := os.WriteFile(cpuFile, []byte("existing-data"), 0o644); err != nil {
		t.Fatal(err)
	}

	p, err := startProfiling(profileConfig{
		cpuPath:        cpuFile,
		allowOverwrite: true,
	})
	if err != nil {
		t.Fatalf("expected success with allowOverwrite=true, got: %v", err)
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	info, err := os.Stat(cpuFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatalf("expected non-empty cpu profile file after overwrite")
	}
}

func TestProfile_InvalidPath(t *testing.T) {
	invalidPath := filepath.Join(t.TempDir(), "non-existent-dir", "sub", "cpu.pprof")
	_, err := startProfiling(profileConfig{
		cpuPath: invalidPath,
	})
	if err == nil {
		t.Fatalf("expected error for non-existent directory path")
	}
}

func TestProfile_StopIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	cpuFile := filepath.Join(tmpDir, "cpu.pprof")
	p, err := startProfiling(profileConfig{
		cpuPath: cpuFile,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := p.Stop(); err != nil {
		t.Fatalf("first Stop failed: %v", err)
	}
	// Gọi Stop lần 2 phải không lỗi
	if err := p.Stop(); err != nil {
		t.Fatalf("second Stop failed: %v", err)
	}
}

func TestProfile_PartialFailureCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	cpuFile := filepath.Join(tmpDir, "cpu.pprof")
	invalidBlock := filepath.Join(tmpDir, "nonexistent", "block.pprof")

	_, err := startProfiling(profileConfig{
		cpuPath:   cpuFile,
		blockPath: invalidBlock,
	})
	if err == nil {
		t.Fatalf("expected error when block profile path is invalid")
	}

	// Đảm bảo CPU profile đã được dừng sau khi block profile thất bại
	// Thử bật lại CPU profile mới để chứng minh không bị xung đột pprof
	newCPU := filepath.Join(tmpDir, "cpu2.pprof")
	p2, err2 := startProfiling(profileConfig{
		cpuPath: newCPU,
	})
	if err2 != nil {
		t.Fatalf("failed to start new cpu profile after cleanup: %v", err2)
	}
	_ = p2.Stop()
}

func TestProfile_Permissions0600(t *testing.T) {
	tmpDir := t.TempDir()
	cpuFile := filepath.Join(tmpDir, "cpu.pprof")
	blockFile := filepath.Join(tmpDir, "block.pprof")

	p, err := startProfiling(profileConfig{
		cpuPath:   cpuFile,
		blockPath: blockFile,
	})
	if err != nil {
		t.Fatalf("startProfiling failed: %v", err)
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("p.Stop() failed: %v", err)
	}

	fiCPU, err := os.Stat(cpuFile)
	if err != nil {
		t.Fatalf("stat cpu file failed: %v", err)
	}
	if perm := fiCPU.Mode().Perm(); perm != 0o600 {
		t.Fatalf("expected cpu profile permissions 0600, got %04o", perm)
	}

	fiBlock, err := os.Stat(blockFile)
	if err != nil {
		t.Fatalf("stat block file failed: %v", err)
	}
	if perm := fiBlock.Mode().Perm(); perm != 0o600 {
		t.Fatalf("expected block profile permissions 0600, got %04o", perm)
	}
}

func TestProfile_ConflictSamePath(t *testing.T) {
	tmpDir := t.TempDir()
	shared := filepath.Join(tmpDir, "shared.pprof")

	_, err := startProfiling(profileConfig{
		cpuPath:        shared,
		blockPath:      shared,
		allowOverwrite: true,
	})
	if err == nil {
		t.Fatalf("expected error when cpu and block profile use identical path")
	}
}

func TestProfile_ConflictCleanPath(t *testing.T) {
	tmpDir := t.TempDir()
	path1 := filepath.Join(tmpDir, "prof.pprof")
	path2 := filepath.Join(tmpDir, ".", "prof.pprof")

	_, err := startProfiling(profileConfig{
		cpuPath:        path1,
		blockPath:      path2,
		allowOverwrite: true,
	})
	if err == nil {
		t.Fatalf("expected error when cpu and block profile resolve to same clean path")
	}
}

func TestProfile_ConflictHardlink(t *testing.T) {
	tmpDir := t.TempDir()
	path1 := filepath.Join(tmpDir, "target.pprof")
	if err := os.WriteFile(path1, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	path2 := filepath.Join(tmpDir, "hardlink.pprof")
	if err := os.Link(path1, path2); err != nil {
		t.Skipf("hardlink not supported: %v", err)
	}

	_, err := startProfiling(profileConfig{
		cpuPath:        path1,
		blockPath:      path2,
		allowOverwrite: true,
	})
	if err == nil {
		t.Fatalf("expected error when cpu and block profile point to same inode (hardlink)")
	}
}

func TestProfile_ConflictSymlink(t *testing.T) {
	tmpDir := t.TempDir()
	path1 := filepath.Join(tmpDir, "real.pprof")
	if err := os.WriteFile(path1, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	path2 := filepath.Join(tmpDir, "symlink.pprof")
	if err := os.Symlink(path1, path2); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	_, err := startProfiling(profileConfig{
		cpuPath:        path1,
		blockPath:      path2,
		allowOverwrite: true,
	})
	if err == nil {
		t.Fatalf("expected error when cpu and block profile resolve to same symlink target")
	}
}

func TestProfile_StopErrorIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	cpuFile := filepath.Join(tmpDir, "cpu.pprof")

	p, err := startProfiling(profileConfig{
		cpuPath: cpuFile,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Đóng file sớm để kích hoạt lỗi khi profiler đóng file trong Stop()
	_ = p.cpuFile.Close()

	err1 := p.Stop()
	if err1 == nil {
		t.Fatalf("expected error from first Stop() due to pre-closed file")
	}

	err2 := p.Stop()
	if err2 == nil {
		t.Fatalf("expected error from second Stop() to match first error, got nil")
	}
	if err1.Error() != err2.Error() {
		t.Fatalf("expected identical error on subsequent Stop(), got %v vs %v", err1, err2)
	}
}

func TestProfile_BackgroundFlagsPreserved(t *testing.T) {
	args := []string{
		"sync",
		"-c", "strand.yaml",
		"--background",
		"-d",
		"--cpu-profile", "/tmp/cpu.pprof",
		"--block-profile", "/tmp/block.pprof",
		"--block-profile-rate", "10",
		"--profile-overwrite",
	}

	child := childArgs(args)
	// Đảm bảo --background và -d bị loại trừ
	for _, a := range child {
		if a == "--background" || a == "-d" {
			t.Fatalf("expected background flag removed, found: %s", a)
		}
	}

	// Đảm bảo toàn bộ cờ profiling được giữ nguyên
	expected := []string{
		"--cpu-profile", "/tmp/cpu.pprof",
		"--block-profile", "/tmp/block.pprof",
		"--block-profile-rate", "10",
		"--profile-overwrite",
	}
	for _, exp := range expected {
		found := false
		for _, a := range child {
			if a == exp {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected flag %s preserved in childArgs, got: %v", exp, child)
		}
	}
}
