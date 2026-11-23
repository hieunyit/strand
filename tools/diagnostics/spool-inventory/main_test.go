package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/spool"
)

func TestSpoolInventoryTool_DryRunAndApply(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	testFile := filepath.Join(trustedDir, "tool_orphan.bin")
	if err := os.WriteFile(testFile, []byte(strings.Repeat("w", 512)), 0600); err != nil {
		t.Fatal(err)
	}

	// Tạo Pebble database ban đầu
	sp, err := spool.New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = sp.Close(); err != nil {
		t.Fatal(err)
	}

	// Build binary tạm thời
	binPath := filepath.Join(temp, "spool-inventory")
	cmdBuild := exec.Command("go", "build", "-o", binPath, ".")
	if out, err := cmdBuild.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v, output: %s", err, string(out))
	}

	// 1. Dry run
	cmdDry := exec.Command(binPath, "-spool-dir", dbDir, "-trusted-root", trustedDir, "-json")
	var stdoutDry bytes.Buffer
	cmdDry.Stdout = &stdoutDry
	if err := cmdDry.Run(); err != nil {
		t.Fatalf("dry run execution failed: %v", err)
	}

	if !strings.Contains(stdoutDry.String(), `"discovered_files": 1`) {
		t.Fatalf("dry run output unexpected: %s", stdoutDry.String())
	}

	// Kiểm tra xem db chưa bị ghi nhận
	sp2, err := spool.New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	if sp2.Usage().LocalBytes != 0 {
		t.Fatalf("usage before apply: %d, want 0", sp2.Usage().LocalBytes)
	}
	if err = sp2.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Apply
	cmdApply := exec.Command(binPath, "-spool-dir", dbDir, "-trusted-root", trustedDir, "-apply", "-json")
	var stdoutApply bytes.Buffer
	cmdApply.Stdout = &stdoutApply
	if err := cmdApply.Run(); err != nil {
		t.Fatalf("apply execution failed: %v", err)
	}

	if !strings.Contains(stdoutApply.String(), `"imported_files": 1`) {
		t.Fatalf("apply output unexpected: %s", stdoutApply.String())
	}

	// Kiểm tra xem db đã được cập nhật
	sp3, err := spool.New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sp3.Close()

	if sp3.Usage().LocalBytes != 512 {
		t.Fatalf("usage after apply: %d, want 512", sp3.Usage().LocalBytes)
	}
}
