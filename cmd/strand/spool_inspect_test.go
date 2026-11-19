package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/spool"
)

func TestSpoolInspectCmd_DryRunAndApply(t *testing.T) {
	temp := t.TempDir()
	dbDir := filepath.Join(temp, "db")
	trustedDir := filepath.Join(temp, "trusted_spill")
	if err := os.MkdirAll(trustedDir, 0700); err != nil {
		t.Fatal(err)
	}

	testFile := filepath.Join(trustedDir, "orphan.bin")
	if err := os.WriteFile(testFile, []byte(strings.Repeat("z", 1024)), 0600); err != nil {
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

	// 1. Chạy spool-inspect ở chế độ dry-run mặc định
	cmd := newSpoolInspectCmd()
	var outBuf bytes.Buffer
	cmd.SetOut(&outBuf)
	cmd.SetErr(&outBuf)
	cmd.SetArgs([]string{
		"--spool-dir", dbDir,
		"--trusted-root", trustedDir,
		"--json",
	})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("spool-inspect dry-run failed: %v", err)
	}

	// Kiểm tra xem database vẫn chưa bị thay đổi
	sp2, err := spool.New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	if sp2.Usage().LocalBytes != 0 {
		t.Fatalf("dry-run should not charge bytes: %d", sp2.Usage().LocalBytes)
	}
	if err = sp2.Close(); err != nil {
		t.Fatal(err)
	}

	// 2. Chạy spool-inspect với cờ --apply
	cmdApply := newSpoolInspectCmd()
	cmdApply.SetArgs([]string{
		"--spool-dir", dbDir,
		"--trusted-root", trustedDir,
		"--apply",
	})

	if err := cmdApply.Execute(); err != nil {
		t.Fatalf("spool-inspect --apply failed: %v", err)
	}

	// Kiểm tra xem database đã cập nhật accounting bền vững
	sp3, err := spool.New(dbDir)
	if err != nil {
		t.Fatal(err)
	}
	defer sp3.Close()

	if sp3.Usage().LocalBytes != 1024 {
		t.Fatalf("usage after --apply: %d, want 1024", sp3.Usage().LocalBytes)
	}
}
