package main

import (
	"errors"
	"path/filepath"
	"testing"

	"strand/internal/exitcode"
)

// review R08: `strand sync --dry-run` từng chạy pipeline thật. Phải bị từ chối (exit 2) trước khi
// đọc cấu hình/chạm DSN: đường dẫn cấu hình không tồn tại chứng minh không có gì được nạp.
func TestR08_SyncDryRunRejected(t *testing.T) {
	t.Cleanup(func() {
		dryRun, cfgPath = false, ""
		rootCmd.SetArgs(nil)
	})
	rootCmd.SetArgs([]string{"sync", "--dry-run", "-c", filepath.Join(t.TempDir(), "missing.yaml")})
	err := rootCmd.Execute()
	var se *exitcode.Error
	if !errors.As(err, &se) || se.Code != "dry_run_unsupported" || se.ExitCode != exitcode.InvalidSyntax {
		t.Fatalf("want dry_run_unsupported (exit 2), got %v", err)
	}
}
