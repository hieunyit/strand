package seed_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/seed"
	"strand/schema"
)

// helper tạo thư mục mock mydumper backup
func createMockMydumperBackup(t *testing.T, metadataContent string) string {
	t.Helper()
	dir := t.TempDir()
	if metadataContent == "" {
		metadataContent = "[source]\n# SOURCE_LOG_FILE = \"binlog.000001\"\n# SOURCE_LOG_POS = 1234\n# executed_gtid_set = \"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100\"\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "metadata"), []byte(metadataContent), 0o644); err != nil {
		t.Fatalf("failed to write mock metadata: %v", err)
	}
	return dir
}

// TestSD1_SeedPlan_Mydumper_LockMode kiểm tra quy tắc xác minh chế độ khóa cho mydumper (02 §9).
func TestSD1_SeedPlan_Mydumper_LockMode(t *testing.T) {
	ctx := context.Background()
	planner := seed.NewMydumperPlanner()

	const goodLog = `** Message: 21:07:26.100: Started dump at: 2026-11-02 21:07:26
** Message: 21:07:26.200: Connected to MySQL server
** Message: 21:07:26.300: Thread 1 dumping table app.orders
** Message: 21:07:26.400: Finished dump at: 2026-11-02 21:07:26
`

	const goodLogMainConnectionClosed = `** Message: 21:07:26.100: Started dump at: 2026-11-02 21:07:26
** Message: 21:07:26.300: Thread 1 dumping table app.orders
** Message: 21:07:26.500: Main connection closed
`

	const noLockLog = `** Message: 21:07:29.414: Executing in NO_LOCK mode, we are not able to ensure that backup will be consistent
** Message: 21:07:29.500: Thread 1 dumping table app.orders
** Message: 21:07:29.900: Finished dump at: 2026-11-02 21:07:29
`

	const incompleteLog = `** Message: 21:07:26.100: Started dump at: 2026-11-02 21:07:26
** Message: 21:07:26.200: Connected to MySQL server
`

	tests := []struct {
		name                 string
		logContent           string
		provideLog           bool
		acceptUnverifiedLock bool
		wantErr              bool
		errContains          string
		wantWarning          bool
		warningContains      string
	}{
		{
			name:                 "NoLog_NoFlag_Rejected",
			provideLog:           false,
			acceptUnverifiedLock: false,
			wantErr:              true,
			errContains:          "mydumper metadata does not record the lock mode",
		},
		{
			name:                 "LogWithNoLock_Rejected",
			logContent:           noLockLog,
			provideLog:           true,
			acceptUnverifiedLock: false,
			wantErr:              true,
			errContains:          "NO_LOCK",
		},
		{
			name:                 "LogWithoutCompletion_Rejected",
			logContent:           incompleteLog,
			provideLog:           true,
			acceptUnverifiedLock: false,
			wantErr:              true,
			errContains:          "mydumper log does not show a completed dump",
		},
		{
			name:                 "GoodLog_Accepted",
			logContent:           goodLog,
			provideLog:           true,
			acceptUnverifiedLock: false,
			wantErr:              false,
			wantWarning:          false,
		},
		{
			name:                 "GoodLog_MainConnectionClosed_Accepted",
			logContent:           goodLogMainConnectionClosed,
			provideLog:           true,
			acceptUnverifiedLock: false,
			wantErr:              false,
			wantWarning:          false,
		},
		{
			name:                 "NoLog_AcceptFlag_AcceptedWithWarning",
			provideLog:           false,
			acceptUnverifiedLock: true,
			wantErr:              false,
			wantWarning:          true,
			warningContains:      "WARNING: mydumper lock mode not verified",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backupDir := createMockMydumperBackup(t, "")
			var dumpLogPath string
			if tc.provideLog {
				dumpLogPath = filepath.Join(t.TempDir(), "mydumper.log")
				if err := os.WriteFile(dumpLogPath, []byte(tc.logContent), 0o644); err != nil {
					t.Fatalf("failed to write test dump log: %v", err)
				}
			}

			src := seed.SeedSource{
				Engine:                   "mysql",
				Kind:                     "mydumper",
				BackupRef:                backupDir,
				DumpLog:                  dumpLogPath,
				AcceptUnverifiedLockMode: tc.acceptUnverifiedLock,
			}

			manifest, err := planner.Plan(ctx, src)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.errContains)
				}
				if code := exitcode.GetExitCode(err); code != exitcode.Rejected {
					t.Fatalf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
				}
				if !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if manifest == nil {
				t.Fatalf("expected non-nil manifest")
			}

			if tc.wantWarning {
				if len(manifest.Warnings) == 0 {
					t.Fatalf("expected manifest warnings, got none")
				}
				found := false
				for _, w := range manifest.Warnings {
					if strings.Contains(w, tc.warningContains) {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("expected warning containing %q, got %v", tc.warningContains, manifest.Warnings)
				}
			} else {
				if len(manifest.Warnings) > 0 {
					t.Fatalf("expected no warnings, got %v", manifest.Warnings)
				}
			}

			// Kiểm tra hợp lệ theo seed.schema.json
			raw, err := json.Marshal(manifest)
			if err != nil {
				t.Fatalf("marshal manifest: %v", err)
			}
			schemaBytes, err := os.ReadFile("../../schema/seed.schema.json")
			if err != nil {
				t.Fatalf("read seed.schema.json: %v", err)
			}
			val, err := schema.NewSeedSchemaValidator(schemaBytes)
			if err != nil {
				t.Fatalf("validator: %v", err)
			}
			if err := val.Validate(raw); err != nil {
				t.Fatalf("validate manifest against schema: %v", err)
			}
		})
	}
}

// TestSD1_SeedPlan_Mydumper_LegacyMetadataNoLock_Rejected kiểm tra metadata mydumper bản cũ
// ghi chế độ NO_LOCK trực tiếp trong file metadata (spec 02 §9).
func TestSD1_SeedPlan_Mydumper_LegacyMetadataNoLock_Rejected(t *testing.T) {
	ctx := context.Background()
	planner := seed.NewMydumperPlanner()

	backupDir := createMockMydumperBackup(t, "[source]\nexecuted_gtid_set = \"3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100\"\nlock_mode: NO_LOCK\n")
	logPath := filepath.Join(t.TempDir(), "mydumper.log")
	_ = os.WriteFile(logPath, []byte("** Message: 21:07:26.400: Finished dump at: 2026-11-02 21:07:26\n"), 0o644)

	src := seed.SeedSource{
		Engine:    "mysql",
		Kind:      "mydumper",
		BackupRef: backupDir,
		DumpLog:   logPath,
	}

	_, err := planner.Plan(ctx, src)
	if err == nil {
		t.Fatalf("expected error for legacy metadata with NO_LOCK, got nil")
	}
	if code := exitcode.GetExitCode(err); code != exitcode.Rejected {
		t.Fatalf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
	}
	if !strings.Contains(err.Error(), "NO_LOCK") {
		t.Fatalf("expected NO_LOCK error, got %v", err)
	}
}
