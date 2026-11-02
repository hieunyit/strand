package seed_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/seed"
	"strand/schema"
)

// helper tạo thư mục backup XtraBackup mẫu
func createMockXtraBackup(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write mock file %s: %v", name, err)
		}
	}
	return dir
}

// TestSD1_SeedPlan_XtraBackup_MetadataCrossCheck kiểm tra AC1 (P1-12):
// seed plan đọc P từ metadata của backup, đối chiếu chéo với target đã restore (SD-1).
func TestSD1_SeedPlan_XtraBackup_MetadataCrossCheck(t *testing.T) {
	gtid := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-982341"
	backupDir := createMockXtraBackup(t, map[string]string{
		"xtrabackup_checkpoints": "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 12345678\nlast_lsn = 12345678\n",
		"xtrabackup_binlog_info": "binlog.000001\t1234\t" + gtid + "\n",
		"xtrabackup_info":        "tool_version = 8.4.0-6\nserver_version = 8.4.0\nserver_uuid = 3E11FA47-71CA-11E1-9E33-C80AA9429562\n",
		"schema.sql":             "CREATE TABLE app.orders (id INT PRIMARY KEY);",
	})

	planner := seed.NewXtraBackupPlanner()
	ctx := context.Background()

	// 1. Trường hợp thành công: restored_gtid_executed khớp chính xác với P
	src := seed.SeedSource{
		Engine:             "mysql",
		Kind:               "xtrabackup",
		BackupRef:          backupDir,
		Tables:             []string{"app.orders", "app.users"},
		TargetVersion:      "8.4.6",
		TargetRestoredGTID: gtid,
	}

	manifest, err := planner.Plan(ctx, src)
	if err != nil {
		t.Fatalf("planner.Plan failed: %v", err)
	}

	if manifest.StartPosition.GTIDSet != gtid {
		t.Fatalf("expected StartPosition %s, got %s", gtid, manifest.StartPosition.GTIDSet)
	}
	if manifest.PositionCrossCheck == nil || !manifest.PositionCrossCheck.Match {
		t.Fatalf("expected PositionCrossCheck.Match == true, got %v", manifest.PositionCrossCheck)
	}
	if manifest.ToolPrerelease {
		t.Fatalf("expected ToolPrerelease == false for GA tool version 8.4.0-6")
	}

	// Xác thực manifest xuất ra khớp JSON Schema seed.schema.json
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest failed: %v", err)
	}

	schemaBytes, err := os.ReadFile("../../schema/seed.schema.json")
	if err != nil {
		t.Fatalf("read seed.schema.json failed: %v", err)
	}
	validator, err := schema.NewSeedSchemaValidator(schemaBytes)
	if err != nil {
		t.Fatalf("create validator failed: %v", err)
	}
	if err := validator.Validate(manifestJSON); err != nil {
		t.Fatalf("manifest failed seed.schema.json validation: %v", err)
	}
	t.Logf("PASS: manifest matches seed.schema.json perfectly")

	// 2. Trường hợp lệch vị trí: restored_gtid_executed khác P -> Bị từ chối (SD-1)
	srcMismatch := seed.SeedSource{
		Engine:             "mysql",
		Kind:               "xtrabackup",
		BackupRef:          backupDir,
		Tables:             []string{"app.orders"},
		TargetVersion:      "8.4.6",
		TargetRestoredGTID: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-900000", // Lệch vị trí
	}

	_, errMismatch := planner.Plan(ctx, srcMismatch)
	if errMismatch == nil {
		t.Fatalf("expected error on position cross-check mismatch, got nil")
	}
	if code := exitcode.GetExitCode(errMismatch); code != exitcode.Rejected {
		t.Fatalf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
	}
	t.Logf("PASS AC1: seed plan reads P from metadata and cross-checks successfully; rejects mismatch")
}

// TestSD2_CorruptedBackup_Rejected kiểm tra AC2 (P1-12):
// Backup hỏng (thiếu file, sai GTID, log có khoảng trống) bị từ chối 100%.
func TestSD2_CorruptedBackup_Rejected(t *testing.T) {
	planner := seed.NewXtraBackupPlanner()
	ctx := context.Background()

	// 1. Thiếu file xtrabackup_checkpoints
	t.Run("MissingCheckpoints", func(t *testing.T) {
		dir := createMockXtraBackup(t, map[string]string{
			"xtrabackup_binlog_info": "binlog.000001\t1234\tuuid:1-100\n",
		})
		_, err := planner.Plan(ctx, seed.SeedSource{BackupRef: dir})
		if err == nil {
			t.Fatalf("expected error for missing xtrabackup_checkpoints, got nil")
		}
		if code := exitcode.GetExitCode(err); code != exitcode.Rejected {
			t.Fatalf("expected exit code %d, got %d", exitcode.Rejected, code)
		}
	})

	// 2. Thiếu file xtrabackup_binlog_info
	t.Run("MissingBinlogInfo", func(t *testing.T) {
		dir := createMockXtraBackup(t, map[string]string{
			"xtrabackup_checkpoints": "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 100\n",
		})
		_, err := planner.Plan(ctx, seed.SeedSource{BackupRef: dir})
		if err == nil {
			t.Fatalf("expected error for missing xtrabackup_binlog_info, got nil")
		}
		if code := exitcode.GetExitCode(err); code != exitcode.Rejected {
			t.Fatalf("expected exit code %d, got %d", exitcode.Rejected, code)
		}
	})

	// 3. Sai định dạng GTID (thiếu trường GTID trong xtrabackup_binlog_info)
	t.Run("InvalidGTIDFormat", func(t *testing.T) {
		dir := createMockXtraBackup(t, map[string]string{
			"xtrabackup_checkpoints": "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 100\n",
			"xtrabackup_binlog_info": "binlog.000001\t1234\n", // Thiếu gtid_set
		})
		_, err := planner.Plan(ctx, seed.SeedSource{BackupRef: dir})
		if err == nil {
			t.Fatalf("expected error for invalid GTID in xtrabackup_binlog_info, got nil")
		}
		if code := exitcode.GetExitCode(err); code != exitcode.Rejected {
			t.Fatalf("expected exit code %d, got %d", exitcode.Rejected, code)
		}
	})

	// 4. GTID có khoảng trống (holes/gap) trong backup position
	t.Run("GTIDWithHoles", func(t *testing.T) {
		dir := createMockXtraBackup(t, map[string]string{
			"xtrabackup_checkpoints": "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 100\n",
			// Có lỗ hổng: 1-50 và 60-100 (thiếu 51-59)
			"xtrabackup_binlog_info": "binlog.000001\t1234\t3E11FA47-71CA-11E1-9E33-C80AA9429562:1-50:60-100\n",
		})
		_, err := planner.Plan(ctx, seed.SeedSource{BackupRef: dir})
		if err == nil {
			t.Fatalf("expected error for GTID with holes, got nil")
		}
		if code := exitcode.GetExitCode(err); code != exitcode.DataError {
			t.Fatalf("expected exit code %d (DataError / log_gap), got %d", exitcode.DataError, code)
		}
	})

	// 5. Source đã purge vượt quá P và không có archive (log_gap)
	t.Run("LogGapSourcePurgedWithoutArchive", func(t *testing.T) {
		dir := createMockXtraBackup(t, map[string]string{
			"xtrabackup_checkpoints": "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 100\n",
			"xtrabackup_binlog_info": "binlog.000001\t1234\t3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000\n",
		})
		// Source đã purge đến 2000 (vượt quá P=1000) và không có archive
		src := seed.SeedSource{
			BackupRef:        dir,
			SourcePurgedGTID: "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2000",
			ArchiveUntilGTID: "",
		}
		_, err := planner.Plan(ctx, src)
		if err == nil {
			t.Fatalf("expected log_gap error when source has purged past P, got nil")
		}
		if code := exitcode.GetExitCode(err); code != exitcode.DataError {
			t.Fatalf("expected exit code %d (DataError), got %d", exitcode.DataError, code)
		}
	})

	t.Logf("PASS AC2: Corrupted backup (missing files, invalid GTID, holes, log gap) rejected 100%%")
}

// TestSD4_PrereleaseTool_RejectedWithoutFlag kiểm tra AC3 (P1-12):
// Backup XtraBackup bản RC bị từ chối nếu không có --allow-prerelease-tool.
func TestSD4_PrereleaseTool_RejectedWithoutFlag(t *testing.T) {
	planner := seed.NewXtraBackupPlanner()
	ctx := context.Background()

	// Bản backup tạo bằng XtraBackup 9.7 bản RC
	backupDir := createMockXtraBackup(t, map[string]string{
		"xtrabackup_checkpoints": "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 100\n",
		"xtrabackup_binlog_info": "binlog.000001\t1234\t3E11FA47-71CA-11E1-9E33-C80AA9429562:1-500\n",
		"xtrabackup_info":        "tool_version = 9.7.0-rc1\nserver_version = 9.7.0\n",
	})

	// 1. Không có cờ --allow-prerelease-tool -> BỊ TỪ CHỐI 100%
	srcWithoutFlag := seed.SeedSource{
		Engine:              "mysql",
		Kind:                "xtrabackup",
		BackupRef:           backupDir,
		AllowPrereleaseTool: false,
	}

	_, err := planner.Plan(ctx, srcWithoutFlag)
	if err == nil {
		t.Fatalf("expected error for RC tool without --allow-prerelease-tool, got nil")
	}
	if code := exitcode.GetExitCode(err); code != exitcode.Rejected {
		t.Fatalf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
	}
	t.Logf("PASS: RC tool rejected without flag: %v", err)

	// 2. Có cờ --allow-prerelease-tool -> ĐƯỢC CHẤP NHẬN và đánh dấu tool_prerelease = true
	srcWithFlag := seed.SeedSource{
		Engine:              "mysql",
		Kind:                "xtrabackup",
		BackupRef:           backupDir,
		AllowPrereleaseTool: true,
		TargetVersion:       "9.7.1", // SD-4: target version is required to prove compatibility
	}

	manifest, err := planner.Plan(ctx, srcWithFlag)
	if err != nil {
		t.Fatalf("expected success with --allow-prerelease-tool, got err: %v", err)
	}
	if !manifest.ToolPrerelease {
		t.Fatalf("expected ToolPrerelease == true, got false")
	}
	if manifest.ToolVersion != "9.7.0-rc1" {
		t.Fatalf("expected ToolVersion 9.7.0-rc1, got %s", manifest.ToolVersion)
	}

	t.Logf("PASS AC3: RC tool rejected without flag, accepted when --allow-prerelease-tool is passed")
}
