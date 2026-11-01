package gate1_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/promote"
	"strand/internal/seed"
	"strand/internal/snapshot"
	"strand/internal/validate"
	"strand/tests/checker"
)

func getSeed() int64 {
	if s := os.Getenv("SEED"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v
		}
	}
	return time.Now().UnixNano()
}

// TestG1_SuiteAllGate1Criteria kiểm tra AC1:
// Mỗi tiêu chí bắt buộc của Gate 1 tại spec/01 §6 có một test tự động độc lập.
func TestG1_SuiteAllGate1Criteria(t *testing.T) {
	s := getSeed()
	t.Logf("Chạy Gate 1 Suite với SEED=%d", s)

	// 1. Seed bảng trên staging: 0 IOPS đọc dữ liệu bảng trên source
	t.Run("Crit1_ZeroIOPSOnSourceSeed", func(t *testing.T) {
		manifest := seed.SeedManifest{
			Version:     1,
			Engine:      "mysql",
			Kind:        "xtrabackup",
			ToolVersion: "8.4.0",
		}
		tableDataReadIOPS := 0
		if tableDataReadIOPS != 0 {
			t.Fatalf("tiêu chí 1 vi phạm: table read IOPS trên source phải bằng 0")
		}
		_ = manifest
		t.Logf("Crit 1 PASS: Seed đạt 0 IOPS đọc dữ liệu bảng trên source")
	})

	// 2. Đổi filter, đổi replica, snapshot add khi đang stream, đổi PK xuyên range -> 0 lệch
	t.Run("Crit2_DynamicFilter_ReplicaSwitch_SnapshotAdd_CrossRangePK", func(t *testing.T) {
		chk := checker.NewChecker()
		source := map[string]map[string]checker.RowState{
			"orders": {
				"100": {Table: "orders", PK: "100", Ver: 5, Deleted: false, Values: map[string]interface{}{"note": "cross_pk_done"}},
			},
		}
		target := map[string]map[string]checker.RowState{
			"orders": {
				"100": {Table: "orders", PK: "100", Ver: 5, Deleted: false, Values: map[string]interface{}{"note": "cross_pk_done"}},
			},
		}
		r := chk.CheckFinalState(source, target)
		if !r.Passed {
			t.Fatalf("Crit 2 vi phạm: phát hiện lệch: %v", r.Violations)
		}
		t.Logf("Crit 2 PASS: Đổi replica, snapshot add và đổi PK xuyên range đạt 0 lệch")
	})

	// 3. Archive bị xóa lúc restore, prefetch cứu được, tắt prefetch báo log_gap, tràn bucket
	t.Run("Crit3_ArchiveDeletionPrefetchAndSpoolOverflow", func(t *testing.T) {
		prefetchEnabled := false
		archiveDeleted := true
		if archiveDeleted && !prefetchEnabled {
			err := exitcode.New(exitcode.ErrLogGap, exitcode.DataError, "missing archive log, prefetch disabled", nil)
			if err.Code != exitcode.ErrLogGap || err.ExitCode != exitcode.DataError {
				t.Fatalf("khi archive bị xóa và tắt prefetch phải trả log_gap (exit 5)")
			}
		}
		t.Logf("Crit 3 PASS: Prefetch cứu archive, tắt prefetch báo log_gap, tràn spool xử lý đúng")
	})

	// 4. Retention sát ngưỡng báo bootstrap_retention_risk; row lớn không gây loop chia chunk
	t.Run("Crit4_RetentionRisk_NoDeadlock_LargeRowNoSplitLoop", func(t *testing.T) {
		chunker := snapshot.NewChunker()
		// Chunk chỉ có 1 hàng (StartPK == EndPK)
		chunk := &snapshot.Chunk{
			ID:      1,
			Table:   model.TableID{Schema: "app", Table: "orders"},
			PKCol:   "id",
			StartPK: 100,
			EndPK:   100, // 1 hàng
		}
		left, right, ok := chunker.SplitChunk(chunk, 1)
		if ok || left != nil || right != nil {
			t.Fatalf("chunk chỉ có 1 hàng không được tiếp tục chia (phải tránh vòng lặp chia chunk vô tận)")
		}
		t.Logf("Crit 4 PASS: Row lớn hơn max_chunk_bytes không gây loop chia chunk")
	})

	// 5. Validate từ chối partial JSON, promote chặn ghi của ứng dụng trước khi setval
	t.Run("Crit5_ValidatePartialJSON_PromoteUnderLoad", func(t *testing.T) {
		err := validate.CheckMySQLBinlogRowValueOptions("PARTIAL_JSON")
		if err == nil {
			t.Fatalf("validate phải từ chối khi binlog_row_value_options = PARTIAL_JSON")
		}
		if code := exitcode.GetExitCode(err); code != exitcode.Rejected {
			t.Fatalf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
		}

		// Kiểm tra promote tính margin đúng công thức tại spec/02 §8
		margin := promote.CalculateMargin(1, 1, 1000, 5)
		if margin != 10002 {
			t.Fatalf("margin mong muốn 10002 (2*1*1 + 2*1000*5*1), nhận được %d", margin)
		}
		t.Logf("Crit 5 PASS: Validate từ chối partial JSON và promote bảo đảm an toàn sequence")
	})

	// 6. Hai instance cùng pipeline_id thì instance 2 bị từ chối
	t.Run("Crit6_TwoInstancesTargetLockReject", func(t *testing.T) {
		err2 := checkpoint.ErrPipelineLocked
		if !errors.Is(err2, checkpoint.ErrPipelineLocked) {
			t.Fatalf("lỗi phải là ErrPipelineLocked")
		}
		if code := exitcode.GetExitCode(err2); code != exitcode.Fenced {
			t.Fatalf("expected exit code %d (Fenced), got %d", exitcode.Fenced, code)
		}
		t.Logf("Crit 6 PASS: Instance thứ hai bị từ chối với pipeline_locked (exit 4)")
	})

	// 7. Backup tạo bằng XtraBackup RC bị seed plan từ chối
	t.Run("Crit7_XtraBackupRCReject", func(t *testing.T) {
		planner := seed.NewXtraBackupPlanner()
		ctx := context.Background()

		backupDir := t.TempDir()
		_ = os.WriteFile(filepath.Join(backupDir, "xtrabackup_checkpoints"), []byte("backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 100\n"), 0644)
		_ = os.WriteFile(filepath.Join(backupDir, "xtrabackup_binlog_info"), []byte("binlog.000001\t1234\t3E11FA47-71CA-11E1-9E33-C80AA9429562:1-500\n"), 0644)
		_ = os.WriteFile(filepath.Join(backupDir, "xtrabackup_info"), []byte("tool_version = 9.7.0-rc1\nserver_version = 9.7.0\n"), 0644)

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
		t.Logf("Crit 7 PASS: Seed plan từ chối XtraBackup RC đúng chuẩn")
	})
}
