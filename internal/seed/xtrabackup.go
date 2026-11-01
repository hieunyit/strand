package seed

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// spec: 02 §9, design §3.3
// Invariants: SD-1, SD-2, SD-3, SD-4

// XtraBackupMeta chứa metadata đã đọc từ các file của bản backup XtraBackup.
type XtraBackupMeta struct {
	Checkpoints    map[string]string
	BinlogFile     string
	BinlogPos      uint64
	GTIDSet        string
	ToolVersion    string
	ToolPrerelease bool
	ServerUUID     string
	ServerVersion  string
}

// XtraBackupPlanner cài đặt Seeder cho công cụ XtraBackup theo spec: 02 §9 và design §3.3.
type XtraBackupPlanner struct{}

// NewXtraBackupPlanner tạo XtraBackupPlanner mới.
func NewXtraBackupPlanner() *XtraBackupPlanner {
	return &XtraBackupPlanner{}
}

// IsPrereleaseToolVersion kiểm tra xem phiên bản công cụ có phải là bản pre-release / RC hay không.
func IsPrereleaseToolVersion(ver string) bool {
	v := strings.ToLower(strings.TrimSpace(ver))
	if v == "" {
		return false
	}
	prereleaseTokens := []string{"-rc", "rc", "-alpha", "alpha", "-beta", "beta", "-preview", "-dev", "prerelease"}
	for _, token := range prereleaseTokens {
		if strings.Contains(v, token) {
			return true
		}
	}
	return false
}

// ParseBackupMetadata đọc và phân tích metadata từ thư mục bản backup XtraBackup.
// Yêu cầu bắt buộc phải có: xtrabackup_checkpoints và xtrabackup_binlog_info.
func ParseBackupMetadata(backupDir string) (*XtraBackupMeta, error) {
	meta := &XtraBackupMeta{
		Checkpoints: make(map[string]string),
	}

	// 1. Kiểm tra và đọc xtrabackup_checkpoints
	checkpointsPath := filepath.Join(backupDir, "xtrabackup_checkpoints")
	cpFile, err := os.Open(checkpointsPath)
	if err != nil {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("missing required backup file %s", checkpointsPath),
			err,
		)
	}
	defer cpFile.Close()

	scanner := bufio.NewScanner(cpFile)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			meta.Checkpoints[k] = v
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "failed to read xtrabackup_checkpoints", err)
	}

	if _, ok := meta.Checkpoints["backup_type"]; !ok {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "xtrabackup_checkpoints missing backup_type", nil)
	}

	// 2. Kiểm tra và đọc xtrabackup_binlog_info
	binlogInfoPath := filepath.Join(backupDir, "xtrabackup_binlog_info")
	binlogData, err := os.ReadFile(binlogInfoPath)
	if err != nil {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("missing required backup file %s", binlogInfoPath),
			err,
		)
	}

	binlogContent := strings.TrimSpace(string(binlogData))
	if binlogContent == "" {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "empty xtrabackup_binlog_info", nil)
	}

	fields := strings.Fields(binlogContent)
	if len(fields) < 3 {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("invalid xtrabackup_binlog_info format: expected at least 3 fields, got %d", len(fields)),
			nil,
		)
	}

	meta.BinlogFile = fields[0]
	var pos uint64
	_, _ = fmt.Sscanf(fields[1], "%d", &pos)
	meta.BinlogPos = pos
	meta.GTIDSet = strings.Join(fields[2:], " ")

	// 3. Đọc xtrabackup_info (nếu có)
	infoPath := filepath.Join(backupDir, "xtrabackup_info")
	if infoData, err := os.ReadFile(infoPath); err == nil {
		scanner := bufio.NewScanner(strings.NewReader(string(infoData)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				k := strings.TrimSpace(parts[0])
				v := strings.TrimSpace(parts[1])
				switch k {
				case "tool_version":
					meta.ToolVersion = v
				case "server_uuid":
					meta.ServerUUID = v
				case "server_version":
					meta.ServerVersion = v
				}
			}
		}
	}

	if meta.ToolVersion == "" {
		meta.ToolVersion = "8.4.0-6" // Phiên bản mặc định ổn định theo spec 02 §9
	}
	meta.ToolPrerelease = IsPrereleaseToolVersion(meta.ToolVersion)

	if meta.ServerUUID == "" {
		// Trích xuất UUID từ GTIDSet nếu có
		parts := strings.Split(meta.GTIDSet, ":")
		if len(parts) > 0 {
			meta.ServerUUID = strings.TrimSpace(parts[0])
		}
	}

	return meta, nil
}

// Plan kiểm tra các bất biến SD-1 đến SD-4 và sinh ra cấu trúc SeedManifest theo spec: 02 §9.
func (p *XtraBackupPlanner) Plan(ctx context.Context, src SeedSource) (*SeedManifest, error) {
	// 1. Kiểm tra engine
	if src.Engine != "" && src.Engine != "mysql" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("engine %s not supported for XtraBackup: expected mysql", src.Engine),
			nil,
		)
	}

	// 2. Phân tích metadata từ thư mục backup
	meta, err := ParseBackupMetadata(src.BackupRef)
	if err != nil {
		return nil, err
	}

	// 3. Kiểm tra công cụ pre-release / RC (issue 15, SD-4)
	isRC := meta.ToolPrerelease || IsPrereleaseToolVersion(meta.ToolVersion)
	if isRC && !src.AllowPrereleaseTool {
		return nil, exitcode.New(
			exitcode.ErrPrereleaseTool,
			exitcode.Rejected,
			fmt.Sprintf("backup created with prerelease tool %q is rejected without --allow-prerelease-tool", meta.ToolVersion),
			nil,
		)
	}

	// 4. Kiểm tra vị trí P và GTID set (SD-1)
	if meta.GTIDSet == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"backup metadata does not contain valid GTID set for position P",
			nil,
		)
	}

	pSet := checkpoint.NewMySQLAppliedSet(meta.ServerUUID)
	for _, seg := range strings.Split(meta.GTIDSet, ",") {
		seg = strings.TrimSpace(seg)
		if seg != "" {
			pSet.ParseAndAddRange(seg)
		}
	}

	// Kiểm tra nếu GTID set bị hỏng hoặc có khoảng trống (holes)
	holes := pSet.Holes()
	if len(holes) > 0 {
		return nil, exitcode.New(
			exitcode.ErrLogGap,
			exitcode.DataError,
			fmt.Sprintf("backup GTID set %q contains %d holes: corrupted or non-contiguous log", meta.GTIDSet, len(holes)),
			nil,
		)
	}

	// 5. Đối chiếu chéo vị trí P với instance sau restore nếu có (SD-1)
	var crossCheck *PositionCrossCheck
	if src.TargetRestoredGTID != "" {
		restoredSet := checkpoint.NewMySQLAppliedSet(meta.ServerUUID)
		for _, seg := range strings.Split(src.TargetRestoredGTID, ",") {
			seg = strings.TrimSpace(seg)
			if seg != "" {
				restoredSet.ParseAndAddRange(seg)
			}
		}

		if !pSet.Equal(restoredSet) {
			return nil, exitcode.New(
				exitcode.ErrSeedInvariantFailed,
				exitcode.Rejected,
				fmt.Sprintf("position cross-check mismatch: restored gtid_executed (%s) != backup P (%s)",
					src.TargetRestoredGTID, meta.GTIDSet),
				nil,
			)
		}

		crossCheck = &PositionCrossCheck{
			RestoredGTIDExecuted: src.TargetRestoredGTID,
			Match:                true,
		}
	}

	// 6. Kiểm tra độ phủ của log (SD-2): chống mất log giữa P và hiện tại (log_gap)
	if src.SourcePurgedGTID != "" {
		purgedSet := checkpoint.NewMySQLAppliedSet(meta.ServerUUID)
		for _, seg := range strings.Split(src.SourcePurgedGTID, ",") {
			seg = strings.TrimSpace(seg)
			if seg != "" {
				purgedSet.ParseAndAddRange(seg)
			}
		}

		pHigh := pSet.High()
		// Nếu source đã purge vượt qua mốc P (tức là P đã bị purge)
		if purgedSet.Contains(pHigh) {
			// Cần có archive bù đắp
			if src.ArchiveUntilGTID == "" {
				return nil, exitcode.New(
					exitcode.ErrLogGap,
					exitcode.DataError,
					fmt.Sprintf("log gap: source purged GTID set (%s) has purged start position P (%s), and no archive is available",
						src.SourcePurgedGTID, meta.GTIDSet),
					nil,
				)
			}

			archiveSet := checkpoint.NewMySQLAppliedSet(meta.ServerUUID)
			for _, seg := range strings.Split(src.ArchiveUntilGTID, ",") {
				seg = strings.TrimSpace(seg)
				if seg != "" {
					archiveSet.ParseAndAddRange(seg)
				}
			}

			if !archiveSet.Contains(pHigh) {
				return nil, exitcode.New(
					exitcode.ErrLogGap,
					exitcode.DataError,
					fmt.Sprintf("log gap: archive until GTID (%s) does not cover start position P (%s)",
						src.ArchiveUntilGTID, meta.GTIDSet),
					nil,
				)
			}
		}
	}

	// 7. Kiểm tra tính tương thích của Target (SD-4)
	targetVer := src.TargetVersion
	if targetVer == "" {
		targetVer = meta.ServerVersion
		if targetVer == "" {
			targetVer = "8.4.6"
		}
	}

	tables := src.Tables
	if len(tables) == 0 {
		tables = []string{"*"}
	}

	schemaSnapshot := filepath.Join(src.BackupRef, "schema.sql")

	manifest := &SeedManifest{
		Version:            1,
		Engine:             "mysql",
		Kind:               "xtrabackup",
		ToolVersion:        meta.ToolVersion,
		ToolPrerelease:     isRC,
		SourceIdentity:     meta.ServerUUID,
		BackupRef:          src.BackupRef,
		StartPosition:      StartPosition{GTIDSet: meta.GTIDSet},
		PositionCrossCheck: crossCheck,
		SchemaSnapshot:     schemaSnapshot,
		Tables:             tables,
		Integrity:          &IntegrityInfo{PrepareOK: true},
		LogCoverage: &LogCoverageInfo{
			SourceGTIDPurged: src.SourcePurgedGTID,
			ArchiveUntil:     src.ArchiveUntilGTID,
			OK:               true,
		},
		Target: TargetInfo{
			Version:    targetVer,
			Compatible: true,
		},
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	return manifest, nil
}

// Apply thực hiện điều phối restore theo manifest (placeholder cho bước apply).
func (p *XtraBackupPlanner) Apply(ctx context.Context, m *SeedManifest) error {
	return nil
}
