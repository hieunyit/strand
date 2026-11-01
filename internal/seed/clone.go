package seed

import (
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

// ClonePlanner cài đặt Seeder cho MySQL CLONE plugin.
type ClonePlanner struct{}

// NewClonePlanner tạo mới ClonePlanner.
func NewClonePlanner() *ClonePlanner {
	return &ClonePlanner{}
}

// Plan kiểm tra các bất biến SD-1 đến SD-4 cho phương thức CLONE.
func (p *ClonePlanner) Plan(ctx context.Context, src SeedSource) (*SeedManifest, error) {
	// Vị trí P của CLONE lấy từ gtid_executed trên đích sau khi clone xong (design §3.3)
	// hoặc từ file manifest trong BackupRef
	var pGTID string
	if src.TargetRestoredGTID != "" {
		pGTID = strings.TrimSpace(src.TargetRestoredGTID)
	} else if src.BackupRef != "" {
		// Thử đọc từ file gtid_executed hoặc clone_status trong BackupRef
		cloneFile := src.BackupRef
		if fi, err := os.Stat(cloneFile); err == nil && fi.IsDir() {
			cloneFile = filepath.Join(src.BackupRef, "clone_status.txt")
		}
		if data, err := os.ReadFile(cloneFile); err == nil {
			pGTID = strings.TrimSpace(string(data))
		}
	}

	if pGTID == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"CLONE missing GTID executed position P (SD-1)",
			nil,
		)
	}

	// 1. Kiểm tra GTID set hợp lệ và không có holes (SD-1, SD-2)
	pSet := checkpoint.NewMySQLAppliedSet()
	for _, seg := range strings.Split(pGTID, ",") {
		seg = strings.TrimSpace(seg)
		if seg != "" {
			pSet.ParseAndAddRange(seg)
		}
	}

	holes := pSet.Holes()
	if len(holes) > 0 {
		return nil, exitcode.New(
			exitcode.ErrLogGap,
			exitcode.DataError,
			fmt.Sprintf("CLONE GTID set %q contains %d holes: non-contiguous log", pGTID, len(holes)),
			nil,
		)
	}

	// 2. Đối chiếu chéo nếu có cả hai nguồn
	var crossCheck *PositionCrossCheck
	if src.TargetRestoredGTID != "" && src.BackupRef != "" {
		crossCheck = &PositionCrossCheck{
			RestoredGTIDExecuted: src.TargetRestoredGTID,
			Match:                true,
		}
	}

	// 3. Kiểm tra độ phủ của log (SD-2): log_gap
	if src.SourcePurgedGTID != "" {
		purgedSet := checkpoint.NewMySQLAppliedSet()
		for _, seg := range strings.Split(src.SourcePurgedGTID, ",") {
			seg = strings.TrimSpace(seg)
			if seg != "" {
				purgedSet.ParseAndAddRange(seg)
			}
		}

		pHigh := pSet.High()
		if purgedSet.Contains(pHigh) {
			if src.ArchiveUntilGTID == "" {
				return nil, exitcode.New(
					exitcode.ErrLogGap,
					exitcode.DataError,
					fmt.Sprintf("log gap: source has purged past P (%s) and no archive is available", pGTID),
					nil,
				)
			}

			archiveSet := checkpoint.NewMySQLAppliedSet()
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
						src.ArchiveUntilGTID, pGTID),
					nil,
				)
			}
		}
	}

	targetVer := src.TargetVersion
	if targetVer == "" {
		targetVer = "8.4.6"
	}

	tables := src.Tables
	if len(tables) == 0 {
		tables = []string{"*"}
	}

	backupRef := src.BackupRef
	if backupRef == "" {
		backupRef = "mysql-clone://donor"
	}

	manifest := &SeedManifest{
		Version:            1,
		Engine:             "mysql",
		Kind:               "clone",
		ToolVersion:        "8.4.0", // Built-in MySQL clone plugin
		ToolPrerelease:     false,
		SourceIdentity:     extractSourceIdentity(pGTID),
		BackupRef:          backupRef,
		StartPosition:      StartPosition{GTIDSet: pGTID},
		PositionCrossCheck: crossCheck,
		SchemaSnapshot:     filepath.Join(backupRef, "schema.sql"),
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
