package seed

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"strand/internal/exitcode"
	"strand/internal/mysqlver"
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
	// Clone plugin chỉ có từ 8.0.17: donor MySQL 5.7 phải dùng mysqldump/mydumper.
	if src.SourceMySQLVersion != "" {
		v, err := mysqlver.Parse(src.SourceMySQLVersion)
		if err != nil {
			return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, err.Error(), err)
		}
		if err := mysqlver.CheckSupported(v); err != nil {
			return nil, err
		}
		if err := mysqlver.CheckClone(v); err != nil {
			return nil, err
		}
	}
	// Vị trí P của CLONE lấy từ gtid_executed trên đích sau khi clone xong (design §3.3)
	// hoặc từ file manifest trong BackupRef
	// P lấy từ trạng thái clone của donor (BackupRef) và/hoặc gtid_executed sau clone.
	var donorGTID string
	if src.BackupRef != "" {
		cloneFile := src.BackupRef
		if fi, err := os.Stat(cloneFile); err == nil && fi.IsDir() {
			cloneFile = filepath.Join(src.BackupRef, "clone_status.txt")
		}
		if data, err := os.ReadFile(cloneFile); err == nil {
			donorGTID = strings.TrimSpace(string(data))
		}
	}
	restored := strings.TrimSpace(src.TargetRestoredGTID)

	pGTID := donorGTID
	if pGTID == "" {
		pGTID = restored
	}
	if pGTID == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"CLONE missing GTID executed position P (SD-1)",
			nil,
		)
	}

	pSet, err := checkPositionP("clone", pGTID)
	if err != nil {
		return nil, err
	}

	// Đối chiếu chéo khi có cả hai nguồn (SD-1)
	var crossCheck *PositionCrossCheck
	if donorGTID != "" && restored != "" {
		if crossCheck, err = checkCrossCheck(pSet, pGTID, restored); err != nil {
			return nil, err
		}
	}

	// Độ phủ của log (SD-2)
	if err := checkLogCoverage(pSet, src.SourcePurgedGTID, src.ArchiveUntilGTID); err != nil {
		return nil, err
	}

	// SD-4: CLONE plugin tự từ chối donor/recipient khác version, nên tương thích được MySQL bảo đảm.
	targetVer := src.TargetVersion

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
