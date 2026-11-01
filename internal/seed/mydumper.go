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

// MydumperMeta chứa metadata đã đọc từ file metadata của mydumper.
type MydumperMeta struct {
	BinlogFile  string
	BinlogPos   uint64
	GTID        string
	LockMode    string
	ToolVersion string
}

// MydumperPlanner cài đặt Seeder cho công cụ mydumper theo spec: 02 §9 và design §3.3.
type MydumperPlanner struct{}

// NewMydumperPlanner tạo mới MydumperPlanner.
func NewMydumperPlanner() *MydumperPlanner {
	return &MydumperPlanner{}
}

// ParseMydumperMetadata đọc và phân tích metadata từ file metadata của mydumper.
func ParseMydumperMetadata(path string) (*MydumperMeta, error) {
	metaPath := path
	fi, err := os.Stat(path)
	if err != nil {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("cannot access mydumper path %s", path),
			err,
		)
	}
	if fi.IsDir() {
		metaPath = filepath.Join(path, "metadata")
	}

	file, err := os.Open(metaPath)
	if err != nil {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("missing mydumper metadata file %s", metaPath),
			err,
		)
	}
	defer file.Close()

	meta := &MydumperMeta{
		ToolVersion: "1.0.3-1", // Phiên bản chuẩn theo spec 02 §9
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Nhận diện phiên bản mydumper từ comment
		if strings.HasPrefix(line, "#") {
			if strings.Contains(strings.ToLower(line), "mydumper") {
				fields := strings.Fields(line)
				for _, f := range fields {
					if strings.HasPrefix(f, "v") || strings.HasPrefix(f, "1.") {
						meta.ToolVersion = strings.TrimPrefix(f, "v")
					}
				}
			}
			continue
		}

		// Phân tích Log = ... hoặc Log: ...
		cleanLine := strings.ReplaceAll(line, "\t", " ")
		lower := strings.ToLower(cleanLine)

		if strings.Contains(lower, "log =") || strings.Contains(lower, "log:") {
			parts := strings.SplitN(cleanLine, "=", 2)
			if len(parts) < 2 {
				parts = strings.SplitN(cleanLine, ":", 2)
			}
			if len(parts) == 2 {
				meta.BinlogFile = strings.TrimSpace(parts[1])
			}
		} else if strings.Contains(lower, "pos =") || strings.Contains(lower, "pos:") {
			parts := strings.SplitN(cleanLine, "=", 2)
			if len(parts) < 2 {
				parts = strings.SplitN(cleanLine, ":", 2)
			}
			if len(parts) == 2 {
				_, _ = fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &meta.BinlogPos)
			}
		} else if strings.Contains(lower, "gtid:") || strings.Contains(lower, "gtid =") {
			parts := strings.SplitN(cleanLine, ":", 2)
			if len(parts) < 2 {
				parts = strings.SplitN(cleanLine, "=", 2)
			}
			if len(parts) == 2 {
				meta.GTID = strings.TrimSpace(parts[1])
			}
		} else if strings.Contains(lower, "lock mode") || strings.Contains(lower, "lock_mode") || strings.Contains(lower, "no-locks") || strings.Contains(lower, "no_locks") || strings.Contains(lower, "no-lock") || strings.Contains(lower, "no_lock") {
			if strings.Contains(lower, "no-locks") || strings.Contains(lower, "no_locks") || strings.Contains(lower, "no-lock") || strings.Contains(lower, "no_lock") {
				meta.LockMode = "NO_LOCK"
			} else {
				parts := strings.SplitN(cleanLine, ":", 2)
				if len(parts) < 2 {
					parts = strings.SplitN(cleanLine, "=", 2)
				}
				if len(parts) == 2 {
					meta.LockMode = strings.ToUpper(strings.TrimSpace(parts[1]))
				} else {
					meta.LockMode = "NO_LOCK"
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "failed reading mydumper metadata", err)
	}

	// Kiểm tra chế độ NO_LOCK
	// spec: 02 §9 "mydumper v1.0.3-1 (không chấp nhận NO_LOCK)"
	if strings.Contains(meta.LockMode, "NO_LOCK") || strings.Contains(meta.LockMode, "NO-LOCK") || strings.EqualFold(meta.LockMode, "NONE") {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"mydumper backup with NO_LOCK mode is rejected: snapshot is not transactionally consistent (spec 02 §9)",
			nil,
		)
	}

	if meta.GTID == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"missing or empty GTID in mydumper metadata",
			nil,
		)
	}

	return meta, nil
}

// Plan kiểm tra các bất biến SD-1 đến SD-4 cho bản backup mydumper.
func (p *MydumperPlanner) Plan(ctx context.Context, src SeedSource) (*SeedManifest, error) {
	meta, err := ParseMydumperMetadata(src.BackupRef)
	if err != nil {
		return nil, err
	}

	// 1. Kiểm tra GTID set hợp lệ và không có holes (SD-1, SD-2)
	pSet := checkpoint.NewMySQLAppliedSet()
	for _, seg := range strings.Split(meta.GTID, ",") {
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
			fmt.Sprintf("mydumper backup GTID set %q contains %d holes: non-contiguous log", meta.GTID, len(holes)),
			nil,
		)
	}

	// 2. Đối chiếu chéo vị trí P với instance sau restore/load (SD-1)
	var crossCheck *PositionCrossCheck
	if src.TargetRestoredGTID != "" {
		restoredSet := checkpoint.NewMySQLAppliedSet()
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
					src.TargetRestoredGTID, meta.GTID),
				nil,
			)
		}

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
					fmt.Sprintf("log gap: source has purged past P (%s) and no archive is available", meta.GTID),
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
						src.ArchiveUntilGTID, meta.GTID),
					nil,
				)
			}
		}
	}

	engine := src.Engine
	if engine == "" {
		engine = "mysql"
	}

	targetVer := src.TargetVersion
	if targetVer == "" {
		targetVer = "8.4.6"
	}

	tables := src.Tables
	if len(tables) == 0 {
		tables = []string{"*"}
	}

	schemaSnapshot := filepath.Join(src.BackupRef, "metadata")

	manifest := &SeedManifest{
		Version:            1,
		Engine:             engine,
		Kind:               "mydumper",
		ToolVersion:        meta.ToolVersion,
		ToolPrerelease:     false,
		SourceIdentity:     extractSourceIdentity(meta.GTID),
		BackupRef:          src.BackupRef,
		StartPosition:      StartPosition{GTIDSet: meta.GTID},
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

func extractSourceIdentity(gtid string) string {
	parts := strings.Split(gtid, ":")
	if len(parts) > 0 {
		return strings.TrimSpace(parts[0])
	}
	return "mysql-source"
}
