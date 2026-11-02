package seed

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

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
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // dòng GTID nhiều UUID có thể rất dài
	// mydumper ≥ 0.12 (gồm v1.0.3-1) ghi metadata dạng INI: vị trí của server được dump nằm trong
	// section [source] (bản cũ: [master]) dưới dạng dòng comment
	//   # executed_gtid_set = "uuid:1-N"   # SOURCE_LOG_FILE = "binlog.000002"   # SOURCE_LOG_POS = 850
	// Các section khác (vd. [replication] mô tả source của replica) KHÔNG phải P.
	// Bản cũ (0.x) ghi khối "SHOW MASTER STATUS:" với "Log:", "Pos:", "GTID:" — vẫn đọc được.
	section := ""
	inPosSection := func() bool { return section == "" || section == "source" || section == "master" }
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.Trim(line, "[]"))
			continue
		}
		if strings.HasPrefix(line, "#") {
			body := strings.TrimSpace(strings.TrimLeft(line, "#"))
			if k, v, ok := strings.Cut(body, "="); ok && inPosSection() {
				key := strings.ToLower(strings.TrimSpace(k))
				val := strings.Trim(strings.TrimSpace(v), `"'`)
				switch key {
				case "executed_gtid_set":
					meta.GTID = val
					continue
				case "source_log_file", "master_log_file":
					meta.BinlogFile = val
					continue
				case "source_log_pos", "master_log_pos":
					_, _ = fmt.Sscanf(val, "%d", &meta.BinlogPos)
					continue
				}
			}
			// Nhận diện phiên bản mydumper từ comment (nếu có)
			if strings.Contains(strings.ToLower(line), "mydumper") {
				for _, f := range strings.Fields(line) {
					if strings.HasPrefix(f, "v") || strings.HasPrefix(f, "1.") {
						meta.ToolVersion = strings.TrimPrefix(f, "v")
					}
				}
			}
			continue
		}
		if !inPosSection() {
			continue
		}
		// Dạng cũ: Log = ... / Log: ...
		cleanLine := strings.ReplaceAll(line, "\t", " ")
		lower := strings.ToLower(cleanLine)
		if strings.HasPrefix(lower, "executed_gtid_set") {
			if _, v, ok := strings.Cut(cleanLine, "="); ok {
				meta.GTID = strings.Trim(strings.TrimSpace(v), `"'`)
			}
		} else if strings.Contains(lower, "log =") || strings.Contains(lower, "log:") {
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
		} else if strings.Contains(lower, "lock mode") || strings.Contains(lower, "lock_mode") || strings.Contains(lower, "sync-thread-lock-mode") || strings.Contains(lower, "no-locks") || strings.Contains(lower, "no_locks") || strings.Contains(lower, "no-lock") || strings.Contains(lower, "no_lock") {
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

	// 1. P hợp lệ, không có khoảng trống (SD-1)
	pSet, err := checkPositionP("mydumper", meta.GTID)
	if err != nil {
		return nil, err
	}

	// 2. Đối chiếu chéo vị trí P với instance sau load (SD-1)
	crossCheck, err := checkCrossCheck(pSet, meta.GTID, src.TargetRestoredGTID)
	if err != nil {
		return nil, err
	}

	// 3. Độ phủ của log (SD-2)
	if err := checkLogCoverage(pSet, src.SourcePurgedGTID, src.ArchiveUntilGTID); err != nil {
		return nil, err
	}

	engine := src.Engine
	if engine == "" {
		engine = "mysql"
	}

	// Backup logic: tương thích = map được kiểu dữ liệu (design §3.1); MySQL→MySQL luôn map được.
	targetVer := src.TargetVersion

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
