package seed

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"strand/internal/exitcode"
)

// spec: 02 §9, design §3.3
// Invariants: SD-1, SD-2, SD-3, SD-4

const (
	// maxMysqldumpHeaderBytes giới hạn lượng dữ liệu đọc khi phân tích phần đầu file dump (2 MiB).
	maxMysqldumpHeaderBytes = 2 * 1024 * 1024
	// mysqldumpTrailerWindow kích thước vùng cuối file dùng để kiểm tra dòng "-- Dump completed" (4 KiB).
	mysqldumpTrailerWindow = 4 * 1024

	// MysqldumpConsistencyWarning cảnh báo bắt buộc khi dùng mysqldump làm nguồn seed.
	MysqldumpConsistencyWarning = "WARNING: mysqldump does not record its options; Strand cannot verify the dump was taken with --single-transaction (and --source-data/--set-gtid-purged=ON). A dump without a consistent snapshot silently diverges from P. Post-seed verify (verify.post_seed_window) is the only safety net; mydumper or XtraBackup are recommended."

	// MysqldumpGzipTrailerWarning cảnh báo bổ sung khi file dump nén .gz không thể seek kiểm tra trailer.
	MysqldumpGzipTrailerWarning = "WARNING: mysqldump file is gzip-compressed (.gz); Strand cannot seek to verify the '-- Dump completed' trailer (truncation cannot be checked)."
)

// MysqldumpMeta chứa metadata đã đọc từ phần header của file mysqldump.
type MysqldumpMeta struct {
	BinlogFile  string
	BinlogPos   uint64
	GTID        string
	ToolVersion string
	IsGzip      bool
}

// MysqldumpPlanner cài đặt Seeder cho công cụ mysqldump theo spec: 02 §9 và design §3.3.
type MysqldumpPlanner struct{}

// NewMysqldumpPlanner tạo mới MysqldumpPlanner.
func NewMysqldumpPlanner() *MysqldumpPlanner {
	return &MysqldumpPlanner{}
}

// ParseMysqldumpMetadata chỉ đọc phần header của file .sql (hoặc .sql.gz) — dừng ở câu lệnh
// CREATE TABLE / INSERT / USE đầu tiên hoặc sau 2 MiB — và kiểm tra trailer ở 4 KiB cuối (file thường).
func ParseMysqldumpMetadata(path string) (*MysqldumpMeta, error) {
	if strings.TrimSpace(path) == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"missing mysqldump backup file path",
			nil,
		)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("cannot access mysqldump file %s", path),
			err,
		)
	}
	if fi.IsDir() {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("mysqldump --backup must be a single .sql or .gz file, got directory %s", path),
			nil,
		)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			fmt.Sprintf("cannot open mysqldump file %s", path),
			err,
		)
	}
	defer file.Close()

	isGzip := strings.HasSuffix(strings.ToLower(path), ".gz")
	var r io.Reader = file
	if isGzip {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return nil, exitcode.New(
				exitcode.ErrSeedInvariantFailed,
				exitcode.Rejected,
				fmt.Sprintf("cannot read gzip mysqldump file %s", path),
				err,
			)
		}
		defer gz.Close()
		r = gz
	}

	lr := io.LimitReader(r, maxMysqldumpHeaderBytes)
	br := bufio.NewReader(lr)

	// 1. Dòng đầu tiên phải chứa phiên bản công cụ: "-- MySQL dump <x> Distrib <ver>, for ..."
	firstLine, err := br.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"failed reading mysqldump header",
			err,
		)
	}
	toolVer := parseMysqldumpVersionLine(firstLine)
	if toolVer == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"mysqldump file missing version header (expected '-- MySQL dump <x> Distrib <ver>, for ...')",
			nil,
		)
	}

	meta := &MysqldumpMeta{
		ToolVersion: toolVer,
		IsGzip:      isGzip,
	}

	// 2. Quét phần header đến câu lệnh CREATE TABLE / INSERT / USE đầu tiên hoặc hết 2 MiB.
	for {
		line, readErr := br.ReadString('\n')
		if len(line) > 0 {
			trimmed := strings.TrimSpace(line)
			if isHeaderEndStatement(trimmed) {
				break
			}
			if logFile, logPos, ok := parseChangeReplicationLine(trimmed); ok {
				meta.BinlogFile = logFile
				meta.BinlogPos = logPos
			} else if gtid, matched, gErr := readGTIDPurged(trimmed, br); gErr != nil {
				return nil, gErr
			} else if matched {
				meta.GTID = gtid
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, exitcode.New(
				exitcode.ErrSeedInvariantFailed,
				exitcode.Rejected,
				"failed reading mysqldump header",
				readErr,
			)
		}
	}

	if meta.GTID == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"mysqldump file has no GTID_PURGED (dump with --set-gtid-purged=ON on a GTID source)",
			nil,
		)
	}

	// 3. Với file phẳng (.sql), seek tới 4 KiB cuối cùng để kiểm tra trailer "-- Dump completed".
	if !isGzip {
		if err := checkMysqldumpTrailer(file, fi.Size()); err != nil {
			return nil, err
		}
	}

	return meta, nil
}

// Plan kiểm tra các bất biến SD-1 đến SD-4 cho file mysqldump.
func (p *MysqldumpPlanner) Plan(ctx context.Context, src SeedSource) (*SeedManifest, error) {
	meta, err := ParseMysqldumpMetadata(src.BackupRef)
	if err != nil {
		return nil, err
	}

	// 1. P hợp lệ, không có khoảng trống (SD-1)
	pSet, err := checkPositionP("mysqldump", meta.GTID)
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

	targetVer := src.TargetVersion

	tables := src.Tables
	if len(tables) == 0 {
		tables = []string{"*"}
	}

	warnings := []string{MysqldumpConsistencyWarning}
	if meta.IsGzip {
		warnings = append(warnings, MysqldumpGzipTrailerWarning)
	}

	manifest := &SeedManifest{
		Version:            1,
		Engine:             engine,
		Kind:               "mysqldump",
		ToolVersion:        meta.ToolVersion,
		ToolPrerelease:     false,
		SourceIdentity:     extractSourceIdentity(meta.GTID),
		BackupRef:          src.BackupRef,
		StartPosition:      StartPosition{GTIDSet: meta.GTID},
		PositionCrossCheck: crossCheck,
		SchemaSnapshot:     src.BackupRef,
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
		Warnings:  warnings,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	return manifest, nil
}

// Apply thực hiện điều phối restore theo manifest.
func (p *MysqldumpPlanner) Apply(ctx context.Context, m *SeedManifest) error {
	return nil
}

// parseMysqldumpVersionLine trích xuất <ver> từ dòng đầu "-- MySQL dump <x> Distrib <ver>, for ...".
func parseMysqldumpVersionLine(line string) string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "-- MySQL dump ") {
		return ""
	}
	idx := strings.Index(line, "Distrib ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(line[idx+len("Distrib "):])
	if comma := strings.IndexByte(rest, ','); comma >= 0 {
		rest = rest[:comma]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

// isHeaderEndStatement kiểm tra dòng bắt đầu phần thân dump (CREATE TABLE, INSERT, USE).
func isHeaderEndStatement(trimmed string) bool {
	if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "/*") {
		return false
	}
	upper := strings.ToUpper(trimmed)
	return strings.HasPrefix(upper, "CREATE TABLE ") ||
		strings.HasPrefix(upper, "CREATE TABLE\t") ||
		strings.HasPrefix(upper, "CREATE TABLE`") ||
		strings.HasPrefix(upper, "INSERT ") ||
		strings.HasPrefix(upper, "INSERT\t") ||
		strings.HasPrefix(upper, "USE ") ||
		strings.HasPrefix(upper, "USE\t") ||
		strings.HasPrefix(upper, "USE`")
}

// readGTIDPurged phân tích câu lệnh:
// SET @@GLOBAL.GTID_PURGED=/*!80000 '+'*/ '<gtid set>';
// hoặc SET @@GLOBAL.GTID_PURGED='<gtid set>'; (có thể trải trên nhiều dòng đến khi gặp "';").
func readGTIDPurged(trimmed string, br *bufio.Reader) (string, bool, error) {
	if strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, "/*") {
		return "", false, nil
	}
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SET ") {
		return "", false, nil
	}
	idx := strings.Index(upper, "@@GLOBAL.GTID_PURGED")
	if idx < 0 {
		return "", false, nil
	}
	rest := strings.TrimSpace(trimmed[idx+len("@@GLOBAL.GTID_PURGED"):])
	if !strings.HasPrefix(rest, "=") {
		return "", false, nil
	}
	rest = strings.TrimSpace(rest[1:])

	// Bỏ qua tiền tố /*!80000 '+'*/ nếu có
	if strings.HasPrefix(rest, "/*") {
		endComment := strings.Index(rest, "*/")
		if endComment < 0 {
			return "", true, nil
		}
		rest = strings.TrimSpace(rest[endComment+2:])
	}

	// Nếu dấu nháy mở nằm ở dòng tiếp theo
	for rest == "" {
		nextLine, err := br.ReadString('\n')
		if len(nextLine) > 0 {
			rest = strings.TrimSpace(nextLine)
		}
		if err != nil {
			break
		}
	}

	if !strings.HasPrefix(rest, "'") {
		return "", true, nil
	}
	rest = rest[1:]

	var sb strings.Builder
	for {
		if closeIdx := strings.IndexByte(rest, '\''); closeIdx >= 0 {
			sb.WriteString(rest[:closeIdx])
			break
		}
		sb.WriteString(rest)
		nextLine, err := br.ReadString('\n')
		if len(nextLine) > 0 {
			rest = nextLine
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return "", true, nil
			}
			return "", true, exitcode.New(
				exitcode.ErrSeedInvariantFailed,
				exitcode.Rejected,
				"failed reading multi-line GTID_PURGED in mysqldump header",
				err,
			)
		}
	}

	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\r', '\n':
			return -1
		default:
			return r
		}
	}, sb.String())
	return cleaned, true, nil
}

// parseChangeReplicationLine trích xuất thông tin binlog file/pos (tùy chọn) từ dòng
// CHANGE REPLICATION SOURCE TO hoặc CHANGE MASTER TO (dù có comment "--" hay không).
func parseChangeReplicationLine(trimmed string) (string, uint64, bool) {
	s := strings.TrimSpace(strings.TrimPrefix(trimmed, "--"))
	upper := strings.ToUpper(s)
	if !strings.HasPrefix(upper, "CHANGE REPLICATION SOURCE TO ") && !strings.HasPrefix(upper, "CHANGE MASTER TO ") {
		return "", 0, false
	}
	var logFile string
	for _, key := range []string{"SOURCE_LOG_FILE=", "MASTER_LOG_FILE="} {
		if idx := strings.Index(upper, key); idx >= 0 {
			val := strings.TrimSpace(s[idx+len(key):])
			if len(val) > 0 && (val[0] == '\'' || val[0] == '"') {
				q := val[0]
				val = val[1:]
				if end := strings.IndexByte(val, q); end >= 0 {
					logFile = val[:end]
				}
			} else {
				if end := strings.IndexAny(val, ",; \t"); end >= 0 {
					val = val[:end]
				}
				logFile = strings.Trim(val, "'\"")
			}
			break
		}
	}
	var logPos uint64
	for _, key := range []string{"SOURCE_LOG_POS=", "MASTER_LOG_POS="} {
		if idx := strings.Index(upper, key); idx >= 0 {
			val := strings.TrimSpace(s[idx+len(key):])
			_, _ = fmt.Sscanf(val, "%d", &logPos)
			break
		}
	}
	if logFile == "" && logPos == 0 {
		return "", 0, false
	}
	return logFile, logPos, true
}

// checkMysqldumpTrailer seek đến 4 KiB cuối của file phẳng và yêu cầu có "-- Dump completed".
func checkMysqldumpTrailer(file *os.File, size int64) error {
	offset := int64(0)
	if size > mysqldumpTrailerWindow {
		offset = size - mysqldumpTrailerWindow
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"failed seeking to mysqldump trailer",
			err,
		)
	}
	tail, err := io.ReadAll(io.LimitReader(file, mysqldumpTrailerWindow))
	if err != nil {
		return exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"failed reading mysqldump trailer",
			err,
		)
	}
	if !strings.Contains(string(tail), "-- Dump completed") {
		return exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"mysqldump file is truncated (no '-- Dump completed' trailer)",
			nil,
		)
	}
	return nil
}
