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

	"strand/internal/checkpoint"
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
	BinlogFile string
	BinlogPos  uint64
	GTID       string
	// GTIDInTrailer: GTID_PURGED nằm ở CUỐI file ("-- GTID state at the end of the backup", mysqldump 5.7
	// --single-transaction). Giá trị đó có thể là gtid_executed lúc KẾT THÚC dump (muộn hơn snapshot) nên
	// KHÔNG được dùng làm P; chỉ dùng vị trí CHANGE MASTER để tính P.
	GTIDInTrailer bool
	ToolVersion   string
	IsGzip        bool
}

// MysqldumpPlanner cài đặt Seeder cho công cụ mysqldump theo spec: 02 §9 và design §3.3.
type MysqldumpPlanner struct {
	// Resolver tính GTID set tại vị trí CHANGE MASTER từ binlog của source; nil = không có --source.
	Resolver BinlogPositionResolver
}

// MysqldumpGTIDLessNote ghi chú khi seed không có GTID (nguồn 5.5 / gtid_mode=OFF).
const MysqldumpGTIDLessNote = "NOTE: source has no GTID (MySQL 5.5 or gtid_mode=OFF): the seed start position is binlog file/position only; the pipeline needs binlog position mode (not GTID) to start from it."

// NewMysqldumpPlanner tạo mới MysqldumpPlanner.
func NewMysqldumpPlanner() *MysqldumpPlanner {
	return &MysqldumpPlanner{}
}

// NewMysqldumpPlannerWithSource tạo planner có thể đọc binlog của source (DSN) để tính P tại vị trí dump.
func NewMysqldumpPlannerWithSource(dsn string) *MysqldumpPlanner {
	p := &MysqldumpPlanner{}
	if strings.TrimSpace(dsn) != "" {
		p.Resolver = NewSourceBinlogResolver(dsn)
	}
	return p
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

	br := bufio.NewReaderSize(r, 64*1024)
	scanned := 0

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
		scanned += len(line)
		if scanned > maxMysqldumpHeaderBytes {
			break
		}
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

	// mysqldump 5.7 (--single-transaction) ghi GTID_PURGED ở cuối file: đọc phần đuôi để biết (không dùng làm P).
	if meta.GTID == "" {
		tail, tErr := readDumpTail(file, br, fi.Size(), isGzip)
		if tErr != nil {
			return nil, tErr
		}
		if g := findGTIDPurgedInTail(tail); g != "" {
			meta.GTID, meta.GTIDInTrailer = g, true
		}
	}
	if meta.GTID == "" && meta.BinlogFile == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"mysqldump file has no GTID_PURGED (dump with --set-gtid-purged=ON on a GTID source) and no CHANGE MASTER position (--master-data=2 / --source-data=2)",
			nil,
		)
	}
	if meta.GTIDInTrailer && meta.BinlogFile == "" {
		return nil, exitcode.New(
			exitcode.ErrSeedInvariantFailed,
			exitcode.Rejected,
			"mysqldump GTID_PURGED is at the end of the file and there is no CHANGE MASTER position: with --single-transaction that value can be later than the snapshot (transactions committed during the dump would be skipped); re-dump with --master-data=2 (--source-data=2)",
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

	gtid := meta.GTID
	fromBinlog, gtidLess := false, false
	var notes []string
	// Vị trí CHANGE MASTER (lấy dưới FTWRL cùng snapshot) là điểm nhất quán. GTID_PURGED ở header (8.0)
	// được giữ nguyên; thiếu hoặc nằm ở cuối file (5.7) thì tính P từ binlog của source tại vị trí đó.
	if meta.BinlogFile != "" && (meta.GTID == "" || meta.GTIDInTrailer) {
		if p.Resolver == nil {
			return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
				fmt.Sprintf("mysqldump has no usable GTID_PURGED at the start; P must be computed from the source binlog at %s:%d: pass --source", meta.BinlogFile, meta.BinlogPos), nil)
		}
		g, enabled, rErr := p.Resolver(ctx, meta.BinlogFile, meta.BinlogPos)
		if rErr != nil {
			return nil, rErr
		}
		if !enabled {
			gtidLess = true
			gtid = ""
			notes = append(notes, MysqldumpGTIDLessNote)
		} else {
			gtid, fromBinlog = g, true
			if meta.GTIDInTrailer {
				notes = append(notes, fmt.Sprintf("NOTE: GTID_PURGED at the end of the dump (%s) was ignored: it can be later than the snapshot. P = GTID set computed from the source binlog at %s:%d.", meta.GTID, meta.BinlogFile, meta.BinlogPos))
			}
		}
	}

	var pSet *checkpoint.MySQLAppliedSet
	var crossCheck *PositionCrossCheck
	if !gtidLess {
		// 1. P hợp lệ, không có khoảng trống (SD-1)
		pSet, err = checkPositionP("mysqldump", gtid)
		if err != nil {
			return nil, err
		}

		// 2. Đối chiếu chéo vị trí P với instance sau load (SD-1)
		if fromBinlog {
			// Target đã load dump mang GTID_PURGED cuối file (>= P): chỉ yêu cầu ⊇ P.
			crossCheck, err = checkCrossCheckSuperset(pSet, gtid, src.TargetRestoredGTID)
		} else {
			crossCheck, err = checkCrossCheck(pSet, gtid, src.TargetRestoredGTID)
		}
		if err != nil {
			return nil, err
		}
	}

	// 3. Độ phủ của log (SD-2)
	if !gtidLess {
		if err := checkLogCoverage(pSet, src.SourcePurgedGTID, src.ArchiveUntilGTID); err != nil {
			return nil, err
		}
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

	warnings := append([]string{MysqldumpConsistencyWarning}, notes...)
	if meta.IsGzip {
		warnings = append(warnings, MysqldumpGzipTrailerWarning)
	}

	srcIdentity := ""
	if gtid != "" {
		srcIdentity = extractSourceIdentity(gtid)
	}
	manifest := &SeedManifest{
		Version:        1,
		Engine:         engine,
		Kind:           "mysqldump",
		ToolVersion:    meta.ToolVersion,
		ToolPrerelease: false,
		SourceIdentity: srcIdentity,
		BackupRef:      src.BackupRef,
		StartPosition: StartPosition{GTIDSet: gtid, BinlogFile: meta.BinlogFile, BinlogPos: meta.BinlogPos,
			GTIDFromBinlog: fromBinlog},
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

// mysqldumpTailWindow: vùng cuối file quét tìm GTID_PURGED ở trailer (tập GTID nhiều dòng có thể dài).
const mysqldumpTailWindow = 64 * 1024

// readDumpTail trả về tối đa 64 KiB cuối của dump: file phẳng thì seek, .gz thì đọc tuần tự phần còn lại.
func readDumpTail(file *os.File, rest io.Reader, size int64, isGzip bool) ([]byte, error) {
	fail := func(err error) error {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "failed reading mysqldump trailer", err)
	}
	if !isGzip {
		off := int64(0)
		if size > mysqldumpTailWindow {
			off = size - mysqldumpTailWindow
		}
		if _, err := file.Seek(off, io.SeekStart); err != nil {
			return nil, fail(err)
		}
		b, err := io.ReadAll(io.LimitReader(file, mysqldumpTailWindow))
		if err != nil {
			return nil, fail(err)
		}
		return b, nil
	}
	buf := make([]byte, 0, 4*mysqldumpTailWindow)
	chunk := make([]byte, 256*1024)
	for {
		n, err := rest.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if len(buf) > 2*mysqldumpTailWindow {
			buf = append(buf[:0], buf[len(buf)-mysqldumpTailWindow:]...)
		}
		if err != nil {
			if len(buf) > mysqldumpTailWindow {
				buf = buf[len(buf)-mysqldumpTailWindow:]
			}
			if errors.Is(err, io.EOF) {
				return buf, nil
			}
			return nil, fail(err)
		}
	}
}

// findGTIDPurgedInTail tìm câu "SET @@GLOBAL.GTID_PURGED=...;" trong phần đuôi dump.
func findGTIDPurgedInTail(tail []byte) string {
	br := bufio.NewReader(strings.NewReader(string(tail)))
	for {
		line, err := br.ReadString('\n')
		if len(line) > 0 {
			if g, matched, _ := readGTIDPurged(strings.TrimSpace(line), br); matched && g != "" {
				return g
			}
		}
		if err != nil {
			return ""
		}
	}
}
