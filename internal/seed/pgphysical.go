package seed

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// spec: 02 §2 (SD-1..SD-4), §9; design "PG seed" (pgBackRest / WAL-G)
//
// Phần chung của planner backup vật lý PostgreSQL (pgbackrest, walg). Planner chỉ đọc metadata JSON
// (`pgbackrest info --output=json`, `wal-g backup-list --detail --json`, `wal-g wal-show --detailed-json`)
// đưa vào qua file; không gọi binary.
//
// Quy trình (design): restore backup thành standby đuổi theo archive → tạo slot, P = consistent_point →
// target PITR tới recovery_target_lsn = P + pause → kiểm pg_last_wal_replay_lsn() = P → promote.
// Vì vậy:
//   - SD-1: P lấy từ slot (src.ConsistentPointLSN do CLI đọc từ slot), backup phải kết thúc (stop LSN) ≤ P
//     để PITR tới được P, cùng system_identifier với source; nếu đã có target thì replay LSN phải = P.
//   - SD-2: archive WAL phải phủ liên tục từ segment bắt đầu backup tới segment chứa P, TRÊN timeline của
//     backup; thiếu / không chứng minh được → log_gap. Phạm vi archive là danh sách dải liên tục theo
//     timeline, không phải min/max toàn cục (review N08): [1,10] và [100,110] KHÔNG phủ [5,105].
//     Backup không được expire trong ETA (ước lượng từ retention full + chu kỳ full backup).
//   - SD-3: schema nằm trong chính backup vật lý.
//   - SD-4: cùng major version PostgreSQL.

// defaultWALSegmentSize là wal_segment_size mặc định của PostgreSQL.
const defaultWALSegmentSize = 16 << 20

// pgPhysicalBackup là dạng chuẩn hóa của một backup pgBackRest/WAL-G.
type pgPhysicalBackup struct {
	Label       string
	Full        bool
	FullLabel   string // label của full backup gốc (rỗng = tự suy theo thời gian)
	Error       bool
	Permanent   bool
	StartLSN    uint64
	StopLSN     uint64
	StartTime   time.Time
	Timeline    uint32 // timeline của WAL bắt đầu backup (0 = không biết)
	Version     string // server version của cluster được backup
	SystemID    string
	ToolVersion string
}

// walSeg là một segment WAL: timeline + số segment tuyệt đối (LSN / wal_segment_size).
type walSeg struct {
	TLI uint32
	Seg uint64
}

// walInterval là một dải segment liên tục [Start, End] (bao gồm hai đầu) đã có trong archive trên
// timeline TLI.
type walInterval struct {
	TLI        uint32
	Start, End uint64
}

// pgArchiveCoverage là phạm vi WAL đã có trong archive, dạng danh sách dải theo timeline (review N08:
// trước đây chỉ giữ min/max toàn cục và bỏ timeline → lỗ giữa hai dải không bị phát hiện).
type pgArchiveCoverage struct {
	Known     bool
	Intervals []walInterval
	// BoundsOnly: nguồn metadata chỉ cho biên min/max (`pgbackrest info`), không liệt kê từng segment →
	// không chứng minh được tính liên tục bên trong.
	BoundsOnly bool
	Missing    []walSeg // segment công cụ báo thiếu (wal-show missing_segments)
	Describe   string
}

// segsPerLogID là số segment trong một "log id" 4 GiB (phần XXXXXXXX của tên file WAL).
func segsPerLogID(segSize uint64) uint64 { return 0x100000000 / segSize }

// walFileName dựng tên file WAL 24 ký tự từ timeline + số segment.
func walFileName(tli uint32, seg, segSize uint64) string {
	n := segsPerLogID(segSize)
	return fmt.Sprintf("%08X%08X%08X", tli, seg/n, seg%n)
}

// intervalsMinus trả các dải con của [lo, hi] trên timeline tli sau khi bỏ các segment missing.
func intervalsMinus(tli uint32, lo, hi uint64, missing []uint64) []walInterval {
	ms := append([]uint64(nil), missing...)
	sort.Slice(ms, func(i, j int) bool { return ms[i] < ms[j] })
	var out []walInterval
	cur := lo
	for _, m := range ms {
		if m < cur || m > hi {
			continue
		}
		if m > cur {
			out = append(out, walInterval{TLI: tli, Start: cur, End: m - 1})
		}
		cur = m + 1
	}
	if cur <= hi {
		out = append(out, walInterval{TLI: tli, Start: cur, End: hi})
	}
	return out
}

// intervalsFromSegments gộp danh sách segment rời (inventory archive) thành các dải liên tục theo timeline.
func intervalsFromSegments(segs []walSeg) []walInterval {
	ss := append([]walSeg(nil), segs...)
	sort.Slice(ss, func(i, j int) bool {
		if ss[i].TLI != ss[j].TLI {
			return ss[i].TLI < ss[j].TLI
		}
		return ss[i].Seg < ss[j].Seg
	})
	var out []walInterval
	for _, x := range ss {
		if n := len(out); n > 0 && out[n-1].TLI == x.TLI && x.Seg <= out[n-1].End+1 {
			if x.Seg > out[n-1].End {
				out[n-1].End = x.Seg
			}
			continue
		}
		out = append(out, walInterval{TLI: x.TLI, Start: x.Seg, End: x.Seg})
	}
	return out
}

// coveredUntil: trên timeline tli, hợp các dải phủ liên tục từ from tới segment nào (ok=false nếu
// from không nằm trong dải nào).
func coveredUntil(ivs []walInterval, tli uint32, from uint64) (end uint64, ok bool) {
	var own []walInterval
	for _, iv := range ivs {
		if iv.TLI == tli && iv.End >= iv.Start {
			own = append(own, iv)
		}
	}
	sort.Slice(own, func(i, j int) bool { return own[i].Start < own[j].Start })
	for _, iv := range own {
		switch {
		case !ok:
			if iv.Start <= from && iv.End >= from {
				end, ok = iv.End, true
			}
		case iv.Start <= end+1:
			if iv.End > end {
				end = iv.End
			}
		default:
			return end, true
		}
	}
	return end, ok
}

// describeIntervals in danh sách dải dạng tên file WAL (cho thông báo lỗi).
func describeIntervals(ivs []walInterval, segSize uint64) string {
	parts := make([]string, 0, len(ivs))
	for _, iv := range ivs {
		parts = append(parts, walFileName(iv.TLI, iv.Start, segSize)+".."+walFileName(iv.TLI, iv.End, segSize))
	}
	if len(parts) == 0 {
		return "(empty)"
	}
	if len(parts) > 6 {
		parts = append(parts[:6], fmt.Sprintf("… (%d ranges)", len(ivs)))
	}
	return strings.Join(parts, ", ")
}

// flexString nhận giá trị JSON dạng chuỗi hoặc số và giữ nguyên văn bản (không qua float64, tránh mất
// chính xác với system_identifier 64-bit).
type flexString string

func (f *flexString) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	switch {
	case s == "null":
		*f = ""
	case strings.HasPrefix(s, `"`):
		u, err := strconv.Unquote(s)
		if err != nil {
			return err
		}
		*f = flexString(u)
	default:
		*f = flexString(s)
	}
	return nil
}

// readBackupInfo đọc file metadata JSON.
func readBackupInfo(kind, ref string) ([]byte, error) {
	if strings.TrimSpace(ref) == "" {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("%s: --backup must point to the backup info JSON file", kind), nil)
	}
	data, err := os.ReadFile(ref)
	if err != nil {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("%s: cannot read backup info %s: %v", kind, ref, err), err)
	}
	return data, nil
}

// walSegmentNo trả số segment chứa LSN.
func walSegmentNo(lsn, segSize uint64) uint64 { return lsn / segSize }

// parseWALFileName đổi tên file WAL 24 ký tự (TTTTTTTTXXXXXXXXYYYYYYYY) thành timeline + số segment
// (review N08: trước đây bỏ timeline).
func parseWALFileName(name string, segSize uint64) (walSeg, error) {
	name = strings.TrimSpace(name)
	if len(name) < 24 {
		return walSeg{}, fmt.Errorf("invalid WAL file name %q", name)
	}
	tli, err := strconv.ParseUint(name[0:8], 16, 32)
	if err != nil {
		return walSeg{}, fmt.Errorf("invalid WAL file name %q: %w", name, err)
	}
	logNo, err := strconv.ParseUint(name[8:16], 16, 32)
	if err != nil {
		return walSeg{}, fmt.Errorf("invalid WAL file name %q: %w", name, err)
	}
	segNo, err := strconv.ParseUint(name[16:24], 16, 32)
	if err != nil {
		return walSeg{}, fmt.Errorf("invalid WAL file name %q: %w", name, err)
	}
	if segNo >= segsPerLogID(segSize) {
		return walSeg{}, fmt.Errorf("invalid WAL file name %q: segment %X out of range for wal_segment_size %d", name, segNo, segSize)
	}
	return walSeg{TLI: uint32(tli), Seg: logNo*segsPerLogID(segSize) + segNo}, nil
}

// pgMajor trích major version PostgreSQL: "16.2" → "16", "160002" → "16", "18beta1" → "18", "9.6.1" → "9.6".
func pgMajor(v string) string {
	v = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(v), "PostgreSQL "))
	if n, err := strconv.Atoi(v); err == nil {
		if n >= 100000 {
			return strconv.Itoa(n / 10000)
		}
		if n >= 90000 { // 90603 → 9.6
			return fmt.Sprintf("%d.%d", n/10000, (n/100)%100)
		}
		return strconv.Itoa(n)
	}
	end := 0
	for end < len(v) && (v[end] >= '0' && v[end] <= '9') {
		end++
	}
	major := v[:end]
	if major == "9" && end < len(v) && v[end] == '.' { // dòng 9.x: major gồm hai số
		rest := v[end+1:]
		j := 0
		for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
			j++
		}
		if j > 0 {
			return "9." + rest[:j]
		}
	}
	return major
}

// checkPGPhysicalCompat kiểm SD-4: backup vật lý chỉ restore được trên cùng major version.
func checkPGPhysicalCompat(backupVersion, targetVersion string) (TargetInfo, error) {
	src, dst := pgMajor(backupVersion), pgMajor(targetVersion)
	if src == "" || dst == "" {
		return TargetInfo{}, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-4: cannot prove target compatibility (backup PostgreSQL version %q, target version %q)",
				backupVersion, targetVersion), nil)
	}
	if src != dst {
		return TargetInfo{}, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-4: physical backup of PostgreSQL %s cannot be restored on PostgreSQL %s target (different major version)",
				src, dst), nil)
	}
	return TargetInfo{Version: targetVersion, Compatible: true}, nil
}

// estimateFullExpiry ước lượng thời điểm backup (gốc là full root) bị expire theo retention full N:
// khi có thêm full mới mà số full > N thì full cũ nhất (và các diff/incr/delta phụ thuộc) bị xóa.
// k = số full (không lỗi) có thời điểm ≥ root; root bị xóa khi full mới thứ (N-k+1) hoàn tất, giả định
// full chạy đều đặn mỗi interval kể từ full gần nhất.
func estimateFullExpiry(fulls []time.Time, root time.Time, retention int, interval time.Duration) time.Time {
	k := 0
	var last time.Time
	for _, t := range fulls {
		if !t.Before(root) {
			k++
		}
		if t.After(last) {
			last = t
		}
	}
	remaining := retention - k + 1
	if remaining <= 0 {
		return root // đã vượt retention: lần expire kế tiếp sẽ xóa
	}
	return last.Add(time.Duration(remaining) * interval)
}

// inferFullInterval suy chu kỳ full backup = trung vị khoảng cách giữa các full liên tiếp (cần ≥ 2 full).
func inferFullInterval(fulls []time.Time) time.Duration {
	if len(fulls) < 2 {
		return 0
	}
	ts := append([]time.Time(nil), fulls...)
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	gaps := make([]time.Duration, 0, len(ts)-1)
	for i := 1; i < len(ts); i++ {
		gaps = append(gaps, ts[i].Sub(ts[i-1]))
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	return gaps[len(gaps)/2]
}

// planPGPhysical chạy các kiểm tra SD-1..SD-4 chung cho pgbackrest/walg và dựng manifest.
// ref là tham chiếu backup ghi vào seed.json (vd "pgbackrest://main/<label>"), refFor dựng nó từ label.
func planPGPhysical(kind string, src SeedSource, backups []pgPhysicalBackup, cov pgArchiveCoverage,
	now time.Time, refFor func(label string) string) (*SeedManifest, error) {
	reject := func(format string, a ...any) error {
		return exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, fmt.Sprintf(kind+": "+format, a...), nil)
	}
	segSize := src.WALSegmentSize
	if segSize == 0 {
		segSize = defaultWALSegmentSize
	}

	// SD-1: P là consistent_point của slot.
	if strings.TrimSpace(src.ConsistentPointLSN) == "" {
		return nil, reject("SD-1: P unknown — P must be the consistent_point of the CDC slot (pass --slot and --source)")
	}
	p, err := checkpoint.ParseLSN(src.ConsistentPointLSN)
	if err != nil || p == 0 {
		return nil, reject("SD-1: invalid slot consistent_point %q", src.ConsistentPointLSN)
	}
	pStr := checkpoint.FormatLSN(p)

	// Chọn backup.
	var b *pgPhysicalBackup
	if src.BackupLabel != "" {
		for i := range backups {
			if backups[i].Label == src.BackupLabel {
				b = &backups[i]
			}
		}
		if b == nil {
			return nil, reject("backup %q not found in backup info", src.BackupLabel)
		}
	} else {
		for i := range backups {
			c := &backups[i]
			if c.Error || c.StopLSN == 0 || c.StopLSN > p {
				continue
			}
			if b == nil || c.StopLSN > b.StopLSN {
				b = c
			}
		}
		if b == nil {
			return nil, reject("SD-1: no usable backup finished at or before P %s (PITR cannot go back before the backup's consistency point)", pStr)
		}
	}
	if b.Error {
		return nil, reject("SD-1: backup %s is marked as errored", b.Label)
	}
	if b.StopLSN == 0 {
		return nil, reject("SD-1: backup %s has no stop LSN in metadata", b.Label)
	}
	if b.StopLSN > p {
		return nil, reject("SD-1: backup %s stops at %s, after P %s: target cannot PITR back to P",
			b.Label, checkpoint.FormatLSN(b.StopLSN), pStr)
	}

	// Phiên bản công cụ (issue 15).
	toolVersion := b.ToolVersion
	if toolVersion == "" {
		toolVersion = src.ToolVersion
	}
	var warnings []string
	if toolVersion == "" {
		toolVersion = "unknown"
		warnings = append(warnings, kind+": tool version not recorded in backup metadata (pass --tool-version)")
	}
	isPre := IsPrereleaseToolVersion(toolVersion)
	if isPre && !src.AllowPrereleaseTool {
		return nil, exitcode.New(exitcode.ErrPrereleaseTool, exitcode.Rejected,
			fmt.Sprintf("%s: backup created by pre-release tool %s (use --allow-prerelease-tool)", kind, toolVersion), nil)
	}

	// SD-1: backup phải thuộc đúng cluster của source (system_identifier).
	// Chứng cứ phải đến từ CHÍNH backup: trước đây backup không ghi system identifier thì lấy luôn
	// systemid của source đang chạy — đó không phải bằng chứng backup thuộc cluster này (review N09).
	// Spec (02 §2, §9) không cho ngoại lệ: không chứng minh được SD-1 → từ chối. Vì vậy source_identity
	// trong seed.json luôn là system identifier đọc từ metadata backup, đã khớp với source.
	identity := strings.TrimSpace(b.SystemID)
	switch {
	case identity == "":
		return nil, reject("SD-1: cannot prove backup %s belongs to the source cluster: backup metadata has no system identifier", b.Label)
	case strings.TrimSpace(src.SourceSystemID) == "":
		return nil, reject("SD-1: cannot prove backup %s belongs to the source cluster: source system identifier is unknown (pass --source)", b.Label)
	case strings.TrimSpace(src.SourceSystemID) != identity:
		return nil, reject("SD-1: backup %s belongs to cluster system_identifier %s, source is %s",
			b.Label, identity, src.SourceSystemID)
	}
	if src.SourceVersion != "" && pgMajor(src.SourceVersion) != pgMajor(b.Version) {
		return nil, reject("SD-1: backup %s is PostgreSQL %s but source is %s", b.Label, b.Version, src.SourceVersion)
	}

	// SD-1: đối chiếu chéo với target đã PITR (nếu có).
	var cross *PositionCrossCheck
	if r := strings.TrimSpace(src.TargetRestoredLSN); r != "" {
		rl, err := checkpoint.ParseLSN(r)
		if err != nil {
			return nil, reject("SD-1: invalid target pg_last_wal_replay_lsn %q", r)
		}
		if rl != p {
			return nil, reject("SD-1: position cross-check mismatch: target pg_last_wal_replay_lsn %s != P %s",
				checkpoint.FormatLSN(rl), pStr)
		}
		cross = &PositionCrossCheck{RestoredLSN: checkpoint.FormatLSN(rl), Match: true}
	}

	// SD-4.
	target, err := checkPGPhysicalCompat(b.Version, src.TargetVersion)
	if err != nil {
		return nil, err
	}

	// 02 §9: backup không được expire trong suốt ETA.
	if src.ETA <= 0 {
		return nil, reject("restore ETA must be > 0 to check backup expiry")
	}
	var expiresAt *string
	if !b.Permanent {
		if src.RetentionFull <= 0 {
			return nil, reject("cannot prove backup %s will not expire within ETA %s: retention full unknown (pass --retention-full)",
				b.Label, src.ETA)
		}
		var fulls []time.Time
		root := time.Time{}
		for _, c := range backups {
			if c.Full && !c.Error {
				fulls = append(fulls, c.StartTime)
			}
		}
		switch {
		case b.Full:
			root = b.StartTime
		case b.FullLabel != "":
			for _, c := range backups {
				if c.Label == b.FullLabel {
					root = c.StartTime
				}
			}
		default: // full gần nhất bắt đầu trước backup
			for _, t := range fulls {
				if !t.After(b.StartTime) && t.After(root) {
					root = t
				}
			}
		}
		if root.IsZero() {
			return nil, reject("cannot find the full backup that %s depends on", b.Label)
		}
		interval := src.FullBackupInterval
		if interval <= 0 {
			interval = inferFullInterval(fulls)
		}
		if interval <= 0 {
			return nil, reject("cannot estimate when backup %s expires: fewer than 2 full backups (pass --full-interval)", b.Label)
		}
		exp := estimateFullExpiry(fulls, root, src.RetentionFull, interval)
		if !now.Add(src.ETA).Before(exp) {
			return nil, reject("backup %s is expected to expire at %s (retention full %d, full every %s), within restore ETA %s",
				b.Label, exp.UTC().Format(time.RFC3339), src.RetentionFull, interval, src.ETA)
		}
		s := exp.UTC().Format(time.RFC3339)
		expiresAt = &s
	}

	// SD-2: archive phủ liên tục [segment bắt đầu backup, segment chứa P] trên timeline của backup
	// (review N08).
	logGap := func(format string, a ...any) error {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError, fmt.Sprintf(kind+": log gap: "+format, a...), nil)
	}
	if !cov.Known {
		return nil, logGap("WAL archive coverage unknown, cannot prove WAL from backup %s to P %s is archived", b.Label, pStr)
	}
	tli := b.Timeline
	if tli == 0 {
		return nil, logGap("SD-2 cannot be proven: backup %s metadata does not record its WAL timeline", b.Label)
	}
	needFrom, needTo := walSegmentNo(b.StartLSN, segSize), walSegmentNo(p, segSize)
	needDesc := walFileName(tli, needFrom, segSize) + ".." + walFileName(tli, needTo, segSize)
	// Timeline mới hơn bắt đầu trước/trong khoảng cần: source có thể đã chuyển timeline (failover/PITR)
	// giữa backup và P, restore (recovery_target_timeline = latest) sẽ theo nhánh đó — metadata archive
	// không cho chứng minh nhánh nào là lịch sử của source → không chứng minh được SD-2.
	for _, iv := range cov.Intervals {
		if iv.TLI > tli && iv.Start <= needTo {
			return nil, logGap("SD-2 cannot be proven: archive has WAL on timeline %d from %s, a timeline switch at or before P %s; "+
				"the history from backup %s (timeline %d) to P cannot be verified from the archive metadata",
				iv.TLI, walFileName(iv.TLI, iv.Start, segSize), pStr, b.Label, tli)
		}
	}
	end, ok := coveredUntil(cov.Intervals, tli, needFrom)
	if !ok || end < needTo {
		return nil, logGap("archive %s does not cover WAL %s (backup %s start %s to P %s) contiguously on timeline %d "+
			"(the segment holding P may not be archived yet: run pg_switch_wal() on the primary, or stream it via primary_conninfo)",
			cov.Describe, needDesc, b.Label, checkpoint.FormatLSN(b.StartLSN), pStr, tli)
	}
	for _, m := range cov.Missing {
		if m.TLI == tli && m.Seg >= needFrom && m.Seg <= needTo {
			return nil, logGap("WAL segment %s between backup start and P is missing from archive", walFileName(m.TLI, m.Seg, segSize))
		}
	}
	if cov.BoundsOnly {
		return nil, logGap("SD-2 cannot be proven: archive metadata %s only gives min/max, not whether every segment of %s is archived "+
			"(pass --archive-info with an archive inventory)", cov.Describe, needDesc)
	}
	archiveUntil := walFileName(tli, end, segSize)

	// "tables": restore vật lý chứa cả cluster → mặc định "*" (mọi bảng). Cách hiểu dùng chung ở
	// SeedCovers (review N06); pattern sai cú pháp bị từ chối ngay ở plan thay vì lặng lẽ không khớp.
	tables := src.Tables
	if len(tables) == 0 {
		tables = []string{"*"}
	}
	if err := ValidateTablePatterns(tables); err != nil {
		return nil, reject("--tables: %v", err)
	}
	ref := refFor(b.Label)
	return &SeedManifest{
		Version:            1,
		Engine:             "pg",
		Kind:               kind,
		ToolVersion:        toolVersion,
		ToolPrerelease:     isPre,
		SourceIdentity:     identity,
		BackupRef:          ref,
		StartPosition:      StartPosition{LSN: pStr, Slot: src.Slot},
		PositionCrossCheck: cross,
		SchemaSnapshot:     ref, // SD-3: catalog nằm trong chính backup vật lý
		Tables:             tables,
		Integrity:          &IntegrityInfo{PrepareOK: true},
		LogCoverage:        &LogCoverageInfo{ArchiveUntil: archiveUntil, OK: true},
		BackupExpiresAt:    expiresAt,
		Target:             target,
		Warnings:           warnings,
		CreatedAt:          now.UTC().Format(time.RFC3339),
	}, nil
}
