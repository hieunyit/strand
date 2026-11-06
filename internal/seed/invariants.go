package seed

import (
	"fmt"
	"strings"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// spec: 02 §2 (SD-1..SD-4), §9; design §3.1, §3.3

// parseGTIDSetStrict phân tích GTID set lấy từ metadata/tham số; hỏng thì từ chối (SD-1).
func parseGTIDSetStrict(what, s string) (*checkpoint.MySQLAppliedSet, error) {
	set, err := checkpoint.ParseMySQLGTIDSet(s)
	if err != nil {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("%s: invalid GTID set %q: %v", what, s, err), err)
	}
	return set, nil
}

// checkPositionP kiểm P hợp lệ và không có khoảng trống.
func checkPositionP(kind, p string) (*checkpoint.MySQLAppliedSet, error) {
	pSet, err := parseGTIDSetStrict(kind+" backup position P", p)
	if err != nil {
		return nil, err
	}
	if len(pSet.String()) == 0 {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("%s backup position P is empty", kind), nil)
	}
	// review R33: kiểm theo interval, không liệt kê từng GNO (khoảng trống lớn sẽ treo/OOM).
	if pSet.HasHoles() {
		return nil, exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("%s backup GTID set %q contains %d holes: corrupted or non-contiguous log", kind, p, pSet.HoleCount()), nil)
	}
	return pSet, nil
}

// checkCrossCheck so P với gtid_executed của instance sau restore (SD-1).
func checkCrossCheck(pSet *checkpoint.MySQLAppliedSet, p, restored string) (*PositionCrossCheck, error) {
	if strings.TrimSpace(restored) == "" {
		return nil, nil
	}
	rSet, err := parseGTIDSetStrict("restored gtid_executed", restored)
	if err != nil {
		return nil, err
	}
	if !pSet.Equal(rSet) {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("position cross-check mismatch: restored gtid_executed (%s) != backup P (%s)", restored, p), nil)
	}
	return &PositionCrossCheck{RestoredGTIDExecuted: restored, Match: true}, nil
}

// checkLogCoverage kiểm SD-2: mọi GTID source đã purge mà nằm ngoài P phải có trong archive
// (design §3.3: "P không giao với gtid_purged; nếu có giao thì archive phải phủ phần giao đó").
func checkLogCoverage(pSet *checkpoint.MySQLAppliedSet, purged, archive string) error {
	if strings.TrimSpace(purged) == "" {
		return nil
	}
	purgedSet, err := parseGTIDSetStrict("source gtid_purged", purged)
	if err != nil {
		return err
	}
	missing := purgedSet.Clone().(*checkpoint.MySQLAppliedSet)
	missing.Subtract(pSet)
	if missing.String() == "" {
		return nil
	}
	if strings.TrimSpace(archive) == "" {
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("log gap: source purged %s after P and no archive is available", missing.String()), nil)
	}
	archiveSet, err := parseGTIDSetStrict("archive coverage", archive)
	if err != nil {
		return err
	}
	if !missing.IsSubsetOf(archiveSet) {
		uncovered := missing.Clone().(*checkpoint.MySQLAppliedSet)
		uncovered.Subtract(archiveSet)
		return exitcode.New(exitcode.ErrLogGap, exitcode.DataError,
			fmt.Sprintf("log gap: %s purged on source and not in archive (%s)", uncovered.String(), archive), nil)
	}
	return nil
}

// versionLine trả về dòng version MySQL: "8.0", "8.4", hoặc "9" cho mọi bản 9.x (design §3.1).
func versionLine(v string) string {
	v = strings.TrimSpace(v)
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 || parts[0] == "" {
		return ""
	}
	if parts[0] == "9" {
		return "9"
	}
	return parts[0] + "." + parts[1]
}

// normalizeArch đưa tên kiến trúc về một dạng chuẩn: MySQL báo @@version_compile_machine là
// "x86_64"/"aarch64" (Linux) hoặc "arm64" (macOS), Go báo "amd64"/"arm64" (review R32).
func normalizeArch(a string) string {
	a = strings.ToLower(strings.TrimSpace(a))
	switch a {
	case "amd64", "x86-64", "x64":
		return "x86_64"
	case "arm64":
		return "aarch64"
	}
	return a
}

// checkPhysicalCompat kiểm SD-4 cho backup vật lý: cùng dòng version và cùng kiến trúc CPU.
// Metadata XtraBackup không ghi kiến trúc nên kiến trúc source phải được cung cấp (đọc từ source
// hoặc khai báo); không biết kiến trúc thì không chứng minh được SD-4 nên từ chối (02 §2, §9;
// review R32).
func checkPhysicalCompat(sourceVersion, targetVersion, sourceArch, targetArch string) (TargetInfo, error) {
	src, dst := versionLine(sourceVersion), versionLine(targetVersion)
	if src == "" || dst == "" {
		return TargetInfo{}, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-4: cannot prove target compatibility (backup server version %q, target version %q)",
				sourceVersion, targetVersion), nil)
	}
	if src != dst {
		return TargetInfo{}, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-4: physical backup from MySQL %s line cannot be restored on %s line target",
				sourceVersion, targetVersion), nil)
	}
	sa, ta := normalizeArch(sourceArch), normalizeArch(targetArch)
	if sa == "" || ta == "" {
		return TargetInfo{}, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-4: CPU architecture not verified (backup source arch %q, target arch %q): "+
				"pass --source and --target (reads @@version_compile_machine) or --source-arch",
				sourceArch, targetArch), nil)
	}
	if sa != ta {
		return TargetInfo{}, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("SD-4: physical backup from %s server cannot be restored on %s target", sourceArch, targetArch), nil)
	}
	return TargetInfo{Version: targetVersion, Compatible: true, Arch: ta}, nil
}
