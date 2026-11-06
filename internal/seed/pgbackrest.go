package seed

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// spec: 02 §9; design "PG seed — pgBackRest"
// Invariants: SD-1, SD-2, SD-3, SD-4

// PgBackRestPlanner lập kế hoạch seed từ `pgbackrest info --output=json` (file đưa vào qua --backup).
//
// SD-2 (review N08): `info` chỉ cho min/max của archive, không cho biết giữa hai biên có thiếu segment
// nào không → không đủ chứng minh WAL liên tục. Cần thêm inventory archive qua --archive-info: JSON của
// `pgbackrest repo-ls archive/<stanza>/<archive-id> --recurse --output=json` (object path → {"type": ...})
// hoặc một mảng JSON các tên file WAL. Thiếu inventory → log_gap ("SD-2 cannot be proven").
type PgBackRestPlanner struct {
	// Now cho phép test cố định thời điểm hiện tại; nil = time.Now.
	Now func() time.Time
}

// NewPgBackRestPlanner tạo PgBackRestPlanner.
func NewPgBackRestPlanner() *PgBackRestPlanner { return &PgBackRestPlanner{} }

// Cấu trúc `pgbackrest info --output=json` (chỉ các trường dùng tới).
type pgbrStanza struct {
	Name   string `json:"name"`
	Status struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"status"`
	DB []struct {
		ID       int        `json:"id"`
		SystemID flexString `json:"system-id"`
		Version  flexString `json:"version"`
	} `json:"db"`
	Archive []struct {
		Database struct {
			ID int `json:"id"`
		} `json:"database"`
		ID  string `json:"id"`
		Min string `json:"min"`
		Max string `json:"max"`
	} `json:"archive"`
	Backup []struct {
		Label    string `json:"label"`
		Type     string `json:"type"` // full | diff | incr
		Error    bool   `json:"error"`
		Backrest struct {
			Version string `json:"version"`
		} `json:"backrest"`
		Database struct {
			ID int `json:"id"`
		} `json:"database"`
		LSN struct {
			Start string `json:"start"`
			Stop  string `json:"stop"`
		} `json:"lsn"`
		// Archive: WAL đầu/cuối cần cho backup; tên file WAL đầu cho biết timeline của backup.
		Archive struct {
			Start string `json:"start"`
			Stop  string `json:"stop"`
		} `json:"archive"`
		Timestamp struct {
			Start int64 `json:"start"`
			Stop  int64 `json:"stop"`
		} `json:"timestamp"`
	} `json:"backup"`
}

// Plan đọc metadata pgBackRest và kiểm SD-1..SD-4 (xem planPGPhysical).
func (pl *PgBackRestPlanner) Plan(_ context.Context, src SeedSource) (*SeedManifest, error) {
	const kind = "pgbackrest"
	data, err := readBackupInfo(kind, src.BackupRef)
	if err != nil {
		return nil, err
	}
	var stanzas []pgbrStanza
	if err := json.Unmarshal(data, &stanzas); err != nil {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("pgbackrest: cannot parse `pgbackrest info --output=json`: %v", err), err)
	}
	if len(stanzas) != 1 {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("pgbackrest: backup info must contain exactly one stanza (got %d; use pgbackrest info --stanza=<name>)", len(stanzas)), nil)
	}
	st := stanzas[0]
	if st.Status.Code != 0 {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("pgbackrest: stanza %s status %d: %s", st.Name, st.Status.Code, st.Status.Message), nil)
	}
	now := time.Now()
	if pl.Now != nil {
		now = pl.Now()
	}
	segSize := src.WALSegmentSize
	if segSize == 0 {
		segSize = defaultWALSegmentSize
	}

	dbVersion := map[int]string{}
	dbSystemID := map[int]string{}
	for _, d := range st.DB {
		dbVersion[d.ID] = string(d.Version)
		dbSystemID[d.ID] = string(d.SystemID)
	}

	backups := make([]pgPhysicalBackup, 0, len(st.Backup))
	for _, b := range st.Backup {
		pb := pgPhysicalBackup{
			Label:       b.Label,
			Full:        strings.EqualFold(b.Type, "full"),
			Error:       b.Error,
			StartTime:   time.Unix(b.Timestamp.Start, 0),
			Version:     dbVersion[b.Database.ID],
			SystemID:    dbSystemID[b.Database.ID],
			ToolVersion: b.Backrest.Version,
		}
		if !pb.Full {
			// Label diff/incr có dạng <full label>_<label riêng>.
			if full, _, ok := strings.Cut(b.Label, "_"); ok {
				pb.FullLabel = full
			}
		}
		if b.LSN.Start != "" {
			if pb.StartLSN, err = checkpoint.ParseLSN(b.LSN.Start); err != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("pgbackrest: backup %s has invalid lsn.start %q", b.Label, b.LSN.Start), err)
			}
		}
		if b.Archive.Start != "" {
			ws, err := parseWALFileName(b.Archive.Start, segSize)
			if err != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("pgbackrest: backup %s has invalid archive.start %q", b.Label, b.Archive.Start), err)
			}
			pb.Timeline = ws.TLI
		}
		if b.LSN.Stop != "" {
			if pb.StopLSN, err = checkpoint.ParseLSN(b.LSN.Stop); err != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("pgbackrest: backup %s has invalid lsn.stop %q", b.Label, b.LSN.Stop), err)
			}
		}
		backups = append(backups, pb)
	}

	// Phạm vi archive: lấy theo database id của cluster hiện tại (db cuối danh sách = sau stanza-upgrade).
	cov := pgArchiveCoverage{}
	archiveID := ""
	if len(st.DB) > 0 {
		cur := st.DB[len(st.DB)-1].ID
		for _, a := range st.Archive {
			if a.Database.ID != cur || a.Min == "" || a.Max == "" {
				continue
			}
			lo, err1 := parseWALFileName(a.Min, segSize)
			hi, err2 := parseWALFileName(a.Max, segSize)
			if err1 != nil || err2 != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("pgbackrest: invalid archive range %s..%s", a.Min, a.Max), nil)
			}
			// Chỉ có biên: không biết bên trong có lỗ không (BoundsOnly) — dải ở đây chỉ để báo lỗi rõ hơn.
			ivs := []walInterval{{TLI: lo.TLI, Start: lo.Seg, End: lo.Seg}, {TLI: hi.TLI, Start: hi.Seg, End: hi.Seg}}
			if lo.TLI == hi.TLI {
				ivs = []walInterval{{TLI: lo.TLI, Start: lo.Seg, End: hi.Seg}}
			}
			archiveID = a.ID
			cov = pgArchiveCoverage{Known: true, BoundsOnly: true, Intervals: ivs,
				Describe: fmt.Sprintf("%s..%s (pgbackrest info min/max)", a.Min, a.Max)}
		}
	}
	if src.ArchiveInfoRef != "" {
		raw, err := readBackupInfo(kind, src.ArchiveInfoRef)
		if err != nil {
			return nil, err
		}
		segs, err := parsePgBackRestArchiveInventory(raw, archiveID, segSize)
		if err != nil {
			return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
				fmt.Sprintf("pgbackrest: cannot parse archive inventory %s: %v", src.ArchiveInfoRef, err), err)
		}
		ivs := intervalsFromSegments(segs)
		cov = pgArchiveCoverage{Known: len(ivs) > 0, Intervals: ivs, Describe: describeIntervals(ivs, segSize)}
	}

	return planPGPhysical(kind, src, backups, cov, now, func(label string) string {
		return "pgbackrest://" + st.Name + "/" + label
	})
}

// parsePgBackRestArchiveInventory đọc inventory archive: JSON của `pgbackrest repo-ls --recurse
// --output=json` (object: đường dẫn → {"type": "file"|"path"|"link", ...}) hoặc mảng JSON các đường dẫn /
// tên file. Chỉ lấy file segment WAL (24 hex, tùy chọn "-<sha1>" và đuôi nén); bỏ .history, .backup,
// .partial. Nếu đường dẫn có thành phần archive id (vd "16-1") khác archiveID thì bỏ qua (cluster cũ
// trước stanza-upgrade).
func parsePgBackRestArchiveInventory(raw []byte, archiveID string, segSize uint64) ([]walSeg, error) {
	var paths []string
	var obj map[string]struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &obj); err == nil {
		for p, e := range obj {
			if e.Type == "" || e.Type == "file" {
				paths = append(paths, p)
			}
		}
	} else if err2 := json.Unmarshal(raw, &paths); err2 != nil {
		return nil, fmt.Errorf("want a `pgbackrest repo-ls --output=json` object or a JSON array of WAL file names: %v", err)
	}
	var segs []walSeg
	for _, p := range paths {
		parts := strings.Split(strings.Trim(p, "/"), "/")
		skip := false
		for _, c := range parts[:len(parts)-1] {
			if archiveID != "" && isPgBackRestArchiveID(c) && c != archiveID {
				skip = true
			}
		}
		base := parts[len(parts)-1]
		if skip || !isWALSegmentFile(base) {
			continue
		}
		ws, err := parseWALFileName(base[:24], segSize)
		if err != nil {
			return nil, err
		}
		segs = append(segs, ws)
	}
	return segs, nil
}

// isPgBackRestArchiveID: thư mục archive id của pgBackRest có dạng <major>-<id>, vd "16-1".
func isPgBackRestArchiveID(s string) bool {
	maj, id, ok := strings.Cut(s, "-")
	return ok && maj != "" && id != "" && strings.Trim(maj, "0123456789.") == "" && strings.Trim(id, "0123456789") == ""
}

// isWALSegmentFile: tên file segment WAL trong repo — 24 hex, tùy chọn "-<40 hex>" và đuôi nén.
func isWALSegmentFile(name string) bool {
	if len(name) < 24 {
		return false
	}
	for _, c := range name[:24] {
		if !strings.ContainsRune("0123456789ABCDEFabcdef", c) {
			return false
		}
	}
	rest := name[24:]
	if strings.HasPrefix(rest, "-") {
		if len(rest) < 41 {
			return false
		}
		for _, c := range rest[1:41] {
			if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
				return false
			}
		}
		rest = rest[41:]
	}
	switch rest {
	case "", ".gz", ".lz4", ".zst", ".bz2", ".xz":
		return true
	}
	return false
}
