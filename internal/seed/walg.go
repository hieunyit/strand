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

// spec: 02 §9; design "PG seed — WAL-G"
// Invariants: SD-1, SD-2, SD-3, SD-4

// WalgPlanner lập kế hoạch seed từ `wal-g backup-list --detail --json` (file qua --backup) và
// `wal-g wal-show --detailed-json` (file qua --archive-info) — WAL-G không ghi phạm vi archive trong
// backup-list nên thiếu --archive-info thì không chứng minh được SD-2 (log_gap).
type WalgPlanner struct {
	// Now cho phép test cố định thời điểm hiện tại; nil = time.Now.
	Now func() time.Time
}

// NewWalgPlanner tạo WalgPlanner.
func NewWalgPlanner() *WalgPlanner { return &WalgPlanner{} }

// walgBackup là một phần tử của `wal-g backup-list --detail --json`.
type walgBackup struct {
	BackupName       string     `json:"backup_name"`
	Time             string     `json:"time"`
	StartTime        string     `json:"start_time"`
	PGVersion        flexString `json:"pg_version"`
	StartLSN         flexString `json:"start_lsn"`
	FinishLSN        flexString `json:"finish_lsn"`
	IsPermanent      bool       `json:"is_permanent"`
	SystemIdentifier flexString `json:"system_identifier"`
}

// walgTimeline là một phần tử của `wal-g wal-show --detailed-json`.
type walgTimeline struct {
	ID              int      `json:"id"`
	StartSegment    string   `json:"start_segment"`
	EndSegment      string   `json:"end_segment"`
	MissingSegments []string `json:"missing_segments"`
	Status          string   `json:"status"`
}

func parseWalgTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
	if err != nil {
		return time.Time{}
	}
	return t
}

// Plan đọc metadata WAL-G và kiểm SD-1..SD-4 (xem planPGPhysical).
func (pl *WalgPlanner) Plan(_ context.Context, src SeedSource) (*SeedManifest, error) {
	const kind = "walg"
	data, err := readBackupInfo(kind, src.BackupRef)
	if err != nil {
		return nil, err
	}
	var list []walgBackup
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
			fmt.Sprintf("walg: cannot parse `wal-g backup-list --detail --json`: %v", err), err)
	}
	if len(list) == 0 {
		return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected, "walg: backup list is empty", nil)
	}
	now := time.Now()
	if pl.Now != nil {
		now = pl.Now()
	}
	segSize := src.WALSegmentSize
	if segSize == 0 {
		segSize = defaultWALSegmentSize
	}

	backups := make([]pgPhysicalBackup, 0, len(list))
	for _, b := range list {
		pb := pgPhysicalBackup{
			Label: b.BackupName,
			// Delta backup có tên base_<wal>_D_<wal gốc>.
			Full:      !strings.Contains(b.BackupName, "_D_"),
			Permanent: b.IsPermanent,
			StartTime: parseWalgTime(b.StartTime),
			Version:   string(b.PGVersion),
			SystemID:  string(b.SystemIdentifier),
		}
		if pb.StartTime.IsZero() {
			pb.StartTime = parseWalgTime(b.Time)
		}
		if b.StartLSN != "" {
			if pb.StartLSN, err = checkpoint.ParseLSN(string(b.StartLSN)); err != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("walg: backup %s has invalid start_lsn %q", b.BackupName, b.StartLSN), err)
			}
		}
		if b.FinishLSN != "" {
			if pb.StopLSN, err = checkpoint.ParseLSN(string(b.FinishLSN)); err != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("walg: backup %s has invalid finish_lsn %q", b.BackupName, b.FinishLSN), err)
			}
		}
		backups = append(backups, pb)
	}

	cov := pgArchiveCoverage{}
	if src.ArchiveInfoRef != "" {
		raw, err := readBackupInfo(kind, src.ArchiveInfoRef)
		if err != nil {
			return nil, err
		}
		var tls []walgTimeline
		if err := json.Unmarshal(raw, &tls); err != nil {
			return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
				fmt.Sprintf("walg: cannot parse `wal-g wal-show --detailed-json`: %v", err), err)
		}
		// Gộp các timeline thành một dải segment (số segment không phụ thuộc timeline; timeline mới
		// tiếp nối timeline cha tại switch point).
		for _, tl := range tls {
			if tl.StartSegment == "" || tl.EndSegment == "" {
				continue
			}
			lo, err1 := parseWALFileName(tl.StartSegment, segSize)
			hi, err2 := parseWALFileName(tl.EndSegment, segSize)
			if err1 != nil || err2 != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("walg: invalid timeline %d segment range %s..%s", tl.ID, tl.StartSegment, tl.EndSegment), nil)
			}
			if !cov.Known || lo < cov.MinSeg {
				cov.MinSeg = lo
			}
			if !cov.Known || hi > cov.MaxSeg {
				cov.MaxSeg, cov.MaxName = hi, tl.EndSegment
			}
			cov.Known = true
			for _, m := range tl.MissingSegments {
				n, err := parseWALFileName(m, segSize)
				if err != nil {
					return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
						fmt.Sprintf("walg: invalid missing segment %q", m), err)
				}
				cov.Missing = append(cov.Missing, n)
			}
		}
		if cov.Known {
			cov.Describe = fmt.Sprintf("segments %d..%d", cov.MinSeg, cov.MaxSeg)
		}
	}

	return planPGPhysical(kind, src, backups, cov, now, func(label string) string {
		return "wal-g://" + label
	})
}
