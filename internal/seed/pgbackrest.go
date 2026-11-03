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
	if len(st.DB) > 0 {
		cur := st.DB[len(st.DB)-1].ID
		for _, a := range st.Archive {
			if a.Database.ID != cur || a.Min == "" || a.Max == "" {
				continue
			}
			minSeg, err1 := parseWALFileName(a.Min, segSize)
			maxSeg, err2 := parseWALFileName(a.Max, segSize)
			if err1 != nil || err2 != nil {
				return nil, exitcode.New(exitcode.ErrSeedInvariantFailed, exitcode.Rejected,
					fmt.Sprintf("pgbackrest: invalid archive range %s..%s", a.Min, a.Max), nil)
			}
			cov = pgArchiveCoverage{Known: true, MinSeg: minSeg, MaxSeg: maxSeg, MaxName: a.Max,
				Describe: fmt.Sprintf("%s..%s", a.Min, a.Max)}
		}
	}

	return planPGPhysical(kind, src, backups, cov, now, func(label string) string {
		return "pgbackrest://" + st.Name + "/" + label
	})
}
