package runner

import (
	"testing"
	"time"

	"strand/internal/backup"
)

func TestPITREndNeedsAnAnchoredContinuousRun(t *testing.T) {
	at := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	details := map[string]string{"source_identity": "sys-1", "timeline": "1"}
	base := backup.Record{ID: "base", Source: "pg", Engine: backup.EnginePostgres, Status: backup.StatusComplete, CreatedAt: at,
		JobID: "job", Method: backup.MethodPGBaseBackup, Kind: backup.KindFull, StartTime: &at,
		Position: backup.Position{LSN: "0/3000028"}, EndPosition: backup.Position{LSN: "0/3000100"}, Details: details}
	wal := func(id, from, to string, end time.Time) backup.Record {
		return backup.Record{ID: id, Source: "pg", Engine: backup.EnginePostgres, Status: backup.StatusComplete, CreatedAt: end,
			JobID: "job", Method: backup.MethodWAL, Kind: backup.KindLog, EndTime: &end,
			Position: backup.Position{LSN: from}, EndPosition: backup.Position{LSN: to}, Details: details}
	}
	w1 := wal("w1", "0/3000000", "0/4000000", at.Add(10*time.Minute))
	w2 := wal("w2", "0/4000000", "0/5000000", at.Add(20*time.Minute))
	w3 := wal("w3", "0/5000000", "0/6000000", at.Add(30*time.Minute))
	end := func(rs ...backup.Record) string {
		if e := PITREnd(rs); e != nil {
			return e.Format("15:04")
		}
		return "none"
	}
	if got := end(base, w1, w2, w3); got != "12:30" {
		t.Fatalf("continuous = %s", got)
	}
	if got := end(base, w1, w3); got != "12:10" {
		t.Fatalf("gap: credit stops at the gap, got %s", got)
	}
	if got := end(w1, w2, w3); got != "none" {
		t.Fatalf("no base = %s", got)
	}
	replica := base
	replica.ID, replica.CopyOf = "base-copy", "base-gone"
	if got := end(replica, w1, w2); got != "none" {
		t.Fatalf("a replica is not a PITR base, got %s", got)
	}
	if got := end(base, w2, w3); got != "none" {
		t.Fatalf("logs that do not reach back over the base = %s", got)
	}
}
