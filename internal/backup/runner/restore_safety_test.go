package runner

import (
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
)

// mysqlbinlog --stop-datetime truncates fractional seconds, so a MySQL point-in-time target with a
// fraction would replay up to a second less than asked. It is refused, as for MongoDB.
func TestMySQLPITRRefusesFractionalSecondTarget(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	end := now.Add(-time.Hour)
	base := backup.Record{ID: "base", Source: "prod", Engine: backup.EngineMySQL, Method: backup.MethodMySQLDump,
		Status: backup.StatusComplete, JobID: "job", CreatedAt: end.Add(-time.Minute), StartTime: &end, EndTime: &end,
		Position: backup.Position{BinlogFile: "binlog.000001", BinlogPos: 100, GTIDSet: "server:1"},
		Details:  map[string]string{"source_identity": "server1", "databases": "shop"}}
	log := backup.Record{ID: "log", Source: base.Source, Engine: base.Engine, Method: backup.MethodBinlog,
		Kind: backup.KindLog, Status: backup.StatusComplete, JobID: base.JobID, CreatedAt: now, EndTime: &now,
		Position:    backup.Position{BinlogFile: base.Position.BinlogFile, BinlogPos: 4},
		EndPosition: backup.Position{BinlogFile: base.Position.BinlogFile, BinlogPos: 200},
		Details:     map[string]string{"source_identity": "server1", "seq": "1"}}
	whole := now.Add(-time.Minute)
	if _, err := PlanRestore([]backup.Record{base, log}, nil, &whole); err != nil {
		t.Fatalf("whole-second target: %v", err)
	}
	fractional := whole.Add(250 * time.Millisecond)
	for _, chosen := range []*backup.Record{nil, &base} {
		if plan, err := PlanRestore([]backup.Record{base, log}, chosen, &fractional); err == nil || !strings.Contains(err.Error(), "whole-second") {
			t.Fatalf("fractional target planned (chosen %v): %+v %v", chosen != nil, plan, err)
		}
	}
	env := &Env{}
	plan := RestorePlan{Chain: []backup.Record{base}, Logs: []backup.Record{log}, Target: &fractional}
	if err := env.Restore(t.Context(), plan, RestoreTarget{Database: "shop_restore"}); err == nil || !strings.Contains(err.Error(), "whole-second") {
		t.Fatalf("restore accepted a fractional MySQL target: %v", err)
	}
}
