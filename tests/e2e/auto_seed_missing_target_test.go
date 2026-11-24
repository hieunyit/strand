package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A pipeline that already has a checkpoint does not seed again. When copied
// target tables were dropped since (to start over), the error must say that,
// not only report a missing foreign key whose fix (create_schema) cannot apply.
func TestMDB_E2E_AutoSeedCheckpointWithDroppedTargetTable(t *testing.T) {
	src := startMariaDBImage(t, mdbImage(), "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	dst.exec(t, "GRANT REFERENCES ON app.* TO 'strand'@'%'")
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app",
		"CREATE TABLE app.dm_step (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE app.dm_step_config (id BIGINT PRIMARY KEY, step_id BIGINT,"+
			" CONSTRAINT fk_step_config FOREIGN KEY (step_id) REFERENCES app.dm_step(id) ON DELETE CASCADE ON UPDATE CASCADE) ENGINE=InnoDB",
		"INSERT INTO app.dm_step VALUES (1), (2)", "INSERT INTO app.dm_step_config VALUES (10, 1), (20, 2)")
	cfg := strandCfg{
		Name: "mdb-seed-dropped", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr),
		Include:   []string{"app.*"}, Dir: testDir(t), AutoSeed: true, TablesExtra: klKeylessCfg,
	}
	path := cfg.write(t)
	p := startStrand(t, path)
	waitStrandLog(t, p, "auto-seed committed", time.Minute)
	waitSynced(t, &cfg, p, time.Minute)
	if p.stop(t) != 0 {
		t.Fatalf("pipeline did not stop cleanly\n%s", p.tail())
	}
	dst.exec(t, "DROP TABLE app.dm_step_config")
	out, code := runStrandCLI(t, time.Minute, "sync", "-c", path)
	if code == 0 || !strings.Contains(out, "already has a checkpoint") || !strings.Contains(out, "app.dm_step_config") {
		t.Fatalf("restart with a dropped target table: exit %d, want the checkpoint explained\n%s", code, out)
	}
}
