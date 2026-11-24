package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Operators start over by dropping the target databases, the checkpoint
// database included, while the pipeline keeps its name and local state
// directory. Without a checkpoint the pipeline must seed from scratch.
func TestMDB_E2E_AutoSeedAfterTargetWipedKeepsWorking(t *testing.T) {
	// legacy: a state file written before target_epoch existed (the
	// per-table fallback must then notice the dropped target tables).
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) { testAutoSeedAfterTargetWiped(t, legacy) })
	}
}

func testAutoSeedAfterTargetWiped(t *testing.T, legacy bool) {
	src := startMariaDBImage(t, mdbImage(), "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	dst.exec(t, "GRANT REFERENCES ON app.* TO 'strand'@'%'")
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app",
		"CREATE TABLE app.dm_scenario (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE app.dm_step (id BIGINT PRIMARY KEY, scenario_id BIGINT, CONSTRAINT fk_step_scenario FOREIGN KEY (scenario_id) REFERENCES app.dm_scenario(id) ON DELETE CASCADE ON UPDATE CASCADE) ENGINE=InnoDB",
		"CREATE TABLE app.dm_step_config (id BIGINT PRIMARY KEY, step_id BIGINT, scenario_id BIGINT,"+
			" CONSTRAINT fk_config_step FOREIGN KEY (step_id) REFERENCES app.dm_step(id) ON DELETE CASCADE ON UPDATE CASCADE,"+
			" CONSTRAINT fk_config_scenario FOREIGN KEY (scenario_id) REFERENCES app.dm_scenario(id) ON DELETE CASCADE) ENGINE=InnoDB",
		"CREATE TABLE app.plain (id BIGINT PRIMARY KEY, v VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE app.keyless (v VARCHAR(20)) ENGINE=InnoDB",
		"INSERT INTO app.dm_scenario VALUES (1), (2)", "INSERT INTO app.dm_step VALUES (10, 1), (20, 2)",
		"INSERT INTO app.dm_step_config VALUES (100, 10, 1), (200, 20, 2)",
		"INSERT INTO app.plain VALUES (1, 'a')", "INSERT INTO app.keyless VALUES ('x')")
	cfg := strandCfg{
		Name: "mdb-wiped", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr),
		Include:   []string{"app.*"}, Dir: testDir(t), AutoSeed: true, TablesExtra: klKeylessCfg,
	}
	path := cfg.write(t)
	check := func(p *strandProc) {
		t.Helper()
		for _, q := range []string{"SELECT * FROM app.dm_scenario ORDER BY id", "SELECT * FROM app.dm_step ORDER BY id",
			"SELECT * FROM app.dm_step_config ORDER BY id", "SELECT * FROM app.plain ORDER BY id", "SELECT * FROM app.keyless"} {
			ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, q) })
		}
	}
	p := startStrand(t, path)
	waitSynced(t, &cfg, p, 2*time.Minute)
	check(p)
	if p.stop(t) != 0 {
		t.Fatalf("first run did not stop cleanly\n%s", p.tail())
	}

	dst.exec(t, "DROP DATABASE app", "DROP DATABASE strand", "CREATE DATABASE strand")
	if legacy {
		statePath := filepath.Join(cfg.Dir, "state", "pipeline-state.json")
		raw, err := os.ReadFile(statePath)
		if err != nil {
			t.Fatal(err)
		}
		var state map[string]any
		if err := json.Unmarshal(raw, &state); err != nil {
			t.Fatal(err)
		}
		if _, ok := state["target_epoch"]; !ok {
			t.Fatalf("state has no target_epoch: %s", raw)
		}
		delete(state, "target_epoch")
		if raw, err = json.Marshal(state); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(statePath, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	src.exec(t, "INSERT INTO app.plain VALUES (2, 'after wipe')", "INSERT INTO app.dm_step_config VALUES (300, 10, 1)")
	p = startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 2*time.Minute)
	check(p)
	src.exec(t, "DELETE FROM app.dm_scenario WHERE id=1")
	check(p)
}
