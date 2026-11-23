package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMDB_E2E_AutoSeedGroup(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	src.exec(t, "GRANT RELOAD ON *.* TO 'strand'@'%'",
		"CREATE DATABASE IF NOT EXISTS app",
		"CREATE TABLE app.a_keyless (parent_id BIGINT, label VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE app.b_parents (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
		"ALTER TABLE app.a_keyless ADD FOREIGN KEY (parent_id) REFERENCES app.b_parents(id) ON DELETE CASCADE",
		"CREATE TABLE app.c_keyed (id BIGINT PRIMARY KEY, label VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE app.z_payload (payload MEDIUMBLOB NOT NULL) ENGINE=InnoDB",
		"CREATE TABLE app.zz_tail (label VARCHAR(20)) ENGINE=InnoDB",
		"INSERT INTO app.z_payload VALUES (REPEAT('a', 524288)), (REPEAT('b', 524288)), (REPEAT('c', 524288)), (REPEAT('d', 524288))",
		"INSERT INTO app.zz_tail VALUES ('before anchor')",
		"INSERT INTO app.b_parents VALUES (1), (2)",
		"INSERT INTO app.a_keyless VALUES (1, 'duplicate'), (1, 'duplicate'), (2, 'other')",
		"INSERT INTO app.c_keyed VALUES (1, 'snapshot')")
	dst.exec(t, "GRANT REFERENCES ON app.* TO 'strand'@'%'")
	cfg := strandCfg{
		Name: "mdb-auto-seed", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr),
		Include:   []string{"app.*"}, Dir: testDir(t), AutoSeed: true, TablesExtra: klKeylessCfg,
	}
	path := cfg.write(t)
	if out, code := runStrandCLI(t, time.Minute, "validate", "-c", path); code != 0 {
		t.Fatalf("validate auto-seed: exit %d\n%s", code, out)
	}
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitStrandLog(t, p, "auto-seed table copied (awaiting checkpoint commit)", 20*time.Second)
	p.kill9()
	var visible int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.a_keyless").Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("interrupted copy exposed rows: count=%d err=%v", visible, err)
	}
	var applied string
	if err := dst.DB.QueryRow("SELECT applied FROM strand._strand_checkpoint WHERE pipeline_id=?", cfg.Name).Scan(&applied); err != nil || applied != "" {
		t.Fatalf("interrupted copy left a checkpoint: %q err=%v", applied, err)
	}
	dst.exec(t, "INSERT INTO app.zz_tail VALUES ('existing target')")
	out, code := runStrandCLI(t, time.Minute, "sync", "-c", path)
	if code != 3 || !strings.Contains(out, "target app.zz_tail already contains data") {
		t.Fatalf("nonempty target: exit %d, want a clear refusal\n%s", code, out)
	}
	var sentinel string
	if err := dst.DB.QueryRow("SELECT label FROM app.zz_tail").Scan(&sentinel); err != nil || sentinel != "existing target" {
		t.Fatalf("nonempty target changed: %q err=%v", sentinel, err)
	}
	dst.exec(t, "DELETE FROM app.zz_tail")
	p = startStrand(t, path)
	waitStrandLog(t, p, "auto-seed table copied (awaiting checkpoint commit)", 20*time.Second)
	// The next table is throttled. Changing a later table must not leak into the
	// snapshot: CDC will apply the UPDATE from the row image at the anchor.
	src.exec(t, "UPDATE app.zz_tail SET label='during copy'")
	waitStrandLog(t, p, "auto-seed committed", 20*time.Second)
	waitSynced(t, &cfg, p, time.Minute)
	for _, table := range []string{"a_keyless", "b_parents", "c_keyed", "zz_tail"} {
		query := "SELECT * FROM app." + table
		ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, query) })
	}
	src.exec(t, "DELETE FROM app.b_parents WHERE id=1", "UPDATE app.a_keyless SET label='CDC' WHERE parent_id=2",
		"UPDATE app.c_keyed SET label='CDC' WHERE id=1")
	for _, table := range []string{"a_keyless", "b_parents", "c_keyed", "zz_tail"} {
		query := "SELECT * FROM app." + table
		ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, query) })
	}
	if p.stop(t) != 0 {
		t.Fatalf("initial run did not stop cleanly\n%s", p.tail())
	}
	p = startStrand(t, path)
	waitSynced(t, &cfg, p, time.Minute)
	src.exec(t, "INSERT INTO app.a_keyless VALUES (2, 'after restart'), (2, 'after restart')")
	ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, "SELECT * FROM app.a_keyless") })
	if strings.Contains(p.output.String(), "auto-seed committed") {
		t.Fatal("restart ran a second automatic copy")
	}
}
