package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A MariaDB source account without RELOAD, cascades from one parent that reach
// the same child through several constraints, and a target table created
// beforehand without its cascading foreign key: auto-seed must copy, add the
// missing constraint to the empty target table and replay cascades exactly.
func TestMDB_E2E_AutoSeedCascadesWithoutReload(t *testing.T) {
	src := startMariaDBImage(t, mdbImage(), "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src) // REPLICATION SLAVE/CLIENT, SELECT and LOCK TABLES: no RELOAD
	mdbTargetSetup(t, dst)
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app", "CREATE DATABASE extra",
		"GRANT SELECT, LOCK TABLES ON extra.* TO 'strand'@'%'",
		"CREATE TABLE app.customer (id BIGINT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE app.customer_attribute (id BIGINT PRIMARY KEY, customer_id BIGINT, v VARCHAR(20), CONSTRAINT fk_ca FOREIGN KEY (customer_id) REFERENCES app.customer(id) ON DELETE CASCADE) ENGINE=InnoDB",
		"CREATE TABLE app.dm_scenario (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE app.dm_step (id BIGINT PRIMARY KEY, scenario_id BIGINT, CONSTRAINT fk_step_scenario FOREIGN KEY (scenario_id) REFERENCES app.dm_scenario(id) ON DELETE CASCADE) ENGINE=InnoDB",
		"CREATE TABLE app.dm_log_scenario (id BIGINT PRIMARY KEY, scenario_id BIGINT, step_id BIGINT, note VARCHAR(20),"+
			" CONSTRAINT fk_log_scenario FOREIGN KEY (scenario_id) REFERENCES app.dm_scenario(id) ON DELETE CASCADE,"+
			" CONSTRAINT fk_log_step FOREIGN KEY (step_id) REFERENCES app.dm_step(id) ON DELETE SET NULL) ENGINE=InnoDB",
		"CREATE TABLE app.plain (id BIGINT PRIMARY KEY, v VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE app.plain_note (id BIGINT PRIMARY KEY, plain_id BIGINT, CONSTRAINT note_parent_fk FOREIGN KEY (plain_id) REFERENCES app.plain(id) ON DELETE RESTRICT)",
		"CREATE TABLE extra.customer_note (id BIGINT PRIMARY KEY, parent_id BIGINT, CONSTRAINT fk_external_customer FOREIGN KEY (parent_id) REFERENCES app.customer(id) ON DELETE CASCADE)",
		"INSERT INTO app.customer VALUES (1,'a'),(2,'b')",
		"INSERT INTO app.customer_attribute VALUES (10,1,'x'),(11,1,'y'),(20,2,'z')",
		"INSERT INTO app.dm_scenario VALUES (1),(2)",
		"INSERT INTO app.dm_step VALUES (100,1),(200,2)",
		"INSERT INTO app.dm_log_scenario VALUES (1000,1,100,'s1'),(1001,2,100,'cross'),(2000,2,200,'s2')",
		"INSERT INTO app.plain VALUES (1,'p')",
		"INSERT INTO app.plain_note VALUES (1,1)",
		"INSERT INTO extra.customer_note VALUES (10,1),(20,2)")
	// Created before Strand, without the cascading foreign key (as a manual or tool-made schema).
	dst.exec(t, "CREATE TABLE app.customer (id BIGINT PRIMARY KEY, name VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE app.customer_attribute (id BIGINT PRIMARY KEY, customer_id BIGINT, v VARCHAR(20)) ENGINE=InnoDB",
		"GRANT REFERENCES ON app.* TO 'strand'@'%'",
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON extra.* TO 'strand'@'%'")
	cfg := strandCfg{
		Name: "mdb-auto-seed-cascade", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr),
		Include:   []string{"app.*", "extra.*"}, Dir: testDir(t), AutoSeed: true,
		TablesExtra: klKeylessCfg + "  rename: {extra.customer_note: extra.customer_note_copy}\n",
	}
	path := cfg.write(t)
	dst.exec(t, "ALTER TABLE app.customer_attribute ADD CONSTRAINT unexpected_restrict FOREIGN KEY (customer_id) REFERENCES app.customer(id) ON DELETE RESTRICT")
	out, code := runStrandCLI(t, time.Minute, "sync", "-c", path)
	if code != 3 || !strings.Contains(out, "unexpected foreign key unexpected_restrict") {
		t.Fatalf("incompatible target: exit %d, want foreign-key preflight rejection\n%s", code, out)
	}
	var untouched int
	if err := dst.DB.QueryRow(`SELECT
		(SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='app' AND TABLE_NAME='dm_scenario') +
		(SELECT COUNT(*) FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='app' AND CONSTRAINT_NAME='fk_ca')`).Scan(&untouched); err != nil || untouched != 0 {
		t.Fatalf("preflight altered the target on failure: %d, err=%v", untouched, err)
	}
	dst.exec(t, "ALTER TABLE app.customer_attribute DROP FOREIGN KEY unexpected_restrict")
	// Although this FK is outside the cascade component, native cascade
	// enables FOREIGN_KEY_CHECKS globally and the extra FK would block CDC.
	dst.exec(t, "CREATE TABLE app.plain (id BIGINT PRIMARY KEY, v VARCHAR(20)) ENGINE=InnoDB",
		"CREATE TABLE app.isolated_extra (id BIGINT PRIMARY KEY, plain_id BIGINT, CONSTRAINT isolated_restrict FOREIGN KEY (plain_id) REFERENCES app.plain(id) ON DELETE RESTRICT)")
	out, code = runStrandCLI(t, time.Minute, "sync", "-c", path)
	if code != 3 || !strings.Contains(out, "unexpected foreign key isolated_restrict") {
		t.Fatalf("unrelated target RESTRICT with native checks: exit %d\n%s", code, out)
	}
	dst.exec(t, "DROP TABLE app.isolated_extra")
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitStrandLog(t, p, "auto-seed committed", 2*time.Minute)
	if !strings.Contains(p.output.String(), "added a cascading foreign key missing on the empty target table") {
		t.Fatalf("missing target constraint was not added\n%s", p.tail())
	}
	var rule string
	if err := dst.DB.QueryRow("SELECT DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA='app' AND CONSTRAINT_NAME='fk_ca'").Scan(&rule); err != nil || rule != "CASCADE" {
		t.Fatalf("target fk_ca: rule=%q err=%v", rule, err)
	}
	waitSynced(t, &cfg, p, time.Minute)
	tables := []string{"customer", "customer_attribute", "dm_scenario", "dm_step", "dm_log_scenario", "plain", "plain_note"}
	same := func() {
		t.Helper()
		for _, table := range tables {
			query := "SELECT * FROM app." + table + " ORDER BY 1"
			ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, query) })
		}
	}
	same()
	src.exec(t, "DELETE FROM app.dm_scenario WHERE id=1", // reaches dm_log_scenario directly and through dm_step
		"DELETE FROM app.customer WHERE id=1",
		"UPDATE app.dm_log_scenario SET note='cdc' WHERE id=2000",
		"INSERT INTO app.plain VALUES (2,'q')")
	same()
	ukWaitEqual(t, p, time.Minute, func() string {
		srcRows, srcErr := src.DB.Query("SELECT id FROM extra.customer_note ORDER BY id")
		if srcErr != nil {
			return srcErr.Error()
		}
		defer srcRows.Close()
		var sourceID, targetID int
		if !srcRows.Next() || srcRows.Scan(&sourceID) != nil || srcRows.Next() || srcRows.Err() != nil {
			return "unexpected source rows after cross-schema cascade"
		}
		if err := dst.DB.QueryRow("SELECT id FROM extra.customer_note_copy ORDER BY id LIMIT 1").Scan(&targetID); err != nil || sourceID != targetID {
			return fmt.Sprintf("cross-schema cascade: source id=%d target id=%d err=%v", sourceID, targetID, err)
		}
		return ""
	})
	var left int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.dm_log_scenario WHERE scenario_id=1 OR step_id=100").Scan(&left); err != nil || left != 0 {
		t.Fatalf("cascades not replayed on target: %d rows left, err=%v", left, err)
	}
	if p.stop(t) != 0 {
		t.Fatalf("pipeline did not stop cleanly\n%s", p.tail())
	}
	dst.exec(t, "ALTER TABLE app.dm_log_scenario ADD CONSTRAINT unexpected_restrict FOREIGN KEY (scenario_id) REFERENCES app.dm_scenario(id) ON DELETE RESTRICT")
	out, code = runStrandCLI(t, time.Minute, "sync", "-c", path)
	if code != 3 || !strings.Contains(out, "unexpected foreign key unexpected_restrict") {
		t.Fatalf("resume with an incompatible restrictive foreign key: exit %d\n%s", code, out)
	}
	dst.exec(t, "ALTER TABLE app.dm_log_scenario DROP FOREIGN KEY unexpected_restrict")
	// Restart keeps the pinned constraint names.
	p = startStrand(t, path)
	waitSynced(t, &cfg, p, time.Minute)
	src.exec(t, "DELETE FROM app.dm_scenario WHERE id=2")
	same()
	if strings.Count(p.output.String(), "auto-seed committed") != 0 {
		t.Fatal("restart ran a second automatic copy")
	}
	if p.stop(t) != 0 {
		t.Fatalf("restart did not stop cleanly\n%s", p.tail())
	}
	dst.exec(t, "CREATE TABLE app.target_only (id BIGINT PRIMARY KEY, parent_id BIGINT, CONSTRAINT target_only_fk FOREIGN KEY(parent_id) REFERENCES app.plain(id) ON DELETE CASCADE)")
	cfg.Name, cfg.Dir, cfg.Include, cfg.AutoSeed = "target-only-cascade", testDir(t), []string{"app.plain"}, false
	cfg.TablesExtra = ""
	out, code = runStrandCLI(t, time.Minute, "sync", "-c", cfg.write(t))
	if code != 3 || !strings.Contains(out, "unexpected cascading foreign key target_only_fk") {
		t.Fatalf("a target-only cascade must be rejected even without any source cascade: exit %d\n%s", code, out)
	}
}
