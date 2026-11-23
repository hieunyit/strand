package e2e

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

func TestMDB_E2E_SeededForeignKeyCascade(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	for _, db := range []*mysqlNode{src, dst} {
		db.exec(t, "CREATE DATABASE IF NOT EXISTS app",
			"CREATE TABLE app.parents(id bigint PRIMARY KEY)",
			"CREATE TABLE app.phones(id bigint PRIMARY KEY, parent_id bigint NOT NULL, FOREIGN KEY(parent_id) REFERENCES app.parents(id) ON UPDATE CASCADE ON DELETE CASCADE)",
			"CREATE TABLE app.attrs(id bigint PRIMARY KEY, parent_id bigint NULL, FOREIGN KEY(parent_id) REFERENCES app.parents(id) ON UPDATE SET NULL ON DELETE SET NULL)",
			"CREATE TABLE app.calls(id bigint PRIMARY KEY, phone_id bigint NOT NULL, FOREIGN KEY(phone_id) REFERENCES app.phones(id) ON DELETE CASCADE)",
			"INSERT INTO app.parents VALUES (1), (2)",
			"INSERT INTO app.phones VALUES (10, 1), (20, 2)",
			"INSERT INTO app.attrs VALUES (11, 1), (21, 2)",
			"INSERT INTO app.calls VALUES (101, 10), (201, 20)")
	}
	file, pos := src.masterPos(t)
	cfg := strandCfg{Name: "mdb-cascade", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.*"}, Dir: testDir(t)}
	path := cfg.write(t)
	out, code := runStrandCLI(t, time.Minute, "sync", "-c", path)
	if code != 3 || !strings.Contains(out, "consistent initial copy") {
		t.Fatalf("unseeded startup = %d, expected safe rejection:\n%s", code, out)
	}
	dst.exec(t, "ALTER TABLE app.attrs DROP FOREIGN KEY attrs_ibfk_1")
	out, code = runStrandCLI(t, time.Minute, "sync", "-c", path, "--start-at", fmt.Sprintf("%s:%d", file, pos))
	if code != 3 || !strings.Contains(out, "missing matching target cascading foreign key") {
		t.Fatalf("startup without target constraint = %d, expected safe rejection:\n%s", code, out)
	}
	dst.exec(t, "ALTER TABLE app.attrs ADD CONSTRAINT attrs_parent_fk FOREIGN KEY(parent_id) REFERENCES app.parents(id) ON UPDATE SET NULL ON DELETE SET NULL")
	src.exec(t, "ALTER TABLE app.attrs ADD CONSTRAINT a_cascade FOREIGN KEY(parent_id) REFERENCES app.parents(id) ON DELETE CASCADE")
	dst.exec(t, "ALTER TABLE app.attrs ADD CONSTRAINT z_cascade FOREIGN KEY(parent_id) REFERENCES app.parents(id) ON DELETE CASCADE")
	file, pos = src.masterPos(t)
	out, code = runStrandCLI(t, time.Minute, "sync", "-c", path, "--start-at", fmt.Sprintf("%s:%d", file, pos))
	if code != exitcode.Rejected || !strings.Contains(out, "overlapping cascading foreign keys") {
		t.Fatalf("startup with order-sensitive constraints = %d, expected safe rejection:\n%s", code, out)
	}
	src.exec(t, "ALTER TABLE app.attrs DROP FOREIGN KEY a_cascade")
	dst.exec(t, "ALTER TABLE app.attrs DROP FOREIGN KEY z_cascade")
	file, pos = src.masterPos(t)
	cfg.Bootstrap = "start_at"
	cfg.StartAt = fmt.Sprintf("%s:%d", file, pos)
	path = cfg.write(t)
	process := startStrand(t, path)
	check := func() {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			var diff string
			for _, table := range []string{"parents", "phones", "attrs", "calls"} {
				if diff = klDiff(src.DB, dst.DB, "SELECT * FROM app."+table); diff != "" {
					diff = table + ": " + diff
					break
				}
			}
			if diff == "" {
				return
			}
			if process.exited() || time.Now().After(deadline) {
				t.Fatalf("cascade result differs: %s\n%s", diff, process.tail())
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitStrandLog(t, process, "pipeline running", 20*time.Second)
	src.exec(t, "UPDATE app.parents SET id=3 WHERE id=1")
	check()
	src.exec(t, "DELETE FROM app.parents WHERE id=3")
	check()
	if code := process.stop(t); code != 0 {
		t.Fatalf("stop exit %d\n%s", code, process.tail())
	}
	out, code = runStrandCLI(t, time.Minute, "sync", "-c", path, "--start-at", cfg.StartAt)
	if code != exitcode.InvalidSyntax || !strings.Contains(out, "a checkpoint already exists") {
		t.Fatalf("explicit start position with an existing checkpoint = %d:\n%s", code, out)
	}
	process = startStrand(t, path)
	waitStrandLog(t, process, "pipeline running", 20*time.Second)
	src.exec(t, "DELETE FROM app.parents WHERE id=2")
	check()
	// A source statement that disables FK actions must not run with cascade enabled on the target.
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("SET SESSION foreign_key_checks=0"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO app.phones VALUES (40, 4)"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec("SET SESSION foreign_key_checks=1"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	waitStrandLog(t, process, "disables foreign_key_checks", 20*time.Second)
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.phones WHERE id=40").Scan(&code); err != nil || code != 0 {
		t.Fatalf("statement with disabled source FK reached target: n=%d err=%v", code, err)
	}
}

func TestMDB_E2E_CascadeDDLBlocksHistoricalReplay(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	for _, db := range []*mysqlNode{src, dst} {
		db.exec(t, "CREATE DATABASE IF NOT EXISTS app",
			"CREATE TABLE app.parents(id bigint PRIMARY KEY)",
			"CREATE TABLE app.children(id bigint PRIMARY KEY, parent_id bigint NOT NULL, KEY by_parent(parent_id))",
			"INSERT INTO app.parents VALUES (1)",
			"INSERT INTO app.children VALUES (10, 1)")
	}
	file, pos := src.masterPos(t)
	cfg := strandCfg{Name: "mdb-cascade-history", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.*"}, Dir: testDir(t)}
	path := cfg.write(t)
	process := startStrand(t, path, "--start-at", fmt.Sprintf("%s:%d", file, pos))
	t.Cleanup(func() {
		if !process.exited() {
			process.stop(t)
		}
	})
	waitStrandLog(t, process, "pipeline running", 20*time.Second)
	src.exec(t, "INSERT INTO app.parents VALUES (2)")
	deadline := time.Now().Add(20 * time.Second)
	for {
		if klDiff(src.DB, dst.DB, "SELECT id FROM app.parents") == "" {
			break
		}
		if process.exited() || time.Now().After(deadline) {
			t.Fatalf("initial row was not applied:\n%s", process.tail())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if code := process.stop(t); code != 0 {
		t.Fatalf("initial stop exit %d:\n%s", code, process.tail())
	}
	meta, err := sql.Open("mysql", dst.RootDSN+"strand")
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	loadCheckpoint := func() checkpoint.AppliedSet {
		t.Helper()
		applied, err := checkpoint.LoadAppliedSnapshotMySQL(t.Context(), meta, cfg.Name, model.EngineMySQL)
		if err != nil {
			t.Fatal(err)
		}
		return applied
	}
	before := loadCheckpoint()
	src.exec(t, "ALTER TABLE app.children ADD CONSTRAINT transient_fk FOREIGN KEY(parent_id) REFERENCES app.parents(id) ON DELETE CASCADE",
		"DELETE FROM app.parents WHERE id=1",
		"ALTER TABLE app.children DROP FOREIGN KEY transient_fk",
		"INSERT INTO app.parents VALUES (3)")
	for range 2 {
		out, code := runStrandCLI(t, time.Minute, "sync", "-c", path)
		if code != exitcode.NeedsDecision || !strings.Contains(out, "foreign-key DDL") || !strings.Contains(out, "ADD CONSTRAINT transient_fk") {
			t.Fatalf("replay exit %d, expected rejection at the first FK statement:\n%s", code, out)
		}
		if after := loadCheckpoint(); !before.Equal(after) {
			t.Fatalf("checkpoint advanced across FK DDL: before=%v after=%v", before, after)
		}
	}
	var count int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.parents WHERE id IN (1,2)").Scan(&count); err != nil || count != 2 {
		t.Fatalf("target applied parent delete after the unsafe DDL: count=%d err=%v", count, err)
	}
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.parents WHERE id=3").Scan(&count); err != nil || count != 0 {
		t.Fatalf("target applied the marker after the unsafe DDL: count=%d err=%v", count, err)
	}
}
