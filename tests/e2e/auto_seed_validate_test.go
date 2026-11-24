package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// F04: Validate must predict first-start auto-seed failures, including after a
// previous rejected attempt has left an empty checkpoint row behind. Once the
// copy commits, validation and restart must treat the pipeline as resumed.
func TestMDB_E2E_ValidateAutoSeedFirstStartBlockersAndResume(t *testing.T) {
	src := startMariaDBImage(t, mdbImage(), "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app",
		"CREATE TABLE app.parent (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE app.child (id BIGINT PRIMARY KEY, parent_id BIGINT, CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES app.parent(id) ON DELETE CASCADE) ENGINE=InnoDB",
		"INSERT INTO app.parent VALUES (1)", "INSERT INTO app.child VALUES (10,1)")

	cfg := strandCfg{Name: "mdb-f04-preflight", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr),
		Include:   []string{"app.child"}, Dir: testDir(t), AutoSeed: true, TablesExtra: klKeylessCfg}
	path := cfg.write(t)
	for _, op := range []string{"validate", "sync"} {
		out, code := runStrandCLI(t, time.Minute, op, "-c", path)
		if code != 3 || !strings.Contains(out, "app.parent") || !strings.Contains(out, "scope") {
			t.Fatalf("%s missing-scope preflight: exit %d\n%s", op, code, out)
		}
	}
	assertEmptyAutoSeedCheckpoint(t, dst)

	// Include both FK endpoints and place rows in both existing destination
	// tables. The failed startup's empty checkpoint must not mask this blocker.
	cfg.Include = []string{"app.parent", "app.child"}
	path = cfg.write(t)
	dst.exec(t,
		"CREATE TABLE app.parent (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE app.child (id BIGINT PRIMARY KEY, parent_id BIGINT) ENGINE=InnoDB",
		"INSERT INTO app.parent VALUES (1)", "INSERT INTO app.child VALUES (10,1)",
		"GRANT REFERENCES ON app.* TO 'strand'@'%'")
	for _, op := range []string{"validate", "sync"} {
		out, code := runStrandCLI(t, time.Minute, op, "-c", path)
		if code != 3 || !strings.Contains(out, "target app.") || !strings.Contains(out, "already contains data") {
			t.Fatalf("%s nonempty-target preflight: exit %d\n%s", op, code, out)
		}
	}
	assertEmptyAutoSeedCheckpoint(t, dst)
	var parentRows, childRows int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.parent").Scan(&parentRows); err != nil {
		t.Fatal(err)
	}
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.child").Scan(&childRows); err != nil {
		t.Fatal(err)
	}
	if parentRows != 1 || childRows != 1 {
		t.Fatalf("rejected target was modified: parent=%d child=%d", parentRows, childRows)
	}

	dst.exec(t, "DELETE FROM app.child", "DELETE FROM app.parent")
	dst.exec(t, "ALTER TABLE app.child ADD CONSTRAINT wrong_action FOREIGN KEY (parent_id) REFERENCES app.parent(id) ON DELETE RESTRICT")
	if out, code := runStrandCLI(t, time.Minute, "validate", "-c", path); code != 3 || !strings.Contains(out, "unexpected foreign key wrong_action") {
		t.Fatalf("validate incompatible FK action: exit %d\n%s", code, out)
	}
	dst.exec(t, "ALTER TABLE app.child DROP FOREIGN KEY wrong_action")
	p := startStrand(t, path)
	cleanupStrandOnTestExit(t, p)
	waitStrandLog(t, p, "auto-seed committed", time.Minute)
	waitSynced(t, &cfg, p, time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("initial run exit %d\n%s", code, p.tail())
	}
	if out, code := runStrandCLI(t, time.Minute, "validate", "-c", path); code != 0 {
		t.Fatalf("validate a completed auto-seed pipeline: exit %d\n%s", code, out)
	}
	p = startStrand(t, path)
	cleanupStrandOnTestExit(t, p)
	waitSynced(t, &cfg, p, time.Minute)
	if strings.Contains(p.output.String(), "auto-seed committed") {
		t.Fatal("resume repeated the automatic initial copy")
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("resumed run exit %d\n%s", code, p.tail())
	}
}

func cleanupStrandOnTestExit(t *testing.T, p *strandProc) {
	t.Helper()
	t.Cleanup(func() {
		if !p.exited() {
			p.kill9()
		}
	})
}

func assertEmptyAutoSeedCheckpoint(t *testing.T, dst *mysqlNode) {
	t.Helper()
	var applied string
	if err := dst.DB.QueryRow("SELECT applied FROM strand._strand_checkpoint WHERE pipeline_id=?", "mdb-f04-preflight").Scan(&applied); err != nil {
		t.Fatalf("read failed-start checkpoint: %v", err)
	}
	if applied != "" {
		t.Fatalf("rejected first start published checkpoint %q", applied)
	}
}
