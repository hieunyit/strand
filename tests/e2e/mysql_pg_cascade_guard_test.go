package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

// F01: MySQL cascades require an equivalent, enforced PostgreSQL FK before CDC
// can start. A fresh target is generated from source metadata and checked again
// before its first snapshot; a preexisting target with the wrong rule is rejected.
func TestF01_E2E_MySQLToPGCascadeForeignKeyPreflight(t *testing.T) {
	seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "f01src", 1, nil)
	pg := startPG(t, "f01pg")
	src.exec(t,
		"CREATE DATABASE app",
		"CREATE TABLE app.parent (id BIGINT PRIMARY KEY)",
		"CREATE TABLE app.other_parent (id BIGINT PRIMARY KEY)",
		"CREATE TABLE app.child (id BIGINT PRIMARY KEY, parent_id BIGINT NOT NULL, other_id BIGINT NOT NULL, CONSTRAINT child_parent_fk FOREIGN KEY(parent_id) REFERENCES app.parent(id) ON DELETE CASCADE ON UPDATE RESTRICT, CONSTRAINT child_other_fk FOREIGN KEY(other_id) REFERENCES app.other_parent(id) ON DELETE CASCADE ON UPDATE RESTRICT)",
		"INSERT INTO app.parent VALUES (1)",
		"INSERT INTO app.other_parent VALUES (1)",
		"INSERT INTO app.child VALUES (10,1,1)",
	)
	for _, db := range []string{"f01_mismatch", "f01_match", "f01_fresh"} {
		if _, err := pg.DB.Exec("CREATE DATABASE " + db); err != nil {
			t.Fatal(err)
		}
	}
	pgDSN := func(db string) string { return strings.Replace(pg.DSN, "/strand?", "/"+db+"?", 1) }
	openPG := func(db string) *sql.DB {
		t.Helper()
		d, err := sql.Open("pgx", pgDSN(db))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = d.Close() })
		return d
	}
	for _, db := range []string{"f01_mismatch", "f01_match"} {
		d := openPG(db)
		_, err := d.Exec("CREATE SCHEMA app; CREATE TABLE app.parent(id bigint PRIMARY KEY); CREATE TABLE app.other_parent(id bigint PRIMARY KEY); CREATE TABLE app.child(id bigint PRIMARY KEY, parent_id bigint NOT NULL, other_id bigint NOT NULL, CONSTRAINT child_parent_fk FOREIGN KEY(parent_id) REFERENCES app.parent(id) ON DELETE " + map[bool]string{true: "SET NULL", false: "CASCADE"}[db == "f01_mismatch"] + " ON UPDATE NO ACTION, CONSTRAINT child_other_fk FOREIGN KEY(other_id) REFERENCES app.other_parent(id) ON DELETE CASCADE ON UPDATE NO ACTION)")
		if err != nil {
			t.Fatal(err)
		}
	}

	bad := strandCfg{Name: "f01-pg-mismatch", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: pgDSN("f01_mismatch"), Include: []string{"app.*"}, Dir: testDir(t)}
	badProc := startStrand(t, bad.write(t))
	deadline := time.Now().Add(45 * time.Second)
	for !badProc.exited() && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !badProc.exited() {
		badProc.kill9()
		t.Fatalf("mismatched target FK was not rejected\n%s", badProc.tail())
	}
	if badProc.exitCode() == 0 || !strings.Contains(badProc.tail(), "missing a matching enforced foreign key") {
		t.Fatalf("mismatched target exit=%d; expected FK mismatch rejection\n%s", badProc.exitCode(), badProc.tail())
	}

	for _, tc := range []struct {
		name, database    string
		parentID, childID int
	}{{"matching-existing", "f01_match", 1, 10}, {"fresh-generated", "f01_fresh", 2, 20}} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.parentID != 1 {
				src.exec(t, fmt.Sprintf("INSERT INTO app.parent VALUES (%d)", tc.parentID), fmt.Sprintf("INSERT INTO app.other_parent VALUES (%d)", tc.parentID), fmt.Sprintf("INSERT INTO app.child VALUES (%d,%d,%d)", tc.childID, tc.parentID, tc.parentID))
			}
			cfg := strandCfg{Name: "f01-pg-" + tc.name, TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: pgDSN(tc.database), Include: []string{"app.*"}, Dir: testDir(t)}
			p := startStrand(t, cfg.write(t))
			t.Cleanup(func() {
				if !p.exited() {
					p.kill9()
				}
			})
			waitSynced(t, &cfg, p, 3*time.Minute)
			pgDB := openPG(tc.database)
			pgTarget := *pg
			pgTarget.DB = pgDB
			waitMySQLPGCaughtUp(t, src, &pgTarget, &cfg, p, 3*time.Minute)
			var initial int
			if err := pgDB.QueryRow("SELECT count(*) FROM app.child WHERE id=$1", tc.childID).Scan(&initial); err != nil || initial != 1 {
				t.Fatalf("initial target child count=%d, err=%v", initial, err)
			}
			if _, err := src.DB.Exec("DELETE FROM app.parent WHERE id=?", tc.parentID); err != nil {
				t.Fatal(err)
			}
			waitMySQLPGCaughtUp(t, src, &pgTarget, &cfg, p, 90*time.Second)
			deadline := time.Now().Add(90 * time.Second)
			verified := false
			for time.Now().Before(deadline) {
				var n, parents int
				if err := pgDB.QueryRow("SELECT count(*) FROM app.child WHERE id=$1", tc.childID).Scan(&n); err == nil && n == 0 {
					if err := pgDB.QueryRow("SELECT count(*) FROM app.parent WHERE id=$1", tc.parentID).Scan(&parents); err != nil || parents != 0 {
						t.Fatalf("target parent count=%d, err=%v", parents, err)
					}
					verified = true
					break
				}
				if p.exited() {
					t.Fatalf("pipeline stopped: %s", p.tail())
				}
				time.Sleep(100 * time.Millisecond)
			}
			if !verified {
				t.Fatalf("target child was not removed by its verified FK cascade\n%s", p.tail())
			}
			if tc.name == "matching-existing" {
				src.exec(t, "INSERT INTO app.parent VALUES (99)", "INSERT INTO app.other_parent VALUES (99)", "INSERT INTO app.child VALUES (99,99,99)")
				waitMySQLPGCaughtUp(t, src, &pgTarget, &cfg, p, time.Minute)
				ctx := context.Background()
				conn, err := src.DB.Conn(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				if _, err := conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=0"); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.ExecContext(ctx, "DELETE FROM app.parent WHERE id=99"); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.ExecContext(ctx, "SET SESSION FOREIGN_KEY_CHECKS=1"); err != nil {
					t.Fatal(err)
				}
				waitStrandLog(t, p, "disables foreign_key_checks", 20*time.Second)
				var parents, children int
				if err := pgDB.QueryRow("SELECT (SELECT count(*) FROM app.parent WHERE id=99), (SELECT count(*) FROM app.child WHERE id=99)").Scan(&parents, &children); err != nil || parents != 1 || children != 1 {
					t.Fatalf("disallowed cascade changed target rows: parent=%d child=%d err=%v", parents, children, err)
				}
				// Keep the next subtest's source consistent.
				src.exec(t, "DELETE FROM app.other_parent WHERE id=99")
				return
			}
			if code := p.stop(t); code != 0 {
				t.Fatalf("strand exit code %d\n%s", code, p.tail())
			}
		})
	}
}
