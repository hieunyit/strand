package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// PostgreSQL→PostgreSQL must preserve FK cascades when a replicated primary key
// changes. The source's UPDATE invokes ON UPDATE CASCADE. A target implementation
// that models the PK change as DELETE + INSERT can incorrectly hit ON DELETE
// RESTRICT before it inserts the new parent key. Restart after the first
// transaction also checks that the durable checkpoint resumes past this FK group.
func TestP2_E2E_PGForeignKeyPKUpdateCascadeResume(t *testing.T) {
	const slot, publication = "strand_pg_fk_restart", "strand_pg_fk_restart_pub"
	src := startPG(t, "pgsrc")
	dst := startPG(t, "pgdst")
	src.exec(t, pgSchemaSQL...)
	dst.exec(t, pgSchemaSQL...)
	pgPreload(t, src, 10, 5, 10)
	pgPreload(t, dst, 10, 5, 10)

	const parentDDL = `CREATE TABLE app.fk_parent (id bigint PRIMARY KEY, note text NOT NULL)`
	const restrictChildDDL = `CREATE TABLE app.fk_child (id bigint PRIMARY KEY, parent_id bigint NOT NULL, note text NOT NULL,
		CONSTRAINT fk_child_parent FOREIGN KEY (parent_id) REFERENCES app.fk_parent(id)
		ON UPDATE CASCADE ON DELETE RESTRICT)`
	const cascadeChildDDL = `CREATE TABLE app.fk_cascade_child (id bigint PRIMARY KEY, parent_id bigint NOT NULL, note text NOT NULL,
		CONSTRAINT fk_cascade_child_parent FOREIGN KEY (parent_id) REFERENCES app.fk_parent(id)
		ON UPDATE CASCADE ON DELETE CASCADE)`
	for _, n := range []*pgNode{src, dst} {
		n.exec(t, parentDDL, restrictChildDDL, cascadeChildDDL,
			"ALTER TABLE app.fk_parent ALTER COLUMN note SET STORAGE EXTERNAL",
			"INSERT INTO app.fk_parent VALUES (1, 'parent-1'), (2, repeat('unchanged-toast', 2000))",
			"INSERT INTO app.fk_child VALUES (10, 1, 'child-10'), (20, 2, 'child-20')",
			"INSERT INTO app.fk_cascade_child VALUES (30, 1, 'cascade-30'), (40, 2, 'cascade-40')")
	}
	publicationTables := append(append([]string(nil), pgAppTables...), "app.fk_parent", "app.fk_child", "app.fk_cascade_child")
	src.exec(t, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')", publication, strings.Join(publicationTables, ", ")))
	var startLSN string
	if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', false, true)", slot).Scan(&startLSN); err != nil {
		t.Fatalf("create logical slot: %v", err)
	}

	cfg := strandCfg{Name: "e2e-pg-fk-restart", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, publication),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	path := cfg.write(t)
	cleanupStrand := func(proc *strandProc) {
		t.Cleanup(func() {
			if !proc.exited() {
				proc.kill9()
			}
		})
	}
	p := startStrand(t, path, "--start-at", startLSN)
	cleanupStrand(p)
	waitPGPipelineReady(t, dst, p, cfg.Name, time.Minute)

	// This is a valid source UPDATE and should update the child FK in the same
	// transaction. The matching target constraint must not reject the old-key
	// removal as an ON DELETE RESTRICT operation.
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE app.fk_parent SET id = 101 WHERE id = 1"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit parent PK update with ON UPDATE CASCADE: %v", err)
	}
	waitPGCaughtUp(t, src, dst, &cfg, p, 2*time.Minute)
	var targetParentID int64
	if err := dst.DB.QueryRow("SELECT parent_id FROM app.fk_child WHERE id = 10").Scan(&targetParentID); err != nil {
		t.Fatal(err)
	}
	if targetParentID != 101 {
		t.Fatalf("target child FK after parent PK update = %d, want 101", targetParentID)
	}

	// Abrupt restart at a durable checkpoint. Delete the RESTRICT child first,
	// then delete its parent in the same source transaction; a second child table
	// exercises ON DELETE CASCADE on the target.
	p.kill9()
	p = startStrand(t, path)
	cleanupStrand(p)
	waitPGPipelineReady(t, dst, p, cfg.Name, time.Minute)
	src.exec(t, "UPDATE app.fk_parent SET id = 202 WHERE id = 2")
	waitPGCaughtUp(t, src, dst, &cfg, p, 2*time.Minute)
	var toastBytes int
	if err := dst.DB.QueryRow("SELECT octet_length(note) FROM app.fk_parent WHERE id = 202").Scan(&toastBytes); err != nil || toastBytes != len("unchanged-toast")*2000 {
		t.Fatalf("target TOAST after referenced key update: %d bytes, err=%v", toastBytes, err)
	}
	tx, err = src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("DELETE FROM app.fk_child WHERE id = 20"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Exec("DELETE FROM app.fk_parent WHERE id = 202"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit source parent delete with ON DELETE RESTRICT: %v", err)
	}
	waitPGCaughtUp(t, src, dst, &cfg, p, 2*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}

	comparePG(t, src, dst, "app.fk_parent", "app.fk_child", "app.fk_cascade_child")
	var children int
	if err := dst.DB.QueryRow("SELECT count(*) FROM app.fk_cascade_child WHERE parent_id = 202").Scan(&children); err != nil {
		t.Fatal(err)
	}
	if children != 0 {
		t.Fatalf("target retained %d children after the source delete cascade", children)
	}
	for _, table := range []string{"app.fk_parent", "app.fk_child", "app.fk_cascade_child"} {
		out, code := runStrandCLI(t, 3*time.Minute, "verify", "-c", path, "--mode", "full", "--table", table, "--rate", "1000")
		if code != 0 {
			t.Fatalf("verify %s exit %d:\n%s", table, code, out)
		}
	}
}
