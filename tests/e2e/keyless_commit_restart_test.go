package e2e

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// The deferred target trigger waits inside COMMIT, after all data and checkpoint SQL.
// Suspend the CLI there, then either terminate the waiting backend or let it commit.
// In the second case the process dies without ever consuming the success response.
func TestKL_E2E_PostgresCommitInterruption(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("observes SIGSTOP via Linux /proc")
	}
	for _, mode := range []string{"strict", "relaxed"} {
		for _, side := range []string{"before", "after"} {
			t.Run(mode+"/"+side, func(t *testing.T) {
				klCommitInterruption(t, mode, side)
			})
		}
	}
}

func klCommitInterruption(t *testing.T, mode, side string) {
	src := startPG(t, "source", "-c", "synchronous_commit=on")
	dst := startPG(t, "target", "-c", "fsync=on", "-c", "synchronous_commit=on")
	for _, n := range []*pgNode{src, dst} {
		n.exec(t, "CREATE SCHEMA app", "CREATE TABLE app.control(id int PRIMARY KEY)",
			"CREATE TABLE app.images(k int, s text, payload text)",
			"ALTER TABLE app.images ALTER COLUMN payload SET STORAGE EXTERNAL",
			"INSERT INTO app.images VALUES (1,'x',repeat('payload',2048)),(1,'x',repeat('payload',2048)),(1,'X',repeat('payload',2048)),(1,'x ',repeat('payload',2048)),(2,NULL,NULL),(2,NULL,NULL)")
	}
	src.exec(t, "ALTER TABLE app.images REPLICA IDENTITY FULL", "CREATE EXTENSION pg_walinspect",
		"CREATE PUBLICATION kl_pub FOR TABLE app.control, app.images")
	const gate = 27031
	dst.exec(t, fmt.Sprintf(`CREATE FUNCTION public.hold_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_xact_lock(%d); RETURN NEW; END $$`, gate),
		`CREATE CONSTRAINT TRIGGER hold_commit AFTER INSERT ON app.control DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW WHEN (NEW.id=1) EXECUTE FUNCTION public.hold_commit()`)
	var start string
	if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot('kl_slot','pgoutput',false,true)").Scan(&start); err != nil {
		t.Fatal(err)
	}
	cfg := strandCfg{Name: "kl-commit", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: ", slot: kl_slot, publication: kl_pub", Include: []string{"app.images", "app.control"}, Dir: testDir(t),
		CommitOrder: mode, Bootstrap: "start_at", TablesExtra: klKeylessCfg, PostSeedWindow: "1s", SourceIOPS: 20000}
	path := cfg.write(t)
	p := startStrand(t, path, "--start-at", start)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, time.Minute)

	hold, err := dst.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Rollback()
	if _, err := hold.Exec("SELECT pg_advisory_xact_lock($1)", gate); err != nil {
		t.Fatal(err)
	}

	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var xid, lower string
	if err := tx.QueryRow("SELECT pg_current_xact_id()::text, pg_current_wal_insert_lsn()::text").Scan(&xid, &lower); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"INSERT INTO app.control VALUES (1)",
		"UPDATE app.images SET s='changed' WHERE ctid=(SELECT ctid FROM app.images WHERE s='x' LIMIT 1)",
		"DELETE FROM app.images WHERE ctid=(SELECT ctid FROM app.images WHERE s IS NULL LIMIT 1)",
		"INSERT INTO app.images VALUES (3,NULL,NULL),(3,NULL,NULL)",
	} {
		if _, err := tx.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var commit string
	if err := src.DB.QueryRow(`SELECT start_lsn::text FROM pg_get_wal_records_info($1,pg_current_wal_flush_lsn())
		WHERE xid::text=$2 AND resource_manager='Transaction' AND record_type='COMMIT'`, lower, xid).Scan(&commit); err != nil {
		t.Fatal(err)
	}
	lsn, err := checkpoint.ParseLSN(commit)
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Minute)
	var backend int
	for backend == 0 {
		if err := dst.DB.QueryRow(`SELECT coalesce(max(pid),0) FROM pg_locks
			WHERE locktype='advisory' AND NOT granted AND objid=$1`, gate).Scan(&backend); err != nil {
			t.Fatal(err)
		}
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("writer never reached COMMIT: %s", p.tail())
		}
		if backend == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := p.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	for {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", p.cmd.Process.Pid))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "State:\tT (stopped)") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Strand did not enter SIGSTOP")
		}
		time.Sleep(5 * time.Millisecond)
	}

	if side == "before" {
		var terminated bool
		if err := dst.DB.QueryRow("SELECT pg_terminate_backend($1,5000)", backend).Scan(&terminated); err != nil || !terminated {
			t.Fatalf("terminate writer inside COMMIT: %t, %v", terminated, err)
		}
	}
	if err := hold.Rollback(); err != nil {
		t.Fatal(err)
	}
	wantCommitted := side == "after"
	for {
		var committed bool
		if err := dst.DB.QueryRow("SELECT EXISTS (SELECT FROM app.control WHERE id=1)").Scan(&committed); err != nil {
			t.Fatal(err)
		}
		if committed == wantCommitted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("target transaction committed=%t, want %t", committed, wantCommitted)
		}
		time.Sleep(10 * time.Millisecond)
	}
	hasCommit := func() bool {
		t.Helper()
		applied, err := checkpoint.LoadAppliedSnapshotPG(context.Background(), dst.DB, cfg.Name, model.EnginePG)
		if err != nil {
			t.Fatal(err)
		}
		return applied.Contains(model.Pos{Engine: model.EnginePG, LSN: lsn})
	}
	if hasCommit() != wantCommitted {
		t.Fatalf("data and checkpoint disagree at %s", commit)
	}
	p.kill9()
	const images = "SELECT row_to_json(i)::text FROM app.images i"
	if !wantCommitted {
		var initial bool
		if err := dst.DB.QueryRow(`SELECT count(*)=6 AND count(*) FILTER (WHERE s='x')=2 AND count(*) FILTER (WHERE s IS NULL)=2 FROM app.images`).Scan(&initial); err != nil || !initial {
			t.Fatalf("rolled-back keyless multiplicities: %t err=%v", initial, err)
		}
	} else if diff := klDiff(src.DB, dst.DB, images); diff != "" {
		t.Fatal(diff)
	}

	p = startStrand(t, path)
	waitSynced(t, &cfg, p, time.Minute)
	// A later commit on the same keyless table forces observation past the replay boundary.
	src.exec(t, "INSERT INTO app.images VALUES (4,'restart',NULL)")
	ukWaitEqual(t, p, time.Minute, func() string { return klDiff(src.DB, dst.DB, images) })
	if !hasCommit() {
		t.Fatalf("restart checkpoint missing %s", commit)
	}
	if diff := klDiff(src.DB, dst.DB, "SELECT id FROM app.control"); diff != "" {
		t.Fatal(diff)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("restart exit %d: %s", code, p.tail())
	}
	t.Logf("mixed txn xid=%s commit=%s target_committed_at_SIGKILL=%t; restart exact multiset matched", xid, commit, wantCommitted)
}
