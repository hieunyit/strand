package writer_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/writer"
)

// invariant: SG-1
// EnsureWorkerRow used "ON DUPLICATE KEY UPDATE epoch = VALUES(epoch)": a stale
// instance could write its old epoch back, fence the new owner and keep writing.
func TestSG1_StaleInstanceCannotResetWorkerEpoch(t *testing.T) {
	ctx := context.Background()

	t.Run("MySQL", func(t *testing.T) {
		db, _ := getMySQL(t)
		pid := fmt.Sprintf("sg1-steal-my-%d", time.Now().UnixNano())
		store := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
		if err := store.EnsureTables(ctx); err != nil {
			t.Fatal(err)
		}
		e1, _ := store.AdvanceEpoch(ctx, pid, "a", "h")
		old := writer.NewMySQLWriter(db, pid, 1, e1)
		if err := old.EnsureWorkerRow(ctx); err != nil {
			t.Fatal(err)
		}
		e2, _ := store.AdvanceEpoch(ctx, pid, "b", "h")
		cur := writer.NewMySQLWriter(db, pid, 1, e2)
		if err := cur.EnsureWorkerRow(ctx); err != nil {
			t.Fatal(err)
		}

		err := old.EnsureWorkerRow(ctx)
		if exitcode.GetExitCode(err) != exitcode.Fenced {
			t.Fatalf("stale EnsureWorkerRow must be fenced, got %v", err)
		}
		var epoch int64
		_ = db.QueryRowContext(ctx, "SELECT epoch FROM _strand_worker_delta WHERE pipeline_id=? AND worker_id=1", pid).Scan(&epoch)
		if epoch != e2 {
			t.Fatalf("stale instance reset worker epoch to %d (want %d)", epoch, e2)
		}

		cfg := checkpoint.DefaultCompactorConfig(pid, e1)
		staleCompactor := checkpoint.NewCompactor(db, nil, pid, model.EngineMySQL, model.EngineMySQL, cfg)
		err = staleCompactor.RecordWorkerDelta(ctx, nil, 1, "uuid:1-5")
		if exitcode.GetExitCode(err) != exitcode.Fenced {
			t.Fatalf("stale RecordWorkerDelta must be fenced, got %v", err)
		}
		_ = db.QueryRowContext(ctx, "SELECT epoch FROM _strand_worker_delta WHERE pipeline_id=? AND worker_id=1", pid).Scan(&epoch)
		if epoch != e2 {
			t.Fatalf("stale compactor reset worker epoch to %d (want %d)", epoch, e2)
		}
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := getPostgres(t)
		pid := fmt.Sprintf("sg1-steal-pg-%d", time.Now().UnixNano())
		store := checkpoint.NewPGStore(db, pid, model.EngineMySQL)
		if err := store.EnsureTables(ctx); err != nil {
			t.Fatal(err)
		}
		e1, _ := store.AdvanceEpoch(ctx, pid, "a", "h")
		old := writer.NewPGWriter(db, pid, 1, e1)
		if err := old.EnsureWorkerRow(ctx); err != nil {
			t.Fatal(err)
		}
		e2, _ := store.AdvanceEpoch(ctx, pid, "b", "h")
		if err := writer.NewPGWriter(db, pid, 1, e2).EnsureWorkerRow(ctx); err != nil {
			t.Fatal(err)
		}
		err := old.EnsureWorkerRow(ctx)
		if exitcode.GetExitCode(err) != exitcode.Fenced {
			t.Fatalf("stale EnsureWorkerRow must be fenced, got %v", err)
		}
		var epoch int64
		_ = db.QueryRowContext(ctx, "SELECT epoch FROM _strand_worker_delta WHERE pipeline_id=$1 AND worker_id=1", pid).Scan(&epoch)
		if epoch != e2 {
			t.Fatalf("stale instance reset worker epoch to %d (want %d)", epoch, e2)
		}
	})
}

// invariant: CP-1
// The writer hard-coded "WHERE id = ?" and used only Key[0]. Tables whose PK is not
// called id, composite PKs and reserved-word identifiers must be applied correctly.
func TestCP1_WriterUsesRealPrimaryKey(t *testing.T) {
	ctx := context.Background()
	db, _ := getMySQL(t)
	pid := fmt.Sprintf("pk-my-%d", time.Now().UnixNano())
	ensureMySQLMetadataTables(t, db, pid)
	store := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
	epoch, _ := store.AdvanceEpoch(ctx, pid, "o", "h")

	mustExec(t, db, "DROP TABLE IF EXISTS `order`")
	mustExec(t, db, "CREATE TABLE `order` (tenant INT NOT NULL, `select` VARCHAR(20) NOT NULL, id INT NULL, amount INT, PRIMARY KEY (tenant, `select`))")
	tbl := model.TableID{Schema: "strand", Table: "order"}

	w := writer.NewMySQLWriter(db, pid, 1, epoch)
	w.SetPrimaryKey(tbl, []string{"tenant", "select"})
	if err := w.EnsureWorkerRow(ctx); err != nil {
		t.Fatal(err)
	}
	row := func(op model.Op, tenant int64, code string, amount int64, old ...model.Value) model.RowChange {
		rc := model.RowChange{Table: tbl, Op: op, Key: []model.Value{tenant, code},
			Cols: []model.ColValue{{Name: "tenant", Value: tenant}, {Name: "select", Value: code}, {Name: "id", Value: int64(7)}, {Name: "amount", Value: amount}}}
		if len(old) > 0 {
			rc.OldKey = old
		}
		return rc
	}
	batch := []*model.Txn{{Rows: []model.RowChange{
		row(model.OpInsert, 1, "A", 10),
		row(model.OpInsert, 2, "A", 20), // same id column value, different PK
		row(model.OpUpdate, 1, "A", 11),
		row(model.OpUpdate, 1, "B", 12, int64(1), "A"), // PK change
		row(model.OpUpdate, 3, "C", 30),                // row not present on target yet: must not be lost
		row(model.OpDelete, 2, "A", 20),
	}}}
	if err := w.ApplyBatch(ctx, batch, writer.DeltaRow{Applied: "uuid:1"}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	got := map[string]int64{}
	rows, err := db.QueryContext(ctx, "SELECT tenant, `select`, amount FROM `order`")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var tn int64
		var c string
		var a int64
		_ = rows.Scan(&tn, &c, &a)
		got[fmt.Sprintf("%d/%s", tn, c)] = a
	}
	want := map[string]int64{"1/B": 12, "3/C": 30}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("target state %v, want %v", got, want)
	}
}

// A table whose PK is unknown and composite must be rejected, never guessed.
func TestCP1_WriterRejectsUnknownCompositeKey(t *testing.T) {
	ctx := context.Background()
	db, _ := getMySQL(t)
	pid := fmt.Sprintf("pk-unk-%d", time.Now().UnixNano())
	ensureMySQLMetadataTables(t, db, pid)
	store := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
	epoch, _ := store.AdvanceEpoch(ctx, pid, "o", "h")
	w := writer.NewMySQLWriter(db, pid, 1, epoch)
	_ = w.EnsureWorkerRow(ctx)
	batch := []*model.Txn{{Rows: []model.RowChange{{Table: model.TableID{Schema: "strand", Table: "x"}, Op: model.OpDelete,
		Key: []model.Value{int64(1), "a"}}}}}
	if err := w.ApplyBatch(ctx, batch, writer.DeltaRow{Applied: "uuid:1"}); err == nil {
		t.Fatalf("composite key with unknown PK columns must be rejected")
	}
}

// invariant: CP-5
// Strict mode: batches must hold consecutive seqs, and a failed commit must not
// leave the other workers waiting forever.
func TestCP5_StrictOrdererFailsInsteadOfHanging(t *testing.T) {
	co := writer.NewCommitOrderer("strict", 1)
	waitErr := make(chan error, 1)
	go func() { waitErr <- co.WaitTurn(context.Background(), 2) }()
	time.Sleep(50 * time.Millisecond)
	co.Fail(errors.New("commit of seq 1 failed"))
	select {
	case err := <-waitErr:
		if err == nil {
			t.Fatalf("WaitTurn must return the failure")
		}
	case <-time.After(time.Second):
		t.Fatalf("WaitTurn hangs after the preceding commit failed")
	}
}

func TestCP5_StrictRejectsNonContiguousBatch(t *testing.T) {
	w := writer.NewMySQLWriter(nil, "p", 1, 1)
	w.SetCommitOrderer(writer.NewCommitOrderer("strict", 1))
	a, b := &model.Txn{}, &model.Txn{}
	a.SetSeq(1)
	b.SetSeq(3)
	err := w.ApplyBatch(context.Background(), []*model.Txn{a, b}, writer.DeltaRow{Applied: "uuid:1"})
	if err == nil {
		t.Fatalf("strict batch {1,3} must be rejected (seq 2 would commit after 3)")
	}
}

func TestCP5_StrictWaitTurnRejectsAlreadyPassedSeq(t *testing.T) {
	co := writer.NewCommitOrderer("strict", 1)
	if err := co.WaitTurn(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	co.CommitDone(3) // batch 1..3 committed
	if err := co.WaitTurn(context.Background(), 2); err == nil {
		t.Fatalf("seq 2 is already behind nextCommit=4: must be an error, not a free pass")
	}
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}
