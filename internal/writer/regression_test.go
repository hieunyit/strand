package writer_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
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

// invariant: CP-5, CP-1 (quy tắc 5)
// Strict: row toàn cục ghi trong transaction commit. Chuỗi applied phải được tính sau khi đến
// lượt; nếu tính trước, worker của seq 2 (chạy trước) ghi đè và làm mất GTID của seq 1.
func TestCP5_StrictGlobalCheckpointNoLostUpdate(t *testing.T) {
	ctx := context.Background()
	db, _ := getMySQL(t)
	pid := fmt.Sprintf("strict-global-%d", time.Now().UnixNano())
	ensureMySQLMetadataTables(t, db, pid)
	store := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
	epoch, _ := store.AdvanceEpoch(ctx, pid, "o", "h")
	mustExec(t, db, "CREATE TABLE IF NOT EXISTS strict_global (id INT PRIMARY KEY, v INT)")

	co := writer.NewCommitOrderer("strict", 1)
	var mu sync.Mutex
	global, _ := checkpoint.ParseMySQLGTIDSet("")
	mk := func(seq uint64, id int) *model.Txn {
		tx := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("u:%d", seq)},
			Rows: []model.RowChange{{Table: model.TableID{Schema: "strand", Table: "strict_global"}, Op: model.OpInsert,
				Key: []model.Value{int64(id)}, Cols: []model.ColValue{{Name: "id", Value: int64(id)}, {Name: "v", Value: int64(seq)}}}}}
		tx.SetSeq(seq)
		return tx
	}
	apply := func(w *writer.MySQLWriter, tx *model.Txn) error {
		return w.ApplyBatch(ctx, []*model.Txn{tx}, writer.DeltaRow{Global: func() (string, error) {
			mu.Lock()
			defer mu.Unlock()
			global.Add(tx.SourcePos)
			return global.String(), nil
		}})
	}
	w1, w2 := writer.NewMySQLWriter(db, pid, 1, epoch), writer.NewMySQLWriter(db, pid, 2, epoch)
	w1.SetCommitOrderer(co)
	w2.SetCommitOrderer(co)
	errs := make(chan error, 2)
	go func() { errs <- apply(w2, mk(2, 20)) }() // seq 2 bắt đầu trước
	time.Sleep(200 * time.Millisecond)
	go func() { errs <- apply(w1, mk(1, 10)) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var applied string
	if err := db.QueryRow("SELECT applied FROM _strand_checkpoint WHERE pipeline_id = ?", pid).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if applied != "u:1-2" {
		t.Fatalf("global checkpoint = %q, want u:1-2 (a committed txn was lost)", applied)
	}
}

// Gửi câu lệnh theo lô multi-statement phải cho đúng cùng trạng thái như gửi từng câu
// (thứ tự giữ nguyên, kể cả đổi PK, xóa, giá trị đặc biệt cần escape).
func TestCP1_StatementPipeliningSameResult(t *testing.T) {
	ctx := context.Background()
	_, dsn := getMySQL(t)
	db, err := sql.Open("mysql", dsn+"&multiStatements=true&interpolateParams=true")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	pid := fmt.Sprintf("pipe-%d", time.Now().UnixNano())
	ensureMySQLMetadataTables(t, db, pid)
	store := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
	epoch, _ := store.AdvanceEpoch(ctx, pid, "o", "h")
	mustExec(t, db, "DROP TABLE IF EXISTS `pl_items`")
	mustExec(t, db, "CREATE TABLE `pl_items` (id BIGINT PRIMARY KEY, s VARCHAR(64), b VARBINARY(16))")
	tbl := model.TableID{Schema: "strand", Table: "pl_items"}
	w := writer.NewMySQLWriter(db, pid, 1, epoch)
	w.SetStatementPipelining(true)
	w.SetPrimaryKey(tbl, []string{"id"})
	if err := w.EnsureWorkerRow(ctx); err != nil {
		t.Fatal(err)
	}
	row := func(op model.Op, id int64, sv string, old ...model.Value) model.RowChange {
		rc := model.RowChange{Table: tbl, Op: op, Key: []model.Value{id},
			Cols: []model.ColValue{{Name: "id", Value: id}, {Name: "s", Value: sv}, {Name: "b", Value: []byte{0, '\'', ';', 0xff}}}}
		if len(old) > 0 {
			rc.OldKey = old
		}
		return rc
	}
	var batch []*model.Txn
	for i := int64(1); i <= 300; i++ {
		batch = append(batch, &model.Txn{Rows: []model.RowChange{row(model.OpInsert, i, fmt.Sprintf("v'%d;--", i))}})
	}
	batch = append(batch,
		&model.Txn{Rows: []model.RowChange{row(model.OpUpdate, 1000, "moved", int64(5))}},
		&model.Txn{Rows: []model.RowChange{{Table: tbl, Op: model.OpDelete, Key: []model.Value{int64(7)}}}},
		&model.Txn{Rows: []model.RowChange{row(model.OpUpdate, 2, "last")}})
	if err := w.ApplyBatch(ctx, batch, writer.DeltaRow{Applied: "uuid:1-303"}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	var n int
	var s2, s300 string
	var b []byte
	_ = db.QueryRow("SELECT COUNT(*) FROM pl_items").Scan(&n)
	_ = db.QueryRow("SELECT s, b FROM pl_items WHERE id = 2").Scan(&s2, &b)
	_ = db.QueryRow("SELECT s FROM pl_items WHERE id = 300").Scan(&s300)
	var moved, gone5, gone7 int
	_ = db.QueryRow("SELECT COUNT(*) FROM pl_items WHERE id = 1000").Scan(&moved)
	_ = db.QueryRow("SELECT COUNT(*) FROM pl_items WHERE id = 5").Scan(&gone5)
	_ = db.QueryRow("SELECT COUNT(*) FROM pl_items WHERE id = 7").Scan(&gone7)
	if n != 299 || s2 != "last" || s300 != "v'300;--" || string(b) != string([]byte{0, '\'', ';', 0xff}) || moved != 1 || gone5 != 0 || gone7 != 0 {
		t.Fatalf("pipelined apply wrong: n=%d s2=%q s300=%q b=%v moved=%d gone5=%d gone7=%d", n, s2, s300, b, moved, gone5, gone7)
	}
}
