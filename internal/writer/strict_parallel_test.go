package writer_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/writer"
)

// invariant: CP-5. Strict song song: batch ghi dữ liệu trước rồi mới chờ lượt. Batch seq 2 chạy trước,
// giữ lock của row mà batch seq 1 cần, rồi chờ lượt; seq 1 chờ lock — vòng chờ DB không thấy. Seq 2 phải
// nhường (rollback, chờ lượt không giữ lock, ghi lại): cả hai commit đúng thứ tự, row giữ giá trị của seq 2.
func TestCP5_StrictParallelYieldsLockToTurnHolder_MySQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db, _ := getMySQL(t)
	pid := fmt.Sprintf("strict-par-%d", time.Now().UnixNano())
	ensureMySQLMetadataTables(t, db, pid)
	store := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
	epoch, _ := store.AdvanceEpoch(ctx, pid, "o", "h")
	mustExec(t, db, "DROP TABLE IF EXISTS strict_par")
	mustExec(t, db, "CREATE TABLE strict_par (id INT PRIMARY KEY, v INT)")
	mustExec(t, db, "INSERT INTO strict_par VALUES (1, 0)")
	tbl := model.TableID{Schema: "strand", Table: "strict_par"}

	co := writer.NewCommitOrderer("strict", 1)
	co.SetParallel(20 * time.Millisecond)
	var mu sync.Mutex
	global, _ := checkpoint.ParseMySQLGTIDSet("")
	var order []uint64
	mk := func(seq uint64) *model.Txn {
		tx := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("u:%d", seq)},
			Rows: []model.RowChange{{Table: tbl, Op: model.OpUpdate, Key: []model.Value{int64(1)},
				Cols: []model.ColValue{{Name: "id", Value: int64(1)}, {Name: "v", Value: int64(seq)}}}}}
		tx.SetSeq(seq)
		return tx
	}
	apply := func(wid int, tx *model.Txn) error {
		w := writer.NewMySQLWriter(db, pid, wid, epoch)
		w.SetPrimaryKey(tbl, []string{"id"})
		w.SetCommitOrderer(co)
		return w.ApplyBatch(ctx, []*model.Txn{tx}, writer.DeltaRow{Global: func() (string, error) {
			mu.Lock()
			defer mu.Unlock()
			global.Add(tx.SourcePos)
			order = append(order, tx.Seq())
			return global.String(), nil
		}})
	}
	errs := make(chan error, 2)
	go func() { errs <- apply(2, mk(2)) }() // seq 2 ghi trước, giữ lock row 1 và chờ lượt
	time.Sleep(300 * time.Millisecond)
	go func() { errs <- apply(1, mk(1)) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var v int
	var applied string
	if err := db.QueryRow("SELECT v FROM strict_par WHERE id = 1").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT applied FROM _strand_checkpoint WHERE pipeline_id = ?", pid).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if v != 2 || applied != "u:1-2" || fmt.Sprint(order) != "[1 2]" {
		t.Fatalf("v=%d applied=%q commit order=%v; want v=2 applied=u:1-2 order [1 2]", v, applied, order)
	}
	if co.Yields() == 0 {
		t.Fatal("seq 2 must have yielded its lock to seq 1")
	}
}

func TestCP5_StrictParallelYieldsLockToTurnHolder_PG(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	e := newPGEnv(t, "strict-par-pg")
	tbl := pgTable(t, e.db, "strict_par_pg", "id int PRIMARY KEY, v int")
	pgExec(t, e.db, `INSERT INTO public.strict_par_pg VALUES (1, 0)`)

	co := writer.NewCommitOrderer("strict", 1)
	co.SetParallel(20 * time.Millisecond)
	var mu sync.Mutex
	var appliedSeqs []string
	mk := func(seq uint64) *model.Txn {
		tx := &model.Txn{Rows: []model.RowChange{upd(tbl, []model.Value{1}, cv("id", 1), cv("v", int(seq)))}}
		tx.SetSeq(seq)
		return tx
	}
	apply := func(wid int, tx *model.Txn) error {
		w := writer.NewPGWriter(e.db, e.pid, wid, e.epoch)
		w.SetPrimaryKey(tbl, []string{"id"})
		w.SetCommitOrderer(co)
		return w.ApplyBatch(ctx, []*model.Txn{tx}, writer.DeltaRow{Global: func() (string, error) {
			mu.Lock()
			defer mu.Unlock()
			appliedSeqs = append(appliedSeqs, fmt.Sprint(tx.Seq()))
			return strings.Join(appliedSeqs, ","), nil
		}})
	}
	errs := make(chan error, 2)
	go func() { errs <- apply(2, mk(2)) }()
	time.Sleep(300 * time.Millisecond)
	go func() { errs <- apply(1, mk(1)) }()
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var v int
	var applied string
	if err := e.db.QueryRow(`SELECT v FROM public.strict_par_pg WHERE id = 1`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if err := e.db.QueryRow(`SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1`, e.pid).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if v != 2 || applied != "1,2" {
		t.Fatalf("v=%d applied=%q, want v=2 applied=1,2", v, applied)
	}
	if co.Yields() == 0 {
		t.Fatal("seq 2 must have yielded its lock to seq 1")
	}
}

// Không có vòng chờ: batch đến lượt ghi nhanh thì batch chờ lượt không nhường (không ghi lại vô ích).
func TestStrictParallelNoYieldWhenTurnHolderIsQuick(t *testing.T) {
	co := writer.NewCommitOrderer("strict", 1)
	co.SetParallel(200 * time.Millisecond)
	done := make(chan error, 1)
	go func() {
		y, err := co.WaitTurnOrYield(context.Background(), 2)
		if err == nil && y {
			err = fmt.Errorf("yielded")
		}
		done <- err
	}()
	co.BeginApply(1)
	time.Sleep(30 * time.Millisecond)
	co.EndApply(1)
	co.CommitDone(1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if co.Yields() != 0 {
		t.Fatalf("yields = %d", co.Yields())
	}
	// Batch đến lượt ghi lâu: batch chờ nhường.
	co2 := writer.NewCommitOrderer("strict", 1)
	co2.SetParallel(20 * time.Millisecond)
	co2.BeginApply(1)
	y, err := co2.WaitTurnOrYield(context.Background(), 2)
	if err != nil || !y {
		t.Fatalf("holder stuck applying: yield=%v err=%v", y, err)
	}
}
