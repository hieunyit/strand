package writer

// Bộ phát hiện F11 "rows routed but not written" cho writer MySQL (như writer PG, f11_rows_written_test.go):
// ApplyBatch đếm row nguồn nằm trong câu lệnh đã chạy không lỗi (khối upsert, DELETE, lô multi-statement)
// và so với tổng row đã định tuyến TRƯỚC row delta và commit. Kiểm thử thuần qua driver database/sql giả
// (không DB): driver ghi lại câu lệnh, commit/rollback.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"strand/internal/model"
)

// fakeMyConn: driver giả tối thiểu. Mọi Exec thành công (RowsAffected 1) trừ câu chứa failOn; SELECT
// @@max_allowed_packet trả 64 MiB. Ghi lại câu đã chạy trong transaction và trạng thái commit/rollback.
type fakeMyState struct {
	mu        sync.Mutex
	execs     []string
	committed bool
	rolled    bool
	failOn    string
}

type fakeMyDriver struct{ st *fakeMyState }

func (d fakeMyDriver) Open(string) (driver.Conn, error) { return &fakeMyConn{st: d.st}, nil }

type fakeMyConn struct{ st *fakeMyState }

func (c *fakeMyConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}
func (c *fakeMyConn) Close() error              { return nil }
func (c *fakeMyConn) Begin() (driver.Tx, error) { return fakeMyTx{c.st}, nil }
func (c *fakeMyConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.st.mu.Lock()
	defer c.st.mu.Unlock()
	if c.st.failOn != "" && strings.Contains(q, c.st.failOn) {
		return nil, fmt.Errorf("Error 1062: injected failure")
	}
	c.st.execs = append(c.st.execs, q)
	return driver.RowsAffected(1), nil
}
func (c *fakeMyConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	return &fakeMyRows{v: int64(64 << 20)}, nil
}

type fakeMyTx struct{ st *fakeMyState }

func (t fakeMyTx) Commit() error   { t.st.mu.Lock(); t.st.committed = true; t.st.mu.Unlock(); return nil }
func (t fakeMyTx) Rollback() error { t.st.mu.Lock(); t.st.rolled = true; t.st.mu.Unlock(); return nil }

type fakeMyRows struct {
	v    int64
	done bool
}

func (r *fakeMyRows) Columns() []string { return []string{"v"} }
func (r *fakeMyRows) Close() error      { return nil }
func (r *fakeMyRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = r.v
	return nil
}

var fakeMyDriverSeq struct {
	sync.Mutex
	n int
}

func newFakeMySQLWriter(t *testing.T, pipelining bool) (*MySQLWriter, *fakeMyState) {
	t.Helper()
	st := &fakeMyState{}
	fakeMyDriverSeq.Lock()
	fakeMyDriverSeq.n++
	name := fmt.Sprintf("strand-fake-mysql-%d", fakeMyDriverSeq.n)
	fakeMyDriverSeq.Unlock()
	sql.Register(name, fakeMyDriver{st})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	w := NewMySQLWriter(db, "p", 1, 1)
	w.SetStatementPipelining(pipelining)
	w.SetPrimaryKey(myT, []string{"id"})
	w.SetPrimaryKey(myU, []string{"id"})
	return w, st
}

var (
	myT = model.TableID{Schema: "d", Table: "t"}
	myU = model.TableID{Schema: "d", Table: "u"}
)

func myRow(tb model.TableID, op model.Op, id int64) model.RowChange {
	rc := model.RowChange{Table: tb, Op: op, Key: []model.Value{id}}
	if op != model.OpDelete {
		rc.Cols = []model.ColValue{{Name: "id", Value: id}, {Name: "v", Value: fmt.Sprint(id)}}
	}
	return rc
}

func (st *fakeMyState) deltaUpdated() bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, q := range st.execs {
		if strings.Contains(q, "_strand_worker_delta") {
			return true
		}
	}
	return false
}

// Mọi đường ghi (khối upsert gom qua ranh giới Txn, đổi bảng/tập cột, DELETE, đổi PK = DELETE khóa cũ +
// upsert khóa mới), có và không pipelining: không báo nhầm, Stats.Rows == tổng row đã định tuyến.
func TestF11MySQLDetector_AllPathsCountEveryRow(t *testing.T) {
	for _, pipelining := range []bool{false, true} {
		w, st := newFakeMySQLWriter(t, pipelining)
		pk := myRow(myT, model.OpUpdate, 5)
		pk.OldKey = []model.Value{int64(1)}
		t1 := &model.Txn{Rows: []model.RowChange{
			myRow(myT, model.OpInsert, 1), myRow(myT, model.OpUpdate, 1), myRow(myT, model.OpInsert, 2),
			myRow(myU, model.OpInsert, 1), myRow(myT, model.OpDelete, 2),
		}}
		t2 := &model.Txn{Rows: []model.RowChange{
			pk, myRow(myT, model.OpInsert, 6), myRow(myU, model.OpDelete, 1), myRow(myU, model.OpInsert, 7),
		}}
		var stats ApplyStats
		if err := w.ApplyBatch(context.Background(), []*model.Txn{t1, t2}, DeltaRow{Applied: "a", Stats: &stats}); err != nil {
			t.Fatalf("pipelining %v: %v", pipelining, err)
		}
		if stats.Rows != 9 {
			t.Fatalf("pipelining %v: Stats.Rows = %d, want 9", pipelining, stats.Rows)
		}
		if !st.committed || !st.deltaUpdated() {
			t.Fatalf("pipelining %v: batch not committed with delta row (execs %q)", pipelining, st.execs)
		}
	}
}

// Row mất giữa định tuyến và writer (Txn khai RowCount 3 nhưng chỉ đưa 2 row): lỗi TRƯỚC row delta và
// commit — transaction rollback, lỗi mang seq/vị trí. Trước bản sửa writer MySQL commit batch thiếu row.
func TestF11MySQLDetector_RoutedRowMissingFailsBeforeCommit(t *testing.T) {
	for _, pipelining := range []bool{false, true} {
		w, st := newFakeMySQLWriter(t, pipelining)
		tx := &model.Txn{SourcePos: model.Pos{Engine: model.EngineMySQL, GTID: "u:1-5"}, RowCount: 3,
			Rows: []model.RowChange{myRow(myT, model.OpInsert, 1), myRow(myT, model.OpInsert, 2)}}
		tx.SetSeq(42)
		err := w.ApplyBatch(context.Background(), []*model.Txn{tx}, DeltaRow{Applied: "a"})
		var rn *RowsNotWrittenError
		if !errors.As(err, &rn) {
			t.Fatalf("pipelining %v: want RowsNotWrittenError, got %v", pipelining, err)
		}
		if rn.Routed != 3 || rn.Handed != 2 || rn.Written != 2 || fmt.Sprint(rn.Seqs) != "[42]" || len(rn.Positions) != 1 {
			t.Fatalf("pipelining %v: error fields %+v", pipelining, rn)
		}
		if st.committed || !st.rolled || st.deltaUpdated() {
			t.Fatalf("pipelining %v: batch must roll back before the delta row: committed %v rolled %v execs %q",
				pipelining, st.committed, st.rolled, st.execs)
		}
	}
}

// Lô multi-statement lỗi: không row nào của lô được tính là đã ghi (không dựa vào RowsAffected).
func TestF11MySQLDetector_FailedPipelinedBatchCountsNothing(t *testing.T) {
	q, _ := fakeQueue(true, 1<<20, 64<<20)
	fail := false
	q.send = func(context.Context, string, ...any) error {
		if fail {
			return errors.New("Error 1213: deadlock")
		}
		return nil
	}
	ctx := context.Background()
	if err := q.execRows(ctx, 3, "INSERT 1"); err != nil {
		t.Fatal(err)
	}
	if err := q.execRows(ctx, 1, "DELETE 1"); err != nil {
		t.Fatal(err)
	}
	if q.written != 0 || q.pending != 4 {
		t.Fatalf("before flush: written %d pending %d, want 0/4", q.written, q.pending)
	}
	if err := q.flush(ctx); err != nil {
		t.Fatal(err)
	}
	if q.written != 4 || q.pending != 0 {
		t.Fatalf("after flush: written %d pending %d, want 4/0", q.written, q.pending)
	}
	fail = true
	_ = q.execRows(ctx, 2, "INSERT 2")
	if err := q.flush(ctx); err == nil {
		t.Fatal("flush of failing batch: want error")
	}
	if q.written != 4 || q.pending != 0 {
		t.Fatalf("after failed flush: written %d pending %d, want 4/0", q.written, q.pending)
	}
	// Không pipelining: câu lỗi không được tính.
	q2, _ := fakeQueue(false, 0, 0)
	q2.send = func(context.Context, string, ...any) error { return errors.New("boom") }
	if err := q2.execRows(ctx, 5, "INSERT"); err == nil || q2.written != 0 {
		t.Fatalf("unpipelined failure: err %v written %d", err, q2.written)
	}
}

// Row đã đưa vào applyTxnMySQL nhưng khối upsert đang gom bị bỏ (lỗi kiểu "quên flush" khối cuối):
// written thiếu đúng số row đó và checkRowsWritten báo lỗi.
func TestF11MySQLDetector_UnflushedUpsertIsCaught(t *testing.T) {
	q, _ := fakeQueue(true, 1<<20, 64<<20)
	w := NewMySQLWriter(nil, "p", 1, 1)
	w.SetPrimaryKey(myT, []string{"id"})
	txn := &model.Txn{Rows: []model.RowChange{myRow(myT, model.OpDelete, 9), myRow(myT, model.OpInsert, 1), myRow(myT, model.OpInsert, 2)}}
	var cur *mysqlUpsertBatch
	if err := w.applyTxnMySQL(context.Background(), q, txn, &cur); err != nil {
		t.Fatal(err)
	}
	cur = nil // mô phỏng lỗi: khối upsert còn dở không được gửi
	if err := q.flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	err := checkRowsWritten([]*model.Txn{txn}, txn.TotalRows(), q.handed, q.written)
	var rn *RowsNotWrittenError
	if !errors.As(err, &rn) || rn.Handed != 3 || rn.Written != 1 {
		t.Fatalf("want RowsNotWrittenError handed 3 written 1, got %v", err)
	}
}
