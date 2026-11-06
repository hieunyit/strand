package writer

// Bộ phát hiện F11 "rows routed but not written" (checkRowsWritten): ApplyBatch đếm row nằm trong câu
// lệnh đã chạy thành công và so với tổng row đã định tuyến TRƯỚC commit. Kiểm thử thuần trên pgx.Tx giả
// của mô phỏng writer (f11_apply_sim_test.go), qua seam SetTxBeginner.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"

	"strand/internal/model"
)

func newDetectorDB() (*fDB, *PGWriter) {
	db := &fDB{tabs: map[string]map[int64]fRow{}, ordered: map[string]bool{quotePGTable(wsAccounts): true},
		unique: map[string]string{quotePGTable(wsAccounts): "code"}}
	for _, tb := range []model.TableID{wsItems, wsEvents, wsAccounts} {
		db.tabs[quotePGTable(tb)] = map[int64]fRow{}
	}
	db.tabs[quotePGTable(wsItems)][1] = fRow{"id": int64(1), "ver": int64(1), "qty": int64(0), "doc": "d1"}
	db.tabs[quotePGTable(wsAccounts)][1] = fRow{"id": int64(1), "ver": int64(1), "code": "A"}
	db.tabs[quotePGTable(wsEvents)][9] = fRow{"id": int64(9), "ver": int64(1)}
	w := NewPGWriter(nil, "p", 1, 1)
	for _, tb := range []model.TableID{wsItems, wsEvents, wsAccounts} {
		w.SetPrimaryKey(tb, []string{"id"})
	}
	w.SetTxBeginner(func(context.Context) (pgx.Tx, error) { return db.begin(), nil })
	return db, w
}

func evRow(op model.Op, id, ver int64) model.RowChange {
	rc := model.RowChange{Table: wsEvents, Op: op, Key: []model.Value{id}}
	if op != model.OpDelete {
		rc.Cols = []model.ColValue{{Name: "id", Value: id}, {Name: "ver", Value: ver}}
	}
	return rc
}

// Mọi đường ghi (khối upsert có khử trùng khóa — cả INSERT … ON CONFLICT lẫn COPY + MERGE —, gom DELETE,
// đổi PK, partial UPDATE TOAST, hàng đợi tuần tự của bảng ordered kể cả đổi PK) đều tính đủ row: không
// báo nhầm, Stats.Rows == tổng row đã định tuyến.
func TestF11Detector_AllPathsCountEveryRow(t *testing.T) {
	for _, copyMin := range []int{0, 1000} {
		db, w := newDetectorDB()
		w.SetCopyMinRows(copyMin)
		t1 := &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: 100}, Rows: []model.RowChange{
			evRow(model.OpInsert, 1, 10), evRow(model.OpUpdate, 1, 11), evRow(model.OpInsert, 2, 12), // khóa 1 khử trùng trong khối
			evRow(model.OpDelete, 2, 0), evRow(model.OpDelete, 9, 0),
		}}
		pk := evRow(model.OpUpdate, 5, 13)
		pk.OldKey = []model.Value{int64(1)}
		t2 := &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: 200}, Rows: []model.RowChange{
			pk,
			{Table: wsItems, Op: model.OpUpdate, Key: []model.Value{int64(1)}, Cols: []model.ColValue{
				{Name: "id", Value: int64(1)}, {Name: "ver", Value: int64(14)}, {Name: "qty", Value: int64(1)}, {Name: "doc", Unchanged: true}}},
			{Table: wsAccounts, Op: model.OpUpdate, Key: []model.Value{int64(7)}, OldKey: []model.Value{int64(1)}, Cols: []model.ColValue{
				{Name: "id", Value: int64(7)}, {Name: "ver", Value: int64(15)}, {Name: "code", Value: "B"}}},
			{Table: wsAccounts, Op: model.OpInsert, Key: []model.Value{int64(8)}, Cols: []model.ColValue{
				{Name: "id", Value: int64(8)}, {Name: "ver", Value: int64(16)}, {Name: "code", Value: "A"}}},
			{Table: wsAccounts, Op: model.OpDelete, Key: []model.Value{int64(8)}},
		}}
		t1.SetSeq(1)
		t2.SetSeq(2)
		var st ApplyStats
		if err := w.ApplyBatch(context.Background(), []*model.Txn{t1, t2}, DeltaRow{Applied: "d1", Stats: &st}); err != nil {
			t.Fatalf("copyMin %d: %v", copyMin, err)
		}
		if st.Rows != 10 {
			t.Fatalf("copyMin %d: Stats.Rows = %d, want 10", copyMin, st.Rows)
		}
		if db.delta != "d1" {
			t.Fatalf("copyMin %d: delta row not committed: %q", copyMin, db.delta)
		}
		ev := db.tabs[quotePGTable(wsEvents)]
		if len(ev) != 1 || toI64(ev[5]["ver"]) != 13 {
			t.Fatalf("copyMin %d: events = %v", copyMin, ev)
		}
	}
}

// Row bị mất giữa định tuyến và writer (Txn khai RowCount 3 mà chỉ đưa 2 row): lỗi trước commit, không ghi
// gì (kể cả row delta), lỗi mang seq/vị trí.
func TestF11Detector_RoutedRowMissingFailsBeforeCommit(t *testing.T) {
	db, w := newDetectorDB()
	tx := &model.Txn{SourcePos: model.Pos{Engine: model.EnginePG, LSN: 0x500}, RowCount: 3,
		Rows: []model.RowChange{evRow(model.OpInsert, 1, 10), evRow(model.OpInsert, 2, 11)}}
	tx.SetSeq(42)
	err := w.ApplyBatch(context.Background(), []*model.Txn{tx}, DeltaRow{Applied: "d1"})
	var rn *RowsNotWrittenError
	if !errors.As(err, &rn) {
		t.Fatalf("want RowsNotWrittenError, got %v", err)
	}
	if rn.Routed != 3 || rn.Handed != 2 || rn.Written != 2 || fmt.Sprint(rn.Seqs) != "[42]" || len(rn.Positions) != 1 {
		t.Fatalf("error fields: %+v", rn)
	}
	if db.delta != "" || len(db.tabs[quotePGTable(wsEvents)]) != 1 {
		t.Fatalf("batch must roll back: delta %q, events %v", db.delta, db.tabs[quotePGTable(wsEvents)])
	}
}

// Row đã đưa vào applier nhưng khối/gom DELETE/hàng đợi không được gửi (lỗi kiểu "quên flush"): bộ đếm
// written thiếu đúng số row đó.
func TestF11Detector_UnflushedRowsAreCaught(t *testing.T) {
	for _, lose := range []string{"upsert", "delete", "queue"} {
		db, w := newDetectorDB()
		tx := db.begin()
		a := &pgApplier{w: w, ctx: context.Background(), tx: tx, tmp: map[model.TableID]string{}, copyMinRows: 1000}
		rows := []model.RowChange{evRow(model.OpInsert, 3, 10), evRow(model.OpInsert, 3, 11)}
		switch lose {
		case "delete":
			rows = []model.RowChange{evRow(model.OpDelete, 9, 0)}
		case "queue":
			rows = []model.RowChange{{Table: wsAccounts, Op: model.OpDelete, Key: []model.Value{int64(1)}}}
		}
		batch := []*model.Txn{{Rows: rows}}
		for i := range rows {
			if err := a.apply(&rows[i]); err != nil {
				t.Fatal(err)
			}
		}
		a.up, a.del, a.q, a.qTables, a.qRows = nil, nil, nil, nil, nil // mô phỏng lỗi: phần chờ gửi bị bỏ
		if err := a.flushAll(); err != nil {
			t.Fatal(err)
		}
		err := checkRowsWritten(batch, int64(len(rows)), a.handed, a.written)
		var rn *RowsNotWrittenError
		if !errors.As(err, &rn) || rn.Written != 0 || rn.Handed != int64(len(rows)) {
			t.Fatalf("%s: want RowsNotWrittenError with written 0, got %v", lose, err)
		}
	}
}
