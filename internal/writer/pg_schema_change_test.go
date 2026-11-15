package writer_test

import (
	"context"
	"testing"

	"strand/internal/model"
)

// P3-01: sau khi evolve đổi kiểu cột target (int → bigint), pipeline gọi lại SetPrimaryKey; COPY vào bảng tạm
// không được dùng mô tả cột pgx đã cache theo kiểu cũ. Code cũ: "unable to encode 5000000322 … for int4"
// (TestP3_E2E_SchemaEvolution_AdditiveAuto_MySQLToPG).
func TestPGWriter_CopyAfterTargetColumnWidened(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "copy-widen")
	tbl := pgTable(t, e.db, "copy_widen", "id bigint PRIMARY KEY, cnt int NOT NULL")
	w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})
	w.SetCopyMinRows(1) // mọi khối đi qua COPY
	k := func(id int) []model.Value { return []model.Value{int64(id)} }
	batch := func(base int, cnt int64) []*model.Txn {
		var rows []model.RowChange
		for i := 0; i < 5; i++ {
			rows = append(rows, ins(tbl, k(base+i), cv("id", int64(base+i)), cv("cnt", cnt)))
		}
		return []*model.Txn{{Rows: rows}}
	}
	// Làm nóng cache mô tả trên các kết nối của pool.
	for i := 0; i < 4; i++ {
		if err := w.ApplyBatch(ctx, batch(i*10, 7), e.delta("warm")); err != nil {
			t.Fatalf("warm batch %d: %v", i, err)
		}
	}
	pgExec(t, e.db, `ALTER TABLE public.copy_widen ALTER COLUMN cnt TYPE bigint`)
	w.SetPrimaryKey(tbl, []string{"id"}) // pipeline nạp lại metadata sau DDL
	for i := 0; i < 4; i++ {
		if err := w.ApplyBatch(ctx, batch(100+i*10, 5000000322), e.delta("wide")); err != nil {
			t.Fatalf("batch after widen %d: %v", i, err)
		}
	}
	var n int
	if err := e.db.QueryRow(`SELECT count(*) FROM public.copy_widen WHERE cnt = 5000000322`).Scan(&n); err != nil || n != 20 {
		t.Fatalf("wide rows %d (%v), want 20", n, err)
	}
}
