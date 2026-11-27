package writer_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"strand/internal/model"
)

// spec: 02 §6 (COPY vào bảng tạm rồi MERGE)
//
// Bảng tạm vừa COPY không có thống kê; với bảng đích cỡ vừa planner từng chọn hash join và quét toàn
// bảng đích cho mỗi khối (vài chục row vào bảng 50k row: ~23 ms thay vì ~2 ms, strict PG→PG). MERGE
// phải dò index trên khóa, không quét tuần tự bảng đích. Đo bằng pg_stat_user_tables của bảng đích.
func TestPGWriter_MergeProbesTargetIndex(t *testing.T) {
	ctx := context.Background()
	e := newPGEnv(t, "merge-plan")
	tbl := pgTable(t, e.db, "merge_plan", "id int PRIMARY KEY, a int, b int, f text")
	// Nạp dữ liệu trên một kết nối riêng rồi ép đẩy thống kê của nó (ANALYZE tính một seq_scan, được
	// đẩy trễ): mốc đo phải gồm mọi lần quét của phần chuẩn bị.
	setup, err := e.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO public.merge_plan SELECT g, g, g, repeat('x', 80) FROM generate_series(1, 50000) g`,
		`ANALYZE public.merge_plan`,
		`SELECT pg_stat_force_next_flush()`,
		`SELECT 1`,
	} {
		if _, err := setup.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	_ = setup.Close()

	w := e.writer(t, map[model.TableID][]string{tbl: {"id"}})
	w.SetCopyMinRows(0) // mọi khối qua COPY + MERGE

	seq0, idx0 := mergePlanScans(t, e.db, -1)
	if seq0 == 0 {
		t.Fatal("thống kê của phần chuẩn bị chưa được đẩy (seq_scan của ANALYZE chưa thấy)")
	}

	var batch []*model.Txn
	for i := 0; i < 226; i++ {
		id := 1 + i*211 // 226 khóa rải đều, một phần mới (id > 50000)
		batch = append(batch, &model.Txn{Rows: []model.RowChange{
			ins(tbl, []model.Value{id}, cv("id", id), cv("a", -id), cv("b", i), cv("f", "y")),
		}})
	}
	if err := w.ApplyBatch(ctx, batch, e.delta("merge-plan-1")); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if mc, _ := w.GetStats(); mc == 0 {
		t.Fatal("khối không đi qua MERGE (SetCopyMinRows(0) không còn tác dụng?)")
	}

	seq1, idx1 := mergePlanScans(t, e.db, seq0+idx0)
	if seq1 != seq0 {
		t.Fatalf("MERGE quét tuần tự bảng đích %d lần (idx_scan +%d); muốn dò index trên khóa", seq1-seq0, idx1-idx0)
	}

	var n, neg int
	if err := e.db.QueryRow(`SELECT count(*), count(*) FILTER (WHERE a < 0) FROM public.merge_plan`).Scan(&n, &neg); err != nil {
		t.Fatal(err)
	}
	var fresh int
	for i := 0; i < 226; i++ {
		if 1+i*211 > 50000 {
			fresh++
		}
	}
	if neg != 226 || n != 50000+fresh {
		t.Fatalf("count=%d updated=%d; muốn count=%d updated=226", n, neg, 50000+fresh)
	}
}

// mergePlanScans đọc (seq_scan, idx_scan) của public.merge_plan. Thống kê của backend khác được đẩy
// trễ (PG 15+), nên khi after >= 0 chờ tới khi seq_scan+idx_scan vượt after — tức các lần quét của
// writer đã được ghi nhận (cùng một lần flush), dù plan là quét tuần tự hay dò index.
func mergePlanScans(t *testing.T, db *sql.DB, after int64) (int64, int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var seq, idx int64
		if err := db.QueryRow(`SELECT coalesce(seq_scan, 0), coalesce(idx_scan, 0) FROM pg_stat_user_tables WHERE relid = 'public.merge_plan'::regclass`).
			Scan(&seq, &idx); err != nil {
			t.Fatal(err)
		}
		if after < 0 || seq+idx > after {
			return seq, idx
		}
		if time.Now().After(deadline) {
			t.Fatalf("thống kê bảng không cập nhật sau 30s (seq_scan=%d idx_scan=%d)", seq, idx)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
