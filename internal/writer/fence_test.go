package writer_test

import (
	"context"
	"errors"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/writer"
)

// invariant: SG-1 — review R07. Instance cũ (epoch e) còn chạy sau khi instance mới tăng epoch: mọi
// ghi snapshot của instance cũ (range, tập khóa, phần chưa đọc, clear bảng) bị từ chối trong chính
// transaction và target không đổi.
func TestSG1_SnapshotWritesOfFencedInstanceAreRejected(t *testing.T) {
	e := newPGEnv(t, "pg-fence")
	tbl := pgTable(t, e.db, "fence_t", "id bigint PRIMARY KEY, v int")
	pgExec(t, e.db, `INSERT INTO public.fence_t VALUES (1, 200)`)
	old := checkpoint.WithFence(context.Background(), checkpoint.Fence{PipelineID: e.pid, Epoch: e.epoch})
	stale := []*model.RowChange{{Table: tbl, Op: model.OpInsert, Key: []model.Value{int64(1)}, Cols: []model.ColValue{cv("id", int64(1)), cv("v", int64(100))}}}

	// Trước khi bị fence: ghi được.
	if err := writer.PGReplaceKeys(old, e.db, tbl, "id", []string{"id", "v"}, nil, []int64{999}); err != nil {
		t.Fatalf("write with current epoch: %v", err)
	}
	if _, err := e.store.AdvanceEpoch(context.Background(), e.pid, "new-instance", "hash"); err != nil {
		t.Fatal(err)
	}
	fenced := func(name string, err error) {
		t.Helper()
		var x *exitcode.Error
		if !errors.As(err, &x) || x.Code != exitcode.ErrFenced {
			t.Fatalf("%s by fenced instance: err=%v, want fenced", name, err)
		}
	}
	fenced("PGReplaceRange", writer.PGReplaceRange(old, e.db, tbl, "id", []string{"id", "v"}, stale, 1, 1))
	fenced("PGReplaceKeys", writer.PGReplaceKeys(old, e.db, tbl, "id", []string{"id", "v"}, stale, []int64{1}))
	fenced("PGDeleteUnread", writer.PGDeleteUnread(old, e.db, tbl, "id", 1, 1, true, nil))
	fenced("PGClearTable", writer.PGClearTable(old, e.db, tbl))
	var v int64
	if err := e.db.QueryRow(`SELECT v FROM public.fence_t WHERE id = 1`).Scan(&v); err != nil || v != 200 {
		t.Fatalf("target after fenced writes: v=%d err=%v, want 200", v, err)
	}
}
