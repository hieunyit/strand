package snapshot_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/snapshot"
)

// invariant: SG-1 — review R07, phía target MySQL: flush chunk, xóa phần chưa đọc và clear bảng của
// một instance đã bị instance mới tăng epoch đều bị từ chối trong transaction; target không đổi.
func TestSG1_MySQLSnapshotWritesOfFencedInstanceAreRejected(t *testing.T) {
	_, dstDSN := getMySQLDSNs(t)
	db, err := sql.Open("mysql", dstDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	pid := "fence-mysql-snap"
	st := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
	if err := st.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	ep, err := st.AdvanceEpoch(ctx, pid, "old", "h")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS fence_t"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE fence_t (id BIGINT PRIMARY KEY, v INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO fence_t VALUES (1, 200)"); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := db.QueryRow("SELECT DATABASE()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	tbl := model.TableID{Schema: schema, Table: "fence_t"}
	old := checkpoint.WithFence(ctx, checkpoint.Fence{PipelineID: pid, Epoch: ep})
	row := []*model.RowChange{{Table: tbl, Op: model.OpInsert, Key: []model.Value{int64(1)},
		Cols: []model.ColValue{{Name: "id", Value: int64(1)}, {Name: "v", Value: int64(100)}}}}
	if err := snapshot.FlushRowsToTargetForTest(old, db, tbl, "id", []string{"id", "v"}, nil, 50, 60); err != nil {
		t.Fatalf("flush with current epoch: %v", err)
	}
	if _, err := st.AdvanceEpoch(ctx, pid, "new", "h"); err != nil {
		t.Fatal(err)
	}
	fenced := func(name string, err error) {
		t.Helper()
		var x *exitcode.Error
		if !errors.As(err, &x) || x.Code != exitcode.ErrFenced {
			t.Fatalf("%s by fenced instance: err=%v, want fenced", name, err)
		}
	}
	fenced("flush", snapshot.FlushRowsToTargetForTest(old, db, tbl, "id", []string{"id", "v"}, row, 1, 1))
	fenced("clear unread", snapshot.NewMySQLSnapshotter(snapshot.ChunkOptions{}).ClearUnreadForTest(old, db, tbl, "id", 1, 1))
	fenced("clear table", snapshot.ClearMySQLTableForTest(old, db, tbl))
	var v int
	if err := db.QueryRow("SELECT v FROM fence_t WHERE id = 1").Scan(&v); err != nil || v != 200 {
		t.Fatalf("target after fenced writes: v=%d err=%v, want 200", v, err)
	}
}
