package writer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/writer"
)

func TestS03_MySQLStaleWorkerCreationAfterTakeover(t *testing.T) {
	db, _ := getMySQL(t)
	ctx := context.Background()
	pid := fmt.Sprintf("stale-create-%d", time.Now().UnixNano())
	store := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	epoch, err := store.AdvanceEpoch(ctx, pid, "owner", "hash")
	if err != nil {
		t.Fatal(err)
	}
	current := writer.NewMySQLWriter(db, pid, 1, epoch)
	if err := current.EnsureWorkerRow(ctx); err != nil {
		t.Fatalf("current worker failed: %v", err)
	}
	old := writer.NewMySQLWriter(db, pid, 999, epoch)
	if _, err := store.AdvanceEpoch(ctx, pid, "new-owner", "hash"); err != nil {
		t.Fatal(err)
	}
	if err := old.EnsureWorkerRow(ctx); exitcode.GetExitCode(err) != exitcode.Fenced {
		t.Fatalf("stale creation must be fenced: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM _strand_worker_delta WHERE pipeline_id=? AND worker_id=999`, pid).Scan(&n); err != nil || n != 0 {
		t.Fatalf("stale worker persisted: %d %v", n, err)
	}
}
