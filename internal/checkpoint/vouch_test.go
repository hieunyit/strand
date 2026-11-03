package checkpoint_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Compactor.SetVouch (source PG): coordinator dời mốc base của row toàn cục tới phần đã commit,
// kể cả khi không còn row delta nào (worker rảnh); vouch không đổi gì thì không ghi.
func TestCompactorVouchAdvancesPGBase(t *testing.T) {
	ctx := context.Background()
	db := getPostgres(t)
	pipelineID := fmt.Sprintf("vouch-%d", time.Now().UnixNano())
	store := checkpoint.NewPGStore(db, pipelineID, model.EnginePG)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	epoch, err := store.AdvanceEpoch(ctx, pipelineID, "test", "h")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteGlobal(ctx, checkpoint.Global{PipelineID: pipelineID, Epoch: epoch, Applied: checkpoint.NewPGAppliedSet(100).String()}); err != nil {
		t.Fatal(err)
	}
	// Worker 1 đã commit LSN 150 và 300 (vượt mốc).
	if _, err := db.ExecContext(ctx, "INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied) VALUES ($1, 1, $2, 1, $3)",
		pipelineID, epoch, "150,300"); err != nil {
		t.Fatal(err)
	}

	comp := checkpoint.NewCompactor(db, store, pipelineID, model.EnginePG, model.EnginePG, checkpoint.DefaultCompactorConfig(pipelineID, epoch))
	point := uint64(200)
	comp.SetVouch(func(g checkpoint.AppliedSet) { g.(*checkpoint.PGAppliedSet).Vouch(point, nil) })
	if err := comp.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	g := comp.LastGlobal().(*checkpoint.PGAppliedSet)
	if g.Lwd().LSN != 200 || !g.Contains(model.Pos{Engine: model.EnginePG, LSN: 300}) || g.Contains(model.Pos{Engine: model.EnginePG, LSN: 250}) {
		t.Fatalf("after vouch(200): global %s", g.String())
	}

	// Không còn delta (đã prune), mốc tiến tiếp → vẫn ghi.
	point = 300
	if err := comp.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadApplied(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Lwd().LSN != 300 {
		t.Fatalf("vouch without deltas not persisted: %s", loaded.(*checkpoint.PGAppliedSet).String())
	}

	// Vouch không đổi gì: không ghi (updated_at giữ nguyên).
	var before, after time.Time
	_ = db.QueryRowContext(ctx, "SELECT updated_at FROM _strand_checkpoint WHERE pipeline_id = $1", pipelineID).Scan(&before)
	time.Sleep(10 * time.Millisecond)
	if err := comp.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	_ = db.QueryRowContext(ctx, "SELECT updated_at FROM _strand_checkpoint WHERE pipeline_id = $1", pipelineID).Scan(&after)
	if !after.Equal(before) {
		t.Fatalf("idle compaction rewrote the global row (%s → %s)", before, after)
	}
}
