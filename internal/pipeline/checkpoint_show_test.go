package pipeline

import (
	"context"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Hồi quy: `strand checkpoint show` lỗi "unsupported Scan, storing driver.Value type []uint8 into type
// *time.Time" khi DSN MySQL của target không có parseTime=true (cấu hình host/port/user thông thường).
func TestCheckpointShowWithoutParseTime(t *testing.T) {
	db, _ := startTestDB(t, "mysql") // DSN không có parseTime
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st := checkpoint.NewMySQLStore(db, "cps", model.EngineMySQL)
	if err := st.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AdvanceEpoch(ctx, "cps", "test", "h"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "REPLACE INTO _strand_checkpoint (pipeline_id, epoch, applied, updated_at) VALUES ('cps', 1, '', NOW(6))"); err != nil {
		t.Fatal(err)
	}
	p := &Pipeline{id: "cps", dst: db, dstEngine: model.EngineMySQL, srcEngine: model.EngineMySQL, store: st, epoch: 1}
	res, err := (&adminHandler{p: p}).CheckpointShow(ctx)
	if err != nil {
		t.Fatalf("checkpoint show: %v", err)
	}
	if d := time.Since(res.UpdatedAt); res.UpdatedAt.IsZero() || d < -time.Hour || d > time.Hour {
		t.Fatalf("updated_at = %v, want about now", res.UpdatedAt)
	}
}
