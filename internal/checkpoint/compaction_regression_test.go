package checkpoint_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// invariant: CP-4
// The old code re-checked the same >64 KB string after the sync compaction, so a
// delta could never shrink. A delta whose Txn are already in the global row, or that
// is merely written in a non-canonical form, must be reduced and accepted.
func TestCP4_LargeDeltaIsCompactedNotRejected(t *testing.T) {
	ctx := context.Background()
	db := getMySQL(t)
	pid := fmt.Sprintf("cp4-reg-%d", time.Now().UnixNano())
	store := checkpoint.NewMySQLStore(db, pid, model.EngineMySQL)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	epoch, _ := store.AdvanceEpoch(ctx, pid, "o", "h")
	c := checkpoint.NewCompactor(db, store, pid, model.EngineMySQL, model.EngineMySQL, checkpoint.DefaultCompactorConfig(pid, epoch))

	const u = "11111111-2222-3333-4444-555555555555"

	// 1. Non-canonical list of single GTIDs (>64 KB) is the range u:1-3500 (tiny).
	var b strings.Builder
	for i := 1; i <= 3500; i++ {
		if i > 1 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s:%d", u, i)
	}
	if b.Len() <= checkpoint.MaxDeltaBytes {
		t.Fatalf("setup: delta only %d bytes", b.Len())
	}
	if err := c.RecordWorkerDelta(ctx, nil, 1, b.String()); err != nil {
		t.Fatalf("compressible delta rejected: %v", err)
	}

	// 2. Every odd GNO up to 40000 (holes everywhere, >64 KB even canonical),
	//    but all of them already committed to the global row by worker 2.
	var odd strings.Builder
	odd.WriteString(u)
	for i := 3501; i <= 40000; i += 2 {
		fmt.Fprintf(&odd, ":%d", i)
	}
	if odd.Len() <= checkpoint.MaxDeltaBytes {
		t.Fatalf("setup: odd delta only %d bytes", odd.Len())
	}
	if _, err := db.ExecContext(ctx, "UPDATE _strand_checkpoint SET applied=? WHERE pipeline_id=?", u+":1-40000", pid); err != nil {
		t.Fatal(err)
	}
	if err := c.RecordWorkerDelta(ctx, nil, 2, odd.String()); err != nil {
		t.Fatalf("delta fully covered by global row rejected: %v", err)
	}

	var applied string
	_ = db.QueryRowContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id=? AND worker_id=2", pid).Scan(&applied)
	if len(applied) > checkpoint.MaxDeltaBytes {
		t.Fatalf("stored delta %d bytes > 64 KB", len(applied))
	}

	// Nothing may be lost: global ∪ deltas still contains every GTID.
	all, err := store.LoadApplied(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range []int{1, 3500, 3501, 39999, 40000} {
		if !all.Contains(model.Pos{Engine: model.EngineMySQL, GTID: fmt.Sprintf("%s:%d", u, g)}) {
			t.Fatalf("CP-1 violated: %s:%d lost", u, g)
		}
	}

	// 3. Genuinely irreducible delta is still refused.
	var hard strings.Builder
	hard.WriteString("99999999-2222-3333-4444-555555555555")
	for i := 1; i <= 80000; i += 2 {
		fmt.Fprintf(&hard, ":%d", i)
	}
	if err := c.RecordWorkerDelta(ctx, nil, 3, hard.String()); !errors.Is(err, checkpoint.ErrDeltaExceededLimit) {
		t.Fatalf("expected ErrDeltaExceededLimit, got %v", err)
	}
}
