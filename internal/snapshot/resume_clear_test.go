package snapshot_test

import (
	"context"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

// Regression (review Codex 2026-11-05, TestG1_E2E_Kill9RandomCrashes iteration 7: 1062 trên
// accounts.uk_code): crash giữa lúc flush chunk (target commit) và lưu plan để lại ở target row của phần
// plan còn coi là "Chưa đọc". Khi chạy tiếp, phần đó phải được xóa ở target TRƯỚC khi đăng ký range và
// trước mọi flush, gồm cả các khóa chờ đọc lại (Reread).
// invariant: RS-1/2 — phần chưa đọc ở target rỗng cho tới khi chunk của nó flush.
func TestRS1_ResumeClearsUnreadPartBeforeRegister(t *testing.T) {
	opts := snapshot.DefaultChunkOptions()
	opts.DefaultChunkRows = 2
	s := snapshot.NewMySQLSnapshotter(opts)
	s.Chunks = &memChunkSource{keys: []int64{1, 2, 3, 4, 5, 6, 7, 8}, maxCall: 100}
	// Target: [1,3] đã flush; 4,5 là row cũ của chunk đã flush mà plan chưa kịp ghi; 2 chờ đọc lại.
	target := map[int64]bool{1: true, 2: true, 3: true, 4: true, 5: true}
	var events []string
	s.ClearUnread = func(_ context.Context, _ model.TableID, _ string, lo, hi int64, hasRange bool, keys []int64) error {
		events = append(events, "clear")
		if !hasRange || lo != 4 || hi != 8 || len(keys) != 1 || keys[0] != 2 {
			t.Errorf("ClearUnread(lo=%d hi=%d hasRange=%v keys=%v), want [4,8] and keys [2]", lo, hi, hasRange, keys)
		}
		for k := lo; k <= hi; k++ {
			delete(target, k)
		}
		for _, k := range keys {
			delete(target, k)
		}
		return nil
	}
	flush := func(rows []*model.RowChange) {
		events = append(events, "flush")
		for _, r := range rows {
			target[r.Key[0].(int64)] = true
		}
	}
	s.FlushRows = func(_ context.Context, _ model.TableID, _ string, _ []string, rows []*model.RowChange, _, _ int64) error {
		flush(rows)
		return nil
	}
	s.FlushKeys = func(_ context.Context, _ model.TableID, _ string, _ []string, rows []*model.RowChange, _ []int64) error {
		flush(rows)
		return nil
	}
	s.ClearTarget = func(context.Context, model.TableID) error {
		t.Error("resume must not clear the whole table")
		return nil
	}
	plans := map[string]snapshot.SnapshotPlan{regTable.String(): {MinPK: 1, MaxPK: 8, ChunkRows: 2, NextPK: 4, Reread: []int64{2}}}
	s.LoadPlan = func(tb model.TableID) (snapshot.SnapshotPlan, bool) { p, ok := plans[tb.String()]; return p, ok }
	s.SavePlan = func(tb model.TableID, p snapshot.SnapshotPlan) error { plans[tb.String()] = p; return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.SnapshotAdd(ctx, nil, nil, regTable, "id", []string{"id"},
		func(context.Context, checkpoint.AppliedSet) error { return nil }); err != nil {
		t.Fatalf("SnapshotAdd: %v", err)
	}
	if len(events) == 0 || events[0] != "clear" {
		t.Fatalf("unread part must be cleared before any flush: %v", events)
	}
	for k := int64(1); k <= 8; k++ {
		if !target[k] {
			t.Fatalf("key %d missing on target after resumed snapshot: %v", k, target)
		}
	}
}
