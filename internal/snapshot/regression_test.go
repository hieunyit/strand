package snapshot_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"math"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

var regTable = model.TableID{Schema: "app", Table: "t"}

func regManager(nChunks int64, rows int64) *snapshot.RangeStateManager {
	c := snapshot.NewChunker()
	chunks := c.PlanChunks(regTable, "id", 1, nChunks*rows, rows)
	m := snapshot.NewRangeStateManager()
	m.RegisterTable(regTable, "id", chunks, nChunks*rows)
	return m
}

// invariant: RS-1
// A PK that cannot be mapped to a range used to return ActionApply, bypassing the
// range-state machine (a later chunk flush could then overwrite a newer image).
func TestRS1_UnmappablePKIsRejectedNotApplied(t *testing.T) {
	m := regManager(4, 10)
	for _, key := range [][]model.Value{{"abc"}, {uint64(math.MaxInt64) + 10}, {nil}, {}} {
		ev := &model.RowChange{Table: regTable, Op: model.OpUpdate, Key: key}
		if act, _ := m.HandleStreamEvent(ev, model.Pos{Engine: model.EngineMySQL, GTID: "u:1"}); act != snapshot.ActionReject {
			t.Fatalf("key %v: got %v, want Reject", key, act)
		}
	}
}

// The MySQL text protocol returns integers as []byte; they must still be routed.
func TestRS1_ByteSlicePKRouted(t *testing.T) {
	m := regManager(4, 10)
	ev := &model.RowChange{Table: regTable, Op: model.OpUpdate, Key: []model.Value{[]byte("15")}}
	act, chunk := m.HandleStreamEvent(ev, model.Pos{Engine: model.EngineMySQL, GTID: "u:1"})
	if act != snapshot.ActionDiscard || chunk == nil || chunk.StartPK != 11 {
		t.Fatalf("[]byte PK 15 not routed to pending chunk [11,20]: %v %+v", act, chunk)
	}
}

// invariant: RS-1, RS-2
// Without the rendezvous the chunk is flushed before the stream has processed every
// event up to the high watermark, so the target can briefly go back in time.
func TestRS1_SnapshotAddRequiresRendezvous(t *testing.T) {
	s := snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())
	err := s.SnapshotAdd(context.Background(), nil, nil, regTable, "id", []string{"id"}, nil)
	if err == nil {
		t.Fatalf("SnapshotAdd must refuse a nil rendezvous function")
	}
}

// Performance: routing an event must not scan every chunk (1 TB table = millions of chunks).
func TestRS1_FindChunkIsNotLinear(t *testing.T) {
	m := regManager(500_000, 10)
	ev := &model.RowChange{Table: regTable, Op: model.OpUpdate, Key: []model.Value{int64(500_000*10 - 3)}}
	start := time.Now()
	for i := 0; i < 20_000; i++ {
		m.HandleStreamEvent(ev, model.Pos{Engine: model.EngineMySQL, GTID: "u:1"})
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("20k events took %v with 500k chunks: chunk lookup is linear", d)
	}
}

// Bảng nguồn rỗng: snapshot phải kết thúc ngay (không chạy keyset walk với plan rỗng).
func TestRS1_EmptyTableSnapshotFinishes(t *testing.T) {
	s := snapshot.NewMySQLSnapshotter(snapshot.DefaultChunkOptions())
	saved := false
	s.LoadPlan = func(model.TableID) (snapshot.SnapshotPlan, bool) {
		return snapshot.SnapshotPlan{Empty: true, ChunkRows: 5000}, true
	}
	s.SavePlan = func(model.TableID, snapshot.SnapshotPlan) error { saved = true; return nil }
	err := s.SnapshotAdd(context.Background(), nil, nil, regTable, "id", []string{"id"},
		func(context.Context, checkpoint.AppliedSet) error { return nil })
	if err != nil {
		t.Fatalf("empty table snapshot failed: %v", err)
	}
	_ = saved
}
