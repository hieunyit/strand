package snapshot_test

// Review 4.1: keyset walk `next = end + 1` tràn khi end == math.MaxInt64 (thành MinInt64) → đi lại
// cả bảng mãi; savePlan(EndPK+1) cũng tràn → resume đọc lại cả bảng.

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

// memChunkSource là ChunkSource trong bộ nhớ trên một tập khóa cố định.
type memChunkSource struct {
	keys    []int64
	keyset  int
	maxCall int
}

func (m *memChunkSource) BeforeRegister(context.Context) error { return nil }

func (m *memChunkSource) Bounds(context.Context, model.TableID, string) (*snapshot.TableBounds, error) {
	b := &snapshot.TableBounds{}
	if len(m.keys) > 0 {
		b.MinPK, b.MaxPK = m.keys[0], m.keys[len(m.keys)-1]
		b.Count = snapshot.SpanRows(b.MinPK, b.MaxPK)
	}
	return b, nil
}

func (m *memChunkSource) KeysetEnd(_ context.Context, _ model.TableID, _ string, start, max, rows int64) (int64, error) {
	m.keyset++
	if m.keyset > m.maxCall {
		return 0, fmt.Errorf("keyset walk did not terminate after %d calls (start=%d)", m.maxCall, start)
	}
	var in []int64
	for _, k := range m.keys {
		if k >= start && k <= max {
			in = append(in, k)
		}
	}
	if int64(len(in)) < rows {
		return max, nil
	}
	return in[rows-1], nil
}

func (m *memChunkSource) ReadChunk(_ context.Context, s *snapshot.MySQLSnapshotter, chunk *snapshot.Chunk, _ []string, _ int,
	table model.TableID) (snapshot.ChunkRead, checkpoint.AppliedSet, error) {
	s.Manager.BeginReading(chunk)
	s.Manager.StartReading(chunk, checkpoint.NewMySQLAppliedSet())
	var rows []*model.RowChange
	for _, k := range m.keys {
		if k >= chunk.StartPK && k <= chunk.EndPK {
			rows = append(rows, &model.RowChange{Table: table, Op: model.OpInsert, Key: []model.Value{k},
				Cols: []model.ColValue{{Name: "id", Value: k}}})
		}
	}
	return snapshot.ChunkRead{Rows: rows, Cut: chunk.EndPK}, checkpoint.NewMySQLAppliedSet(), nil
}

func runMaxIntSnapshot(t *testing.T, keys []int64, plans map[string]snapshot.SnapshotPlan) (map[int64]bool, *memChunkSource) {
	t.Helper()
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	opts := snapshot.DefaultChunkOptions()
	opts.DefaultChunkRows = 2
	s := snapshot.NewMySQLSnapshotter(opts)
	src := &memChunkSource{keys: keys, maxCall: 100}
	s.Chunks = src
	flushed := map[int64]bool{}
	s.FlushRows = func(_ context.Context, _ model.TableID, _ string, _ []string, rows []*model.RowChange, _, _ int64) error {
		for _, r := range rows {
			k := r.Key[0].(int64)
			if flushed[k] {
				return fmt.Errorf("key %d flushed twice (keyset walk re-walked the table)", k)
			}
			flushed[k] = true
		}
		return nil
	}
	s.ClearTarget = func(context.Context, model.TableID) error { return nil }
	s.LoadPlan = func(tb model.TableID) (snapshot.SnapshotPlan, bool) { p, ok := plans[tb.String()]; return p, ok }
	s.SavePlan = func(tb model.TableID, p snapshot.SnapshotPlan) error { plans[tb.String()] = p; return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.SnapshotAdd(ctx, nil, nil, regTable, "id", []string{"id"},
		func(context.Context, checkpoint.AppliedSet) error { return nil }); err != nil {
		t.Fatalf("SnapshotAdd: %v", err)
	}
	return flushed, src
}

func TestReview41_SnapshotKeysetWalkTerminatesAtMaxInt64(t *testing.T) {
	keys := []int64{-3, 1, 2, 3, math.MaxInt64 - 1, math.MaxInt64}
	plans := map[string]snapshot.SnapshotPlan{}
	flushed, _ := runMaxIntSnapshot(t, append([]int64(nil), keys...), plans)
	for _, k := range keys {
		if !flushed[k] {
			t.Fatalf("key %d not snapshotted (flushed=%v)", k, flushed)
		}
	}
	p := plans[regTable.String()]
	if !p.Finished || p.NextPK == math.MinInt64 {
		t.Fatalf("plan after full snapshot must be Finished without overflowed NextPK: %+v", p)
	}
	// Resume sau khi đã xong: không được đọc lại gì.
	flushed2, src2 := runMaxIntSnapshot(t, append([]int64(nil), keys...), plans)
	if len(flushed2) != 0 || src2.keyset != 0 {
		t.Fatalf("resume of finished plan re-read the table: flushed=%v keyset calls=%d", flushed2, src2.keyset)
	}
}

// Khoảng PK toàn int64: Count (max-min+1) tràn thành 0 → bảng bị coi là rỗng.
func TestReview41_FullInt64RangeNotTreatedAsEmpty(t *testing.T) {
	keys := []int64{math.MinInt64, 0, math.MaxInt64}
	flushed, _ := runMaxIntSnapshot(t, append([]int64(nil), keys...), map[string]snapshot.SnapshotPlan{})
	for _, k := range keys {
		if !flushed[k] {
			t.Fatalf("key %d not snapshotted (flushed=%v)", k, flushed)
		}
	}
}

func TestReview41_ChunkerArithmeticAtInt64Edges(t *testing.T) {
	c := snapshot.NewChunker()
	done := make(chan []*snapshot.Chunk, 1)
	go func() { done <- c.PlanChunks(regTable, "id", math.MaxInt64-4, math.MaxInt64, 2) }()
	select {
	case chunks := <-done:
		if len(chunks) != 3 || chunks[2].EndPK != math.MaxInt64 || chunks[0].StartPK != math.MaxInt64-4 {
			t.Fatalf("PlanChunks near MaxInt64: %d chunks, last=%+v", len(chunks), chunks[len(chunks)-1])
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("PlanChunks did not terminate near MaxInt64")
	}
	full := &snapshot.Chunk{StartPK: math.MinInt64, EndPK: math.MaxInt64}
	if full.RowCount() != math.MaxInt64 {
		t.Fatalf("RowCount of full int64 range = %d, want saturated MaxInt64", full.RowCount())
	}
	l, r, ok := c.SplitChunk(full, 1)
	if !ok || l.EndPK != -1 || r.StartPK != 0 || l.StartPK != math.MinInt64 || r.EndPK != math.MaxInt64 {
		t.Fatalf("SplitChunk of full int64 range: ok=%v left=[%d,%d] right=[%d,%d]", ok, l.StartPK, l.EndPK, r.StartPK, r.EndPK)
	}
}
