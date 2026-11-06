package snapshot_test

import (
	"context"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

// rowsRecordingSource ghi lại cỡ chunk (rows) mà keyset walk dùng ở mỗi lần cắt chunk.
type rowsRecordingSource struct {
	memChunkSource
	rows []int64
}

func (r *rowsRecordingSource) KeysetEnd(ctx context.Context, t model.TableID, pk string, start, max, rows int64) (int64, error) {
	r.rows = append(r.rows, rows)
	return r.memChunkSource.KeysetEnd(ctx, t, pk, start, max, rows)
}

func runChunkRowsSnapshot(t *testing.T, n int64, hook func(model.TableID, snapshot.ChunkStat) int64) (*rowsRecordingSource, []snapshot.ChunkStat, int) {
	t.Helper()
	keys := make([]int64, n)
	for i := range keys {
		keys[i] = int64(i + 1)
	}
	opts := snapshot.DefaultChunkOptions()
	opts.DefaultChunkRows = 2
	s := snapshot.NewMySQLSnapshotter(opts)
	src := &rowsRecordingSource{memChunkSource: memChunkSource{keys: keys, maxCall: 1000}}
	s.Chunks = src
	flushed := 0
	s.FlushRows = func(_ context.Context, _ model.TableID, _ string, _ []string, rows []*model.RowChange, _, _ int64) error {
		flushed += len(rows)
		return nil
	}
	s.ClearTarget = func(context.Context, model.TableID) error { return nil }
	var stats []snapshot.ChunkStat
	if hook != nil {
		s.NextChunkRows = func(tb model.TableID, st snapshot.ChunkStat) int64 {
			stats = append(stats, st)
			return hook(tb, st)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := s.SnapshotAdd(ctx, nil, nil, regTable, "id", []string{"id"},
		func(context.Context, checkpoint.AppliedSet) error { return nil }); err != nil {
		t.Fatalf("SnapshotAdd: %v", err)
	}
	return src, stats, flushed
}

// Review N11: cỡ chunk của governor (NextChunkRows) phải được dùng cho từng chunk kế tiếp của keyset
// walk; trước đây chunk luôn cố định ChunkRows của plan.
func TestNextChunkRowsDrivesKeysetWalk(t *testing.T) {
	src, stats, flushed := runChunkRowsSnapshot(t, 100, func(_ model.TableID, st snapshot.ChunkStat) int64 { return st.Rows * 2 })
	if flushed != 100 {
		t.Fatalf("flushed %d rows, want 100", flushed)
	}
	want := []int64{2, 4, 8, 16, 32, 64}
	if len(src.rows) != len(want) {
		t.Fatalf("keyset chunk sizes = %v, want %v", src.rows, want)
	}
	for i := range want {
		if src.rows[i] != want[i] {
			t.Fatalf("keyset chunk sizes = %v, want %v", src.rows, want)
		}
	}
	// Số liệu chunk: cỡ đã dùng và số row thực đọc.
	if len(stats) == 0 || stats[0].Rows != 2 || stats[0].ReadRows != 2 || stats[0].Elapsed < 0 {
		t.Fatalf("first chunk stat = %+v", stats)
	}
}

func TestNextChunkRowsNilKeepsPlanSizeAndClampsToMin(t *testing.T) {
	src, _, flushed := runChunkRowsSnapshot(t, 7, nil)
	if flushed != 7 {
		t.Fatalf("flushed %d rows", flushed)
	}
	for _, r := range src.rows {
		if r != 2 {
			t.Fatalf("without NextChunkRows chunk size must stay 2: %v", src.rows)
		}
	}
	src, _, flushed = runChunkRowsSnapshot(t, 5, func(model.TableID, snapshot.ChunkStat) int64 { return 0 })
	if flushed != 5 || len(src.rows) < 2 || src.rows[1] != 1 {
		t.Fatalf("NextChunkRows → 0 must clamp to min_chunk_rows = 1: sizes %v, flushed %d", src.rows, flushed)
	}
}
