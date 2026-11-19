package snapshot

import (
	"strings"
	"testing"

	"strand/internal/model"
)

func rowOf(size int) *model.RowChange {
	return &model.RowChange{Key: []model.Value{int64(1)}, Cols: []model.ColValue{{Name: "id", Value: int64(1)}, {Name: "v", Value: strings.Repeat("x", size)}}}
}

// Flush snapshot cắt lô theo byte và theo max_allowed_packet: trước đây cố định 1000 row — 1000 row 20 KB
// (~20 MB, tới ~40 MB khi escape) vượt max_allowed_packet 16 MiB và bị server cắt kết nối.
func TestSnapshotUpsertBatches(t *testing.T) {
	rows := make([]*model.RowChange, 1000)
	for i := range rows {
		rows[i] = rowOf(20 << 10)
	}
	mp := int64(16 << 20)
	b, err := snapshotUpsertBatches(rows, snapshotUpsertBudget(mp), snapshotUpsertMaxRows, mp)
	if err != nil {
		t.Fatal(err)
	}
	for _, bd := range b {
		var n int64
		for _, r := range rows[bd[0]:bd[1]] {
			n += snapshotRowBytes(r)
		}
		if 2*n > mp {
			t.Fatalf("batch %v ~%d bytes (x2 escape) exceeds max_allowed_packet %d", bd, n, mp)
		}
	}
	if b[len(b)-1][1] != len(rows) || b[0][0] != 0 {
		t.Fatalf("batches do not cover all rows: %v", b)
	}
	// Row nhỏ: ít round-trip hơn (25.000 row ~1,5 KB → vài lô 8 MB thay vì 25 lô 1000 row).
	small := make([]*model.RowChange, 25000)
	for i := range small {
		small[i] = rowOf(1500)
	}
	b, _ = snapshotUpsertBatches(small, snapshotUpsertBudget(64<<20), snapshotUpsertMaxRows, 64<<20)
	if len(b) > 6 {
		t.Fatalf("25,000 small rows → %d batches, want ≤ 6", len(b))
	}
	// Row không thể vừa một gói: lỗi rõ ràng.
	if _, err := snapshotUpsertBatches([]*model.RowChange{rowOf(9 << 20)}, snapshotUpsertBudget(16<<20), snapshotUpsertMaxRows, 16<<20); err == nil || !strings.Contains(err.Error(), "max_allowed_packet") {
		t.Fatalf("oversized row: err=%v", err)
	}
}
