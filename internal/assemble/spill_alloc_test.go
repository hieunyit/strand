package assemble_test

import (
	"runtime"
	"testing"

	"strand/internal/assemble"
	"strand/internal/model"
)

// Mỗi transaction nguồn có một SpillBuffer. Trước đây buffer cấp sẵn 1024 RowChange (~172 KB, phải xóa
// trắng) kể cả cho transaction một dòng, và mảng đó đi theo Txn qua hàng đợi: ở strict PG→PG ~5000 Txn/s
// memclr + GC chiếm hơn nửa CPU và là trần thông lượng. Transaction nhỏ chỉ được cấp đúng phần nó dùng.
func TestSpillBufferSmallTxnAllocation(t *testing.T) {
	dir := t.TempDir()
	tbl := model.TableID{Schema: "bench", Table: "accounts"}
	row := model.RowChange{Table: tbl, Op: model.OpUpdate, Cols: []model.ColValue{{Name: "id", Value: int64(1)}, {Name: "balance", Value: int64(2)}}}
	run := func() {
		sb := assemble.NewSpillBuffer(0, dir)
		if err := sb.Append(row); err != nil {
			t.Fatal(err)
		}
		var txn model.Txn
		if err := sb.Finish(&txn); err != nil {
			t.Fatal(err)
		}
		if len(txn.Rows) != 1 || txn.Rows[0].Table != tbl {
			t.Fatalf("rows = %+v", txn.Rows)
		}
	}
	run()
	const n = 200
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		run()
	}
	runtime.ReadMemStats(&after)
	if per := (after.TotalAlloc - before.TotalAlloc) / n; per > 8<<10 {
		t.Fatalf("one-row transaction allocates %d bytes in the spill buffer, want <= 8 KiB", per)
	}
}
