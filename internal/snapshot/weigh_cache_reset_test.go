package snapshot

import (
	"database/sql"
	"fmt"
	"testing"

	"strand/internal/model"
)

// Chunk snapshot lớn khóa chuỗi (PKWeight): EncodeMany phải dùng đúng các weight vừa tính theo lô. Cache
// weigher bị xóa khi vượt 100.000 phần tử; nếu việc xóa xảy ra giữa lúc điền lô thì Encode quay về hỏi
// MySQL từng giá trị (một truy vấn/row, trong lúc giữ Manager.mu).
func TestEncodeManyLargeChunksStayBatched(t *testing.T) {
	db, err := sql.Open("g2weigh", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := newMySQLWeigherState(db)
	col := PKColumn{Name: "k", Kind: PKWeight, Charset: "utf8mb4", Collation: "x_ci"}
	col2 := PKColumn{Name: "k2", Kind: PKWeight, Charset: "utf8mb4", Collation: "x_ci"}
	c := &PKCodec{Cols: []PKColumn{col, col2}, Weigh: w.weigh, WeighMany: w.weighMany}
	sizes := []int{30000, 30000, 30000, 15456, 15456, 15456, 15456}
	for chunk, rows := range sizes {
		tuples := make([][]model.Value, rows)
		for i := range tuples {
			tuples[i] = []model.Value{fmt.Sprintf("c%d-key-%06d", chunk, i), fmt.Sprintf("c%d-sub-%06d", chunk, i)}
		}
		q0 := g2Queries.Load()
		keys, err := c.EncodeMany(tuples)
		if err != nil {
			t.Fatal(err)
		}
		got := g2Queries.Load() - q0
		t.Logf("chunk %d: %d WEIGHT_STRING queries for %d rows", chunk, got, rows)
		if max := int64(2 * (rows/weighBatch + 2)); got > max {
			t.Errorf("chunk %d: %d queries for %d rows, want <= %d (batched)", chunk, got, rows, max)
		}
		// Khóa theo lô phải giống hệt khóa mã hóa từng tuple (luồng stream dùng Encode).
		single := &PKCodec{Cols: c.Cols, Weigh: newMySQLWeigherState(db).weigh}
		for _, i := range []int{0, rows / 2, rows - 1} {
			k, err := single.Encode(tuples[i])
			if err != nil {
				t.Fatal(err)
			}
			if k != keys[i] {
				t.Fatalf("chunk %d row %d: batched key %q != single key %q", chunk, i, keys[i], k)
			}
		}
	}
}
