package pipeline

import "testing"

// Chi phí giãn nhịp theo byte thật của chunk (e2e N10: chunk 500 row của bảng có AVG_ROW_LENGTH cũ
// 8 KiB bị tính như hàng nghìn trang, mỗi chunk chờ hàng phút ở IOPS thấp).
func TestChunkReadPagesUsesActualBytes(t *testing.T) {
	// 500 row × ~60 byte ≈ 30 KB → ~2 trang, bất kể AVG_ROW_LENGTH cũ là 8 KiB.
	if got := chunkReadPages(30_000, 8192, 500); got > 2 {
		t.Fatalf("pages = %v for a 30 KB chunk, want ≤ 2", got)
	}
	// Không biết byte: dùng chunk_rows hiệu lực, không phải số row mặc định.
	if got, want := chunkReadPages(0, 100, 500), 100.0*500/16384; got < want-1e-9 && got != 1 {
		t.Fatalf("fallback pages = %v, want %v (or the 1-page floor)", got, want)
	}
	if got := chunkReadPages(0, 8192, 500); got != 8192.0*500/16384 {
		t.Fatalf("fallback pages = %v, want %v", got, 8192.0*500/16384)
	}
	if got := chunkReadPages(1, 0, 0); got != 1 {
		t.Fatalf("pages = %v, want the 1-page floor", got)
	}
}
