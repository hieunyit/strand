package governor

import "testing"

// Driver MySQL trả Seconds_Behind_Source dạng uint64 (hoặc []byte với text protocol); trước đây
// chỉ nhận int64/string nên lag luôn bằng 0 và breaker theo replica lag không bao giờ kích hoạt.
func TestBreakerReplicaLagParsesDriverTypes(t *testing.T) {
	for _, v := range []any{uint64(42), int64(42), []byte("42"), "42", int32(42), float64(42)} {
		if got := parseLagValue(v); got != 42 {
			t.Fatalf("%T: parsed %v, want 42", v, got)
		}
	}
}
