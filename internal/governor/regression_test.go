package governor

import "testing"

// Text protocol trả Seconds_Behind_Source dạng []byte; trước đây bị đọc thành 0 nên breaker
// theo replica lag không bao giờ kích hoạt. Kiểm qua đường parse dùng chung.
func TestBreakerReplicaLagFromBytes(t *testing.T) {
	var v any = []byte("42")
	var got float64
	if b, ok := v.([]byte); ok {
		got = parseSecondsBytes(b)
	}
	if got != 42 {
		t.Fatalf("parsed %v, want 42", got)
	}
}
