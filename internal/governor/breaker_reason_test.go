package governor

import (
	"strings"
	"testing"
	"time"
)

// Lý do breaker phải đọc được khi đang mở (log + strand top) và rỗng khi đóng.
func TestBreakerReasonExposedWhileOpen(t *testing.T) {
	b := NewBreaker(BreakerConfig{MaxRatio: 3, CooldownTicks: 2})
	if b.Reason() != "" {
		t.Fatal("closed breaker has a reason")
	}
	b.Evaluate(DBSignals{QueryP99: 40 * time.Millisecond, QueryBaseline: 10 * time.Millisecond})
	if !b.IsOpen() || !strings.Contains(b.Reason(), "p99 latency ratio") {
		t.Fatalf("open=%v reason=%q", b.IsOpen(), b.Reason())
	}
	ok := DBSignals{QueryP99: 10 * time.Millisecond, QueryBaseline: 10 * time.Millisecond}
	b.Evaluate(ok)
	b.Evaluate(ok)
	if b.IsOpen() || b.Reason() != "" {
		t.Fatalf("after cooldown open=%v reason=%q", b.IsOpen(), b.Reason())
	}
}

// Nguồn MySQL/MariaDB: baseline tự học. Câu poll ổn định 40 ms (nguồn bận nhưng không quá tải) không
// được làm breaker mở; trước đây baseline hằng 10 ms → tỉ lệ 4 > 3 → snapshot đứng.
func TestMySQLReaderBaselineIsLearned(t *testing.T) {
	r := NewMySQLDBSignalReader(nil, nil)
	for i := 0; i < 150; i++ {
		r.latencyTracker.Record(40 * time.Millisecond)
	}
	b := NewBreaker(BreakerConfig{MaxRatio: 3})
	open, reason := b.Evaluate(DBSignals{QueryP99: r.latencyTracker.P99(), QueryBaseline: r.latencyTracker.Baseline()})
	if open {
		t.Fatalf("steady 40 ms poll opened the breaker: %s", reason)
	}
	for i := 0; i < 100; i++ {
		r.latencyTracker.Record(200 * time.Millisecond) // tăng 5 lần so với baseline học được
	}
	if open, _ := b.Evaluate(DBSignals{QueryP99: r.latencyTracker.P99(), QueryBaseline: r.latencyTracker.Baseline()}); !open {
		t.Fatal("5x latency increase over the learned baseline did not open the breaker")
	}
}
