package governor

import (
	"testing"
	"time"
)

// A ratio over max_ratio on millisecond latencies is noise (production: baseline 1 ms, p99 3-7 ms, the
// breaker opened ~20 times a day on an idle source). The ratio only counts from MinP99 (default 20 ms).
func TestBreakerLatencyFloor(t *testing.T) {
	ms := time.Millisecond
	for _, c := range []struct {
		name     string
		minP99   time.Duration
		p99, bl  time.Duration
		wantOpen bool
	}{
		{"tiny latencies, ratio 3.4", 0, 3400 * time.Microsecond, ms, false},
		{"tiny latencies, ratio 7.6", 0, 7600 * time.Microsecond, ms, false},
		{"over the default floor", 0, 25 * ms, ms, true},
		{"over the floor, ratio below max", 0, 25 * ms, 10 * ms, false},
		{"floor turned off", -1, 3400 * time.Microsecond, ms, true},
		{"custom floor", 50 * ms, 40 * ms, ms, false},
		{"custom floor reached", 50 * ms, 60 * ms, ms, true},
	} {
		b := NewBreaker(BreakerConfig{MaxRatio: 3, MinP99: c.minP99})
		if open, _ := b.Evaluate(DBSignals{QueryP99: c.p99, QueryBaseline: c.bl}); open != c.wantOpen {
			t.Errorf("%s: open=%v, want %v", c.name, open, c.wantOpen)
		}
	}
	// Hard signals and replica lag are not affected by the latency floor.
	b := NewBreaker(BreakerConfig{MaxReplicaLag: 10 * time.Second})
	if open, _ := b.Evaluate(DBSignals{ReplicaLagSeconds: 30, QueryP99: ms, QueryBaseline: ms}); !open {
		t.Error("replica lag must still open the breaker")
	}
	b = NewBreaker(BreakerConfig{})
	if open, _ := b.Evaluate(DBSignals{HardSignal: true, HardReason: "x", QueryP99: ms, QueryBaseline: ms}); !open {
		t.Error("a hard signal must still open the breaker")
	}
}
