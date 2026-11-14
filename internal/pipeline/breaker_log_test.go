package pipeline

import "testing"

func TestReasonKindIgnoresNumbers(t *testing.T) {
	a := reasonKind("strand query p99 latency ratio (4.00) exceeds max_ratio (3.00)")
	b := reasonKind("strand query p99 latency ratio (0.38) exceeds max_ratio (3.00)")
	if a != b || a == "" {
		t.Fatalf("%q vs %q", a, b)
	}
	if reasonKind("replica lag (12.00s) exceeds max threshold (10.00s)") == a {
		t.Fatal("different reasons collapsed")
	}
}
