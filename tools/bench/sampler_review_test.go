package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// Telemetry chỉ có R không đủ chứng minh phép đo đầy đủ; P=0 hợp lệ không phải metric bị thiếu.
func TestReviewSamplerRequiresCompleteMetrics(t *testing.T) {
	const full = `strand_governor_rate_tokens{pipeline="bench",resource="source_iops"} 100
strand_governor_zone{pipeline="bench"} 0
strand_governor_pressure{pipeline="bench",signal="total"} 0
strand_governor_pressure{pipeline="bench",signal="replica_lag"} 99
strand_breaker_open{pipeline="bench"} 0
strand_lag_seconds{pipeline="bench",phase="synced"} 0
`
	for _, tc := range []struct {
		name, body string
		ok         bool
	}{
		{"only-rate", strings.Split(full, "\n")[0] + "\n", false},
		{"missing-lag", strings.ReplaceAll(full, `strand_lag_seconds{pipeline="bench",phase="synced"} 0`+"\n", ""), false},
		{"zero-total-is-present", full, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, tc.body) }))
			defer server.Close()
			smp := newSampler(strings.TrimPrefix(server.URL, "http://"), "bench")
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); smp.run(ctx, nil, time.Time{}) }()
			defer func() { cancel(); <-done }()
			deadline := time.NewTimer(3 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(10 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-deadline.C:
					t.Fatal("sampler produced no sample")
				case <-tick.C:
					ss := smp.snapshot()
					if len(ss) == 0 {
						continue
					}
					if ss[0].OK != tc.ok {
						t.Fatalf("sample validity=%v want %v: %+v", ss[0].OK, tc.ok, ss[0])
					}
					if tc.ok && ss[0].P != 0 {
						t.Fatalf("zero total pressure replaced by another signal: %v", ss[0].P)
					}
					return
				}
			}
		})
	}
}
