package pipeline

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"strand/internal/governor"
)

func TestBreakerMinP99Config(t *testing.T) {
	for in, want := range map[string]time.Duration{"": 0, "bogus": 0, "-5ms": 0, "0s": -1, "0": -1, "50ms": 50 * time.Millisecond} {
		if got := breakerMinP99(in); got != want {
			t.Errorf("breakerMinP99(%q) = %v, want %v", in, got, want)
		}
	}
}

// Without background work (snapshot done, no auto-seed, no verify) an open breaker holds nothing back:
// it is neither logged as "snapshot paused" nor reported. When work starts it shows (and is logged).
func TestBreakerHiddenWithoutBackgroundWork(t *testing.T) {
	var buf bytes.Buffer
	p := &Pipeline{log: slog.New(slog.NewTextHandler(&buf, nil))}
	p.breaker = governor.NewBreaker(governor.BreakerConfig{MaxRatio: 3})
	p.breaker.Evaluate(governor.DBSignals{QueryP99: 100 * time.Millisecond, QueryBaseline: 10 * time.Millisecond})
	if !p.breaker.IsOpen() {
		t.Fatal("setup: breaker not open")
	}
	open, _ := p.logBreaker(false, "")
	if open || p.breakerShown() || strings.Contains(buf.String(), "OPEN") {
		t.Fatalf("idle pipeline: open=%v shown=%v log=%q", open, p.breakerShown(), buf.String())
	}
	p.bgWork.Add(1)
	open, reason := p.logBreaker(false, "")
	if !open || reason == "" || !p.breakerShown() || !strings.Contains(buf.String(), "snapshot paused") {
		t.Fatalf("with snapshot work: open=%v reason=%q log=%q", open, reason, buf.String())
	}
	p.bgWork.Add(-1)
	if open, _ = p.logBreaker(true, reason); open || !strings.Contains(buf.String(), "closed") {
		t.Fatalf("work finished: open=%v log=%q", open, buf.String())
	}
	p.autoSeeding.Store(true)
	if !p.breakerShown() {
		t.Fatal("auto-seed copy is background work")
	}
	p.autoSeeding.Store(false)
	p.verifying.Store(true)
	if !p.breakerShown() {
		t.Fatal("post-seed verify is background work")
	}
}
