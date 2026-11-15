package pipeline

import (
	"context"
	"testing"
	"time"

	"strand/internal/governor"
	"strand/internal/model"
	"strand/internal/sched"
)

// lagSeconds phải tính cả phần source chưa đọc: hàng đợi nội bộ rỗng (Lwd >= admitted) nhưng đầu source đi
// trước vị trí reader (pause, backpressure, ...) thì lag > 0; source không có gì mới thì vẫn 0.
func TestLagSeconds_SourceAheadOfReader(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "static")
	p.sched = sched.NewScheduler(sched.DefaultConfig())
	p.lastSourceTS.Store(time.Now().Add(-30 * time.Second).Unix())

	if got := p.lagSeconds(); got != 0 {
		t.Fatalf("queue empty, source at reader position: lag = %v, want 0", got)
	}
	p.srcAhead.Store(true)
	if got := p.lagSeconds(); got < 29 || got > 35 {
		t.Fatalf("queue empty, source ahead of reader: lag = %v, want ~30", got)
	}
	// Chưa có mốc thời gian nào (chưa thấy Txn): không bịa lag.
	p.lastSourceTS.Store(0)
	if got := p.lagSeconds(); got != 0 {
		t.Fatalf("no timestamp known: lag = %v, want 0", got)
	}
	// Chưa apply nhưng đã route: dùng mốc router.
	p.lastRoutedTS.Store(time.Now().Add(-10 * time.Second).Unix())
	if got := p.lagSeconds(); got < 9 || got > 15 {
		t.Fatalf("fallback to routed ts: lag = %v, want ~10", got)
	}
}

// Thăm dò đầu source: PG walsender cách head xa → srcAhead; ở head → không; không có pgSignals →
// không (lùi về cách tính theo hàng đợi, không crash).
func TestPollSourceAhead_PG(t *testing.T) {
	p := wiringPipeline(model.EnginePG, "grace-v2")
	p.sched = sched.NewScheduler(sched.DefaultConfig())
	p.lastSourceTS.Store(time.Now().Add(-20 * time.Second).Unix())
	ctx := context.Background()

	p.pollSourceAhead(ctx) // pgSignals nil: không biết
	if p.srcAhead.Load() || p.lagSeconds() != 0 {
		t.Fatal("unknown source head must fall back to queue-based lag (0)")
	}

	p.src = fakePGSource(nil, "1024", 0, float64(64<<20)) // walsender cách head 64 MiB
	p.pgSignals = governor.NewPGDBSignalReader(p.src, "s", nil)
	p.pollSourceAhead(ctx)
	if !p.srcAhead.Load() {
		t.Fatal("source head ahead of reader must set srcAhead")
	}
	if got := p.lagSeconds(); got < 19 {
		t.Fatalf("lag = %v while source is ahead, want >= 19", got)
	}

	p.src = fakePGSource(nil, "1024", 0, 4096)
	p.pgSignals = governor.NewPGDBSignalReader(p.src, "s", nil)
	p.pollSourceAhead(ctx)
	if p.srcAhead.Load() || p.lagSeconds() != 0 {
		t.Fatal("reader at head: lag must be 0 again")
	}
}

// Operator pause làm lag tăng vì chính ta dừng đọc; governor không được coi đó là tín hiệu quá tải.
func TestFeedGrace_OperatorPauseIsNotLagPressure(t *testing.T) {
	p := wiringPipeline(model.EngineMySQL, "grace-v2")
	p.sched = sched.NewScheduler(sched.DefaultConfig())
	gg := governor.NewGraceGovernor(governor.GraceConfig{SoftRate: 600})
	p.gov = gg
	p.lastSourceTS.Store(time.Now().Add(-60 * time.Second).Unix())
	p.srcAhead.Store(true)
	p.paused.Store(true)
	p.feedGrace(context.Background(), gg)
	if got := gg.Pressure(); got != 0 {
		t.Fatalf("operator pause must not feed lag pressure, got %v", got)
	}
}
