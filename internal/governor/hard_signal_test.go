package governor_test

import (
	"context"
	"testing"

	"strand/internal/governor"
)

// 02 §7: tín hiệu cứng lấy delta của counter; lần đọc đầu chỉ lấy mốc.
func TestHardSignal_CounterDeltaFirstSampleNeverTriggers(t *testing.T) {
	d := governor.NewHardSignalDetector()
	status := map[string]uint64{"Innodb_buffer_pool_wait_free": 1000, "Innodb_log_waits": 50}

	if hit, why := d.Evaluate(status, 0, 0); hit {
		t.Fatalf("first sample must not trigger (absolute value is not a signal): %s", why)
	}
	if hit, why := d.Evaluate(status, 0, 0); hit {
		t.Fatalf("unchanged counters must not trigger: %s", why)
	}

	status["Innodb_log_waits"] = 51
	hit, why := d.Evaluate(status, 0, 0)
	if !hit || why == "" {
		t.Fatal("an increase of Innodb_log_waits must trigger")
	}
	if hit, why := d.Evaluate(status, 0, 0); hit {
		t.Fatalf("an increase must trigger exactly once: %s", why)
	}

	status["Innodb_buffer_pool_wait_free"] = 1001
	if hit, _ := d.Evaluate(status, 0, 0); !hit {
		t.Fatal("an increase of Innodb_buffer_pool_wait_free must trigger")
	}

	// Counter giảm (restart / FLUSH STATUS): chỉ đặt lại mốc.
	status["Innodb_log_waits"] = 3
	if hit, why := d.Evaluate(status, 0, 0); hit {
		t.Fatalf("counter reset must not trigger: %s", why)
	}

	// Counter không đọc được ở một tick không làm mất mốc.
	if hit, _ := d.Evaluate(map[string]uint64{}, 0, 0); hit {
		t.Fatal("missing sample must not trigger")
	}
	status["Innodb_log_waits"] = 4
	if hit, _ := d.Evaluate(status, 0, 0); !hit {
		t.Fatal("increase after a missed sample must trigger")
	}
}

func TestHardSignal_SpoolThreshold(t *testing.T) {
	cases := []struct {
		used, hard int64
		want       bool
	}{
		{0, 0, false},
		{100, 0, false}, // không cấu hình hard limit
		{94, 100, false},
		{95, 100, true},
		{100, 100, true},
		{949, 1000, false},
		{950, 1000, true},
	}
	for _, c := range cases {
		if got := governor.SpoolNearFull(c.used, c.hard); got != c.want {
			t.Errorf("SpoolNearFull(%d, %d) = %v, want %v", c.used, c.hard, got, c.want)
		}
		d := governor.NewHardSignalDetector()
		if hit, _ := d.Evaluate(nil, c.used, c.hard); hit != c.want {
			t.Errorf("Evaluate spool %d/%d = %v, want %v", c.used, c.hard, hit, c.want)
		}
	}
}

// onoff: tín hiệu cứng dừng background ngay ở tick đầu tiên, không qua bộ lọc median/EWMA.
func TestHardSignal_OnoffStopsBackgroundImmediately(t *testing.T) {
	ctx := context.Background()
	var used, hard int64 = 10, 100
	reader := governor.NewMySQLDBSignalReader(nil, nil)
	reader.SpoolUsage = func() (int64, int64) { return used, hard }
	g := governor.NewOnoffGovernor(governor.OnoffConfig{SoftRate: 1000, SignalReader: reader})

	for i := 0; i < 3; i++ {
		if err := g.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !g.BackgroundRunning() {
		t.Fatal("background should run without pressure")
	}

	sig, _ := reader.ReadSignals(ctx)
	if sig.HardSignal {
		t.Fatalf("no hard signal expected at 10%% spool: %s", sig.HardReason)
	}

	used = 96
	if err := g.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if g.BackgroundRunning() {
		t.Fatal("hard signal must stop background on the same tick")
	}
	if _, bg := g.CurrentRate(); bg != 0 {
		t.Fatalf("background rate must be 0, got %v", bg)
	}
	if g.Zone() != governor.ZoneRed {
		t.Fatalf("hard signal must put governor in Red, got %v", g.Zone())
	}
	if !g.BreakerOpen() {
		t.Fatal("breaker must see the hard signal")
	}
}

func TestHardSignal_OnoffSimulatedHardSignal(t *testing.T) {
	ctx := context.Background()
	sim := governor.NewSimulatedSignalReader(governor.DBSignals{})
	g := governor.NewOnoffGovernor(governor.OnoffConfig{SoftRate: 1000, SignalReader: sim})
	if err := g.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if !g.BackgroundRunning() {
		t.Fatal("background should start running")
	}
	sim.SetSignals(governor.DBSignals{HardSignal: true, HardReason: "test"})
	if err := g.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if g.BackgroundRunning() {
		t.Fatal("HardSignal must stop background immediately")
	}
}
