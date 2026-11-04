package main

import (
	"math"
	"strings"
	"testing"
	"time"
)

func testOpts() options {
	return options{LowRate: 50, HighRate: 200, Period: 10 * time.Minute, SpikeEvery: 3 * time.Minute, SpikeDur: 10 * time.Second, SpikeMult: 2}
}

func TestRateAtSquareWaveAndSpikes(t *testing.T) {
	l := &load{o: testOpts()}
	cases := []struct {
		at   time.Duration
		want float64
	}{
		{0, 50}, {4 * time.Minute, 50}, {5 * time.Minute, 200}, {9*time.Minute + 59*time.Second, 200},
		{10 * time.Minute, 50}, {3 * time.Minute, 400}, {3*time.Minute + 9*time.Second, 400},
		{3*time.Minute + 10*time.Second, 50}, {6 * time.Minute, 400},
	}
	for _, c := range cases {
		if got := l.rateAt(c.at); got != c.want {
			t.Errorf("rateAt(%s) = %v, want %v", c.at, got, c.want)
		}
	}
}

func TestParseMetricsFiltersPipelineAndLabels(t *testing.T) {
	in := `# HELP x
strand_governor_rate_tokens{pipeline="other",resource="source_iops"} 999
strand_governor_rate_tokens{pipeline="bench",resource="source_iops"} 321.5
strand_governor_zone{pipeline="bench"} 2
strand_governor_pressure{pipeline="bench",signal="lag"} 0.4
strand_governor_pressure{pipeline="bench",signal="total"} 0.9
strand_breaker_open{pipeline="bench"} 1
strand_lag_seconds{pipeline="bench",phase="synced"} 1.25
`
	m := parseMetrics(strings.NewReader(in), "bench")
	if m["strand_governor_rate_tokens|source_iops"] != 321.5 || m["strand_governor_zone|"] != 2 ||
		m["strand_governor_pressure|total"] != 0.9 || m["strand_breaker_open|"] != 1 || m["strand_lag_seconds|"] != 1.25 {
		t.Fatalf("parsed %v", m)
	}
}

func TestP2PLogRPerHalfPeriodSkipsTransientSpikesAndZero(t *testing.T) {
	o := testOpts()
	o.SpikeEvery = 0
	t0 := time.Unix(1_000_000, 0)
	l := &load{o: o, t0: t0}
	var s []sample
	// Nửa chu kỳ 1 [0,5m): bị loại vì snapshot bắt đầu ở 30 s. Nửa 2 [5m,10m): R dao động ×e^0.2 sau
	// 60 s; 60 s đầu có giá trị cực (phải bị bỏ). Nửa 3 [10m,15m): có R = 0 → đếm zero, loại.
	for sec := 0; sec < 15*60; sec++ {
		r := 100.0
		switch {
		case sec >= 300 && sec < 360:
			r = 1 // chuyển tiếp
		case sec >= 360 && sec < 600:
			if sec%2 == 0 {
				r = 100 * math.Exp(0.2)
			}
		case sec >= 600 && sec < 900:
			if sec == 700 {
				r = 0
			}
		}
		s = append(s, sample{T: t0.Add(time.Duration(sec) * time.Second), R: r, OK: true})
	}
	med, mx, n, zero := p2pLogR(s, l, t0.Add(30*time.Second), t0.Add(15*time.Minute), o)
	if n != 1 || zero != 1 || math.Abs(med-0.2) > 1e-9 || math.Abs(mx-0.2) > 1e-9 {
		t.Fatalf("median=%v max=%v n=%d zero=%d", med, mx, n, zero)
	}
}

func TestDecideRules(t *testing.T) {
	mk := func(g string, snap, p99, p2p float64, hard int) runResult {
		return runResult{Governor: g, SnapshotSeconds: snap, AppP99Ms: p99, P2PLogRMedian: p2p, HardLimitExceed: hard, OK: true}
	}
	var runs []runResult
	for i := 0; i < 3; i++ {
		runs = append(runs, mk("onoff", 1000, 20, math.NaN(), 0), mk("static", 1200, 25, math.NaN(), 0), mk("grace-v2", 650, 19, 0.2, 0))
	}
	if _, d := decide(runs); !strings.HasPrefix(d, "grace-v2 đạt") {
		t.Fatalf("expected pass, got %s", d)
	}
	runs[2].SnapshotSeconds = 800 // median vẫn 650
	runs[5].SnapshotSeconds, runs[8].SnapshotSeconds = 750, 750
	if _, d := decide(runs); !strings.Contains(d, "KHÔNG đạt") {
		t.Fatalf("25%% faster must fail, got %s", d)
	}
	runs[2].SnapshotSeconds, runs[5].SnapshotSeconds, runs[8].SnapshotSeconds = 650, 650, 650
	runs[0].HardLimitExceed = 1
	if _, d := decide(runs); !strings.Contains(d, "vượt hard limit") {
		t.Fatalf("hard limit exceed must fail, got %s", d)
	}
	runs[0].HardLimitExceed = 0
	runs[2].P2PLogRMedian = 0.35
	if _, d := decide(runs); !strings.Contains(d, "p2p") {
		t.Fatalf("p2p > 0.3 must fail, got %s", d)
	}
	runs[2].P2PLogRMedian = 0.2
	for _, i := range []int{2, 5, 8} {
		runs[i].AppP99Ms = 21
	}
	if _, d := decide(runs); !strings.Contains(d, "p99") {
		t.Fatalf("worse p99 must fail, got %s", d)
	}
}
