package main

import (
	"testing"
	"time"
)

func TestCalibrateFromEvents(t *testing.T) {
	var ev []Event
	for i := 0; i < 60000; i++ { // 1000 event/s trong 60 s
		ev = append(ev, Event{At: t0.Add(time.Duration(i) * time.Millisecond), LatNS: 1})
	}
	c, err := CalibrateFromEvents(ev, 5*time.Second, 16, "mysql")
	if err != nil || c.SaturationEPS < 999 || c.SaturationEPS > 1001 || c.High != 500 || c.Low != 125 {
		t.Fatalf("%+v %v", c, err)
	}
	if _, err := CalibrateFromEvents(ev[:5000], 5*time.Second, 16, "mysql"); err == nil {
		t.Fatal("too short must fail")
	}
	if _, err := CalibrateFromEvents(nil, 0, 1, "mysql"); err == nil {
		t.Fatal("no events must fail")
	}
}

func TestSummarizeOverhead(t *testing.T) {
	mk := func(pe, pp float64) []OverheadRound {
		return []OverheadRound{{"pristine", 100, 10}, {"patched", pe, pp}, {"patched", pe, pp}, {"pristine", 100, 10}}
	}
	if r, err := SummarizeOverhead(mk(99, 10.2)); err != nil || !r.Pass {
		t.Fatalf("%+v %v", r, err)
	}
	if r, _ := SummarizeOverhead(mk(97, 10)); r.Pass {
		t.Fatal("throughput loss > 2% must fail")
	}
	if r, _ := SummarizeOverhead(mk(100, 10.6)); r.Pass {
		t.Fatal("p99 +6% must fail")
	}
	if _, err := SummarizeOverhead(mk(100, 10)[:3]); err == nil {
		t.Fatal("unbalanced rounds must fail")
	}
	if _, err := SummarizeOverhead(mk(0, 10)); err == nil {
		t.Fatal("zero eps must fail")
	}
	e, p, err := parseSysbenchReport([]byte("    total time:                          10.0000s\n    total number of events:              500\n  Latency (ms):\n         99th percentile:                       45.79\n"))
	if err != nil || e != 50 || p != 45.79 {
		t.Fatalf("%v %v %v", e, p, err)
	}
}
