package main

import (
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

var t0 = time.Unix(1_800_000_000, 0).UTC()

func okRestore() RestoreEvidence {
	return RestoreEvidence{SourceTreeWant: "a", SourceTreeGot: "a", TargetTreeWant: "b", TargetTreeGot: "b", ManifestWant: "m", ManifestGot: "m"}
}

// recFixture: snapshot [t0+60s, t0+660s) trên sóng chu kỳ 10 phút, 1 đoạn client phủ cả khoảng.
func recFixture() (RunRecord, []Sample, func(SegmentRun) (SegmentRaw, error)) {
	rec := RunRecord{Name: "01-onoff-1", Governor: "onoff", Repeat: 1, Attempt: 1, Engine: "mysql", Wave: protoWave,
		SoftIOPS: 400, HardIOPS: 600, LoadT0: t0, StrandStart: t0.Add(60 * time.Second), BootstrapFinished: t0.Add(1260 * time.Second),
		Restore: okRestore(), CompareOK: true, CaughtUp: true, StrandExitSet: true,
		Segments: []SegmentRun{{Index: 0, Start: t0, End: t0.Add(1300 * time.Second)}}}
	var samples []Sample
	for s := 0; s < 1300; s++ {
		samples = append(samples, Sample{T: t0.Add(time.Duration(s) * time.Second), R: 300, Zone: 0, P: 0.2, Breaker: 0, Lag: 1, OK: true})
	}
	parse := func(SegmentRun) (SegmentRaw, error) {
		var ev []Event
		for i := 0; i < 130000; i++ { // 100 event/s, latency 10 ms (+ 1 ms đuôi 2%)
			lat := int64(10_000_000)
			if i%50 == 0 {
				lat = 80_000_000
			}
			ev = append(ev, Event{At: t0.Add(time.Duration(i) * 10 * time.Millisecond), LatNS: lat})
		}
		return SegmentRaw{Events: ev, Reported: uint64(len(ev))}, nil
	}
	return rec, samples, parse
}

func TestAnalyzeCompleteRun(t *testing.T) {
	rec, samples, parse := recFixture()
	res, rows := Analyze(rec, samples, parse)
	if res.Evidence != "" || !res.OK {
		t.Fatalf("expected complete run: evidence=%q err=%q", res.Evidence, res.Error)
	}
	if math.Abs(float64(res.SnapshotSeconds)-1200) > 1e-9 || res.HardLimitExceed != 0 || res.P2PWindows != 3 {
		t.Fatalf("%+v", res)
	}
	if res.AppP99Ms != 80 { // 2% event chậm 80 ms: p99 nearest-rank rơi vào đuôi
		t.Fatalf("p99=%v", res.AppP99Ms)
	}
	if len(rows) != len(samples) || !finite(rows[500].P99ms) {
		t.Fatal("timeseries rows from raw logs missing")
	}
}

func TestAnalyzeCountsHardLimitViolations(t *testing.T) {
	rec, samples, parse := recFixture()
	samples[100].R, samples[200].R, samples[300].R = 601, 900, 600 // 600 = đúng hard, không vượt
	samples[1290].R = 5000                                         // sau snapshot: không tính
	res, _ := Analyze(rec, samples, parse)
	if res.HardLimitExceed != 2 {
		t.Fatalf("exceed=%d", res.HardLimitExceed)
	}
	samples[400].OK, samples[400].R = false, 9999 // mẫu lỗi: không tính vượt, nhưng làm run incomplete
	res, _ = Analyze(rec, samples, parse)
	if res.Evidence == "" || res.InvalidSamples == 0 {
		t.Fatalf("invalid scrape must make evidence incomplete: %+v", res)
	}
}

func TestAnalyzeFailClosedCases(t *testing.T) {
	cases := map[string]func(rec *RunRecord, s *[]Sample, parse *func(SegmentRun) (SegmentRaw, error)){
		"missing telemetry": func(rec *RunRecord, s *[]Sample, _ *func(SegmentRun) (SegmentRaw, error)) { *s = nil },
		"scrape gap": func(rec *RunRecord, s *[]Sample, _ *func(SegmentRun) (SegmentRaw, error)) {
			*s = append((*s)[:200], (*s)[300:]...)
		},
		"restore hash differs": func(rec *RunRecord, _ *[]Sample, _ *func(SegmentRun) (SegmentRaw, error)) {
			rec.Restore.SourceTreeGot = "x"
		},
		"client parse error": func(_ *RunRecord, _ *[]Sample, p *func(SegmentRun) (SegmentRaw, error)) {
			*p = func(SegmentRun) (SegmentRaw, error) { return SegmentRaw{}, fmt.Errorf("truncated") }
		},
		"segment failed": func(rec *RunRecord, _ *[]Sample, _ *func(SegmentRun) (SegmentRaw, error)) { rec.Segments[0].Exit = 1 },
		"no segments":    func(rec *RunRecord, _ *[]Sample, _ *func(SegmentRun) (SegmentRaw, error)) { rec.Segments = nil },
		"low coverage": func(rec *RunRecord, _ *[]Sample, _ *func(SegmentRun) (SegmentRaw, error)) {
			rec.Segments[0].End = rec.StrandStart.Add(500 * time.Second)
		},
		"no bootstrap": func(rec *RunRecord, _ *[]Sample, _ *func(SegmentRun) (SegmentRaw, error)) {
			rec.BootstrapFinished = time.Time{}
		},
		"no p2p window (snapshot short)": func(rec *RunRecord, _ *[]Sample, _ *func(SegmentRun) (SegmentRaw, error)) {
			rec.BootstrapFinished = rec.StrandStart.Add(200 * time.Second)
		},
		"no client events in window": func(_ *RunRecord, _ *[]Sample, p *func(SegmentRun) (SegmentRaw, error)) {
			*p = func(SegmentRun) (SegmentRaw, error) {
				return SegmentRaw{Events: []Event{{At: t0.Add(-time.Hour), LatNS: 1}}}, nil
			}
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			rec, samples, parse := recFixture()
			mut(&rec, &samples, &parse)
			res, _ := Analyze(rec, samples, parse)
			if res.Evidence == "" || res.OK {
				t.Fatalf("must be incomplete, got ok=%v evidence=%q", res.OK, res.Evidence)
			}
		})
	}
	// strand lỗi / so khớp lệch / chưa bắt kịp: run không OK dù bằng chứng đủ
	for name, mut := range map[string]func(r *RunRecord){
		"strand exit 1": func(r *RunRecord) { r.StrandExit = 1 },
		"compare":       func(r *RunRecord) { r.CompareOK = false },
		"not caught up": func(r *RunRecord) { r.CaughtUp = false },
		"exit unseen":   func(r *RunRecord) { r.StrandExitSet = false },
	} {
		rec, samples, parse := recFixture()
		mut(&rec)
		if res, _ := Analyze(rec, samples, parse); res.OK {
			t.Errorf("%s must not be ok", name)
		}
	}
}

func TestP2PLogRWindowsAndZeroHandling(t *testing.T) {
	rec, samples, _ := recFixture()
	// R dao động đúng e^0.2 trong nửa chu kỳ thứ nhất (sau 60 s đầu): p2p = 0.2
	for i := range samples {
		d := samples[i].T.Sub(t0)
		if d >= 60*time.Second && d < 300*time.Second {
			if i%2 == 0 {
				samples[i].R = 300 * math.Exp(0.2)
			}
		}
	}
	// snapshot bao nửa chu kỳ [0,300) cần start ≤ t0
	med, mx, n, z := p2pLogR(samples, t0, t0, t0.Add(500*time.Second), rec.Wave)
	if n != 1 || z != 0 || math.Abs(med-0.2) > 1e-9 || math.Abs(mx-0.2) > 1e-9 {
		t.Fatalf("med=%v max=%v n=%d zero=%d", med, mx, n, z)
	}
	for i := range samples {
		if d := samples[i].T.Sub(t0); d >= 100*time.Second && d < 110*time.Second {
			samples[i].R = 0 // R = 0 trong cửa sổ → loại và đếm riêng
		}
	}
	_, _, n, z = p2pLogR(samples, t0, t0, t0.Add(500*time.Second), rec.Wave)
	if n != 0 || z != 1 {
		t.Fatalf("zero window: n=%d zero=%d", n, z)
	}
	// cửa sổ không nằm trọn trong snapshot thì không dùng
	_, _, n, _ = p2pLogR(samples, t0, t0.Add(10*time.Second), t0.Add(500*time.Second), rec.Wave)
	if n != 0 {
		t.Fatalf("window outside snapshot used: %d", n)
	}
}

func TestCoverageUnion(t *testing.T) {
	a, b := t0, t0.Add(100*time.Second)
	segs := []SegmentRun{
		{Start: t0.Add(-10 * time.Second), End: t0.Add(40 * time.Second)},
		{Start: t0.Add(30 * time.Second), End: t0.Add(60 * time.Second)},
		{Start: t0.Add(80 * time.Second), End: t0.Add(200 * time.Second)},
	}
	if c := coverage(segs, a, b); math.Abs(c-0.8) > 1e-9 {
		t.Fatalf("coverage %v", c)
	}
	if coverage(nil, a, b) != 0 {
		t.Fatal("no segments")
	}
}

func TestTelemetryRoundTripAndSampleValidation(t *testing.T) {
	path := t.TempDir() + "/t.csv"
	in := []Sample{{T: t0, R: 1.5, Zone: 1, P: 0, Breaker: 0, Lag: 2, OK: true}, {T: t0.Add(time.Second)}}
	if err := WriteTelemetry(path, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadTelemetry(path)
	if err != nil || len(out) != 2 || out[0].R != 1.5 || !out[0].OK || out[1].OK || !out[0].T.Equal(t0) {
		t.Fatalf("%+v %v", out, err)
	}
	m := map[string]float64{`strand_governor_rate_tokens|source_iops`: 5, "strand_governor_zone|": 1, `strand_governor_pressure|total`: 0,
		"strand_breaker_open|": 0, "strand_lag_seconds|": 0.5}
	if s := sampleFrom(m, 200, t0, pressureKey("grace-v2")); !s.OK {
		t.Fatal("pressure 0 is valid")
	}
	delete(m, "strand_lag_seconds|")
	if s := sampleFrom(m, 200, t0, pressureKey("grace-v2")); s.OK {
		t.Fatal("missing lag must be invalid")
	}
	m["strand_lag_seconds|"] = math.NaN()
	if s := sampleFrom(m, 200, t0, pressureKey("grace-v2")); s.OK {
		t.Fatal("NaN lag must be invalid")
	}
	m["strand_lag_seconds|"] = 1
	delete(m, `strand_governor_pressure|total`)
	m[`strand_governor_pressure|replica_lag`] = 0.5
	if s := sampleFrom(m, 200, t0, pressureKey("grace-v2")); s.OK {
		t.Fatal("no fallback to another pressure signal")
	}
}

func TestPlotRunWritesSVGFromRows(t *testing.T) {
	rec, samples, parse := recFixture()
	_, rows := Analyze(rec, samples, parse)
	path := t.TempDir() + "/c.svg"
	if err := PlotRun(path, rec, rows); err != nil {
		t.Fatal(err)
	}
	if err := PlotRun(path, rec, nil); err == nil {
		t.Fatal("no rows must be an error")
	}
	b := readFileT(t, path)
	if !strings.Contains(b, "<polyline") || !strings.Contains(b, "p99") {
		t.Fatal("chart missing series")
	}
}

func readFileT(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestSamplerUsesGovernorSpecificPressure(t *testing.T) {
	// static/onoff chỉ xuất signal=replica_lag, grace-v2 xuất total; mỗi loại đọc đúng tín hiệu của mình.
	base := map[string]float64{`strand_governor_rate_tokens|source_iops`: 5, "strand_governor_zone|": 0, "strand_breaker_open|": 0, "strand_lag_seconds|": 0}
	onoff := map[string]float64{`strand_governor_pressure|replica_lag`: 0.3}
	grace := map[string]float64{`strand_governor_pressure|total`: 0.4}
	for k, v := range onoff {
		base[k] = v
	}
	if s := sampleFrom(base, 200, t0, pressureKey("onoff")); !s.OK || s.P != 0.3 {
		t.Fatalf("onoff %+v", s)
	}
	if s := sampleFrom(base, 200, t0, pressureKey("grace-v2")); s.OK {
		t.Fatal("grace-v2 without total must be invalid")
	}
	delete(base, `strand_governor_pressure|replica_lag`)
	for k, v := range grace {
		base[k] = v
	}
	if s := sampleFrom(base, 200, t0, pressureKey("grace-v2")); !s.OK || s.P != 0.4 {
		t.Fatalf("grace %+v", s)
	}
	if s := sampleFrom(base, 200, t0, pressureKey("static")); s.OK {
		t.Fatal("static without replica_lag must be invalid")
	}
}

func TestWindowLatSortedMatchesLinear(t *testing.T) {
	var ev []Event
	for i := 0; i < 500; i++ {
		ev = append(ev, Event{At: t0.Add(time.Duration(i) * 7 * time.Millisecond), LatNS: int64(i)})
	}
	a, b := t0.Add(300*time.Millisecond), t0.Add(2*time.Second)
	x, y := windowLatSorted(ev, a, b), WindowLatencies(ev, a, b)
	if len(x) == 0 || len(x) != len(y) {
		t.Fatalf("%d vs %d", len(x), len(y))
	}
	for i := range x {
		if x[i] != y[i] {
			t.Fatal("mismatch")
		}
	}
}

func TestStartupInvalidSamplesAreToleratedOnlyAtTheStart(t *testing.T) {
	rec, samples, parse := recFixture()
	start := int(rec.StrandStart.Sub(t0) / time.Second)
	for i := 0; i < 3; i++ { // 3 mẫu đầu sau khi chạy strand: metrics chưa sẵn sàng
		samples[start+i].OK = false
	}
	res, _ := Analyze(rec, samples, parse)
	if res.StartupInvalid != 3 || res.InvalidSamples != 0 || res.Evidence != "" {
		t.Fatalf("startup tolerated: %+v", res)
	}
	for i := 3; i < 6; i++ { // 6 mẫu liên tiếp > ngưỡng
		samples[start+i].OK = false
	}
	if res, _ = Analyze(rec, samples, parse); res.Evidence == "" {
		t.Fatal("startup longer than the threshold must be incomplete")
	}
	rec, samples, parse = recFixture()
	samples[start+30].OK = false // lỗi giữa chừng
	if res, _ = Analyze(rec, samples, parse); res.Evidence == "" {
		t.Fatal("mid-window invalid sample must be incomplete")
	}
}
