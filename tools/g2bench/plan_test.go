package main

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

var protoWave = Wave{Low: 50, High: 200, Period: 10 * time.Minute, SpikeEvery: 3 * time.Minute, SpikeDur: 10 * time.Second, SpikeMult: 2}

func TestWaveRateAt(t *testing.T) {
	cases := []struct {
		at   time.Duration
		want float64
	}{
		{0, 50}, {4 * time.Minute, 50}, {5 * time.Minute, 200}, {9*time.Minute + 59*time.Second, 200}, {10 * time.Minute, 50},
		{3 * time.Minute, 400}, {3*time.Minute + 9*time.Second, 400}, {3*time.Minute + 10*time.Second, 50},
		{6 * time.Minute, 400}, {6*time.Minute + 11*time.Second, 200}, {1 * time.Second, 50},
	}
	for _, c := range cases {
		if got := protoWave.RateAt(c.at); got != c.want {
			t.Errorf("RateAt(%v)=%v want %v", c.at, got, c.want)
		}
	}
}

func TestNextSegmentCoversTimelineWithConstantRate(t *testing.T) {
	var at time.Duration
	end := 25 * time.Minute
	var prevEnd time.Duration
	n := 0
	for at < end {
		s := protoWave.NextSegment(at, 30*time.Second)
		if s.Start != at || s.Dur <= 0 || s.Dur > 30*time.Second || (n > 0 && s.Start != prevEnd) {
			t.Fatalf("bad segment %+v at %v", s, at)
		}
		// hằng định: tốc độ ở mọi mốc 1 s trong đoạn bằng tốc độ đoạn
		for d := s.Start; d < s.Start+s.Dur; d += time.Second {
			if protoWave.RateAt(d) != s.Rate {
				t.Fatalf("rate changes inside segment %+v at %v", s, d)
			}
		}
		prevEnd = s.Start + s.Dur
		at = prevEnd
		n++
	}
	// không chunk nhỏ hơn: đoạn đầu dài tới spike 3 phút = 30 s chunk
	if s := protoWave.NextSegment(3*time.Minute, time.Hour); s.Dur != 10*time.Second || s.Rate != 400 {
		t.Fatalf("spike segment %+v", s)
	}
	if s := protoWave.NextSegment(5*time.Minute+30*time.Second, time.Hour); s.Rate != 200 || s.Start+s.Dur != 6*time.Minute {
		t.Fatalf("high segment %+v", s)
	}
}

func TestRunOrderRandomizedDeterministicAndComplete(t *testing.T) {
	gov := []string{"static", "onoff", "grace-v2"}
	a := RunOrder(gov, 3, 42)
	b := RunOrder(gov, 3, 42)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed must give same order")
	}
	if len(a) != 9 {
		t.Fatalf("len %d", len(a))
	}
	seen := map[Job]bool{}
	for _, j := range a {
		seen[j] = true
	}
	if len(seen) != 9 {
		t.Fatal("jobs must be a permutation of 3 governors x 3 repeats")
	}
	diff := 0
	for seed := int64(1); seed <= 20; seed++ {
		if !reflect.DeepEqual(RunOrder(gov, 3, seed), a) {
			diff++
		}
	}
	if diff == 0 {
		t.Fatal("order does not depend on seed")
	}
	sorted := append([]Job(nil), a...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].String() < sorted[j].String() })
	if reflect.DeepEqual(sorted, a) {
		t.Log("order happens to be sorted for this seed") // không lỗi, chỉ ghi
	}
}

func TestOfficialProblems(t *testing.T) {
	ok := Opts{Governors: "static,onoff,grace-v2", Repeats: 3, FixtureDir: "f", SourceContainer: "s", TargetContainer: "t", HelperImage: "h",
		ClientBinary: "/b", ClientSHA: "abc", Wave: protoWave, Warmup: 2 * time.Minute, StreamAfter: 2 * time.Minute,
		Clients: 16, SoftIOPS: 400, HardIOPS: 600, Attest: true}
	if p := OfficialProblems(ok, true); len(p) != 0 {
		t.Fatalf("complete options flagged: %v", p)
	}
	mut := map[string]func(o *Opts){
		"governors": func(o *Opts) { o.Governors = "static,onoff" },
		"repeats":   func(o *Opts) { o.Repeats = 1 },
		"only":      func(o *Opts) { o.Only = "onoff" },
		"restore":   func(o *Opts) { o.FixtureDir = "" },
		"client":    func(o *Opts) { o.ClientSHA = "" },
		"wave":      func(o *Opts) { o.Wave.Period = time.Minute },
		"warmup":    func(o *Opts) { o.Warmup = time.Second },
		"limits":    func(o *Opts) { o.HardIOPS = 400 },
		"attest":    func(o *Opts) { o.Attest = false },
	}
	for name, f := range mut {
		o := ok
		f(&o)
		if len(OfficialProblems(o, true)) == 0 {
			t.Errorf("%s: must be refused", name)
		}
	}
	if len(OfficialProblems(ok, false)) == 0 {
		t.Error("dirty git tree must be refused")
	}
}

func TestClientArgv(t *testing.T) {
	ci := connInfo{Host: "127.0.0.1", Port: "3306", User: "root", Password: "pw"}
	o := Opts{Engine: "mysql", ClientContainer: "cl", ClientBinary: "/opt/sysbench", AppTables: 1, AppRows: 100, Clients: 16}
	a := clientArgv(o, ci, Segment{Dur: 30 * time.Second, Rate: 49.6}, 16, "/out/raw/seg0001", true)
	want := []string{"docker", "exec", "cl", "env", "STRAND_BENCH_RAW_PREFIX=/out/raw/seg0001", "/opt/sysbench"}
	if !reflect.DeepEqual(a[:6], want) {
		t.Fatalf("argv %v", a)
	}
	joined := " " + join(a) + " "
	for _, s := range []string{"--rate=50", "--time=30", "--threads=16", "oltp_read_write run"} {
		if !contains(joined, s) {
			t.Errorf("missing %q in %v", s, a)
		}
	}
	o = Opts{Engine: "postgres", ClientBinary: "pgbench", Jobs: 8, Clients: 4}
	ci.DB = "strand"
	p := clientArgv(o, ci, Segment{Dur: 10 * time.Second, Rate: 123.5}, 4, "/out/raw/s", true)
	pj := join(p)
	for _, s := range []string{"PGPASSWORD=pw", "search_path=bench", "-R 123.5", "-l", "--log-prefix=/out/raw/s", "-j 4", "-T 10", "-b tpcb-like"} {
		if !contains(pj, s) {
			t.Errorf("missing %q in %v", s, p)
		}
	}
}

func join(a []string) string      { return strings.Join(a, " ") }
func contains(s, sub string) bool { return strings.Contains(s, sub) }
