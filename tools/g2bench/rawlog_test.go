package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, path, s string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
}

// sbFixture: 2 worker x n event, latency = 1 ms * (thread*n + i), cách nhau 1 ms từ base.
func sbFixture(t *testing.T, n int, base time.Time) (prefix, out, errp string) {
	dir := t.TempDir()
	prefix = filepath.Join(dir, "seg")
	for th := 0; th < 2; th++ {
		var b strings.Builder
		for i := 1; i <= n; i++ {
			fmt.Fprintf(&b, "%d %d %d %d\n", th, i, base.Add(time.Duration(i)*time.Millisecond).UnixNano(), (th*n+i)*1_000_000)
		}
		fmt.Fprintf(&b, "# STRAND_RAW_THREAD thread_id=%d count=%d errors=0\n", th, n)
		writeFile(t, fmt.Sprintf("%s.%d", prefix, th), b.String())
	}
	out, errp = prefix+".stdout", prefix+".stderr"
	writeFile(t, out, fmt.Sprintf("General statistics:\n    total number of events:              %d\n  ignored errors:                      0      (0.00 per sec.)\n    reconnects:                          0      (0.00 per sec.)\n", 2*n))
	writeFile(t, errp, fmt.Sprintf("STRAND_RAW_EVENTS count=%d errors=0 run_status=0\n", 2*n))
	return
}

func TestParseSysbenchSegmentOKAndP99(t *testing.T) {
	base := time.Unix(1_800_000_000, 0).UTC()
	prefix, out, errp := sbFixture(t, 50, base)
	raw, err := ParseSysbenchSegment(prefix, 2, out, errp, 0)
	if err != nil || len(raw.Events) != 100 || raw.Reported != 100 {
		t.Fatalf("raw=%+v err=%v", raw, err)
	}
	lat := WindowLatencies(raw.Events, base, base.Add(time.Second))
	p, err := P99NearestRank(lat)
	if err != nil || p != 99_000_000 { // nearest rank: ceil(.99*100)=99 -> 99 ms
		t.Fatalf("p99 %d err %v", p, err)
	}
}

func TestParseSysbenchSegmentFailClosed(t *testing.T) {
	base := time.Unix(1_800_000_000, 0).UTC()
	cases := map[string]func(prefix, out, errp string){
		"missing footer": func(p, o, e string) {
			b, _ := os.ReadFile(p + ".1")
			writeFile(t, p+".1", string(b[:strings.LastIndex(string(b), "# STRAND")]))
		},
		"marker count mismatch": func(p, o, e string) {
			writeFile(t, e, "STRAND_RAW_EVENTS count=99 errors=0 run_status=0\n")
		},
		"marker failing": func(p, o, e string) {
			writeFile(t, e, "STRAND_RAW_EVENTS count=100 errors=1 run_status=0\n")
		},
		"no marker": func(p, o, e string) { writeFile(t, e, "") },
		"summary mismatch": func(p, o, e string) {
			writeFile(t, o, "total number of events: 101\nignored errors: 0 (0)\nreconnects: 0 (0)\n")
		},
		"ignored errors": func(p, o, e string) {
			writeFile(t, o, "total number of events: 100\nignored errors: 3 (0.1 per sec.)\nreconnects: 0 (0)\n")
		},
		"reconnects": func(p, o, e string) {
			writeFile(t, o, "total number of events: 100\nignored errors: 0 (0)\nreconnects: 2 (0.1 per sec.)\n")
		},
		"missing worker file": func(p, o, e string) { os.Remove(p + ".1") },
		"clock backwards": func(p, o, e string) {
			b, _ := os.ReadFile(p + ".0")
			lines := strings.Split(string(b), "\n")
			lines[5] = fmt.Sprintf("0 6 %d 1000", base.UnixNano()) // thấp hơn dòng trước
			writeFile(t, p+".0", strings.Join(lines, "\n"))
		},
		"sequence gap": func(p, o, e string) {
			b, _ := os.ReadFile(p + ".0")
			writeFile(t, p+".0", strings.Replace(string(b), "\n0 3 ", "\n0 4 ", 1))
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			prefix, out, errp := sbFixture(t, 50, base)
			mut(prefix, out, errp)
			if _, err := ParseSysbenchSegment(prefix, 2, out, errp, 0); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	prefix, out, errp := sbFixture(t, 50, base)
	if _, err := ParseSysbenchSegment(prefix, 2, out, errp, 1); err == nil {
		t.Fatal("non-zero exit must fail")
	}
	if _, err := ParseSysbenchSegment(prefix, 3, out, errp, 0); err == nil {
		t.Fatal("wrong thread count must fail")
	}
}

func pgFixture(t *testing.T, lines []string, processed int) (prefix, out string) {
	dir := t.TempDir()
	prefix = filepath.Join(dir, "pgb")
	writeFile(t, prefix+".4242", strings.Join(lines[:len(lines)/2], "\n")+"\n")
	writeFile(t, prefix+".4242.1", strings.Join(lines[len(lines)/2:], "\n")+"\n")
	out = prefix + ".stdout"
	writeFile(t, out, fmt.Sprintf("number of transactions actually processed: %d\n", processed))
	return
}

func TestParsePgbenchSegment(t *testing.T) {
	var lines []string
	base := int64(1_800_000_000)
	for i := 1; i <= 100; i++ {
		// client tx latency_us script epoch us [schedule_lag]
		lines = append(lines, fmt.Sprintf("%d %d %d 0 %d %d 12", i%4, i, i*1000, base, i*1000))
	}
	prefix, out := pgFixture(t, lines, 100)
	raw, err := ParsePgbenchSegment(prefix, out, 0)
	if err != nil || len(raw.Events) != 100 || len(raw.RawFiles) != 2 {
		t.Fatalf("raw=%+v err=%v", raw, err)
	}
	start := time.Unix(base, 0).UTC()
	// nửa mở: event i=100 hoàn tất tại +100000us = +100 ms; cửa sổ [start, start+100ms) loại nó.
	lat := WindowLatencies(raw.Events, start, start.Add(100*time.Millisecond))
	if len(lat) != 99 {
		t.Fatalf("half-open window has %d events", len(lat))
	}
	if p, _ := P99NearestRank(lat); p != 99_000_000 {
		t.Fatalf("p99 = %d ns", p)
	}
	// failed / processed mismatch / exit code / bad line
	bad := append([]string(nil), lines...)
	bad[10] = fmt.Sprintf("1 11 failed 0 %d 11000", base)
	p2, o2 := pgFixture(t, bad, 100)
	if _, err := ParsePgbenchSegment(p2, o2, 0); err == nil {
		t.Fatal("failed record must make the segment incomplete")
	}
	p3, o3 := pgFixture(t, lines, 101)
	if _, err := ParsePgbenchSegment(p3, o3, 0); err == nil {
		t.Fatal("processed-count mismatch must fail")
	}
	if _, err := ParsePgbenchSegment(prefix, out, 3); err == nil {
		t.Fatal("exit code must fail")
	}
	p4, o4 := pgFixture(t, append([]string{"garbage"}, lines...), 101)
	if _, err := ParsePgbenchSegment(p4, o4, 0); err == nil {
		t.Fatal("malformed line must fail")
	}
}

func TestPercentileNearestRank(t *testing.T) {
	if _, err := P99NearestRank(nil); err == nil {
		t.Fatal("empty must be an error, not zero")
	}
	lat := make([]int64, 0, 1000)
	for i := 1000; i >= 1; i-- { // không sắp xếp sẵn
		lat = append(lat, int64(i))
	}
	if p, _ := P99NearestRank(lat); p != 990 {
		t.Fatalf("p99=%d", p)
	}
	if p, _ := P99NearestRank([]int64{5}); p != 5 {
		t.Fatal("single value")
	}
	if p, _ := PercentileNearestRank([]int64{1, 2, 3, 4}, 0.5); p != 2 {
		t.Fatalf("p50 of 4 = %d, nearest rank is 2", p)
	}
}
