package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

func absPath(p string) (string, error) { return filepath.Abs(p) }

// ---------------------------------------------------------------------------
// calibrate: năng lực bão hoà của client trên fixture đã khôi phục → chốt biên độ sóng TRƯỚC khi đo.
// ---------------------------------------------------------------------------

// Quy tắc biên độ (Amendment 2026-11-13): cao = 50%, thấp = 12,5% (tỉ lệ 4:1 như bản dev 200:50)
// năng lực bão hoà đo được; chốt vào calibration.json và commit trước lượt đo chính thức.
const (
	highFraction = 0.50
	lowFraction  = 0.125
)

type Calibration struct {
	Engine        string  `json:"engine"`
	Clients       int     `json:"clients"`
	Seconds       float64 `json:"measured_seconds"`
	Events        int     `json:"events_in_window"`
	SaturationEPS float64 `json:"saturation_events_per_s"`
	Low           float64 `json:"low"`
	High          float64 `json:"high"`
	Note          string  `json:"note"`
}

// CalibrateFromEvents: eps trên khoảng [first+trim, last-trim] của log thô.
func CalibrateFromEvents(ev []Event, trim time.Duration, clients int, engine string) (Calibration, error) {
	c := Calibration{Engine: engine, Clients: clients}
	if len(ev) == 0 {
		return c, errors.New("no events")
	}
	first, last := ev[0].At, ev[0].At
	for _, e := range ev {
		if e.At.Before(first) {
			first = e.At
		}
		if e.At.After(last) {
			last = e.At
		}
	}
	a, b := first.Add(trim), last.Add(-trim)
	if !a.Before(b) {
		return c, errors.New("measurement too short for trim")
	}
	for _, e := range ev {
		if !e.At.Before(a) && e.At.Before(b) {
			c.Events++
		}
	}
	c.Seconds = b.Sub(a).Seconds()
	c.SaturationEPS = float64(c.Events) / c.Seconds
	c.High = math.Floor(c.SaturationEPS * highFraction)
	c.Low = math.Floor(c.SaturationEPS * lowFraction)
	if c.Low < 1 || c.High <= c.Low {
		return c, errors.New("saturation too low to define a wave")
	}
	c.Note = "high=50%, low=12.5% of saturation; commit this file before the official run"
	return c, nil
}

func calibrateCmd(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var o Opts
	var dur time.Duration
	var out string
	commonFlags(fs, &o)
	fs.DurationVar(&dur, "duration", 2*time.Minute, "thời gian chạy không giới hạn tốc độ")
	fs.StringVar(&out, "out", "calibration.json", "file kết quả")
	fs.Parse(args)
	if o.SourceDSN == "" || o.ClientBinary == "" || o.FixtureDir == "" {
		return errors.New("--source, --client-binary, --fixture-dir are required")
	}
	fx, err := LoadFixture(o.FixtureDir)
	if err != nil {
		return err
	}
	fixDir, _ := filepath.Abs(o.FixtureDir)
	for _, n := range []struct{ c, name string }{{o.SourceContainer, "source"}, {o.TargetContainer, "target"}} {
		if n.c == "" {
			continue
		}
		if _, err := (dockerNode{Container: n.c, Image: o.HelperImage}).Restore(ctx, fixDir, n.name); err != nil {
			return err
		}
	}
	_ = fx
	ci, err := parseConn(o.Engine, o.SourceDSN)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, o.Engine, o.SourceDSN, 3*time.Minute)
	if err != nil {
		return err
	}
	db.Close()
	outAbs, _ := filepath.Abs(out)
	dir := filepath.Join(filepath.Dir(outAbs), "calibrate-raw")
	if err := os.MkdirAll(filepath.Join(dir, "raw"), 0o755); err != nil {
		return err
	}
	l := &loadDriver{o: o, ci: ci, dir: dir}
	sr := l.runSegment(0, Segment{Dur: dur, Rate: 0})
	raw, err := parseSegment(o)(sr)
	if err != nil {
		return fmt.Errorf("calibration run: %w", err)
	}
	c, err := CalibrateFromEvents(raw.Events, 5*time.Second, o.Clients, o.Engine)
	if err != nil {
		return err
	}
	return writeJSON(out, c)
}

// ---------------------------------------------------------------------------
// overhead: pilot so sysbench nguyên bản và bản instrumented (cùng tốc độ, xen kẽ).
// Tiêu chí (khai báo trước, Amendment 2026-11-13): median throughput patched/pristine >= 0,98
// và median p99 patched/pristine <= 1,05.
// ---------------------------------------------------------------------------

const (
	overheadMinThroughputRatio = 0.98
	overheadMaxP99Ratio        = 1.05
)

var (
	reEPS  = regexp.MustCompile(`(?m)^\s*total number of events:\s+([0-9]+)`)
	reTime = regexp.MustCompile(`(?m)^\s*total time:\s+([0-9.]+)s`)
	reP99  = regexp.MustCompile(`(?m)^\s*99th percentile:\s+([0-9.]+)`)
)

type OverheadRound struct {
	Variant string  `json:"variant"`
	EPS     float64 `json:"events_per_s"`
	P99Ms   float64 `json:"p99_ms_reported"`
}

type OverheadReport struct {
	Rounds          []OverheadRound `json:"rounds"`
	ThroughputRatio float64         `json:"median_throughput_ratio_patched_over_pristine"`
	P99Ratio        float64         `json:"median_p99_ratio_patched_over_pristine"`
	Pass            bool            `json:"pass"`
	Criteria        string          `json:"criteria"`
}

func parseSysbenchReport(stdout []byte) (eps, p99 float64, err error) {
	m1, m2, m3 := reEPS.FindSubmatch(stdout), reP99.FindSubmatch(stdout), reTime.FindSubmatch(stdout)
	if m1 == nil || m2 == nil || m3 == nil {
		return 0, 0, errors.New("sysbench report lacks events / total time / 99th percentile")
	}
	n, _ := strconv.ParseFloat(string(m1[1]), 64)
	secs, _ := strconv.ParseFloat(string(m3[1]), 64)
	p99, _ = strconv.ParseFloat(string(m2[1]), 64)
	if secs <= 0 {
		return 0, 0, errors.New("zero total time")
	}
	return n / secs, p99, nil
}

// SummarizeOverhead: tỉ lệ median patched/pristine và kết luận theo tiêu chí đã khai báo.
func SummarizeOverhead(rounds []OverheadRound) (OverheadReport, error) {
	var pe, pp, be, bp []float64
	for _, r := range rounds {
		if !finite(r.EPS) || !finite(r.P99Ms) || r.EPS <= 0 || r.P99Ms <= 0 {
			return OverheadReport{}, errors.New("invalid round")
		}
		if r.Variant == "patched" {
			pe, pp = append(pe, r.EPS), append(pp, r.P99Ms)
		} else {
			be, bp = append(be, r.EPS), append(bp, r.P99Ms)
		}
	}
	if len(pe) == 0 || len(pe) != len(be) {
		return OverheadReport{}, errors.New("need equal, nonzero rounds of both variants")
	}
	rep := OverheadReport{Rounds: rounds,
		ThroughputRatio: medianOf(pe) / medianOf(be), P99Ratio: medianOf(pp) / medianOf(bp),
		Criteria: fmt.Sprintf("throughput ratio >= %.2f and p99 ratio <= %.2f", overheadMinThroughputRatio, overheadMaxP99Ratio)}
	rep.Pass = rep.ThroughputRatio >= overheadMinThroughputRatio && rep.P99Ratio <= overheadMaxP99Ratio
	return rep, nil
}

func overheadCmd(ctx context.Context, fs *flag.FlagSet, args []string) error {
	var o Opts
	var pristine, patched, out string
	var rate float64
	var dur time.Duration
	var rounds int
	commonFlags(fs, &o)
	fs.StringVar(&pristine, "pristine-binary", "", "sysbench nguyên bản")
	fs.StringVar(&patched, "patched-binary", "", "sysbench instrumented")
	fs.StringVar(&out, "out", "overhead.json", "file kết quả (thư mục chứa nó dùng cho log thô; phải mount vào container client)")
	fs.Float64Var(&rate, "rate", 0, "tốc độ cố định (txn/s); 0 = không giới hạn (bão hoà)")
	fs.DurationVar(&dur, "duration", time.Minute, "mỗi vòng")
	fs.IntVar(&rounds, "rounds", 3, "số vòng mỗi biến thể (xen kẽ)")
	fs.Parse(args)
	if o.Engine != "mysql" || pristine == "" || patched == "" || o.SourceDSN == "" {
		return errors.New("overhead pilot is for sysbench: --engine mysql --source --pristine-binary --patched-binary required")
	}
	ci, err := parseConn("mysql", o.SourceDSN)
	if err != nil {
		return err
	}
	outAbs, _ := filepath.Abs(out)
	dir := filepath.Dir(outAbs)
	var rs []OverheadRound
	for i := 0; i < rounds; i++ {
		order := []string{"pristine", "patched"}
		if i%2 == 1 {
			order = []string{"patched", "pristine"}
		}
		for _, v := range order {
			o.ClientBinary = pristine
			raw := false
			prefix := filepath.Join(dir, fmt.Sprintf("overhead-%d-%s", i, v))
			if v == "patched" {
				o.ClientBinary, raw = patched, true
			}
			argv := clientArgv(o, ci, Segment{Dur: dur, Rate: rate}, o.Clients, prefix, raw)
			// clientArgv không có --percentile; thêm trước lệnh con.
			argv = insertBefore(argv, "oltp_read_write", "--percentile=99")
			b, err := exec.CommandContext(ctx, argv[0], argv[1:]...).CombinedOutput()
			if err != nil {
				return fmt.Errorf("%s round %d: %v: %s", v, i, err, tailString(string(b), 400))
			}
			eps, p99, err := parseSysbenchReport(b)
			if err != nil {
				return err
			}
			rs = append(rs, OverheadRound{Variant: v, EPS: eps, P99Ms: p99})
		}
	}
	rep, err := SummarizeOverhead(rs)
	if err != nil {
		return err
	}
	b, _ := json.MarshalIndent(rep, "", "  ")
	fmt.Println(string(b))
	return os.WriteFile(out, b, 0o644)
}

func insertBefore(argv []string, marker, ins string) []string {
	for i, a := range argv {
		if a == marker {
			return append(append(append([]string(nil), argv[:i]...), ins), argv[i:]...)
		}
	}
	return argv
}
