package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Bản ghi sự kiện thô của một lần chạy (run.json) — chỉ chứa sự kiện đã quan sát.
// Mọi số liệu suy ra nằm ở Analyze, tính lại được từ thư mục lần chạy.
// ---------------------------------------------------------------------------

type SegmentRun struct {
	Index   int       `json:"index"`
	Plan    Segment   `json:"plan"`
	Threads int       `json:"threads"`
	Prefix  string    `json:"raw_prefix"`
	Stdout  string    `json:"stdout"`
	Stderr  string    `json:"stderr"`
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Exit    int       `json:"exit_code"`
	Err     string    `json:"launch_error,omitempty"`
}

type RestoreEvidence struct {
	SourceTreeWant string `json:"source_tree_sha256_want"`
	SourceTreeGot  string `json:"source_tree_sha256_got"`
	TargetTreeWant string `json:"target_tree_sha256_want"`
	TargetTreeGot  string `json:"target_tree_sha256_got"`
	ManifestWant   string `json:"manifest_sha256_want"`
	ManifestGot    string `json:"manifest_sha256_got"`
	Detail         string `json:"detail,omitempty"`
}

// Verify: bằng chứng khôi phục hợp lệ khi cả ba hash đều có và khớp mốc fixture.
func (e RestoreEvidence) Verify() error {
	pairs := []struct{ n, w, g string }{
		{"source datadir tree", e.SourceTreeWant, e.SourceTreeGot},
		{"target datadir tree", e.TargetTreeWant, e.TargetTreeGot},
		{"source logical manifest", e.ManifestWant, e.ManifestGot},
	}
	for _, p := range pairs {
		if p.w == "" || p.g == "" {
			return fmt.Errorf("%s hash missing", p.n)
		}
		if p.w != p.g {
			return fmt.Errorf("%s differs from fixture (%s != %s)", p.n, short(p.g), short(p.w))
		}
	}
	return nil
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

type RunRecord struct {
	Name              string          `json:"name"`
	Governor          string          `json:"governor"`
	Repeat            int             `json:"repeat"`
	Attempt           int             `json:"attempt"`
	Engine            string          `json:"engine"`
	Wave              Wave            `json:"wave"`
	SoftIOPS          int             `json:"soft_iops"`
	HardIOPS          int             `json:"hard_iops"`
	Clients           int             `json:"clients"`
	LoadT0            time.Time       `json:"load_t0"`
	StrandStart       time.Time       `json:"strand_start"`
	BootstrapFinished time.Time       `json:"bootstrap_finished"`
	LoadStopped       time.Time       `json:"load_stopped"`
	Segments          []SegmentRun    `json:"segments"`
	StrandExit        int             `json:"strand_exit_code"`
	StrandExitSet     bool            `json:"strand_exit_observed"`
	Restore           RestoreEvidence `json:"restore"`
	CompareOK         bool            `json:"compare_ok"`
	CompareDetail     string          `json:"compare_detail"`
	CaughtUp          bool            `json:"caught_up"`
	Error             string          `json:"error,omitempty"`
	UptimeBefore      string          `json:"uptime_before"`
	UptimeAfter       string          `json:"uptime_after"`
}

// ---------------------------------------------------------------------------
// Telemetry
// ---------------------------------------------------------------------------

type Sample struct {
	T                        time.Time
	R, Zone, P, Breaker, Lag float64
	OK                       bool
}

func WriteTelemetry(path string, s []Sample) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "t_unix_ns,ok,R,zone,pressure,breaker,lag_s")
	for _, x := range s {
		ok := 0
		if x.OK {
			ok = 1
		}
		fmt.Fprintf(w, "%d,%d,%g,%g,%g,%g,%g\n", x.T.UnixNano(), ok, x.R, x.Zone, x.P, x.Breaker, x.Lag)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func ReadTelemetry(path string) ([]Sample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var out []Sample
	first := true
	for sc.Scan() {
		if first {
			first = false
			continue
		}
		p := strings.Split(sc.Text(), ",")
		if len(p) != 7 {
			return nil, fmt.Errorf("telemetry line %q: need 7 fields", sc.Text())
		}
		ns, err := strconv.ParseInt(p[0], 10, 64)
		if err != nil {
			return nil, err
		}
		s := Sample{T: time.Unix(0, ns).UTC(), OK: p[1] == "1"}
		for i, dst := range []*float64{&s.R, &s.Zone, &s.P, &s.Breaker, &s.Lag} {
			if *dst, err = strconv.ParseFloat(p[2+i], 64); err != nil {
				return nil, err
			}
		}
		out = append(out, s)
	}
	return out, sc.Err()
}

// ---------------------------------------------------------------------------
// Kết quả một lần chạy
// ---------------------------------------------------------------------------

// jf là float64 ghi JSON được: NaN/±Inf thành null.
type jf float64

func (f jf) MarshalJSON() ([]byte, error) {
	if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
		return []byte("null"), nil
	}
	return json.Marshal(float64(f))
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

type RunResult struct {
	Name            string `json:"name"`
	Governor        string `json:"governor"`
	Repeat          int    `json:"repeat"`
	Attempt         int    `json:"attempt"`
	Dir             string `json:"dir"`
	SnapshotSeconds jf     `json:"snapshot_seconds"`
	// p99 ứng dụng tính phía client từ log thô, trên [strand_start, bootstrap_finished).
	AppP99Ms        jf        `json:"app_p99_ms_during_snapshot"`
	AppTxns         int       `json:"app_txns_during_snapshot"`
	RawEventsTotal  uint64    `json:"raw_events_total"`
	Segments        int       `json:"client_segments"`
	LoadCoverage    jf        `json:"load_coverage_of_snapshot"`
	HardLimitExceed int       `json:"hard_limit_exceed_samples"`
	HardMaxR        jf        `json:"max_r_in_snapshot"`
	P2PLogRMedian   jf        `json:"p2p_log_r_median"`
	P2PLogRMax      jf        `json:"p2p_log_r_max"`
	P2PWindows      int       `json:"p2p_windows"`
	P2PZeroWindows  int       `json:"p2p_windows_with_r_zero"`
	Samples         int       `json:"samples"`
	ValidSamples    int       `json:"valid_samples_during_snapshot"`
	InvalidSamples  int       `json:"invalid_samples_during_snapshot"`
	StartupInvalid  int       `json:"startup_invalid_samples"`
	MaxLagSeconds   jf        `json:"max_lag_seconds_during_snapshot"`
	StrandExit      int       `json:"strand_exit_code"`
	RestoreOK       bool      `json:"restore_verified"`
	RestoreDetail   string    `json:"restore_detail,omitempty"`
	CompareOK       bool      `json:"compare_ok"`
	CompareDetail   string    `json:"compare_detail"`
	TimeseriesCSV   string    `json:"timeseries_csv"`
	Evidence        string    `json:"evidence_error,omitempty"` // rỗng = bằng chứng đủ
	OK              bool      `json:"ok"`
	Error           string    `json:"error,omitempty"`
	FixtureManifest string    `json:"fixture_manifest_sha256"`
	Uptime          [2]string `json:"loadavg_before_after"`
}

// Thông số kiểm bằng chứng — cố định trước khi đo (xem Amendment 2026-11-13).
const (
	minLoadCoverage = 0.97 // phần cửa sổ snapshot có ít nhất một tiến trình client đang chạy
	maxP2POfficial  = 0.3
	// Vài giây đầu sau khi chạy `strand sync` endpoint metrics chưa sẵn sàng: mẫu lỗi liên tiếp ở đầu
	// cửa sổ, trước mẫu hợp lệ đầu tiên, được chấp nhận tới ngưỡng này; mọi mẫu lỗi về sau là incomplete.
	maxStartupInvalid = 5
)

type TimeRow struct {
	T      time.Time
	Sample Sample
	Rate   float64
	P99ms  float64
	Events int
}

// Analyze suy ra mọi số liệu của một lần chạy từ bản ghi sự kiện, telemetry thô và log client thô.
func Analyze(rec RunRecord, samples []Sample, parse func(SegmentRun) (SegmentRaw, error)) (RunResult, []TimeRow) {
	res := RunResult{Name: rec.Name, Governor: rec.Governor, Repeat: rec.Repeat, Attempt: rec.Attempt,
		StrandExit: rec.StrandExit, CompareOK: rec.CompareOK, CompareDetail: rec.CompareDetail,
		Error: rec.Error, Segments: len(rec.Segments), Samples: len(samples),
		AppP99Ms: jf(math.NaN()), P2PLogRMedian: jf(math.NaN()), P2PLogRMax: jf(math.NaN()),
		SnapshotSeconds: jf(math.NaN()), Uptime: [2]string{rec.UptimeBefore, rec.UptimeAfter},
		FixtureManifest: rec.Restore.ManifestWant}
	if err := rec.Restore.Verify(); err != nil {
		res.RestoreDetail = err.Error()
	} else {
		res.RestoreOK = true
	}
	if !rec.StrandStart.IsZero() && !rec.BootstrapFinished.IsZero() {
		res.SnapshotSeconds = jf(rec.BootstrapFinished.Sub(rec.StrandStart).Seconds())
	}
	// Log client thô: mọi đoạn phải đọc được, nếu không thì không có p99.
	var events []Event
	var parseErr error
	for _, sg := range rec.Segments {
		if sg.Err != "" || sg.Exit != 0 {
			parseErr = fmt.Errorf("segment %d failed (exit %d %s)", sg.Index, sg.Exit, sg.Err)
			break
		}
		raw, err := parse(sg)
		if err != nil {
			parseErr = fmt.Errorf("segment %d: %w", sg.Index, err)
			break
		}
		res.RawEventsTotal += uint64(len(raw.Events))
		events = append(events, raw.Events...)
	}
	if len(rec.Segments) == 0 && parseErr == nil {
		parseErr = fmt.Errorf("no client segments ran")
	}
	sort.Slice(events, func(i, j int) bool { return events[i].At.Before(events[j].At) })
	haveWindow := !rec.StrandStart.IsZero() && !rec.BootstrapFinished.IsZero() && rec.StrandStart.Before(rec.BootstrapFinished)
	if parseErr != nil {
		res.Evidence = "client raw log: " + parseErr.Error()
	} else if haveWindow {
		lat := WindowLatencies(events, rec.StrandStart, rec.BootstrapFinished)
		res.AppTxns = len(lat)
		if p, err := P99NearestRank(lat); err == nil {
			res.AppP99Ms = jf(float64(p) / 1e6)
		}
		res.LoadCoverage = jf(coverage(rec.Segments, rec.StrandStart, rec.BootstrapFinished))
	}
	// Telemetry trong snapshot.
	for _, s := range samples {
		if !haveWindow || s.T.Before(rec.StrandStart) || s.T.After(rec.BootstrapFinished) {
			continue
		}
		if s.OK && finite(s.R) && s.R >= 0 {
			res.ValidSamples++
			if s.R > float64(res.HardMaxR) {
				res.HardMaxR = jf(s.R)
			}
		} else if res.ValidSamples == 0 {
			res.StartupInvalid++ // Strand chưa mở metrics: chỉ chấp nhận ở đầu cửa sổ (tối đa maxStartupInvalid)
		} else {
			res.InvalidSamples++
		}
		if s.OK && s.R > float64(rec.HardIOPS)+1e-9 {
			res.HardLimitExceed++
		}
		if s.OK && finite(s.Lag) && s.Lag > float64(res.MaxLagSeconds) {
			res.MaxLagSeconds = jf(s.Lag)
		}
	}
	if haveWindow {
		med, mx, nw, zw := p2pLogR(samples, rec.LoadT0, rec.StrandStart, rec.BootstrapFinished, rec.Wave)
		res.P2PLogRMedian, res.P2PLogRMax, res.P2PWindows, res.P2PZeroWindows = jf(med), jf(mx), nw, zw
	}
	var rows []TimeRow
	if parseErr == nil {
		rows = buildRows(rec, samples, events)
	}
	if res.Evidence == "" {
		res.Evidence = evidenceError(res)
	}
	res.OK = res.Evidence == "" && res.Error == "" && rec.StrandExitSet && res.StrandExit == 0 &&
		res.CompareOK && rec.CaughtUp
	if res.Evidence == "" && !res.OK && res.Error == "" {
		res.Error = "run failed: exit/compare/catch-up"
	}
	return res, rows
}

// coverage: phần [a,b) có ít nhất một tiến trình client đang chạy.
func coverage(segs []SegmentRun, a, b time.Time) float64 {
	type iv struct{ s, e time.Time }
	var ivs []iv
	for _, s := range segs {
		st, en := s.Start, s.End
		if st.Before(a) {
			st = a
		}
		if en.After(b) {
			en = b
		}
		if st.Before(en) {
			ivs = append(ivs, iv{st, en})
		}
	}
	sort.Slice(ivs, func(i, j int) bool { return ivs[i].s.Before(ivs[j].s) })
	var covered time.Duration
	var cur *iv
	for i := range ivs {
		if cur == nil || ivs[i].s.After(cur.e) {
			if cur != nil {
				covered += cur.e.Sub(cur.s)
			}
			c := ivs[i]
			cur = &c
		} else if ivs[i].e.After(cur.e) {
			cur.e = ivs[i].e
		}
	}
	if cur != nil {
		covered += cur.e.Sub(cur.s)
	}
	return float64(covered) / float64(b.Sub(a))
}

// Thiếu phép đo = incomplete, không phải bằng chứng "0 vượt hard limit" hay "p2p = 0".
func evidenceError(r RunResult) string {
	switch {
	case !r.RestoreOK:
		return "restore not verified: " + r.RestoreDetail
	case r.AppTxns <= 0 || !finite(float64(r.AppP99Ms)) || r.AppP99Ms <= 0:
		return "thiếu p99 phía client"
	case !finite(float64(r.SnapshotSeconds)) || r.SnapshotSeconds <= 0:
		return "thiếu thời gian snapshot"
	case float64(r.LoadCoverage) < minLoadCoverage:
		return fmt.Sprintf("tải client chỉ phủ %.1f%% cửa sổ snapshot", float64(r.LoadCoverage)*100)
	case r.InvalidSamples > 0 || r.StartupInvalid > maxStartupInvalid || r.ValidSamples <= 0 ||
		float64(r.ValidSamples) < float64(r.SnapshotSeconds)-float64(r.StartupInvalid)-2:
		return "telemetry thiếu hoặc scrape lỗi trong snapshot"
	case r.P2PWindows <= 0 || !finite(float64(r.P2PLogRMedian)) || !finite(float64(r.P2PLogRMax)):
		return "không có cửa sổ ổn định đo p2p hợp lệ"
	}
	return ""
}

// p2pLogR: với mỗi nửa chu kỳ tải không đổi nằm trọn trong [snapStart, snapEnd], bỏ 60 s đầu và khoảng spike,
// tính max(ln R) − min(ln R); trả trung vị, max, số cửa sổ dùng được, số cửa sổ có R = 0 (bị loại, đếm riêng).
func p2pLogR(samples []Sample, t0, snapStart, snapEnd time.Time, w Wave) (median, maxV float64, n, zero int) {
	half := w.Period / 2
	if half <= 0 {
		return math.NaN(), math.NaN(), 0, 0
	}
	var vals []float64
	for k := 0; ; k++ {
		ws := t0.Add(time.Duration(k) * half)
		we := ws.Add(half)
		if ws.After(snapEnd) {
			break
		}
		if ws.Before(snapStart) || we.After(snapEnd) {
			continue
		}
		from := ws.Add(60 * time.Second)
		lo, hi := math.Inf(1), math.Inf(-1)
		hasZero, cnt := false, 0
		for _, s := range samples {
			if s.T.Before(from) || !s.T.Before(we) || !s.OK {
				continue
			}
			if d := s.T.Sub(t0); w.SpikeEvery > 0 && d >= w.SpikeEvery && d%w.SpikeEvery < w.SpikeDur+5*time.Second {
				continue
			}
			if s.R <= 0 {
				hasZero = true
				continue
			}
			v := math.Log(s.R)
			lo, hi = math.Min(lo, v), math.Max(hi, v)
			cnt++
		}
		if hasZero {
			zero++
			continue
		}
		if cnt >= 10 {
			vals = append(vals, hi-lo)
		}
	}
	if len(vals) == 0 {
		return math.NaN(), math.NaN(), 0, zero
	}
	sort.Float64s(vals)
	return medianOf(vals), vals[len(vals)-1], len(vals), zero
}

func medianOf(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// buildRows: bảng thời gian theo mẫu telemetry; p99 cửa sổ 10 s tính từ log client thô.
func buildRows(rec RunRecord, samples []Sample, events []Event) []TimeRow {
	rows := make([]TimeRow, 0, len(samples))
	for _, s := range samples {
		lat := windowLatSorted(events, s.T.Add(-10*time.Second), s.T)
		p, err := P99NearestRank(lat)
		p99 := math.NaN()
		if err == nil {
			p99 = float64(p) / 1e6
		}
		rows = append(rows, TimeRow{T: s.T, Sample: s, Rate: rec.Wave.RateAt(s.T.Sub(rec.LoadT0)), P99ms: p99, Events: len(lat)})
	}
	return rows
}

func WriteTimeseriesCSV(path string, rec RunRecord, rows []TimeRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "t_s,phase,R,lnR,zone,pressure,breaker,lag_s,ok,target_rate,p99_10s_ms,events_10s")
	for _, r := range rows {
		phase := "pre"
		switch {
		case !r.T.Before(rec.BootstrapFinished) && !rec.BootstrapFinished.IsZero():
			phase = "stream"
		case !r.T.Before(rec.StrandStart):
			phase = "snapshot"
		}
		lnR := math.NaN()
		if r.Sample.R > 0 {
			lnR = math.Log(r.Sample.R)
		}
		ok := 0
		if r.Sample.OK {
			ok = 1
		}
		fmt.Fprintf(w, "%.0f,%s,%.3f,%.4f,%.0f,%.4f,%.0f,%.3f,%d,%.0f,%.3f,%d\n", r.T.Sub(rec.StrandStart).Seconds(), phase,
			r.Sample.R, lnR, r.Sample.Zone, r.Sample.P, r.Sample.Breaker, r.Sample.Lag, ok, r.Rate, r.P99ms, r.Events)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// windowLatSorted như WindowLatencies nhưng events đã sắp theo thời gian: tìm nhị phân thay vì quét hết.
func windowLatSorted(ev []Event, start, end time.Time) []int64 {
	i := sort.Search(len(ev), func(i int) bool { return !ev[i].At.Before(start) })
	var out []int64
	for ; i < len(ev) && ev[i].At.Before(end); i++ {
		out = append(out, ev[i].LatNS)
	}
	return out
}
