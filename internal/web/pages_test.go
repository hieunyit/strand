package web

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

func TestRingBuffer(t *testing.T) {
	r := NewRing[int](3)
	if len(r.Snapshot()) != 0 || r.Len() != 0 {
		t.Fatal("empty ring")
	}
	r.Add(1)
	r.Add(2)
	if got := fmt.Sprint(r.Snapshot()); got != "[1 2]" || r.Len() != 2 {
		t.Fatalf("partial: %s", got)
	}
	for i := 3; i <= 7; i++ {
		r.Add(i)
	}
	if got := fmt.Sprint(r.Snapshot()); got != "[5 6 7]" || r.Len() != 3 {
		t.Fatalf("wrapped: %s", got)
	}
	// 1 giờ ở nhịp 5 giây = 720 điểm.
	if RingSize != 720 || DefaultSampleInterval != 5*time.Second {
		t.Fatalf("ring size %d interval %v", RingSize, DefaultSampleInterval)
	}
}

func TestSamplerSamplesAndBuildsOverview(t *testing.T) {
	e := newEnv(t, nil)
	s := e.srv.Sampler()
	s.SampleOnce(context.Background())
	snap := s.Latest("orders")
	if snap == nil || !snap.Reachable || snap.State != StateSynced || snap.Top.Throughput != 1234 {
		t.Fatalf("latest %+v", snap)
	}
	e.clock.Advance(5 * time.Second)
	s.SampleOnce(context.Background())
	e.clock.Advance(5 * time.Second)
	s.SampleOnce(context.Background())
	ser := s.Series("orders")
	if len(ser) != 3 || ser[0].Lag != 0.25 || ser[2].T.Sub(ser[0].T) != 10*time.Second {
		t.Fatalf("series %+v", ser)
	}
	ov := s.BuildOverview()
	// Rows/s là tốc độ đã làm mượt: bộ đếm applied_rows của mock đứng yên (5555) nên sau 3 mẫu tốc độ là 0,
	// dù `top` báo tức thời 1234 (mẫu đầu tiên chưa có cửa sổ thì lấy giá trị đó làm gốc).
	if ov.Total != 1 || ov.Counts[StateSynced] != 1 || ov.MaxLagSeconds != 0.25 || ov.MaxLagPipeline != "orders" || ov.RowsPerSec != s.Latest("orders").Rates.Rows || ov.RowsPerSec != 0 {
		t.Fatalf("overview %+v", ov)
	}
	// Pipeline bị gỡ khỏi cấu hình thì dữ liệu cũng bị dọn (không rò bộ nhớ).
	if err := e.srv.reg.RemovePipeline("orders"); err != nil {
		t.Fatal(err)
	}
	e.srv.reg.Refresh(context.Background())
	s.SampleOnce(context.Background())
	if s.Latest("orders") != nil || len(s.Series("orders")) != 0 {
		t.Fatal("removed pipeline must be dropped from the sampler")
	}
}

func TestSamplerApplyRateFromMetrics(t *testing.T) {
	e := newEnv(t, nil)
	rows := 1000.0
	ms := newMetricsServer(t, func() string {
		return fmt.Sprintf("# HELP x\nstrand_apply_rows_total %g\nstrand_holes 3\nstrand_writeset_entries{k=\"a\"} 4\nstrand_writeset_entries{k=\"b\"} 6\nstrand_spool_bytes 2048\nstrand_checkpoint_compaction_lag_seconds 1.5\nother_metric 9\n", rows)
	})
	// Ghi lại cấu hình pipeline có metrics.
	cfgPath := e.srv.reg.cfg.Pipelines[0].Config
	b, _ := os.ReadFile(cfgPath)
	e.writePipelineFile("orders", cfgPath, string(b)+"  metrics: \""+ms+"\"\n", 0o640)
	s := e.srv.Sampler()
	s.SampleOnce(context.Background())
	e.clock.Advance(5 * time.Second)
	rows += 500
	s.SampleOnce(context.Background())
	ser := s.Series("orders")
	if len(ser) != 2 {
		t.Fatalf("series %+v", ser)
	}
	last := ser[1]
	if last.ApplyRate != 100 || last.Holes != 3 || last.Writeset != 10 || last.SpoolBytes != 2048 {
		t.Fatalf("sample %+v", last)
	}
	if ser[0].ApplyRate != 0 {
		t.Fatalf("first sample has no rate: %+v", ser[0])
	}
}

func TestParseProm(t *testing.T) {
	m, err := ParseProm(strings.NewReader("# TYPE a gauge\nstrand_a 1.5\nstrand_b{x=\"1\"} 2\nstrand_b{x=\"2\"} 3 1700000000\nnot_strand 5\nstrand_bad abc\nstrand_c{le=\"+Inf\"} 7\n"))
	if err != nil || m["strand_a"] != 1.5 || m["strand_b"] != 5 || m["strand_c"] != 7 || len(m) != 3 {
		t.Fatalf("%v %v", m, err)
	}
}

func TestPagesRenderLiveData(t *testing.T) {
	e := newEnv(t, nil)
	for i := 0; i < 3; i++ {
		e.sampleNow()
		e.clock.Advance(5 * time.Second)
	}
	c := e.client()
	e.login(c, testPassword)
	get := func(p string) string {
		resp, body := e.do(c, "GET", p, "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("GET %s: %d", p, resp.StatusCode)
		}
		return body
	}
	ov := get("/")
	// Rows/s is the 30 s average of the applied-rows counter (flat in the mock, so 0); lag has stable units.
	for _, want := range []string{"orders", "synced", "mysql → postgres", `data-bind="rows">0<`, "0.25 s"} {
		if !strings.Contains(ov, want) {
			t.Errorf("overview lacks %q", want)
		}
	}
	g := get("/pipelines/orders")
	for _, want := range []string{"Source", "Replication", "Applying changes", "Target", "Copy position", "grace-v2", "uuid-1234:1-50", "<polyline", "Lag"} {
		if !strings.Contains(g, want) {
			t.Errorf("graph tab lacks %q", want)
		}
	}
	tb := get("/pipelines/orders/tables")
	for _, want := range []string{"app.users", "app.orders", "app.codes", "Done 2, Pending 6", "250,000 / 1,000,000", "25.0%", "generic key", `<span data-bind="tables_done">1</span> done, <span data-bind="tables_running">1</span> running, <span data-bind="tables_pending">1</span> pending`} {
		if !strings.Contains(tb, want) {
			t.Errorf("tables tab lacks %q:\n%s", want, tb)
		}
	}
	cp := get("/pipelines/orders/checkpoint")
	for _, want := range []string{"uuid-1234:1-99", "Epoch", "4"} {
		if !strings.Contains(cp, want) {
			t.Errorf("checkpoint tab lacks %q", want)
		}
	}
	er := get("/pipelines/orders/errors")
	for _, want := range []string{"snapshot slow", "table=app.orders", "ERROR", "dial mysql://u:********@h/db failed"} {
		if !strings.Contains(er, want) {
			t.Errorf("errors tab lacks %q", want)
		}
	}
	cf := get("/pipelines/orders/config")
	if !strings.Contains(cf, "tables.mask") || !strings.Contains(cf, "********") {
		t.Errorf("config tab: %s", cf)
	}
	// CSP: không có style hoặc script nội tuyến trong trang.
	for _, p := range []string{"/", "/pipelines/orders", "/pipelines/orders/tables"} {
		body := get(p)
		if strings.Contains(body, " style=") || strings.Contains(body, "<style") || strings.Contains(body, "onclick=") {
			t.Errorf("%s uses inline style/script, which the CSP blocks", p)
		}
		if strings.Contains(body, "http://") && !strings.Contains(body, "http://www.w3.org") {
			t.Errorf("%s references an external URL", p)
		}
	}
	// Trang trả đủ CSRF cho form logout.
	if !strings.Contains(ov, `name="csrf"`) {
		t.Error("logout form lacks csrf")
	}
}

func TestAPIJSONShapes(t *testing.T) {
	e := newEnv(t, nil)
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	var ov struct {
		Counts    map[string]int
		Total     int
		Pipelines []PipelineView
	}
	_, body := e.do(c, "GET", "/api/v1/overview", "", nil)
	if err := json.Unmarshal([]byte(body), &ov); err != nil || ov.Total != 1 || ov.Counts["synced"] != 1 || ov.Pipelines[0].Name != "orders" || !ov.Pipelines[0].Reachable {
		t.Fatalf("overview: %v %s", err, body)
	}
	var tv TablesView
	_, body = e.do(c, "GET", "/api/v1/pipelines/orders/tables", "", nil)
	if err := json.Unmarshal([]byte(body), &tv); err != nil || len(tv.Tables) != 3 || tv.Done != 1 || tv.Running != 1 || tv.Pending != 1 {
		t.Fatalf("tables: %v %s", err, body)
	}
	var sv StatusView
	_, body = e.do(c, "GET", "/api/v1/pipelines/orders/status", "", nil)
	if err := json.Unmarshal([]byte(body), &sv); err != nil || sv.Status == nil || sv.Status.Epoch != 4 {
		t.Fatalf("status: %v %s", err, body)
	}
	var ev ErrorsView
	_, body = e.do(c, "GET", "/api/v1/pipelines/orders/errors", "", nil)
	if err := json.Unmarshal([]byte(body), &ev); err != nil || len(ev.Live) != 2 || ev.Live[0].Level != "WARN" {
		t.Fatalf("errors: %v %s", err, body)
	}
	var ser SeriesView
	_, body = e.do(c, "GET", "/api/v1/pipelines/orders/series", "", nil)
	if err := json.Unmarshal([]byte(body), &ser); err != nil || len(ser.Samples) != 1 || ser.IntervalSeconds != 5 {
		t.Fatalf("series: %v %s", err, body)
	}
}

func TestStoppedPipelineSystemdShowsExitCode(t *testing.T) {
	e := newEnv(t, func(e *testEnv, c *Config) {
		// Socket không tồn tại: pipeline dừng. systemd báo failed, exit 5.
		dir := t.TempDir()
		p := filepath.Join(dir, "dead.yaml")
		if err := os.WriteFile(p, []byte("name: dead\nsource: {kind: postgres}\ntarget: {kind: postgres}\nobservability: {admin_socket: "+dir+"/none.sock}\n"), 0o640); err != nil {
			t.Fatal(err)
		}
		c.Pipelines = append(c.Pipelines, PipelineEntry{Name: "dead", Config: p, SystemdUnit: "strand@dead"})
		e.cmd.setOut("systemctl show strand@dead.service", "ActiveState=failed\nSubState=failed\nExecMainStatus=5\nExecMainPID=0\n")
		e.cmd.setOut("journalctl -u strand@dead.service", `{"time":"2026-11-15T10:00:00Z","level":"ERROR","msg":"[log_gap] source purged the log: dsn postgres://u:Zz9@h/d"}`+"\n"+`{"time":"2026-11-15T10:00:01Z","level":"INFO","msg":"bye"}`+"\n")
	})
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	if st := e.srv.Sampler().Latest("dead").State; st != StateFailed {
		t.Fatalf("state %s, want failed", st)
	}
	_, body := e.do(c, "GET", "/pipelines/dead/errors", "", nil)
	for _, want := range []string{"Last exit code 5", "log_gap", "systemd", "needs an operator", "pipeline is not running"} {
		if !strings.Contains(body, want) {
			t.Errorf("errors page lacks %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "Zz9") {
		t.Error("log secret leaked")
	}
	_, body = e.do(c, "GET", "/pipelines/dead/logs", "", nil)
	if !strings.Contains(body, "journalctl -u strand@dead.service") || strings.Contains(body, "Zz9") {
		t.Errorf("logs page: %s", body)
	}
	_, body = e.do(c, "GET", "/pipelines/dead/tables", "", nil)
	if !strings.Contains(body, "not running") {
		t.Errorf("tables of a stopped pipeline must say so: %s", body)
	}
	_, body = e.do(c, "GET", "/pipelines/dead", "", nil)
	if !strings.Contains(body, "Last exit code") || !strings.Contains(body, "failed") {
		t.Errorf("detail header: %s", body)
	}
	_, body = e.do(c, "GET", "/api/v1/overview", "", nil)
	if !strings.Contains(body, `"failed": 1`) {
		t.Errorf("overview counts: %s", body)
	}
}

func TestStoppedBackgroundPipelineInfersExitFromLog(t *testing.T) {
	var logPath string
	e := newEnv(t, func(e *testEnv, c *Config) {
		dir := t.TempDir()
		logPath = filepath.Join(dir, "strand.log")
		if err := os.WriteFile(logPath, []byte("time=2026-11-15T10:00:00Z level=INFO msg=starting\ntime=2026-11-15T10:00:05Z level=ERROR msg=\"[schema_change_pending] DDL on app.t needs the target to match\"\n"), 0o640); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, "bg.yaml")
		if err := os.WriteFile(p, []byte("name: bg\nsource: {kind: mysql}\ntarget: {kind: mysql}\nobservability: {admin_socket: "+dir+"/none.sock}\n"), 0o640); err != nil {
			t.Fatal(err)
		}
		c.Pipelines = append(c.Pipelines, PipelineEntry{Name: "bg", Config: p, LogFile: logPath, PidFile: filepath.Join(dir, "strand.pid")})
	})
	e.sampleNow()
	c := e.client()
	e.login(c, testPassword)
	if st := e.srv.Sampler().Latest("bg").State; st != StateStopped {
		t.Fatalf("state %s, want stopped", st)
	}
	var ev ErrorsView
	_, body := e.do(c, "GET", "/api/v1/pipelines/bg/errors", "", nil)
	if err := json.Unmarshal([]byte(body), &ev); err != nil || ev.LastExit == nil || ev.LastExit.Code != 10 || ev.ExitFrom != "log" || ev.ErrorCode != "schema_change_pending" {
		t.Fatalf("errors: %v %s", err, body)
	}
	_, body = e.do(c, "GET", "/pipelines/bg/logs", "", nil)
	if !strings.Contains(body, "file "+logPath) || !strings.Contains(body, "schema_change_pending") {
		t.Errorf("bg logs: %s", body)
	}
}

func TestExplainExitCoversTable(t *testing.T) {
	for code, auto := range map[int]bool{0: false, 1: true, 2: false, 3: false, 4: false, 5: false, 10: false, 137: true} {
		e := ExplainExit(code)
		if e.Code != code || e.Meaning == "" || e.AutoRestart != auto {
			t.Errorf("exit %d: %+v", code, e)
		}
	}
	if ExplainExit(77).Meaning != "Unknown exit code" {
		t.Error("unknown code")
	}
	for code, want := range map[string]int{"log_gap": 5, "missing_pk": 3, "pipeline_locked": 4, "promote_needs_decision": 10, "invalid_syntax": 2} {
		if got, ok := exitForErrorCode(code); !ok || got != want {
			t.Errorf("%s -> %d %v", code, got, ok)
		}
	}
	if _, ok := exitForErrorCode("whatever"); ok {
		t.Error("unknown error code")
	}
}

func TestParseLogEventsJSONAndText(t *testing.T) {
	lines := []string{
		`{"time":"t1","level":"INFO","msg":"a"}`,
		`{"time":"t2","level":"WARN","msg":"b"}`,
		`time=t3 level=ERROR msg="c d" x=1`,
		`time=t4 level=INFO msg=e`,
		`garbage`, `{broken`,
	}
	ev := ParseLogEvents(lines, 10)
	if len(ev) != 2 || ev[0].Message != "b" || ev[1].Message != "c d  x=1" || ev[1].Time != "t3" || ev[1].Level != "ERROR" {
		t.Fatalf("%+v", ev)
	}
	if got := ParseLogEvents(lines, 1); len(got) != 1 || got[0].Message != "c d  x=1" {
		t.Fatalf("limit: %+v", got)
	}
	if ErrorCodeFromLine("Error: [log_gap] x") != "log_gap" || ErrorCodeFromLine("no code") != "" {
		t.Fatal("error code")
	}
	rec := EventsToLog([]admin.EventRecord{{Time: time.Unix(0, 0).UTC(), Level: "WARN", Message: "m", Attrs: map[string]string{"b": "2", "a": "1"}}})
	if rec[0].Message != "m  a=1  b=2" {
		t.Fatalf("%q", rec[0].Message)
	}
}

func TestTailFileLargeAndMissing(t *testing.T) {
	p := filepath.Join(t.TempDir(), "big.log")
	var sb strings.Builder
	for i := 0; i < 50000; i++ {
		fmt.Fprintf(&sb, "line %d\n", i)
	}
	if err := os.WriteFile(p, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := TailFile(p, 3)
	if err != nil || strings.Join(got, ",") != "line 49997,line 49998,line 49999" {
		t.Fatalf("%v %v", got, err)
	}
	if _, err := TailFile(filepath.Join(t.TempDir(), "none"), 3); err == nil {
		t.Fatal("missing file must error")
	}
	if got, err := TailFile(p, 100000); err != nil || len(got) > 60000 || len(got) == 0 {
		t.Fatalf("window must bound the read: %d %v", len(got), err)
	}
}

func TestLineChartSVG(t *testing.T) {
	t0 := time.Date(2026, 11, 15, 10, 0, 0, 0, time.UTC)
	var pts []Point
	for i := 0; i < 10; i++ {
		pts = append(pts, Point{T: t0.Add(time.Duration(i) * 5 * time.Second), V: float64(i * 10)})
	}
	svg := string(LineChart("Lag <x>", "seconds", []ChartSeries{{Name: "lag", Class: "s1", Points: pts}, {Name: "second", Class: "s2", Points: pts[:1]}}, 520, 150))
	for _, want := range []string{`<svg class="linechart"`, `<polyline class="line s1"`, `<circle class="pt s2"`, "17:00:00", "17:00:45", ">100<", ">0<", "Lag &lt;x&gt;", `class="legend"`} {
		if !strings.Contains(svg, want) {
			t.Errorf("chart lacks %q:\n%s", want, svg)
		}
	}
	if strings.Contains(svg, "<x>") || strings.Contains(svg, " style=") || strings.Contains(svg, "NaN") || strings.Contains(svg, "Inf") {
		t.Errorf("unsafe or invalid output:\n%s", svg)
	}
	// Điểm đầu ở trái, điểm cuối ở phải: x tăng dần, y của giá trị lớn nhỏ hơn (cao hơn).
	if !strings.Contains(svg, "points=\"52.0,") {
		t.Errorf("first point must sit at the left padding: %s", svg)
	}
	empty := string(LineChart("t", "u", nil, 520, 150))
	if !strings.Contains(empty, "no data") || strings.Contains(empty, "<polyline") {
		t.Errorf("empty chart: %s", empty)
	}
	nan := string(LineChart("t", "u", []ChartSeries{{Points: []Point{{T: t0, V: nanVal()}, {T: t0.Add(time.Second), V: 3}, {T: t0.Add(2 * time.Second), V: 4}}}}, 300, 100))
	if strings.Contains(nan, "NaN") {
		t.Errorf("NaN leaked: %s", nan)
	}
	flat := string(LineChart("t", "u", []ChartSeries{{Points: []Point{{T: t0, V: 0}, {T: t0.Add(time.Second), V: 0}}}}, 300, 100))
	if !strings.Contains(flat, "<polyline") || strings.Contains(flat, "NaN") {
		t.Errorf("all-zero series: %s", flat)
	}
}

func nanVal() float64 { var z float64; return z / z }

func TestSparklineProgressAndGraphSVG(t *testing.T) {
	sp := string(Sparkline([]float64{1, 3, 2, 5}, 90, 22))
	if !strings.Contains(sp, `<polyline class="line s1" points="1.0,`) {
		t.Errorf("sparkline: %s", sp)
	}
	if strings.Contains(string(Sparkline(nil, 90, 22)), "polyline") {
		t.Error("empty sparkline must be blank")
	}
	pb := string(ProgressBar(37.5))
	if !strings.Contains(pb, `width="37.50"`) || strings.Contains(pb, " style=") {
		t.Errorf("progress: %s", pb)
	}
	if !strings.Contains(string(ProgressBar(250)), `width="100.00"`) || !strings.Contains(string(ProgressBar(-5)), `width="0.00"`) {
		t.Error("progress must clamp")
	}
	g := string(PipelineGraph([]GraphNode{{Title: "Source", Class: "ok", Lines: []string{"mysql", "lag 1 s"}}, {Title: "T<arget>", Class: "bad"}}))
	for _, want := range []string{`class="node ok"`, `class="node bad"`, "Source", "lag 1 s", "T&lt;arget&gt;", "marker-end"} {
		if !strings.Contains(g, want) {
			t.Errorf("graph lacks %q: %s", want, g)
		}
	}
	for in, want := range map[float64]string{0: "0", 0.256: "0.26", 12.34: "12.3", 123.4: "123", 12345: "12.3k", 2.5e6: "2.5M"} {
		if got := FormatValue(in); got != want {
			t.Errorf("FormatValue(%v) = %s, want %s", in, got, want)
		}
	}
	for in, want := range map[float64]float64{0: 1, 0.7: 1, 1.1: 2, 3: 5, 7: 10, 130: 200} {
		if got := niceMax(in); got != want {
			t.Errorf("niceMax(%v) = %v, want %v", in, got, want)
		}
	}
}
