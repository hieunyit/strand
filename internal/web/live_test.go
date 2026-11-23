package web

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/admin"
)

// ---- smoothing, hysteresis, state rule (pure functions) ----

func TestRateWindowAveragesOverWindow(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var w rateWindow
	if _, ok := w.add(t0, 0, RateWindow); ok {
		t.Fatal("one point has no rate")
	}
	// Bursty counter: +1000 then +0 alternating every 5 s. The instantaneous rate jumps 200 <-> 0,
	// the 30 s window stays near the true average of 100 rows/s.
	v := 0.0
	var rates []float64
	for i := 1; i <= 24; i++ {
		if i%2 == 1 {
			v += 1000
		}
		r, ok := w.add(t0.Add(time.Duration(i)*5*time.Second), v, RateWindow)
		if !ok {
			t.Fatalf("no rate at %d", i)
		}
		rates = append(rates, r)
	}
	for _, r := range rates[8:] { // after the window filled
		if r < 80 || r > 120 {
			t.Fatalf("windowed rate must stay near 100, got %v (all %v)", r, rates)
		}
	}
	if span := w.pts[len(w.pts)-1].t.Sub(w.pts[0].t); span < RateWindow || span > RateWindow+5*time.Second {
		t.Fatalf("window span %v", span)
	}
	// Counter reset (pipeline restarted): no negative rate, the window restarts.
	if r, ok := w.add(t0.Add(200*time.Second), 10, RateWindow); ok || r < 0 {
		t.Fatalf("after reset: %v %v", r, ok)
	}
	if r, ok := w.add(t0.Add(205*time.Second), 60, RateWindow); !ok || r != 10 {
		t.Fatalf("rate after reset %v %v", r, ok)
	}
	// Same timestamp again: ignored, previous rate.
	if r, ok := w.add(t0.Add(205*time.Second), 9999, RateWindow); !ok || r != 10 {
		t.Fatalf("duplicate timestamp %v %v", r, ok)
	}
}

func TestEWMA(t *testing.T) {
	if ewma(0, false, 50, time.Second, RateWindow) != 50 {
		t.Fatal("first value seeds")
	}
	got := ewma(0, true, 100, 30*time.Second, 30*time.Second)
	if want := 100 * (1 - math.Exp(-1)); math.Abs(got-want) > 1e-9 {
		t.Fatalf("ewma %v want %v", got, want)
	}
	if ewma(7, true, 100, 0, RateWindow) != 7 {
		t.Fatal("dt=0 keeps the previous value")
	}
	// A single spike moves a 30 s EWMA only by ~15% at a 5 s interval.
	if v := ewma(100, true, 1000, 5*time.Second, RateWindow); v > 260 {
		t.Fatalf("spike not damped: %v", v)
	}
}

func TestLagLevelHysteresis(t *testing.T) {
	steps := []struct {
		lag  float64
		want string
	}{{10, ""}, {29.9, ""}, {30, "warn"}, {26, "warn"}, {24.5, "warn"}, {23.9, ""}, {31, "warn"}, {300, "bad"}, {250, "bad"}, {239, "warn"}, {25, "warn"}, {1, ""}, {math.NaN(), ""}, {-3, ""}}
	lvl := ""
	for i, s := range steps {
		lvl = lagLevel(lvl, s.lag)
		if lvl != s.want {
			t.Fatalf("step %d lag %v: %q want %q", i, s.lag, lvl, s.want)
		}
	}
}

func TestNextStateNeedsTwoAgreeingSamples(t *testing.T) {
	cases := []struct{ shown, lastRaw, raw, want string }{
		{"", "", StateSynced, StateSynced},                        // first sample
		{StateSynced, StateSynced, StateUnreachable, StateSynced}, // one bad poll: keep
		{StateSynced, StateUnreachable, StateUnreachable, StateUnreachable},
		{StateSynced, StateSynced, StatePaused, StateSynced},
		{StateSynced, StatePaused, StatePaused, StatePaused},
		{StateSynced, StateSynced, StateFailed, StateFailed},                 // immediate
		{StateBootstrapping, StateBootstrapping, StateStopped, StateStopped}, // immediate
		{StateUnreachable, StateUnreachable, StateSynced, StateUnreachable},
		{StatePaused, StatePaused, StatePaused, StatePaused},
	}
	for _, c := range cases {
		if got := nextState(c.shown, c.lastRaw, c.raw); got != c.want {
			t.Errorf("nextState(%q, %q, %q) = %q, want %q", c.shown, c.lastRaw, c.raw, got, c.want)
		}
	}
}

func TestFmtLagStableUnits(t *testing.T) {
	for in, want := range map[float64]string{0: "0.00 s", -1: "0.00 s", 0.25: "0.25 s", 9.994: "9.99 s", 12.34: "12.3 s", 59.9: "59.9 s", 65: "1m 05s", 3725: "1h 02m"} {
		if got := fmtLag(in); got != want {
			t.Errorf("fmtLag(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestNewLinesOverlap(t *testing.T) {
	add, ok := newLines([]string{"a", "b", "c"}, []string{"b", "c", "d", "e"})
	if !ok || strings.Join(add, ",") != "d,e" {
		t.Fatalf("%v %v", add, ok)
	}
	if add, ok := newLines([]string{"a", "b"}, []string{"a", "b"}); !ok || len(add) != 0 {
		t.Fatalf("no new lines: %v %v", add, ok)
	}
	if add, ok := newLines([]string{"a"}, []string{"x", "y"}); ok || len(add) != 2 {
		t.Fatalf("no overlap: %v %v", add, ok)
	}
	// repeated lines: the longest overlap wins
	if add, _ := newLines([]string{"x", "x"}, []string{"x", "x", "y"}); strings.Join(add, ",") != "y" {
		t.Fatalf("repeats: %v", add)
	}
}

func TestHubReplayAndResync(t *testing.T) {
	h := newLiveHub()
	for i := 0; i < 5; i++ {
		h.publish("sample", "a", map[string]int{"i": i}, map[string]int{"lite": i})
	}
	s, replay, ok, seq := h.subscribe("", 3)
	if !ok || seq != 5 || len(replay) != 2 || replay[0].ID != 4 || replay[1].ID != 5 {
		t.Fatalf("replay %v %v %d", ok, replay, seq)
	}
	if string(replay[0].payloadFor("")) != `{"lite":3}` || string(replay[0].payloadFor("a")) != `{"i":3}` {
		t.Fatalf("payloads %s %s", replay[0].payloadFor(""), replay[0].payloadFor("a"))
	}
	h.unsubscribe(s)
	if _, _, ok, _ := h.subscribe("", 99); ok {
		t.Fatal("an id from the future needs a resync")
	}
	for i := 0; i < hubRingSize+10; i++ {
		h.publish("overview", "", i, nil)
	}
	if _, _, ok, _ := h.subscribe("", 2); ok {
		t.Fatal("an id older than the ring needs a resync")
	}
	// a slow subscriber never blocks publish; it is marked lagged
	slow, _, _, _ := h.subscribe("", 0)
	for i := 0; i < subBuffer+5; i++ {
		h.publish("overview", "", i, nil)
	}
	if !slow.lagged.Load() {
		t.Fatal("full subscriber must be marked lagged")
	}
}

// ---- sampler: stale values, smoothing, hysteresis with a real admin socket ----

// countingPipeline counts every admin call; block makes Top hang like a SIGSTOPped process.
type countingPipeline struct {
	*recPipeline
	calls   atomic.Int64
	block   atomic.Bool
	applied atomic.Int64
	top     atomic.Pointer[admin.TopInfo]
}

func (m *countingPipeline) hit(ctx context.Context) error {
	m.calls.Add(1)
	// hang while blocked (the server side never sees the client's timeout, so poll the flag)
	for m.block.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return nil
}
func (m *countingPipeline) Top(ctx context.Context) (*admin.TopInfo, error) {
	if err := m.hit(ctx); err != nil {
		return nil, err
	}
	if top := m.top.Load(); top != nil {
		return top, nil
	}
	return m.recPipeline.Top(ctx)
}
func (m *countingPipeline) Status(ctx context.Context) (*admin.StatusInfo, error) {
	if err := m.hit(ctx); err != nil {
		return nil, err
	}
	st, err := m.recPipeline.Status(ctx)
	if st != nil {
		st.AppliedRows = m.applied.Load()
	}
	return st, err
}
func (m *countingPipeline) Tables(ctx context.Context) ([]admin.TableInfo, error) {
	if err := m.hit(ctx); err != nil {
		return nil, err
	}
	return m.recPipeline.Tables(ctx)
}
func (m *countingPipeline) CheckpointShow(ctx context.Context) (*admin.CheckpointResult, error) {
	if err := m.hit(ctx); err != nil {
		return nil, err
	}
	return m.recPipeline.CheckpointShow(ctx)
}
func (m *countingPipeline) Events(ctx context.Context) ([]admin.EventRecord, error) {
	if err := m.hit(ctx); err != nil {
		return nil, err
	}
	return m.recPipeline.Events(ctx)
}
func (m *countingPipeline) SourceTables(ctx context.Context) ([]admin.SourceTable, error) {
	m.calls.Add(1)
	return m.recPipeline.SourceTables(ctx)
}

// countedEnv: pipeline "orders" served by a counting admin socket, managed by (fake) systemd as running,
// sampled every second (poll timeout 1 s).
func countedEnv(t *testing.T) (*testEnv, *countingPipeline) {
	t.Helper()
	var cp *countingPipeline
	e := newEnv(t, func(e *testEnv, c *Config) {
		sockDir, err := os.MkdirTemp("", "swc")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(sockDir) })
		sock := filepath.Join(sockDir, "c.sock")
		cp = &countingPipeline{recPipeline: &recPipeline{mockPipeline: &mockPipeline{events: e.mock.events}}}
		asrv := admin.NewServer(sock, cp, nil)
		if err := asrv.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = asrv.Close() })
		if err := os.WriteFile(c.Pipelines[0].Config, []byte(fmt.Sprintf(pipelineYAML, filepath.Join(e.dir, "spool"), sock)), 0o640); err != nil {
			t.Fatal(err)
		}
		c.Pipelines[0].SystemdUnit = "strand@orders.service"
		e.cmd.setOut("systemctl show strand@orders.service", "ActiveState=active\nSubState=running\nExecMainPID=4242\n")
		c.SampleIntervalSec = 1
	})
	return e, cp
}

func TestSamplerKeepsLastGoodValuesWhenPollFails(t *testing.T) {
	e, cp := countedEnv(t)
	s := e.srv.Sampler()
	s.SampleOnce(context.Background())
	first := s.Latest("orders")
	if first == nil || first.Stale || !first.Reachable || first.State != StateSynced || first.Status == nil || len(first.Tables) != 3 {
		t.Fatalf("first sample %+v", first)
	}
	okAt := first.LastOK

	cp.block.Store(true) // admin socket hangs: the poll times out
	e.clock.Advance(time.Second)
	start := time.Now()
	s.SampleOnce(context.Background())
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("poll not bounded by its timeout: %v", d)
	}
	sn := s.Latest("orders")
	if !sn.Stale || sn.Reachable || sn.Top == nil || sn.Top.LagSeconds != 0.25 || !sn.LastOK.Equal(okAt) || sn.PollErr == "" {
		t.Fatalf("stale sample must keep the last good values: %+v", sn)
	}
	if sn.RawState != StateUnreachable || sn.State != StateSynced {
		t.Fatalf("one failed poll must not flip the state: raw %s shown %s", sn.RawState, sn.State)
	}
	if len(sn.Tables) != 3 || sn.Status == nil || sn.Checkpoint == nil {
		t.Fatal("stale snap keeps tables/status/checkpoint")
	}
	// pages show the stale marker and the old numbers, not blanks
	c, _ := e.loginOK()
	_, body := e.do(c, "GET", "/pipelines/orders", "", nil)
	if !strings.Contains(body, `card head is-stale`) || strings.Contains(body, `data-stale-tag hidden`) || !strings.Contains(body, ">0.25 s<") {
		t.Errorf("detail page while stale: %s", body)
	}

	e.clock.Advance(time.Second)
	s.SampleOnce(context.Background())
	if sn := s.Latest("orders"); sn.State != StateUnreachable || !sn.Stale || sn.Top == nil {
		t.Fatalf("two failed polls agree: state %s stale %v", sn.State, sn.Stale)
	}

	cp.block.Store(false)
	e.clock.Advance(time.Second)
	s.SampleOnce(context.Background())
	sn = s.Latest("orders")
	if sn.Stale || !sn.Reachable || sn.State != StateUnreachable || sn.RawState != StateSynced {
		t.Fatalf("recovery: one good sample clears stale but the badge waits for a second: %+v", sn)
	}
	e.clock.Advance(time.Second)
	s.SampleOnce(context.Background())
	if sn := s.Latest("orders"); sn.State != StateSynced {
		t.Fatalf("second good sample: %s", sn.State)
	}
}

func TestSamplerSmoothedRowsPerSecond(t *testing.T) {
	e, cp := countedEnv(t)
	s := e.srv.Sampler()
	// 500 rows every second for 40 s, but delivered in bursts (1000, 0, 1000, 0...)
	var rates []float64
	for i := 0; i < 40; i++ {
		if i%2 == 0 {
			cp.applied.Add(1000)
		}
		s.SampleOnce(context.Background())
		rates = append(rates, s.Latest("orders").Rates.Rows)
		e.clock.Advance(time.Second)
	}
	for _, r := range rates[32:] {
		if r < 450 || r > 550 {
			t.Fatalf("smoothed rows/s must stay near 500: %v", rates)
		}
	}
	ser := s.Series("orders")
	if last := ser[len(ser)-1]; last.Rows != rates[len(rates)-1] {
		t.Fatalf("series carries the smoothed rate: %+v", last)
	}
}

// ---- no admin call on the request path ----

func TestPagesAndStreamNeverCallTheAdminSocket(t *testing.T) {
	e, cp := countedEnv(t)
	c, _ := e.loginOK()
	paths := []string{"/", "/pipelines", "/pipelines/orders", "/pipelines/orders/tables", "/pipelines/orders/checkpoint",
		"/pipelines/orders/errors", "/pipelines/orders/config", "/pipelines/orders/logs", "/pipelines/orders/jobs",
		"/pipelines/orders/repair", "/pipelines/orders/promote",
		"/api/v1/overview", "/api/v1/pipelines", "/api/v1/pipelines/orders", "/api/v1/pipelines/orders/status",
		"/api/v1/pipelines/orders/tables", "/api/v1/pipelines/orders/checkpoint", "/api/v1/pipelines/orders/errors",
		"/api/v1/pipelines/orders/series", "/api/v1/live?pipeline=orders", "/api/v1/live"}
	get := func() {
		for _, p := range paths {
			if resp, _ := e.do(c, "GET", p, "", nil); resp.StatusCode != 200 {
				t.Fatalf("GET %s: %d", p, resp.StatusCode)
			}
		}
	}
	// before the first sample: pages render, say so, and still do not ask the pipeline
	get()
	if n := cp.calls.Load(); n != 0 {
		t.Fatalf("requests before any sample made %d admin calls", n)
	}
	_, body := e.do(c, "GET", "/pipelines/orders/tables", "", nil)
	if !strings.Contains(body, "no sample yet") {
		t.Errorf("tables tab before the first sample: %s", body)
	}
	e.sampleNow()
	perPoll := cp.calls.Load()
	if perPoll < 1 || perPoll > 5 {
		t.Fatalf("one poll = top+status+tables+checkpoint+events, got %d calls", perPoll)
	}
	get()
	st := openStream(t, e, c, "/api/v1/stream?pipeline=orders&logs=1", nil)
	st.next(t, "snapshot")
	st.close()
	if n := cp.calls.Load(); n != perPoll {
		t.Fatalf("pages/API/stream made %d admin calls", n-perPoll)
	}
	// an admin socket that hangs (SIGSTOP) does not slow the pages down
	cp.block.Store(true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); e.sampleNow() }() // blocked in the poll for ~1 s
	time.Sleep(50 * time.Millisecond)
	for _, p := range []string{"/", "/pipelines/orders", "/pipelines/orders/tables", "/api/v1/live?pipeline=orders"} {
		t0 := time.Now()
		if resp, _ := e.do(c, "GET", p, "", nil); resp.StatusCode != 200 {
			t.Fatalf("GET %s: %d", p, resp.StatusCode)
		}
		if d := time.Since(t0); d > 400*time.Millisecond {
			t.Errorf("GET %s took %v while the admin socket hangs", p, d)
		}
	}
	wg.Wait()
	cp.block.Store(false) // release the hung server-side handlers
}

func TestPipelineVerificationEvidenceReachesAPIAndPage(t *testing.T) {
	e, cp := countedEnv(t)
	c, _ := e.loginOK()
	assertResponse := func(path string, present, absent []string) {
		t.Helper()
		resp, body := e.do(c, "GET", path, "", nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
		for _, fragment := range present {
			if !strings.Contains(body, fragment) {
				t.Errorf("%s missing %q", path, fragment)
			}
		}
		for _, fragment := range absent {
			if strings.Contains(body, fragment) {
				t.Errorf("%s retained %q", path, fragment)
			}
		}
	}
	cp.top.Store(&admin.TopInfo{
		State: "synced", CDCState: "caught_up",
		Verification: &admin.VerificationEvidence{
			Status: "incomplete", CheckedAt: e.clock.Now(),
			Tables: []admin.VerificationTable{
				{Table: "app.keyed", Status: "checked", Method: "sample-and-changed-keys"},
				{Table: "app.keyless", Status: "incomplete", Reason: "source is changing"},
			},
		},
	})
	e.sampleNow()
	assertResponse("/api/v1/pipelines/orders",
		[]string{`"cdc_state": "caught_up"`, `"verification":`, "app.keyed", "app.keyless", "source is changing"}, nil)
	assertResponse("/api/v1/live?pipeline=orders",
		[]string{`"cdc_state": "caught_up"`, `"verification_status": "incomplete"`}, nil)
	assertResponse("/pipelines/orders",
		[]string{`data-verification-status="incomplete"`, "app.keyed", "app.keyless", "source is changing"}, nil)

	cp.top.Store(&admin.TopInfo{State: "synced"})
	e.clock.Advance(time.Second)
	e.sampleNow()
	assertResponse("/api/v1/live?pipeline=orders", nil, []string{`"verification_status"`, "caught_up"})
	assertResponse("/pipelines/orders", nil, []string{"data-verification-status", "source is changing"})
}

// ---- SSE endpoint ----

type sseStream struct {
	resp *http.Response
	r    *bufio.Reader
	evs  chan sseEvent
}

type sseEvent struct {
	ID, Type, Data, Comment string
}

func openStream(t *testing.T, e *testEnv, c *http.Client, path string, hdr map[string]string) *sseStream {
	t.Helper()
	req, _ := http.NewRequest("GET", e.ts.URL+path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		t.Fatalf("stream %s: %d", path, resp.StatusCode)
	}
	s := &sseStream{resp: resp, r: bufio.NewReader(resp.Body), evs: make(chan sseEvent, 64)}
	go func() {
		defer close(s.evs)
		var ev sseEvent
		for {
			line, err := s.r.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case line == "":
				if ev != (sseEvent{}) {
					s.evs <- ev
				}
				ev = sseEvent{}
			case strings.HasPrefix(line, ":"):
				ev.Comment = strings.TrimSpace(line[1:])
			case strings.HasPrefix(line, "id: "):
				ev.ID = line[4:]
			case strings.HasPrefix(line, "event: "):
				ev.Type = line[7:]
			case strings.HasPrefix(line, "data: "):
				ev.Data = line[6:]
			case strings.HasPrefix(line, "retry: "):
				ev.Comment = line
			}
		}
	}()
	return s
}

// next returns the next event of type typ (skipping others); typ "" = any event or comment.
func (s *sseStream) next(t *testing.T, typ string) sseEvent {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-s.evs:
			if !ok {
				t.Fatalf("stream closed while waiting for %q", typ)
			}
			if typ == "" || ev.Type == typ || (typ == "#comment" && ev.Type == "" && ev.Comment != "") {
				return ev
			}
		case <-timeout:
			t.Fatalf("timeout waiting for %q", typ)
		}
	}
}

func (s *sseStream) close() { s.resp.Body.Close() }

func TestStreamRequiresAuth(t *testing.T) {
	tok := NewAPIToken()
	e := newEnv(t, func(e *testEnv, c *Config) { c.APITokenHash = HashAPIToken(tok) })
	anon := e.client()
	for _, p := range []string{"/api/v1/stream", "/api/v1/stream?pipeline=orders", "/api/v1/live"} {
		if resp, _ := e.do(anon, "GET", p, "", nil); resp.StatusCode != 401 {
			t.Errorf("anonymous %s: %d", p, resp.StatusCode)
		}
		if resp, _ := e.do(anon, "GET", p, "", map[string]string{"Authorization": "Bearer nope"}); resp.StatusCode != 401 {
			t.Errorf("bad token %s: %d", p, resp.StatusCode)
		}
	}
	// a static token works (no cookie, no CSRF: GET only)
	st := openStream(t, e, anon, "/api/v1/stream", map[string]string{"Authorization": "Bearer " + tok})
	if ct := st.resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("content type %q", ct)
	}
	if csp := st.resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") {
		t.Errorf("CSP missing on the stream: %q", csp)
	}
	if cc := st.resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("cache-control %q", cc)
	}
	st.next(t, "snapshot")
	st.close()
	c, _ := e.loginOK()
	if resp, _ := e.do(c, "GET", "/api/v1/stream?pipeline=nope", "", nil); resp.StatusCode != 404 {
		t.Errorf("unknown pipeline: %d", resp.StatusCode)
	}
}

func TestStreamEventsHeartbeatAndResume(t *testing.T) {
	e := newEnv(t, nil)
	e.srv.sseHeartbeat = 60 * time.Millisecond
	c, csrf := e.loginOK()
	e.sampleNow()

	st := openStream(t, e, c, "/api/v1/stream?pipeline=orders", nil)
	if ev := st.next(t, ""); ev.Comment != "retry: 3000" {
		t.Fatalf("first frame %+v", ev)
	}
	snap := st.next(t, "snapshot")
	var ls LiveSnapshot
	if err := json.Unmarshal([]byte(snap.Data), &ls); err != nil {
		t.Fatal(err, snap.Data)
	}
	if ls.IntervalS != 5 || len(ls.Pipelines) != 1 || ls.Pipelines[0].Name != "orders" || ls.Pipelines[0].F["lag"] != "0.25 s" ||
		!ls.Pipelines[0].TablesFull || len(ls.Pipelines[0].TableRows) != 3 || ls.Overview.Total != 1 || ls.Pipelines[0].Tables == nil || ls.Pipelines[0].Tables.Total != 3 {
		t.Fatalf("snapshot %s", snap.Data)
	}
	if strings.Contains(snap.Data, "LeakyPass") {
		t.Fatal("secrets in the event list must be masked")
	}
	if id, _ := strconv.ParseUint(snap.ID, 10, 64); id != ls.LastID || id == 0 {
		t.Fatalf("snapshot id %q vs last_event_id %d", snap.ID, ls.LastID)
	}
	if hb := st.next(t, "#comment"); !strings.HasPrefix(hb.Comment, "hb ") {
		t.Fatalf("heartbeat %+v", hb)
	}

	e.clock.Advance(5 * time.Second)
	e.sampleNow()
	smp := st.next(t, "sample")
	var rec LiveRecord
	if err := json.Unmarshal([]byte(smp.Data), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Name != "orders" || rec.State != StateSynced || rec.Pt == nil || rec.Pt.Lag != 0.25 || rec.F["state"] != "synced" || rec.Lag == nil {
		t.Fatalf("sample %s", smp.Data)
	}
	if len(rec.TableRows) != 0 || len(rec.EventsNew) != 0 {
		t.Fatalf("unchanged tables/events are not resent: %s", smp.Data)
	}
	ov := st.next(t, "overview")
	if !strings.Contains(ov.Data, `"total":1`) || !strings.Contains(ov.Data, `"attention":[]`) {
		t.Fatalf("overview %s", ov.Data)
	}
	lastSeen := ov.ID
	st.close()

	// two more samples while disconnected; reconnecting with Last-Event-ID replays exactly what was missed
	e.clock.Advance(5 * time.Second)
	e.sampleNow()
	st2 := openStream(t, e, c, "/api/v1/stream?pipeline=orders", map[string]string{"Last-Event-ID": lastSeen})
	st2.next(t, "") // retry
	ev := st2.next(t, "")
	if ev.Type != "sample" {
		t.Fatalf("resume must replay, got %+v", ev)
	}
	want, _ := strconv.ParseUint(lastSeen, 10, 64)
	if id, _ := strconv.ParseUint(ev.ID, 10, 64); id != want+1 {
		t.Fatalf("replay starts at %d, want %d", id, want+1)
	}
	st2.next(t, "overview")
	st2.close()

	// an id the server does not know (restart): full resync
	st3 := openStream(t, e, c, "/api/v1/stream?pipeline=all", map[string]string{"Last-Event-ID": "999999"})
	st3.next(t, "")
	snap3 := st3.next(t, "")
	if snap3.Type != "snapshot" {
		t.Fatalf("unknown id must resync: %+v", snap3)
	}
	var ls3 LiveSnapshot
	_ = json.Unmarshal([]byte(snap3.Data), &ls3)
	if len(ls3.Pipelines) != 1 || ls3.Pipelines[0].TableRows != nil {
		t.Fatalf("pipeline=all carries no table rows: %s", snap3.Data)
	}

	// logout ends an open stream at the next heartbeat
	if resp, _ := e.do(c, "POST", "/api/v1/logout", "", map[string]string{csrfHeader: csrf}); resp.StatusCode != 200 {
		t.Fatalf("logout %d", resp.StatusCode)
	}
	st3.next(t, "bye")
	st3.close()
}

func TestStreamTableDeltasAndEvents(t *testing.T) {
	e := newEnv(t, nil)
	c, _ := e.loginOK()
	e.sampleNow()
	st := openStream(t, e, c, "/api/v1/stream?pipeline=orders", nil)
	st.next(t, "snapshot")
	// a new warning appears in the pipeline's event ring
	e.mock.mu.Lock()
	e.mock.events = append(e.mock.events, admin.EventRecord{Time: time.Now(), Level: "ERROR", Message: "apply failed password=Secret1"})
	e.mock.mu.Unlock()
	e.clock.Advance(5 * time.Second)
	e.sampleNow()
	var rec LiveRecord
	_ = json.Unmarshal([]byte(st.next(t, "sample").Data), &rec)
	if len(rec.EventsNew) != 1 || !strings.Contains(rec.EventsNew[0].Message, "apply failed") || strings.Contains(rec.EventsNew[0].Message, "Secret1") {
		t.Fatalf("events delta %+v", rec.EventsNew)
	}
	st.close()
	// lite records (other pages) never carry tables or events
	st2 := openStream(t, e, c, "/api/v1/stream", nil)
	st2.next(t, "snapshot")
	e.clock.Advance(5 * time.Second)
	e.sampleNow()
	if d := st2.next(t, "sample").Data; strings.Contains(d, "table_rows") || strings.Contains(d, "events_new") || strings.Contains(d, "g_lag") {
		t.Fatalf("lite record too big: %s", d)
	}
	st2.close()
}

func TestStreamLogTail(t *testing.T) {
	e := bgEnv(t, logStart, logRun)
	c, _ := e.loginOK()
	st := openStream(t, e, c, "/api/v1/stream?pipeline=bg&logs=1", nil)
	var lg struct {
		Lines   []string `json:"lines"`
		Initial bool     `json:"initial"`
	}
	_ = json.Unmarshal([]byte(st.next(t, "log").Data), &lg)
	if !lg.Initial || len(lg.Lines) != 2 {
		t.Fatalf("initial tail %+v", lg)
	}
	lp := e.srv.reg.Get("bg").LogFile
	f, err := os.OpenFile(lp, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(f, `{"time":"2026-11-15T21:00:02Z","level":"INFO","msg":"next line password=hunter2"}`)
	f.Close()
	_ = json.Unmarshal([]byte(st.next(t, "log").Data), &lg)
	if lg.Initial || len(lg.Lines) != 1 || !strings.Contains(lg.Lines[0], "next line") || strings.Contains(lg.Lines[0], "hunter2") {
		t.Fatalf("appended %+v", lg)
	}
	st.close()
}

func TestKickAcceptsStateAfterAction(t *testing.T) {
	e := newEnv(t, nil)
	s := e.srv.Sampler()
	s.SampleOnce(context.Background())
	if s.Latest("orders").State != StateSynced {
		t.Fatal("setup")
	}
	s.Kick(context.Background(), "orders")
	waitFor(t, "kick sample", func() bool {
		return s.Latest("orders").At.After(time.Time{}) && s.Series("orders") != nil && len(s.Series("orders")) == 2
	})
}
