package web

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
	"strand/internal/web/store"
)

// writePipelineLog replaces the background log of the pipeline (what `strand sync --background` writes).
func writePipelineLog(t *testing.T, e *testEnv, name string, lines ...string) {
	t.Helper()
	p := e.srv.reg.Get(name)
	if p == nil {
		t.Fatalf("no pipeline %s", name)
	}
	logPath, _ := p.BgPaths()
	if err := os.WriteFile(logPath, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func slogLine(at time.Time, level, msg string) string {
	return "time=" + at.Format(time.RFC3339Nano) + " level=" + level + ` msg="` + msg + `"`
}

// downPoller: the admin socket of a stopped pipeline (nothing listens).
type downPoller struct{}

var errSockDown = errors.New("dial unix: connect: no such file or directory")

func (downPoller) Top(context.Context) (*admin.TopInfo, error)       { return nil, errSockDown }
func (downPoller) Status(context.Context) (*admin.StatusInfo, error) { return nil, errSockDown }
func (downPoller) Tables(context.Context) ([]admin.TableInfo, error) { return nil, errSockDown }
func (downPoller) CheckpointShow(context.Context) (*admin.CheckpointResult, error) {
	return nil, errSockDown
}
func (downPoller) Events(context.Context) ([]admin.EventRecord, error) { return nil, errSockDown }

func syncCallCount(e *testEnv) int {
	n := 0
	for _, cl := range e.fakeCalls() {
		if len(cl.Args) > 0 && cl.Args[0] == "sync" {
			n++
		}
	}
	return n
}

// Regression: a background pipeline stopped from the CLI (`strand stop`: clean exit, "pipeline stopped cleanly")
// kept desired=running and the next web start restarted it. Now the web records desired=stopped (reason
// observed_stop, audit desired.observed_stop), does not autostart it and says "stopped outside the web"; a crash
// (error exit, or SIGKILL: no clean line) keeps desired=running; the web's own stops are not mistaken for
// outside stops.
func TestObservedStopOutsideWebIsNotAutostarted(t *testing.T) {
	storeKinds(t, func(t *testing.T, mk func() store.Store) {
		ctx := context.Background()
		st := mk()
		e := dbEnv(t, st, nil, nil)
		c, csrf := e.loginOK()
		if code, body := e.act(c, csrf, "orders", "start", nil); code != 200 {
			t.Fatalf("start: %d %v", code, body)
		}
		// Operator runs `strand stop` while the web is down (or up): clean exit, no pid file.
		t0 := time.Now()
		writePipelineLog(t, e, "orders", slogLine(t0.Add(-time.Minute), "INFO", "pipeline running"), slogLine(t0, "INFO", "pipeline stopped cleanly"))
		e2 := e.restart()
		before := syncCallCount(e2)
		if got := e2.srv.ResumeDesired(ctx); len(got) != 0 || syncCallCount(e2) != before {
			t.Fatalf("pipeline stopped from the CLI was autostarted: %v", got)
		}
		ds, _ := st.DesiredStates(ctx)
		if ds["orders"].Desired != store.DesiredStopped || ds["orders"].Reason != store.ReasonObservedStop {
			t.Fatalf("desired after observed stop = %+v", ds["orders"])
		}
		if a := e2.auditFor("desired.observed_stop"); len(a) != 1 || a[0]["pipeline"] != "orders" {
			t.Fatalf("observed stop audit = %v", a)
		}
		// The UI says so (the process is gone: its admin socket does not answer).
		dial := e2.srv.samp.Dial
		e2.srv.samp.Dial = func(string) AdminPoller { return downPoller{} }
		e2.srv.samp.SampleOnce(ctx)
		c2, csrf2 := e2.loginOK()
		_, body := e2.do(c2, "GET", "/api/v1/pipelines/orders", "", nil)
		if !strings.Contains(body, `"stopped_outside_web": true`) {
			t.Fatalf("API does not report the outside stop: %s", body)
		}
		if _, page := e2.do(c2, "GET", "/pipelines/orders", "", nil); !strings.Contains(page, "stopped outside the web") {
			t.Fatal("detail page does not say the pipeline was stopped outside the web")
		}
		e2.srv.samp.Dial = dial
		// A second restart does not log it again.
		e3 := e2.restart()
		_ = e3.srv.ResumeDesired(ctx)
		if a := e3.auditFor("desired.observed_stop"); len(a) != 1 {
			t.Fatalf("observed stop recorded twice: %d", len(a))
		}
		e2 = e3
		c2, csrf2 = e2.loginOK()

		// Start again from the web (reason cleared), then the process crashes: autostart is still wanted.
		if code, _ := e2.act(c2, csrf2, "orders", "start", nil); code != 200 {
			t.Fatal("restart from the web failed")
		}
		if ds, _ = st.DesiredStates(ctx); ds["orders"].Desired != store.DesiredRunning || ds["orders"].Reason != "" {
			t.Fatalf("desired after web start = %+v", ds["orders"])
		}
		for _, tail := range [][]string{
			{slogLine(time.Now(), "INFO", "pipeline running"), slogLine(time.Now(), "ERROR", "pipeline failed") + ` error="[log_gap] gone"`},
			{slogLine(time.Now(), "INFO", "pipeline running")}, // SIGKILL / OOM: no clean line
		} {
			writePipelineLog(t, e2, "orders", tail...)
			e4 := e2.restart()
			n := syncCallCount(e4)
			if got := e4.srv.ResumeDesired(ctx); len(got) != 1 || syncCallCount(e4) != n+1 {
				t.Fatalf("crashed pipeline not autostarted (%v): %v", tail, got)
			}
			if ds, _ = st.DesiredStates(ctx); ds["orders"].Desired != store.DesiredRunning {
				t.Fatalf("crash changed desired: %+v", ds["orders"])
			}
			e2 = e4
		}

		// The web's own stop (shutdown_stop_pipelines, restart...) is not an outside stop.
		done := e2.srv.beginWebStop("orders")
		writePipelineLog(t, e2, "orders", slogLine(time.Now(), "INFO", "pipeline running"), slogLine(time.Now(), "INFO", "pipeline stopped cleanly"))
		e2.srv.samp.SampleOnce(ctx) // live sampler during the web stop: ignored
		done()
		e2.srv.samp.SampleOnce(ctx)
		e5 := e2.restart()
		n := syncCallCount(e5)
		if got := e5.srv.ResumeDesired(ctx); len(got) != 1 || syncCallCount(e5) != n+1 {
			t.Fatalf("pipeline stopped by the web itself was not autostarted: %v", got)
		}

		// Live: the running web sees the clean stop on its next sample.
		writePipelineLog(t, e5, "orders", slogLine(time.Now(), "INFO", "pipeline running"), slogLine(time.Now().Add(time.Second), "INFO", "pipeline stopped cleanly"))
		e5.srv.samp.SampleOnce(ctx)
		if ds, _ = st.DesiredStates(ctx); ds["orders"].Desired != store.DesiredStopped || ds["orders"].Reason != store.ReasonObservedStop {
			t.Fatalf("live observed stop not recorded: %+v", ds["orders"])
		}
	})
}

func TestParseLogOutcomeCleanStop(t *testing.T) {
	at := time.Date(2026, 11, 19, 8, 0, 0, 123456789, time.UTC)
	for _, c := range []struct {
		name  string
		lines []string
		clean bool
	}{
		{"clean", []string{slogLine(at.Add(-time.Hour), "INFO", "pipeline running"), slogLine(at, "INFO", "pipeline stopped cleanly")}, true},
		{"json", []string{`{"time":"` + at.Format(time.RFC3339Nano) + `","level":"INFO","msg":"pipeline stopped cleanly"}`}, true},
		{"restarted after the clean stop (killed)", []string{slogLine(at, "INFO", "pipeline stopped cleanly"), slogLine(at.Add(time.Minute), "INFO", "pipeline running")}, false},
		{"failed", []string{slogLine(at, "INFO", "pipeline running"), slogLine(at, "ERROR", "pipeline failed")}, false},
	} {
		o := ParseLogOutcome(c.lines)
		if o.Clean != c.clean || (c.clean && !o.CleanAt.Equal(at)) {
			t.Errorf("%s: outcome = %+v", c.name, o)
		}
	}
}
