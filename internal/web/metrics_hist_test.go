package web

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"strand/internal/web/store"
)

// DB mode: sampler points are written in batches, the 1 h ring is reloaded after a web restart, and the
// Graph tab / series API serve 6h..7d ranges from the database (raw + 1-minute rollups).
func TestMetricsHistoryInDatabase(t *testing.T) {
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		e.srv.samp.SampleOnce(ctx)
		e.clock.Advance(5 * time.Second)
	}
	e.srv.flushMetrics()
	if raw, _ := st.MetricsCount(); raw != 3 {
		t.Fatalf("raw samples in DB = %d, want 3", raw)
	}
	// Older history: 30 h ago (rolled up) and 3 h ago (raw).
	now := e.clock.Now().UTC()
	var old []store.MetricSample
	for i := 0; i < 24; i++ {
		old = append(old, store.MetricSample{Pipeline: "orders", T: now.Add(-30*time.Hour + time.Duration(i)*5*time.Second), State: "synced", Lag: 42})
		old = append(old, store.MetricSample{Pipeline: "orders", T: now.Add(-3*time.Hour + time.Duration(i)*5*time.Second), State: "synced", Lag: 7})
	}
	must0(t, st.AppendMetrics(ctx, old))
	e.srv.maintainMetrics()
	raw, rolled := st.MetricsCount()
	if rolled == 0 || raw != 3+24 {
		t.Fatalf("after maintenance raw=%d rollup=%d (want raw 27: the 30 h old samples expire, rollups kept)", raw, rolled)
	}

	e2 := e.restart()
	if n := len(e2.srv.samp.Series("orders")); n != 3 {
		t.Fatalf("ring after restart has %d samples, want 3 from the database", n)
	}
	c, _ := e2.loginOK()
	for rng, want := range map[string]float64{"6h": 7, "7d": 42} {
		resp, body := e2.do(c, "GET", "/api/v1/pipelines/orders/series?range="+rng, "", nil)
		if resp.StatusCode != 200 {
			t.Fatalf("series %s: %d", rng, resp.StatusCode)
		}
		var v SeriesView
		must0(t, json.Unmarshal([]byte(body), &v))
		found := false
		for _, s := range v.Samples {
			if s.Lag == want {
				found = true
			}
		}
		if v.Range != rng || !found {
			t.Fatalf("series %s: %+v (want a point with lag %v)", rng, v, want)
		}
	}
	resp, body := e2.do(c, "GET", "/pipelines/orders?range=24h", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `href="?range=7d"`) || !strings.Contains(body, `aria-current="true">24 h`) {
		t.Fatalf("graph tab range selector missing: %d", resp.StatusCode)
	}
}

// File mode: only the in-memory hour; the API says so for longer ranges.
func TestMetricsHistoryFileMode(t *testing.T) {
	e := newEnv(t, nil)
	c, _ := e.loginOK()
	resp, body := e.do(c, "GET", "/api/v1/pipelines/orders/series?range=7d", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `"range": "1h"`) || !strings.Contains(body, "PostgreSQL") {
		t.Fatalf("file mode 7d: %d %s", resp.StatusCode, body)
	}
	_, page := e.do(c, "GET", "/pipelines/orders", "", nil)
	if strings.Contains(page, `href="?range=7d"`) {
		t.Fatal("file mode offers ranges it cannot serve")
	}
}
