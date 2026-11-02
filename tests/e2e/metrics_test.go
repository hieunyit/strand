package e2e

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	a := l.Addr().String()
	l.Close()
	return a
}

// P1-19: đủ metric của 02 §11 (tên đúng, có label pipeline), /healthz trả 200 khi pipeline chạy.
func TestP1_E2E_MetricsContractAndHealthz(t *testing.T) {
	src, dst := newPair(t)
	wl := newWorkload(src.DB, 1)
	wl.Preload(t, 2000)
	wl.Start(2, 2000)
	defer wl.Stop()
	cfg := strandCfg{Name: "e2e-metrics", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t), MetricsAddr: freeAddr(t)}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)
	time.Sleep(3 * time.Second)

	resp, err := http.Get("http://" + cfg.MetricsAddr + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("/healthz: %v %v", err, resp)
	}
	resp.Body.Close()
	resp, err = http.Get("http://" + cfg.MetricsAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	text := string(body)
	for _, name := range []string{
		"strand_lag_seconds", "strand_apply_txn_total", "strand_apply_rows_total", "strand_holes",
		"strand_checkpoint_compaction_lag_seconds", "strand_writeset_entries", "strand_pending_cross_range",
		"strand_pending_cross_range_oldest_seconds", "strand_governor_rate_tokens", "strand_governor_zone",
		"strand_governor_pressure", "strand_sensitivity_unknown", "strand_breaker_open", "strand_spool_bytes",
		"strand_snapshot_ranges", "strand_seed_progress_ratio", "strand_retention_headroom_seconds",
		"strand_verify_mismatch_total", "strand_verify_unverifiable_hot_total", "strand_fenced_total",
		"strand_spool_capacity_bytes",
	} {
		if !strings.Contains(text, name+"{") || !strings.Contains(text, `pipeline="e2e-metrics"`) {
			t.Errorf("metric %s missing or without pipeline label", name)
		}
	}
	if !strings.Contains(text, `strand_apply_txn_total{pipeline="e2e-metrics"}`) {
		t.Errorf("apply counter not exported")
	}
	p.stop(t)
}
