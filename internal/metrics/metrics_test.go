package metrics_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"strand/internal/metrics"
)

// expectedMetrics là danh sách 20 metrics quy định tại spec/02 §11.
var expectedMetrics = []string{
	"strand_lag_seconds",
	"strand_apply_txn_total",
	"strand_apply_rows_total",
	"strand_holes",
	"strand_checkpoint_compaction_lag_seconds",
	"strand_writeset_entries",
	"strand_pending_cross_range",
	"strand_pending_cross_range_oldest_seconds",
	"strand_governor_rate_tokens",
	"strand_governor_zone",
	"strand_governor_pressure",
	"strand_sensitivity_unknown",
	"strand_breaker_open",
	"strand_spool_bytes",
	"strand_snapshot_ranges",
	"strand_seed_progress_ratio",
	"strand_retention_headroom_seconds",
	"strand_verify_mismatch_total",
	"strand_verify_unverifiable_hot_total",
	"strand_fenced_total",
}

// TestMT1_AllMetricsRegisteredAndScraped kiểm tra AC1:
// Đầy đủ 20 metrics trong S02 §11, tên đúng, xuất hiện trên endpoint /metrics.
func TestMT1_AllMetricsRegisteredAndScraped(t *testing.T) {
	m := metrics.NewMetrics(nil)
	pipe := "orders-sync"

	// Cập nhật giá trị cho từng metric để chúng xuất hiện trong output scrape
	m.LagSeconds.WithLabelValues(pipe, "synced").Set(0.5)
	m.ApplyTxnTotal.WithLabelValues(pipe).Add(10)
	m.ApplyRowsTotal.WithLabelValues(pipe).Add(500)
	m.Holes.WithLabelValues(pipe).Set(0)
	m.CheckpointCompactionLagSeconds.WithLabelValues(pipe).Set(0.2)
	m.WritesetEntries.WithLabelValues(pipe, "orders").Set(120)
	m.PendingCrossRange.WithLabelValues(pipe).Set(0)
	m.PendingCrossRangeOldestSeconds.WithLabelValues(pipe).Set(0)
	m.GovernorRateTokens.WithLabelValues(pipe, "source_iops").Set(600)
	m.GovernorZone.WithLabelValues(pipe).Set(0)
	m.GovernorPressure.WithLabelValues(pipe, "replica_lag").Set(0.1)
	m.SensitivityUnknown.WithLabelValues(pipe).Set(0)
	m.BreakerOpen.WithLabelValues(pipe).Set(0)
	m.SpoolBytes.WithLabelValues(pipe, "local").Set(1024 * 1024)
	m.SnapshotRanges.WithLabelValues(pipe, "orders", "done").Set(10)
	m.SeedProgressRatio.WithLabelValues(pipe, "orders").Set(1.0)
	m.RetentionHeadroomSeconds.WithLabelValues(pipe).Set(7200)
	m.VerifyMismatchTotal.WithLabelValues(pipe, "orders").Add(0)
	m.VerifyUnverifiableHotTotal.WithLabelValues(pipe, "orders").Add(0)
	m.FencedTotal.WithLabelValues(pipe).Add(0)

	health := metrics.NewHealthChecker(10 * time.Second)
	srv := metrics.NewServer("127.0.0.1:0", m, health, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("start metrics server failed: %v", err)
	}
	defer srv.Close()

	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", srv.Addr()))
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics status %d (mong muốn 200)", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /metrics body failed: %v", err)
	}
	body := string(bodyBytes)

	// Kiểm tra sự hiện diện của toàn bộ 20 metric
	for _, metricName := range expectedMetrics {
		if !strings.Contains(body, metricName) {
			t.Errorf("thiếu metric trong /metrics: %s", metricName)
		}
	}
}

// TestMT2_HealthzReturns200WhenProgressing kiểm tra AC2:
// /healthz trả 200 khi vòng lặp chính còn tiến, và trả 503 khi kẹt hoặc lỗi.
func TestMT2_HealthzReturns200WhenProgressing(t *testing.T) {
	stallTimeout := 100 * time.Millisecond
	health := metrics.NewHealthChecker(stallTimeout)
	m := metrics.NewMetrics(nil)

	srv := metrics.NewServer("127.0.0.1:0", m, health, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("start server failed: %v", err)
	}
	defer srv.Close()

	url := fmt.Sprintf("http://%s/healthz", srv.Addr())

	// 1. Khi vòng lặp chính vừa tiến -> 200 OK
	health.RecordProgress()
	resp1, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("mong muốn status 200 khi còn tiến, nhận được: %d", resp1.StatusCode)
	}

	var data1 map[string]interface{}
	if err := json.NewDecoder(resp1.Body).Decode(&data1); err != nil || data1["status"] != "ok" {
		t.Fatalf("body healthz không hợp lệ: %v", data1)
	}

	// 2. Chờ vượt quá stallTimeout (vòng lặp chính bị kẹt) -> 503 Service Unavailable
	time.Sleep(150 * time.Millisecond)
	resp2, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("mong muốn status 503 khi bị kẹt tiến độ, nhận được: %d", resp2.StatusCode)
	}

	// 3. Vòng lặp chính tiếp tục chạy (RecordProgress) -> phục hồi về 200 OK
	health.RecordProgress()
	resp3, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("mong muốn status 200 sau khi phục hồi, nhận được: %d", resp3.StatusCode)
	}

	// 4. Khi gặp lỗi nghiêm trọng (SetFailed) -> 503 Service Unavailable
	health.SetFailed("database connection permanently lost")
	resp4, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET /healthz failed: %v", err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("mong muốn status 503 khi failed, nhận được: %d", resp4.StatusCode)
	}
}
