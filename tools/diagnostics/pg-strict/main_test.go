package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestScrapeMetrics(t *testing.T) {
	mockBody := `# HELP strand_apply_txn_total Number of applied transactions
# TYPE strand_apply_txn_total counter
strand_apply_txn_total{pipeline="pg-strict-diag"} 1450
# HELP strand_apply_rows_total Number of applied rows
# TYPE strand_apply_rows_total counter
strand_apply_rows_total{pipeline="pg-strict-diag"} 1450
# HELP strand_lag_seconds Replication lag in seconds
# TYPE strand_lag_seconds gauge
strand_lag_seconds{pipeline="pg-strict-diag"} 0.042
`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, mockBody)
	}))
	defer ts.Close()

	appliedTxns, appliedRows, lag := scrapeMetrics(ts.URL)
	if appliedTxns != 1450 {
		t.Errorf("expected 1450 txns, got %d", appliedTxns)
	}
	if appliedRows != 1450 {
		t.Errorf("expected 1450 rows, got %d", appliedRows)
	}
	if lag < 0.041 || lag > 0.043 {
		t.Errorf("expected ~0.042 lag, got %f", lag)
	}
}

func TestSortWaitStats(t *testing.T) {
	store := map[string]*WaitEventStat{
		"IO:WALSync:active":      {Type: "IO", Event: "WALSync", State: "active", Count: 120},
		"CPU:running:active":     {Type: "CPU", Event: "running", State: "active", Count: 45},
		"Client:ClientRead:idle": {Type: "Client", Event: "ClientRead", State: "idle", Count: 250},
	}
	sorted := sortWaitStats(store)
	if len(sorted) != 3 {
		t.Fatalf("expected 3 items, got %d", len(sorted))
	}
	if sorted[0].Event != "ClientRead" || sorted[0].Count != 250 {
		t.Errorf("expected ClientRead first with 250 count, got %s %d", sorted[0].Event, sorted[0].Count)
	}
	if sorted[1].Event != "WALSync" || sorted[1].Count != 120 {
		t.Errorf("expected WALSync second with 120 count, got %s %d", sorted[1].Event, sorted[1].Count)
	}
	if sorted[2].Event != "running" || sorted[2].Count != 45 {
		t.Errorf("expected running third with 45 count, got %s %d", sorted[2].Event, sorted[2].Count)
	}
}
