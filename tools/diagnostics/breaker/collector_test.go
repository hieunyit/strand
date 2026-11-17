package breaker

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestDSNRedaction(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "mysql://admin:SuperSecret123@127.0.0.1:3306/strand",
			expected: "mysql://admin:***@127.0.0.1:3306/strand",
		},
		{
			input:    "root:my-db-pass@tcp(localhost:3306)/test_db?parseTime=true",
			expected: "root:***@tcp(localhost:3306)/test_db?parseTime=true",
		},
		{
			input:    "",
			expected: "",
		},
	}

	for _, c := range cases {
		got := RedactDSN(c.input)
		if got != c.expected {
			t.Errorf("RedactDSN(%q) = %q; want %q", c.input, got, c.expected)
		}
		if strings.Contains(got, "SuperSecret123") || strings.Contains(got, "my-db-pass") {
			t.Errorf("Redacted DSN leaked secret password: %s", got)
		}
	}
}

func TestSanitizeEngineStatus(t *testing.T) {
	fixture := `=====================================
2026-11-17 04:12:00 0x7f8a1234 INNODB MONITOR OUTPUT
=====================================
Per second averages calculated from the last 15 seconds
-----------------
BACKGROUND THREAD
-----------------
srv_master_thread loops: 100 srv_active
----------
SEMAPHORES
----------
OS WAIT ARRAY INFO: reservation count 42
--Thread 14023134 has waited at btr0cur.cc line 560 for 0.00 seconds the semaphore:
Mutex at 0x7f8a1234, handle 23, lock_word 0
------------
TRANSACTIONS
------------
Trx id counter 109283
Purge done for trx's n:o < 109280 undo n:o < 0 state: running
LIST OF TRANSACTIONS FOR EACH SESSION:
---TRANSACTION 109282, ACTIVE 5 sec inserting
mysql tables in use 1, locked 1
LOCK WAIT 2 lock struct(s), heap size 1136, 1 row lock(s)
MySQL thread id 14, OS thread handle 14023134, query id 492 localhost root update
INSERT INTO sensitive_customer_data (id, card_number, secret_key) VALUES (1, '4111-2222-3333-4444', 'token_secret_xyz')
--------
FILE I/O
--------
I/O thread 0 state: waiting for completed aio requests (insert buffer thread)
0 pending normal aio reads, 0 pending aio writes
1 pending log flushes (fsync), 0 pending chkp writes
---
LOG
---
Log sequence number          104857600
Log flushed up to            104850000
Pages flushed up to          104840000
Last checkpoint at           104830000
0 pending log flushes, 0 pending chkp writes
`

	parsed := ParseAndSanitizeEngineStatus(fixture, time.Now().UTC())

	if parsed.LogSequenceNumber != 104857600 {
		t.Errorf("Expected LogSequenceNumber 104857600, got %d", parsed.LogSequenceNumber)
	}
	if parsed.LogFlushedUpTo != 104850000 {
		t.Errorf("Expected LogFlushedUpTo 104850000, got %d", parsed.LogFlushedUpTo)
	}
	if parsed.PagesFlushedUpTo != 104840000 {
		t.Errorf("Expected PagesFlushedUpTo 104840000, got %d", parsed.PagesFlushedUpTo)
	}
	if parsed.LastCheckpointAt != 104830000 {
		t.Errorf("Expected LastCheckpointAt 104830000, got %d", parsed.LastCheckpointAt)
	}
	if parsed.LogPendingFsyncs != 1 {
		t.Errorf("Expected LogPendingFsyncs 1, got %d", parsed.LogPendingFsyncs)
	}

	// Verify sensitive query and secret tokens are sanitized
	if strings.Contains(parsed.RawSanitizedText, "4111-2222-3333-4444") {
		t.Errorf("Sanitized engine status leaked card number: %s", parsed.RawSanitizedText)
	}
	if strings.Contains(parsed.RawSanitizedText, "token_secret_xyz") {
		t.Errorf("Sanitized engine status leaked secret token: %s", parsed.RawSanitizedText)
	}
	if strings.Contains(parsed.RawSanitizedText, "INSERT INTO sensitive_customer_data") {
		t.Errorf("Sanitized engine status leaked raw SQL statement: %s", parsed.RawSanitizedText)
	}
}

func TestCounterDeltaAndReset(t *testing.T) {
	t0 := time.Now().UTC()
	t1 := t0.Add(5 * time.Second)

	// Normal progression
	prev := GlobalCounters{
		Timestamp:                t0,
		InnodbLogWaits:           10,
		InnodbBufferPoolWaitFree: 5,
		InnodbDataFsyncs:         100,
		InnodbOsLogFsyncs:        80,
		InnodbOsLogWritten:       5000,
		InnodbDataWrites:         200,
		HandlerCommit:            150,
	}

	curr := GlobalCounters{
		Timestamp:                t1,
		InnodbLogWaits:           15,
		InnodbBufferPoolWaitFree: 5,
		InnodbDataFsyncs:         120,
		InnodbOsLogFsyncs:        95,
		InnodbOsLogWritten:       7500,
		InnodbDataWrites:         250,
		HandlerCommit:            190,
	}

	delta := CalculateCounterDelta(prev, curr)
	if delta.ResetDetected {
		t.Errorf("Expected no reset, got ResetDetected=true")
	}
	if delta.DeltaInnodbLogWaits != 5 {
		t.Errorf("Expected DeltaInnodbLogWaits 5, got %d", delta.DeltaInnodbLogWaits)
	}
	if delta.DeltaInnodbOsLogWritten != 2500 {
		t.Errorf("Expected DeltaInnodbOsLogWritten 2500, got %d", delta.DeltaInnodbOsLogWritten)
	}

	// Counter Reset (e.g. server restart or FLUSH STATUS)
	t2 := t1.Add(5 * time.Second)
	restartedCurr := GlobalCounters{
		Timestamp:                t2,
		InnodbLogWaits:           2, // Drop from 15 to 2
		InnodbBufferPoolWaitFree: 0,
		InnodbDataFsyncs:         10,
		InnodbOsLogFsyncs:        8,
		InnodbOsLogWritten:       500,
		InnodbDataWrites:         20,
		HandlerCommit:            15,
	}

	resetDelta := CalculateCounterDelta(curr, restartedCurr)
	if !resetDelta.ResetDetected {
		t.Errorf("Expected reset detected on counter drop")
	}
	if resetDelta.DeltaInnodbLogWaits != 2 {
		t.Errorf("Expected reset delta to use current value 2, got %d", resetDelta.DeltaInnodbLogWaits)
	}
}

func TestTelemetryCSVParserAndAlignment(t *testing.T) {
	csvData := `t_unix_ns,ok,R,zone,pressure,breaker,lag_s,p99_ms,baseline_ms,cpu,iowait
1794829237762956353,1,400.0,0,0.0194,0,0.3079,12.5,12.0,25.4,1.2
1794829300000000000,1,350.0,0,0.0500,0,0.3080,15.0,12.0,30.0,2.0
1794829387762956861,1,144.5,2,1.2182,1,0.3079,42.0,12.0,85.2,15.8
`

	parser := NewTelemetryParser()
	rows, err := parser.ParseCSV(strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(rows))
	}

	// Verify row 0 (max R)
	if rows[0].R != 400.0 {
		t.Errorf("Row 0 R = %f, want 400.0", rows[0].R)
	}
	if rows[0].Breaker != 0 {
		t.Errorf("Row 0 Breaker = %d, want 0", rows[0].Breaker)
	}

	// Verify row 2 (min R with breaker)
	if rows[2].R != 144.5 {
		t.Errorf("Row 2 R = %f, want 144.5", rows[2].R)
	}
	if rows[2].Breaker != 1 {
		t.Errorf("Row 2 Breaker = %d, want 1", rows[2].Breaker)
	}
	if rows[2].Ratio != 3.5 { // 42.0 / 12.0 = 3.5
		t.Errorf("Row 2 Ratio = %f, want 3.5", rows[2].Ratio)
	}
	if rows[2].CPUPct == nil || *rows[2].CPUPct != 85.2 {
		t.Errorf("Row 2 CPU mismatch")
	}
	if rows[2].IOWaitPct == nil || *rows[2].IOWaitPct != 15.8 {
		t.Errorf("Row 2 IOWait mismatch")
	}

	// Run P2P Analysis
	result, err := AnalyzeP2P(rows, 3)
	if err != nil {
		t.Fatalf("AnalyzeP2P failed: %v", err)
	}

	expectedRatio := math.Log(400.0 / 144.5)
	if math.Abs(result.WorstLogRatio-expectedRatio) > 1e-6 {
		t.Errorf("WorstLogRatio = %f, want %f", result.WorstLogRatio, expectedRatio)
	}
	if !strings.Contains(result.DirectCause, "Circuit breaker opened") {
		t.Errorf("Expected direct cause to identify circuit breaker trip: %s", result.DirectCause)
	}
	if result.LatencyCauseStatus != "UNRESOLVED" {
		t.Errorf("Expected LatencyCauseStatus UNRESOLVED without DB incident logs, got %s", result.LatencyCauseStatus)
	}
}

func TestTimezoneAndMissingMetricsHandling(t *testing.T) {
	// CSV with RFC3339 timestamps in different timezones (+07:00, UTC, -05:00) and missing metric columns
	csvData := `timestamp,throughput,breaker,zone
2026-11-16T18:40:37+07:00,400.0,0,0
2026-11-16T11:42:00Z,300.0,0,1
2026-11-16T06:43:07-05:00,144.5,1,2
`
	parser := NewTelemetryParser()
	rows, err := parser.ParseCSV(strings.NewReader(csvData))
	if err != nil {
		t.Fatalf("ParseCSV failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("Expected 3 rows, got %d", len(rows))
	}

	// Verify timestamps parsed into UTC correctly
	if rows[0].ParsedTime.UTC().Hour() != 11 || rows[0].ParsedTime.UTC().Minute() != 40 {
		t.Errorf("Timestamp 0 UTC conversion incorrect: %v", rows[0].ParsedTime)
	}
	if rows[2].ParsedTime.UTC().Hour() != 11 || rows[2].ParsedTime.UTC().Minute() != 43 {
		t.Errorf("Timestamp 2 UTC conversion incorrect: %v", rows[2].ParsedTime)
	}

	// Missing fields (CPU, IO, P99) did not break parsing
	if rows[0].CPUPct != nil {
		t.Errorf("Expected CPUPct nil for row 0")
	}

	result, err := AnalyzeP2P(rows, 3)
	if err != nil {
		t.Fatalf("AnalyzeP2P failed: %v", err)
	}
	if math.Abs(result.WorstLogRatio-1.018185) > 1e-3 {
		t.Errorf("Expected p2p ratio ~1.018, got %f", result.WorstLogRatio)
	}
}
