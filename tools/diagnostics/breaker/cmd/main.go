package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"strand/tools/diagnostics/breaker"
)

func main() {
	var (
		dsnFlag        = flag.String("dsn", "", "Database DSN (e.g. user:password@tcp(127.0.0.1:3306)/). If empty, checks STRAND_DIAG_DSN env var.")
		telemetryFlag  = flag.String("telemetry", "", "Path to telemetry CSV file to analyze")
		windowFlag     = flag.Int("window", 10, "Sliding window size for p2p analysis")
		outFlag        = flag.String("out", "", "Output path for diagnostic snapshot or analysis json (0600 permissions)")
		pollInterval   = flag.Duration("interval", 1*time.Second, "Polling interval when capturing counter deltas")
		samplesCount   = flag.Int("samples", 1, "Number of status snapshots to collect (>=2 computes delta)")
		alignTelemetry = flag.String("align-csv", "", "Optional telemetry CSV to align with DB snapshot")
	)
	flag.Parse()

	// 1. Telemetry analysis mode (p2p calculation, direct vs latency cause)
	if *telemetryFlag != "" {
		fmt.Printf("[INFO] Loading telemetry from %s\n", *telemetryFlag)
		rows, err := breaker.LoadTelemetryCSV(*telemetryFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Failed to load telemetry: %v\n", err)
			os.Exit(1)
		}
		result, err := breaker.AnalyzeP2P(rows, *windowFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Analysis failed: %v\n", err)
			os.Exit(1)
		}

		outJSON, _ := json.MarshalIndent(result, "", "  ")
		fmt.Println(string(outJSON))

		if *outFlag != "" {
			if err := breaker.SaveProtectedArtifact(outJSON, *outFlag); err != nil {
				fmt.Fprintf(os.Stderr, "[ERROR] Failed to write output: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("[INFO] Saved analysis to %s (permissions 0600)\n", *outFlag)
		}
		return
	}

	// 2. Diagnostic Collector mode
	dsn := *dsnFlag
	if dsn == "" {
		dsn = os.Getenv("STRAND_DIAG_DSN")
	}

	if dsn == "" {
		fmt.Fprintf(os.Stderr, "[ERROR] No DSN provided. Explicit DSN is required via -dsn or STRAND_DIAG_DSN environment variable.\n")
		fmt.Fprintf(os.Stderr, "Strand breaker diagnostics will NEVER auto-connect to arbitrary databases.\n")
		fmt.Fprintf(os.Stderr, "Usage example:\n  STRAND_DIAG_DSN='user:pass@tcp(127.0.0.1:3306)/' %s -samples 2 -interval 2s -out diag.json\n", os.Args[0])
		os.Exit(1)
	}

	fmt.Printf("[INFO] Connecting with explicit DSN: %s\n", breaker.RedactDSN(dsn))
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Ping failed: %v\n", err)
		os.Exit(1)
	}

	collector := breaker.NewDiagnosticCollector(db, dsn)

	var lastSnap *breaker.DiagnosticSnapshot
	count := *samplesCount
	if count < 1 {
		count = 1
	}

	for i := 0; i < count; i++ {
		if i > 0 {
			time.Sleep(*pollInterval)
		}
		snap, err := collector.Collect(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Collection failed at sample %d: %v\n", i+1, err)
			os.Exit(1)
		}
		lastSnap = snap
		fmt.Printf("[INFO] Collected sample %d/%d at %s (Version: %s)\n",
			i+1, count, snap.CollectedAt.Format(time.RFC3339), snap.ServerVersion)
		if snap.StatusDelta != nil {
			fmt.Printf("       ΔInnodb_log_waits: %d, ΔInnodb_buffer_pool_wait_free: %d, ΔInnodb_os_log_fsyncs: %d\n",
				snap.StatusDelta.DeltaInnodbLogWaits,
				snap.StatusDelta.DeltaBufferPoolWaitFree,
				snap.StatusDelta.DeltaInnodbOsLogFsyncs,
			)
		}
	}

	// Align with telemetry if provided
	if *alignTelemetry != "" {
		rows, err := breaker.LoadTelemetryCSV(*alignTelemetry)
		if err == nil && len(rows) > 0 {
			// Find row closest in time
			var closest *breaker.TelemetryRow
			var minDiff time.Duration = 1000 * time.Hour
			for idx := range rows {
				diff := lastSnap.CollectedAt.Sub(rows[idx].ParsedTime)
				if diff < 0 {
					diff = -diff
				}
				if diff < minDiff {
					minDiff = diff
					closest = &rows[idx]
				}
			}
			lastSnap.AlignedTelemetry = closest
		}
	}

	outJSON, _ := json.MarshalIndent(lastSnap, "", "  ")
	if *outFlag != "" {
		if err := breaker.SaveProtectedSnapshot(lastSnap, *outFlag); err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Failed to save snapshot to %s: %v\n", *outFlag, err)
			os.Exit(1)
		}
		fmt.Printf("[INFO] Snapshot saved to %s (permissions 0600)\n", *outFlag)
	} else {
		fmt.Println(string(outJSON))
	}
}
