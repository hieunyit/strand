// Package main implements the pg-strict diagnostic harness and benchmark runner.
// It sets up isolated source/target PostgreSQL instances, applies fixed schemas and deterministic seeds,
// measures raw single-connection commit throughput, runs a concurrent OLTP workload on the source,
// invokes `strand sync` in strict mode with CPU and block profiling, and captures pg_stat_activity waits,
// rows per source transaction, optional batch traces, and applied throughput.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	SourceDSN                  string
	TargetDSN                  string
	AutoDocker                 bool
	AllowDestructiveExistingDB bool
	SrcPort                    int
	DstPort                    int
	CPUQuota                   string
	Duration                   time.Duration
	Warmup                     time.Duration
	Clients                    int
	Rows                       int
	Seed                       int64
	OutputDir                  string
	TxnTrace                   bool
	StrandBin                  string
	MetricsPort                int
	BatchMaxWait               string
}

type MetricSample struct {
	Timestamp      time.Time `json:"timestamp"`
	Phase          string    `json:"phase"`
	AppliedTxns    int64     `json:"applied_txns"`
	AppliedRows    int64     `json:"applied_rows"`
	SourceCommits  int64     `json:"source_commits"`
	LagSeconds     float64   `json:"lag_seconds"`
	ApplyTxnRate   float64   `json:"apply_txn_rate"`
	ApplyRowRate   float64   `json:"apply_row_rate"`
	SourceXactRate float64   `json:"source_xact_rate"`
	RowsPerTxn     float64   `json:"rows_per_source_txn"`
}

type WaitEventStat struct {
	Type  string `json:"type"`
	Event string `json:"event"`
	State string `json:"state"`
	Count int64  `json:"count"`
}

type BenchmarkReport struct {
	StartTime             time.Time         `json:"start_time"`
	EndTime               time.Time         `json:"end_time"`
	DurationSec           float64           `json:"duration_sec"`
	WarmupSec             float64           `json:"warmup_sec"`
	SteadySec             float64           `json:"steady_sec"`
	Clients               int               `json:"clients"`
	SeedRows              int               `json:"seed_rows"`
	Seed                  int64             `json:"seed"`
	Reproduces280to300    bool              `json:"reproduces_280_to_300"`
	RawTargetCommitTPS    float64           `json:"raw_target_commit_tps"`
	ApplyToRawCommitRatio float64           `json:"apply_txns_to_raw_commits_ratio"`
	WarmupTPS             float64           `json:"warmup_tps"`
	SteadyTPS             float64           `json:"steady_tps"`
	SteadyRowRate         float64           `json:"steady_row_rate"`
	SourceCommitRate      float64           `json:"source_commit_rate"`
	RowsPerSourceTxn      float64           `json:"rows_per_source_txn"`
	TargetTopWaits        []WaitEventStat   `json:"target_top_waits"`
	SourceTopWaits        []WaitEventStat   `json:"source_top_waits"`
	Versions              map[string]string `json:"versions"`
	FsyncSettings         map[string]string `json:"fsync_settings"`
	Observations          []string          `json:"observations"`
	ArtifactFiles         map[string]string `json:"artifact_files"`
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.SourceDSN, "source", "", "Source PostgreSQL DSN (if empty and auto-docker is enabled, spins up docker container)")
	flag.StringVar(&cfg.TargetDSN, "target", "", "Target PostgreSQL DSN (if empty and auto-docker is enabled, spins up docker container)")
	flag.BoolVar(&cfg.AutoDocker, "auto-docker", true, "Manage isolated docker containers for source and target")
	flag.BoolVar(&cfg.AllowDestructiveExistingDB, "allow-destructive-existing-databases", false, "DANGER: allow truncating bench.accounts and replacing the diagnostic publication in external source/target databases")
	flag.IntVar(&cfg.SrcPort, "src-port", 0, "Source loopback host port for auto-docker (0: Docker assigns a free port)")
	flag.IntVar(&cfg.DstPort, "dst-port", 0, "Target loopback host port for auto-docker (0: Docker assigns a free port)")
	flag.StringVar(&cfg.CPUQuota, "cpus", "2.0", "Docker container CPU limit")
	flag.DurationVar(&cfg.Duration, "duration", 15*time.Second, "Steady measurement window duration")
	flag.DurationVar(&cfg.Warmup, "warmup", 5*time.Second, "Warmup window duration")
	flag.IntVar(&cfg.Clients, "clients", 4, "Number of concurrent workload worker clients")
	flag.IntVar(&cfg.Rows, "rows", 5000, "Number of seed rows in bench.accounts")
	flag.Int64Var(&cfg.Seed, "seed", 42, "Random seed for data and workload")
	flag.StringVar(&cfg.OutputDir, "out-dir", "reports/code-review-20261117/followup-agy/evidence/pg-strict", "Directory for output artifacts")
	flag.BoolVar(&cfg.TxnTrace, "txntrace", false, "Enable STRAND_TXN_TRACE (note instrumentation overhead)")
	flag.StringVar(&cfg.StrandBin, "strand-bin", "", "Path to strand binary (if empty, builds ./cmd/strand)")
	flag.IntVar(&cfg.MetricsPort, "metrics-port", 0, "Port for strand metrics server (0: OS assigns a free port)")
	flag.StringVar(&cfg.BatchMaxWait, "batch-max-wait", "0s", "target.batch.max_wait duration string (default 0s)")
	flag.Parse()

	if err := runBenchmark(cfg); err != nil {
		log.Fatalf("Benchmark error: %v", err)
	}
}

func runBenchmark(cfg Config) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := validateConfig(cfg); err != nil {
		return err
	}
	if err := privateDir(cfg.OutputDir); err != nil {
		return fmt.Errorf("create private out-dir: %w", err)
	}
	unlock, err := lockOutputDir(cfg.OutputDir)
	if err != nil {
		return err
	}
	defer unlock()
	// Configuration was moved outside the artifact directory; use an absolute
	// spool path so its location does not depend on the temporary config path.
	cfg.OutputDir, err = filepath.Abs(cfg.OutputDir)
	if err != nil {
		return err
	}
	if cfg.AutoDocker {
		// Retain only IDs returned by successful creates. A failed second create
		// still cleans up the first; no pre-existing container is ever removed.
		var owned []string
		defer func() { cleanupContainers(owned, dockerCommand) }()
		var err error
		var id string
		id, cfg.SrcPort, err = startContainer("source", cfg.SrcPort, cfg.CPUQuota, dockerCommand)
		if id != "" {
			owned = append(owned, id)
		}
		if err != nil {
			return err
		}
		id, cfg.DstPort, err = startContainer("target", cfg.DstPort, cfg.CPUQuota, dockerCommand)
		if id != "" {
			owned = append(owned, id)
		}
		if err != nil {
			return err
		}
		cfg.SourceDSN = fmt.Sprintf("postgres://postgres:dev@127.0.0.1:%d/strand?sslmode=disable", cfg.SrcPort)
		cfg.TargetDSN = fmt.Sprintf("postgres://postgres:dev@127.0.0.1:%d/strand?sslmode=disable", cfg.DstPort)
	}

	log.Printf("Waiting for source and target PostgreSQL to become ready...")
	srcDB, err := waitForDB(ctx, cfg.SourceDSN, 30*time.Second)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer srcDB.Close()

	dstDB, err := waitForDB(ctx, cfg.TargetDSN, 30*time.Second)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer dstDB.Close()

	log.Printf("Capturing database settings and versions...")
	fsyncSettings := make(map[string]string)
	for _, setting := range []string{"fsync", "synchronous_commit", "wal_level", "shared_buffers", "max_wal_size"} {
		var val string
		if err := dstDB.QueryRowContext(ctx, "SELECT setting FROM pg_settings WHERE name = $1", setting).Scan(&val); err == nil {
			fsyncSettings[setting] = val
		}
	}

	versions := map[string]string{
		"go_version":  runtime.Version(),
		"os_arch":     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		"strand_tool": "pg-strict-diag",
		"docker_cpu":  cfg.CPUQuota,
	}
	var pgVersion string
	if err := dstDB.QueryRowContext(ctx, "SELECT version()").Scan(&pgVersion); err == nil {
		versions["postgres_target"] = pgVersion
	}
	if err := srcDB.QueryRowContext(ctx, "SELECT version()").Scan(&pgVersion); err == nil {
		versions["postgres_source"] = pgVersion
	}

	log.Printf("Initializing schemas and deterministic seed data (seed=%d, rows=%d)...", cfg.Seed, cfg.Rows)
	if err := setupDatabases(ctx, srcDB, dstDB, cfg.Seed, cfg.Rows); err != nil {
		return fmt.Errorf("setup databases: %w", err)
	}

	// Measure Raw Target Single-Connection Commit TPS
	log.Printf("Measuring separate single-connection update/commit workload on target PostgreSQL...")
	rawCommitTPS := measureRawCommitTPS(ctx, dstDB, 3*time.Second)
	log.Printf("Raw single-connection commit TPS on target: %.1f TPS", rawCommitTPS)

	// Prepare Strand binary
	strandBin := cfg.StrandBin
	if strandBin == "" {
		tmpBin := filepath.Join(cfg.OutputDir, "strand_bin")
		log.Printf("Building strand binary to %s...", tmpBin)
		bcmd := exec.Command("/usr/local/go/bin/go", "build", "-o", tmpBin, "./cmd/strand")
		if out, err := bcmd.CombinedOutput(); err != nil {
			return fmt.Errorf("build strand: %v: %s", err, string(out))
		}
		strandBin = tmpBin
	}

	// Create strand config
	privateConfigDir, err := os.MkdirTemp("", "strand-pg-strict-config-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(privateConfigDir)
	configPath := filepath.Join(privateConfigDir, "strand.yaml")
	spoolDir := filepath.Join(cfg.OutputDir, "spool")
	if err := privateDir(spoolDir); err != nil {
		return err
	}

	batchSection := ""
	if cfg.BatchMaxWait != "" && cfg.BatchMaxWait != "0s" {
		batchSection = fmt.Sprintf("  batch:\n    max_wait: %s\n    max_rows: 2000\n", cfg.BatchMaxWait)
	}

	configContent := fmt.Sprintf(`version: 1
name: pg-strict-diag
source:
  kind: postgres
  dsn: %q
  slot: strand_strict_diag_slot
  publication: strand_strict_diag_pub
  create_slot: true
target:
  kind: postgres
  dsn: %q
  commit_order: strict
%stables:
  include: ["bench.*"]
bootstrap:
  default: logical
checkpoint:
  store: target
  flush_every: 200ms
resources:
  spool:
    path: %q
    hard: 2GiB
observability:
  metrics: "127.0.0.1:%d"
  log:
    level: info
    format: json
`, cfg.SourceDSN, cfg.TargetDSN, batchSection, spoolDir, cfg.MetricsPort)

	if err := writePrivateFile(configPath, []byte(configContent)); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	redact := newArtifactRedactor(cfg.SourceDSN, cfg.TargetDSN)
	redactedConfigPath := filepath.Join(cfg.OutputDir, "strand.redacted.yaml")
	if err := writePrivateFile(redactedConfigPath, []byte(redact(configContent))); err != nil {
		return err
	}

	cpuProfilePath := filepath.Join(cfg.OutputDir, "cpu.pprof")
	blockProfilePath := filepath.Join(cfg.OutputDir, "block.pprof")
	strandLogPath := filepath.Join(cfg.OutputDir, "strand.log")
	strandLogFile, err := os.OpenFile(strandLogPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create strand log: %w", err)
	}
	defer strandLogFile.Close()

	log.Printf("Starting `strand sync` in strict mode with CPU and block profiling enabled...")
	syncArgs := []string{
		"sync",
		"-c", configPath,
		"--cpu-profile=" + cpuProfilePath,
		"--block-profile=" + blockProfilePath,
		"--profile-overwrite",
	}

	syncCmd := exec.Command(strandBin, syncArgs...)
	metricsReady := make(chan string, 1)
	childLog := &redactingWriter{dst: strandLogFile, redact: redact, onLines: func(lines string) {
		if addr := childMetricsAddress(lines); addr != "" {
			select {
			case metricsReady <- addr:
			default:
			}
		}
	}}
	defer childLog.Flush()
	syncCmd.Stdout = childLog
	syncCmd.Stderr = childLog
	tracePath := ""
	if cfg.TxnTrace {
		tracePath = filepath.Join(privateConfigDir, "txntrace.log")
	}
	syncCmd.Env = childEnvironment(tracePath)

	if err := syncCmd.Start(); err != nil {
		return fmt.Errorf("start strand sync: %w", err)
	}

	defer func() {
		if syncCmd.Process != nil && (syncCmd.ProcessState == nil || !syncCmd.ProcessState.Exited()) {
			_ = syncCmd.Process.Signal(syscall.SIGTERM)
			done := make(chan error, 1)
			go func() { done <- syncCmd.Wait() }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = syncCmd.Process.Kill()
				<-done
			}
		}
	}()

	// Discover the endpoint from this child's successful bind. A fixed/shared
	// endpoint could otherwise belong to a concurrent diagnostic run.
	var metricsAddr string
	select {
	case metricsAddr = <-metricsReady:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(15 * time.Second):
		return fmt.Errorf("child did not announce its own metrics listener (see %s)", strandLogPath)
	}
	metricsURL := "http://" + metricsAddr + "/metrics"
	log.Printf("Waiting for strand sync metrics endpoint at %s...", metricsURL)
	if err := waitForHTTP(ctx, metricsURL, 15*time.Second); err != nil {
		return fmt.Errorf("wait for metrics server: %w (check %s)", err, strandLogPath)
	}

	log.Printf("Starting workload generator (%d concurrent clients, seed=%d)...", cfg.Clients, cfg.Seed)
	workloadStop := make(chan struct{})
	var sourceCommits atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(clientID)*1000))
			for {
				select {
				case <-workloadStop:
					return
				default:
				}
				rowID := rng.Intn(cfg.Rows) + 1
				delta := rng.Intn(100) + 1
				_, err := srcDB.Exec("UPDATE bench.accounts SET balance = balance + $1, updated_at = now() WHERE id = $2", delta, rowID)
				if err != nil {
					time.Sleep(5 * time.Millisecond)
					continue
				}
				sourceCommits.Add(1)
			}
		}(i)
	}

	// Samplers for metrics and wait events
	waitStatsDst := make(map[string]*WaitEventStat)
	waitStatsSrc := make(map[string]*WaitEventStat)
	var waitMu sync.Mutex
	stopSampling := make(chan struct{})
	var samples []MetricSample

	waitsDone := make(chan struct{})
	go func() {
		defer close(waitsDone)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				sampleWaits(dstDB, waitStatsDst, &waitMu)
				sampleWaits(srcDB, waitStatsSrc, &waitMu)
			}
		}
	}()

	// Metric poller
	pollerDone := make(chan struct{})
	var currentPhase phaseLabel
	currentPhase.Store("warmup")
	var prevAppliedTxns int64
	var prevAppliedRows int64
	var prevSourceCommits int64
	var prevSampleTime time.Time

	go func() {
		defer close(pollerDone)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		prevSampleTime = time.Now()
		for {
			select {
			case <-stopSampling:
				return
			case t := <-ticker.C:
				appTxns, appRows, lag := scrapeMetrics(metricsURL)
				sCommits := sourceCommits.Load()
				elapsed := t.Sub(prevSampleTime).Seconds()
				if elapsed <= 0 {
					elapsed = 0.5
				}

				txnRate := float64(appTxns-prevAppliedTxns) / elapsed
				rowRate := float64(appRows-prevAppliedRows) / elapsed
				srcRate := float64(sCommits-prevSourceCommits) / elapsed

				var bSize float64
				if appTxns > prevAppliedTxns {
					bSize = float64(appRows-prevAppliedRows) / float64(appTxns-prevAppliedTxns)
				}

				samples = append(samples, MetricSample{
					Timestamp:      t,
					Phase:          currentPhase.Load(),
					AppliedTxns:    appTxns,
					AppliedRows:    appRows,
					SourceCommits:  sCommits,
					LagSeconds:     lag,
					ApplyTxnRate:   txnRate,
					ApplyRowRate:   rowRate,
					SourceXactRate: srcRate,
					RowsPerTxn:     bSize,
				})

				prevAppliedTxns = appTxns
				prevAppliedRows = appRows
				prevSourceCommits = sCommits
				prevSampleTime = t
			}
		}
	}()

	log.Printf("Running warmup period for %v...", cfg.Warmup)
	time.Sleep(cfg.Warmup)

	currentPhase.Store("steady")
	warmupEndTxns, warmupEndRows, _ := scrapeMetrics(metricsURL)
	warmupEndSrcCommits := sourceCommits.Load()
	steadyStart := time.Now()

	log.Printf("Warmup finished. Running steady window for %v...", cfg.Duration)
	time.Sleep(cfg.Duration)

	steadyEnd := time.Now()
	steadyEndTxns, steadyEndRows, _ := scrapeMetrics(metricsURL)
	steadyEndSrcCommits := sourceCommits.Load()

	log.Printf("Stopping workload and sampling...")
	close(workloadStop)
	wg.Wait()
	close(stopSampling)
	<-pollerDone
	<-waitsDone

	log.Printf("Stopping `strand sync` gracefully via SIGTERM...")
	_ = syncCmd.Process.Signal(syscall.SIGTERM)
	if err := syncCmd.Wait(); err != nil {
		return fmt.Errorf("strand sync failed: %w (see redacted child log)", err)
	}
	if err := childLog.Flush(); err != nil {
		return fmt.Errorf("flush redacted child log: %w", err)
	}
	if cfg.TxnTrace {
		raw, err := os.ReadFile(filepath.Join(privateConfigDir, "txntrace.log"))
		if err != nil {
			return fmt.Errorf("read transaction trace: %w", err)
		}
		if err := writePrivateFile(filepath.Join(cfg.OutputDir, "txntrace.log"), []byte(redact(string(raw)))); err != nil {
			return err
		}
	}

	// Calculate results
	steadyDur := steadyEnd.Sub(steadyStart).Seconds()
	steadyTxns := float64(steadyEndTxns - warmupEndTxns)
	steadyRows := float64(steadyEndRows - warmupEndRows)
	steadySrc := float64(steadyEndSrcCommits - warmupEndSrcCommits)

	steadyTPS := 0.0
	steadyRowRate := 0.0
	sourceCommitRate := 0.0
	rowsPerTxn := 0.0

	if steadyDur > 0 {
		steadyTPS = steadyTxns / steadyDur
		steadyRowRate = steadyRows / steadyDur
		sourceCommitRate = steadySrc / steadyDur
	}
	if steadyTxns > 0 {
		rowsPerTxn = steadyRows / steadyTxns
	}

	warmupTPS := 0.0
	if cfg.Warmup.Seconds() > 0 {
		warmupTPS = float64(warmupEndTxns) / cfg.Warmup.Seconds()
	}

	reproduces := (steadyTPS >= 280.0 && steadyTPS <= 300.0)
	applyRawRatio := 0.0
	if rawCommitTPS > 0 {
		applyRawRatio = steadyTPS / rawCommitTPS
	}

	log.Printf("Results: Steady TPS = %.1f, Warmup TPS = %.1f, Raw PG TPS = %.1f, Apply txns/raw commits ratio = %.2f, Rows/source txn = %.2f, Reproduces 280-300: %v",
		steadyTPS, warmupTPS, rawCommitTPS, applyRawRatio, rowsPerTxn, reproduces)

	// Collect top waits
	topWaitsDst := sortWaitStats(waitStatsDst)
	topWaitsSrc := sortWaitStats(waitStatsSrc)

	// Format text summaries of pprof if pprof tool is available
	_ = exec.Command("/usr/local/go/bin/go", "tool", "pprof", "-text", "-nodecount=15", cpuProfilePath).Run()
	_ = exec.Command("/usr/local/go/bin/go", "tool", "pprof", "-text", "-nodecount=15", blockProfilePath).Run()

	var observations []string
	observations = append(observations, fmt.Sprintf("Source workload generation achieved %.1f commits/sec across %d concurrent clients.", sourceCommitRate, cfg.Clients))
	observations = append(observations, fmt.Sprintf("Separate target single-connection update/commit workload achieved %.1f commits/sec.", rawCommitTPS))
	observations = append(observations, fmt.Sprintf("Strand strict mode applied %.1f source transactions/sec, averaging %.2f rows/source transaction. Target commit batch size is not measured by these counters. The raw single-connection commit rate is a separate workload, not an apply throughput ceiling.", steadyTPS, rowsPerTxn))

	if len(topWaitsDst) > 0 {
		observations = append(observations, fmt.Sprintf("Top target wait event: %s:%s (%d occurrences)", topWaitsDst[0].Type, topWaitsDst[0].Event, topWaitsDst[0].Count))
	}

	if reproduces {
		observations = append(observations, "Local throughput is within the 280-300 source-transactions/sec comparison band; this alone does not establish the user-host root cause.")
	} else {
		observations = append(observations, "This isolated local run did not reproduce the reported throughput band. Storage latency and batching remain hypotheses requiring measurements on the affected host.")
	}

	report := BenchmarkReport{
		StartTime:             steadyStart,
		EndTime:               steadyEnd,
		DurationSec:           cfg.Duration.Seconds(),
		WarmupSec:             cfg.Warmup.Seconds(),
		SteadySec:             steadyDur,
		Clients:               cfg.Clients,
		SeedRows:              cfg.Rows,
		Seed:                  cfg.Seed,
		Reproduces280to300:    reproduces,
		RawTargetCommitTPS:    rawCommitTPS,
		ApplyToRawCommitRatio: applyRawRatio,
		WarmupTPS:             warmupTPS,
		SteadyTPS:             steadyTPS,
		SteadyRowRate:         steadyRowRate,
		SourceCommitRate:      sourceCommitRate,
		RowsPerSourceTxn:      rowsPerTxn,
		TargetTopWaits:        topWaitsDst,
		SourceTopWaits:        topWaitsSrc,
		Versions:              versions,
		FsyncSettings:         fsyncSettings,
		Observations:          observations,
		ArtifactFiles: map[string]string{
			"cpu_profile":     cpuProfilePath,
			"block_profile":   blockProfilePath,
			"redacted_config": redactedConfigPath,
			"strand_log":      strandLogPath,
		},
	}

	summaryPath := filepath.Join(cfg.OutputDir, "summary.json")
	summaryData, _ := json.MarshalIndent(report, "", "  ")
	if err := os.WriteFile(summaryPath, summaryData, 0o644); err != nil {
		return fmt.Errorf("write summary: %w", err)
	}

	metricsPath := filepath.Join(cfg.OutputDir, "metrics_timeseries.jsonl")
	mFile, err := os.Create(metricsPath)
	if err == nil {
		defer mFile.Close()
		for _, s := range samples {
			line, _ := json.Marshal(s)
			mFile.Write(append(line, '\n'))
		}
	}

	waitsPath := filepath.Join(cfg.OutputDir, "waits.txt")
	wFile, err := os.Create(waitsPath)
	if err == nil {
		defer wFile.Close()
		fmt.Fprintf(wFile, "=== TARGET POSTGRES WAIT EVENTS ===\n")
		for _, w := range topWaitsDst {
			fmt.Fprintf(wFile, "%-15s %-25s %-10s %d\n", w.Type, w.Event, w.State, w.Count)
		}
		fmt.Fprintf(wFile, "\n=== SOURCE POSTGRES WAIT EVENTS ===\n")
		for _, w := range topWaitsSrc {
			fmt.Fprintf(wFile, "%-15s %-25s %-10s %d\n", w.Type, w.Event, w.State, w.Count)
		}
	}

	log.Printf("Benchmark complete! Artifacts written to %s", cfg.OutputDir)
	return nil
}

func measureRawCommitTPS(ctx context.Context, db *sql.DB, dur time.Duration) float64 {
	deadline := time.Now().Add(dur)
	var count int64
	start := time.Now()
	for time.Now().Before(deadline) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			break
		}
		_, err = tx.ExecContext(ctx, "UPDATE bench.accounts SET balance = balance + 1 WHERE id = 1")
		if err != nil {
			_ = tx.Rollback()
			break
		}
		if err := tx.Commit(); err != nil {
			break
		}
		count++
	}
	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed
}

func setupDatabases(ctx context.Context, src, dst *sql.DB, seed int64, rows int) error {
	for _, db := range []*sql.DB{src, dst} {
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS bench"); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS bench.accounts (
				id BIGINT PRIMARY KEY,
				balance BIGINT NOT NULL,
				status VARCHAR(32) NOT NULL,
				updated_at TIMESTAMPTZ NOT NULL
			);
		`); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, "TRUNCATE bench.accounts"); err != nil {
			return err
		}
	}

	_, _ = src.ExecContext(ctx, "DROP PUBLICATION IF EXISTS strand_strict_diag_pub")
	if _, err := src.ExecContext(ctx, "CREATE PUBLICATION strand_strict_diag_pub FOR TABLE bench.accounts"); err != nil {
		return fmt.Errorf("create publication: %w", err)
	}

	log.Printf("Inserting %d seed rows into source...", rows)
	tx, err := src.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO bench.accounts (id, balance, status, updated_at) VALUES ($1, $2, $3, now())")
	if err != nil {
		return err
	}
	defer stmt.Close()

	rng := rand.New(rand.NewSource(seed))
	for i := 1; i <= rows; i++ {
		bal := rng.Int63n(100000) + 1000
		if _, err := stmt.ExecContext(ctx, i, bal, "active"); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("Copying seed rows to target for baseline...")
	dtx, err := dst.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer dtx.Rollback()
	dstmt, err := dtx.PrepareContext(ctx, "INSERT INTO bench.accounts (id, balance, status, updated_at) VALUES ($1, $2, $3, now())")
	if err != nil {
		return err
	}
	defer dstmt.Close()
	rng = rand.New(rand.NewSource(seed))
	for i := 1; i <= rows; i++ {
		bal := rng.Int63n(100000) + 1000
		if _, err := dstmt.ExecContext(ctx, i, bal, "active"); err != nil {
			return err
		}
	}
	return dtx.Commit()
}

func sampleWaits(db *sql.DB, store map[string]*WaitEventStat, mu *sync.Mutex) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	rows, err := db.QueryContext(ctx, `
		SELECT COALESCE(wait_event_type, 'CPU') AS wait_type,
		       COALESCE(wait_event, 'running') AS wait_event,
		       COALESCE(state, 'unknown') AS state,
		       COUNT(*) AS cnt
		FROM pg_stat_activity
		WHERE backend_type = 'client backend' AND pid != pg_backend_pid()
		GROUP BY 1, 2, 3
	`)
	if err != nil {
		return
	}
	defer rows.Close()

	mu.Lock()
	defer mu.Unlock()

	for rows.Next() {
		var wtype, wevent, state string
		var cnt int64
		if err := rows.Scan(&wtype, &wevent, &state, &cnt); err == nil {
			key := fmt.Sprintf("%s:%s:%s", wtype, wevent, state)
			stat, ok := store[key]
			if !ok {
				stat = &WaitEventStat{Type: wtype, Event: wevent, State: state}
				store[key] = stat
			}
			stat.Count += cnt
		}
	}
}

func sortWaitStats(store map[string]*WaitEventStat) []WaitEventStat {
	var list []WaitEventStat
	for _, v := range store {
		list = append(list, *v)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Count > list[j].Count
	})
	return list
}

// Prometheus text counters can use scientific notation (for example 1e+06).
// Check bounds before converting floating-point values to signed integer counts.
func parseMetricCounter(value string) (int64, error) {
	if n, err := strconv.ParseInt(value, 10, 64); err == nil && n >= 0 {
		return n, nil
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f >= 0x1p63 || math.Trunc(f) != f {
		return 0, fmt.Errorf("invalid metric counter %q", value)
	}
	return int64(f), nil
}

// isMetricNamed checks if the Prometheus line matches metric name with word boundaries
// (either space, tab, or '{' for labels), preventing false prefix matches like metric_duration.
func isMetricNamed(line, name string) bool {
	if !strings.HasPrefix(line, name) {
		return false
	}
	rest := line[len(name):]
	return len(rest) > 0 && (rest[0] == ' ' || rest[0] == '\t' || rest[0] == '{')
}

func scrapeMetrics(url string) (appliedTxns int64, appliedRows int64, lagSeconds float64) {
	client := http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(url)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") {
			continue
		}
		if isMetricNamed(line, "strand_apply_txn_total") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := parseMetricCounter(parts[len(parts)-1])
				if err != nil {
					log.Printf("warning: scrapeMetrics failed to parse strand_apply_txn_total %q: %v", parts[len(parts)-1], err)
					continue
				}
				appliedTxns = val
			}
		} else if isMetricNamed(line, "strand_apply_rows_total") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := parseMetricCounter(parts[len(parts)-1])
				if err != nil {
					log.Printf("warning: scrapeMetrics failed to parse strand_apply_rows_total %q: %v", parts[len(parts)-1], err)
					continue
				}
				appliedRows = val
			}
		} else if isMetricNamed(line, "strand_lag_seconds") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				val, err := strconv.ParseFloat(parts[len(parts)-1], 64)
				if err != nil || math.IsNaN(val) || math.IsInf(val, 0) {
					log.Printf("warning: scrapeMetrics failed to parse strand_lag_seconds %q: %v", parts[len(parts)-1], err)
					continue
				}
				lagSeconds = val
			}
		}
	}
	return
}

func waitForDB(ctx context.Context, dsn string, timeout time.Duration) (*sql.DB, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		db, err := sql.Open("pgx", dsn)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
			err = db.PingContext(pingCtx)
			cancel()
			if err == nil {
				return db, nil
			}
			db.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("database not ready after %v", timeout)
}

func waitForHTTP(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("endpoint %s not ready after %v", url, timeout)
}
