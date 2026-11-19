package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	var (
		goBin       = flag.String("go", "", "Path to Go binary (defaults to /usr/local/go/bin/go or go on PATH)")
		workspace   = flag.String("workspace", "", "Strand workspace directory (default: current directory)")
		artifacts   = flag.String("artifacts", "", "Output artifacts directory")
		iterations  = flag.Int("iterations", 15, "ToastPKMove stress iterations (when stress15 is enabled)")
		loadSec     = flag.Int("load-sec", 15, "Seconds of concurrent workload for bounded/stress tests")
		seed        = flag.Int64("seed", 0, "Seed for randomized tests (default: timestamp nanoseconds)")
		runStress15 = flag.Bool("stress15", false, "Execute heavy 15-iteration ToastPKMove and kill tests")
		runFull     = flag.Bool("fullsuite", false, "Execute full test suite (labeled as fullsuite stage)")
		timeout     = flag.Duration("timeout", 45*time.Minute, "Global timeout for all executed stages")
		verbose     = flag.Bool("v", true, "Print stage commands and live output to stdout")
	)
	flag.Parse()

	ws := *workspace
	if ws == "" {
		cwd, err := os.Getwd()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get current working directory: %v\n", err)
			os.Exit(1)
		}
		ws = cwd
	}
	ws, _ = filepath.Abs(ws)

	art := *artifacts
	if art == "" {
		art = filepath.Join(ws, "reports/code-review-20261117/followup-agy/evidence/remaining-gate")
	}

	cfg := Config{
		GoBinary:        *goBin,
		WorkspaceDir:    ws,
		ArtifactsDir:    art,
		ToastIterations: *iterations,
		ToastLoadSec:    *loadSec,
		Seed:            *seed,
		Timeout:         *timeout,
		RunStress15:     *runStress15,
		RunFullSuite:    *runFull,
		Verbose:         *verbose,
	}

	runner := NewRunner(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	// Intercept termination signals for graceful cleanup of OWN resources only
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		fmt.Fprintf(os.Stderr, "\nReceived shutdown signal (%v). Initiating cancellation and cleanup of owned resources...\n", sig)
		cancel()
		<-sigCh
		fmt.Fprintf(os.Stderr, "\nForced exit requested.\n")
		os.Exit(2)
	}()

	fmt.Println("================================================================================")
	fmt.Println("             STRAND REMAINING REVIEW GATE RUNNER")
	fmt.Println("================================================================================")
	fmt.Printf("Workspace:    %s\n", ws)
	fmt.Printf("Artifacts:    %s\n", art)
	fmt.Printf("Stress15:     %v (iterations: %d)\n", *runStress15, *iterations)
	fmt.Printf("FullSuite:    %v\n", *runFull)
	fmt.Println("--------------------------------------------------------------------------------")

	err := runner.Run(ctx)

	fmt.Println("\n================================================================================")
	fmt.Println(runner.SummaryMarkdown())
	fmt.Println("================================================================================")

	if err != nil || runner.report.OverallStatus != "PASS" {
		if err != nil {
			fmt.Fprintf(os.Stderr, "Gate run failed: %v\n", err)
		}
		os.Exit(1)
	}

	os.Exit(0)
}
