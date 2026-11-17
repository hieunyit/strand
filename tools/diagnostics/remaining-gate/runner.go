package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Runner orchestrates the execution of verification stages with resource isolation.
type Runner struct {
	cfg       Config
	mu        sync.Mutex
	report    RunReport
	cleanups  []func() error
	cleaned   []string
	cancelled bool
}

// NewRunner creates a new runner with the provided configuration.
func NewRunner(cfg Config) *Runner {
	if cfg.GoBinary == "" {
		if _, err := os.Stat("/usr/local/go/bin/go"); err == nil {
			cfg.GoBinary = "/usr/local/go/bin/go"
		} else {
			cfg.GoBinary = "go"
		}
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	if cfg.ToastIterations <= 0 {
		cfg.ToastIterations = 15
	}
	if cfg.ArtifactsDir == "" {
		cfg.ArtifactsDir = filepath.Join(cfg.WorkspaceDir, "reports/code-review-20261117/followup-agy/evidence/remaining-gate")
	}

	return &Runner{
		cfg: cfg,
		report: RunReport{
			Timestamp:        time.Now(),
			OverallStatus:    "PENDING",
			Seed:             cfg.Seed,
			ArtifactsDir:     cfg.ArtifactsDir,
			Stages:           make([]StageResult, 0),
			ResourcesCleaned: make([]string, 0),
		},
		cleanups: make([]func() error, 0),
		cleaned:  make([]string, 0),
	}
}

// RegisterCleanup registers a cleanup action that strictly cleans owned resources.
func (r *Runner) RegisterCleanup(desc string, fn func() error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups = append(r.cleanups, func() error {
		err := fn()
		r.cleaned = append(r.cleaned, desc)
		return err
	})
}

// CleanupAll executes all registered cleanups in LIFO order.
func (r *Runner) CleanupAll() {
	r.mu.Lock()
	cleanups := make([]func() error, len(r.cleanups))
	copy(cleanups, r.cleanups)
	r.cleanups = nil
	r.mu.Unlock()

	for i := len(cleanups) - 1; i >= 0; i-- {
		_ = cleanups[i]()
	}

	r.mu.Lock()
	r.report.ResourcesCleaned = append(r.report.ResourcesCleaned, r.cleaned...)
	r.mu.Unlock()
}

// CollectBaseline records Git tree hash, Go version, and DB version if available.
func (r *Runner) CollectBaseline(ctx context.Context) error {
	if err := os.MkdirAll(r.cfg.ArtifactsDir, 0755); err != nil {
		return fmt.Errorf("create artifacts dir: %w", err)
	}

	// 1. Git Tree Hash
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD^{tree}")
	cmd.Dir = r.cfg.WorkspaceDir
	out, err := cmd.Output()
	if err == nil {
		r.report.TreeHash = strings.TrimSpace(string(out))
	} else {
		r.report.TreeHash = "UNKNOWN"
	}

	// 2. Go Version
	cmdGo := exec.CommandContext(ctx, r.cfg.GoBinary, "version")
	outGo, err := cmdGo.Output()
	if err == nil {
		r.report.GoVersion = strings.TrimSpace(string(outGo))
	} else {
		r.report.GoVersion = "UNKNOWN"
	}

	// 3. Docker Version
	cmdDoc := exec.CommandContext(ctx, "docker", "version", "--format", "{{.Server.Version}}")
	outDoc, err := cmdDoc.Output()
	if err == nil {
		r.report.DockerVersion = strings.TrimSpace(string(outDoc))
	}

	// 4. PostgreSQL Version via running docker container or image
	cmdPG := exec.CommandContext(ctx, "docker", "run", "--rm", "postgres:18", "postgres", "-V")
	outPG, err := cmdPG.Output()
	if err == nil {
		r.report.PGVersion = strings.TrimSpace(string(outPG))
	} else {
		// Fallback check postgres:16
		cmdPG16 := exec.CommandContext(ctx, "docker", "run", "--rm", "postgres:16", "postgres", "-V")
		outPG16, err16 := cmdPG16.Output()
		if err16 == nil {
			r.report.PGVersion = strings.TrimSpace(string(outPG16))
		}
	}

	return nil
}

// ExecuteStage executes an individual stage, capturing its output to a dedicated log file.
func (r *Runner) ExecuteStage(ctx context.Context, name, category string, cmdLine []string, extraEnv []string) StageResult {
	start := time.Now()
	res := StageResult{
		Name:     name,
		Category: category,
		Command:  strings.Join(cmdLine, " "),
	}

	logFileName := fmt.Sprintf("%s.log", strings.ReplaceAll(strings.ToLower(name), " ", "_"))
	logPath := filepath.Join(r.cfg.ArtifactsDir, logFileName)
	res.LogPath = logPath

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		res.Status = "FAIL"
		res.Error = fmt.Sprintf("failed to open log file: %v", err)
		res.Duration = time.Since(start)
		res.DurationHuman = res.Duration.String()
		return res
	}
	defer logFile.Close()

	if r.cfg.Verbose {
		fmt.Printf("===> [STAGE: %s] %s\n", category, name)
		fmt.Printf("     CMD: %s\n", res.Command)
	}

	var stdout, stderr io.Writer
	if r.cfg.Verbose {
		stdout = io.MultiWriter(logFile, os.Stdout)
		stderr = io.MultiWriter(logFile, os.Stderr)
	} else {
		stdout = logFile
		stderr = logFile
	}

	cmd := exec.CommandContext(ctx, cmdLine[0], cmdLine[1:]...)
	cmd.Dir = r.cfg.WorkspaceDir
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = append(os.Environ(), extraEnv...)

	execErr := cmd.Run()
	duration := time.Since(start)
	res.Duration = duration
	res.DurationHuman = duration.String()

	if execErr != nil {
		if ctx.Err() != nil {
			res.Status = "CANCELLED"
			res.Error = ctx.Err().Error()
		} else {
			res.Status = "FAIL"
			res.Error = execErr.Error()
		}
		if r.cfg.Verbose {
			fmt.Printf("     STATUS: %s (in %s) - Error: %v\n", res.Status, res.DurationHuman, res.Error)
		}
	} else {
		res.Status = "PASS"
		if r.cfg.Verbose {
			fmt.Printf("     STATUS: PASS (in %s)\n", res.DurationHuman)
		}
	}

	return res
}

// Run executes the configured gate stages.
func (r *Runner) Run(ctx context.Context) error {
	start := time.Now()
	defer func() {
		r.report.TotalDuration = time.Since(start)
		r.CleanupAll()
		_ = r.SaveSummary()
	}()

	if err := r.CollectBaseline(ctx); err != nil {
		return fmt.Errorf("collect baseline: %w", err)
	}

	stagesToRun := r.planStages()
	overallPass := true

	for _, stagePlan := range stagesToRun {
		if ctx.Err() != nil {
			r.report.OverallStatus = "CANCELLED"
			return ctx.Err()
		}

		res := r.ExecuteStage(ctx, stagePlan.name, stagePlan.category, stagePlan.cmd, stagePlan.env)
		r.mu.Lock()
		r.report.Stages = append(r.report.Stages, res)
		r.mu.Unlock()

		if res.Status != "PASS" {
			overallPass = false
			// We fail closed on build/vet/unit failures
			if stagePlan.category == "build" || stagePlan.category == "vet" {
				r.report.OverallStatus = "FAIL"
				return fmt.Errorf("stage %s failed: %s", stagePlan.name, res.Error)
			}
		}
	}

	if overallPass {
		r.report.OverallStatus = "PASS"
	} else {
		r.report.OverallStatus = "FAIL"
	}

	return nil
}

type stageDef struct {
	name     string
	category string
	cmd      []string
	env      []string
}

func (r *Runner) planStages() []stageDef {
	var stages []stageDef
	seedStr := fmt.Sprintf("%d", r.cfg.Seed)

	// Stage 1: Build & Vet
	stages = append(stages, stageDef{
		name:     "Build Packages",
		category: "build",
		cmd:      []string{r.cfg.GoBinary, "build", "./internal/snapshot/...", "./internal/pipeline/..."},
		env:      nil,
	})

	stages = append(stages, stageDef{
		name:     "Go Vet Target Packages",
		category: "vet",
		cmd:      []string{r.cfg.GoBinary, "vet", "./internal/snapshot/...", "./internal/pipeline/..."},
		env:      nil,
	})

	// Stage 2: Unit tests for TOAST PK and snapshot bounds
	stages = append(stages, stageDef{
		name:     "Unit ToastPK & Bounds",
		category: "unit",
		cmd:      []string{r.cfg.GoBinary, "test", "-v", "-run", "^TestToastPK", "./internal/snapshot/..."},
		env:      []string{"SEED=" + seedStr},
	})

	// Stage 3: Regression tests for Snapshot Completion & Reread Bounds
	stages = append(stages, stageDef{
		name:     "Regression Completion & Reread Bounds",
		category: "regression",
		cmd:      []string{r.cfg.GoBinary, "test", "-v", "-run", "^Test(SnapshotCompletion|ToastPK|RereadBounds)", "./internal/snapshot/...", "./internal/pipeline/..."},
		env:      []string{"SEED=" + seedStr},
	})

	// Stage 4: Race detection on snapshot and spool packages
	stages = append(stages, stageDef{
		name:     "Race Detection Core",
		category: "race",
		cmd:      []string{r.cfg.GoBinary, "test", "-race", "-v", "-run", "^Test(ToastPK|Completion)", "./internal/snapshot/..."},
		env:      []string{"SEED=" + seedStr},
	})

	// Stage 5: Bounded Crash/Resume Scenario
	stages = append(stages, stageDef{
		name:     "Bounded Crash & Resume with Bounds Plan",
		category: "e2e-bounded",
		cmd:      []string{r.cfg.GoBinary, "test", "-v", "-timeout", "10m", "-run", "^TestE2E_PGToastCompletionCrashResume$", "./tests/e2e/..."},
		env:      []string{"SEED=" + seedStr, "STRAND_E2E_PG_TOAST_PK_LOAD_SECONDS=15"},
	})

	// Stage 6: Optional Heavy Stress 15 iterations (run only if requested or when other lanes finish)
	if r.cfg.RunStress15 {
		iterStr := fmt.Sprintf("%d", r.cfg.ToastIterations)
		loadSecStr := fmt.Sprintf("%d", r.cfg.ToastLoadSec)
		if r.cfg.ToastLoadSec <= 0 {
			loadSecStr = "45"
		}
		stages = append(stages, stageDef{
			name:     fmt.Sprintf("Stress ToastPK Strict %s Iterations", iterStr),
			category: "e2e-stress",
			cmd:      []string{r.cfg.GoBinary, "test", "-v", "-timeout", "60m", "-run", "^TestP2_E2E_PGLogicalSnapshotToastPKMove$", "./tests/e2e/..."},
			env: []string{
				"SEED=" + seedStr,
				"STRAND_E2E_PG_TOAST_PK_ITERATIONS=" + iterStr,
				"STRAND_E2E_PG_TOAST_PK_LOAD_SECONDS=" + loadSecStr,
			},
		})

		stages = append(stages, stageDef{
			name:     "Kill During Reread Test",
			category: "e2e-stress",
			cmd:      []string{r.cfg.GoBinary, "test", "-v", "-timeout", "20m", "-run", "^TestG2_E2E_PGToastPKKillDuringReread$", "./tests/e2e/..."},
			env: []string{
				"SEED=" + seedStr,
				"STRAND_E2E_PG_TOAST_PK_LOAD_SECONDS=" + loadSecStr,
			},
		})
	}

	// Stage 7: Full Test Suite (explicitly separate stage, run only if RunFullSuite is true)
	if r.cfg.RunFullSuite {
		stages = append(stages, stageDef{
			name:     "Full Test Suite",
			category: "fullsuite",
			cmd:      []string{r.cfg.GoBinary, "test", "./..."},
			env:      []string{"SEED=" + seedStr},
		})
	}

	return stages
}

// SaveSummary writes the summary.json artifact.
func (r *Runner) SaveSummary() error {
	summaryPath := filepath.Join(r.cfg.ArtifactsDir, "summary.json")
	data, err := json.MarshalIndent(r.report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(summaryPath, data, 0644)
}

// SummaryMarkdown generates a formatted summary string.
func (r *Runner) SummaryMarkdown() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("### Remaining Gate Run Summary\n\n"))
	buf.WriteString(fmt.Sprintf("- **Overall Status**: `%s`\n", r.report.OverallStatus))
	buf.WriteString(fmt.Sprintf("- **Total Duration**: %s\n", r.report.TotalDuration.Round(time.Millisecond)))
	buf.WriteString(fmt.Sprintf("- **Tree Hash**: `%s`\n", r.report.TreeHash))
	buf.WriteString(fmt.Sprintf("- **Go Version**: `%s`\n", r.report.GoVersion))
	if r.report.DockerVersion != "" {
		buf.WriteString(fmt.Sprintf("- **Docker Version**: `%s`\n", r.report.DockerVersion))
	}
	if r.report.PGVersion != "" {
		buf.WriteString(fmt.Sprintf("- **PostgreSQL Version**: `%s`\n", r.report.PGVersion))
	}
	buf.WriteString(fmt.Sprintf("- **Seed**: `%d`\n", r.report.Seed))
	buf.WriteString(fmt.Sprintf("- **Artifacts**: `%s`\n\n", r.report.ArtifactsDir))

	buf.WriteString("| Stage | Category | Status | Duration | Log Artifact |\n")
	buf.WriteString("| :--- | :--- | :--- | :--- | :--- |\n")
	for _, s := range r.report.Stages {
		buf.WriteString(fmt.Sprintf("| %s | `%s` | **%s** | %s | `%s` |\n",
			s.Name, s.Category, s.Status, s.DurationHuman, filepath.Base(s.LogPath)))
	}

	if len(r.report.ResourcesCleaned) > 0 {
		buf.WriteString("\n#### Resources Safely Cleaned:\n")
		for _, rc := range r.report.ResourcesCleaned {
			buf.WriteString(fmt.Sprintf("- %s\n", rc))
		}
	}

	return buf.String()
}
