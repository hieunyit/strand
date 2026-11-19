package main

import (
	"time"
)

// Config defines the options for the remaining-gate runner.
type Config struct {
	GoBinary        string        `json:"go_binary"`
	WorkspaceDir    string        `json:"workspace_dir"`
	ArtifactsDir    string        `json:"artifacts_dir"`
	Stages          []string      `json:"stages"`
	ToastIterations int           `json:"toast_iterations"`
	ToastLoadSec    int           `json:"toast_load_sec"`
	Seed            int64         `json:"seed"`
	Timeout         time.Duration `json:"timeout"`
	RunFullSuite    bool          `json:"run_full_suite"`
	RunStress15     bool          `json:"run_stress15"`
	Verbose         bool          `json:"verbose"`
}

// StageResult captures the outcome and telemetry of a single verification stage.
type StageResult struct {
	Name          string        `json:"name"`
	Category      string        `json:"category"` // build, vet, unit, regression, race, e2e-bounded, e2e-stress, fullsuite
	Status        string        `json:"status"`   // PASS, FAIL, SKIPPED, CANCELLED
	Duration      time.Duration `json:"duration_ns"`
	DurationHuman string        `json:"duration_human"`
	Command       string        `json:"command"`
	LogPath       string        `json:"log_path,omitempty"`
	Error         string        `json:"error,omitempty"`
}

// RunReport stores the full execution report of the remaining-gate run.
type RunReport struct {
	Timestamp        time.Time     `json:"timestamp"`
	OverallStatus    string        `json:"overall_status"`
	TotalDuration    time.Duration `json:"total_duration_ns"`
	TreeHash         string        `json:"tree_hash"`
	GoVersion        string        `json:"go_version"`
	DockerVersion    string        `json:"docker_version,omitempty"`
	PGVersion        string        `json:"pg_version,omitempty"`
	Seed             int64         `json:"seed"`
	Stages           []StageResult `json:"stages"`
	ArtifactsDir     string        `json:"artifacts_dir"`
	ResourcesCleaned []string      `json:"resources_cleaned"`
}
