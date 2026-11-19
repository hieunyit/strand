package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRemainingGate_BaselineCollection(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		GoBinary:     "/usr/local/go/bin/go",
		WorkspaceDir: tmpDir,
		ArtifactsDir: filepath.Join(tmpDir, "artifacts"),
		Seed:         42,
	}

	runner := NewRunner(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := runner.CollectBaseline(ctx); err != nil {
		t.Fatalf("CollectBaseline failed: %v", err)
	}

	if runner.report.GoVersion == "" {
		t.Errorf("expected non-empty GoVersion")
	}
	if runner.report.Seed != 42 {
		t.Errorf("expected seed 42, got %d", runner.report.Seed)
	}
	if _, err := os.Stat(cfg.ArtifactsDir); os.IsNotExist(err) {
		t.Errorf("expected artifacts directory to be created")
	}
}

func TestRemainingGate_ResourceCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		WorkspaceDir: tmpDir,
		ArtifactsDir: filepath.Join(tmpDir, "artifacts"),
		Seed:         100,
	}
	runner := NewRunner(cfg)

	cleanedFile := filepath.Join(tmpDir, "test_resource.txt")
	if err := os.WriteFile(cleanedFile, []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}

	runner.RegisterCleanup("temporary test file", func() error {
		return os.Remove(cleanedFile)
	})

	runner.CleanupAll()

	if _, err := os.Stat(cleanedFile); !os.IsNotExist(err) {
		t.Errorf("expected test resource file to be deleted by cleanup")
	}

	if len(runner.report.ResourcesCleaned) != 1 || runner.report.ResourcesCleaned[0] != "temporary test file" {
		t.Errorf("unexpected cleaned resources log: %+v", runner.report.ResourcesCleaned)
	}
}

func TestRemainingGate_StageExecution(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		WorkspaceDir: tmpDir,
		ArtifactsDir: filepath.Join(tmpDir, "artifacts"),
		Seed:         123,
		Verbose:      false,
	}
	runner := NewRunner(cfg)
	_ = os.MkdirAll(cfg.ArtifactsDir, 0755)

	ctx := context.Background()
	res := runner.ExecuteStage(ctx, "echo test", "unit", []string{"echo", "hello gate"}, nil)

	if res.Status != "PASS" {
		t.Fatalf("expected PASS, got %s (err: %s)", res.Status, res.Error)
	}

	data, err := os.ReadFile(res.LogPath)
	if err != nil {
		t.Fatalf("failed to read stage log: %v", err)
	}
	if string(data) != "hello gate\n" {
		t.Errorf("unexpected stage log content: %q", string(data))
	}
}
