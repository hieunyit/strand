package validate_test

import (
	"context"
	"testing"

	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/validate"
)

func TestPreflight_Valid(t *testing.T) {
	cfg := &config.Config{
		Version: 1,
		Name:    "orders-sync",
		Source: config.SourceConfig{
			Kind: "mysql",
			DSN:  "root@tcp(127.0.0.1:3306)/orders",
		},
		Target: config.TargetConfig{
			Kind:        "postgres",
			DSN:         "postgres://user:pass@127.0.0.1:5432/orders",
			CommitOrder: "strict",
			WriteMode:   "upsert",
		},
		Tables: config.TablesConfig{
			Include: []string{"app.orders"},
		},
	}

	err := validate.Preflight(context.Background(), cfg)
	if err != nil {
		t.Fatalf("expected valid preflight, got: %v", err)
	}
}

func TestPreflight_InvalidSourceKind(t *testing.T) {
	cfg := &config.Config{
		Version: 1,
		Name:    "test",
		Source: config.SourceConfig{
			Kind: "sqlite",
			DSN:  "test.db",
		},
		Target: config.TargetConfig{
			Kind: "mysql",
			DSN:  "root@tcp(127.0.0.1:3306)/orders",
		},
		Tables: config.TablesConfig{
			Include: []string{"app.orders"},
		},
	}

	err := validate.Preflight(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected error for unsupported source kind")
	}
	code := exitcode.GetExitCode(err)
	if code != exitcode.Rejected {
		t.Errorf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
	}
}

func TestPreflight_EmptyTables(t *testing.T) {
	cfg := &config.Config{
		Version: 1,
		Name:    "test",
		Source: config.SourceConfig{
			Kind: "mysql",
			DSN:  "root@tcp(127.0.0.1:3306)/orders",
		},
		Target: config.TargetConfig{
			Kind: "postgres",
			DSN:  "postgres://user:pass@127.0.0.1:5432/orders",
		},
		Tables: config.TablesConfig{
			Include: []string{},
		},
	}

	err := validate.Preflight(context.Background(), cfg)
	if err == nil {
		t.Fatalf("expected error for empty tables")
	}
	code := exitcode.GetExitCode(err)
	if code != exitcode.Rejected {
		t.Errorf("expected exit code %d (Rejected), got %d", exitcode.Rejected, code)
	}
}

func TestPreflight_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := &config.Config{}
	err := validate.Preflight(ctx, cfg)
	if err == nil {
		t.Fatalf("expected error on canceled context")
	}
}
