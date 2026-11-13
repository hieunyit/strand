package pipeline

// Dừng theo yêu cầu (SIGINT/SIGTERM → ctx của Run bị hủy) khi pipeline còn đang khởi động phải là dừng
// sạch (nil → exit 0), không phải "Error: read source collations: context canceled" exit 1. Lỗi thật vẫn
// phải trả ra.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"strand/internal/config"
	"strand/internal/exitcode"
)

func TestStopRequestedIsClean(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()
	boom := errors.New("connection refused")
	wrapped := fmt.Errorf("read source collations: %w", context.Canceled)
	coded := exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected, "log_bin is OFF", context.Canceled)
	for _, c := range []struct {
		name   string
		parent context.Context
		err    error
		clean  bool
	}{
		{"signal during startup", canceled, wrapped, true},
		{"signal, error carries exit code but cause is cancellation", canceled, coded, true},
		{"no error", canceled, nil, true},
		{"genuine error while stop was requested", canceled, boom, false},
		{"cancellation not from the signal (internal cancel, parent alive)", live, wrapped, false},
		{"genuine error, no stop requested", live, boom, false},
	} {
		got := stopRequestedIsClean(c.parent, log, c.err)
		if c.clean && got != nil {
			t.Errorf("%s: got %v, want nil", c.name, got)
		}
		if !c.clean && got == nil {
			t.Errorf("%s: error swallowed", c.name)
		}
	}
}

func unreachableCfg(t *testing.T) *config.Config {
	t.Helper()
	cfg := &config.Config{Name: "stop_startup",
		Source: config.SourceConfig{Kind: "mysql", DSN: "root:x@tcp(127.0.0.1:1)/app", ServerID: 4601},
		Target: config.TargetConfig{Kind: "mysql", DSN: "root:x@tcp(127.0.0.1:1)/dst"},
		Tables: config.TablesConfig{Include: []string{"app.t"}}}
	cfg.Resources.Spool.Path = t.TempDir()
	cfg.Governor.Algorithm = "static"
	return cfg
}

// Run với ctx đã hủy: dừng sạch. Cùng cấu hình với ctx sống (DB không tới được): lỗi thật, exit != 0.
func TestRun_StopBeforeStartIsCleanButGenuineFailureIsNot(t *testing.T) {
	opts := func(t *testing.T) Options {
		return Options{Config: unreachableCfg(t), ShutdownTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(canceled, opts(t)); err != nil {
		t.Fatalf("stop requested before start: %v, want nil", err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	err := Run(ctx, opts(t))
	if err == nil {
		t.Fatal("unreachable source: Run returned nil, want an error")
	}
	if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("genuine failure reported as cancellation: %v", err)
	}
}

// Gửi tín hiệu dừng (hủy ctx) ở nhiều thời điểm trong lúc khởi động trên DB thật: mọi lần phải trả nil.
func TestRun_StopAtAnyPointDuringStartupIsClean(t *testing.T) {
	db, dsn := startTestDB(t, "mysql")
	ctx := context.Background()
	for _, q := range []string{"CREATE DATABASE dst", "CREATE TABLE app.s1 (id BIGINT PRIMARY KEY, v VARCHAR(20))",
		"CREATE TABLE dst.s1 LIKE app.s1", "INSERT INTO app.s1 VALUES (1, 'a'), (2, 'b')"} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	cfg := &config.Config{Name: "stop_startup_db",
		Source: config.SourceConfig{Kind: "mysql", DSN: dsn, ServerID: 4602},
		Target: config.TargetConfig{Kind: "mysql", DSN: strings.Replace(dsn, "/app?", "/dst?", 1)},
		Tables: config.TablesConfig{Include: []string{"app.s1"}, Rename: map[string]string{"app.s1": "dst.s1"}}}
	cfg.Resources.Spool.Path = t.TempDir()
	cfg.Governor.Algorithm = "static"
	for _, d := range []time.Duration{0, time.Millisecond, 3 * time.Millisecond, 8 * time.Millisecond, 15 * time.Millisecond,
		30 * time.Millisecond, 50 * time.Millisecond, 80 * time.Millisecond, 120 * time.Millisecond, 180 * time.Millisecond,
		250 * time.Millisecond, 400 * time.Millisecond, 700 * time.Millisecond, 1200 * time.Millisecond} {
		rctx, cancel := context.WithCancel(ctx)
		timer := time.AfterFunc(d, cancel)
		err := Run(rctx, Options{Config: cfg, ShutdownTimeout: 10 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		timer.Stop()
		cancel()
		if err != nil {
			t.Errorf("stop after %s: Run returned %v, want nil (clean stop)", d, err)
		}
	}
}
