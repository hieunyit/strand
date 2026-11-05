package pipeline

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"strings"
	"testing"
	"time"

	"strand/internal/config"
)

// review R05 (invariant: RS-1 — ảnh row ở target bằng ảnh source): MySQL→MySQL với DSN source đặt
// time_zone khác UTC. TIMESTAMP qua snapshot và qua stream phải giữ đúng thời điểm (UNIX_TIMESTAMP).
func TestR05_MySQLTimestampSurvivesNonUTCSourceSession(t *testing.T) {
	db, dsn := startTestDB(t, "mysql")
	ctx := context.Background()
	mustExec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec("CREATE DATABASE dst")
	mustExec("CREATE TABLE app.ev (id BIGINT PRIMARY KEY, ts TIMESTAMP(3) NULL)")
	mustExec("CREATE TABLE dst.ev LIKE app.ev")
	mustExec("SET time_zone = '+00:00'")
	mustExec("INSERT INTO app.ev VALUES (1, '2026-01-01 00:00:00.250')")

	srcDSN := dsn + "&time_zone=" + url.QueryEscape("'+07:00'")
	cfg := &config.Config{Name: "tz_r05",
		Source: config.SourceConfig{Kind: "mysql", DSN: srcDSN, ServerID: 4242},
		Target: config.TargetConfig{Kind: "mysql", DSN: strings.Replace(dsn, "/app?", "/dst?", 1)},
		Tables: config.TablesConfig{Include: []string{"app.ev"}, Rename: map[string]string{"app.ev": "dst.ev"}}}
	cfg.Resources.Spool.Path = t.TempDir()
	cfg.Resources.Workers.Max = 2
	cfg.Governor.Algorithm = "static"
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Run(rctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	}()
	defer func() {
		cancel()
		<-done
	}()
	epochs := func(id int) (src, dst float64, ok bool) {
		if db.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP(ts) FROM app.ev WHERE id = ?", id).Scan(&src) != nil {
			return 0, 0, false
		}
		if db.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP(ts) FROM dst.ev WHERE id = ?", id).Scan(&dst) != nil {
			return 0, 0, false
		}
		return src, dst, true
	}
	wait := func(id int) {
		t.Helper()
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if s, d, ok := epochs(id); ok {
				if s != d {
					t.Fatalf("row %d: source epoch %.3f, target epoch %.3f (drift %.0f s)", id, s, d, d-s)
				}
				return
			}
			select {
			case err := <-done:
				done <- err
				t.Fatalf("pipeline exited: %v", err)
			default:
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("row %d not replicated", id)
	}
	wait(1) // snapshot
	mustExec("INSERT INTO app.ev VALUES (2, '2026-06-30 23:59:59.999')")
	wait(2) // stream
}
