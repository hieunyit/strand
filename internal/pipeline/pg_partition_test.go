package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"strand/internal/config"
	"strand/internal/exitcode"
)

// invariant: CP-1/RS-1 — review R02. Source PG có bảng partitioned: row của partition lá không bao giờ
// bị bỏ âm thầm. Publication mặc định → từ chối khi khởi động; publication bị đổi khi đang chạy → dừng
// với lỗi ở row đầu tiên của partition lá, không tiến checkpoint qua nó.
func TestR02_PGPartitionLeafRowsAreNeverSilentlyDropped(t *testing.T) {
	root, rootDSN := startTestDB(t, "pg")
	ctx := context.Background()
	dsn := func(db string) string { return strings.Replace(rootDSN, "/app?", "/"+db+"?", 1) }
	exec := func(t *testing.T, db *sql.DB, q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	setup := func(t *testing.T, name, pubOpts string) (*sql.DB, *sql.DB, *config.Config) {
		exec(t, root, "CREATE DATABASE src_"+name)
		exec(t, root, "CREATE DATABASE dst_"+name)
		src, err := sql.Open("pgx", dsn("src_"+name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { src.Close() })
		dst, err := sql.Open("pgx", dsn("dst_"+name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { dst.Close() })
		for _, q := range []string{"CREATE SCHEMA app",
			"CREATE TABLE app.events(id BIGINT PRIMARY KEY, v INT) PARTITION BY RANGE(id)",
			"CREATE TABLE app.events_p0 PARTITION OF app.events FOR VALUES FROM (0) TO (100)",
			"INSERT INTO app.events VALUES (1, 10)",
			"CREATE PUBLICATION pub FOR ALL TABLES" + pubOpts} {
			exec(t, src, q)
		}
		cfg := &config.Config{Name: "part_" + name,
			Source: config.SourceConfig{Kind: "postgres", DSN: dsn("src_" + name), Slot: "slot_" + name, Publication: "pub", CreateSlot: true},
			Target: config.TargetConfig{Kind: "postgres", DSN: dsn("dst_" + name)},
			Tables: config.TablesConfig{Include: []string{"app.*"}}}
		cfg.Resources.Spool.Path = t.TempDir()
		cfg.Resources.Workers.Max = 2
		cfg.Governor.Algorithm = "static"
		return src, dst, cfg
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("default_publication_rejected", func(t *testing.T) {
		_, _, cfg := setup(t, "default", "")
		rctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		err := Run(rctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: quiet})
		var e *exitcode.Error
		if !errors.As(err, &e) || e.Code != exitcode.ErrBinlogConfigInvalid || !strings.Contains(err.Error(), "publish_via_partition_root") {
			t.Fatalf("Run = %v, want rejection naming publish_via_partition_root", err)
		}
	})

	t.Run("publication_changed_while_running_stops", func(t *testing.T) {
		src, dst, cfg := setup(t, "changed", " WITH (publish_via_partition_root = true)")
		rctx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- Run(rctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: quiet}) }()
		waitRows := func(id, want int) {
			t.Helper()
			deadline := time.Now().Add(60 * time.Second)
			for time.Now().Before(deadline) {
				var n int
				_ = dst.QueryRow("SELECT COUNT(*) FROM app.events WHERE id = $1", id).Scan(&n)
				if n == want {
					return
				}
				select {
				case err := <-done:
					t.Fatalf("pipeline exited early: %v", err)
				default:
				}
				time.Sleep(100 * time.Millisecond)
			}
			t.Fatalf("row id=%d did not reach the target", id)
		}
		waitRows(1, 1)
		exec(t, src, "INSERT INTO app.events VALUES (2, 20)")
		waitRows(2, 1) // publish_via_partition_root: row đến dưới tên bảng gốc
		exec(t, src, "ALTER PUBLICATION pub SET (publish_via_partition_root = false)")
		exec(t, src, "INSERT INTO app.events VALUES (3, 30)")
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "partition app.events_p0") {
				t.Fatalf("Run = %v, want stop on partition leaf row", err)
			}
		case <-time.After(60 * time.Second):
			t.Fatal("pipeline kept running after a partition-leaf row: rows would be silently dropped")
		}
	})
}
