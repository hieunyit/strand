package pipeline

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"strand/internal/config"
)

// Cột chuỗi charset không phải UTF-8 (latin1) qua snapshot VÀ qua stream MySQL→MySQL phải giữ đúng ký tự.
// Binlog đưa giá trị dạng byte theo charset của cột; SELECT đưa theo charset của kết nối (utf8mb4).
func TestCharset_Latin1ValuesSurviveSnapshotAndStream(t *testing.T) {
	db, dsn := startTestDB(t, "mysql")
	ctx := context.Background()
	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec("CREATE DATABASE dst")
	mustExec("CREATE TABLE app.l1 (id BIGINT PRIMARY KEY, s VARCHAR(20) CHARACTER SET latin1, u VARCHAR(20) CHARACTER SET utf8mb4, tx TEXT CHARACTER SET latin1)")
	mustExec("CREATE TABLE dst.l1 LIKE app.l1")
	mustExec("INSERT INTO app.l1 VALUES (1, 'café Ñandú ü', 'café', 'naïve')")
	cfg := &config.Config{Name: "cs",
		Source: config.SourceConfig{Kind: "mysql", DSN: dsn, ServerID: 4545},
		Target: config.TargetConfig{Kind: "mysql", DSN: strings.Replace(dsn, "/app?", "/dst?", 1)},
		Tables: config.TablesConfig{Include: []string{"app.l1"}, Rename: map[string]string{"app.l1": "dst.l1"}}}
	cfg.Resources.Spool.Path = t.TempDir()
	cfg.Governor.Algorithm = "static"
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- Run(rctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	}()
	defer func() { cancel(); <-done }()
	wait := func(id int, want string) {
		t.Helper()
		var got string
		for i := 0; i < 300; i++ {
			if db.QueryRowContext(ctx, "SELECT CONCAT(s, '|', u, '|', tx) FROM dst.l1 WHERE id = ?", id).Scan(&got) == nil && got == want {
				return
			}
			select {
			case err := <-done:
				t.Fatalf("pipeline exited: %v", err)
			default:
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("row %d on target = %q, want %q", id, got, want)
	}
	wait(1, "café Ñandú ü|café|naïve") // snapshot
	mustExec("INSERT INTO app.l1 VALUES (2, 'Ångström ß', 'Ångström', 'Ærø')")
	wait(2, "Ångström ß|Ångström|Ærø") // stream
}
