package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"strand/internal/config"
	"strand/internal/model"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCDCPublicationRejectsIncompleteScope(t *testing.T) {
	src, srcDSN := startTestDB(t, "pg")
	if _, err := src.Exec("CREATE DATABASE review_dst"); err != nil {
		t.Fatal(err)
	}
	dstDSN := strings.Replace(srcDSN, "/app?", "/review_dst?", 1)
	if _, err := src.Exec("CREATE TABLE public.a(id bigint primary key, v int); CREATE TABLE public.b(id bigint primary key); CREATE PUBLICATION review_pub FOR TABLE public.a"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, change, message string }{
		{"missing_table", "", "does not include public.b"},
		{"row_filter", "ALTER PUBLICATION review_pub SET TABLE public.a WHERE (id > 10), public.b", "row filter for public.a"},
		{"omitted_column", "ALTER PUBLICATION review_pub SET TABLE public.a (id), public.b", "omits columns of public.a"},
		{"missing_delete", "ALTER PUBLICATION review_pub SET TABLE public.a, public.b; ALTER PUBLICATION review_pub SET (publish='insert,update')", "must publish insert, update and delete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.change != "" {
				if _, err := src.Exec(tc.change); err != nil {
					t.Fatal(err)
				}
			}
			cfg := &config.Config{Name: "review_pg", Source: config.SourceConfig{Kind: "postgres", DSN: srcDSN, Slot: "review_slot", Publication: "review_pub", CreateSlot: true}, Target: config.TargetConfig{Kind: "postgres", DSN: dstDSN}, Tables: config.TablesConfig{Include: []string{"public.a", "public.b"}}}
			cfg.Resources.Spool.Path = t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			err := Run(ctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("sync error = %v, want %q", err, tc.message)
			}
		})
	}
}

func TestCDCRouterReportsTerminalError(t *testing.T) {

	for i := 0; i < 100; i++ {
		p := &Pipeline{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
		p.seenCond = sync.NewCond(&p.seenMu)
		p.pauseCond = sync.NewCond(&p.pauseMu)
		txns := make(chan *model.Txn)
		streamErr := make(chan error, 1)
		streamErr <- errors.New("replication disconnected")
		close(txns)
		p.router(context.Background(), txns, streamErr)
		if p.failed() == nil {
			t.Fatalf("lost terminal error in trial %d", i)
		}
	}

}

func TestCDCMySQLStopsBeforeTruncate(t *testing.T) {
	src, srcDSN := startTestDB(t, "mysql")
	dst, dstDSN := startTestDB(t, "mysql")
	exec := func(db *sql.DB, q string) {
		t.Helper()
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	exec(src, "CREATE TABLE app.events(id bigint primary key); INSERT INTO app.events VALUES(1)")
	cfg := &config.Config{Name: "review_mysql", Source: config.SourceConfig{Kind: "mysql", DSN: srcDSN, ServerID: 88001}, Target: config.TargetConfig{Kind: "mysql", DSN: dstDSN}, Tables: config.TablesConfig{Include: []string{"app.events"}}}
	cfg.Resources.Spool.Path = t.TempDir()
	cfg.Governor.Algorithm = "static"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{Config: cfg, ShutdownTimeout: 5 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	}()
	defer func() {
		cancel()
		if done != nil {
			select {
			case <-done:
			case <-time.After(8 * time.Second):
			}
		}
	}()
	wait := func(id int) {
		t.Helper()
		deadline := time.Now().Add(35 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			if dst.QueryRow("SELECT count(*) FROM app.events WHERE id=?", id).Scan(&n) == nil && n == 1 {
				return
			}
			select {
			case err := <-done:
				t.Fatalf("pipeline stopped: %v", err)
			default:
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("target did not receive id %d", id)
	}
	wait(1)
	exec(src, "INSERT INTO app.events VALUES(2)")
	wait(2)
	exec(src, "TRUNCATE TABLE app.events; INSERT INTO app.events VALUES(3)")
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "TRUNCATE on replicated table app.events") {
			t.Fatalf("sync error = %v", err)
		}
		done = nil
	case <-time.After(20 * time.Second):
		t.Fatal("pipeline did not stop at TRUNCATE")
	}
	var n int
	if err := dst.QueryRow("SELECT count(*) FROM app.events WHERE id=3").Scan(&n); err != nil || n != 0 {
		t.Fatalf("post-TRUNCATE row reached the target or query failed: n=%d err=%v", n, err)
	}
}

func TestCDCMySQLRejectsCascadeWithExcludedParent(t *testing.T) {
	src, _ := startTestDB(t, "mysql")
	if _, err := src.Exec("CREATE TABLE app.parents(id bigint primary key); CREATE TABLE app.children(id bigint primary key, parent_id bigint, CONSTRAINT fk_parent FOREIGN KEY(parent_id) REFERENCES app.parents(id) ON DELETE CASCADE)"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	filter, err := newTableFilter([]string{"app.children"}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	child := model.TableID{Schema: "app", Table: "children"}
	p := &Pipeline{src: src, dst: src, filter: filter, srcEngine: model.EngineMySQL, dstEngine: model.EngineMySQL, tables: map[model.TableID]*tableMeta{child: {}}}
	_, err = p.planMySQLCascade(ctx)
	if err == nil || !strings.Contains(err.Error(), "requires its parent app.parents") {
		t.Fatalf("cascade preflight error = %v", err)
	}
}
