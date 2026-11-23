package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"strand/internal/config"
	"strand/internal/tunnel/tunneltest"
)

func TestPipelinePostgresCDCThroughSSH(t *testing.T) {
	source, srcDSN := startTestDB(t, "pg")
	if _, err := source.Exec(`CREATE DATABASE tunnel_dst`); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`
		CREATE TABLE public.items(id bigint PRIMARY KEY);
		INSERT INTO public.items VALUES (1);
		CREATE PUBLICATION ssh_pub FOR TABLE public.items
	`); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(srcDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/tunnel_dst"
	destination, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()

	conf := &config.Config{
		Name:   "tunneled_pg",
		Source: config.SourceConfig{Kind: "postgres", DSN: srcDSN, Slot: "ssh_slot", Publication: "ssh_pub", CreateSlot: true},
		Target: config.TargetConfig{Kind: "postgres", DSN: u.String()},
		Tables: config.TablesConfig{Include: []string{"public.items"}},
	}
	assertCDCThroughSSH(t, conf, source, destination, "public.items", "$1")
}

func TestPipelineMySQLCDCThroughSSH(t *testing.T) {
	source, srcDSN := startTestDB(t, "mysql")
	destination, dstDSN := startTestDB(t, "mysql")
	if _, err := source.Exec("CREATE TABLE app.items(id bigint PRIMARY KEY); INSERT INTO app.items VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	conf := &config.Config{
		Name:   "tunneled_mysql",
		Source: config.SourceConfig{Kind: "mysql", DSN: srcDSN, ServerID: 88341},
		Target: config.TargetConfig{Kind: "mysql", DSN: dstDSN},
		Tables: config.TablesConfig{Include: []string{"app.items"}},
	}
	assertCDCThroughSSH(t, conf, source, destination, "app.items", "?")
}

func assertCDCThroughSSH(t *testing.T, conf *config.Config, source, destination *sql.DB, table, placeholder string) {
	t.Helper()
	bastion := tunneltest.StartSSHServer(t, false)
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", filepath.Dir(bastion.IdentityFile()))
	t.Setenv("STRAND_WEB_SSH_KNOWN_HOSTS", bastion.KnownHostsFile())
	host, rawPort, err := net.SplitHostPort(bastion.Addr())
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatal(err)
	}
	bastionConfig := &config.SSHTunnelConfig{Host: host, Port: port, User: "strand", IdentityFile: bastion.IdentityFile()}
	conf.Source.SSHTunnel = bastionConfig
	conf.Target.SSHTunnel = bastionConfig

	conf.Resources.Spool.Path = t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	run := make(chan error, 1)
	go func() {
		run <- Run(ctx, Options{Config: conf, ShutdownTimeout: 3 * time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
		close(run)
	}()
	defer func() {
		cancel()
		if err := <-run; err != nil {
			t.Errorf("stop pipeline: %v", err)
		}
	}()
	await := func(id int) {
		t.Helper()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			var n int
			if err := destination.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE id="+placeholder, id).Scan(&n); err == nil && n == 1 {
				return
			}
			select {
			case err := <-run:
				t.Fatalf("pipeline stopped while waiting for row %d: %v", id, err)
			case <-ctx.Done():
				t.Fatal(fmt.Errorf("waiting for row %d: %w", id, ctx.Err()))
			case <-ticker.C:
			}
		}
	}
	await(1)
	if _, err := source.Exec("INSERT INTO " + table + " VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	await(2)
	if len(bastion.Requested()) < 3 {
		t.Fatalf("expected SQL metadata, replication and writer connections through the bastion, got %v", bastion.Requested())
	}
}
