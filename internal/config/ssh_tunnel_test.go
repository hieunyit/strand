package config

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"strand/internal/tunnel/tunneltest"
)

func TestPipelineSSHTunnelRoutesSQLAndPreservesTLSNames(t *testing.T) {
	bastion := tunneltest.StartSSHServer(t, false)
	t.Setenv("STRAND_WEB_SSH_KNOWN_HOSTS", bastion.KnownHostsFile())
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", filepath.Dir(bastion.IdentityFile()))
	bastionHost, bastionPort, err := net.SplitHostPort(bastion.Addr())
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(bastionPort)
	ssh := &SSHTunnelConfig{Host: bastionHost, Port: port, User: "test", IdentityFile: bastion.IdentityFile()}
	_, remotePort, err := net.SplitHostPort(tunneltest.EchoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	remote := net.JoinHostPort("localhost", remotePort)
	mysqlDSN := fmt.Sprintf("test@tcp(%s)/app?tls=true", remote)
	pgURL := url.URL{Scheme: "postgres", User: url.User("test"), Host: remote, Path: "/app", RawQuery: "sslmode=verify-full"}
	pgDSN := pgURL.String()
	cfg := Config{
		Source: SourceConfig{Kind: "mysql", DSN: mysqlDSN, SSHTunnel: ssh},
		Target: TargetConfig{Kind: "postgres", DSN: pgDSN, SSHTunnel: ssh},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cleanup, err := OpenSSHTunnels(ctx, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cleanup)
	my, err := mysql.ParseDSN(cfg.Source.DSN)
	if err != nil {
		t.Fatal(err)
	}
	if my.Addr == remote || my.TLS == nil || my.TLS.ServerName != "localhost" || my.TLS.InsecureSkipVerify {
		t.Fatalf("MySQL tunnel endpoint or TLS changed: addr=%q tls=%+v", my.Addr, my.TLS)
	}
	pg, err := ParsePGConnConfig(cfg.Target.DSN)
	if err != nil {
		t.Fatal(err)
	}
	if pg.Host != "127.0.0.1" || pg.TLSConfig == nil || pg.TLSConfig.ServerName != "localhost" || pg.TLSConfig.InsecureSkipVerify {
		t.Fatalf("PostgreSQL tunnel endpoint or TLS changed: host=%q tls=%+v", pg.Host, pg.TLSConfig)
	}
	for _, addr := range []string{my.Addr, net.JoinHostPort(pg.Host, strconv.Itoa(int(pg.Port)))} {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := io.WriteString(conn, "ping"); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
			t.Fatalf("tunneled connection: response %q: %v", buf, err)
		}
	}
	for _, got := range bastion.Requested() {
		if got != remote {
			t.Errorf("bastion resolved %q, want %q", got, remote)
		}
	}
	cleanup()
	if cfg.Source.DSN != mysqlDSN || cfg.Target.DSN != pgDSN {
		t.Fatal("configured DSNs were not restored")
	}
}

func TestPipelineSSHTunnelRejectsUntrustedBastion(t *testing.T) {
	bastion := tunneltest.StartSSHServer(t, false)
	unrelated := tunneltest.StartSSHServer(t, false)
	t.Setenv("STRAND_WEB_SSH_KNOWN_HOSTS", unrelated.KnownHostsFile())
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", filepath.Dir(bastion.IdentityFile()))
	host, rawPort, err := net.SplitHostPort(bastion.Addr())
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(rawPort)
	ssh := &SSHTunnelConfig{Host: host, Port: port, User: "test", IdentityFile: bastion.IdentityFile()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = ssh.Open(ctx, "mysql", "localhost:3306")
	if err == nil || !strings.Contains(err.Error(), "host-key verification failed") {
		t.Fatalf("expected host-key rejection, got %v", err)
	}
}
