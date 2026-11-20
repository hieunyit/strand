package sshtun

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/tunnel/tunneltest"
)

func TestOpenForConnection_NilTunnel(t *testing.T) {
	ctx := context.Background()
	conn := backup.Connection{
		Host:   "db.internal",
		Port:   3306,
		Engine: backup.ConnMySQL,
	}
	h, p, closeFn, err := OpenForConnection(ctx, conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h != "db.internal" || p != 3306 {
		t.Fatalf("expected db.internal:3306, got %s:%d", h, p)
	}
	if closeFn == nil {
		t.Fatal("expected non-nil closeFn")
	}
	closeFn() // should be no-op
}

func TestOpenForConnection_RefuseURISecret(t *testing.T) {
	ctx := context.Background()
	keyDir := t.TempDir()
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)

	srv := tunneltest.StartSSHServer(t, false)
	identData, err := os.ReadFile(srv.IdentityFile())
	if err != nil {
		t.Fatal(err)
	}
	identPath := filepath.Join(keyDir, "id_ed25519")
	if err := os.WriteFile(identPath, identData, 0o600); err != nil {
		t.Fatal(err)
	}

	connStored := backup.Connection{
		Host:       "db.internal",
		Port:       27017,
		Engine:     backup.ConnMongoDB,
		SecretMode: backup.SecretStored,
		URISet:     true,
		SSHTunnel: &backup.SSHTunnel{
			Host:          "bastion.internal",
			User:          "ops",
			IdentityRef:   identPath,
			KnownHostsRef: srv.KnownHostsFile(),
		},
	}
	_, _, _, err = OpenForConnection(ctx, connStored)
	if err == nil {
		t.Fatal("expected error when connection has URISet and SSHTunnel, got nil")
	}
	if !strings.Contains(err.Error(), "ssh_tunnel cannot be used with a connection URI; use host/port/user") {
		t.Fatalf("unexpected error: %v", err)
	}

	connEnv := backup.Connection{
		Host:       "db.internal",
		Port:       27017,
		Engine:     backup.ConnMongoDB,
		SecretMode: backup.SecretEnv,
		URIEnv:     "MONGO_URI",
		SSHTunnel: &backup.SSHTunnel{
			Host:          "bastion.internal",
			User:          "ops",
			IdentityRef:   identPath,
			KnownHostsRef: srv.KnownHostsFile(),
		},
	}
	_, _, _, err = OpenForConnection(ctx, connEnv)
	if err == nil {
		t.Fatal("expected error when connection has URIEnv and SSHTunnel, got nil")
	}
	if !strings.Contains(err.Error(), "ssh_tunnel cannot be used with a connection URI; use host/port/user") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOpenForConnection_MissingKnownHosts(t *testing.T) {
	ctx := context.Background()
	conn := backup.Connection{
		Host:   "db.internal",
		Port:   3306,
		Engine: backup.ConnMySQL,
		SSHTunnel: &backup.SSHTunnel{
			Host: "bastion.internal",
			User: "ops",
		},
	}
	_, _, _, err := OpenForConnection(ctx, conn)
	if err == nil {
		t.Fatal("expected error for missing known_hosts_ref, got nil")
	}
	if !strings.Contains(err.Error(), "ssh_tunnel needs known_hosts_ref") {
		t.Fatalf("expected 'ssh_tunnel needs known_hosts_ref', got %v", err)
	}
}

func TestOpenForConnection_IdentityOutsideKeyDir(t *testing.T) {
	ctx := context.Background()
	keyDir := filepath.Join(t.TempDir(), "allowed-keys")
	_ = os.MkdirAll(keyDir, 0o700)
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)

	outsideDir := t.TempDir()
	outsideKey := filepath.Join(outsideDir, "id_rsa")
	_ = os.WriteFile(outsideKey, []byte("key-data"), 0o600)

	srv := tunneltest.StartSSHServer(t, false)
	knownHosts := srv.KnownHostsFile()

	conn := backup.Connection{
		Host:   "db.internal",
		Port:   3306,
		Engine: backup.ConnMySQL,
		SSHTunnel: &backup.SSHTunnel{
			Host:          "bastion.internal",
			User:          "ops",
			IdentityRef:   outsideKey,
			KnownHostsRef: knownHosts,
		},
	}
	_, _, _, err := OpenForConnection(ctx, conn)
	if err == nil {
		t.Fatal("expected error for key outside STRAND_WEB_SSH_KEY_DIR, got nil")
	}
	if !strings.Contains(err.Error(), "STRAND_WEB_SSH_KEY_DIR") {
		t.Fatalf("expected STRAND_WEB_SSH_KEY_DIR error, got %v", err)
	}
}

func TestOpenForConnection_SuccessEcho(t *testing.T) {
	srv := tunneltest.StartSSHServer(t, false)
	keyDir := t.TempDir()
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)

	// Copy identity into keyDir
	identInDir := filepath.Join(keyDir, "id_ed25519")
	data, err := os.ReadFile(srv.IdentityFile())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(identInDir, data, 0o600); err != nil {
		t.Fatal(err)
	}

	echoAddr := tunneltest.EchoServer(t)
	echoHost, echoPortStr, _ := net.SplitHostPort(echoAddr)
	echoPort, _ := strconv.Atoi(echoPortStr)

	bastionHost, bastionPortStr, _ := net.SplitHostPort(srv.Addr())
	bastionPort, _ := strconv.Atoi(bastionPortStr)

	conn := backup.Connection{
		Host:   echoHost,
		Port:   echoPort,
		Engine: backup.ConnMySQL,
		SSHTunnel: &backup.SSHTunnel{
			Host:          bastionHost,
			Port:          bastionPort,
			User:          "ops",
			IdentityRef:   identInDir,
			KnownHostsRef: srv.KnownHostsFile(),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h, p, closeFn, err := OpenForConnection(ctx, conn)
	if err != nil {
		t.Fatalf("OpenForConnection failed: %v", err)
	}
	if h != "127.0.0.1" || p <= 0 {
		t.Fatalf("expected 127.0.0.1:<port>, got %s:%d", h, p)
	}

	// Send traffic to the forwarded listener
	targetAddr := net.JoinHostPort(h, strconv.Itoa(p))
	tcpConn, err := net.DialTimeout("tcp", targetAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("failed to connect to forwarded port: %v", err)
	}
	msg := "hello-via-tunnel\n"
	if _, err := tcpConn.Write([]byte(msg)); err != nil {
		t.Fatalf("failed to write: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(tcpConn, buf); err != nil {
		t.Fatalf("failed to read echo: %v", err)
	}
	if string(buf) != msg {
		t.Fatalf("expected %q, got %q", msg, string(buf))
	}
	_ = tcpConn.Close()

	// Verify closeFn closes the listener
	closeFn()
	time.Sleep(50 * time.Millisecond)
	_, dialErr := net.DialTimeout("tcp", targetAddr, 200*time.Millisecond)
	if dialErr == nil {
		t.Fatal("expected connection to fail after closeFn, but it succeeded")
	}
}
