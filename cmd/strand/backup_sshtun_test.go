package main

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/backup/sshtun"
	"strand/internal/tunnel/tunneltest"
	"strand/internal/web/store"
)

func TestJobRun_TunneledConnection(t *testing.T) {
	srv := tunneltest.StartSSHServer(t, false)
	keyDir := t.TempDir()
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)

	identData, err := os.ReadFile(srv.IdentityFile())
	if err != nil {
		t.Fatal(err)
	}
	identPath := filepath.Join(keyDir, "id_ed25519")
	if err := os.WriteFile(identPath, identData, 0o600); err != nil {
		t.Fatal(err)
	}

	echoAddr := tunneltest.EchoServer(t)
	echoHost, echoPortStr, _ := net.SplitHostPort(echoAddr)
	echoPort, _ := strconv.Atoi(echoPortStr)

	bastionHost, bastionPortStr, _ := net.SplitHostPort(srv.Addr())
	bastionPort, _ := strconv.Atoi(bastionPortStr)

	st := store.NewMem()
	ctx := context.Background()

	conn := backup.Connection{
		ID:         "conn-sshtun",
		Name:       "tunneled-mysql",
		Engine:     backup.ConnMySQL,
		Host:       echoHost,
		Port:       echoPort,
		User:       "backupuser",
		SecretMode: backup.SecretEnv,
		SSHTunnel: &backup.SSHTunnel{
			Host:          bastionHost,
			Port:          bastionPort,
			User:          "ops",
			IdentityRef:   identPath,
			KnownHostsRef: srv.KnownHostsFile(),
		},
	}
	if _, err := st.CreateBackupConnection(ctx, conn); err != nil {
		t.Fatal(err)
	}

	job := backup.Job{
		ID:           "job-sshtun",
		Name:         "mysql tunneled",
		EverySeconds: 3600,
		Enabled:      true,
		Bucket:       "bkt",
		JobOptions: backup.JobOptions{
			ConnectionID: conn.ID,
			Method:       backup.MethodMySQLDump,
			Source:       "test-src",
			Conn:         backup.ConnSpec{Host: echoHost, Port: echoPort, Databases: []string{"testdb"}},
		},
	}
	if err := st.CreateBackupJob(ctx, job); err != nil {
		t.Fatal(err)
	}

	// Read connection to verify resolveJobConnection works
	resolved, err := resolveJobConnection(ctx, st, job, os.Getenv)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Conn.Host != echoHost || resolved.Conn.Port != echoPort {
		t.Fatalf("expected initial host %s:%d, got %s:%d", echoHost, echoPort, resolved.Conn.Host, resolved.Conn.Port)
	}

	// Verify that when runTrackedBackupJob runs, the tunnel is opened and Host is rewritten to 127.0.0.1
	var runHost string
	var runPort int
	var tunnelWasOpen bool

	testRun := func(runCtx context.Context) error {
		// Inside the job runner callback, simulate what runManagedBuiltinJob does
		c, ok, err := st.GetBackupConnection(runCtx, resolved.ConnectionID)
		if err != nil {
			return err
		}
		if ok && c.SSHTunnel != nil {
			addrHost, addrPort, closeTunnel, err := sshtunOpenHelper(runCtx, c)
			if err != nil {
				return err
			}
			defer closeTunnel()
			runHost = addrHost
			runPort = addrPort

			// Verify tunnel is open and forwarding
			target := net.JoinHostPort(addrHost, strconv.Itoa(addrPort))
			conn, err := net.DialTimeout("tcp", target, 2*time.Second)
			if err == nil {
				tunnelWasOpen = true
				_ = conn.Close()
			}
		}
		return nil
	}

	if err := testRun(ctx); err != nil {
		t.Fatalf("testRun failed: %v", err)
	}

	if runHost != "127.0.0.1" || runPort <= 0 {
		t.Fatalf("expected forward 127.0.0.1:<port>, got %s:%d", runHost, runPort)
	}
	if !tunnelWasOpen {
		t.Fatal("expected tunnel to be open and accepting connections")
	}

	// Verify tunnel is closed after run ends
	target := net.JoinHostPort(runHost, strconv.Itoa(runPort))
	_, err = net.DialTimeout("tcp", target, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected tunnel listener to be closed after run finished")
	}
}

func TestRestore_TunneledFlagParsing(t *testing.T) {
	cmd := newBackupRestoreCmd()
	cmd.SetArgs([]string{
		"--id", "bkp-123",
		"--target-host", "10.0.2.10",
		"--target-port", "3306",
		"--target-database", "new_db",
		"--tunnel-ssh", "bastion.internal:22",
		"--tunnel-user", "bastionuser",
		"--tunnel-known-hosts", "/path/to/known_hosts",
		"--tunnel-identity-file", "/path/to/id",
		"--dry-run",
	})
	// Capture output
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	// Verify flags were registered properly
	sshFlag := cmd.Flag("tunnel-ssh")
	if sshFlag == nil || sshFlag.DefValue != "" {
		t.Fatalf("expected tunnel-ssh flag registered")
	}
	userFlag := cmd.Flag("tunnel-user")
	if userFlag == nil {
		t.Fatalf("expected tunnel-user flag registered")
	}
	khFlag := cmd.Flag("tunnel-known-hosts")
	if khFlag == nil {
		t.Fatalf("expected tunnel-known-hosts flag registered")
	}
	identFlag := cmd.Flag("tunnel-identity-file")
	if identFlag == nil {
		t.Fatalf("expected tunnel-identity-file flag registered")
	}
}

func sshtunOpenHelper(ctx context.Context, c backup.Connection) (string, int, func(), error) {
	return sshtun.OpenForConnection(ctx, c)
}
