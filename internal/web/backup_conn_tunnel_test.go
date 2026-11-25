package web

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"strand/internal/backup"
	"strand/internal/sshprofile"
	"strand/internal/tunnel/tunneltest"
)

func createTestKnownHosts(t *testing.T) string {
	t.Helper()
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostSigner, err := ssh.NewSignerFromKey(hostPriv)
	if err != nil {
		t.Fatal(err)
	}
	khFile := filepath.Join(t.TempDir(), "known_hosts")
	line := "bastion.example.com " + string(ssh.MarshalAuthorizedKey(hostSigner.PublicKey()))
	if err := os.WriteFile(khFile, []byte(strings.TrimSpace(line)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return khFile
}

func TestConnTest_UnreachableBastion(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	khFile := createTestKnownHosts(t)

	// Test unsaved connection with unreachable bastion
	body := a.must("POST", "/api/v1/backup-connections/test", map[string]any{
		"engine": "mysql", "host": "10.255.255.1", "port": 3306, "user": "root",
		"secret_mode": "stored",
		"ssh_tunnel": map[string]any{
			"host":            "127.0.0.1",
			"port":            1, // unreachable port
			"user":            "ops",
			"known_hosts_ref": khFile,
		},
	}, 200)

	var res struct {
		OK     bool `json:"ok"`
		Checks []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
			Detail string `json:"detail"`
		} `json:"checks"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("decode test result: %v", err)
	}
	if res.OK {
		t.Fatal("expected test to fail due to unreachable bastion")
	}
	var foundSSHTunnelCheck bool
	for _, chk := range res.Checks {
		if chk.Name == "ssh_tunnel" {
			foundSSHTunnelCheck = true
			if chk.Status != "fail" {
				t.Fatalf("expected ssh_tunnel check status 'fail', got %q", chk.Status)
			}
		}
	}
	if !foundSSHTunnelCheck {
		t.Fatalf("expected check named 'ssh_tunnel' in checks list, got: %+v", res.Checks)
	}
}

func TestConnTest_SuccessWithTunnel(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()

	keyDir := filepath.Join(t.TempDir(), "allowed-keys")
	_ = os.MkdirAll(keyDir, 0o700)
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

	bastionHost, bastionPortStr, _ := net.SplitHostPort(srv.Addr())
	bastionPort, _ := strconv.Atoi(bastionPortStr)

	body := a.must("POST", "/api/v1/backup-connections/test", map[string]any{
		"engine": "mysql", "host": "db.internal", "port": 3306, "user": "root",
		"secret_mode": "stored",
		"ssh_tunnel": map[string]any{
			"host":            bastionHost,
			"port":            bastionPort,
			"user":            "ops",
			"identity_ref":    identPath,
			"known_hosts_ref": srv.KnownHostsFile(),
		},
	}, 200)

	var res struct {
		OK     bool `json:"ok"`
		Checks []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
			Detail string `json:"detail"`
		} `json:"checks"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("decode test result: %v", err)
	}
	if !res.OK {
		t.Fatalf("expected test to pass, got error: %s", res.Error)
	}
	if len(res.Checks) == 0 || res.Checks[0].Name != "ssh_tunnel" || res.Checks[0].Status != "ok" {
		t.Fatalf("expected first check to be ssh_tunnel: ok, got: %+v", res.Checks)
	}
	lastTgt := e.fake.last()
	if lastTgt.Host != "127.0.0.1" || lastTgt.TLSServerName != "db.internal" {
		t.Fatalf("checker received unexpected target: host=%s tlsServerName=%s", lastTgt.Host, lastTgt.TLSServerName)
	}
}

func TestConnSave_InvalidIdentityOrMissingKnownHosts(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()

	keyDir := filepath.Join(t.TempDir(), "allowed-keys")
	_ = os.MkdirAll(keyDir, 0o700)
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)

	khFile := createTestKnownHosts(t)

	// 1. Missing known_hosts_ref when ssh_tunnel is provided
	resp, body := a.do("POST", "/api/v1/backup-connections", map[string]any{
		"name": "conn-no-kh", "engine": "mysql", "host": "db.internal", "user": "root",
		"secret_mode": "stored",
		"ssh_tunnel": map[string]any{
			"host": "bastion.internal",
			"user": "ops",
		},
	})
	if resp != 400 || !strings.Contains(body, "known_hosts") {
		t.Fatalf("expected 400 with known_hosts error, got %d: %s", resp, body)
	}

	// 2. IdentityRef outside STRAND_WEB_SSH_KEY_DIR
	resp, body = a.do("POST", "/api/v1/backup-connections", map[string]any{
		"name": "conn-bad-key", "engine": "mysql", "host": "db.internal", "user": "root",
		"secret_mode": "stored",
		"ssh_tunnel": map[string]any{
			"host":            "bastion.internal",
			"user":            "ops",
			"identity_ref":    "/etc/shadow",
			"known_hosts_ref": khFile,
		},
	})
	if resp != 400 || !strings.Contains(body, "STRAND_WEB_SSH_KEY_DIR") {
		t.Fatalf("expected 400 with STRAND_WEB_SSH_KEY_DIR error, got %d: %s", resp, body)
	}

	// 3. Stored URI with ssh_tunnel refused with 400
	validKey := filepath.Join(keyDir, "id_rsa")
	_ = os.WriteFile(validKey, []byte("valid-private-key"), 0o600)
	resp, body = a.do("POST", "/api/v1/backup-connections", map[string]any{
		"name": "conn-uri-tunnel", "engine": "mongodb", "host": "mongo.internal", "user": "root",
		"secret_mode": "stored",
		"uri":         "mongodb://mongo.internal:27017/admin",
		"ssh_tunnel": map[string]any{
			"host":            "bastion.internal",
			"user":            "ops",
			"identity_ref":    validKey,
			"known_hosts_ref": khFile,
		},
	})
	if resp != 400 || !strings.Contains(body, "ssh_tunnel cannot be used with a connection URI; use host/port/user") {
		t.Fatalf("expected 400 with URI error, got %d: %s", resp, body)
	}

	// 4. Env URI (uri_env) with ssh_tunnel refused with 400
	resp, body = a.do("POST", "/api/v1/backup-connections", map[string]any{
		"name": "conn-urienv-tunnel", "engine": "mongodb", "host": "mongo.internal", "user": "root",
		"secret_mode": "stored",
		"uri_env":     "MONGO_URI",
		"ssh_tunnel": map[string]any{
			"host":            "bastion.internal",
			"user":            "ops",
			"identity_ref":    validKey,
			"known_hosts_ref": khFile,
		},
	})
	if resp != 400 || !strings.Contains(body, "ssh_tunnel cannot be used with a connection URI; use host/port/user") {
		t.Fatalf("expected 400 with URI error, got %d: %s", resp, body)
	}
}

func TestConnTest_RefuseSSHTunnelWithURI(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	khFile := createTestKnownHosts(t)

	keyDir := filepath.Join(t.TempDir(), "allowed-keys")
	_ = os.MkdirAll(keyDir, 0o700)
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)
	validKey := filepath.Join(keyDir, "id_rsa")
	_ = os.WriteFile(validKey, []byte("valid-private-key"), 0o600)

	body := a.must("POST", "/api/v1/backup-connections/test", map[string]any{
		"engine":      "mongodb",
		"host":        "mongo.internal",
		"secret_mode": "stored",
		"uri":         "mongodb://mongo.internal:27017/admin",
		"ssh_tunnel": map[string]any{
			"host":            "bastion.internal",
			"user":            "ops",
			"identity_ref":    validKey,
			"known_hosts_ref": khFile,
		},
	}, 200)

	var res struct {
		OK     bool `json:"ok"`
		Checks []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
			Detail string `json:"detail"`
		} `json:"checks"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		t.Fatalf("decode test result: %v", err)
	}
	if res.OK {
		t.Fatal("expected test to fail (ok:false) when uri is provided with ssh_tunnel")
	}
	var foundSSHTunnelCheck bool
	for _, chk := range res.Checks {
		if chk.Name == "ssh_tunnel" {
			foundSSHTunnelCheck = true
			if chk.Status != "fail" {
				t.Fatalf("expected ssh_tunnel check status 'fail', got %q", chk.Status)
			}
			if !strings.Contains(chk.Detail, "ssh_tunnel cannot be used with a connection URI; use host/port/user") {
				t.Fatalf("expected error message in detail, got %q", chk.Detail)
			}
		}
	}
	if !foundSSHTunnelCheck {
		t.Fatalf("expected check named 'ssh_tunnel' in checks list, got: %+v", res.Checks)
	}
}

func TestRestore_ChildGetsTunnelParamsNoSecretInArgv(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()

	keyDir := filepath.Join(t.TempDir(), "allowed-keys")
	_ = os.MkdirAll(keyDir, 0o700)
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)

	// Create valid key inside keyDir
	validKey := filepath.Join(keyDir, "id_rsa")
	_ = os.WriteFile(validKey, []byte("valid-private-key"), 0o600)

	khFile := createTestKnownHosts(t)

	// Create connection with SSHTunnel
	connID := connID(t, a.must("POST", "/api/v1/backup-connections", map[string]any{
		"name": "target-tunneled", "engine": "mysql", "host": "10.0.1.5", "port": 3306, "user": "restoreuser",
		"secret_mode": "stored", "password": "super-secret-target-password",
		"ssh_tunnel": map[string]any{
			"host":            "bastion.example.com",
			"port":            2222,
			"user":            "tunneluser",
			"identity_ref":    validKey,
			"known_hosts_ref": khFile,
		},
	}, 201))

	// Seed backup
	tTime, _ := time.Parse(time.RFC3339, "2026-11-20T10:00:00Z")
	tBase := tTime.Add(-1 * time.Hour)
	seedRichBackup(t, e.st, "bkp-restore-tun", "mysql-prod", "mysql", "mysqldump", backup.KindFull, tBase, backup.Position{GTIDSet: "u:1-100"}, false, nil, "", &tBase, nil, "", nil)

	req := map[string]any{
		"backup_id":       "bkp-restore-tun",
		"connection_id":   connID,
		"target_database": "restored_db",
		"confirm":         "restored_db",
	}

	code, body := a.do("POST", "/api/v1/backup-restores", req)
	if code != http.StatusAccepted {
		t.Fatalf("restore request returned %d: %s", code, body)
	}

	specs := e.rec.take()
	if len(specs) != 1 {
		t.Fatalf("expected 1 job spec, got %d", len(specs))
	}

	args := strings.Join(specs[0].Args, " ")
	// Check that tunnel parameters are in argv
	if !strings.Contains(args, "--tunnel-ssh bastion.example.com:2222") {
		t.Errorf("argv missing --tunnel-ssh: %s", args)
	}
	if !strings.Contains(args, "--tunnel-user tunneluser") {
		t.Errorf("argv missing --tunnel-user: %s", args)
	}
	if !strings.Contains(args, "--tunnel-known-hosts "+khFile) {
		t.Errorf("argv missing --tunnel-known-hosts: %s", args)
	}
	if !strings.Contains(args, "--tunnel-identity-file "+validKey) {
		t.Errorf("argv missing --tunnel-identity-file: %s", args)
	}

	// Verify no secrets appear in argv
	if strings.Contains(args, "super-secret-target-password") {
		t.Errorf("argv leaks secret password: %s", args)
	}
}

func TestRestore_UsesSavedTunnelProfile(t *testing.T) {
	e := newCPEnv(t)
	a := e.admin()
	profile := sshprofile.Profile{ID: "shared-restore", Host: "bastion.example.com", User: "sshuser", Port: 22}
	if err := sshprofile.Save(t.Context(), e.st, []sshprofile.Profile{profile}); err != nil {
		t.Fatal(err)
	}
	id := connID(t, a.must("POST", "/api/v1/backup-connections", map[string]any{
		"name": "shared-mysql", "engine": "mysql", "host": "db.internal", "user": "restoreuser",
		"secret_mode": "stored", "ssh_tunnel": map[string]any{"profile": profile.ID},
	}, http.StatusCreated))
	recordedAt := time.Now().UTC()
	seedRichBackup(t, e.st, "bkp-profile", "mysql-prod", "mysql", "mysqldump", backup.KindFull, recordedAt,
		backup.Position{GTIDSet: "u:1-100"}, false, nil, "", &recordedAt, nil, "", nil)
	a.must("POST", "/api/v1/backup-restores", map[string]any{
		"backup_id": "bkp-profile", "connection_id": id, "target_database": "restored_db", "confirm": "restored_db",
	}, http.StatusAccepted)
	specs := e.rec.take()
	if len(specs) != 1 {
		t.Fatalf("expected one restore job, got %d", len(specs))
	}
	args := strings.Join(specs[0].Args, " ")
	if !strings.Contains(args, "--tunnel-profile shared-restore") || strings.Contains(args, "--tunnel-ssh") || strings.Contains(args, "bastion.example.com") {
		t.Fatalf("restore did not use only the saved profile reference: %s", args)
	}
}
