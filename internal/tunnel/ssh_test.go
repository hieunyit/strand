package tunnel

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

type testSSHServer struct {
	addr      string
	knownHost string // known_hosts file trusting this server
	identity  string // client private key file
	mu        sync.Mutex
	targets   []string
	dropKA    bool // stop answering global requests (dead bastion)
	conn      net.Conn
}

func (s *testSSHServer) requested() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
}

func (s *testSSHServer) disconnect() {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func startSSHServer(t *testing.T, dropKeepalive bool) *testSSHServer {
	t.Helper()
	dir := t.TempDir()
	_, hostPriv, _ := ed25519.GenerateKey(rand.Reader)
	hostSigner, _ := ssh.NewSignerFromKey(hostPriv)
	clientPub, clientPriv, _ := ed25519.GenerateKey(rand.Reader)
	sshClientPub, _ := ssh.NewPublicKey(clientPub)
	block, err := ssh.MarshalPrivateKey(clientPriv, "")
	if err != nil {
		t.Fatal(err)
	}
	srv := &testSSHServer{identity: filepath.Join(dir, "id"), knownHost: filepath.Join(dir, "known_hosts"), dropKA: dropKeepalive}
	os.WriteFile(srv.identity, pem.EncodeToMemory(block), 0o600)
	cfg := &ssh.ServerConfig{PublicKeyCallback: func(_ ssh.ConnMetadata, k ssh.PublicKey) (*ssh.Permissions, error) {
		if string(k.Marshal()) == string(sshClientPub.Marshal()) {
			return nil, nil
		}
		return nil, io.EOF
	}}
	cfg.AddHostKey(hostSigner)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	srv.addr = ln.Addr().String()
	os.WriteFile(srv.knownHost, []byte(knownhosts.Line([]string{srv.addr}, hostSigner.PublicKey())+"\n"), 0o600)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.serve(c, cfg)
		}
	}()
	return srv
}

func (s *testSSHServer) serve(c net.Conn, cfg *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(c, cfg)
	if err != nil {
		c.Close()
		return
	}
	s.mu.Lock()
	s.conn = c
	s.mu.Unlock()
	if s.dropKA {
		go func() { // read but never reply: requests time out on the client
			for range reqs {
			}
		}()
	} else {
		go ssh.DiscardRequests(reqs)
	}
	for nc := range chans {
		if nc.ChannelType() != "direct-tcpip" {
			nc.Reject(ssh.UnknownChannelType, "no")
			continue
		}
		var p struct {
			Host       string
			Port       uint32
			OriginHost string
			OriginPort uint32
		}
		_ = ssh.Unmarshal(nc.ExtraData(), &p)
		target := net.JoinHostPort(p.Host, strconv.Itoa(int(p.Port)))
		s.mu.Lock()
		s.targets = append(s.targets, target)
		s.mu.Unlock()
		up, err := net.Dial("tcp", target)
		if err != nil {
			nc.Reject(ssh.ConnectionFailed, err.Error())
			continue
		}
		ch, creqs, _ := nc.Accept()
		go ssh.DiscardRequests(creqs)
		go func() { io.Copy(ch, up); ch.CloseWrite(); ch.Close() }()
		go func() { io.Copy(up, ch); up.Close() }()
	}
}

func TestRejectsChangedHostKeyAtSameAddress(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	srv := startSSHServer(t, false)
	other := startSSHServer(t, false)
	otherEntry, err := os.ReadFile(other.knownHost)
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(otherEntry))
	if len(fields) < 3 {
		t.Fatalf("malformed test known_hosts entry %q", otherEntry)
	}
	changed := knownhosts.Line([]string{srv.addr}, mustParseHostKey(t, fields[1]+" "+fields[2])) + "\n"
	if err := os.WriteFile(srv.knownHost, []byte(changed), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Start(context.Background(), Config{SSHAddress: srv.addr, SSHUser: "u", IdentityFile: srv.identity, HostKeysFile: srv.knownHost, Remote: "127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "host-key verification failed") {
		t.Fatalf("changed host key error = %v, want host-key verification failure", err)
	}
}

func TestPinnedHostKeyRejectsDifferentServerEvenWithTrustedFile(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	srv := startSSHServer(t, false)
	other := startSSHServer(t, false)
	key, err := ProbeHostKey(context.Background(), other.addr)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{SSHAddress: srv.addr, SSHUser: "u", IdentityFile: srv.identity, HostKey: key, HostKeysFile: srv.knownHost}
	if err := Check(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "host key mismatch") {
		t.Fatalf("expected mismatched pin to fail even with a matching known_hosts file: %v", err)
	}
}

func TestSSHDialerReconnectsAndSupportsDeadlines(t *testing.T) {
	srv := startSSHServer(t, false)
	key, err := ProbeHostKey(t.Context(), srv.addr)
	if err != nil {
		t.Fatal(err)
	}
	dialer := NewDialer(t.Context(), func(context.Context) (Config, error) {
		identity, err := os.ReadFile(srv.identity)
		return Config{SSHAddress: srv.addr, SSHUser: "strand", HostKey: key, PrivateKey: identity}, err
	})
	defer dialer.Close()
	addr := echoServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	checkEcho := func(conn net.Conn) {
		t.Helper()
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Write([]byte("hello")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 5)
		if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "hello" {
			t.Fatalf("read SSH echo: %q, %v", buf, err)
		}
	}
	first, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	checkEcho(first)
	srv.disconnect()
	for {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			checkEcho(conn)
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("SSH session did not reconnect: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func mustParseHostKey(t *testing.T, encoded string) ssh.PublicKey {
	t.Helper()
	key, _, _, _, err := ssh.ParseAuthorizedKey([]byte(encoded))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestForwardClosesAfterSSHConnectionDrops(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	srv := startSSHServer(t, false)
	f, err := Start(context.Background(), Config{SSHAddress: srv.addr, SSHUser: "backup", IdentityFile: srv.identity, HostKeysFile: srv.knownHost, Remote: echoServer(t), KeepAlive: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	local, err := net.DialTimeout("tcp", f.Address(), time.Second)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	defer local.Close()
	if _, err := local.Write([]byte("before-drop")); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	got := make([]byte, len("before-drop"))
	_ = local.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(local, got); err != nil || string(got) != "before-drop" {
		_ = f.Close()
		t.Fatalf("pre-drop echo %q: %v", got, err)
	}
	srv.disconnect()
	select {
	case <-f.Done():
	case <-time.After(3 * time.Second):
		_ = f.Close()
		t.Fatal("forward did not close after SSH connection loss")
	}
	_ = local.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := local.Read(make([]byte, 1)); err == nil {
		t.Fatal("existing local connection remained open after SSH connection loss")
	}
	if _, err := net.DialTimeout("tcp", f.Address(), 200*time.Millisecond); err == nil {
		t.Fatal("closed forward still accepts connections")
	}
}

func echoServer(t *testing.T) string {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { io.Copy(c, c); c.Close() }()
		}
	}()
	return ln.Addr().String()
}

func TestForwardOnlyToConfiguredRemote(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	srv := startSSHServer(t, false)
	remote := echoServer(t)
	f, err := Start(context.Background(), Config{SSHAddress: srv.addr, SSHUser: "backup", IdentityFile: srv.identity, HostKeysFile: srv.knownHost, Remote: remote})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if !strings.HasPrefix(f.Address(), "127.0.0.1:") {
		t.Fatalf("listener %s is not loopback", f.Address())
	}
	for i := 0; i < 2; i++ {
		c, err := net.Dial("tcp", f.Address())
		if err != nil {
			t.Fatal(err)
		}
		c.Write([]byte("ping\n"))
		buf := make([]byte, 5)
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "ping\n" {
			t.Fatalf("echo through tunnel: %q %v", buf, err)
		}
		c.Close()
	}
	for _, target := range srv.requested() {
		if target != remote {
			t.Fatalf("tunnel forwarded to %s; only %s is allowed (no open relay)", target, remote)
		}
	}
}

func TestRejectsUnknownHostKeyAndNonLoopbackListen(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	srv := startSSHServer(t, false)
	other := startSSHServer(t, false) // trusted file of a different server
	_, err := Start(context.Background(), Config{SSHAddress: srv.addr, SSHUser: "u", IdentityFile: srv.identity, HostKeysFile: other.knownHost, Remote: "127.0.0.1:1"})
	if err == nil {
		t.Fatal("unknown host key must be rejected")
	}
	_, err = Start(context.Background(), Config{SSHAddress: srv.addr, SSHUser: "u", IdentityFile: srv.identity, HostKeysFile: srv.knownHost, Remote: "127.0.0.1:1", LocalAddress: "0.0.0.0:0"})
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback listen err = %v", err)
	}
}

// A port that accepts TCP but never speaks SSH must not hang the backup run (and its execution lock).
func TestHandshakeTimeoutAndCancel(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	srv := startSSHServer(t, false)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	os.WriteFile(srv.knownHost+"2", nil, 0o600)
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := Start(context.Background(), Config{SSHAddress: ln.Addr().String(), SSHUser: "u", IdentityFile: srv.identity, HostKeysFile: srv.knownHost + "2", Remote: "127.0.0.1:1", HandshakeTimeout: 300 * time.Millisecond})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || time.Since(start) > 3*time.Second {
			t.Fatalf("err=%v after %v", err, time.Since(start))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SSH handshake against a silent server hung")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := Start(ctx, Config{SSHAddress: ln.Addr().String(), SSHUser: "u", IdentityFile: srv.identity, HostKeysFile: srv.knownHost + "2", Remote: "127.0.0.1:1", HandshakeTimeout: time.Minute})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled handshake succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not stop the SSH handshake")
	}
}

// A bastion that stops answering is detected by keepalives and the forward closes.
func TestKeepAliveClosesDeadForward(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	srv := startSSHServer(t, true)
	f, err := Start(context.Background(), Config{SSHAddress: srv.addr, SSHUser: "u", IdentityFile: srv.identity, HostKeysFile: srv.knownHost, Remote: "127.0.0.1:1", KeepAlive: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("forward to an unresponsive SSH server was not closed")
	}
	if _, err := net.DialTimeout("tcp", f.Address(), time.Second); err == nil {
		t.Fatal("closed forward still accepts connections")
	}
}
