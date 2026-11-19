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
}

func (s *testSSHServer) requested() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
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
