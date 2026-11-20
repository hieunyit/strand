// Package tunneltest provides an in-process SSH server and test utilities for tunnel integration tests.
package tunneltest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// SSHServer is an in-process SSH server for tests.
type SSHServer struct {
	addr      string
	knownHost string // known_hosts file trusting this server
	identity  string // client private key file
	mu        sync.Mutex
	targets   []string
	dropKA    bool
	listener  net.Listener
}

// Addr returns the host:port the SSH server is listening on.
func (s *SSHServer) Addr() string { return s.addr }

// IdentityFile returns the path to the client private key accepted by this server.
func (s *SSHServer) IdentityFile() string { return s.identity }

// KnownHostsFile returns the path to a known_hosts file trusting this server.
func (s *SSHServer) KnownHostsFile() string { return s.knownHost }

// Requested returns the list of target addresses requested via direct-tcpip.
func (s *SSHServer) Requested() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
}

// StartSSHServer starts an in-process SSH server.
func StartSSHServer(t testing.TB, dropKeepalive bool) *SSHServer {
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
	srv := &SSHServer{identity: filepath.Join(dir, "id"), knownHost: filepath.Join(dir, "known_hosts"), dropKA: dropKeepalive}
	if err := os.WriteFile(srv.identity, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
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
	srv.listener = ln
	srv.addr = ln.Addr().String()
	line := knownhosts.Line([]string{srv.addr}, hostSigner.PublicKey()) + "\n"
	if err := os.WriteFile(srv.knownHost, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
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

func (s *SSHServer) serve(c net.Conn, cfg *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(c, cfg)
	if err != nil {
		c.Close()
		return
	}
	if s.dropKA {
		go func() {
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
		ch, creqs, err := nc.Accept()
		if err != nil {
			up.Close()
			continue
		}
		go ssh.DiscardRequests(creqs)
		go func() { io.Copy(ch, up); ch.CloseWrite(); ch.Close() }()
		go func() { io.Copy(up, ch); up.Close() }()
	}
}

// EchoServer starts a local TCP echo server on loopback and registers its cleanup.
func EchoServer(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln.Addr().String()
}
