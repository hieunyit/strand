package web

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func TestWebTunnelOpensAndStopsVerifiedLoopbackForward(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	keyDir := t.TempDir()
	t.Setenv("STRAND_WEB_SSH_KEY_DIR", keyDir)
	sshAddr, identity, hosts := webTestSSHServer(t, keyDir)
	t.Setenv("STRAND_WEB_SSH_KNOWN_HOSTS", hosts)
	remote := webTestEchoServer(t)

	e := opsEnv(t, false, nil)
	e.sampleNow()
	c, csrf := e.loginOK()
	hdr := map[string]string{csrfHeader: csrf, "Content-Type": "application/json"}
	payload, _ := json.Marshal(map[string]string{"ssh": sshAddr, "user": "backup", "remote": remote, "listen": "127.0.0.1:0", "identity_file": identity})
	resp, body := e.do(c, "POST", "/api/v1/tunnels", string(payload), hdr)
	if resp.StatusCode != 200 {
		t.Fatalf("open tunnel: status=%d body=%s", resp.StatusCode, body)
	}
	var opened struct {
		Tunnel struct {
			ID           string `json:"id"`
			LocalAddress string `json:"local_address"`
		} `json:"tunnel"`
	}
	if err := json.Unmarshal([]byte(body), &opened); err != nil {
		t.Fatal(err)
	}
	if opened.Tunnel.ID == "" {
		t.Fatalf("missing tunnel ID: %s", body)
	}
	local, err := net.DialTimeout("tcp", opened.Tunnel.LocalAddress, 2*time.Second)
	if err != nil {
		t.Fatalf("connect local tunnel: %v", err)
	}
	if _, err := local.Write([]byte("verified-forward")); err != nil {
		t.Fatal(err)
	}
	_ = local.SetReadDeadline(time.Now().Add(2 * time.Second))
	got := make([]byte, len("verified-forward"))
	if _, err := io.ReadFull(local, got); err != nil || string(got) != "verified-forward" {
		t.Fatalf("tunnel echo %q: %v", got, err)
	}
	_ = local.Close()

	resp, body = e.do(c, "POST", "/api/v1/tunnels/"+opened.Tunnel.ID+"/stop", `{}`, hdr)
	if resp.StatusCode != 200 {
		t.Fatalf("stop tunnel: status=%d body=%s", resp.StatusCode, body)
	}
	if conn, err := net.DialTimeout("tcp", opened.Tunnel.LocalAddress, 150*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("local tunnel listener remains open after stop")
	}
	if audits := e.auditFor("tunnel.start"); len(audits) < 2 {
		t.Fatalf("expected start and success audit records, got %d", len(audits))
	}
	if len(e.auditFor("tunnel.stop")) == 0 {
		t.Fatal("missing tunnel.stop audit record")
	}
}

func webTestSSHServer(t *testing.T, dir string) (addr, identity, hosts string) {
	t.Helper()
	_, hostPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	hostSigner, err := ssh.NewSignerFromKey(hostPriv)
	if err != nil {
		t.Fatal(err)
	}
	clientPub, clientPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(clientPub)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(clientPriv, "")
	if err != nil {
		t.Fatal(err)
	}
	identity = filepath.Join(dir, "client-key")
	if err := os.WriteFile(identity, pem.EncodeToMemory(block), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &ssh.ServerConfig{PublicKeyCallback: func(_ ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if string(key.Marshal()) == string(sshPub.Marshal()) {
			return nil, nil
		}
		return nil, io.EOF
	}}
	cfg.AddHostKey(hostSigner)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	addr = ln.Addr().String()
	hosts = filepath.Join(dir, "known_hosts")
	if err := os.WriteFile(hosts, []byte(knownhosts.Line([]string{addr}, hostSigner.PublicKey())+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_, chans, reqs, err := ssh.NewServerConn(conn, cfg)
				if err != nil {
					_ = conn.Close()
					return
				}
				go ssh.DiscardRequests(reqs)
				for nc := range chans {
					if nc.ChannelType() != "direct-tcpip" {
						_ = nc.Reject(ssh.UnknownChannelType, "unsupported")
						continue
					}
					var target struct {
						Host       string
						Port       uint32
						OriginHost string
						OriginPort uint32
					}
					if err := ssh.Unmarshal(nc.ExtraData(), &target); err != nil {
						_ = nc.Reject(ssh.ConnectionFailed, "invalid target")
						continue
					}
					up, err := net.Dial("tcp", net.JoinHostPort(target.Host, strconv.Itoa(int(target.Port))))
					if err != nil {
						_ = nc.Reject(ssh.ConnectionFailed, "dial failed")
						continue
					}
					ch, requests, err := nc.Accept()
					if err != nil {
						_ = up.Close()
						continue
					}
					go ssh.DiscardRequests(requests)
					go func() { _, _ = io.Copy(ch, up); _ = ch.CloseWrite(); _ = ch.Close(); _ = up.Close() }()
					go func() { _, _ = io.Copy(up, ch); _ = up.Close() }()
				}
			}()
		}
	}()
	return addr, identity, hosts
}

func webTestEchoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(conn, conn); _ = conn.Close() }()
		}
	}()
	return ln.Addr().String()
}
