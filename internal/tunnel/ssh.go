// Package tunnel provides verified SSH local port forwarding for backup jobs.
package tunnel

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
)

// Config describes an SSH local forward. HostKeysFile is required; this package
// never accepts unknown or changed host keys automatically.
type Config struct {
	SSHAddress   string // host:port; port defaults to 22
	SSHUser      string
	IdentityFile string
	HostKeysFile string
	LocalAddress string // host:port; bind to loopback by default
	Remote       string // host:port as resolved from the SSH server
}

// Forward owns one SSH session and a local TCP listener until Close or ctx end.
type Forward struct {
	listener net.Listener
	client   *ssh.Client
	closed   chan struct{}
	once     sync.Once
}

// Start opens a verified SSH connection and starts accepting local connections.
func Start(ctx context.Context, cfg Config) (*Forward, error) {
	if ctx == nil {
		return nil, errors.New("tunnel: context is required")
	}
	if strings.TrimSpace(cfg.SSHAddress) == "" || strings.TrimSpace(cfg.SSHUser) == "" || strings.TrimSpace(cfg.Remote) == "" {
		return nil, errors.New("tunnel: SSH address, SSH user, and remote address are required")
	}
	if cfg.HostKeysFile == "" {
		return nil, errors.New("tunnel: known_hosts file is required")
	}
	if cfg.LocalAddress == "" {
		cfg.LocalAddress = "127.0.0.1:0"
	}
	if err := validateLocalAddress(cfg.LocalAddress); err != nil {
		return nil, fmt.Errorf("tunnel: invalid local address: %w", err)
	}
	if err := validateTCPAddress(cfg.Remote); err != nil {
		return nil, fmt.Errorf("tunnel: invalid remote address: %w", err)
	}
	sshAddr, err := normalizeSSHAddress(cfg.SSHAddress)
	if err != nil {
		return nil, err
	}
	hostKeys, err := knownhosts.New(cfg.HostKeysFile)
	if err != nil {
		return nil, fmt.Errorf("tunnel: load known_hosts: %w", err)
	}
	auth, cleanup, err := authMethods(cfg.IdentityFile)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	clientCfg := &ssh.ClientConfig{
		User: cfg.SSHUser, Auth: auth, HostKeyCallback: hostKeys,
		Timeout: 15 * time.Second,
	}
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", sshAddr)
	if err != nil {
		return nil, fmt.Errorf("tunnel: connect to SSH server: %w", err)
	}
	cc, chans, reqs, err := ssh.NewClientConn(conn, sshAddr, clientCfg)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tunnel: SSH authentication or host-key verification failed: %w", err)
	}
	client := ssh.NewClient(cc, chans, reqs)
	listener, err := net.Listen("tcp", cfg.LocalAddress)
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("tunnel: listen on local address: %w", err)
	}
	if tcp, ok := listener.Addr().(*net.TCPAddr); !ok || !tcp.IP.IsLoopback() {
		_ = listener.Close()
		_ = client.Close()
		return nil, errors.New("tunnel: resolved local listener is not loopback")
	}
	f := &Forward{listener: listener, client: client, closed: make(chan struct{})}
	go f.accept(ctx, cfg.Remote)
	go func() {
		select {
		case <-ctx.Done():
			_ = f.Close()
		case <-f.closed:
		}
	}()
	return f, nil
}

func normalizeSSHAddress(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err == nil {
		if host == "" || port == "" {
			return "", errors.New("tunnel: SSH address must include a host and port")
		}
		return net.JoinHostPort(host, port), nil
	}
	if strings.Contains(err.Error(), "missing port in address") && !strings.Contains(address, ":") {
		return net.JoinHostPort(address, "22"), nil
	}
	return "", fmt.Errorf("tunnel: SSH address must be host or host:port: %w", err)
}

func validateTCPAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if host == "" || port == "" {
		return errors.New("host and port are required")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return errors.New("port must be between 1 and 65535")
	}
	return nil
}

func validateLocalAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return errors.New("local forwarding must bind to loopback only")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return errors.New("local port must be between 0 and 65535")
	}
	return nil
}

func authMethods(identityFile string) ([]ssh.AuthMethod, func(), error) {
	var methods []ssh.AuthMethod
	var closers []io.Closer
	cleanup := func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}
	if identityFile != "" {
		keyBytes, err := os.ReadFile(identityFile)
		if err != nil {
			return nil, cleanup, fmt.Errorf("tunnel: read SSH identity file: %w", err)
		}
		key, err := ssh.ParsePrivateKey(keyBytes)
		if err != nil {
			return nil, cleanup, fmt.Errorf("tunnel: parse SSH identity (encrypted keys are not supported; use an SSH agent): %w", err)
		}
		methods = append(methods, ssh.PublicKeys(key))
	}
	if socket := os.Getenv("SSH_AUTH_SOCK"); socket != "" {
		conn, err := net.DialTimeout("unix", socket, 2*time.Second)
		if err != nil {
			cleanup()
			return nil, cleanup, fmt.Errorf("tunnel: connect to SSH agent: %w", err)
		}
		closers = append(closers, conn)
		agentClient := agent.NewClient(conn)
		methods = append(methods, ssh.PublicKeysCallback(agentClient.Signers))
	}
	if len(methods) == 0 {
		cleanup()
		return nil, cleanup, errors.New("tunnel: provide --identity-file or SSH_AUTH_SOCK")
	}
	return methods, cleanup, nil
}

func (f *Forward) accept(ctx context.Context, remote string) {
	for {
		local, err := f.listener.Accept()
		if err != nil {
			select {
			case <-f.closed:
			default:
				_ = f.Close()
			}
			return
		}
		go func() {
			defer local.Close()
			if ctx.Err() != nil {
				return
			}
			upstream, err := f.client.Dial("tcp", remote)
			if err != nil {
				return
			}
			defer upstream.Close()
			copyDone := make(chan struct{}, 2)
			go func() { _, _ = io.Copy(upstream, local); closeWrite(upstream); copyDone <- struct{}{} }()
			go func() { _, _ = io.Copy(local, upstream); closeWrite(local); copyDone <- struct{}{} }()
			<-copyDone
			_ = local.SetDeadline(time.Now().Add(5 * time.Second))
			_ = upstream.SetDeadline(time.Now().Add(5 * time.Second))
			<-copyDone
		}()
	}
}

func closeWrite(c net.Conn) {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

// Address is the bound local endpoint; with port 0, callers should use this
// actual address to configure the backup client's connection.
func (f *Forward) Address() string { return f.listener.Addr().String() }

// Close stops accepting connections and closes the SSH transport.
func (f *Forward) Close() error {
	var err error
	f.once.Do(func() {
		close(f.closed)
		if closeErr := f.listener.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = closeErr
		}
		if closeErr := f.client.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	})
	return err
}

// DefaultKnownHosts returns ~/.ssh/known_hosts without creating the file.
func DefaultKnownHosts() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".ssh", "known_hosts"), nil
}
