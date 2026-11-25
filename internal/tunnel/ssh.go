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

// Config describes an SSH local forward. Supply a pinned HostKey or a trusted HostKeysFile.
type Config struct {
	SSHAddress   string // host:port; port defaults to 22
	SSHUser      string
	IdentityFile string
	PrivateKey   []byte // in-memory SSH identity, mutually exclusive with IdentityFile
	Password     []byte // SSH password authentication, mutually exclusive with keys
	HostKey      ssh.PublicKey
	HostKeysFile string
	LocalAddress string // host:port; bind to loopback by default
	Remote       string // host:port as resolved from the SSH server
	// HandshakeTimeout bounds the TCP connect plus SSH handshake/authentication (default 15s).
	HandshakeTimeout time.Duration
	// KeepAlive is the interval of SSH keepalive requests (default 30s); the forward is closed when the
	// server stops answering, so a backup client fails instead of hanging on a dead bastion.
	KeepAlive time.Duration
}

// ClearCredentials wipes the in-memory credentials loaded for a tunnel.
func (cfg Config) ClearCredentials() {
	clear(cfg.PrivateKey)
	clear(cfg.Password)
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
	if cfg.LocalAddress == "" {
		cfg.LocalAddress = "127.0.0.1:0"
	}
	if err := validateLocalAddress(cfg.LocalAddress); err != nil {
		return nil, fmt.Errorf("tunnel: invalid local address: %w", err)
	}
	if err := validateTCPAddress(cfg.Remote); err != nil {
		return nil, fmt.Errorf("tunnel: invalid remote address: %w", err)
	}
	client, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if cfg.KeepAlive <= 0 {
		cfg.KeepAlive = 30 * time.Second
	}
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
	go f.keepAlive(cfg.KeepAlive)
	go func() {
		select {
		case <-ctx.Done():
			_ = f.Close()
		case <-f.closed:
		}
	}()
	return f, nil
}

// Check verifies the bastion's host key and SSH authentication without opening a forward.
func Check(ctx context.Context, cfg Config) error {
	client, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	_ = client.Close()
	return nil
}

// ProbeHostKey reads the server key and aborts before exchanging credentials.
func ProbeHostKey(ctx context.Context, address string) (ssh.PublicKey, error) {
	var offered ssh.PublicKey
	cfg := &ssh.ClientConfig{User: "strand-host-key-check", HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
		offered = key
		return errors.New("SSH host key probe stops before authentication")
	}}
	client, err := handshake(ctx, address, 0, cfg)
	if client != nil {
		_ = client.Close()
	}
	if offered != nil {
		return offered, nil
	}
	return nil, err
}

func connect(ctx context.Context, cfg Config) (*ssh.Client, error) {
	if strings.TrimSpace(cfg.SSHUser) == "" {
		return nil, errors.New("tunnel: SSH user is required")
	}
	var hostKeys ssh.HostKeyCallback
	if cfg.HostKey != nil {
		hostKeys = ssh.FixedHostKey(cfg.HostKey)
	} else {
		if cfg.HostKeysFile == "" {
			return nil, errors.New("tunnel: trusted host key or known_hosts file is required")
		}
		var err error
		hostKeys, err = knownhosts.New(cfg.HostKeysFile)
		if err != nil {
			return nil, fmt.Errorf("tunnel: load known_hosts: %w", err)
		}
	}
	auth, cleanup, err := authMethods(cfg.IdentityFile, cfg.PrivateKey, cfg.Password)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return handshake(ctx, cfg.SSHAddress, cfg.HandshakeTimeout, &ssh.ClientConfig{
		User: cfg.SSHUser, Auth: auth, HostKeyCallback: hostKeys,
	})
}

func handshake(ctx context.Context, address string, timeout time.Duration, clientCfg *ssh.ClientConfig) (*ssh.Client, error) {
	if ctx == nil {
		return nil, errors.New("tunnel: context is required")
	}
	sshAddr, err := normalizeSSHAddress(address)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", sshAddr)
	if err != nil {
		return nil, fmt.Errorf("tunnel: connect to SSH server: %w", err)
	}
	// ClientConfig.Timeout only applies to ssh.Dial; with NewClientConn a server that accepts TCP but never
	// speaks SSH would block forever (and with it the backup job's execution lock). Bound the handshake by
	// a deadline and by ctx.
	_ = conn.SetDeadline(time.Now().Add(timeout))
	stopWatch := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	cc, chans, reqs, err := ssh.NewClientConn(conn, sshAddr, clientCfg)
	stopWatch()
	if err != nil {
		_ = conn.Close()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("tunnel: SSH handshake cancelled: %w", ctx.Err())
		}
		return nil, fmt.Errorf("tunnel: SSH authentication or host-key verification failed: %w", err)
	}
	_ = conn.SetDeadline(time.Time{})
	return ssh.NewClient(cc, chans, reqs), nil
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

func authMethods(identityFile string, privateKey, password []byte) ([]ssh.AuthMethod, func(), error) {
	var methods []ssh.AuthMethod
	var closers []io.Closer
	cleanup := func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}
	if len(password) > 0 {
		if identityFile != "" || len(privateKey) > 0 {
			return nil, cleanup, errors.New("tunnel: choose SSH password or private key authentication")
		}
		return []ssh.AuthMethod{ssh.PasswordCallback(func() (string, error) { return string(password), nil })}, cleanup, nil
	}
	if len(privateKey) > 0 {
		if identityFile != "" {
			return nil, cleanup, errors.New("tunnel: choose an inline SSH private key or an identity file")
		}
		key, err := ssh.ParsePrivateKey(privateKey)
		if err != nil {
			return nil, cleanup, errors.New("tunnel: invalid SSH private key or unsupported passphrase-protected key")
		}
		methods = append(methods, ssh.PublicKeys(key))
		return methods, cleanup, nil
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

func (f *Forward) keepAlive(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-f.closed:
			return
		case <-t.C:
		}
		reply := make(chan error, 1)
		go func() {
			_, _, err := f.client.SendRequest("keepalive@openssh.com", true, nil)
			reply <- err
		}()
		timeout := time.NewTimer(every)
		select {
		case err := <-reply:
			timeout.Stop()
			if err != nil {
				_ = f.Close()
				return
			}
		case <-timeout.C:
			_ = f.Close()
			return
		case <-f.closed:
			timeout.Stop()
			return
		}
	}
}

// Done is closed when the forward stops (Close, ctx end, or a dead SSH connection).
func (f *Forward) Done() <-chan struct{} { return f.closed }

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

// ValidateSecurity checks identity and known_hosts paths according to Strand's SSH security policy:
// knownHosts is required, must exist as a regular file, and must contain valid trusted host keys.
// If identityFile is provided, STRAND_WEB_SSH_KEY_DIR must be configured, identityFile must resolve
// within it without escaping, must be a regular file, and its permissions must not allow group or other access.
func ValidateSecurity(identityFile, knownHosts string) error {
	if strings.TrimSpace(knownHosts) == "" {
		return errors.New("known_hosts: known_hosts file is required")
	}
	knownHosts = filepath.Clean(knownHosts)
	khInfo, err := os.Stat(knownHosts)
	if err != nil || !khInfo.Mode().IsRegular() {
		return errors.New("known_hosts: path must be an existing regular file")
	}
	if _, err := knownhosts.New(knownHosts); err != nil {
		return fmt.Errorf("known_hosts: cannot load trusted host keys: %w", err)
	}

	return validateIdentityFile(identityFile)
}

// PrepareServiceTrust resolves the configured trust file for profiles without a pinned host key.
func (cfg *Config) PrepareServiceTrust() error {
	if cfg.HostKey != nil {
		return validateIdentityFile(cfg.IdentityFile)
	}
	if cfg.HostKeysFile == "" {
		file, err := ConfiguredKnownHosts()
		if err != nil {
			return err
		}
		cfg.HostKeysFile = file
	}
	return ValidateSecurity(cfg.IdentityFile, cfg.HostKeysFile)
}

func validateIdentityFile(identityFile string) error {
	if strings.TrimSpace(identityFile) != "" {
		keyDir := os.Getenv("STRAND_WEB_SSH_KEY_DIR")
		if keyDir == "" {
			return errors.New("identity_file: SSH key directory not configured (STRAND_WEB_SSH_KEY_DIR is required)")
		}
		cleanKey, err := filepath.EvalSymlinks(identityFile)
		if err != nil {
			return errors.New("identity_file: key file does not exist or cannot be resolved")
		}
		cleanKey, _ = filepath.Abs(cleanKey)
		cleanKeyDir, err := filepath.EvalSymlinks(keyDir)
		if err != nil {
			return errors.New("identity_file: configured SSH key directory does not exist")
		}
		cleanKeyDir, _ = filepath.Abs(cleanKeyDir)
		if cleanKey != cleanKeyDir && !strings.HasPrefix(cleanKey, cleanKeyDir+string(filepath.Separator)) {
			return errors.New("identity_file: key path must be within allow-listed directory STRAND_WEB_SSH_KEY_DIR")
		}
		st, err := os.Stat(cleanKey)
		if err != nil || !st.Mode().IsRegular() {
			return errors.New("identity_file: key must be a regular file")
		}
		if st.Mode().Perm()&0o077 != 0 {
			return errors.New("identity_file: permissions must not allow group or other access")
		}
	}
	return nil
}

// DefaultKnownHosts returns ~/.ssh/known_hosts without creating the file.
func DefaultKnownHosts() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".ssh", "known_hosts"), nil
}

// ConfiguredKnownHosts resolves the trust file for server-managed tunnels.
func ConfiguredKnownHosts() (string, error) {
	if path := strings.TrimSpace(os.Getenv("STRAND_WEB_SSH_KNOWN_HOSTS")); path != "" {
		if !filepath.IsAbs(path) {
			return "", errors.New("STRAND_WEB_SSH_KNOWN_HOSTS must be an absolute path")
		}
		return filepath.Clean(path), nil
	}
	return DefaultKnownHosts()
}
