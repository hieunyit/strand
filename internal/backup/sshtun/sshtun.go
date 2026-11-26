// Package sshtun provides helpers to open and manage SSH tunnels for database connections.
package sshtun

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"

	"strand/internal/backup"
	"strand/internal/sshprofile"
	"strand/internal/tunnel"
	"strand/internal/web/store"
)

// ValidateSecurity validates known_hosts and identity_ref against Strand's SSH security policy.
func ValidateSecurity(identityFile, knownHosts string) error {
	return tunnel.ValidateSecurity(identityFile, knownHosts)
}

// OpenForConnection opens an SSH tunnel for connection c if c.SSHTunnel != nil.
// If c.SSHTunnel is nil, it returns c.Host and c.Port unchanged with a no-op close function.
// Otherwise, it starts an in-process local port forward to c.Host:c.Port (defaulting port if 0)
// and returns 127.0.0.1, the forwarded local port, a close function, and nil error.
func OpenForConnection(ctx context.Context, c backup.Connection) (addrHost string, addrPort int, closeFn func(), err error) {
	return OpenForStoredConnection(ctx, c, nil, nil)
}

// OpenForStoredConnection also resolves a shared tunnel profile from metadata.
func OpenForStoredConnection(ctx context.Context, c backup.Connection, metadata sshprofile.Metadata, box *store.Box) (string, int, func(), error) {
	if c.SSHTunnel == nil {
		return c.Host, c.Port, func() {}, nil
	}
	if c.URISet || c.URIEnv != "" || len(c.URISealed) > 0 {
		return "", 0, nil, errors.New("ssh_tunnel cannot be used with a connection URI; use host/port/user")
	}
	tun := c.SSHTunnel
	sshPort := tun.Port
	if sshPort <= 0 {
		sshPort = 22
	}
	cfg := tunnel.Config{SSHAddress: net.JoinHostPort(tun.Host, strconv.Itoa(sshPort)),
		SSHUser: tun.User, IdentityFile: tun.IdentityRef, HostKeysFile: tun.KnownHostsRef}
	if tun.Profile != "" {
		if metadata == nil {
			return "", 0, nil, errors.New("SSH tunnel profile needs the metadata database")
		}
		var err error
		cfg, err = sshprofile.Config(ctx, metadata, tun.Profile, box)
		if err != nil {
			return "", 0, nil, err
		}
		defer cfg.ClearCredentials()
	} else if strings.TrimSpace(cfg.HostKeysFile) == "" {
		return "", 0, nil, errors.New("ssh_tunnel needs known_hosts_ref")
	}
	if err := cfg.PrepareServiceTrust(); err != nil {
		return "", 0, nil, err
	}
	remotePort := c.Port
	if remotePort == 0 {
		remotePort = backup.DefaultPort(c.CatalogEngine())
	}
	if remotePort == 0 {
		return "", 0, nil, errors.New("ssh_tunnel: remote port is required")
	}
	cfg.Remote = net.JoinHostPort(c.Host, strconv.Itoa(remotePort))

	f, err := tunnel.Start(ctx, cfg)
	if err != nil {
		return "", 0, nil, fmt.Errorf("start ssh tunnel: %w", err)
	}

	localAddr := f.Address()
	host, portStr, err := net.SplitHostPort(localAddr)
	if err != nil {
		_ = f.Close()
		return "", 0, nil, fmt.Errorf("parse tunnel local address %q: %w", localAddr, err)
	}
	port, _ := strconv.Atoi(portStr)

	slog.Info("ssh tunnel opened", "bastion", cfg.SSHAddress, "remote_host", c.Host, "remote_port", remotePort, "local_addr", localAddr)

	closed := false
	closeFunc := func() {
		if !closed {
			closed = true
			_ = f.Close()
			slog.Info("ssh tunnel closed", "bastion", cfg.SSHAddress, "remote_host", c.Host, "local_addr", localAddr)
		}
	}

	return host, port, closeFunc, nil
}
