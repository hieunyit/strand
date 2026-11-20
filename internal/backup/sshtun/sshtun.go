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
	"strand/internal/tunnel"
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
	if c.SSHTunnel == nil {
		return c.Host, c.Port, func() {}, nil
	}
	if c.URISet || c.URIEnv != "" || len(c.URISealed) > 0 {
		return "", 0, nil, errors.New("ssh_tunnel cannot be used with a connection URI; use host/port/user")
	}
	tun := c.SSHTunnel
	if strings.TrimSpace(tun.KnownHostsRef) == "" {
		return "", 0, nil, errors.New("ssh_tunnel needs known_hosts_ref")
	}
	if err := ValidateSecurity(tun.IdentityRef, tun.KnownHostsRef); err != nil {
		return "", 0, nil, err
	}

	remotePort := c.Port
	if remotePort == 0 {
		remotePort = backup.DefaultPort(c.CatalogEngine())
	}
	if remotePort == 0 {
		return "", 0, nil, errors.New("ssh_tunnel: remote port is required")
	}
	remoteAddr := net.JoinHostPort(c.Host, strconv.Itoa(remotePort))

	sshPort := tun.Port
	if sshPort <= 0 {
		sshPort = 22
	}
	sshAddr := net.JoinHostPort(tun.Host, strconv.Itoa(sshPort))

	cfg := tunnel.Config{
		SSHAddress:   sshAddr,
		SSHUser:      tun.User,
		IdentityFile: tun.IdentityRef,
		HostKeysFile: tun.KnownHostsRef,
		Remote:       remoteAddr,
		LocalAddress: "127.0.0.1:0",
	}

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

	slog.Info("ssh tunnel opened", "bastion", tun.Host, "bastion_port", sshPort, "remote_host", c.Host, "remote_port", remotePort, "local_addr", localAddr)

	closed := false
	closeFunc := func() {
		if !closed {
			closed = true
			_ = f.Close()
			slog.Info("ssh tunnel closed", "bastion", tun.Host, "remote_host", c.Host, "local_addr", localAddr)
		}
	}

	return host, port, closeFunc, nil
}
