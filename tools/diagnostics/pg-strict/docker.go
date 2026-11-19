package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
)

type dockerRunner func(args ...string) ([]byte, error)

func dockerCommand(args ...string) ([]byte, error) {
	cmd := exec.Command("docker", args...)
	out, err := cmd.Output() // successful pull/warning stderr is not a container ID
	if exit, ok := err.(*exec.ExitError); ok {
		return out, fmt.Errorf("%w: %s", err, exit.Stderr)
	}
	return out, err
}

func startContainer(role string, port int, cpus string, run dockerRunner) (id string, hostPort int, err error) {
	var token [12]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", 0, err
	}
	name := "strand-strict-" + role + "-" + hex.EncodeToString(token[:])
	// Loopback binding with an empty host port asks Docker to allocate atomically.
	binding := "127.0.0.1::5432"
	if port != 0 {
		binding = fmt.Sprintf("127.0.0.1:%d:5432", port)
	}
	args := []string{"run", "-d", "--name", name, "--label", "strand.diagnostic.run=" + name, "--cpus=" + cpus,
		"-p", binding, "-e", "POSTGRES_PASSWORD=dev", "-e", "POSTGRES_DB=strand", "postgres:18"}
	if role == "source" {
		args = append(args, "-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4")
	}
	args = append(args, "-c", "fsync=on", "-c", "synchronous_commit=on", "-c", "shared_buffers=256MB")
	out, err := run(args...)
	if err != nil {
		return "", 0, fmt.Errorf("create %s container: %w: %s", role, err, out)
	}
	id = strings.TrimSpace(string(out))
	// docker run returns a full container ID. Refuse unexpected output as a cleanup target.
	decoded, decodeErr := hex.DecodeString(id)
	if decodeErr != nil || len(decoded) != 32 {
		return "", 0, fmt.Errorf("docker returned invalid container ID")
	}
	out, err = run("port", id, "5432/tcp")
	if err != nil {
		return id, 0, fmt.Errorf("discover %s port: %w: %s", role, err, out)
	}
	host, portText, err := net.SplitHostPort(strings.TrimSpace(string(out)))
	if err != nil || host != "127.0.0.1" {
		return id, 0, fmt.Errorf("unexpected Docker port binding %q", out)
	}
	hostPort, err = strconv.Atoi(portText)
	if err != nil || hostPort <= 0 || hostPort > 65535 {
		return id, 0, fmt.Errorf("invalid Docker host port %q", portText)
	}
	return id, hostPort, nil
}

func cleanupContainers(ids []string, run dockerRunner) {
	for _, id := range ids {
		_, _ = run("rm", "-f", id)
	}
}
