package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/jackc/pgx/v5"
)

func validateConfig(cfg Config) error {
	if cfg.AutoDocker && (cfg.SourceDSN != "" || cfg.TargetDSN != "") {
		return fmt.Errorf("external DSNs require -auto-docker=false; refusing to silently ignore them")
	}
	if !cfg.AutoDocker {
		if cfg.SourceDSN == "" || cfg.TargetDSN == "" {
			return fmt.Errorf("external source and target DSNs are required")
		}
		if !cfg.AllowDestructiveExistingDB {
			return fmt.Errorf("refusing to mutate external databases: use fresh isolated databases or explicitly pass -allow-destructive-existing-databases (TRUNCATE bench.accounts and replace strand_strict_diag_pub)")
		}
	}
	if cfg.Rows <= 0 || cfg.Clients <= 0 || cfg.Duration <= 0 || cfg.Warmup < 0 {
		return fmt.Errorf("rows, clients and duration must be positive, warmup nonnegative")
	}
	return nil
}

// Existing directories are never chmodded: an arbitrary -out-dir may be a
// shared directory or the workspace itself. Only directories created by this
// call may have permissions tightened. Reject symlinks in every path component
// before creating descendants, so a symlinked parent cannot redirect writes.
func privateDir(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	current := filepath.VolumeName(abs) + string(os.PathSeparator)
	for _, component := range strings.Split(strings.TrimPrefix(abs, current), string(os.PathSeparator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		created := false
		if os.IsNotExist(err) {
			err = os.Mkdir(current, 0o700)
			if err != nil && !os.IsExist(err) {
				return err
			}
			created = err == nil
			info, err = os.Lstat(current)
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlink directory component %q", current)
		}
		if !info.IsDir() {
			return fmt.Errorf("output directory component %q is not a directory", current)
		}
		if created {
			if err := requireOwnedDir(current, info); err != nil {
				return err
			}
			if info.Mode().Perm() != 0o700 {
				if err := os.Chmod(current, 0o700); err != nil {
					return err
				}
			}
		}
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return err
	}
	if err := requireOwnedDir(abs, info); err != nil {
		return err
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf("refusing existing output directory %q with mode %04o: choose a fresh path or an owned private 0700 directory (permissions left unchanged)", abs, info.Mode().Perm())
	}
	return nil
}

func requireOwnedDir(path string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("refusing output directory %q: ownership by the current user is not established", path)
	}
	return nil
}

// Atomic replacement also protects an existing permissive destination and avoids
// following a pre-existing symlink when writing a credential-bearing file.
func writePrivateFile(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".private-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// Replace both complete DSNs and extracted passwords: child errors may print a
// decoded password or a URL-escaped one, even when the original DSN was keyword syntax.
func newArtifactRedactor(dsns ...string) func(string) string {
	var replacements []string
	for _, dsn := range dsns {
		if dsn == "" {
			continue
		}
		replacements = append(replacements, dsn, "[REDACTED_DSN]")
		if cfg, err := pgx.ParseConfig(dsn); err == nil && cfg.Password != "" {
			for _, password := range []string{cfg.Password, url.QueryEscape(cfg.Password), url.PathEscape(cfg.Password), strings.TrimSuffix(strings.TrimPrefix(strconv.Quote(cfg.Password), `"`), `"`)} {
				replacements = append(replacements, password, "[REDACTED]")
			}
		}
	}
	return strings.NewReplacer(replacements...).Replace
}

// Child stdout and stderr may write concurrently, and credentials may be split
// across Write calls. Buffer through newline before redacting any persisted text.
type redactingWriter struct {
	mu      sync.Mutex
	dst     io.Writer
	redact  func(string) string
	pending []byte
	onLines func(string)
}

func (w *redactingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending = append(w.pending, p...)
	if n := bytes.LastIndexByte(w.pending, '\n'); n >= 0 {
		if w.onLines != nil {
			w.onLines(string(w.pending[:n+1]))
		}
		if _, err := io.WriteString(w.dst, w.redact(string(w.pending[:n+1]))); err != nil {
			return 0, err
		}
		w.pending = append(w.pending[:0], w.pending[n+1:]...)
	}
	return len(p), nil
}
func (w *redactingWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, err := io.WriteString(w.dst, w.redact(string(w.pending)))
	w.pending = nil
	return err
}

type phaseLabel struct{ value atomic.Value }

func (p *phaseLabel) Store(v string) { p.value.Store(v) }
func (p *phaseLabel) Load() string   { return p.value.Load().(string) }

// Only a successful listener announcement from this child establishes ownership
// of the dynamically allocated endpoint. Other addresses/messages are ignored.
func childMetricsAddress(lines string) string {
	scanner := bufio.NewScanner(strings.NewReader(lines))
	for scanner.Scan() {
		var entry struct {
			Message string `json:"msg"`
			Addr    string `json:"addr"`
		}
		if json.Unmarshal(scanner.Bytes(), &entry) != nil || entry.Message != "metrics and healthz server listening" {
			continue
		}
		host, port, err := net.SplitHostPort(entry.Addr)
		n, parseErr := strconv.Atoi(port)
		if err == nil && parseErr == nil && host == "127.0.0.1" && n > 0 && n <= 65535 {
			return entry.Addr
		}
	}
	return ""
}

// Concurrent diagnostics must not share profile/log/spool paths even when their
// containers are separate. An existing lock is never removed by another run.
func lockOutputDir(dir string) (func(), error) {
	path := filepath.Join(dir, ".pg-strict.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("reserve artifact directory (choose a distinct -out-dir; remove stale lock only after checking its owner): %w", err)
	}
	fmt.Fprintf(f, "pid=%d\n", os.Getpid())
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, err
	}
	return func() { _ = os.Remove(path) }, nil
}

// Never inherit an uncontrolled trace destination into the child. All trace
// artifacts produced by this harness must pass through its private export path.
func childEnvironment(tracePath string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "STRAND_TXN_TRACE=") {
			env = append(env, entry)
		}
	}
	if tracePath != "" {
		env = append(env, "STRAND_TXN_TRACE="+tracePath)
	}
	return env
}
