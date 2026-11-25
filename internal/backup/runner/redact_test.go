package runner

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// runStderrTool runs a fake tool whose stderr is produced by script and returns the process (waited) and
// what was logged.
func runStderrTool(t *testing.T, script string, inv Invocation) (*Process, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pg_basebackup"), []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	inv.Tool = "pg_basebackup"
	var log syncBuffer
	p, err := Start(context.Background(), envOf(map[string]string{EnvToolsDir: dir, "STRAND_BACKUP_TMPDIR": t.TempDir()}), inv, nil, &log)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	_, _ = io.Copy(io.Discard, p)
	_ = p.Wait()
	return p, log.String()
}

// A secret written to stderr in two pieces (two writes) is still redacted from the log.
func TestToolLogRedactsSecretSplitAcrossWrites(t *testing.T) {
	_, logged := runStderrTool(t, "printf 'auth with sup' >&2; sleep 0.2; printf 'ersecret9 failed\\n' >&2\n",
		Invocation{Env: []string{"PGPASSWORD=supersecret9"}})
	if strings.Contains(logged, "sup") || strings.Contains(logged, "ersecret9") {
		t.Fatalf("secret leaked into the log: %q", logged)
	}
	if !strings.Contains(logged, "auth with [redacted] failed") {
		t.Fatalf("log = %q", logged)
	}
}

// Positions are parsed from the raw stderr, and non-secret settings (PGSSLMODE, PGHOSTADDR, {dir} paths)
// are not redacted: a coincidence must not blank the LSN/timeline or the diagnostics.
func TestPositionsSurviveRedaction(t *testing.T) {
	stderr := "pg_basebackup: write-ahead log start point: 0/2000028 on timeline 1\\n" +
		"pg_basebackup: write-ahead log end point: 0/2000100\\npg_basebackup: sslmode require host 10.0.0.7\\n"
	p, logged := runStderrTool(t, "printf '"+stderr+"' >&2\n", Invocation{Env: []string{"PGPASSWORD=2000028",
		"PGSSLMODE=require", "PGHOSTADDR=10.0.0.7", "PGSSLROOTCERT={dir}/postgres-ca.pem"}})
	start, end, tl := ParsePGBaseBackupLog(p.rawStderr())
	if start.LSN != "0/2000028" || end.LSN != "0/2000100" || tl != "1" {
		t.Fatalf("positions = %+v %+v %q", start, end, tl)
	}
	for _, out := range []string{p.Stderr(), logged} {
		if strings.Contains(out, "2000028") {
			t.Fatalf("password not redacted: %q", out)
		}
		if !strings.Contains(out, "sslmode require host 10.0.0.7") {
			t.Fatalf("non-secret settings redacted: %q", out)
		}
	}
	for _, s := range secretsOf(Invocation{Env: []string{"PGSSLMODE=require", "PGHOSTADDR=10.0.0.7", "PGSSLROOTCERT={dir}/ca.pem"},
		Files: map[string]string{"postgres-ca.pem": "-----BEGIN CERTIFICATE-----\nMIIB==\n-----END CERTIFICATE-----\n"}}) {
		t.Errorf("non-secret listed as secret: %q", s)
	}
}
