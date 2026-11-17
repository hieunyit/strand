package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExternalDatabasesRequireExplicitDestructiveOptIn(t *testing.T) {
	cfg := Config{SourceDSN: "postgres://example/src", TargetDSN: "postgres://example/dst", Rows: 1, Clients: 1, Duration: time.Second}
	if err := runBenchmark(cfg); err == nil || !strings.Contains(err.Error(), "refusing to mutate external databases") {
		t.Fatalf("must refuse before connecting or creating artifacts: %v", err)
	}
	cfg.AllowDestructiveExistingDB = true
	if err := validateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.AutoDocker = true
	if err := validateConfig(cfg); err == nil {
		t.Fatal("silently ignored external credentials")
	}
}

func TestPrivateArtifactsReplacePermissiveFileAndSymlink(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	if err := privateDir(dir); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(dir); err != nil || st.Mode().Perm() != 0o700 {
		t.Fatalf("private dir: %v %v", st, err)
	}
	outside := filepath.Join(t.TempDir(), "untouched")
	if err := os.WriteFile(outside, []byte("sentinel"), 0o644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "strand.yaml")
	for _, symlink := range []bool{false, true} {
		os.Remove(path)
		if symlink {
			if err := os.Symlink(outside, path); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if err := writePrivateFile(path, []byte("private config")); err != nil {
			t.Fatal(err)
		}
		if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
			t.Fatalf("private file: %v %v", st, err)
		}
		if raw, _ := os.ReadFile(outside); string(raw) != "sentinel" {
			t.Fatal("followed symlink")
		}
	}
}

func TestActualChildOutputIsRedactedAcrossWrites(t *testing.T) {
	if os.Getenv("STRAND_DIAG_REDACTION_CHILD") == "1" {
		// Force a credential split across writes, plus a final unterminated line.
		os.Stdout.WriteString("source postgres://u:canary")
		os.Stdout.WriteString("%21@host/db\npassword canary!\n")
		os.Stderr.WriteString("keyword host=localhost user=u password='keyword secret' dbname=db\nkeyword secret")
		return
	}
	var log bytes.Buffer
	writer := &redactingWriter{dst: &log, redact: newArtifactRedactor("postgres://u:canary%21@host/db", "host=localhost user=u password='keyword secret' dbname=db")}
	cmd := exec.Command(os.Args[0], "-test.run=^TestActualChildOutputIsRedactedAcrossWrites$")
	cmd.Env = append(os.Environ(), "STRAND_DIAG_REDACTION_CHILD=1")
	cmd.Stdout, cmd.Stderr = writer, writer
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"canary", "keyword secret", "postgres://u:"} {
		if strings.Contains(log.String(), secret) {
			t.Fatalf("actual child leaked %q: %s", secret, log.String())
		}
	}
	if !strings.Contains(log.String(), "REDACTED") {
		t.Fatalf("did not capture redacted child output: %s", log.String())
	}
}

func TestDockerCleanupOnlyCreatedIDsAndDynamicPorts(t *testing.T) {
	var calls [][]string
	id := strings.Repeat("a", 64)
	run := func(args ...string) ([]byte, error) {
		calls = append(calls, append([]string(nil), args...))
		switch args[0] {
		case "run":
			return []byte(id + "\n"), nil
		case "port":
			return []byte("127.0.0.1:43123\n"), nil
		case "rm":
			return nil, nil
		}
		return nil, errors.New("unexpected command")
	}
	got, port, err := startContainer("source", 0, "2", run)
	if err != nil || got != id || port != 43123 {
		t.Fatalf("start: id=%q port=%d err=%v", got, port, err)
	}
	firstName := calls[0][3]
	second, _, err := startContainer("source", 0, "2", run)
	if err != nil {
		t.Fatal(err)
	}
	if calls[2][3] == firstName {
		t.Fatal("container names reused across runs")
	}
	if !strings.Contains(strings.Join(calls[0], " "), "127.0.0.1::5432") {
		t.Fatal("not using a dynamic loopback binding")
	}
	cleanupContainers([]string{got, second}, run)
	for _, call := range calls {
		if call[0] == "rm" && (len(call) != 3 || call[2] != id) {
			t.Fatalf("removed unowned container: %v", call)
		}
	}
	// Failure after a successful create retains ownership so caller can clean up.
	failed := func(args ...string) ([]byte, error) {
		if args[0] == "run" {
			return []byte(id), nil
		}
		return nil, errors.New("port discovery failed")
	}
	got, _, err = startContainer("target", 0, "2", failed)
	if err == nil || got != id {
		t.Fatalf("lost ownership after successful create: %q %v", got, err)
	}
}

func TestPhaseLabelConcurrentTransition(t *testing.T) {
	var phase phaseLabel
	phase.Store("warmup")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			phase.Store("steady")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			if got := phase.Load(); got != "warmup" && got != "steady" {
				t.Errorf("invalid phase: %q", got)
			}
		}
	}()
	wg.Wait()
}

func TestSummaryMetricUnitsDoNotClaimCommitBatches(t *testing.T) {
	raw, err := json.Marshal(BenchmarkReport{RowsPerSourceTxn: 2, ApplyToRawCommitRatio: 3})
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if strings.Contains(text, "avg_batch_size") || strings.Contains(text, "pipeline_efficiency") || !strings.Contains(text, `"rows_per_source_txn":2`) {
		t.Fatalf("misleading metric units: %s", text)
	}
	raw, _ = json.Marshal(MetricSample{RowsPerTxn: 2})
	if strings.Contains(string(raw), "avg_batch_size") || !strings.Contains(string(raw), `"rows_per_source_txn":2`) {
		t.Fatal(string(raw))
	}
}

func TestChildMetricsListenerOwnership(t *testing.T) {
	for _, line := range []string{`{"msg":"other listener","addr":"127.0.0.1:23456"}`, `{"msg":"metrics and healthz server listening","addr":"0.0.0.0:23456"}`, `{"msg":"metrics and healthz server listening","addr":"127.0.0.1:0"}`} {
		if addr := childMetricsAddress(line); addr != "" {
			t.Fatalf("accepted unowned/invalid endpoint %s", addr)
		}
	}
	line := `{"msg":"metrics and healthz server listening","addr":"127.0.0.1:23456"}`
	if addr := childMetricsAddress("other log\n" + line + "\n"); addr != "127.0.0.1:23456" {
		t.Fatalf("listener=%q", addr)
	}
}

func TestOutputDirectoryCannotBeSharedByConcurrentRuns(t *testing.T) {
	dir := t.TempDir()
	release, err := lockOutputDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := lockOutputDir(dir); err == nil {
		other()
		t.Fatal("second run acquired active artifact directory")
	}
	raw, err := os.ReadFile(filepath.Join(dir, ".pg-strict.lock"))
	if err != nil || !strings.Contains(string(raw), "pid=") {
		t.Fatalf("second run removed owner's lock: %s %v", raw, err)
	}
	release()
	releaseAgain, err := lockOutputDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	releaseAgain()
}

func TestDockerExecutableWarningsAreNotContainerIdentity(t *testing.T) {
	dir := t.TempDir()
	id := strings.Repeat("b", 64)
	calls := filepath.Join(dir, "calls")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$DIAG_DOCKER_CALLS"
case "$1" in
run) printf 'pulling fixture image (warning)\n' >&2; printf '%s\n' "$DIAG_DOCKER_ID" ;;
port) printf '127.0.0.1:43210\n' ;;
rm) test "$3" = "$DIAG_DOCKER_ID" ;;
*) exit 97 ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	t.Setenv("DIAG_DOCKER_CALLS", calls)
	t.Setenv("DIAG_DOCKER_ID", id)
	got, port, err := startContainer("source", 0, "2", dockerCommand)
	if err != nil || got != id || port != 43210 {
		t.Fatalf("stdout identity: got=%q port=%d err=%v", got, port, err)
	}
	cleanupContainers([]string{got}, dockerCommand)
	raw, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "run ") || lines[1] != "port "+id+" 5432/tcp" || lines[2] != "rm -f "+id {
		t.Fatalf("unexpected real CLI lifecycle: %s", raw)
	}
}

func TestChildDoesNotInheritUnredactedTraceDestination(t *testing.T) {
	t.Setenv("STRAND_TXN_TRACE", "/unowned/plaintext-trace")
	for _, trace := range []string{"", "/private/trace"} {
		count := 0
		for _, entry := range childEnvironment(trace) {
			if strings.HasPrefix(entry, "STRAND_TXN_TRACE=") {
				count++
				if entry != "STRAND_TXN_TRACE="+trace || trace == "" {
					t.Fatalf("uncontrolled trace path: %s", entry)
				}
			}
		}
		if (trace != "" && count != 1) || (trace == "" && count != 0) {
			t.Fatalf("trace count=%d path=%q", count, trace)
		}
	}
}

func TestPrivateDirRejectsExistingPublicDirectoryWithoutChangingMode(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "existing")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Set the fixture mode explicitly so this assertion does not depend on umask.
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := privateDir(dir); err == nil || !strings.Contains(err.Error(), "permissions left unchanged") {
		t.Fatalf("public directory not rejected: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("existing directory mode changed: %v %v", info, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("rejected directory was modified: %v %v", entries, err)
	}
}

func TestPrivateDirRejectsSymlinkTargetAndParentWithoutEscape(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{link, filepath.Join(link, "new-private-child")} {
		if err := privateDir(path); err == nil || !strings.Contains(err.Error(), "symlink directory component") {
			t.Fatalf("symlink accepted: path=%q err=%v", path, err)
		}
	}
	info, err := os.Stat(outside)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("symlink target permissions changed: %v %v", info, err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("created descendants outside requested tree: %v %v", entries, err)
	}
}

func TestPrivateDirCreatesFreshNestedPrivatePathAndReusesOwnedPrivateDir(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "fresh", "nested")
	for i := 0; i < 2; i++ {
		if err := privateDir(path); err != nil {
			t.Fatalf("private path on attempt %d: %v", i, err)
		}
	}
	for _, dir := range []string{filepath.Dir(path), path} {
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("fresh path is not private: %v %v", info, err)
		}
	}
}
