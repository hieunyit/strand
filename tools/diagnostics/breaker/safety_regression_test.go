package breaker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDSNRedactionAcceptsDriverPasswordGrammar(t *testing.T) {
	for _, raw := range []string{
		":diagnostic-password@tcp(localhost:3306)/db",
		"u:p@partial-password@tcp(localhost:3306)/db",
		"u:password/with:more@signs@tcp(localhost:3306)/db",
		"u:password://with/slash@tcp(localhost:3306)/db",
	} {
		got := RedactDSN(raw)
		if got != raw[:strings.IndexByte(raw, ':')+1]+"***@tcp(localhost:3306)/db" {
			t.Errorf("password was not fully masked: %q -> %q", raw, got)
		}
	}
	if got := RedactDSN("u:malformed-password@tcp(host"); got != "[REDACTED_DSN]" {
		t.Fatalf("malformed DSN was exported: %q", got)
	}
}

func TestProtectedSnapshotReplacesExistingFilePrivately(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil {
		t.Fatal(err)
	}
	// A hard link must retain the old bytes and mode as well.
	alias := filepath.Join(dir, "alias")
	if err := os.Link(path, alias); err != nil {
		t.Fatal(err)
	}
	if err := SaveProtectedSnapshot(&DiagnosticSnapshot{ServerVersion: "fixture"}, path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("snapshot is not private: info=%v err=%v", info, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got DiagnosticSnapshot
	if err := json.Unmarshal(data, &got); err != nil || got.ServerVersion != "fixture" {
		t.Fatalf("invalid snapshot: %s, %v", data, err)
	}
	old, err := os.ReadFile(alias)
	if err != nil || string(old) != "old" {
		t.Fatalf("modified hard link: %q, %v", old, err)
	}
}

func TestProtectedSnapshotDoesNotFollowDestinationSymlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("preserve me"), 0644); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "snapshot.json")
	if err := os.Symlink(victim, path); err != nil {
		t.Fatal(err)
	}
	if err := SaveProtectedSnapshot(&DiagnosticSnapshot{}, path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(victim)
	if err != nil || string(data) != "preserve me" {
		t.Fatalf("modified symlink target: %q, %v", data, err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		t.Fatalf("snapshot destination is not a private regular file: %v, %v", info, err)
	}
}

func TestEngineStatusOmitsMultilineQueriesAndRecordDumps(t *testing.T) {
	fixture := `----------
SEMAPHORES
----------
OS WAIT ARRAY INFO: reservation count 42
OS WAIT ARRAY INFO: signal count 30
RW-shared spins 0, rounds 5, OS waits 3
------------------------
LATEST FOREIGN KEY ERROR
------------------------
INSERT INTO accounts(card)
VALUES ('multiline-card-sentinel')
DATA TUPLE: 1 fields;
0: len 10; hex 7265636f7264736563726574; asc recordsecret;;
------------------------
LATEST DETECTED DEADLOCK
------------------------
WITH customer AS (
  VALUES ('cte-sensitive-sentinel')
)
CALL sensitive_procedure('procedure-secret-sentinel')
------------
TRANSACTIONS
------------
/* query-comment-sentinel */
RECORD LOCKS space id 1 page no 2
0: len 4; hex 64617461; asc rowdata-sentinel;;
--------
FILE I/O
--------
0 pending normal aio reads, 2 pending aio writes
1 pending log flushes (fsync), 0 pending chkp writes
---
LOG
---
Log sequence number 123456
Log flushed up to 123400
`
	got := ParseAndSanitizeEngineStatus(fixture, time.Now())
	for _, secret := range []string{"multiline-card-sentinel", "7265636f7264736563726574", "recordsecret", "cte-sensitive-sentinel", "procedure-secret-sentinel", "query-comment-sentinel", "rowdata-sentinel"} {
		if strings.Contains(got.RawSanitizedText, secret) || strings.Contains(got.SemaphoresWaitInfo, secret) {
			t.Errorf("status exported secret %q: %+v", secret, got)
		}
	}
	if got.LogSequenceNumber != 123456 || got.LogFlushedUpTo != 123400 || got.LogPendingFsyncs != 1 {
		t.Fatalf("lost diagnostic counters: %+v", got)
	}
	if !strings.Contains(got.SemaphoresWaitInfo, "reservation count 42") || !strings.Contains(got.SemaphoresWaitInfo, "OS waits 3") {
		t.Fatalf("lost numeric semaphore diagnostics: %q", got.SemaphoresWaitInfo)
	}
}
