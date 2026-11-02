package mysql

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"testing"
	"time"

	"strand/internal/exitcode"
)

// A network failure while connecting to the source used to be reported as
// binlog_config_invalid (exit 3). systemd has RestartPreventExitStatus=3, so a
// transient outage permanently stopped the pipeline instead of restarting it.
func TestReaderConnectFailureIsNotConfigRejection(t *testing.T) {
	r, err := New(Config{Host: "127.0.0.1", Port: 1, User: "x", ServerID: 9})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err = r.Start(ctx, nil)
	if err == nil {
		t.Fatalf("connection to 127.0.0.1:1 unexpectedly succeeded")
	}
	if code := exitcode.GetExitCode(err); code == exitcode.Rejected {
		t.Fatalf("transient connect failure mapped to exit %d (no restart under systemd): %v", code, err)
	}
}
