package main

import (
	"strings"
	"testing"
)

func TestCLIRestoreFlagsValidation(t *testing.T) {
	// 1. Invalid threads > 16
	_, _, err := runBackup(t, "restore", "--id", "bkp-123", "--target-database", "new_db", "--threads", "20")
	if err == nil || !strings.Contains(err.Error(), "threads must be between 1 and 16") {
		t.Fatalf("expected error for --threads 20, got: %v", err)
	}

	// 2. Invalid object name
	_, _, err = runBackup(t, "restore", "--id", "bkp-123", "--target-database", "new_db", "--objects", "tbl1,tbl;drop")
	if err == nil || !strings.Contains(err.Error(), "invalid object name") {
		t.Fatalf("expected error for invalid object name, got: %v", err)
	}

	// 3. Threads and objects flags accepted syntactically
	// Missing --yes or database store will fail after validation
	_, _, err = runBackup(t, "restore", "--id", "bkp-123", "--target-database", "new_db", "--threads", "4", "--objects", "users,orders")
	if err == nil {
		t.Fatal("expected failure due to store or missing --yes, not success")
	}
	// It should NOT fail on threads or objects validation
	if strings.Contains(err.Error(), "threads") || strings.Contains(err.Error(), "object") {
		t.Fatalf("unexpected validation error: %v", err)
	}
}
