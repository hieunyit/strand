package promote_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"testing"

	"strand/internal/checkpoint"
	"strand/internal/promote"
)

const u = "3e11fa47-71ca-11e1-9e33-c80aa9429562"

// invariant: PR-1
// Step 3 "apply remaining log" was `return nil`: promote opened writes while the
// target could still be missing committed source transactions.
func TestPR1_ApplyRemainingWithoutSourceRefuses(t *testing.T) {
	d := promote.NewMySQLTargetDriver(nil)
	if err := d.ApplyRemaining(context.Background()); err == nil {
		t.Fatalf("ApplyRemaining must not succeed without a way to verify the target caught up")
	}
}

// invariant: PR-1
// The target is caught up only when every GTID executed on the (fenced) source is
// in the applied set stored on the target.
func TestPR1_MissingFromApplied(t *testing.T) {
	applied, _ := checkpoint.ParseAppliedSet("mysql", u+":1-100")
	missing, err := promote.MissingFromApplied(u+":1-120", applied)
	if err != nil {
		t.Fatal(err)
	}
	if missing != u+":101-120" {
		t.Fatalf("missing = %q", missing)
	}
	missing, err = promote.MissingFromApplied(u+":1-100", applied)
	if err != nil || missing != "" {
		t.Fatalf("expected caught up, got %q %v", missing, err)
	}
	if _, err := promote.MissingFromApplied("garbage:x", applied); err == nil {
		t.Fatalf("corrupt source gtid_executed must be an error")
	}
}
