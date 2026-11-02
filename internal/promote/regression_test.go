package promote_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"errors"
	"strings"
	"testing"

	"strand/internal/exitcode"

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

// invariant: PR-1 (decision P1-17, câu 2, hướng 1)
// When the source cannot be fenced because it is unreachable, promote must stop
// with exit 10 before touching the target. A partition between Strand and the source
// does not mean the source is dead; opening writes would create two writable primaries.
func TestPR1_UnfencedSourceNeedsOperatorDecision(t *testing.T) {
	ctx := context.Background()
	source := newMockSourceDriver(false)
	target := newMockTargetDriver()
	store := promote.NewMemoryStateStore()
	cfg := promote.Config{PipelineID: "pr1-unfenced"}

	progress, err := promote.NewPromoter(cfg, store, source, target, nil, nil).Run(ctx)
	var ee *exitcode.Error
	if !errors.As(err, &ee) || ee.ExitCode != exitcode.NeedsDecision || ee.Code != exitcode.ErrPromoteNeedsDecision {
		t.Fatalf("expected exit 10 promote_needs_decision, got %v", err)
	}
	if progress.State != promote.StateAwaitingOperator {
		t.Fatalf("state = %s, want awaiting_operator", progress.State)
	}
	if progress.IsStepCompleted(promote.StepFenceSource) {
		t.Fatalf("fence must not be marked completed when the source was not fenced")
	}
	if target.holdCalls != 0 || target.openCalls != 0 {
		t.Fatalf("target touched before the operator decided (hold=%d open=%d)", target.holdCalls, target.openCalls)
	}

	// Operator confirms the source is isolated by other means and re-runs.
	cfg.SourceFencedExternally = true
	progress, err = promote.NewPromoter(cfg, store, source, target, nil, nil).Run(ctx)
	if err != nil {
		t.Fatalf("re-run with confirmation failed: %v", err)
	}
	if progress.State != promote.StatePromoted || !progress.IsStepCompleted(promote.StepFenceSource) {
		t.Fatalf("expected promoted with fence completed, got %s %v", progress.State, progress.CompletedSteps)
	}
}

// A reachable source whose fence fails is a hard error, not a decision.
func TestPR1_ReachableSourceFenceFailureIsError(t *testing.T) {
	source := newMockSourceDriver(true)
	source.fenceHook = func() error { return errors.New("access denied for SET GLOBAL super_read_only") }
	target := newMockTargetDriver()
	cfg := promote.Config{PipelineID: "pr1-fencefail", SourceFencedExternally: true}
	_, err := promote.NewPromoter(cfg, promote.NewMemoryStateStore(), source, target, nil, nil).Run(context.Background())
	if err == nil || exitcode.GetExitCode(err) == exitcode.NeedsDecision {
		t.Fatalf("fence failure on a reachable source must be an error (flag must not override it), got %v", err)
	}
	if target.openCalls != 0 {
		t.Fatalf("target writes opened after failed fence")
	}
}

type mockSummarizingTargetDriver struct {
	*mockTargetDriver
	summary    string
	summaryErr error
}

func (m *mockSummarizingTargetDriver) FailoverSummary(ctx context.Context) (string, error) {
	return m.summary, m.summaryErr
}

// invariant: PR-1 (decision P1-17, câu 3 = "gộp")
// Khi dừng exit 10 vì không fence được source, progress và message lỗi phải hiển thị
// tập giao dịch đã apply ở target để người vận hành biết mức mất dữ liệu.
func TestPR1_UnfencedStopShowsFailoverSummary(t *testing.T) {
	ctx := context.Background()
	source := newMockSourceDriver(false)
	target := &mockSummarizingTargetDriver{
		mockTargetDriver: newMockTargetDriver(),
		summary:          "target applied up to u:1-100",
	}
	store := promote.NewMemoryStateStore()
	cfg := promote.Config{PipelineID: "pr1-summary"}

	progress, err := promote.NewPromoter(cfg, store, source, target, nil, nil).Run(ctx)
	var ee *exitcode.Error
	if !errors.As(err, &ee) || ee.ExitCode != exitcode.NeedsDecision || ee.Code != exitcode.ErrPromoteNeedsDecision {
		t.Fatalf("expected exit 10 promote_needs_decision, got %v", err)
	}
	if !strings.Contains(progress.FailoverSummary, "u:1-100") {
		t.Fatalf("progress.FailoverSummary = %q, want containing u:1-100", progress.FailoverSummary)
	}
	if !strings.Contains(err.Error(), "u:1-100") {
		t.Fatalf("error message = %q, want containing u:1-100", err.Error())
	}

	// Khi đọc checkpoint lỗi, summary bắt đầu bằng "unknown" và vẫn dừng với exit 10.
	targetErr := &mockSummarizingTargetDriver{
		mockTargetDriver: newMockTargetDriver(),
		summaryErr:       errors.New("checkpoint table unreadable"),
	}
	storeErr := promote.NewMemoryStateStore()
	progress2, err2 := promote.NewPromoter(cfg, storeErr, source, targetErr, nil, nil).Run(ctx)
	if !errors.As(err2, &ee) || ee.ExitCode != exitcode.NeedsDecision || ee.Code != exitcode.ErrPromoteNeedsDecision {
		t.Fatalf("expected exit 10 promote_needs_decision on summary error, got %v", err2)
	}
	if !strings.HasPrefix(progress2.FailoverSummary, "unknown") {
		t.Fatalf("progress.FailoverSummary = %q, want prefix \"unknown\"", progress2.FailoverSummary)
	}
}
