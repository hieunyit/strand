package promote_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/promote"
)

func TestDecisionLineRoundTrip(t *testing.T) {
	p := &promote.Progress{
		PipelineID:         "orders",
		State:              promote.StateAwaitingOperator,
		CurrentStep:        promote.StepApplyRemaining,
		CompletedSteps:     []promote.PromoteStep{promote.StepFenceSource, promote.StepHoldReadOnly},
		SuggestedSequences: map[string]int64{"app.b_seq": 20, "app.a_seq": 10},
		FailoverSummary:    "target applied up to uuid:1-99",
		DecisionReason:     promote.DecisionSourceLostAfterFence,
	}
	line := promote.DecisionFrom(p, "source went away").Line()
	if !strings.HasPrefix(line, promote.DecisionLinePrefix) || strings.Contains(line, "\n") {
		t.Fatalf("line = %q", line)
	}
	out := "noise\n" + promote.DecisionLinePrefix + "{not json\n" + line + "\nError [promote_needs_decision]: x\n"
	d, ok := promote.ParseDecision(out)
	if !ok || d.Reason != promote.DecisionSourceLostAfterFence || !d.Fenced || d.CurrentStep != promote.StepApplyRemaining ||
		d.Message != "source went away" || d.FailoverSummary != p.FailoverSummary ||
		!reflect.DeepEqual(d.SuggestedSequences, p.SuggestedSequences) {
		t.Fatalf("parsed = %+v ok=%v", d, ok)
	}
	// Dòng hỏng nằm sau dòng hợp lệ: vẫn lấy dòng hợp lệ cuối cùng.
	if d2, ok := promote.ParseDecision(line + "\n" + promote.DecisionLinePrefix + "garbage\n"); !ok || d2.Reason != d.Reason {
		t.Fatalf("fallback parse = %+v ok=%v", d2, ok)
	}
	if _, ok := promote.ParseDecision("=== PROMOTE NEEDS OPERATOR DECISION ===\nno json here\n"); ok {
		t.Fatalf("parsed a decision from output without the line")
	}
	// Không fence: Fenced=false; không có gợi ý: không có khóa suggested_sequences.
	l2 := promote.DecisionFrom(&promote.Progress{DecisionReason: promote.DecisionFenceFailed}, "m").Line()
	if strings.Contains(l2, "suggested_sequences") || !strings.Contains(l2, `"fenced":false`) {
		t.Fatalf("fence-failed line = %s", l2)
	}
}

// Source fence được, apply nốt log xong, rồi mới mất kết nối lúc đọc sequence độc lập: lý do dừng là
// independent_sequences (khác fence_failed / source_lost_after_fence), Web dựa vào đó để chọn lời khuyên.
func TestDecisionReasonIndependentSequences(t *testing.T) {
	source := newMockSourceDriver(true)
	target := newMockTargetDriver()
	target.applyHook = func() error {
		source.mu.Lock()
		source.reachable = false
		source.mu.Unlock()
		return nil
	}
	seqs := []promote.SequenceInfo{{Name: "app.ext_seq", Type: promote.SequenceTypeIndependent, CacheSize: 1, IncrementBy: 1, LastSyncedValue: 500}}
	p := promote.NewPromoter(promote.Config{PipelineID: "dec-seq", PeakRate: 10, SyncIntervalSec: 1}, promote.NewMemoryStateStore(), source, target, seqs, nil)
	progress, err := p.Run(context.Background())
	var ee *exitcode.Error
	if !errors.As(err, &ee) || ee.ExitCode != exitcode.NeedsDecision {
		t.Fatalf("want exit 10, got %v", err)
	}
	if progress.DecisionReason != promote.DecisionIndependentSequences || !progress.IsStepCompleted(promote.StepFenceSource) {
		t.Fatalf("reason=%q completed=%v", progress.DecisionReason, progress.CompletedSteps)
	}
	d := promote.DecisionFrom(progress, ee.Message)
	if !d.Fenced || d.SuggestedSequences["app.ext_seq"] <= 500 {
		t.Fatalf("decision = %+v", d)
	}
}
