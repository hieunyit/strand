package verify

// Việc 8: source PG đọc qua standby không ghi được watermark — LogApplyWaiter phải lỗi nhanh (StandbyGrace)
// với hướng dẫn rõ thay vì chờ hết Limit (2 phút) trên source rảnh; lwd đã phủ replay LSN thì không lỗi.

import (
	"context"
	"strings"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

func TestWaitLimitShortOnlyForStandbyWithoutWatermark(t *testing.T) {
	w := &LogApplyWaiter{}
	if got := w.waitLimit(ApplyMark{}); got != 2*time.Minute {
		t.Fatalf("primary mark: %s", got)
	}
	if got := w.waitLimit(ApplyMark{standby: true}); got != DefaultStandbyGrace {
		t.Fatalf("standby mark: %s", got)
	}
	w = &LogApplyWaiter{StandbyGrace: 3 * time.Second, Limit: time.Second}
	if got := w.waitLimit(ApplyMark{standby: true}); got != time.Second {
		t.Fatalf("grace capped by Limit: %s", got)
	}
}

func pgWaiter(lwd uint64, grace, limit time.Duration) *LogApplyWaiter {
	return &LogApplyWaiter{SourceEngine: model.EnginePG, TargetEngine: model.EnginePG, StandbyGrace: grace, Limit: limit,
		Poll: 5 * time.Millisecond,
		load: func(context.Context) (checkpoint.AppliedSet, error) { return checkpoint.NewPGAppliedSet(lwd), nil }}
}

func TestStandbyWaitFailsFastWithGuidance(t *testing.T) {
	m := ApplyMark{lsn: 1000, text: "0/3E8", standby: true}
	start := time.Now()
	err := pgWaiter(500, 50*time.Millisecond, time.Hour).Wait(context.Background(), m)
	if err == nil || !strings.Contains(err.Error(), "standby") || !strings.Contains(err.Error(), "--replica-dsn") {
		t.Fatalf("err = %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("standby wait took %s, want ~grace", d)
	}
	// lwd đã phủ replay LSN của standby: theo kịp, không lỗi.
	if err := pgWaiter(1000, 50*time.Millisecond, time.Hour).Wait(context.Background(), m); err != nil {
		t.Fatalf("covered standby mark: %v", err)
	}
}

func TestPrimaryMarkKeepsFullLimit(t *testing.T) {
	// Mark có watermark (primary): lỗi thường sau Limit, không phải thông báo standby.
	err := pgWaiter(500, time.Millisecond, 60*time.Millisecond).Wait(context.Background(), ApplyMark{lsn: 1000, text: "0/3E8"})
	if err == nil || strings.Contains(err.Error(), "standby") || !strings.Contains(err.Error(), "within 60ms") {
		t.Fatalf("err = %v", err)
	}
}
