package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

// review R11: router đang chờ trong waitIfPaused phải thoát khi ctx bị hủy (SIGTERM lúc pause),
// không chỉ khi fail() broadcast.
func TestWaitIfPausedReturnsOnCancel(t *testing.T) {
	p := &Pipeline{}
	p.pauseCond = sync.NewCond(&p.pauseMu)
	p.paused.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { p.waitIfPaused(ctx); close(done) }()
	select {
	case <-done:
		t.Fatalf("waitIfPaused returned while paused and ctx alive")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("waitIfPaused did not return after ctx cancel")
	}
}
