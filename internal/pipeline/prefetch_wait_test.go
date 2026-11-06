package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// review R03: spool prefetch chỉ được xóa khi mọi Txn prefetch đã qua router, không chỉ khi
// Lwd == admitted (Txn cuối có thể còn trong channel / router chưa Admit).
func TestWaitPrefetchAppliedWaitsForRouted(t *testing.T) {
	p := &Pipeline{}
	var lwd atomic.Uint64
	// Đã Admit và apply tới seq 5, nhưng 2 Txn prefetch cuối chưa qua router.
	p.admitted.Store(5)
	lwd.Store(5)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- p.waitPrefetchApplied(ctx, 2, lwd.Load, time.Millisecond) }()

	select {
	case ok := <-done:
		t.Fatalf("returned %v while prefetch txns are still unrouted (Lwd == admitted)", ok)
	case <-time.After(100 * time.Millisecond):
	}
	// Router xử lý hai Txn còn lại: Admit seq 6, 7; Lwd vẫn 5.
	p.admitted.Store(7)
	p.routed.Store(2)
	select {
	case ok := <-done:
		t.Fatalf("returned %v before Lwd reached the last routed admit", ok)
	case <-time.After(100 * time.Millisecond):
	}
	lwd.Store(7)
	select {
	case ok := <-done:
		if !ok {
			t.Fatalf("returned false after all prefetch txns applied")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("did not return after all prefetch txns routed and applied")
	}
}

func TestWaitPrefetchAppliedCtxCancel(t *testing.T) {
	p := &Pipeline{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- p.waitPrefetchApplied(ctx, 1, func() uint64 { return 0 }, time.Millisecond) }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatalf("returned true after ctx cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("did not return after ctx cancel")
	}
}
