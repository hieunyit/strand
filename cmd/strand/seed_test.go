package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/exitcode"
)

func TestCoordinateRestoreAndPrefetch_Success(t *testing.T) {
	var restoreFinished atomic.Bool
	n, err := coordinateRestoreAndPrefetch(
		context.Background(),
		func(ctx context.Context) error {
			time.Sleep(30 * time.Millisecond)
			restoreFinished.Store(true)
			return nil
		},
		func(ctx context.Context, until <-chan struct{}) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-until:
				if !restoreFinished.Load() {
					t.Errorf("until closed before restore finished")
				}
				return 123, nil
			}
		},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 123 {
		t.Fatalf("got n=%d, want 123", n)
	}
}

func TestCoordinateRestoreAndPrefetch_RestoreFails_Exits1(t *testing.T) {
	var untilClosed atomic.Bool
	_, err := coordinateRestoreAndPrefetch(
		context.Background(),
		func(ctx context.Context) error {
			return runRestoreShellCmd(ctx, "exit 7")
		},
		func(ctx context.Context, until <-chan struct{}) (int, error) {
			select {
			case <-ctx.Done():
				return 5, ctx.Err()
			case <-until:
				untilClosed.Store(true)
				return 5, nil
			}
		},
	)
	if err == nil {
		t.Fatalf("expected error when restore-cmd exits non-zero")
	}
	if untilClosed.Load() {
		t.Fatalf("until should not close when restore-cmd fails")
	}
	if got := exitcode.GetExitCode(err); got != exitcode.Unclassified {
		t.Fatalf("got exit code %d, want %d (%v)", got, exitcode.Unclassified, err)
	}
}

func TestCoordinateRestoreAndPrefetch_PrefetchFails_CancelsRestoreAndKeepsExitCode(t *testing.T) {
	start := time.Now()
	wantErr := exitcode.New(exitcode.ErrPrefetchExhausted, exitcode.DataError, "spool hard limit reached", nil)
	_, err := coordinateRestoreAndPrefetch(
		context.Background(),
		func(ctx context.Context) error {
			return runRestoreShellCmd(ctx, "sleep 30")
		},
		func(ctx context.Context, until <-chan struct{}) (int, error) {
			time.Sleep(30 * time.Millisecond)
			return 10, wantErr
		},
	)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("restore-cmd was not cancelled promptly; took %v", elapsed)
	}
	if got := exitcode.GetExitCode(err); got != exitcode.DataError {
		t.Fatalf("got exit code %d, want %d (%v)", got, exitcode.DataError, err)
	}
}
