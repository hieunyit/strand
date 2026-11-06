package pipeline

import (
	"context"
	"testing"
	"time"

	"strand/internal/config"
)

// TestPace_PauseSnapshot kiểm tra `pace(ctx)` chờ khi `snapPaused=true` và thoát khi
// `snapPaused=false`, kể cả khi `retentionRisk=true` (quyết định 3B).
func TestPace_PauseSnapshot(t *testing.T) {
	t.Run("pause_and_resume_snapshot", func(t *testing.T) {
		p := &Pipeline{
			govAlgo: "static",
			govTick: 10 * time.Millisecond,
			cfg:     &config.Config{},
		}
		p.snapPaused.Store(true)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- p.pace(ctx, 0)
		}()

		select {
		case err := <-done:
			t.Fatalf("pace trả về sớm khi snapPaused=true: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		p.snapPaused.Store(false)

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("pace trả về lỗi không mong muốn: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("pace không trả về trong 1s sau khi đặt snapPaused=false")
		}
	})

	t.Run("overrides_retention_risk", func(t *testing.T) {
		p := &Pipeline{
			govAlgo: "static",
			govTick: 10 * time.Millisecond,
			cfg:     &config.Config{},
		}
		p.retentionRisk.Store(true)
		p.snapPaused.Store(true)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- p.pace(ctx, 0)
		}()

		select {
		case err := <-done:
			t.Fatalf("pace trả về sớm khi retentionRisk=true và snapPaused=true: %v", err)
		case <-time.After(100 * time.Millisecond):
		}

		p.snapPaused.Store(false)

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("pace trả về lỗi không mong muốn: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("pace không trả về trong 1s sau khi đặt snapPaused=false")
		}
	})
}
