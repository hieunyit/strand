package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/checkpoint"
)

var syncStartAt string

// CheckpointFlusher là interface thực hiện ghi checkpoint khi dừng an toàn.
type CheckpointFlusher interface {
	FlushCheckpoint(ctx context.Context) error
}

// SyncCoordinator điều phối quá trình chạy pipeline và xử lý SIGTERM.
type SyncCoordinator struct {
	logger          *slog.Logger
	flusher         CheckpointFlusher
	shutdownTimeout time.Duration
	stopCh          chan struct{}
	doneCh          chan struct{}
	mu              sync.Mutex
	running         bool
}

// NewSyncCoordinator khởi tạo SyncCoordinator.
func NewSyncCoordinator(flusher CheckpointFlusher, timeout time.Duration, logger *slog.Logger) *SyncCoordinator {
	if timeout <= 0 {
		timeout = 120 * time.Second // Khớp TimeoutStopSec=120 trong spec/03 §4
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &SyncCoordinator{
		logger:          logger,
		flusher:         flusher,
		shutdownTimeout: timeout,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
	}
}

// Run chạy vòng lặp sync và bắt tín hiệu SIGTERM để ghi checkpoint rồi thoát.
func (c *SyncCoordinator) Run(ctx context.Context) error {
	c.mu.Lock()
	c.running = true
	c.mu.Unlock()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(sigCh)

	c.logger.Info("sync coordinator running, waiting for work or stop signal")

	select {
	case <-ctx.Done():
		c.logger.Info("context cancelled, initiating shutdown")
		return c.gracefulStop()
	case sig := <-sigCh:
		c.logger.Info("signal received, initiating graceful shutdown", "signal", sig.String())
		return c.gracefulStop()
	case <-c.stopCh:
		c.logger.Info("stop requested internally, initiating shutdown")
		return c.gracefulStop()
	}
}

// Stop kích hoạt dừng sớm (dùng cho testing hoặc lệnh pause/stop nội bộ).
func (c *SyncCoordinator) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		c.running = false
		close(c.stopCh)
	}
}

func (c *SyncCoordinator) gracefulStop() error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), c.shutdownTimeout)
	defer cancel()

	start := time.Now()
	c.logger.Info("flushing checkpoint before exit...")

	if c.flusher != nil {
		if err := c.flusher.FlushCheckpoint(shutdownCtx); err != nil {
			c.logger.Error("failed to flush checkpoint during shutdown", "error", err)
			return fmt.Errorf("flush checkpoint on shutdown: %w", err)
		}
	}

	elapsed := time.Since(start)
	c.logger.Info("checkpoint flushed and shutdown completed successfully", "duration", elapsed.String())
	return nil
}

// mockCheckpointFlusher phục vụ trường hợp chạy mặc định hoặc test
type mockCheckpointFlusher struct {
	store  checkpoint.CheckpointStore
	global checkpoint.Global
}

func (m *mockCheckpointFlusher) FlushCheckpoint(ctx context.Context) error {
	if m.store != nil {
		return m.store.WriteGlobal(ctx, m.global)
	}
	return nil
}

func newSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Chạy pipeline đồng bộ CDC (spec/02 §10, spec/03 §4)",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
			coordinator := NewSyncCoordinator(&mockCheckpointFlusher{}, 120*time.Second, logger)
			return coordinator.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&syncStartAt, "start-at", "", "Vị trí bắt đầu: seed | <gtid> | <lsn>")
	return cmd
}
