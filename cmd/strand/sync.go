package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"strand/internal/admin"
	"strand/internal/config"
	"strand/internal/exitcode"
	"strand/internal/pipeline"
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

var syncSeedManifest string

func newSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Chạy pipeline đồng bộ CDC (spec/02 §10, spec/03 §4)",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfgPath == "" {
				return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing config file path (use -c or --config)", nil)
			}
			cfg, err := config.LoadFile(cfgPath)
			if err != nil {
				return err
			}
			level := slog.LevelInfo
			switch strings.ToLower(cfg.Observability.Log.Level) {
			case "debug":
				level = slog.LevelDebug
			case "warn":
				level = slog.LevelWarn
			case "error":
				level = slog.LevelError
			}
			var handler slog.Handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
			if strings.EqualFold(cfg.Observability.Log.Format, "text") {
				handler = slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
			}
			logger := slog.New(handler)
			for _, k := range cfg.UnimplementedKeys() {
				logger.Warn(config.UnimplementedWarning(k), "key", k)
			}

			// SIGTERM/SIGINT: ngừng đọc, chờ writer xong, ghi checkpoint rồi thoát (01 §3).
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			return pipeline.Run(ctx, pipeline.Options{
				Config:       cfg,
				StartAt:      syncStartAt,
				SeedManifest: syncSeedManifest,
				Logger:       logger,
			})
		},
	}
	cmd.Flags().StringVar(&syncStartAt, "start-at", "", "Vị trí bắt đầu: seed | <gtid>")
	cmd.Flags().StringVar(&syncSeedManifest, "seed-manifest", "seed.json", "Đường dẫn seed.json khi --start-at seed")
	return cmd
}

func newSnapshotCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "snapshot", Short: "Snapshot logic bảng khi pipeline đang chạy (spec/02 §10)"}
	var table string
	add := &cobra.Command{
		Use:   "add",
		Short: "Thêm và snapshot một bảng khi pipeline đang chạy",
		RunE: func(cmd *cobra.Command, args []string) error {
			if table == "" {
				return exitcode.New("missing_table", exitcode.InvalidSyntax, "missing --table", nil)
			}
			if dryRun {
				fmt.Printf("snapshot add %s (dry-run)\n", table)
				return nil
			}
			if err := admin.NewClient(getAdminSocketPath()).SnapshotAdd(cmd.Context(), table); err != nil {
				return fmt.Errorf("strand snapshot add failed: %w", err)
			}
			fmt.Printf("Đã bắt đầu snapshot bảng %s\n", table)
			return nil
		},
	}
	add.Flags().StringVar(&table, "table", "", "Bảng schema.table")
	add.Flags().StringVar(&adminSocketPath, "socket", "", "Đường dẫn admin socket UNIX")
	cmd.AddCommand(add)
	return cmd
}
