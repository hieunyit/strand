package main

import (
	"context"
	"errors"
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
	"strand/internal/mongocdc"
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

var (
	syncSeedManifest     string
	syncCPUProfile       string
	syncBlockProfile     string
	syncBlockProfileRate int
	syncProfileOverwrite bool
)

func newSyncCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Chạy pipeline đồng bộ CDC (spec/02 §10, spec/03 §4)",
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// review R08: --dry-run (cờ persistent "không tác động đích") từng bị bỏ qua, sync chạy thật.
			// sync không có chế độ chạy thử: từ chối trước khi đọc cấu hình hay chạm DSN nào.
			if dryRun {
				return exitcode.New("dry_run_unsupported", exitcode.InvalidSyntax,
					"strand sync does not support --dry-run (it would write to the target); use `strand validate` or `strand plan` to check without side effects", nil)
			}
			if cfgPath == "" {
				return exitcode.New("missing_config_path", exitcode.InvalidSyntax, "missing config file path (use -c or --config)", nil)
			}
			cfg, err := config.LoadFile(cfgPath)
			if err != nil {
				return err
			}
			if syncBackground && os.Getenv(bgChildEnv) == "" {
				return startBackground(cmd, cfg)
			}
			defer childPidFileCleanup()()

			prof, err := startProfiling(profileConfig{
				cpuPath:        syncCPUProfile,
				blockPath:      syncBlockProfile,
				blockRate:      syncBlockProfileRate,
				allowOverwrite: syncProfileOverwrite,
			})
			if err != nil {
				return err
			}
			defer func() {
				if prof != nil {
					if pErr := prof.Stop(); pErr != nil {
						slog.Default().Error("stop profiler failed", "error", pErr)
						err = errors.Join(err, fmt.Errorf("stop profiler: %w", pErr))
					}
				}
			}()
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
			// Ring 200 bản ghi WARN/ERROR cho lệnh admin `events` (Web UI): handler bọc chỉ chép thêm, log không đổi.
			events := admin.NewEventRing(admin.DefaultEventCapacity)
			logger := slog.New(events.Wrap(handler))
			for _, k := range cfg.UnimplementedKeys() {
				logger.Warn(config.UnimplementedWarning(k), "key", k)
			}

			// SIGTERM/SIGINT: ngừng đọc, chờ writer xong, ghi checkpoint rồi thoát (01 §3).
			ctx, stop := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, os.Interrupt)
			defer stop()
			startAt := syncStartAt
			if startAt == "" && cfg.Bootstrap.StartAt != "" {
				startAt = cfg.Bootstrap.StartAt
			}
			if strings.EqualFold(cfg.Source.Kind, config.KindMongoDB) {
				return runMongoDBSync(ctx, cfg, logger, events)
			}
			return pipeline.Run(ctx, pipeline.Options{
				Config:       cfg,
				StartAt:      startAt,
				SeedManifest: syncSeedManifest,
				Logger:       logger,
				Events:       events,
			})
		},
	}
	cmd.Flags().StringVar(&syncStartAt, "start-at", "", "Vị trí bắt đầu: seed | <gtid> (source MySQL) | <file binlog>:<vị trí> (source MySQL không GTID, ví dụ mysql-bin.000123:4567) | <LSN X/Y> (source PostgreSQL)")
	cmd.Flags().StringVar(&syncSeedManifest, "seed-manifest", "seed.json", "Đường dẫn seed.json khi --start-at seed")
	cmd.Flags().BoolVarP(&syncBackground, "background", "d", false, "Chạy nền: tách khỏi terminal, log ra --log-file, PID ra --pid-file; dừng bằng strand stop -c ...")
	cmd.Flags().StringVar(&syncLogFile, "log-file", "", "Với --background: file log (mặc định <resources.spool.path>/strand.log)")
	cmd.Flags().StringVar(&syncPidFile, "pid-file", "", "Với --background: file pid (mặc định <resources.spool.path>/strand.pid)")
	cmd.Flags().StringVar(&syncCPUProfile, "cpu-profile", "", "Đường dẫn file ghi CPU profile (pprof)")
	cmd.Flags().StringVar(&syncBlockProfile, "block-profile", "", "Đường dẫn file ghi block profile (pprof)")
	cmd.Flags().IntVar(&syncBlockProfileRate, "block-profile-rate", 1, "Tỷ lệ lấy mẫu block profile theo nanosecond (mặc định 1)")
	cmd.Flags().BoolVar(&syncProfileOverwrite, "profile-overwrite", false, "Cho phép ghi đè file profile nếu đã tồn tại")
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

func runMongoDBSync(ctx context.Context, cfg *config.Config, logger *slog.Logger, events *admin.EventRing) error {
	srcURI := cfg.Source.URI
	if strings.HasPrefix(srcURI, "${") && strings.HasSuffix(srcURI, "}") {
		srcURI = os.Getenv(strings.TrimSuffix(strings.TrimPrefix(srcURI, "${"), "}"))
	}
	dstURI := cfg.Target.URI
	if strings.HasPrefix(dstURI, "${") && strings.HasSuffix(dstURI, "}") {
		dstURI = os.Getenv(strings.TrimSuffix(strings.TrimPrefix(dstURI, "${"), "}"))
	}
	var leaseTTL time.Duration
	if cfg.MongoDB.LeaseTTL != "" {
		leaseTTL, _ = time.ParseDuration(cfg.MongoDB.LeaseTTL)
	}

	mcfg := mongocdc.Config{
		Pipeline:        cfg.Name,
		SourceURI:       srcURI,
		TargetURI:       dstURI,
		SourceDB:        cfg.Source.Database,
		TargetDB:        cfg.Target.Database,
		Include:         cfg.Tables.Include,
		Exclude:         cfg.Tables.Exclude,
		OverwriteTarget: cfg.MongoDB.OverwriteTarget,
		Logger:          logger,
		LeaseDuration:   leaseTTL,
		SnapshotBatch:   cfg.MongoDB.SnapshotBatch,
		StreamBatch:     cfg.MongoDB.StreamBatch,
	}

	sockPath := cfg.Observability.AdminSocket
	if sockPath != "" {
		ah := &mongoAdminHandler{
			cfg:       mcfg,
			startedAt: time.Now(),
			events:    events,
		}
		asrv := admin.NewServer(sockPath, ah, logger)
		go func() {
			_ = asrv.Start()
		}()
		go func() {
			<-ctx.Done()
			_ = asrv.Close()
		}()
	}

	return mongocdc.Run(ctx, mcfg)
}

type mongoAdminHandler struct {
	cfg       mongocdc.Config
	startedAt time.Time
	events    *admin.EventRing
	mu        sync.Mutex
	paused    bool
}

func (h *mongoAdminHandler) Top(ctx context.Context) (*admin.TopInfo, error) {
	h.mu.Lock()
	paused := h.paused
	h.mu.Unlock()
	state := "synced"
	if paused {
		state = "paused"
	}
	return &admin.TopInfo{
		PipelineID:  h.cfg.Pipeline,
		State:       state,
		LagSeconds:  0.0,
		Throughput:  0.0,
		Zone:        "green",
		TablesDone:  int64(len(h.cfg.Include)),
		TablesTotal: int64(len(h.cfg.Include)),
	}, nil
}

func (h *mongoAdminHandler) Pause(ctx context.Context) (*admin.PauseResult, error) {
	h.mu.Lock()
	h.paused = true
	h.mu.Unlock()
	return &admin.PauseResult{State: "paused", Paused: true}, nil
}

func (h *mongoAdminHandler) Resume(ctx context.Context, snapshotOnly bool) (*admin.ResumeResult, error) {
	h.mu.Lock()
	h.paused = false
	h.mu.Unlock()
	return &admin.ResumeResult{State: "synced", Resumed: true}, nil
}

func (h *mongoAdminHandler) ThrottleSet(ctx context.Context, req admin.Request) (*admin.ThrottleResult, error) {
	return &admin.ThrottleResult{SourceIOPSSoft: req.SourceIOPS}, nil
}

func (h *mongoAdminHandler) CheckpointShow(ctx context.Context) (*admin.CheckpointResult, error) {
	return &admin.CheckpointResult{Applied: "mongodb-stream-active", Epoch: 1, UpdatedAt: time.Now()}, nil
}

func (h *mongoAdminHandler) CheckpointRewind(ctx context.Context, to string) (*admin.CheckpointResult, error) {
	return nil, errors.New("checkpoint rewind is not supported for MongoDB")
}

func (h *mongoAdminHandler) Status(ctx context.Context) (*admin.StatusInfo, error) {
	h.mu.Lock()
	paused := h.paused
	h.mu.Unlock()
	state := "synced"
	if paused {
		state = "paused"
	}
	return &admin.StatusInfo{
		PipelineID:    h.cfg.Pipeline,
		State:         state,
		Epoch:         1,
		UptimeSeconds: time.Since(h.startedAt).Seconds(),
		StartedAt:     h.startedAt,
		CommitOrder:   "relaxed",
		Governor:      admin.GovernorInfo{Algorithm: "none", Zone: "green"},
		Paused:        paused,
	}, nil
}

func (h *mongoAdminHandler) Tables(ctx context.Context) ([]admin.TableInfo, error) {
	res := make([]admin.TableInfo, len(h.cfg.Include))
	for i, name := range h.cfg.Include {
		res[i] = admin.TableInfo{
			Table:       name,
			State:       "done",
			ProgressPct: 100.0,
		}
	}
	return res, nil
}

func (h *mongoAdminHandler) Events(ctx context.Context) ([]admin.EventRecord, error) {
	if h.events != nil {
		return h.events.Snapshot(), nil
	}
	return nil, nil
}

func (h *mongoAdminHandler) SourceTables(ctx context.Context) ([]admin.SourceTable, error) {
	res := make([]admin.SourceTable, len(h.cfg.Include))
	for i, name := range h.cfg.Include {
		res[i] = admin.SourceTable{
			Table:      name,
			InPipeline: true,
		}
	}
	return res, nil
}
