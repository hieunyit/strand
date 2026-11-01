package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// spec: 02 §2 (CP-1, CP-4), §4 quy tắc 3-4, design §1

// MaxDeltaBytes là giới hạn kích thước row delta (mặc định 64 KB theo CP-4).
const MaxDeltaBytes = 64 * 1024

// ErrDeltaExceededLimit trả về khi worker delta vượt trần 64 KB mà compaction không thể giải phóng.
// invariant: CP-4
var ErrDeltaExceededLimit = fmt.Errorf("worker delta exceeds 64 KB limit")

// CompactionStep đại diện cho các bước trong chu kỳ compaction phục vụ kiểm thử kill/crash (CP-1).
type CompactionStep int

const (
	StepStart CompactionStep = iota + 1
	StepReadDeltas
	StepMerge
	StepWriteGlobal
	StepPruneDeltas
	StepComplete
)

func (s CompactionStep) String() string {
	switch s {
	case StepStart:
		return "StepStart"
	case StepReadDeltas:
		return "StepReadDeltas"
	case StepMerge:
		return "StepMerge"
	case StepWriteGlobal:
		return "StepWriteGlobal"
	case StepPruneDeltas:
		return "StepPruneDeltas"
	case StepComplete:
		return "StepComplete"
	default:
		return fmt.Sprintf("Step(%d)", int(s))
	}
}

// WorkerDelta lưu thông tin một row delta từ _strand_worker_delta.
type WorkerDelta struct {
	WorkerID int
	Epoch    int64
	BatchNo  int64
	Applied  string
}

// CompactorConfig chứa các tham số cấu hình cho Compactor.
type CompactorConfig struct {
	PipelineID    string
	Epoch         int64
	Interval      time.Duration // Chu kỳ compaction (100–500 ms theo S02 §4 quy tắc 4, mặc định 200 ms)
	MaxDeltaBytes int           // Trần kích thước row delta (mặc định 64 KB)
}

// DefaultCompactorConfig tạo cấu hình mặc định cho compaction.
func DefaultCompactorConfig(pipelineID string, epoch int64) CompactorConfig {
	return CompactorConfig{
		PipelineID:    pipelineID,
		Epoch:         epoch,
		Interval:      200 * time.Millisecond,
		MaxDeltaBytes: MaxDeltaBytes,
	}
}

// Compactor điều phối compaction định kỳ và đồng bộ từ các row delta vào row toàn cục.
// invariant: CP-1, CP-4
type Compactor struct {
	db           *sql.DB
	store        CheckpointStore
	pipelineID   string
	sourceEngine model.Engine
	targetEngine model.Engine
	cfg          CompactorConfig

	mu       sync.Mutex
	running  bool
	stopCh   chan struct{}
	doneCh   chan struct{}
	stepHook func(step CompactionStep) error
}

// NewCompactor khởi tạo một instance Compactor mới.
func NewCompactor(
	db *sql.DB,
	store CheckpointStore,
	pipelineID string,
	sourceEngine model.Engine,
	targetEngine model.Engine,
	cfg CompactorConfig,
) *Compactor {
	if cfg.Interval <= 0 {
		cfg.Interval = 200 * time.Millisecond
	}
	if cfg.MaxDeltaBytes <= 0 {
		cfg.MaxDeltaBytes = MaxDeltaBytes
	}

	return &Compactor{
		db:           db,
		store:        store,
		pipelineID:   pipelineID,
		sourceEngine: sourceEngine,
		targetEngine: targetEngine,
		cfg:          cfg,
	}
}

// SetStepHook gán hook tiêm lỗi tại các bước compaction phục vụ kiểm thử CP-1.
func (c *Compactor) SetStepHook(hook func(step CompactionStep) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stepHook = hook
}

// Compact thực hiện một chu kỳ compaction: đọc row delta, gộp vào row toàn cục, ghi lại với điều kiện epoch, rồi cắt tỉa delta.
// invariant: CP-1
func (c *Compactor) Compact(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	epoch := c.cfg.Epoch
	if c.store != nil && c.store.Epoch() != 0 {
		epoch = c.store.Epoch()
	}

	if c.stepHook != nil {
		if err := c.stepHook(StepStart); err != nil {
			return err
		}
	}

	// 1. Đọc toàn bộ row delta của pipeline và epoch
	var deltas []WorkerDelta
	var query string
	if c.targetEngine == model.EnginePG {
		query = "SELECT worker_id, epoch, batch_no, applied FROM _strand_worker_delta WHERE pipeline_id = $1 AND epoch = $2"
	} else {
		query = "SELECT worker_id, epoch, batch_no, applied FROM _strand_worker_delta WHERE pipeline_id = ? AND epoch = ?"
	}

	rows, err := c.db.QueryContext(ctx, query, c.pipelineID, epoch)
	if err != nil {
		return fmt.Errorf("failed to query worker deltas for compaction: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var d WorkerDelta
		if err := rows.Scan(&d.WorkerID, &d.Epoch, &d.BatchNo, &d.Applied); err != nil {
			return fmt.Errorf("failed to scan worker delta: %w", err)
		}
		deltas = append(deltas, d)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error querying deltas: %w", err)
	}

	if c.stepHook != nil {
		if err := c.stepHook(StepReadDeltas); err != nil {
			return err
		}
	}

	// Kiểm tra xem có delta nào chứa dữ liệu không
	hasWork := false
	for _, d := range deltas {
		if strings.TrimSpace(d.Applied) != "" {
			hasWork = true
			break
		}
	}
	if !hasWork {
		return nil
	}

	// 2. Đọc row toàn cục hiện tại và gộp deltas vào
	var globalAppliedStr string
	var globalQuery string
	if c.targetEngine == model.EnginePG {
		globalQuery = "SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1 AND epoch = $2"
	} else {
		globalQuery = "SELECT applied FROM _strand_checkpoint WHERE pipeline_id = ? AND epoch = ?"
	}

	err = c.db.QueryRowContext(ctx, globalQuery, c.pipelineID, epoch).Scan(&globalAppliedStr)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to read global checkpoint: %w", err)
	}

	globalSet, err := ParseAppliedSet(c.sourceEngine, globalAppliedStr)
	if err != nil {
		return fmt.Errorf("failed to parse global applied set: %w", err)
	}

	for _, d := range deltas {
		if strings.TrimSpace(d.Applied) == "" {
			continue
		}
		workerSet, err := ParseAppliedSet(c.sourceEngine, d.Applied)
		if err != nil {
			return fmt.Errorf("failed to parse worker delta: %w", err)
		}
		globalSet.Merge(workerSet)
	}

	if c.stepHook != nil {
		if err := c.stepHook(StepMerge); err != nil {
			return err
		}
	}

	// 3. Ghi row toàn cục mới trong transaction riêng có cùng điều kiện epoch (S02 §4 quy tắc 4)
	newGlobalStr := formatAppliedSet(globalSet)
	now := time.Now().UTC()

	var updateGlobalQuery string
	if c.targetEngine == model.EnginePG {
		updateGlobalQuery = "UPDATE _strand_checkpoint SET applied = $1, updated_at = $2 WHERE pipeline_id = $3 AND epoch = $4"
	} else {
		updateGlobalQuery = "UPDATE _strand_checkpoint SET applied = ?, updated_at = ? WHERE pipeline_id = ? AND epoch = ?"
	}

	res, err := c.db.ExecContext(ctx, updateGlobalQuery, newGlobalStr, now, c.pipelineID, epoch)
	if err != nil {
		return fmt.Errorf("failed to update global checkpoint: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if affected != 1 {
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, "checkpoint compaction fenced: epoch has changed", nil)
	}

	if c.stepHook != nil {
		if err := c.stepHook(StepWriteGlobal); err != nil {
			return err
		}
	}

	// 4. Cắt tỉa (prune) worker deltas đã được gộp thành công
	// Chỉ reset/prune các worker delta có batch_no trùng với batch_no lúc đọc
	// (đảm bảo nếu worker đã commit batch mới trong lúc compaction chạy, batch mới không bị mất)
	for _, d := range deltas {
		if strings.TrimSpace(d.Applied) == "" {
			continue
		}
		var pruneQuery string
		if c.targetEngine == model.EnginePG {
			pruneQuery = "UPDATE _strand_worker_delta SET applied = '' WHERE pipeline_id = $1 AND worker_id = $2 AND epoch = $3 AND batch_no = $4"
		} else {
			pruneQuery = "UPDATE _strand_worker_delta SET applied = '' WHERE pipeline_id = ? AND worker_id = ? AND epoch = ? AND batch_no = ?"
		}
		_, err := c.db.ExecContext(ctx, pruneQuery, c.pipelineID, d.WorkerID, epoch, d.BatchNo)
		if err != nil {
			return fmt.Errorf("failed to prune worker delta: %w", err)
		}
	}

	if c.stepHook != nil {
		if err := c.stepHook(StepPruneDeltas); err != nil {
			return err
		}
	}

	if c.stepHook != nil {
		if err := c.stepHook(StepComplete); err != nil {
			return err
		}
	}

	return nil
}

// Start kích hoạt compaction chạy ngầm định kỳ (mỗi 100–500 ms).
func (c *Compactor) Start(ctx context.Context) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	c.running = true
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	c.mu.Unlock()

	go func() {
		defer close(c.doneCh)
		ticker := time.NewTicker(c.cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				_ = c.Compact(ctx)
			}
		}
	}()
}

// Stop dừng background compactor và chờ hoàn tất.
func (c *Compactor) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	close(c.stopCh)
	done := c.doneCh
	c.mu.Unlock()

	if done != nil {
		<-done
	}
}

// RecordWorkerDelta ghi nhận row delta cho một worker có kiểm tra rào chắn 64 KB (CP-4).
// Nếu delta vượt 64 KB, tự kích hoạt compaction đồng bộ. Nếu vẫn vượt thì từ chối (trả về ErrDeltaExceededLimit).
// invariant: CP-4
func (c *Compactor) RecordWorkerDelta(ctx context.Context, tx *sql.Tx, workerID int, applied string) error {
	epoch := c.cfg.Epoch
	if c.store != nil && c.store.Epoch() != 0 {
		epoch = c.store.Epoch()
	}

	// Kiểm tra nếu delta vượt 64 KB: kích hoạt compaction đồng bộ
	if len(applied) > c.cfg.MaxDeltaBytes {
		if err := c.Compact(ctx); err != nil {
			return fmt.Errorf("sync compaction failed: %w", err)
		}
		// Sau khi compaction hoàn tất, kiểm tra lại nếu một đơn vị delta duy nhất vẫn vượt 64 KB
		if len(applied) > c.cfg.MaxDeltaBytes {
			return ErrDeltaExceededLimit
		}
	}

	// Ghi vào _strand_worker_delta
	var err error
	if c.targetEngine == model.EnginePG {
		query := `
			INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied)
			VALUES ($1, $2, $3, 1, $4)
			ON CONFLICT (pipeline_id, worker_id) DO UPDATE
			SET applied = EXCLUDED.applied, batch_no = _strand_worker_delta.batch_no + 1, epoch = EXCLUDED.epoch
			WHERE _strand_worker_delta.epoch = EXCLUDED.epoch
		`
		if tx != nil {
			var res sql.Result
			res, err = tx.ExecContext(ctx, query, c.pipelineID, workerID, epoch, applied)
			if err == nil {
				affected, _ := res.RowsAffected()
				if affected == 0 {
					return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, "worker delta write fenced: epoch changed", nil)
				}
			}
		} else {
			var res sql.Result
			res, err = c.db.ExecContext(ctx, query, c.pipelineID, workerID, epoch, applied)
			if err == nil {
				affected, _ := res.RowsAffected()
				if affected == 0 {
					return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, "worker delta write fenced: epoch changed", nil)
				}
			}
		}
	} else {
		query := `
			INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied)
			VALUES (?, ?, ?, 1, ?)
			ON DUPLICATE KEY UPDATE applied = VALUES(applied), batch_no = batch_no + 1, epoch = VALUES(epoch)
		`
		if tx != nil {
			_, err = tx.ExecContext(ctx, query, c.pipelineID, workerID, epoch, applied)
		} else {
			_, err = c.db.ExecContext(ctx, query, c.pipelineID, workerID, epoch, applied)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to record worker delta: %w", err)
	}

	return nil
}

func formatAppliedSet(set AppliedSet) string {
	if s, ok := set.(fmt.Stringer); ok {
		return s.String()
	}
	return ""
}
