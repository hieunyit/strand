package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"strand/internal/model"
)

// MySQLStore cài đặt CheckpointStore cho target MySQL theo spec/02 §4.
type MySQLStore struct {
	mu           sync.RWMutex
	db           *sql.DB
	pipelineID   string
	sourceEngine model.Engine
	currentEpoch int64
}

// NewMySQLStore khởi tạo MySQLStore.
func NewMySQLStore(db *sql.DB, pipelineID string, sourceEngine model.Engine) *MySQLStore {
	return &MySQLStore{
		db:           db,
		pipelineID:   pipelineID,
		sourceEngine: sourceEngine,
	}
}

// EnsureTables tạo đủ 3 bảng metadata trên MySQL target theo spec/02 §4.
func (s *MySQLStore) EnsureTables(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS _strand_pipeline (
			pipeline_id  VARCHAR(64) PRIMARY KEY,
			epoch        BIGINT NOT NULL,
			owner        VARCHAR(255) NOT NULL,
			config_hash  VARCHAR(64) NOT NULL,
			meta_version INT NOT NULL,
			started_at   TIMESTAMP(6) NOT NULL
		) ENGINE=InnoDB;`,

		`CREATE TABLE IF NOT EXISTS _strand_checkpoint (
			pipeline_id  VARCHAR(64) PRIMARY KEY,
			epoch        BIGINT NOT NULL,
			applied      MEDIUMTEXT NOT NULL,
			updated_at   TIMESTAMP(6) NOT NULL
		) ENGINE=InnoDB;`,

		`CREATE TABLE IF NOT EXISTS _strand_worker_delta (
			pipeline_id  VARCHAR(64) NOT NULL,
			worker_id    SMALLINT NOT NULL,
			epoch        BIGINT NOT NULL,
			batch_no     BIGINT NOT NULL,
			applied      MEDIUMTEXT NOT NULL,
			PRIMARY KEY (pipeline_id, worker_id)
		) ENGINE=InnoDB;`,
	}

	for _, q := range queries {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("failed to execute DDL on MySQL: %w", err)
		}
	}
	return nil
}

// Lock lấy khóa advisory GET_LOCK trên MySQL.
// spec: 02 §4 quy tắc 1
func (s *MySQLStore) Lock(ctx context.Context, pipelineID string) (func() error, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire db connection for lock: %w", err)
	}

	lockName := fmt.Sprintf("strand:%s", pipelineID)
	var res sql.NullInt64

	// Timeout = 0 để kiểm tra ngay lập tức mà không chờ
	err = conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lockName).Scan(&res)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("GET_LOCK query failed: %w", err)
	}

	if !res.Valid || res.Int64 != 1 {
		_ = conn.Close()
		return nil, ErrPipelineLocked
	}

	var once sync.Once
	release := func() error {
		var relErr error
		once.Do(func() {
			var relRes sql.NullInt64
			if err := conn.QueryRowContext(context.Background(), "SELECT RELEASE_LOCK(?)", lockName).Scan(&relRes); err != nil {
				relErr = fmt.Errorf("RELEASE_LOCK query failed: %w", err)
			}
			if closeErr := conn.Close(); closeErr != nil && relErr == nil {
				relErr = closeErr
			}
		})
		return relErr
	}

	return release, nil
}

// AdvanceEpoch tăng epoch trong một transaction và đồng bộ sang checkpoint & deltas.
// spec: 02 §4 quy tắc 2
func (s *MySQLStore) AdvanceEpoch(ctx context.Context, pipelineID, owner, configHash string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var existingEpoch sql.NullInt64
	err = tx.QueryRowContext(ctx, "SELECT epoch FROM _strand_pipeline WHERE pipeline_id = ? FOR UPDATE", pipelineID).Scan(&existingEpoch)

	var newEpoch int64
	if err == sql.ErrNoRows {
		newEpoch = 1
		_, err = tx.ExecContext(ctx,
			"INSERT INTO _strand_pipeline (pipeline_id, epoch, owner, config_hash, meta_version, started_at) VALUES (?, 1, ?, ?, 1, NOW(6))",
			pipelineID, owner, configHash,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to insert _strand_pipeline: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			"INSERT INTO _strand_checkpoint (pipeline_id, epoch, applied, updated_at) VALUES (?, 1, '', NOW(6)) ON DUPLICATE KEY UPDATE epoch = 1",
			pipelineID,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to insert initial _strand_checkpoint: %w", err)
		}
	} else if err != nil {
		return 0, fmt.Errorf("failed to select _strand_pipeline: %w", err)
	} else {
		newEpoch = existingEpoch.Int64 + 1
		_, err = tx.ExecContext(ctx,
			"UPDATE _strand_pipeline SET epoch = ?, owner = ?, config_hash = ?, started_at = NOW(6) WHERE pipeline_id = ?",
			newEpoch, owner, configHash, pipelineID,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to update _strand_pipeline: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			"UPDATE _strand_checkpoint SET epoch = ? WHERE pipeline_id = ?",
			newEpoch, pipelineID,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to update _strand_checkpoint epoch: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			"UPDATE _strand_worker_delta SET epoch = ? WHERE pipeline_id = ?",
			newEpoch, pipelineID,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to update _strand_worker_delta epoch: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit epoch transaction: %w", err)
	}

	s.currentEpoch = newEpoch
	return newEpoch, nil
}

// Epoch trả về epoch hiện tại.
func (s *MySQLStore) Epoch() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentEpoch
}

// LoadApplied nạp AppliedSet gộp từ checkpoint toàn cục và các worker delta.
func (s *MySQLStore) LoadApplied(ctx context.Context) (AppliedSet, error) {
	s.mu.RLock()
	epoch := s.currentEpoch
	s.mu.RUnlock()

	var appliedStr string
	err := s.db.QueryRowContext(ctx, "SELECT applied FROM _strand_checkpoint WHERE pipeline_id = ?", s.pipelineID).Scan(&appliedStr)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to query global checkpoint: %w", err)
	}

	globalSet, err := ParseAppliedSet(s.sourceEngine, appliedStr)
	if err != nil {
		return nil, err
	}

	// Đọc mọi row delta có cùng epoch
	rows, err := s.db.QueryContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id = ? AND epoch = ?", s.pipelineID, epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to query worker deltas: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var deltaStr string
		if err := rows.Scan(&deltaStr); err != nil {
			return nil, err
		}
		workerSet, err := ParseAppliedSet(s.sourceEngine, deltaStr)
		if err != nil {
			return nil, err
		}
		if globalSet, err = MergeChecked(globalSet, workerSet); err != nil {
			return nil, fmt.Errorf("merge worker delta: %w", err)
		}
	}

	return globalSet, rows.Err()
}

// WriteGlobal ghi lại checkpoint toàn cục có kiểm tra điều kiện epoch.
func (s *MySQLStore) WriteGlobal(ctx context.Context, g Global) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE _strand_checkpoint SET applied = ?, updated_at = NOW(6) WHERE pipeline_id = ? AND epoch = ?",
		g.Applied, g.PipelineID, g.Epoch,
	)
	if err != nil {
		return fmt.Errorf("failed to update _strand_checkpoint: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("fenced: epoch mismatched or checkpoint row missing")
	}

	return nil
}

// Close đóng tài nguyên.
func (s *MySQLStore) Close() error {
	return nil
}

// LoadAppliedSnapshotMySQL đọc tập đã apply hiện hành của pipeline trên target MySQL
// (row toàn cục ∪ mọi row delta của epoch hiện tại) mà không cần giữ khóa pipeline.
// Dùng cho promote để kiểm target đã theo kịp source.
func LoadAppliedSnapshotMySQL(ctx context.Context, db *sql.DB, pipelineID string, sourceEngine model.Engine) (AppliedSet, error) {
	var epoch int64
	if err := db.QueryRowContext(ctx, "SELECT epoch FROM _strand_pipeline WHERE pipeline_id = ?", pipelineID).Scan(&epoch); err != nil {
		return nil, fmt.Errorf("read pipeline epoch: %w", err)
	}
	var global string
	if err := db.QueryRowContext(ctx, "SELECT applied FROM _strand_checkpoint WHERE pipeline_id = ?", pipelineID).Scan(&global); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("read global checkpoint: %w", err)
	}
	set, err := ParseAppliedSet(sourceEngine, global)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id = ? AND epoch = ?", pipelineID, epoch)
	if err != nil {
		return nil, fmt.Errorf("read worker deltas: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		ds, err := ParseAppliedSet(sourceEngine, d)
		if err != nil {
			return nil, err
		}
		if set, err = MergeChecked(set, ds); err != nil {
			return nil, fmt.Errorf("merge worker delta: %w", err)
		}
	}
	return set, rows.Err()
}
