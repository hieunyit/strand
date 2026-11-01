package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/zeebo/xxh3"

	"strand/internal/model"
)

// PGStore cài đặt CheckpointStore cho target PostgreSQL theo spec/02 §4.
type PGStore struct {
	mu           sync.RWMutex
	db           *sql.DB
	pipelineID   string
	sourceEngine model.Engine
	currentEpoch int64
}

// NewPGStore khởi tạo PGStore.
func NewPGStore(db *sql.DB, pipelineID string, sourceEngine model.Engine) *PGStore {
	return &PGStore{
		db:           db,
		pipelineID:   pipelineID,
		sourceEngine: sourceEngine,
	}
}

// EnsureTables tạo đủ 3 bảng metadata trên PostgreSQL target theo spec/02 §4.
func (s *PGStore) EnsureTables(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS _strand_pipeline (
			pipeline_id  text PRIMARY KEY,
			epoch        bigint NOT NULL,
			owner        text   NOT NULL,
			config_hash  text   NOT NULL,
			meta_version int    NOT NULL,
			started_at   timestamptz NOT NULL
		);`,

		`CREATE TABLE IF NOT EXISTS _strand_checkpoint (
			pipeline_id  text PRIMARY KEY,
			epoch        bigint NOT NULL,
			applied      text   NOT NULL,
			updated_at   timestamptz NOT NULL
		) WITH (fillfactor = 50);`,

		`CREATE TABLE IF NOT EXISTS _strand_worker_delta (
			pipeline_id  text     NOT NULL,
			worker_id    smallint NOT NULL,
			epoch        bigint   NOT NULL,
			batch_no     bigint   NOT NULL,
			applied      text     NOT NULL,
			PRIMARY KEY (pipeline_id, worker_id)
		) WITH (fillfactor = 50);`,
	}

	for _, q := range queries {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("failed to execute DDL on PostgreSQL: %w", err)
		}
	}
	return nil
}

func pgAdvisoryLockKeys(pipelineID string) (int32, int32) {
	h := xxh3.HashString("strand:" + pipelineID)
	return int32(h >> 32), int32(h & 0xFFFFFFFF)
}

// Lock lấy khóa advisory lock hai số int4 trên PostgreSQL.
// spec: 02 §4 quy tắc 1
func (s *PGStore) Lock(ctx context.Context, pipelineID string) (func() error, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire db connection for pg lock: %w", err)
	}

	k1, k2 := pgAdvisoryLockKeys(pipelineID)
	var locked bool

	err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1, $2)", k1, k2).Scan(&locked)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("pg_try_advisory_lock query failed: %w", err)
	}

	if !locked {
		_ = conn.Close()
		return nil, ErrPipelineLocked
	}

	var once sync.Once
	release := func() error {
		var relErr error
		once.Do(func() {
			var unlocked bool
			if err := conn.QueryRowContext(context.Background(), "SELECT pg_advisory_unlock($1, $2)", k1, k2).Scan(&unlocked); err != nil {
				relErr = fmt.Errorf("pg_advisory_unlock query failed: %w", err)
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
func (s *PGStore) AdvanceEpoch(ctx context.Context, pipelineID, owner, configHash string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin pg transaction: %w", err)
	}
	defer tx.Rollback()

	var existingEpoch sql.NullInt64
	err = tx.QueryRowContext(ctx, "SELECT epoch FROM _strand_pipeline WHERE pipeline_id = $1 FOR UPDATE", pipelineID).Scan(&existingEpoch)

	var newEpoch int64
	if err == sql.ErrNoRows {
		newEpoch = 1
		_, err = tx.ExecContext(ctx,
			"INSERT INTO _strand_pipeline (pipeline_id, epoch, owner, config_hash, meta_version, started_at) VALUES ($1, 1, $2, $3, 1, NOW())",
			pipelineID, owner, configHash,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to insert _strand_pipeline on PG: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			"INSERT INTO _strand_checkpoint (pipeline_id, epoch, applied, updated_at) VALUES ($1, 1, '', NOW()) ON CONFLICT (pipeline_id) DO UPDATE SET epoch = 1",
			pipelineID,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to insert initial _strand_checkpoint on PG: %w", err)
		}
	} else if err != nil {
		return 0, fmt.Errorf("failed to select _strand_pipeline on PG: %w", err)
	} else {
		newEpoch = existingEpoch.Int64 + 1
		_, err = tx.ExecContext(ctx,
			"UPDATE _strand_pipeline SET epoch = $1, owner = $2, config_hash = $3, started_at = NOW() WHERE pipeline_id = $4",
			newEpoch, owner, configHash, pipelineID,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to update _strand_pipeline on PG: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			"UPDATE _strand_checkpoint SET epoch = $1 WHERE pipeline_id = $2",
			newEpoch, pipelineID,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to update _strand_checkpoint epoch on PG: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			"UPDATE _strand_worker_delta SET epoch = $1 WHERE pipeline_id = $2",
			newEpoch, pipelineID,
		)
		if err != nil {
			return 0, fmt.Errorf("failed to update _strand_worker_delta epoch on PG: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit pg epoch transaction: %w", err)
	}

	s.currentEpoch = newEpoch
	return newEpoch, nil
}

// Epoch trả về epoch hiện tại.
func (s *PGStore) Epoch() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentEpoch
}

// LoadApplied nạp AppliedSet gộp từ checkpoint toàn cục và các worker delta.
func (s *PGStore) LoadApplied(ctx context.Context) (AppliedSet, error) {
	s.mu.RLock()
	epoch := s.currentEpoch
	s.mu.RUnlock()

	var appliedStr string
	err := s.db.QueryRowContext(ctx, "SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1", s.pipelineID).Scan(&appliedStr)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to query global checkpoint on PG: %w", err)
	}

	globalSet, err := ParseAppliedSet(s.sourceEngine, appliedStr)
	if err != nil {
		return nil, err
	}

	// Đọc mọi row delta có cùng epoch
	rows, err := s.db.QueryContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id = $1 AND epoch = $2", s.pipelineID, epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to query worker deltas on PG: %w", err)
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
		globalSet.Merge(workerSet)
	}

	return globalSet, rows.Err()
}

// WriteGlobal ghi lại checkpoint toàn cục có kiểm tra điều kiện epoch.
func (s *PGStore) WriteGlobal(ctx context.Context, g Global) error {
	res, err := s.db.ExecContext(ctx,
		"UPDATE _strand_checkpoint SET applied = $1, updated_at = NOW() WHERE pipeline_id = $2 AND epoch = $3",
		g.Applied, g.PipelineID, g.Epoch,
	)
	if err != nil {
		return fmt.Errorf("failed to update _strand_checkpoint on PG: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("fenced: epoch mismatched or checkpoint row missing on PG")
	}

	return nil
}

// Close đóng tài nguyên.
func (s *PGStore) Close() error {
	return nil
}
