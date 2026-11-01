package promote

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// MemoryStateStore lưu trữ tiến trình promote trong bộ nhớ (dùng cho testing).
type MemoryStateStore struct {
	mu        sync.RWMutex
	store     map[string]*Progress
	saveHooks []func(p *Progress)
}

// NewMemoryStateStore khởi tạo MemoryStateStore.
func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{
		store: make(map[string]*Progress),
	}
}

// LoadProgress nạp progress theo pipelineID.
func (m *MemoryStateStore) LoadProgress(ctx context.Context, pipelineID string) (*Progress, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p, ok := m.store[pipelineID]
	if !ok {
		return nil, nil
	}
	// Trả về bản sao
	copyP := *p
	copyP.CompletedSteps = append([]PromoteStep(nil), p.CompletedSteps...)
	return &copyP, nil
}

// SaveProgress lưu progress theo pipelineID.
func (m *MemoryStateStore) SaveProgress(ctx context.Context, progress *Progress) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	progress.UpdatedAt = time.Now()
	copyP := *progress
	copyP.CompletedSteps = append([]PromoteStep(nil), progress.CompletedSteps...)
	m.store[progress.PipelineID] = &copyP

	for _, hook := range m.saveHooks {
		hook(&copyP)
	}
	return nil
}

// OnSave đăng ký hook khi lưu (hữu ích cho testing kiểm tra trạng thái tức thời).
func (m *MemoryStateStore) OnSave(hook func(p *Progress)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saveHooks = append(m.saveHooks, hook)
}

// DBStateStore lưu trữ tiến trình promote vào bảng _strand_promote trong target DB.
type DBStateStore struct {
	db *sql.DB
}

// NewDBStateStore khởi tạo DBStateStore.
func NewDBStateStore(db *sql.DB) *DBStateStore {
	return &DBStateStore{db: db}
}

// EnsureTable tạo bảng _strand_promote nếu chưa tồn tại.
func (s *DBStateStore) EnsureTable(ctx context.Context) error {
	q := `CREATE TABLE IF NOT EXISTS _strand_promote (
		pipeline_id VARCHAR(64) PRIMARY KEY,
		state VARCHAR(32) NOT NULL,
		current_step VARCHAR(32) NOT NULL,
		completed_steps JSON NOT NULL,
		sequences JSON,
		suggested_sequences JSON,
		updated_at TIMESTAMP NOT NULL
	);`
	_, err := s.db.ExecContext(ctx, q)
	return err
}

// LoadProgress nạp progress từ DB.
func (s *DBStateStore) LoadProgress(ctx context.Context, pipelineID string) (*Progress, error) {
	if err := s.EnsureTable(ctx); err != nil {
		return nil, fmt.Errorf("ensure _strand_promote table: %w", err)
	}

	q := `SELECT pipeline_id, state, current_step, completed_steps, sequences, suggested_sequences, updated_at
		FROM _strand_promote WHERE pipeline_id = ?;`
	row := s.db.QueryRowContext(ctx, q, pipelineID)

	var p Progress
	var completedJSON, seqsJSON, suggJSON []byte
	err := row.Scan(&p.PipelineID, &p.State, &p.CurrentStep, &completedJSON, &seqsJSON, &suggJSON, &p.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("scan _strand_promote: %w", err)
	}

	if len(completedJSON) > 0 {
		_ = json.Unmarshal(completedJSON, &p.CompletedSteps)
	}
	if len(seqsJSON) > 0 {
		_ = json.Unmarshal(seqsJSON, &p.Sequences)
	}
	if len(suggJSON) > 0 {
		_ = json.Unmarshal(suggJSON, &p.SuggestedSequences)
	}
	return &p, nil
}

// SaveProgress lưu progress vào DB.
func (s *DBStateStore) SaveProgress(ctx context.Context, progress *Progress) error {
	if err := s.EnsureTable(ctx); err != nil {
		return fmt.Errorf("ensure _strand_promote table: %w", err)
	}

	completedJSON, _ := json.Marshal(progress.CompletedSteps)
	seqsJSON, _ := json.Marshal(progress.Sequences)
	suggJSON, _ := json.Marshal(progress.SuggestedSequences)

	q := `REPLACE INTO _strand_promote (pipeline_id, state, current_step, completed_steps, sequences, suggested_sequences, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?);`
	_, err := s.db.ExecContext(ctx, q,
		progress.PipelineID,
		string(progress.State),
		string(progress.CurrentStep),
		completedJSON,
		seqsJSON,
		suggJSON,
		progress.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("save _strand_promote: %w", err)
	}
	return nil
}
