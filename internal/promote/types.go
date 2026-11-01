package promote

import (
	"context"
	"time"
)

// PromoteStep định nghĩa các bước tuần tự của promote theo spec/02 §8.
type PromoteStep string

const (
	StepFenceSource     PromoteStep = "fence_source"     // Bước 1: Fence source cũ
	StepHoldReadOnly    PromoteStep = "hold_readonly"    // Bước 2: Giữ target read-only
	StepApplyRemaining  PromoteStep = "apply_remaining"  // Bước 3: Apply nốt phần log còn lại
	StepSetval          PromoteStep = "setval"           // Bước 4: Chạy setval cho mọi sequence và identity
	StepOpenWrites      PromoteStep = "open_writes"      // Bước 5: Mở quyền ghi
	StepSwitchDNS       PromoteStep = "switch_dns"       // Bước 6: Chuyển DNS hoặc VIP
)

// State đại diện trạng thái pipeline liên quan promote theo spec/02 §12.
type State string

const (
	StateValidating       State = "validating"
	StatePromoting        State = "promoting"
	StateAwaitingOperator State = "awaiting_operator" // spec/02 §12: Cần quyết định, ví dụ sequence độc lập khi source không truy cập được
	StatePromoted         State = "promoted"          // spec/02 §12: Target đã mở ghi; pipeline kết thúc
)

// SequenceType phân loại sequence theo spec/02 §8.
type SequenceType string

const (
	SequenceTypeColumn      SequenceType = "column"      // Sequence gắn với cột (max(col) + margin)
	SequenceTypeIndependent SequenceType = "independent" // Sequence độc lập (source last_value + margin)
)

// SequenceInfo chứa thông tin sequence cần setval.
type SequenceInfo struct {
	Name            string       `json:"name"`
	Table           string       `json:"table"`
	Column          string       `json:"column"`
	Type            SequenceType `json:"type"`
	CacheSize       int64        `json:"cache_size"`
	IncrementBy     int64        `json:"increment_by"`
	LastSyncedValue int64        `json:"last_synced_value"`
	CurrentValue    int64        `json:"current_value"`
	SuggestedValue  int64        `json:"suggested_value"`
	FinalValue      int64        `json:"final_value"`
}

// Config cấu hình cho lệnh promote.
type Config struct {
	PipelineID        string           `json:"pipeline_id"`
	TargetDSN         string           `json:"target_dsn"`
	SourceDSN         string           `json:"source_dsn"`
	DryRun            bool             `json:"dry_run"`
	PeakRate          float64          `json:"peak_rate"`          // r_peak (keys/sec)
	SyncIntervalSec   float64          `json:"sync_interval_sec"`  // Delta t_sync (seconds)
	SequenceOverrides map[string]int64 `json:"sequence_overrides"` // Giá trị do người vận hành chỉ định
}

// Progress lưu trữ tiến trình promote để có thể resume từ bước đang dở.
type Progress struct {
	PipelineID         string           `json:"pipeline_id"`
	State              State            `json:"state"`
	CurrentStep        PromoteStep      `json:"current_step"`
	CompletedSteps     []PromoteStep    `json:"completed_steps"`
	Sequences          []SequenceInfo   `json:"sequences"`
	SuggestedSequences map[string]int64 `json:"suggested_sequences"`
	UpdatedAt          time.Time        `json:"updated_at"`
}

// IsStepCompleted kiểm tra xem một bước đã hoàn thành chưa.
func (p *Progress) IsStepCompleted(step PromoteStep) bool {
	for _, s := range p.CompletedSteps {
		if s == step {
			return true
		}
	}
	return false
}

// MarkStepCompleted đánh dấu một bước hoàn thành.
func (p *Progress) MarkStepCompleted(step PromoteStep) {
	if !p.IsStepCompleted(step) {
		p.CompletedSteps = append(p.CompletedSteps, step)
	}
	p.UpdatedAt = time.Now()
}

// SourceDriver điều khiển source DB trong promote.
type SourceDriver interface {
	Fence(ctx context.Context) error
	IsReachable(ctx context.Context) bool
	ReadSequenceLastValue(ctx context.Context, seqName string) (int64, error)
}

// TargetDriver điều khiển target DB trong promote.
type TargetDriver interface {
	HoldReadOnly(ctx context.Context) error
	IsReadOnly(ctx context.Context) (bool, error)
	ApplyRemaining(ctx context.Context) error
	FindColumnMax(ctx context.Context, table, column string) (int64, error)
	Setval(ctx context.Context, seq SequenceInfo, val int64) error
	OpenWrites(ctx context.Context) error
}

// StateStore quản lý lưu và nạp tiến trình promote.
type StateStore interface {
	LoadProgress(ctx context.Context, pipelineID string) (*Progress, error)
	SaveProgress(ctx context.Context, progress *Progress) error
}
