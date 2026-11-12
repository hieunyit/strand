package promote

import (
	"context"
	"time"
)

// PromoteStep định nghĩa các bước tuần tự của promote theo spec/02 §8.
type PromoteStep string

const (
	StepFenceSource    PromoteStep = "fence_source"    // Bước 1: Fence source cũ
	StepHoldReadOnly   PromoteStep = "hold_readonly"   // Bước 2: Giữ target read-only
	StepApplyRemaining PromoteStep = "apply_remaining" // Bước 3: Apply nốt phần log còn lại
	StepSetval         PromoteStep = "setval"          // Bước 4: Chạy setval cho mọi sequence và identity
	StepOpenWrites     PromoteStep = "open_writes"     // Bước 5: Mở quyền ghi
	StepSwitchDNS      PromoteStep = "switch_dns"      // Bước 6: Chuyển DNS hoặc VIP
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
	// Feeds: các cột số nguyên (của sequence độc lập) có DEFAULT gọi nextval của sequence này (pg_depend
	// deptype 'n' từ pg_attrdef). Source MySQL không có sequence nên giá trị của sequence độc lập ở target
	// PG được suy ra từ max (giảm dần: min) của các cột này ở target, như sequence gắn cột (02 §8).
	Feeds []ColumnRef `json:"feeds,omitempty"`
	// Decided: FinalValue của sequence độc lập đã được chốt (đọc từ source sau fence, hoặc người vận hành
	// chỉ định qua override) và đã lưu vào _strand_promote. Chạy lại promote dùng tiếp giá trị này,
	// không hỏi lại người vận hành, và không bao giờ hạ nó xuống.
	Decided bool `json:"decided,omitempty"`
}

// ColumnRef là một cột của bảng target (tên bảng định dạng như SequenceInfo.Table).
type ColumnRef struct {
	Table  string `json:"table"`
	Column string `json:"column"`
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
	// SourceFencedExternally: người vận hành xác nhận source cũ đã bị cô lập bằng cách khác
	// (tắt máy, STONITH, chặn mạng) khi Strand không fence được vì source không truy cập được.
	SourceFencedExternally bool `json:"source_fenced_externally"`
}

// Progress lưu trữ tiến trình promote để có thể resume từ bước đang dở.
type Progress struct {
	PipelineID         string           `json:"pipeline_id"`
	State              State            `json:"state"`
	CurrentStep        PromoteStep      `json:"current_step"`
	CompletedSteps     []PromoteStep    `json:"completed_steps"`
	Sequences          []SequenceInfo   `json:"sequences"`
	SuggestedSequences map[string]int64 `json:"suggested_sequences"`
	FailoverSummary    string           `json:"failover_summary,omitempty"`
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

// FenceHolder là interface tùy chọn cho SourceDriver mà fence cần được GIỮ trong suốt promote (MySQL
// 5.5/5.6/5.7<5.7.8: FLUSH TABLES WITH READ LOCK trên một kết nối riêng). FenceHeld trả lỗi nếu fence
// đang không được giữ bởi tiến trình này (khóa mất, hoặc promote chạy lại sau khi tiến trình cũ chết:
// khóa đã nhả dù bước fence đã đánh dấu xong). Promoter fence lại khi resume và kiểm trước khi mở ghi.
type FenceHolder interface {
	FenceHeld(ctx context.Context) error
}

// SequencelessSource là interface tùy chọn cho SourceDriver: HasSequences() = false nghĩa là source
// không có sequence (MySQL dùng AUTO_INCREMENT). Khi đó sequence độc lập ở target PG không có bản tương
// ứng ở source: promote lấy giá trị từ các cột target mà sequence cấp giá trị (SequenceInfo.Feeds)
// cộng margin, không đọc source và không hỏi người vận hành.
type SequencelessSource interface {
	HasSequences() bool
}

// TargetDriver điều khiển target DB trong promote.
type TargetDriver interface {
	HoldReadOnly(ctx context.Context) error
	IsReadOnly(ctx context.Context) (bool, error)
	ApplyRemaining(ctx context.Context) error
	FindColumnMax(ctx context.Context, table, column string) (int64, error)
	// Setval đặt sequence tới val, không bao giờ lùi theo chiều của sequence (tăng: GREATEST, giảm: LEAST).
	Setval(ctx context.Context, seq SequenceInfo, val int64) error
	OpenWrites(ctx context.Context) error
}

// ColumnMinFinder là interface tùy chọn cho TargetDriver: MIN của cột, dùng cho sequence gắn cột GIẢM
// dần (INCREMENT âm) — giá trị kế tiếp phải thấp hơn mọi giá trị đã dùng nên lấy min(cột) - margin.
// Driver không hỗ trợ thì promote dừng hỏi người vận hành (exit 10) thay vì setval sai chiều.
type ColumnMinFinder interface {
	FindColumnMin(ctx context.Context, table, column string) (int64, error)
}

// FailoverSummarizer là interface tùy chọn cho TargetDriver để tóm tắt tập giao dịch đã apply
// tại target khi promote dừng vì không fence được source (quyết định P1-17, câu 3).
type FailoverSummarizer interface {
	FailoverSummary(ctx context.Context) (string, error)
}

// StateStore quản lý lưu và nạp tiến trình promote.
type StateStore interface {
	LoadProgress(ctx context.Context, pipelineID string) (*Progress, error)
	SaveProgress(ctx context.Context, progress *Progress) error
}

// ReadOnlyProgressLoader: StateStore nạp được progress mà không ghi gì (không tạo bảng); promote
// --dry-run dùng giao diện này khi store hỗ trợ (review R31).
type ReadOnlyProgressLoader interface {
	LoadProgressReadOnly(ctx context.Context, pipelineID string) (*Progress, error)
}
