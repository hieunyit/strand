package admin

import (
	"context"
	"time"
)

// Các tên lệnh điều khiển qua admin socket theo spec/02 §10.
const (
	CmdTop              = "top"
	CmdPause            = "pause"
	CmdResume           = "resume"
	CmdThrottleSet      = "throttle_set"
	CmdCheckpointShow   = "checkpoint_show"
	CmdCheckpointRewind = "checkpoint_rewind"
	CmdSnapshotAdd      = "snapshot_add"
)

// SnapshotAdder là interface tùy chọn cho `strand snapshot add --table <t>` (02 §10).
type SnapshotAdder interface {
	SnapshotAdd(ctx context.Context, table string) error
}

// Request chứa thông tin yêu cầu gửi qua admin socket.
type Request struct {
	Command      string `json:"command"`
	SnapshotOnly bool   `json:"snapshot_only,omitempty"` // Dùng cho resume --snapshot-only
	SourceIOPS   int    `json:"source_iops,omitempty"`   // Dùng cho throttle set
	TargetIOPS   int    `json:"target_iops,omitempty"`   // Dùng cho throttle set
	Soft         bool   `json:"soft,omitempty"`          // Dùng cho throttle set
	RewindTo     string `json:"rewind_to,omitempty"`     // Dùng cho checkpoint rewind
	Table        string `json:"table,omitempty"`         // Dùng cho snapshot add
}

// Response chứa kết quả trả về từ admin socket.
type Response struct {
	OK               bool              `json:"ok"`
	Error            string            `json:"error,omitempty"`
	Top              *TopInfo          `json:"top,omitempty"`
	PauseResult      *PauseResult      `json:"pause_result,omitempty"`
	ResumeResult     *ResumeResult     `json:"resume_result,omitempty"`
	ThrottleResult   *ThrottleResult   `json:"throttle_result,omitempty"`
	CheckpointResult *CheckpointResult `json:"checkpoint_result,omitempty"`
}

// TopInfo chứa thông tin hiển thị của lệnh `strand top` theo spec/02 §10.
type TopInfo struct {
	PipelineID string  `json:"pipeline_id"`
	State      string  `json:"state"`
	LagSeconds float64 `json:"lag_seconds"`
	Throughput float64 `json:"throughput"` // rows/sec hoặc MB/s
	Zone       string  `json:"zone"`       // Zone GRACE hoặc onoff (e.g. green, yellow, red, breaker)
	Progress   float64 `json:"progress"`   // 0.0 - 100.0 %
	ETASeconds float64 `json:"eta_seconds"`
}

// PauseResult chứa kết quả của lệnh `strand pause`.
type PauseResult struct {
	State  string `json:"state"`
	Paused bool   `json:"paused"`
}

// ResumeResult chứa kết quả của lệnh `strand resume`.
type ResumeResult struct {
	State        string `json:"state"`
	SnapshotOnly bool   `json:"snapshot_only"`
	Resumed      bool   `json:"resumed"`
}

// ThrottleResult chứa kết quả của lệnh `strand throttle set`.
type ThrottleResult struct {
	SourceIOPSSoft int `json:"source_iops_soft"`
	SourceIOPSHard int `json:"source_iops_hard"`
	TargetIOPSSoft int `json:"target_iops_soft"`
	TargetIOPSHard int `json:"target_iops_hard"`
}

// CheckpointResult chứa thông tin của lệnh `strand checkpoint show|rewind`.
type CheckpointResult struct {
	PipelineID string    `json:"pipeline_id"`
	Epoch      int64     `json:"epoch"`
	Applied    string    `json:"applied"`
	RewoundTo  string    `json:"rewound_to,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// PipelineAdminHandler là interface xử lý các lệnh quản trị của pipeline.
type PipelineAdminHandler interface {
	Top(ctx context.Context) (*TopInfo, error)
	Pause(ctx context.Context) (*PauseResult, error)
	Resume(ctx context.Context, snapshotOnly bool) (*ResumeResult, error)
	ThrottleSet(ctx context.Context, req Request) (*ThrottleResult, error)
	CheckpointShow(ctx context.Context) (*CheckpointResult, error)
	CheckpointRewind(ctx context.Context, to string) (*CheckpointResult, error)
}
