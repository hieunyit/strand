package admin

import (
	"context"
	"fmt"
	"time"
)

// Các tên lệnh điều khiển qua admin socket theo spec/02 §10.
const (
	CmdTop              = "top"
	CmdPause            = "pause"
	CmdPauseSnapshot    = "pause_snapshot"
	CmdResume           = "resume"
	CmdThrottleSet      = "throttle_set"
	CmdCheckpointShow   = "checkpoint_show"
	CmdCheckpointRewind = "checkpoint_rewind"
	CmdSnapshotAdd      = "snapshot_add"
	CmdVerifyDelta      = "verify_delta"
	// Lệnh chỉ quan sát cho Web UI (docs/PLAN-WEB-UI.md §5): không đổi trạng thái pipeline.
	CmdStatus = "status"
	CmdTables = "tables"
	CmdEvents = "events"
	// Chỉ đọc: liệt kê bảng của source để chọn khi `snapshot add` (Web UI, M3).
	CmdSourceTables = "source_tables"
)

// StatusProvider là interface tùy chọn cho lệnh `status`.
type StatusProvider interface {
	Status(ctx context.Context) (*StatusInfo, error)
}

// TablesProvider là interface tùy chọn cho lệnh `tables`.
type TablesProvider interface {
	Tables(ctx context.Context) ([]TableInfo, error)
}

// EventsProvider là interface tùy chọn cho lệnh `events`.
type EventsProvider interface {
	Events(ctx context.Context) ([]EventRecord, error)
}

// SourceTablesProvider là interface tùy chọn cho lệnh `source_tables`.
type SourceTablesProvider interface {
	SourceTables(ctx context.Context) ([]SourceTable, error)
}

// SourceTable là một bảng của source; InPipeline = đã nằm trong danh sách bảng đang chạy của pipeline.
type SourceTable struct {
	Table      string `json:"table"`
	InPipeline bool   `json:"in_pipeline"`
}

// DeltaVerifier là interface tùy chọn cho `strand verify --mode delta`.
type DeltaVerifier interface {
	VerifyDelta(ctx context.Context) (string, error)
}

// SnapshotAdder là interface tùy chọn cho `strand snapshot add --table <t>` (02 §10).
type SnapshotAdder interface {
	SnapshotAdd(ctx context.Context, table string) error
}

// SnapshotPauser là interface tùy chọn cho `strand pause --snapshot` (02 §10).
type SnapshotPauser interface {
	PauseSnapshot(ctx context.Context) (*PauseResult, error)
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
	Message          string            `json:"message,omitempty"`
	Top              *TopInfo          `json:"top,omitempty"`
	PauseResult      *PauseResult      `json:"pause_result,omitempty"`
	ResumeResult     *ResumeResult     `json:"resume_result,omitempty"`
	ThrottleResult   *ThrottleResult   `json:"throttle_result,omitempty"`
	CheckpointResult *CheckpointResult `json:"checkpoint_result,omitempty"`
	Status           *StatusInfo       `json:"status,omitempty"`
	Tables           []TableInfo       `json:"tables,omitempty"`
	Events           []EventRecord     `json:"events,omitempty"`
	SourceTables     []SourceTable     `json:"source_tables,omitempty"`
}

// GovernorInfo mô tả bộ điều tốc (spec/02 §6) tại thời điểm hỏi.
type GovernorInfo struct {
	Algorithm      string  `json:"algorithm"`
	ForegroundRate float64 `json:"foreground_rate"`
	BackgroundRate float64 `json:"background_rate"`
	Zone           string  `json:"zone"`
	Pressure       float64 `json:"pressure"`
	BreakerOpen    bool    `json:"breaker_open"`
	BreakerReason  string  `json:"breaker_reason,omitempty"`
}

// StatusInfo là kết quả lệnh `status`: thông tin tĩnh/chậm đổi của một lần chạy `strand sync`.
type StatusInfo struct {
	PipelineID     string       `json:"pipeline_id"`
	State          string       `json:"state"`
	Epoch          int64        `json:"epoch"`
	StartedAt      time.Time    `json:"started_at"`
	UptimeSeconds  float64      `json:"uptime_seconds"`
	StartAt        string       `json:"start_at,omitempty"` // giá trị --start-at (rỗng = tiếp tục từ checkpoint)
	StartPosition  string       `json:"start_position"`     // tập đã apply lúc khởi động
	SourceKind     string       `json:"source_kind"`
	TargetKind     string       `json:"target_kind"`
	CommitOrder    string       `json:"commit_order"`
	Workers        int          `json:"workers"`
	Paused         bool         `json:"paused"`
	SnapshotPaused bool         `json:"snapshot_paused"`
	AppliedTxns    int64        `json:"applied_txns"`
	AppliedRows    int64        `json:"applied_rows"`
	Governor       GovernorInfo `json:"governor"`
}

// TableInfo là trạng thái snapshot của một bảng (lệnh `tables`).
type TableInfo struct {
	Table string `json:"table"`
	// State: done | running | pending (snapshot logic); bảng seed/start_at đã xong báo done.
	State string `json:"state"`
	// Ranges: số range theo trạng thái (chỉ bảng đang snapshot).
	Ranges       map[string]int `json:"ranges,omitempty"`
	RowsEstimate int64          `json:"rows_estimate,omitempty"` // thống kê của source (có thể lệch)
	RowsCopied   int64          `json:"rows_copied,omitempty"`   // xấp xỉ theo tiến độ PK
	ProgressPct  float64        `json:"progress_pct"`
	ETASeconds   float64        `json:"eta_seconds,omitempty"`
	PK           []string       `json:"pk,omitempty"`
	Key          string         `json:"key,omitempty"` // khóa hàng: "PRIMARY (id)", "UNIQUE uk (a)" hoặc "full row (keyless)"
	GenericPK    bool           `json:"generic_pk"`    // PK không phải một cột số nguyên (khóa tổng quát)

	// CDC activity of the running pipeline process (since it started; a snapshot copy is not counted):
	// row changes applied to the target by operation, when the last one was committed on the target, and
	// the source commit time of that change (MongoDB: of the batch that carried it).
	Inserts      int64      `json:"inserts,omitempty"`
	Updates      int64      `json:"updates,omitempty"`
	Deletes      int64      `json:"deletes,omitempty"`
	LastChangeAt *time.Time `json:"last_change_at,omitempty"`
	LastSourceAt *time.Time `json:"last_source_at,omitempty"`

	// Quiet is set by the Web console (never by the pipeline): the table used to change regularly and has had
	// no change for unusually long (internal/web/table_history.go).
	Quiet bool `json:"quiet,omitempty"`
}

// Changes is the number of CDC row changes applied (inserts + updates + deletes).
func (t TableInfo) Changes() int64 { return t.Inserts + t.Updates + t.Deletes }

// VerificationEvidence records the scope of a historical post-seed check.
type VerificationEvidence struct {
	Reason        string              `json:"reason,omitempty"`
	EndCheckpoint string              `json:"end_checkpoint,omitempty"`
	Status        string              `json:"status"`
	Method        string              `json:"method"`
	CheckedAt     time.Time           `json:"checked_at"`
	Tables        []VerificationTable `json:"tables"`
}

type VerificationTable struct {
	Table    string `json:"table"`
	Status   string `json:"status"`
	Method   string `json:"method,omitempty"`
	Position string `json:"position,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// TopInfo chứa thông tin hiển thị của lệnh `strand top` theo spec/02 §10.
type TopInfo struct {
	CDCState     string                `json:"cdc_state,omitempty"`
	Verification *VerificationEvidence `json:"verification,omitempty"`
	PipelineID   string                `json:"pipeline_id"`
	State        string                `json:"state"`
	LagSeconds   float64               `json:"lag_seconds"`
	Throughput   float64               `json:"throughput"` // rows/sec hoặc MB/s
	Zone         string                `json:"zone"`       // Zone GRACE hoặc onoff (e.g. green, yellow, red, breaker)
	Progress     float64               `json:"progress"`   // 0.0 - 100.0 %
	ETASeconds   float64               `json:"eta_seconds"`

	// Bootstrap (snapshot logic): bảng đang chạy, số bảng xong/tổng, row đã ghi / ước lượng, tốc độ.
	// SnapshotTable: các bảng đang snapshot, nối bằng ", " (giữ cho client cũ); SnapshotTables: danh sách
	// (bootstrap.logical.max_parallel > 1 có thể nhiều bảng cùng chạy).
	SnapshotTable     string   `json:"snapshot_table,omitempty"`
	SnapshotTables    []string `json:"snapshot_tables,omitempty"`
	TablesDone        int64    `json:"tables_done,omitempty"`
	TablesTotal       int64    `json:"tables_total,omitempty"`
	SnapshotRows      int64    `json:"snapshot_rows,omitempty"`
	SnapshotRowsTotal int64    `json:"snapshot_rows_total,omitempty"` // ước lượng từ thống kê của source
	SnapshotRate      float64  `json:"snapshot_rate,omitempty"`       // row/giây (~60 s gần nhất)
	BreakerOpen       bool     `json:"breaker_open,omitempty"`
	BreakerReason     string   `json:"breaker_reason,omitempty"`
	// SlotWAL: WAL mà replication slot của pipeline (source PG) đang giữ, từ mẫu governor gần nhất.
	SlotWAL *SlotWAL `json:"slot_wal,omitempty"`
}

// SlotWAL là WAL mà replication slot của source PostgreSQL đang giữ.
type SlotWAL struct {
	RetainedBytes int64  `json:"retained_bytes"`
	KeepBytes     int64  `json:"keep_bytes"` // max_slot_wal_keep_size; -1 = không giới hạn
	Status        string `json:"wal_status,omitempty"`
}

// SlotWALIdleWarnBytes: không có giới hạn mà slot giữ từ chừng này WAL trở lên thì cảnh báo.
const SlotWALIdleWarnBytes = 1 << 30

// Warning trả câu cảnh báo khi WAL slot giữ đáng lo (rỗng = ổn): vượt giới hạn (unreserved/lost), từ
// 50% max_slot_wal_keep_size, hoặc ≥ 1 GiB khi không giới hạn (đĩa source có thể đầy).
func (s *SlotWAL) Warning() string {
	if s == nil || s.RetainedBytes < 0 {
		return ""
	}
	mb := func(b int64) string { return fmt.Sprintf("%d MB", b>>20) }
	switch {
	case s.Status == "unreserved" || s.Status == "lost":
		return fmt.Sprintf("the replication slot keeps %s of WAL and is past max_slot_wal_keep_size (wal_status=%s): PostgreSQL invalidates it unless the pipeline catches up, and then the data must be copied again", mb(s.RetainedBytes), s.Status)
	case s.KeepBytes > 0 && s.RetainedBytes*2 >= s.KeepBytes:
		return fmt.Sprintf("the replication slot keeps %s of WAL, %d%% of max_slot_wal_keep_size (%s)", mb(s.RetainedBytes), s.RetainedBytes*100/s.KeepBytes, mb(s.KeepBytes))
	case s.KeepBytes < 0 && s.RetainedBytes >= SlotWALIdleWarnBytes:
		return fmt.Sprintf("the replication slot keeps %s of WAL and max_slot_wal_keep_size = -1 (no limit): if the pipeline stays behind, the source disk fills up", mb(s.RetainedBytes))
	}
	return ""
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
