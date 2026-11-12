package checkpoint

import (
	"context"
	"fmt"
	"time"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// ErrPipelineLocked trả về khi có một instance khác đang giữ khóa pipeline.
// spec: 02 §4, §12
var ErrPipelineLocked = exitcode.New(exitcode.ErrPipelineLocked, exitcode.Fenced, "pipeline is locked by another instance", nil)

// Global đại diện cho thông tin checkpoint toàn cục trong _strand_checkpoint.
// spec: 02 §3, §4
type Global struct {
	PipelineID string
	Epoch      int64
	Applied    string
	UpdatedAt  time.Time
}

// CheckpointStore là interface quản lý metadata, checkpoint và khóa pipeline theo spec/02 §3, §4.
type CheckpointStore interface {
	// EnsureTables tạo đủ 3 bảng metadata (_strand_pipeline, _strand_checkpoint, _strand_worker_delta).
	EnsureTables(ctx context.Context) error

	// Lock lấy khóa advisory lock cho pipeline. Trả về hàm release để giải phóng khóa.
	// Nếu instance khác đang giữ khóa, trả về ErrPipelineLocked (exit code 4).
	Lock(ctx context.Context, pipelineID string) (release func() error, err error)

	// AdvanceEpoch tăng epoch lên 1 trong một transaction, cập nhật cho _strand_checkpoint và row delta.
	// spec: 02 §4 quy tắc 2
	AdvanceEpoch(ctx context.Context, pipelineID, owner, configHash string) (int64, error)

	// Epoch trả về epoch hiện tại của instance.
	Epoch() int64

	// LoadApplied nạp AppliedSet gộp từ row toàn cục và mọi row delta.
	LoadApplied(ctx context.Context) (AppliedSet, error)

	// WriteGlobal ghi lại checkpoint toàn cục có kiểm tra điều kiện epoch.
	WriteGlobal(ctx context.Context, g Global) error

	// Close đóng kết nối store.
	Close() error
}

// ParseAppliedSet phân tích chuỗi applied thành AppliedSet tương ứng với engine.
// Chuỗi hỏng luôn trả lỗi: một checkpoint đọc sai im lặng có thể làm mất (CP-1)
// hoặc apply lại (CP-2) dữ liệu.
func ParseAppliedSet(engine model.Engine, appliedStr string) (AppliedSet, error) {
	switch engine {
	case model.EngineMySQL:
		// Chế độ vị trí binlog (source không GTID): JSON {"mode":"binlog_pos",...}. GTID set không bao giờ
		// bắt đầu bằng "{", nên nhận dạng theo chuỗi không mơ hồ; chuỗi rỗng vẫn là GTID set rỗng và được
		// MergeChecked nhường chỗ cho tập vị trí.
		if IsBinlogPosApplied(appliedStr) {
			set, err := ParseBinlogPosApplied(appliedStr)
			if err != nil {
				return nil, fmt.Errorf("parse MySQL binlog position applied set: %w", err)
			}
			return set, nil
		}
		set := NewMySQLAppliedSet()
		if err := set.AddGTIDSet(appliedStr); err != nil {
			return nil, fmt.Errorf("parse MySQL applied set: %w", err)
		}
		return set, nil
	case model.EnginePG:
		return parsePGApplied(appliedStr)
	default:
		return nil, fmt.Errorf("unknown source engine %q", engine)
	}
}

func splitGTIDSet(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' || s[i] == '\n' {
			trimmed := trimSpace(s[start:i])
			if trimmed != "" {
				parts = append(parts, trimmed)
			}
			start = i + 1
		}
	}
	if start < len(s) {
		trimmed := trimSpace(s[start:])
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func trimSpace(s string) string {
	start := 0
	for start < len(s) && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n') {
		start++
	}
	end := len(s)
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\r' || s[end-1] == '\n') {
		end--
	}
	return s[start:end]
}
