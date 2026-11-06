package exitcode

import (
	"errors"
	"fmt"
)

// Bảng exit code quy định tại spec/02-specs.md §10
const (
	Success       = 0  // Thành công
	Unclassified  = 1  // Lỗi không phân loại
	InvalidSyntax = 2  // Sai cú pháp hoặc cấu hình không hợp lệ
	Rejected      = 3  // validate hoặc plan từ chối
	Fenced        = 4  // pipeline_locked hoặc fenced
	DataError     = 5  // Lỗi dữ liệu hoặc log: log_gap, prefetch_exhausted, verify_mismatch
	NeedsDecision = 10 // Cần người vận hành quyết định, ví dụ promote khi source không truy cập được
)

// Các mã lỗi chuẩn snake_case quy định tại spec/02-specs.md §12
const (
	// Exit 3: Lỗi validate & seed plan
	ErrBinlogConfigInvalid   = "binlog_config_invalid"
	ErrMissingPK             = "missing_pk"
	ErrCollationLooser       = "collation_looser"
	ErrTypeUnmappable        = "type_unmappable"
	ErrInsufficientPrivilege = "insufficient_privilege"
	ErrSeedInvariantFailed   = "seed_invariant_failed"
	ErrPrereleaseTool        = "prerelease_tool"
	// ErrTargetNotTransactional: bảng đích hoặc bảng _strand_* trên target MySQL không dùng InnoDB,
	// nên ghi dữ liệu và checkpoint không nằm chung một transaction (review R26).
	ErrTargetNotTransactional = "target_not_transactional"
	// ErrTableTargetChanged: tables.rename đổi bảng đích của bảng đã bootstrap xong (review N14).
	ErrTableTargetChanged = "table_target_changed"

	// Exit 4: Tranh chấp khóa pipeline / bị fence
	ErrPipelineLocked = "pipeline_locked"
	ErrFenced         = "fenced"

	// Exit 5: Lỗi dữ liệu hoặc log
	ErrLogGap            = "log_gap"
	ErrPrefetchExhausted = "prefetch_exhausted"
	ErrVerifyMismatch    = "verify_mismatch"
	// ErrVerifyIncomplete: verify delta chỉ kiểm được một phần tập thay đổi vì có bảng vượt giới hạn ghi
	// khóa — không được báo sạch (review N12).
	ErrVerifyIncomplete = "verify_incomplete"

	// Exit 10: Dừng chờ người vận hành
	ErrPromoteNeedsDecision = "promote_needs_decision"
	ErrSchemaChangePending  = "schema_change_pending"
)

// Error mang mã lỗi spec và exit code tương ứng
type Error struct {
	Code     string
	ExitCode int
	Message  string
	Err      error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error {
	return e.Err
}

// New tạo Error mới với mã lỗi và exit code
func New(code string, exitCode int, message string, err error) *Error {
	return &Error{
		Code:     code,
		ExitCode: exitCode,
		Message:  message,
		Err:      err,
	}
}

// GetExitCode trích xuất exit code từ error. Nếu không phân loại được thì trả về 1 (Unclassified).
func GetExitCode(err error) int {
	if err == nil {
		return Success
	}
	var se *Error
	if errors.As(err, &se) {
		return se.ExitCode
	}
	return Unclassified
}
