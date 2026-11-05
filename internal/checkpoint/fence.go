package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"strand/internal/exitcode"
)

// Fence là epoch của instance đang giữ pipeline (invariant: SG-1). Transaction ghi dữ liệu không đi qua
// writer stream (snapshot flush, xóa phần chưa đọc, clear bảng) kiểm fence TRONG transaction (review R07):
// đọc _strand_pipeline.epoch với khóa chia sẻ. AdvanceEpoch của instance mới khóa ghi đúng row đó
// (SELECT … FOR UPDATE) nên phải chờ các transaction snapshot đang mở commit xong; sau đó mọi transaction
// của instance cũ thấy epoch mới và rollback. Stream writer không đụng row này nên không tranh khóa.
type Fence struct {
	PipelineID string
	Epoch      int64
}

type fenceKey struct{}

// WithFence gắn fence vào ctx; các hàm ghi snapshot dùng ctx đó sẽ kiểm epoch trong transaction.
func WithFence(ctx context.Context, f Fence) context.Context {
	return context.WithValue(ctx, fenceKey{}, f)
}

// FenceFrom trả fence gắn trong ctx (ok=false nếu không có: test hoặc công cụ chạy độc lập).
func FenceFrom(ctx context.Context) (Fence, bool) {
	f, ok := ctx.Value(fenceKey{}).(Fence)
	return f, ok
}

// Querier là phần chung của *sql.Tx và *sql.Conn dùng để kiểm fence.
type Querier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// CheckFence kiểm (trong transaction q) rằng epoch hiện tại của pipeline vẫn là của instance này.
// Không có fence trong ctx thì không kiểm. pg chọn cú pháp placeholder.
func CheckFence(ctx context.Context, q Querier, pg bool) error {
	f, ok := FenceFrom(ctx)
	if !ok {
		return nil
	}
	query := "SELECT epoch FROM _strand_pipeline WHERE pipeline_id = ? FOR SHARE"
	if pg {
		query = "SELECT epoch FROM _strand_pipeline WHERE pipeline_id = $1 FOR SHARE"
	}
	var cur int64
	err := q.QueryRowContext(ctx, query, f.PipelineID).Scan(&cur)
	if errors.Is(err, sql.ErrNoRows) {
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("pipeline %s has no metadata row: fenced", f.PipelineID), nil)
	}
	if err != nil {
		return fmt.Errorf("check pipeline epoch: %w", err)
	}
	if cur != f.Epoch {
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced,
			fmt.Sprintf("snapshot write fenced: pipeline %s epoch is %d, this instance holds %d", f.PipelineID, cur, f.Epoch), nil)
	}
	return nil
}
