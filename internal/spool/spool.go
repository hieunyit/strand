// Package spool cài đặt hàng đợi log lưu trữ giao dịch trên đĩa sử dụng Pebble theo spec/02-specs.md §3 và spec/design.md §1.
package spool

import (
	"context"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// SpoolUsage chứa thống kê dung lượng byte theo từng tầng của Spool.
// spec: 02 §3, 11
type SpoolUsage struct {
	LocalBytes  int64 `json:"local_bytes"`
	BucketBytes int64 `json:"bucket_bytes"`
}

// SpoolIter là iterator để duyệt qua các Txn trong spool theo thứ tự.
type SpoolIter interface {
	Next(ctx context.Context) (*model.Txn, error)
	Close() error
}

// Spool là interface hàng đợi log trên đĩa cho transaction theo spec/02-specs.md §3.
type Spool interface {
	// Append có thể nhận quyền giữ SpillPath từ caller; sau khi gọi, caller phải
	// kiểm SpillOwned trước khi dọn, kể cả khi trả lỗi sau lúc đã buffer segment.
	// Caller không dùng đồng thời Txn này ở nơi khác trong lúc Append chạy.
	Append(ctx context.Context, t *model.Txn) error
	Flush(ctx context.Context) error
	Iter(ctx context.Context, from model.Pos) (SpoolIter, error)
	Truncate(ctx context.Context, applied checkpoint.AppliedSet) error
	Usage() SpoolUsage // số byte theo từng tầng
	Close() error
}
