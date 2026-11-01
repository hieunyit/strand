// Package spool cài đặt hàng đợi log lưu trữ giao dịch trên đĩa sử dụng Pebble theo spec/02-specs.md §3 và spec/design.md §1.
package spool

import (
	"context"
	"io"

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
	Append(ctx context.Context, t *model.Txn) error
	Iter(ctx context.Context, from model.Pos) (SpoolIter, error)
	Truncate(ctx context.Context, applied checkpoint.AppliedSet) error
	Usage() SpoolUsage // số byte theo từng tầng
	Close() error
}

// SliceSpoolIter duyệt trên mảng trong bộ nhớ (dùng cho test hoặc fallback).
type SliceSpoolIter struct {
	txns []*model.Txn
	idx  int
}

func NewSliceSpoolIter(txns []*model.Txn) *SliceSpoolIter {
	return &SliceSpoolIter{txns: txns}
}

func (it *SliceSpoolIter) Next(ctx context.Context) (*model.Txn, error) {
	if it.idx >= len(it.txns) {
		return nil, io.EOF
	}
	t := it.txns[it.idx]
	it.idx++
	return t, nil
}

func (it *SliceSpoolIter) Close() error {
	it.txns = nil
	return nil
}
