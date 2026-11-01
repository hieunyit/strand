// Package assemble chịu trách nhiệm lắp ráp các sự kiện thô thành Txn hoàn chỉnh theo spec/02-specs.md §3.
package assemble

import (
	"context"
	"strings"
	"sync"

	"strand/internal/model"
	"strand/internal/reader"
)

// Option cấu hình Assembler.
type Option func(*Assembler)

// WithMaxInMemRows đặt giới hạn số dòng giữ trong RAM trước khi tràn đĩa.
func WithMaxInMemRows(n int) Option {
	return func(a *Assembler) {
		a.maxInMem = n
	}
}

// WithSpillDir đặt thư mục lưu file tràn đĩa.
func WithSpillDir(dir string) Option {
	return func(a *Assembler) {
		a.spillDir = dir
	}
}

// WithSchemaRegistry đặt SchemaRegistry tùy biến.
func WithSchemaRegistry(reg *SchemaRegistry) Option {
	return func(a *Assembler) {
		a.registry = reg
	}
}

// Assembler lắp ráp các sự kiện từ reader thành các Txn hoàn chỉnh.
// spec: 02 §3, D §1
type Assembler struct {
	mu           sync.Mutex
	registry     *SchemaRegistry
	maxInMem     int
	spillDir     string

	currentTxn   *model.Txn
	currentSpill *SpillBuffer
	inTxn        bool
}

// New khởi tạo Assembler mới.
func New(opts ...Option) *Assembler {
	a := &Assembler{
		registry: NewSchemaRegistry(),
		maxInMem: 10000,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Registry trả về SchemaRegistry của Assembler.
func (a *Assembler) Registry() *SchemaRegistry {
	return a.registry
}

func (a *Assembler) ensureTxnStarted(pos model.Pos) {
	if !a.inTxn {
		a.inTxn = true
		a.currentTxn = &model.Txn{
			SourcePos: pos,
		}
		a.currentSpill = NewSpillBuffer(a.maxInMem, a.spillDir)
	}
}

// Push nhận một reader.Event và trả về *model.Txn nếu transaction hoàn tất (hoặc nil nếu chưa hoàn tất).
func (a *Assembler) Push(ev reader.Event) (*model.Txn, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch ev.Type {
	case reader.EventGTID:
		a.ensureTxnStarted(ev.Pos)
		a.currentTxn.SourcePos = ev.Pos

	case reader.EventQuery:
		q := strings.ToUpper(strings.TrimSpace(ev.Query))
		if q == "BEGIN" {
			a.ensureTxnStarted(ev.Pos)
		} else if q == "COMMIT" {
			if a.inTxn {
				return a.finishTxn()
			}
		}

	case reader.EventTableMap:
		a.ensureTxnStarted(ev.Pos)
		// Đảm bảo bảng có trong registry
		if a.registry.GetTable(ev.Schema, ev.Table) == nil {
			a.registry.RegisterTable(ev.Schema, ev.Table, nil, nil)
		}

	case reader.EventRows:
		a.ensureTxnStarted(ev.Pos)
		tableID := model.TableID{Schema: ev.Schema, Table: ev.Table}

		switch ev.Op {
		case model.OpInsert:
			for _, row := range ev.Rows {
				key := a.registry.ExtractPK(ev.Schema, ev.Table, row)
				cols := a.registry.ExtractCols(ev.Schema, ev.Table, row)
				rc := model.RowChange{
					Table: tableID,
					Op:    model.OpInsert,
					Key:   key,
					Cols:  cols,
				}
				if err := a.currentSpill.Append(rc); err != nil {
					return nil, err
				}
			}

		case model.OpDelete:
			for _, row := range ev.Rows {
				key := a.registry.ExtractPK(ev.Schema, ev.Table, row)
				cols := a.registry.ExtractCols(ev.Schema, ev.Table, row)
				rc := model.RowChange{
					Table: tableID,
					Op:    model.OpDelete,
					Key:   key,
					Cols:  cols,
				}
				if err := a.currentSpill.Append(rc); err != nil {
					return nil, err
				}
			}

		case model.OpUpdate:
			// Các hàng update trong binlog đi theo cặp: [Before, After, Before, After, ...]
			for i := 0; i < len(ev.Rows); i += 2 {
				before := ev.Rows[i]
				var after []any
				if i+1 < len(ev.Rows) {
					after = ev.Rows[i+1]
				} else {
					after = before
				}

				oldPK := a.registry.ExtractPK(ev.Schema, ev.Table, before)
				newPK := a.registry.ExtractPK(ev.Schema, ev.Table, after)

				var oldKey []model.Value
				if !KeysEqual(oldPK, newPK) {
					// spec: 02 §3 OldKey khác nil khi UPDATE đổi PK
					oldKey = oldPK
				}

				cols := a.registry.ExtractCols(ev.Schema, ev.Table, after)
				rc := model.RowChange{
					Table:  tableID,
					Op:     model.OpUpdate,
					Key:    newPK,
					OldKey: oldKey,
					Cols:   cols,
				}
				if err := a.currentSpill.Append(rc); err != nil {
					return nil, err
				}
			}
		}

	case reader.EventXID:
		if a.inTxn {
			return a.finishTxn()
		}
	}

	return nil, nil
}

func (a *Assembler) finishTxn() (*model.Txn, error) {
	if err := a.currentSpill.Finish(a.currentTxn); err != nil {
		return nil, err
	}
	res := a.currentTxn

	a.inTxn = false
	a.currentTxn = nil
	a.currentSpill = nil

	return res, nil
}

// Process nhận stream reader.Event qua channel và xuất ra stream *model.Txn qua channel.
func (a *Assembler) Process(ctx context.Context, in <-chan reader.Event) (<-chan *model.Txn, <-chan error) {
	out := make(chan *model.Txn, 128)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-in:
				if !ok {
					return
				}
				if ev.Type == reader.EventError {
					select {
					case errCh <- ev.Err:
					case <-ctx.Done():
					}
					return
				}

				txn, err := a.Push(ev)
				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
					}
					return
				}

				if txn != nil {
					select {
					case out <- txn:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out, errCh
}
