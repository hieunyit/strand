// Package assemble chịu trách nhiệm lắp ráp các sự kiện thô thành Txn hoàn chỉnh theo spec/02-specs.md §3.
package assemble

import (
	"context"
	"fmt"
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

// UniqueIndex mô tả một unique index (ngoài PK) theo tên cột.
type UniqueIndex struct {
	Name    string
	Columns []string
}

// ForeignKeyMeta mô tả một khóa ngoại: các cột ở bảng con tham chiếu tới bảng cha.
type ForeignKeyMeta struct {
	Columns     []string
	ParentTable model.TableID
}

// KeyMeta là metadata unique key và FK của một bảng, dùng để dựng writeset (spec 02 §6).
type KeyMeta struct {
	Unique      []UniqueIndex
	ForeignKeys []ForeignKeyMeta
}

// WithKeyMetadata cung cấp metadata unique key / FK để Assembler điền RowChange.UniqueKeys
// và RowChange.ForeignKeys (gồm cả giá trị cũ khi UPDATE đổi unique key).
func WithKeyMetadata(meta map[model.TableID]KeyMeta) Option {
	return func(a *Assembler) {
		a.keyMeta = meta
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
	mu       sync.Mutex
	registry *SchemaRegistry
	maxInMem int
	spillDir string

	currentTxn   *model.Txn
	currentSpill *SpillBuffer
	inTxn        bool
	sawBegin     bool // đã thấy QUERY "BEGIN" (Txn DML); chưa thấy mà gặp QUERY khác => DDL
	keyMeta      map[model.TableID]KeyMeta
}

// colIndex trả về vị trí cột theo tên trong schema, -1 nếu không có.
func colIndex(t *TableSchema, name string) int {
	if t == nil {
		return -1
	}
	for i, c := range t.Columns {
		if c == name {
			return i
		}
	}
	return -1
}

func pickValues(t *TableSchema, row []any, cols []string) ([]model.Value, bool) {
	out := make([]model.Value, len(cols))
	for i, c := range cols {
		idx := colIndex(t, c)
		if idx < 0 || idx >= len(row) {
			return nil, false
		}
		if row[idx] == nil {
			return nil, false // NULL không tham gia unique/FK
		}
		out[i] = row[idx]
	}
	return out, true
}

// enrichKeys điền unique key và FK cho row từ metadata (before có thể nil).
func (a *Assembler) enrichKeys(tid model.TableID, t *TableSchema, before, after []any, rc *model.RowChange) {
	meta, ok := a.keyMeta[tid]
	if !ok {
		return
	}
	for _, uk := range meta.Unique {
		var key, old []model.Value
		var okNew, okOld bool
		if after != nil {
			key, okNew = pickValues(t, after, uk.Columns)
		}
		if before != nil {
			old, okOld = pickValues(t, before, uk.Columns)
		}
		switch {
		case okNew && okOld:
			if KeysEqual(key, old) {
				rc.AddUniqueKey(uk.Name, key)
			} else {
				rc.AddUniqueKey(uk.Name, key, old)
			}
		case okNew:
			rc.AddUniqueKey(uk.Name, key)
		case okOld:
			rc.AddUniqueKey(uk.Name, old)
		}
	}
	for _, fk := range meta.ForeignKeys {
		for _, img := range [][]any{after, before} {
			if img == nil {
				continue
			}
			if v, ok := pickValues(t, img, fk.Columns); ok {
				rc.AddForeignKey(fk.ParentTable, v)
			}
		}
	}
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
		if a.inTxn {
			return nil, fmt.Errorf("GTID %s arrived before transaction %s terminated: events lost",
				ev.Pos.GTID, a.currentTxn.SourcePos.GTID)
		}
		a.ensureTxnStarted(ev.Pos)
		a.currentTxn.SourcePos = ev.Pos
		if !ev.Timestamp.IsZero() {
			a.currentTxn.SourceTS = ev.Timestamp.Unix()
		}

	case reader.EventQuery:
		q := strings.ToUpper(strings.TrimSpace(ev.Query))
		switch {
		case q == "BEGIN":
			a.ensureTxnStarted(ev.Pos)
			a.sawBegin = true
		case q == "COMMIT":
			if a.inTxn {
				return a.finishTxn()
			}
		case !a.sawBegin:
			// Txn DDL: GTID + QUERY, không có BEGIN/XID. Phát thành Txn riêng để GTID được ghi nhận.
			a.ensureTxnStarted(ev.Pos)
			a.currentTxn.DDL = ev.Query
			return a.finishTxn()
		}

	case reader.EventTableMap:
		a.ensureTxnStarted(ev.Pos)
		// Schema lấy từ metadata của TABLE_MAP (binlog_row_metadata=FULL) khi có.
		if len(ev.Columns) > 0 {
			a.registry.RegisterTable(ev.Schema, ev.Table, ev.PKColumns, ev.Columns)
		} else if a.registry.GetTable(ev.Schema, ev.Table) == nil {
			a.registry.RegisterTable(ev.Schema, ev.Table, nil, nil)
		}

	case reader.EventRows:
		a.ensureTxnStarted(ev.Pos)
		tableID := model.TableID{Schema: ev.Schema, Table: ev.Table}
		tbl := a.registry.GetTable(ev.Schema, ev.Table)
		if tbl == nil {
			a.registry.RegisterTable(ev.Schema, ev.Table, nil, nil)
			tbl = a.registry.GetTable(ev.Schema, ev.Table)
		}

		switch ev.Op {
		case model.OpInsert:
			for _, row := range ev.Rows {
				key := tbl.ExtractPK(row)
				cols := tbl.ExtractCols(row)
				rc := model.RowChange{
					Table: tableID,
					Op:    model.OpInsert,
					Key:   key,
					Cols:  cols,
				}
				a.enrichKeys(tableID, tbl, nil, row, &rc)
				if err := a.currentSpill.Append(rc); err != nil {
					return nil, err
				}
			}

		case model.OpDelete:
			for _, row := range ev.Rows {
				key := tbl.ExtractPK(row)
				cols := tbl.ExtractCols(row)
				rc := model.RowChange{
					Table: tableID,
					Op:    model.OpDelete,
					Key:   key,
					Cols:  cols,
				}
				a.enrichKeys(tableID, tbl, row, nil, &rc)
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

				oldPK := tbl.ExtractPK(before)
				newPK := tbl.ExtractPK(after)

				var oldKey []model.Value
				if !KeysEqual(oldPK, newPK) {
					// spec: 02 §3 OldKey khác nil khi UPDATE đổi PK
					oldKey = oldPK
				}

				cols := tbl.ExtractCols(after)
				rc := model.RowChange{
					Table:  tableID,
					Op:     model.OpUpdate,
					Key:    newPK,
					OldKey: oldKey,
					Cols:   cols,
				}
				a.enrichKeys(tableID, tbl, before, after, &rc)
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
	a.sawBegin = false
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
