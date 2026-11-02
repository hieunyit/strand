package assemble

import (
	"fmt"
	"io"
	"os"

	"strand/internal/model"
)

// RowIterator cho phép đọc tuần tự các dòng thay đổi trong transaction (kể cả khi đã tràn đĩa).
type RowIterator interface {
	Next() (*model.RowChange, error)
	Close() error
}

// SliceRowIterator duyệt trên mảng trong bộ nhớ.
type SliceRowIterator struct {
	rows []model.RowChange
	idx  int
}

func NewSliceRowIterator(rows []model.RowChange) *SliceRowIterator {
	return &SliceRowIterator{rows: rows}
}

func (it *SliceRowIterator) Next() (*model.RowChange, error) {
	if it.idx >= len(it.rows) {
		return nil, io.EOF
	}
	r := &it.rows[it.idx]
	it.idx++
	return r, nil
}

func (it *SliceRowIterator) Close() error {
	it.rows = nil
	return nil
}

// DiskRowIterator đọc tuần tự các RowChange đã tràn ra file (định dạng model.SpillWriter, không mất dữ liệu).
type DiskRowIterator struct {
	r *model.SpillReader
}

func (it *DiskRowIterator) Next() (*model.RowChange, error) { return it.r.Next() }
func (it *DiskRowIterator) Close() error                    { return it.r.Close() }

// SpillBuffer quản lý việc đệm RowChange cho một transaction, tự động tràn đĩa khi vượt ngưỡng RAM.
// spec: design §6.4
type SpillBuffer struct {
	maxInMem int
	dir      string

	inMem   []model.RowChange
	spilled bool
	spillF  *os.File
	writer  *model.SpillWriter
	count   int64
	bytes   int64

	tables   []model.TableID
	tableSet map[model.TableID]struct{}
}

func NewSpillBuffer(maxInMem int, dir string) *SpillBuffer {
	if maxInMem <= 0 {
		maxInMem = 10000 // Mặc định 10.000 dòng trong RAM
	}
	if dir == "" {
		dir = os.TempDir()
	}

	return &SpillBuffer{
		maxInMem: maxInMem,
		dir:      dir,
		inMem:    make([]model.RowChange, 0, 1024),
		tableSet: make(map[model.TableID]struct{}),
	}
}

// IsSpilled cho biết buffer đã tràn xuống đĩa chưa.
func (sb *SpillBuffer) IsSpilled() bool {
	return sb.spilled
}

// noteTables ghi nhận bảng của row và bảng cha qua FK, để scheduler dựng phụ thuộc cấp bảng.
func (sb *SpillBuffer) noteTables(rc *model.RowChange) {
	add := func(t model.TableID) {
		if _, ok := sb.tableSet[t]; !ok {
			sb.tableSet[t] = struct{}{}
			sb.tables = append(sb.tables, t)
		}
	}
	add(rc.Table)
	for _, fk := range rc.ForeignKeys {
		add(fk.ParentTable)
	}
}

func estimateRowBytes(rc *model.RowChange) int64 {
	n := int64(32)
	for _, c := range rc.Cols {
		n += int64(len(c.Name)) + 16
		switch v := c.Value.(type) {
		case string:
			n += int64(len(v))
		case []byte:
			n += int64(len(v))
		}
	}
	return n
}

func (sb *SpillBuffer) spill() error {
	if sb.spilled {
		return nil
	}

	f, err := os.CreateTemp(sb.dir, "strand-txn-*.spill")
	if err != nil {
		return fmt.Errorf("failed to create spill file: %w", err)
	}
	sb.spillF = f
	sb.writer = model.NewSpillWriter(f)
	sb.spilled = true

	for i := range sb.inMem {
		if err := sb.writer.Write(&sb.inMem[i]); err != nil {
			return fmt.Errorf("failed to write spill row: %w", err)
		}
	}
	sb.inMem = nil // Giải phóng RAM ngay lập tức
	return nil
}

func (sb *SpillBuffer) Append(rc model.RowChange) error {
	sb.count++
	sb.bytes += estimateRowBytes(&rc)
	sb.noteTables(&rc)

	if !sb.spilled {
		if len(sb.inMem) < sb.maxInMem {
			sb.inMem = append(sb.inMem, rc)
			return nil
		}
		if err := sb.spill(); err != nil {
			return err
		}
	}

	if err := sb.writer.Write(&rc); err != nil {
		return fmt.Errorf("failed to write spill row: %w", err)
	}
	return nil
}

func (sb *SpillBuffer) Finish(txn *model.Txn) error {
	txn.RowCount = sb.count
	txn.Bytes = sb.bytes

	if sb.spilled {
		if err := sb.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush spill buffer: %w", err)
		}
		if err := sb.spillF.Sync(); err != nil {
			return fmt.Errorf("failed to sync spill file: %w", err)
		}
		txn.SpillPath = sb.spillF.Name()
		txn.SpilledTables = append([]model.TableID(nil), sb.tables...)
		txn.Rows = nil // Đảm bảo RAM không giữ bất kỳ row nào
		if err := sb.spillF.Close(); err != nil {
			return fmt.Errorf("failed to close spill file: %w", err)
		}
	} else {
		txn.Rows = sb.inMem
	}

	return nil
}

// OpenIterator mở RowIterator cho một transaction (từ RAM hoặc từ đĩa).
func (sb *SpillBuffer) OpenIterator(txn *model.Txn) (RowIterator, error) {
	return OpenRowIterator(txn)
}

// OpenRowIterator mở RowIterator cho một transaction bất kỳ (không cần SpillBuffer gốc).
func OpenRowIterator(txn *model.Txn) (RowIterator, error) {
	if !txn.IsSpilled() {
		return NewSliceRowIterator(txn.Rows), nil
	}
	r, err := model.OpenSpillReader(txn.SpillPath)
	if err != nil {
		return nil, err
	}
	return &DiskRowIterator{r: r}, nil
}
