package assemble

import (
	"bufio"
	"encoding/binary"
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

// DiskRowIterator đọc tuần tự các RowChange đã được tuần tự hóa ra file tràn trên đĩa.
type DiskRowIterator struct {
	file   *os.File
	reader *bufio.Reader
	tables []model.TableID
}

func (it *DiskRowIterator) Next() (*model.RowChange, error) {
	var tableIdx uint32
	if err := binary.Read(it.reader, binary.LittleEndian, &tableIdx); err != nil {
		return nil, err // io.EOF hoặc lỗi đọc
	}

	var opByte byte
	if err := binary.Read(it.reader, binary.LittleEndian, &opByte); err != nil {
		return nil, err
	}

	var op model.Op
	switch opByte {
	case 1:
		op = model.OpInsert
	case 2:
		op = model.OpUpdate
	case 3:
		op = model.OpDelete
	}

	table := model.TableID{}
	if int(tableIdx) < len(it.tables) {
		table = it.tables[tableIdx]
	}

	var keyVal int64
	if err := binary.Read(it.reader, binary.LittleEndian, &keyVal); err != nil {
		return nil, err
	}

	var hasOldKey byte
	if err := binary.Read(it.reader, binary.LittleEndian, &hasOldKey); err != nil {
		return nil, err
	}

	var oldKey []model.Value
	if hasOldKey == 1 {
		var oldKeyVal int64
		if err := binary.Read(it.reader, binary.LittleEndian, &oldKeyVal); err != nil {
			return nil, err
		}
		oldKey = []model.Value{oldKeyVal}
	}

	rc := &model.RowChange{
		Table:  table,
		Op:     op,
		Key:    []model.Value{keyVal},
		OldKey: oldKey,
	}

	return rc, nil
}

func (it *DiskRowIterator) Close() error {
	if it.file != nil {
		return it.file.Close()
	}
	return nil
}

// SpillBuffer quản lý việc đệm RowChange cho một transaction, tự động tràn đĩa khi vượt ngưỡng RAM.
// spec: design §6.4
type SpillBuffer struct {
	maxInMem int
	dir      string

	inMem    []model.RowChange
	spilled  bool
	spillF   *os.File
	writer   *bufio.Writer
	count    int64
	bytes    int64

	tables    []model.TableID
	tableMap  map[string]uint32
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
		tableMap: make(map[string]uint32),
	}
}

func (sb *SpillBuffer) getTableIdx(t model.TableID) uint32 {
	key := fmt.Sprintf("%s.%s", t.Schema, t.Table)
	if idx, exists := sb.tableMap[key]; exists {
		return idx
	}
	idx := uint32(len(sb.tables))
	sb.tables = append(sb.tables, t)
	sb.tableMap[key] = idx
	return idx
}

func (sb *SpillBuffer) writeRowToFile(rc *model.RowChange) error {
	tIdx := sb.getTableIdx(rc.Table)
	if err := binary.Write(sb.writer, binary.LittleEndian, tIdx); err != nil {
		return err
	}

	var opByte byte = 1
	if rc.Op == model.OpUpdate {
		opByte = 2
	} else if rc.Op == model.OpDelete {
		opByte = 3
	}
	if err := binary.Write(sb.writer, binary.LittleEndian, opByte); err != nil {
		return err
	}

	var keyVal int64
	if len(rc.Key) > 0 {
		if v, ok := rc.Key[0].(int); ok {
			keyVal = int64(v)
		} else if v, ok := rc.Key[0].(int64); ok {
			keyVal = v
		} else if v, ok := rc.Key[0].(uint64); ok {
			keyVal = int64(v)
		}
	}
	if err := binary.Write(sb.writer, binary.LittleEndian, keyVal); err != nil {
		return err
	}

	if len(rc.OldKey) > 0 {
		if err := binary.Write(sb.writer, binary.LittleEndian, byte(1)); err != nil {
			return err
		}
		var oldKeyVal int64
		if v, ok := rc.OldKey[0].(int); ok {
			oldKeyVal = int64(v)
		} else if v, ok := rc.OldKey[0].(int64); ok {
			oldKeyVal = v
		} else if v, ok := rc.OldKey[0].(uint64); ok {
			oldKeyVal = int64(v)
		}
		if err := binary.Write(sb.writer, binary.LittleEndian, oldKeyVal); err != nil {
			return err
		}
	} else {
		if err := binary.Write(sb.writer, binary.LittleEndian, byte(0)); err != nil {
			return err
		}
	}

	sb.bytes += 18 // ước lượng kích thước byte
	return nil
}

func (sb *SpillBuffer) Append(rc model.RowChange) error {
	sb.count++
	sb.bytes += 64

	if !sb.spilled {
		if len(sb.inMem) < sb.maxInMem {
			sb.inMem = append(sb.inMem, rc)
			return nil
		}

		// Vượt ngưỡng RAM: Khởi tạo file tràn đĩa và flush toàn bộ inMem
		f, err := os.CreateTemp(sb.dir, "strand-txn-*.spill")
		if err != nil {
			return fmt.Errorf("failed to create spill file: %w", err)
		}
		sb.spillF = f
		sb.writer = bufio.NewWriterSize(f, 64*1024)
		sb.spilled = true

		for i := range sb.inMem {
			if err := sb.writeRowToFile(&sb.inMem[i]); err != nil {
				return err
			}
		}
		sb.inMem = nil // Giải phóng RAM ngay lập tức
	}

	return sb.writeRowToFile(&rc)
}

func (sb *SpillBuffer) Finish(txn *model.Txn) error {
	txn.RowCount = sb.count
	txn.Bytes = sb.bytes

	if sb.spilled {
		if err := sb.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush spill buffer: %w", err)
		}
		txn.SpillPath = sb.spillF.Name()
		txn.Rows = nil // Đảm bảo RAM không giữ bất kỳ row nào
		_ = sb.spillF.Close()
	} else {
		txn.Rows = sb.inMem
	}

	return nil
}

// OpenIterator mở RowIterator cho một transaction (từ RAM hoặc từ đĩa).
func (sb *SpillBuffer) OpenIterator(txn *model.Txn) (RowIterator, error) {
	if !txn.IsSpilled() {
		return NewSliceRowIterator(txn.Rows), nil
	}

	f, err := os.Open(txn.SpillPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open spill file %s: %w", txn.SpillPath, err)
	}

	return &DiskRowIterator{
		file:   f,
		reader: bufio.NewReaderSize(f, 64*1024),
		tables: sb.tables,
	}, nil
}
