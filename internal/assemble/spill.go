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
	var header [14]byte
	if _, err := io.ReadFull(it.reader, header[:]); err != nil {
		return nil, err // io.EOF hoặc lỗi đọc
	}

	tableIdx := binary.LittleEndian.Uint32(header[0:4])
	opByte := header[4]
	keyVal := int64(binary.LittleEndian.Uint64(header[5:13]))
	hasOldKey := header[13]

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

	var oldKey []model.Value
	if hasOldKey == 1 {
		var oldKeyBuf [8]byte
		if _, err := io.ReadFull(it.reader, oldKeyBuf[:]); err != nil {
			return nil, err
		}
		oldKey = []model.Value{int64(binary.LittleEndian.Uint64(oldKeyBuf[:]))}
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

	inMem   []model.RowChange
	spilled bool
	spillF  *os.File
	writer  *bufio.Writer
	count   int64
	bytes   int64

	tables   []model.TableID
	tableMap map[model.TableID]uint32
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
		tableMap: make(map[model.TableID]uint32),
	}
}

// IsSpilled cho biết buffer đã tràn xuống đĩa chưa.
func (sb *SpillBuffer) IsSpilled() bool {
	return sb.spilled
}

func (sb *SpillBuffer) getTableIdx(t model.TableID) uint32 {
	if idx, exists := sb.tableMap[t]; exists {
		return idx
	}
	idx := uint32(len(sb.tables))
	sb.tables = append(sb.tables, t)
	sb.tableMap[t] = idx
	return idx
}

func extractInt64Val(val any) int64 {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case uint64:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	case int16:
		return int64(v)
	case uint16:
		return int64(v)
	case int8:
		return int64(v)
	case uint8:
		return int64(v)
	default:
		return 0
	}
}

func (sb *SpillBuffer) writeRowDirect(tIdx uint32, opByte byte, keyVal int64, hasOldKey byte, oldKeyVal int64) error {
	var buf [22]byte
	binary.LittleEndian.PutUint32(buf[0:4], tIdx)
	buf[4] = opByte
	binary.LittleEndian.PutUint64(buf[5:13], uint64(keyVal))
	buf[13] = hasOldKey
	n := 14
	if hasOldKey == 1 {
		binary.LittleEndian.PutUint64(buf[14:22], uint64(oldKeyVal))
		n = 22
	}
	if _, err := sb.writer.Write(buf[:n]); err != nil {
		return err
	}
	sb.bytes += int64(n)
	return nil
}

func (sb *SpillBuffer) writeRowToFile(rc *model.RowChange) error {
	tIdx := sb.getTableIdx(rc.Table)
	var opByte byte = 1
	if rc.Op == model.OpUpdate {
		opByte = 2
	} else if rc.Op == model.OpDelete {
		opByte = 3
	}

	var keyVal int64
	if len(rc.Key) > 0 {
		keyVal = extractInt64Val(rc.Key[0])
	}

	var hasOldKey byte = 0
	var oldKeyVal int64
	if len(rc.OldKey) > 0 {
		hasOldKey = 1
		oldKeyVal = extractInt64Val(rc.OldKey[0])
	}

	return sb.writeRowDirect(tIdx, opByte, keyVal, hasOldKey, oldKeyVal)
}

// AppendFast ghi trực tiếp một dòng vào đĩa khi buffer đã tràn.
func (sb *SpillBuffer) AppendFast(tIdx uint32, opByte byte, keyVal int64, hasOldKey byte, oldKeyVal int64) error {
	sb.count++
	return sb.writeRowDirect(tIdx, opByte, keyVal, hasOldKey, oldKeyVal)
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
	sb.writer = bufio.NewWriterSize(f, 256*1024)
	sb.spilled = true

	for i := range sb.inMem {
		if err := sb.writeRowToFile(&sb.inMem[i]); err != nil {
			return err
		}
	}
	sb.inMem = nil // Giải phóng RAM ngay lập tức
	return nil
}

func (sb *SpillBuffer) Append(rc model.RowChange) error {
	sb.count++

	if !sb.spilled {
		if len(sb.inMem) < sb.maxInMem {
			sb.bytes += 64
			sb.inMem = append(sb.inMem, rc)
			return nil
		}

		if err := sb.spill(); err != nil {
			return err
		}
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
		reader: bufio.NewReaderSize(f, 256*1024),
		tables: sb.tables,
	}, nil
}
