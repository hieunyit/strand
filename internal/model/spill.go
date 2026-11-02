package model

// Spill file codec for Txn too large to keep in RAM (design §6.4).
// The file is self-describing (table and column-name dictionaries are written
// inline) and lossless: every RowChange field, including all column values, is
// preserved. Unknown value types are rejected instead of being silently converted.

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

const (
	recTable byte = 'T'
	recName  byte = 'N'
	recRow   byte = 'R'
)

const (
	valNil byte = iota
	valInt
	valUint
	valString
	valBytes
	valFloat64
	valFloat32
	valBool
	valTime
	valGoInt // kiểu Go `int` (chỉ khi PreserveGoInt)
)

// SpillWriter ghi tuần tự các RowChange ra file tràn.
type SpillWriter struct {
	// PreserveGoInt giữ kiểu Go `int` khi đọc lại (mặc định int → int64).
	PreserveGoInt bool
	w             *bufio.Writer
	tables        map[TableID]uint32
	names         map[string]uint32
	buf           []byte
}

// NewSpillWriter tạo SpillWriter ghi vào w.
func NewSpillWriter(w io.Writer) *SpillWriter {
	return &SpillWriter{
		w:      bufio.NewWriterSize(w, 256*1024),
		tables: make(map[TableID]uint32),
		names:  make(map[string]uint32),
		buf:    make([]byte, 0, 256),
	}
}

func appendString(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func (sw *SpillWriter) tableIdx(t TableID) (uint32, error) {
	if idx, ok := sw.tables[t]; ok {
		return idx, nil
	}
	idx := uint32(len(sw.tables))
	sw.tables[t] = idx
	b := []byte{recTable}
	b = binary.AppendUvarint(b, uint64(idx))
	b = appendString(b, t.Schema)
	b = appendString(b, t.Table)
	_, err := sw.w.Write(b)
	return idx, err
}

func (sw *SpillWriter) nameIdx(n string) (uint32, error) {
	if idx, ok := sw.names[n]; ok {
		return idx, nil
	}
	idx := uint32(len(sw.names))
	sw.names[n] = idx
	b := []byte{recName}
	b = binary.AppendUvarint(b, uint64(idx))
	b = appendString(b, n)
	_, err := sw.w.Write(b)
	return idx, err
}

// AppendValue mã hóa một giá trị cột/khóa. Kiểu không hỗ trợ trả lỗi.
func AppendValue(b []byte, v Value) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(b, valNil), nil
	case int:
		return binary.AppendVarint(append(b, valInt), int64(x)), nil
	case int8:
		return binary.AppendVarint(append(b, valInt), int64(x)), nil
	case int16:
		return binary.AppendVarint(append(b, valInt), int64(x)), nil
	case int32:
		return binary.AppendVarint(append(b, valInt), int64(x)), nil
	case int64:
		return binary.AppendVarint(append(b, valInt), x), nil
	case uint:
		return binary.AppendUvarint(append(b, valUint), uint64(x)), nil
	case uint8:
		return binary.AppendUvarint(append(b, valUint), uint64(x)), nil
	case uint16:
		return binary.AppendUvarint(append(b, valUint), uint64(x)), nil
	case uint32:
		return binary.AppendUvarint(append(b, valUint), uint64(x)), nil
	case uint64:
		return binary.AppendUvarint(append(b, valUint), x), nil
	case string:
		return appendString(append(b, valString), x), nil
	case []byte:
		b = binary.AppendUvarint(append(b, valBytes), uint64(len(x)))
		return append(b, x...), nil
	case float64:
		return binary.LittleEndian.AppendUint64(append(b, valFloat64), math.Float64bits(x)), nil
	case float32:
		return binary.LittleEndian.AppendUint32(append(b, valFloat32), math.Float32bits(x)), nil
	case bool:
		if x {
			return append(b, valBool, 1), nil
		}
		return append(b, valBool, 0), nil
	case time.Time:
		tb, err := x.MarshalBinary()
		if err != nil {
			return nil, err
		}
		b = binary.AppendUvarint(append(b, valTime), uint64(len(tb)))
		return append(b, tb...), nil
	default:
		return nil, fmt.Errorf("spill: unsupported value type %T", v)
	}
}

func (sw *SpillWriter) appendValue(b []byte, v Value) ([]byte, error) {
	if x, ok := v.(int); ok && sw.PreserveGoInt {
		return binary.AppendVarint(append(b, valGoInt), int64(x)), nil
	}
	return AppendValue(b, v)
}

func (sw *SpillWriter) appendValues(b []byte, vs []Value) ([]byte, error) {
	b = binary.AppendUvarint(b, uint64(len(vs)))
	var err error
	for _, v := range vs {
		if b, err = sw.appendValue(b, v); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func (sw *SpillWriter) appendOptValues(b []byte, vs []Value) ([]byte, error) {
	if vs == nil {
		return append(b, 0), nil
	}
	return sw.appendValues(append(b, 1), vs)
}

func opByte(op Op) byte {
	switch op {
	case OpInsert:
		return 1
	case OpUpdate:
		return 2
	case OpDelete:
		return 3
	}
	return 0
}

func byteOp(b byte) (Op, error) {
	switch b {
	case 1:
		return OpInsert, nil
	case 2:
		return OpUpdate, nil
	case 3:
		return OpDelete, nil
	}
	return "", fmt.Errorf("spill: unknown op %d", b)
}

// Write ghi một RowChange.
func (sw *SpillWriter) Write(rc *RowChange) error {
	ti, err := sw.tableIdx(rc.Table)
	if err != nil {
		return err
	}
	fkIdx := make([]uint32, len(rc.ForeignKeys))
	for i, fk := range rc.ForeignKeys {
		if fkIdx[i], err = sw.tableIdx(fk.ParentTable); err != nil {
			return err
		}
	}
	colIdx := make([]uint32, len(rc.Cols))
	for i, c := range rc.Cols {
		if colIdx[i], err = sw.nameIdx(c.Name); err != nil {
			return err
		}
	}

	b := append(sw.buf[:0], recRow)
	b = binary.AppendUvarint(b, uint64(ti))
	b = append(b, opByte(rc.Op))
	if b, err = sw.appendValues(b, rc.Key); err != nil {
		return err
	}
	if b, err = sw.appendOptValues(b, rc.OldKey); err != nil {
		return err
	}
	b = binary.AppendUvarint(b, uint64(len(rc.UniqueKeys)))
	for _, uk := range rc.UniqueKeys {
		b = appendString(b, uk.Name)
		if b, err = sw.appendValues(b, uk.Key); err != nil {
			return err
		}
		if b, err = sw.appendOptValues(b, uk.OldKey); err != nil {
			return err
		}
	}
	b = binary.AppendUvarint(b, uint64(len(rc.ForeignKeys)))
	for i, fk := range rc.ForeignKeys {
		b = binary.AppendUvarint(b, uint64(fkIdx[i]))
		if b, err = sw.appendValues(b, fk.ParentKey); err != nil {
			return err
		}
	}
	b = binary.AppendUvarint(b, uint64(len(rc.Cols)))
	for i, c := range rc.Cols {
		b = binary.AppendUvarint(b, uint64(colIdx[i]))
		if b, err = sw.appendValue(b, c.Value); err != nil {
			return err
		}
		if c.Unchanged {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
	}
	sw.buf = b
	_, err = sw.w.Write(b)
	return err
}

// Flush đẩy dữ liệu đệm xuống writer bên dưới.
func (sw *SpillWriter) Flush() error { return sw.w.Flush() }

// SpillReader đọc lại file tràn.
type SpillReader struct {
	f      *os.File
	r      *bufio.Reader
	tables []TableID
	names  []string
}

// NewSpillReader đọc luồng spill từ r (không cần file).
func NewSpillReader(r io.Reader) *SpillReader {
	return &SpillReader{r: bufio.NewReader(r)}
}

// OpenSpillReader mở file tràn tại path.
func OpenSpillReader(path string) (*SpillReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open spill file %s: %w", path, err)
	}
	return &SpillReader{f: f, r: bufio.NewReaderSize(f, 256*1024)}, nil
}

func (sr *SpillReader) readString() (string, error) {
	n, err := binary.ReadUvarint(sr.r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(sr.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func (sr *SpillReader) readValue() (Value, error) {
	tag, err := sr.r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch tag {
	case valNil:
		return nil, nil
	case valInt:
		return binary.ReadVarint(sr.r)
	case valGoInt:
		v, err := binary.ReadVarint(sr.r)
		return int(v), err
	case valUint:
		return binary.ReadUvarint(sr.r)
	case valString:
		return sr.readString()
	case valBytes:
		n, err := binary.ReadUvarint(sr.r)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, n)
		_, err = io.ReadFull(sr.r, buf)
		return buf, err
	case valFloat64:
		var buf [8]byte
		if _, err := io.ReadFull(sr.r, buf[:]); err != nil {
			return nil, err
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(buf[:])), nil
	case valFloat32:
		var buf [4]byte
		if _, err := io.ReadFull(sr.r, buf[:]); err != nil {
			return nil, err
		}
		return math.Float32frombits(binary.LittleEndian.Uint32(buf[:])), nil
	case valBool:
		c, err := sr.r.ReadByte()
		return c == 1, err
	case valTime:
		n, err := binary.ReadUvarint(sr.r)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(sr.r, buf); err != nil {
			return nil, err
		}
		var t time.Time
		err = t.UnmarshalBinary(buf)
		return t, err
	}
	return nil, fmt.Errorf("spill: unknown value tag %d", tag)
}

func (sr *SpillReader) readValues() ([]Value, error) {
	n, err := binary.ReadUvarint(sr.r)
	if err != nil {
		return nil, err
	}
	vs := make([]Value, n)
	for i := range vs {
		if vs[i], err = sr.readValue(); err != nil {
			return nil, err
		}
	}
	return vs, nil
}

func (sr *SpillReader) readOptValues() ([]Value, error) {
	flag, err := sr.r.ReadByte()
	if err != nil || flag == 0 {
		return nil, err
	}
	return sr.readValues()
}

func (sr *SpillReader) table(idx uint64) (TableID, error) {
	if idx >= uint64(len(sr.tables)) {
		return TableID{}, fmt.Errorf("spill: undefined table index %d", idx)
	}
	return sr.tables[idx], nil
}

// Next trả về RowChange kế tiếp, hoặc io.EOF khi hết file.
func (sr *SpillReader) Next() (*RowChange, error) {
	for {
		kind, err := sr.r.ReadByte()
		if err != nil {
			return nil, err // io.EOF ở ranh giới record là kết thúc hợp lệ
		}
		rc, err := sr.readRecord(kind)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if rc != nil {
			return rc, nil
		}
	}
}

func (sr *SpillReader) readRecord(kind byte) (*RowChange, error) {
	switch kind {
	case recTable:
		idx, err := binary.ReadUvarint(sr.r)
		if err != nil {
			return nil, err
		}
		schema, err := sr.readString()
		if err != nil {
			return nil, err
		}
		table, err := sr.readString()
		if err != nil {
			return nil, err
		}
		if idx != uint64(len(sr.tables)) {
			return nil, fmt.Errorf("spill: table index out of order")
		}
		sr.tables = append(sr.tables, TableID{Schema: schema, Table: table})
		return nil, nil
	case recName:
		idx, err := binary.ReadUvarint(sr.r)
		if err != nil {
			return nil, err
		}
		name, err := sr.readString()
		if err != nil {
			return nil, err
		}
		if idx != uint64(len(sr.names)) {
			return nil, fmt.Errorf("spill: name index out of order")
		}
		sr.names = append(sr.names, name)
		return nil, nil
	case recRow:
	default:
		return nil, fmt.Errorf("spill: unknown record kind %q", kind)
	}

	rc := &RowChange{}
	ti, err := binary.ReadUvarint(sr.r)
	if err != nil {
		return nil, err
	}
	if rc.Table, err = sr.table(ti); err != nil {
		return nil, err
	}
	ob, err := sr.r.ReadByte()
	if err != nil {
		return nil, err
	}
	if rc.Op, err = byteOp(ob); err != nil {
		return nil, err
	}
	if rc.Key, err = sr.readValues(); err != nil {
		return nil, err
	}
	if rc.OldKey, err = sr.readOptValues(); err != nil {
		return nil, err
	}
	nUK, err := binary.ReadUvarint(sr.r)
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < nUK; i++ {
		var uk UniqueKey
		if uk.Name, err = sr.readString(); err != nil {
			return nil, err
		}
		if uk.Key, err = sr.readValues(); err != nil {
			return nil, err
		}
		if uk.OldKey, err = sr.readOptValues(); err != nil {
			return nil, err
		}
		rc.UniqueKeys = append(rc.UniqueKeys, uk)
	}
	nFK, err := binary.ReadUvarint(sr.r)
	if err != nil {
		return nil, err
	}
	for i := uint64(0); i < nFK; i++ {
		var fk ForeignKey
		pi, err := binary.ReadUvarint(sr.r)
		if err != nil {
			return nil, err
		}
		if fk.ParentTable, err = sr.table(pi); err != nil {
			return nil, err
		}
		if fk.ParentKey, err = sr.readValues(); err != nil {
			return nil, err
		}
		rc.ForeignKeys = append(rc.ForeignKeys, fk)
	}
	nCols, err := binary.ReadUvarint(sr.r)
	if err != nil {
		return nil, err
	}
	if nCols > 0 {
		rc.Cols = make([]ColValue, nCols)
	}
	for i := range rc.Cols {
		ni, err := binary.ReadUvarint(sr.r)
		if err != nil {
			return nil, err
		}
		if ni >= uint64(len(sr.names)) {
			return nil, fmt.Errorf("spill: undefined column name index %d", ni)
		}
		rc.Cols[i].Name = sr.names[ni]
		if rc.Cols[i].Value, err = sr.readValue(); err != nil {
			return nil, err
		}
		u, err := sr.r.ReadByte()
		if err != nil {
			return nil, err
		}
		rc.Cols[i].Unchanged = u == 1
	}
	return rc, nil
}

// Close đóng file (nếu có).
func (sr *SpillReader) Close() error {
	if sr.f == nil {
		return nil
	}
	return sr.f.Close()
}

// ForEachRow duyệt mọi RowChange của Txn, kể cả khi Txn đã tràn đĩa.
func (t *Txn) ForEachRow(fn func(*RowChange) error) error {
	if !t.IsSpilled() {
		for i := range t.Rows {
			if err := fn(&t.Rows[i]); err != nil {
				return err
			}
		}
		return nil
	}
	sr, err := OpenSpillReader(t.SpillPath)
	if err != nil {
		return err
	}
	defer sr.Close()
	var n int64
	for {
		rc, err := sr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read spill %s: %w", t.SpillPath, err)
		}
		n++
		if err := fn(rc); err != nil {
			return err
		}
	}
	if t.RowCount > 0 && n != t.RowCount {
		return fmt.Errorf("spill %s: expected %d rows, read %d", t.SpillPath, t.RowCount, n)
	}
	return nil
}
