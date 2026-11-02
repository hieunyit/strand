package spool

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"strand/internal/model"
)

const (
	typeNil    byte = 0
	typeInt64  byte = 1
	typeUint64 byte = 2
	typeString byte = 3
	typeBytes  byte = 4
	typeFloat  byte = 5
	typeBool   byte = 6
	typeTime   byte = 7
	typeInt    byte = 8
)

func writeString(w io.Writer, s string) error {
	b := []byte(s)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readString(r io.Reader) (string, error) {
	var n uint32
	if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
		return "", err
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func encodeValue(w io.Writer, val model.Value) error {
	if val == nil {
		return binary.Write(w, binary.LittleEndian, typeNil)
	}
	switch v := val.(type) {
	case int:
		if err := binary.Write(w, binary.LittleEndian, typeInt); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, int64(v))
	case int64:
		if err := binary.Write(w, binary.LittleEndian, typeInt64); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v)
	case int32:
		if err := binary.Write(w, binary.LittleEndian, typeInt64); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, int64(v))
	case uint64:
		if err := binary.Write(w, binary.LittleEndian, typeUint64); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v)
	case uint32:
		if err := binary.Write(w, binary.LittleEndian, typeUint64); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, uint64(v))
	case string:
		if err := binary.Write(w, binary.LittleEndian, typeString); err != nil {
			return err
		}
		return writeString(w, v)
	case []byte:
		if err := binary.Write(w, binary.LittleEndian, typeBytes); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		_, err := w.Write(v)
		return err
	case float64:
		if err := binary.Write(w, binary.LittleEndian, typeFloat); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v)
	case bool:
		if err := binary.Write(w, binary.LittleEndian, typeBool); err != nil {
			return err
		}
		var b byte = 0
		if v {
			b = 1
		}
		return binary.Write(w, binary.LittleEndian, b)
	case time.Time:
		if err := binary.Write(w, binary.LittleEndian, typeTime); err != nil {
			return err
		}
		return binary.Write(w, binary.LittleEndian, v.UnixNano())
	default:
		s := fmt.Sprintf("%v", v)
		if err := binary.Write(w, binary.LittleEndian, typeString); err != nil {
			return err
		}
		return writeString(w, s)
	}
}

func decodeValue(r io.Reader) (model.Value, error) {
	var t byte
	if err := binary.Read(r, binary.LittleEndian, &t); err != nil {
		return nil, err
	}
	switch t {
	case typeNil:
		return nil, nil
	case typeInt:
		var v int64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return int(v), nil
	case typeInt64:
		var v int64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case typeUint64:
		var v uint64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case typeString:
		return readString(r)
	case typeBytes:
		var n uint32
		if err := binary.Read(r, binary.LittleEndian, &n); err != nil {
			return nil, err
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf, nil
	case typeFloat:
		var v float64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		return v, nil
	case typeBool:
		var b byte
		if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
			return nil, err
		}
		return b == 1, nil
	case typeTime:
		var nano int64
		if err := binary.Read(r, binary.LittleEndian, &nano); err != nil {
			return nil, err
		}
		return time.Unix(0, nano), nil
	default:
		return nil, fmt.Errorf("unknown value type: %d", t)
	}
}

// Định dạng v2 (không mất dữ liệu): magic, các trường của Txn, rồi các row theo codec spill
// của model (giữ UniqueKeys/ForeignKeys cho writeset, mọi kiểu giá trị).
const txnMagicV2 = "STX2"

// encodeTxn tuần tự hóa model.Txn thành byte buffer.
func encodeTxn(t *model.Txn) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString(txnMagicV2)
	for _, s := range []string{string(t.SourcePos.Engine), t.SourcePos.GTID, t.SpillPath, t.DDL} {
		if err := writeString(&buf, s); err != nil {
			return nil, err
		}
	}
	for _, v := range []any{t.SourcePos.LSN, t.SchemaVer, t.Bytes, t.RowCount, t.SourceTS, uint32(len(t.SpilledTables)), uint32(len(t.Rows))} {
		if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	}
	for _, tb := range t.SpilledTables {
		if err := writeString(&buf, tb.Schema); err != nil {
			return nil, err
		}
		if err := writeString(&buf, tb.Table); err != nil {
			return nil, err
		}
	}
	sw := model.NewSpillWriter(&buf)
	sw.PreserveGoInt = true
	for i := range t.Rows {
		if err := sw.Write(&t.Rows[i]); err != nil {
			return nil, err
		}
	}
	if err := sw.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeTxn giải mã byte buffer thành model.Txn.
func decodeTxn(data []byte) (*model.Txn, error) {
	if len(data) < len(txnMagicV2) || string(data[:len(txnMagicV2)]) != txnMagicV2 {
		return nil, fmt.Errorf("spool: unknown txn encoding")
	}
	r := bytes.NewReader(data[len(txnMagicV2):])
	t := &model.Txn{}
	strs := make([]string, 4)
	for i := range strs {
		s, err := readString(r)
		if err != nil {
			return nil, err
		}
		strs[i] = s
	}
	t.SourcePos.Engine, t.SourcePos.GTID, t.SpillPath, t.DDL = model.Engine(strs[0]), strs[1], strs[2], strs[3]
	var nTables, nRows uint32
	for _, v := range []any{&t.SourcePos.LSN, &t.SchemaVer, &t.Bytes, &t.RowCount, &t.SourceTS, &nTables, &nRows} {
		if err := binary.Read(r, binary.LittleEndian, v); err != nil {
			return nil, err
		}
	}
	for i := uint32(0); i < nTables; i++ {
		s, err := readString(r)
		if err != nil {
			return nil, err
		}
		tb, err := readString(r)
		if err != nil {
			return nil, err
		}
		t.SpilledTables = append(t.SpilledTables, model.TableID{Schema: s, Table: tb})
	}
	sr := model.NewSpillReader(r)
	for i := uint32(0); i < nRows; i++ {
		rc, err := sr.Next()
		if err != nil {
			return nil, fmt.Errorf("spool: decode row %d: %w", i, err)
		}
		t.Rows = append(t.Rows, *rc)
	}
	return t, nil
}
