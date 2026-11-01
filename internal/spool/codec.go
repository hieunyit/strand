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

// encodeTxn tuần tự hóa model.Txn thành byte buffer.
func encodeTxn(t *model.Txn) ([]byte, error) {
	var buf bytes.Buffer

	// SourcePos
	if err := writeString(&buf, string(t.SourcePos.Engine)); err != nil {
		return nil, err
	}
	if err := writeString(&buf, t.SourcePos.GTID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, t.SourcePos.LSN); err != nil {
		return nil, err
	}

	// SchemaVer, Bytes, RowCount, SpillPath
	if err := binary.Write(&buf, binary.LittleEndian, t.SchemaVer); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, t.Bytes); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, t.RowCount); err != nil {
		return nil, err
	}
	if err := writeString(&buf, t.SpillPath); err != nil {
		return nil, err
	}

	// Rows
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(t.Rows))); err != nil {
		return nil, err
	}
	for i := range t.Rows {
		rc := &t.Rows[i]
		if err := writeString(&buf, rc.Table.Schema); err != nil {
			return nil, err
		}
		if err := writeString(&buf, rc.Table.Table); err != nil {
			return nil, err
		}
		var opByte byte = 1
		if rc.Op == model.OpUpdate {
			opByte = 2
		} else if rc.Op == model.OpDelete {
			opByte = 3
		}
		if err := binary.Write(&buf, binary.LittleEndian, opByte); err != nil {
			return nil, err
		}

		// Key
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(rc.Key))); err != nil {
			return nil, err
		}
		for _, kv := range rc.Key {
			if err := encodeValue(&buf, kv); err != nil {
				return nil, err
			}
		}

		// OldKey
		hasOldKey := byte(0)
		if len(rc.OldKey) > 0 {
			hasOldKey = 1
		}
		if err := binary.Write(&buf, binary.LittleEndian, hasOldKey); err != nil {
			return nil, err
		}
		if hasOldKey == 1 {
			if err := binary.Write(&buf, binary.LittleEndian, uint32(len(rc.OldKey))); err != nil {
				return nil, err
			}
			for _, okv := range rc.OldKey {
				if err := encodeValue(&buf, okv); err != nil {
					return nil, err
				}
			}
		}

		// Cols
		if err := binary.Write(&buf, binary.LittleEndian, uint32(len(rc.Cols))); err != nil {
			return nil, err
		}
		for _, col := range rc.Cols {
			if err := writeString(&buf, col.Name); err != nil {
				return nil, err
			}
			if err := encodeValue(&buf, col.Value); err != nil {
				return nil, err
			}
			unchanged := byte(0)
			if col.Unchanged {
				unchanged = 1
			}
			if err := binary.Write(&buf, binary.LittleEndian, unchanged); err != nil {
				return nil, err
			}
		}
	}

	return buf.Bytes(), nil
}

// decodeTxn giải mã byte buffer thành model.Txn.
func decodeTxn(data []byte) (*model.Txn, error) {
	r := bytes.NewReader(data)
	t := &model.Txn{}

	// SourcePos
	engine, err := readString(r)
	if err != nil {
		return nil, err
	}
	t.SourcePos.Engine = model.Engine(engine)

	gtid, err := readString(r)
	if err != nil {
		return nil, err
	}
	t.SourcePos.GTID = gtid

	if err := binary.Read(r, binary.LittleEndian, &t.SourcePos.LSN); err != nil {
		return nil, err
	}

	// SchemaVer, Bytes, RowCount, SpillPath
	if err := binary.Read(r, binary.LittleEndian, &t.SchemaVer); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &t.Bytes); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.LittleEndian, &t.RowCount); err != nil {
		return nil, err
	}
	spillPath, err := readString(r)
	if err != nil {
		return nil, err
	}
	t.SpillPath = spillPath

	// Rows
	var numRows uint32
	if err := binary.Read(r, binary.LittleEndian, &numRows); err != nil {
		return nil, err
	}
	if numRows > 0 {
		t.Rows = make([]model.RowChange, numRows)
		for i := uint32(0); i < numRows; i++ {
			rc := &t.Rows[i]
			schema, err := readString(r)
			if err != nil {
				return nil, err
			}
			tbl, err := readString(r)
			if err != nil {
				return nil, err
			}
			rc.Table = model.TableID{Schema: schema, Table: tbl}

			var opByte byte
			if err := binary.Read(r, binary.LittleEndian, &opByte); err != nil {
				return nil, err
			}
			switch opByte {
			case 1:
				rc.Op = model.OpInsert
			case 2:
				rc.Op = model.OpUpdate
			case 3:
				rc.Op = model.OpDelete
			default:
				return nil, fmt.Errorf("unknown op byte: %d", opByte)
			}

			// Key
			var numKeys uint32
			if err := binary.Read(r, binary.LittleEndian, &numKeys); err != nil {
				return nil, err
			}
			if numKeys > 0 {
				rc.Key = make([]model.Value, numKeys)
				for k := uint32(0); k < numKeys; k++ {
					v, err := decodeValue(r)
					if err != nil {
						return nil, err
					}
					rc.Key[k] = v
				}
			}

			// OldKey
			var hasOldKey byte
			if err := binary.Read(r, binary.LittleEndian, &hasOldKey); err != nil {
				return nil, err
			}
			if hasOldKey == 1 {
				var numOldKeys uint32
				if err := binary.Read(r, binary.LittleEndian, &numOldKeys); err != nil {
					return nil, err
				}
				if numOldKeys > 0 {
					rc.OldKey = make([]model.Value, numOldKeys)
					for k := uint32(0); k < numOldKeys; k++ {
						v, err := decodeValue(r)
						if err != nil {
							return nil, err
						}
						rc.OldKey[k] = v
					}
				}
			}

			// Cols
			var numCols uint32
			if err := binary.Read(r, binary.LittleEndian, &numCols); err != nil {
				return nil, err
			}
			if numCols > 0 {
				rc.Cols = make([]model.ColValue, numCols)
				for c := uint32(0); c < numCols; c++ {
					colName, err := readString(r)
					if err != nil {
						return nil, err
					}
					colVal, err := decodeValue(r)
					if err != nil {
						return nil, err
					}
					var unchanged byte
					if err := binary.Read(r, binary.LittleEndian, &unchanged); err != nil {
						return nil, err
					}
					rc.Cols[c] = model.ColValue{
						Name:      colName,
						Value:     colVal,
						Unchanged: unchanged == 1,
					}
				}
			}
		}
	}

	return t, nil
}
