package mysql

import (
	"bytes"
	"compress/flate"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/go-mysql-org/go-mysql/replication"
)

// Cột COMPRESSED của MariaDB: binlog mang giá trị đã nén (SELECT trả giá trị gốc). Định dạng (kiểm trên binlog
// thật, 10.6):
//   - độ dài 0 (chuỗi rỗng): không có header;
//   - header 0x00 + dữ liệu gốc: giá trị ngắn hơn column_compression_threshold hoặc nén không có lợi;
//   - header 0x80 | 0x08 (deflate thô, column_compression_zlib_wrap=OFF mặc định) hoặc 0x80 (bọc zlib, wrap=ON)
//     | n (n = 1..4: số byte độ dài gốc), n byte độ dài gốc big-endian, rồi luồng nén.
//
// Header lạ hoặc độ dài sau giải nén khác độ dài khai báo thì lỗi (dừng, không ghi sai).
func decompressMariaDBValue(v []byte) ([]byte, error) {
	if len(v) == 0 {
		return v, nil
	}
	h := v[0]
	if h&0x80 == 0 {
		if h != 0 {
			return nil, fmt.Errorf("unknown compressed-column header 0x%02x", h)
		}
		return v[1:], nil
	}
	if h&0x70 != 0 {
		return nil, fmt.Errorf("unsupported compression method in header 0x%02x", h)
	}
	n := int(h & 0x07)
	if n < 1 || n > 4 || len(v) < 1+n {
		return nil, fmt.Errorf("invalid compressed-column header 0x%02x (length %d)", h, len(v))
	}
	var orig uint64
	for _, b := range v[1 : 1+n] {
		orig = orig<<8 | uint64(b)
	}
	body := bytes.NewReader(v[1+n:])
	var r io.Reader
	if h&0x08 != 0 {
		r = flate.NewReader(body)
	} else {
		zr, err := zlib.NewReader(body)
		if err != nil {
			return nil, fmt.Errorf("zlib header: %w", err)
		}
		r = zr
	}
	out, err := io.ReadAll(io.LimitReader(r, int64(orig)+1))
	if err != nil {
		return nil, fmt.Errorf("inflate: %w", err)
	}
	if uint64(len(out)) != orig {
		return nil, fmt.Errorf("inflated %d bytes, header says %d", len(out), orig)
	}
	return out, nil
}

// decompressRows giải nén (tại chỗ) các cột nén (chỉ số cols) của rows. Kiểu giá trị giữ như cột không nén:
// VARCHAR → string, BLOB/TEXT → []byte.
func decompressRows(tm *replication.TableMapEvent, rows [][]any, cols []int) error {
	for _, row := range rows {
		for _, i := range cols {
			if i >= len(row) || row[i] == nil {
				continue
			}
			var raw []byte
			isString := false
			switch v := row[i].(type) {
			case string:
				raw, isString = []byte(v), true
			case []byte:
				raw = v
			default:
				return fmt.Errorf("%s.%s column %d: unexpected compressed value %T", tm.Schema, tm.Table, i+1, v)
			}
			out, err := decompressMariaDBValue(raw)
			if err != nil {
				return fmt.Errorf("%s.%s column %d: %w", tm.Schema, tm.Table, i+1, err)
			}
			if isString {
				row[i] = string(out)
			} else {
				row[i] = out
			}
		}
	}
	return nil
}
