package pg

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"

	"strand/internal/model"
)

/*
Bảng mapping kiểu dữ liệu pgoutput (text format) sang model.Value (Go):

| PostgreSQL Type          | OID    | Định dạng text ví dụ        | Kiểu Go đích (model.Value)        | Ghi chú                                   |
|--------------------------|--------|------------------------------|-----------------------------------|-------------------------------------------|
| bool                     | 16     | "t", "f"                     | bool (true/false)                 | Chuẩn boolean                             |
| int2, int4, int8         | 21,23,20 | "42", "-1000", "9223372..."  | int64                             | Thống nhất kiểu số nguyên có dấu 64-bit   |
| oid                      | 26     | "16384"                      | int64                             | Định danh đối tượng PostgreSQL            |
| float4, float8           | 700,701| "3.14159", "NaN", "Infinity" | float64                           | extra_float_digits=3: không mất số        |
| numeric                  | 1700   | "123456789.987654321"        | string                            | Giữ nguyên chuỗi chính xác (không mất số) |
| bytea                    | 17     | "\\xDEADBEEF"                | []byte                            | Decode hex bytes (hoặc escape format)     |
| date                     | 1082   | "2026-11-03"                 | time.Time (UTC, 00:00)            | "infinity"/"-infinity"/"... BC" → string  |
| timestamp                | 1114   | "2026-11-03 13:37:49.123456" | time.Time (giờ tường, gắn UTC)    | như date với giá trị đặc biệt             |
| timestamptz              | 1184   | "2026-11-03 13:37:49.1+00"   | time.Time (UTC)                   | phiên đặt TimeZone=UTC; như date          |
| text, varchar, bpchar    | 25,1043,1042 | "chuỗi văn bản"       | string                            | Chuỗi ký tự UTF-8                         |
| char, name               | 18, 19 | "c", "table_name"            | string                            | Ký tự đơn hoặc định danh                  |
| json, jsonb              | 114,3802 | "{\"k\":\"v\"}"             | string                            | Chuỗi JSON hợp lệ                         |
| uuid                     | 2950   | "a0eebc99-9c0b-4ef8-bb6d-..."| string                            | Định danh UUID tiêu chuẩn                 |
| time, timetz             | 1083,1266 | "13:37:49", "13:37:49+07"  | string                            | Giờ trong ngày                            |
| interval                 | 1186   | "1 year 2 months"            | string                            | Khoảng thời gian                          |
| NULL ('n')               | —      | (không có payload)           | nil                               | Giá trị null                              |
| TOAST Unchanged ('u')    | —      | (không gửi dữ liệu)          | model.ColValue{Unchanged: true}   | Cột TOAST không đổi theo spec 02 §3, §8   |
| Các kiểu khác (enum/...) | —      | "giá trị"                    | string                            | Fallback giữ chuỗi an toàn (cả mảng)      |
| Binary ('b')             | —      | —                            | lỗi                               | Reader không bật binary nên coi là lỗi    |

Phiên replication được mở với DateStyle=ISO, TimeZone=UTC, IntervalStyle=postgres, bytea_output=hex,
extra_float_digits=3 (xem replicationConfig trong reader.go) để định dạng text ở trên là cố định.
Giá trị không parse được theo bảng trên làm reader phát EventError (không bỏ row im lặng).
*/

// Danh sách các OID phổ biến trong PostgreSQL
const (
	BoolOID        uint32 = 16
	ByteaOID       uint32 = 17
	CharOID        uint32 = 18
	NameOID        uint32 = 19
	Int8OID        uint32 = 20
	Int2OID        uint32 = 21
	Int4OID        uint32 = 23
	TextOID        uint32 = 25
	OIDOID         uint32 = 26
	JSONOID        uint32 = 114
	Float4OID      uint32 = 700
	Float8OID      uint32 = 701
	BPCharOID      uint32 = 1042
	VarcharOID     uint32 = 1043
	DateOID        uint32 = 1082
	TimeOID        uint32 = 1083
	TimestampOID   uint32 = 1114
	TimestamptzOID uint32 = 1184
	IntervalOID    uint32 = 1186
	TimetzOID      uint32 = 1266
	NumericOID     uint32 = 1700
	UUIDOID        uint32 = 2950
	JSONBOID       uint32 = 3802
)

// formatColValue chuyển đổi giá trị một cột từ pglogrepl.TupleDataColumn sang kiểu Go (model.Value).
func formatColValue(col *pglogrepl.TupleDataColumn, relCol *pglogrepl.RelationMessageColumn) (any, error) {
	colName := ""
	var dataType uint32
	if relCol != nil {
		colName = relCol.Name
		dataType = relCol.DataType
	}

	switch col.DataType {
	case pglogrepl.TupleDataTypeNull:
		return nil, nil

	case pglogrepl.TupleDataTypeToast:
		// spec: 02 §3 ColValue.Unchanged = true với cột TOAST vắng mặt
		return model.ColValue{
			Name:      colName,
			Value:     nil,
			Unchanged: true,
		}, nil

	case pglogrepl.TupleDataTypeText:
		return parseTextValue(dataType, string(col.Data))

	default:
		// Reader không bật option binary của pgoutput nên 'b' không được phép xuất hiện; nếu có thì
		// báo lỗi thay vì đoán kiểu.
		return nil, fmt.Errorf("cột %q: kiểu tuple data %q không hỗ trợ", colName, col.DataType)
	}
}

// parseTextValue phân tích chuỗi text từ pgoutput sang kiểu Go theo bảng mapping ở đầu file.
// Lỗi parse được trả về cho reader (reader phát EventError), không bao giờ bỏ qua row im lặng.
func parseTextValue(oid uint32, s string) (any, error) {
	switch oid {
	case BoolOID:
		switch s {
		case "t", "true":
			return true, nil
		case "f", "false":
			return false, nil
		}
		return nil, fmt.Errorf("giá trị bool không hợp lệ: %q", s)

	case Int2OID, Int4OID, Int8OID, OIDOID:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse int64 từ %q (oid %d): %w", s, oid, err)
		}
		return v, nil

	case Float4OID, Float8OID:
		// strconv.ParseFloat nhận cả "NaN", "Infinity", "-Infinity".
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("parse float64 từ %q (oid %d): %w", s, oid, err)
		}
		return v, nil

	case NumericOID:
		// Giữ nguyên chuỗi để không mất độ chính xác (kể cả "NaN", "Infinity").
		return s, nil

	case ByteaOID:
		return decodeBytea(s)

	case DateOID, TimestampOID, TimestamptzOID:
		if isSpecialTemporal(s) {
			// infinity/-infinity và ngày trước Công nguyên không biểu diễn được bằng time.Time một cách
			// nhất quán: giữ nguyên chuỗi text.
			return s, nil
		}
		var (
			t   time.Time
			err error
		)
		switch oid {
		case DateOID:
			t, err = time.ParseInLocation("2006-01-02", s, time.UTC)
		case TimestampOID:
			t, err = parseTimestamp(s)
		default:
			t, err = parseTimestamptz(s)
		}
		if err != nil {
			return nil, fmt.Errorf("parse thời gian từ %q (oid %d): %w", s, oid, err)
		}
		return t, nil

	default:
		// text, varchar, bpchar, char, name, json, jsonb, uuid, time, timetz, interval, enum, mảng, kiểu
		// người dùng định nghĩa...: giữ nguyên chuỗi text.
		return s, nil
	}
}

// isSpecialTemporal nhận diện giá trị ngày giờ không map được sang time.Time.
func isSpecialTemporal(s string) bool {
	return s == "infinity" || s == "-infinity" || strings.HasSuffix(s, " BC")
}

// decodeBytea phân tích định dạng hex hoặc escape của cột bytea.
func decodeBytea(s string) ([]byte, error) {
	if strings.HasPrefix(s, "\\x") || strings.HasPrefix(s, "\\X") {
		b, err := hex.DecodeString(s[2:])
		if err != nil {
			return nil, fmt.Errorf("decode hex bytea: %w", err)
		}
		return b, nil
	}

	// Escape format: xử lý các chuỗi \xxx (octal) hoặc \\
	res := make([]byte, 0, len(s))
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+1 < len(s) {
			if s[i+1] == '\\' {
				res = append(res, '\\')
				i += 2
				continue
			}
			if i+3 < len(s) && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
				val, err := strconv.ParseUint(s[i+1:i+4], 8, 8)
				if err == nil {
					res = append(res, byte(val))
					i += 4
					continue
				}
			}
		}
		res = append(res, s[i])
		i++
	}
	return res, nil
}

func isOctal(b byte) bool {
	return b >= '0' && b <= '7'
}

// parseTimestamp parse timestamp không múi giờ (DateStyle=ISO, phiên replication đặt sẵn); trả về time.Time UTC.
// Phần lẻ của giây (nếu có) được time.Parse tự nhận.
func parseTimestamp(s string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02 15:04:05", s, time.UTC)
}

// parseTimestamptz parse timestamptz dạng ISO với offset "+07", "+05:30" hoặc "+05:53:28".
// Phiên replication đặt TimeZone=UTC nên thực tế offset luôn là "+00".
func parseTimestamptz(s string) (time.Time, error) {
	var lastErr error
	for _, l := range []string{"2006-01-02 15:04:05-07", "2006-01-02 15:04:05-07:00", "2006-01-02 15:04:05-07:00:00"} {
		t, err := time.Parse(l, s)
		if err == nil {
			return t.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}
