package snapshot

// Khóa chính tổng quát cho snapshot logic (việc C, mức 1).
//
// Range-state quyết "khóa thuộc chunk nào" bằng so sánh trong Go, còn keyset walk và SELECT của chunk
// dùng thứ tự của DB (ORDER BY pk, WHERE pk > ?). Hai thứ tự phải GIỐNG HỆT, và phép bằng trong Go phải
// khớp phép bằng của PK trong DB — lệch là mất dữ liệu âm thầm (event bị xếp nhầm vào range "Chưa đọc"
// rồi bị bỏ, hoặc row bị xóa ở flush). Vì vậy:
//
//   - Tuple PK được mã hóa thành chuỗi byte "memcomparable": so byte (bytes.Compare / so string) cho đúng
//     thứ tự tuple của DB. Mỗi thành phần tự phân định (độ dài cố định hoặc có escape + terminator) nên
//     nối các thành phần giữ thứ tự từ điển theo cột.
//   - Chỉ nhận các kiểu mà thứ tự byte chứng minh được là trùng với DB (bảng dưới); kiểu khác bị từ chối
//     khi dựng codec (PKUnsupportedError), không đoán.
//   - Cùng một giá trị phải cho cùng mã dù đến từ binlog / pgoutput (chuỗi, số nguyên Go) hay từ SELECT
//     ([]byte text protocol): mỗi kiểu chuẩn hóa từ mọi dạng Go đã biết về một dạng duy nhất.
//
// | Kiểu                                   | Mã hóa                                   | Vì sao đúng thứ tự DB                 |
// |----------------------------------------|------------------------------------------|---------------------------------------|
// | số nguyên có dấu (MySQL/PG), YEAR      | 8 byte big-endian, lật bit dấu           | thứ tự số                             |
// | số nguyên không dấu (MySQL UNSIGNED)   | 8 byte big-endian                        | thứ tự số                             |
// | DECIMAL / NUMERIC                      | dấu, số mũ, chữ số (bỏ 0 thừa), kết thúc | thứ tự số; 1.0 = 1.00                 |
// | DATE / DATETIME / TIMESTAMP (MySQL, PG)| số nguyên giữ thứ tự từ các trường lịch  | thứ tự thời gian (TIMESTAMP ở UTC)    |
// | TIME (MySQL)                           | micro giây có dấu                        | thứ tự thời lượng                     |
// | BINARY/VARBINARY, bytea, _0900_bin,    | byte có escape 0x00→00 FF, kết thúc 00 01| so byte (memcmp, ngắn hơn đứng trước) |
// | PG text/varchar COLLATE "C"/"POSIX"    |                                          |                                       |
// | MySQL *_bin PAD SPACE                  | bỏ space cuối rồi như trên; TỪ CHỐI giá  | PAD SPACE = so sau khi đệm space; chỉ |
// |                                        | trị có byte < 0x20                       | lệch khi có byte < 0x20 (xem padKey)   |
// | PG bpchar COLLATE "C"                  | bỏ space cuối rồi như bytes              | bpcharcmp so sau khi bỏ space cuối    |
// | PG uuid                                | 16 byte                                  | uuid_cmp = memcmp                     |
//
// Mức 2 (collation không nhị phân, WEIGHT_STRING) và mức 3 (PG ICU/libc) chưa nằm trong codec này.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"strand/internal/model"
)

// PKKind là cách mã hóa một cột PK.
type PKKind int

const (
	PKInt       PKKind = iota + 1 // số nguyên có dấu (kể cả YEAR)
	PKUint                        // số nguyên không dấu
	PKDecimal                     // DECIMAL / NUMERIC
	PKDateTime                    // DATE, DATETIME, TIMESTAMP (MySQL chuỗi; PG time.Time)
	PKTime                        // MySQL TIME
	PKBytes                       // so byte không đệm
	PKPadBytes                    // MySQL PAD SPACE nhị phân
	PKTrimBytes                   // PG bpchar "C"
	PKUUID                        // PG uuid
	PKWeight                      // MySQL chuỗi collation không nhị phân: WEIGHT_STRING do MySQL tính (mức 2)
)

func (k PKKind) String() string {
	switch k {
	case PKInt:
		return "int"
	case PKUint:
		return "uint"
	case PKDecimal:
		return "decimal"
	case PKDateTime:
		return "datetime"
	case PKTime:
		return "time"
	case PKBytes:
		return "bytes"
	case PKPadBytes:
		return "pad-space-bytes"
	case PKTrimBytes:
		return "trimmed-bytes"
	case PKUUID:
		return "uuid"
	case PKWeight:
		return "weight-string"
	}
	return fmt.Sprintf("PKKind(%d)", int(k))
}

// PKColumn là một cột PK và cách mã hóa của nó.
type PKColumn struct {
	Name string
	Kind PKKind
	// SQLType: kiểu cột để ép tham số khi so sánh (MySQL DECIMAL(p,s): so DECIMAL với tham số chuỗi
	// bị ép về double và mất chính xác). Rỗng = không ép.
	SQLType string
	// Collation: collation của cột chuỗi (để so khớp với bảng đích; thứ tự phải giống nhau hai phía).
	Collation string
	// Charset: bộ ký tự của cột chuỗi (PKWeight: để tính weight của giá trị như của cột).
	Charset string
	// PadSpace: collation PAD SPACE (PKWeight): DB so như thể chuỗi ngắn hơn được đệm space, nên weight
	// thô không so byte được ('a' = 'a ', 'a' > 'a\t') — weigher chuyển qua PadSpaceWeightKey (review N04).
	PadSpace bool
}

// PKCodec mã hóa tuple PK của một bảng.
type PKCodec struct {
	Cols []PKColumn
	// Weigh trả WEIGHT_STRING của giá trị v theo collation của cột (PKWeight, mức 2) — tính bởi chính
	// MySQL (MySQLPKCodec gắn sẵn, có cache). nil: codec không có cột PKWeight.
	Weigh func(col PKColumn, v model.Value) ([]byte, error)
}

// Key là tuple PK đã mã hóa memcomparable (so sánh string = so thứ tự tuple của DB).
type Key string

// PKUnsupportedError: cột PK có kiểu/collation mà codec không chứng minh được thứ tự.
type PKUnsupportedError struct {
	Column, Type, Reason string
}

func (e *PKUnsupportedError) Error() string {
	return fmt.Sprintf("primary key column %s (%s) is not supported by logical snapshot: %s", e.Column, e.Type, e.Reason)
}

// MySQLPKColumn chọn cách mã hóa cho một cột PK MySQL từ information_schema: dataType = DATA_TYPE,
// columnType = COLUMN_TYPE (để biết unsigned), charset/collation/padAttribute của cột chuỗi (rỗng nếu
// không phải chuỗi; padAttribute = COLLATIONS.PAD_ATTRIBUTE "PAD SPACE" | "NO PAD").
func MySQLPKColumn(name, dataType, columnType, charset, collation, padAttribute string) (PKColumn, error) {
	dt := strings.ToLower(dataType)
	unsupported := func(reason string) (PKColumn, error) {
		return PKColumn{}, &PKUnsupportedError{Column: name, Type: columnType, Reason: reason}
	}
	switch dt {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		if strings.Contains(strings.ToLower(columnType), "unsigned") {
			return PKColumn{Name: name, Kind: PKUint}, nil
		}
		return PKColumn{Name: name, Kind: PKInt}, nil
	case "year":
		return PKColumn{Name: name, Kind: PKInt}, nil
	case "decimal", "numeric":
		return PKColumn{Name: name, Kind: PKDecimal, SQLType: strings.ToUpper(strings.TrimSuffix(strings.ToLower(columnType), " unsigned"))}, nil
	case "date", "datetime", "timestamp":
		return PKColumn{Name: name, Kind: PKDateTime}, nil
	case "time":
		return PKColumn{Name: name, Kind: PKTime}, nil
	case "binary", "varbinary":
		return PKColumn{Name: name, Kind: PKBytes}, nil
	case "char", "varchar":
		cs, coll := strings.ToLower(charset), strings.ToLower(collation)
		switch cs {
		case "binary":
			return PKColumn{Name: name, Kind: PKBytes}, nil
		case "utf8mb4", "utf8mb3", "utf8", "latin1", "ascii":
		default:
			return unsupported("character set " + charset + " has no byte-order binary collation guarantee")
		}
		if !strings.HasSuffix(coll, "_bin") {
			// Mức 2: thứ tự và phép bằng của collation lấy từ WEIGHT_STRING của chính MySQL (không cài lại
			// collation trong Go).
			return PKColumn{Name: name, Kind: PKWeight, Collation: collation, Charset: charset,
				PadSpace: !strings.EqualFold(strings.TrimSpace(padAttribute), "NO PAD")}, nil
		}
		if strings.EqualFold(strings.TrimSpace(padAttribute), "NO PAD") {
			return PKColumn{Name: name, Kind: PKBytes}, nil
		}
		return PKColumn{Name: name, Kind: PKPadBytes}, nil
	}
	return unsupported("type " + dataType + " is not supported")
}

// PGPKColumn chọn cách mã hóa cho một cột PK PostgreSQL từ catalog: typname (pg_type.typname của kiểu
// gốc, ví dụ int8, numeric, text, bpchar, uuid) và tên collation của cột (rỗng nếu kiểu không có
// collation).
func PGPKColumn(name, typname, collation string) (PKColumn, error) {
	c, err := pgPKColumn(name, typname, collation)
	if err == nil {
		c.SQLType, c.Collation = typname, collation
	}
	return c, err
}

func pgPKColumn(name, typname, collation string) (PKColumn, error) {
	unsupported := func(reason string) (PKColumn, error) {
		return PKColumn{}, &PKUnsupportedError{Column: name, Type: typname, Reason: reason}
	}
	cBytes := func(kind PKKind) (PKColumn, error) {
		switch collation {
		case "C", "POSIX", "ucs_basic":
			return PKColumn{Name: name, Kind: kind}, nil
		}
		return unsupported(fmt.Sprintf("collation %q is not byte-ordered (use COLLATE \"C\", or seed the table from a backup)", collation))
	}
	switch strings.ToLower(typname) {
	case "int2", "int4", "int8":
		return PKColumn{Name: name, Kind: PKInt}, nil
	case "numeric":
		return PKColumn{Name: name, Kind: PKDecimal}, nil
	case "date", "timestamp", "timestamptz":
		return PKColumn{Name: name, Kind: PKDateTime}, nil
	case "bytea":
		return PKColumn{Name: name, Kind: PKBytes}, nil
	case "uuid":
		return PKColumn{Name: name, Kind: PKUUID}, nil
	case "text", "varchar":
		return cBytes(PKBytes)
	case "bpchar":
		return cBytes(PKTrimBytes)
	}
	return unsupported("type " + typname + " is not supported")
}

// SameOrder: hai codec cho cùng thứ tự và cùng phép bằng (cùng cột, cùng kiểu mã hóa, cùng collation)
// — điều kiện để xóa theo range khóa ở target bằng cận đọc từ source.
func (c *PKCodec) SameOrder(o *PKCodec) error {
	if len(c.Cols) != len(o.Cols) {
		return fmt.Errorf("primary key has %d columns on one side and %d on the other", len(c.Cols), len(o.Cols))
	}
	for i := range c.Cols {
		a, b := c.Cols[i], o.Cols[i]
		if a.Name != b.Name || a.Kind != b.Kind || !strings.EqualFold(a.SQLType, b.SQLType) || !strings.EqualFold(a.Collation, b.Collation) {
			return fmt.Errorf("primary key column %d differs: %s %s %s %s vs %s %s %s %s", i+1,
				a.Name, a.Kind, a.SQLType, a.Collation, b.Name, b.Kind, b.SQLType, b.Collation)
		}
	}
	return nil
}

// IsSingleInt: PK một cột số nguyên có dấu — đường cũ (int64) biểu diễn được trực tiếp.
func (c *PKCodec) IsSingleInt() bool {
	return c != nil && len(c.Cols) == 1 && c.Cols[0].Kind == PKInt
}

// Encode mã hóa tuple PK (theo thứ tự cột của codec).
func (c *PKCodec) Encode(vals []model.Value) (Key, error) {
	if len(vals) != len(c.Cols) {
		return "", fmt.Errorf("primary key has %d values, want %d (%v)", len(vals), len(c.Cols), c.colNames())
	}
	var b []byte
	for i, col := range c.Cols {
		if col.Kind == PKWeight {
			if c.Weigh == nil {
				return "", fmt.Errorf("primary key column %s: collation %s needs WEIGHT_STRING from the source (no weigher)", col.Name, col.Collation)
			}
			if vals[i] == nil {
				return "", fmt.Errorf("primary key column %s: NULL in primary key", col.Name)
			}
			w, err := c.Weigh(col, vals[i])
			if err != nil {
				return "", fmt.Errorf("primary key column %s: weight string: %w", col.Name, err)
			}
			b = appendEscaped(b, w)
			continue
		}
		var err error
		if b, err = appendKeyPart(b, col.Kind, vals[i]); err != nil {
			return "", fmt.Errorf("primary key column %s: %w", col.Name, err)
		}
	}
	return Key(b), nil
}

func (c *PKCodec) colNames() []string {
	out := make([]string, len(c.Cols))
	for i, col := range c.Cols {
		out[i] = col.Name
	}
	return out
}

// EncodeInt64: mã của khóa một cột số nguyên có dấu (đường int64).
func EncodeInt64(v int64) Key {
	return Key(appendInt(nil, v))
}

func appendInt(b []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(b, uint64(v)^(1<<63))
}

func appendKeyPart(b []byte, kind PKKind, v model.Value) ([]byte, error) {
	if v == nil {
		return nil, fmt.Errorf("NULL in primary key")
	}
	switch kind {
	case PKInt:
		n, err := toInt64(v)
		if err != nil {
			return nil, err
		}
		return appendInt(b, n), nil
	case PKUint:
		n, err := toUint64(v)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.AppendUint64(b, n), nil
	case PKDecimal:
		s, err := toText(v)
		if err != nil {
			return nil, err
		}
		return appendDecimal(b, s)
	case PKDateTime:
		n, err := dateTimeOrdinal(v)
		if err != nil {
			return nil, err
		}
		return appendInt(b, n), nil
	case PKTime:
		s, err := toText(v)
		if err != nil {
			return nil, err
		}
		n, err := timeMicros(s)
		if err != nil {
			return nil, err
		}
		return appendInt(b, n), nil
	case PKBytes:
		raw, err := toBytes(v)
		if err != nil {
			return nil, err
		}
		return appendEscaped(b, raw), nil
	case PKPadBytes:
		raw, err := toBytes(v)
		if err != nil {
			return nil, err
		}
		raw = bytes.TrimRight(raw, " ")
		for _, c := range raw {
			if c < 0x20 {
				// PAD SPACE so như thể chuỗi ngắn hơn được đệm space: "a" > "a\t". Bỏ space cuối rồi so byte
				// cho "a" < "a\t" — chỉ sai khi có byte < 0x20, nên từ chối đúng các giá trị đó.
				return nil, fmt.Errorf("value %q contains a control byte 0x%02x: its PAD SPACE order cannot be represented", raw, c)
			}
		}
		return appendEscaped(b, raw), nil
	case PKTrimBytes:
		raw, err := toBytes(v)
		if err != nil {
			return nil, err
		}
		return appendEscaped(b, bytes.TrimRight(raw, " ")), nil
	case PKUUID:
		s, err := toText(v)
		if err != nil {
			return nil, err
		}
		u, err := hex.DecodeString(strings.ReplaceAll(strings.Trim(s, "{}"), "-", ""))
		if err != nil || len(u) != 16 {
			return nil, fmt.Errorf("invalid uuid %q", s)
		}
		return append(b, u...), nil
	}
	return nil, fmt.Errorf("unknown key kind %v", kind)
}

// appendEscaped: 0x00 → 00 FF, kết thúc 00 01. Giữ thứ tự memcmp kể cả khi một chuỗi là tiền tố của
// chuỗi kia (tiền tố kết thúc bằng 00 01 < 00 FF hoặc mọi byte ≥ 01 tiếp theo của chuỗi dài hơn).
func appendEscaped(b, raw []byte) []byte {
	for _, c := range raw {
		if c == 0 {
			b = append(b, 0, 0xFF)
		} else {
			b = append(b, c)
		}
	}
	return append(b, 0, 1)
}

func toText(v model.Value) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, uint:
		return fmt.Sprint(x), nil
	}
	return "", fmt.Errorf("unexpected value type %T", v)
}

func toBytes(v model.Value) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	}
	return nil, fmt.Errorf("unexpected value type %T for a byte key", v)
}

func toInt64(v model.Value) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int32:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, fmt.Errorf("%d overflows a signed key", x)
		}
		return int64(x), nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return 0, fmt.Errorf("%d overflows a signed key", x)
		}
		return int64(x), nil
	case float64:
		if x != math.Trunc(x) || x > math.MaxInt64 || x < math.MinInt64 {
			return 0, fmt.Errorf("%v is not an exact integer", x)
		}
		return int64(x), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(x), 10, 64)
	case []byte:
		return strconv.ParseInt(strings.TrimSpace(string(x)), 10, 64)
	}
	return 0, fmt.Errorf("unexpected value type %T for an integer key", v)
}

func toUint64(v model.Value) (uint64, error) {
	switch x := v.(type) {
	case uint64:
		return x, nil
	case uint32:
		return uint64(x), nil
	case uint16:
		return uint64(x), nil
	case uint8:
		return uint64(x), nil
	case uint:
		return uint64(x), nil
	case int64, int32, int16, int8, int:
		n, _ := toInt64(x)
		if n < 0 {
			// go-mysql trả cột UNSIGNED dưới dạng số có dấu khi giá trị vượt MaxInt64 ở một số đường; MySQL
			// không lưu số âm trong cột UNSIGNED, nên số âm ở đây là bit pattern của uint64.
			return uint64(n), nil
		}
		return uint64(n), nil
	case string:
		return strconv.ParseUint(strings.TrimSpace(x), 10, 64)
	case []byte:
		return strconv.ParseUint(strings.TrimSpace(string(x)), 10, 64)
	}
	return 0, fmt.Errorf("unexpected value type %T for an unsigned key", v)
}

// appendDecimal mã hóa số thập phân dạng text giữ thứ tự số. Dạng: lớp (01 âm, 02 không, 03 dương);
// với số khác 0: số mũ thập phân (int16 lật bit dấu, đảo nếu âm) rồi các chữ số có nghĩa ('0'+1…'9'+1,
// bỏ 0 cuối) và byte kết thúc 00 (đảo nếu âm: FF). 1.0, 1.00, 01 cho cùng mã.
func appendDecimal(b []byte, s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	neg := false
	switch {
	case strings.HasPrefix(s, "-"):
		neg, s = true, s[1:]
	case strings.HasPrefix(s, "+"):
		s = s[1:]
	}
	intPart, frac, _ := strings.Cut(s, ".")
	if intPart == "" && frac == "" {
		return nil, fmt.Errorf("invalid decimal %q", s)
	}
	for _, r := range intPart + frac {
		if r < '0' || r > '9' {
			return nil, fmt.Errorf("invalid decimal %q", s)
		}
	}
	digits := strings.TrimLeft(intPart, "0")
	exp := len(digits) // vị trí dấu thập phân sau chữ số có nghĩa đầu tiên
	if digits == "" {
		// Phần nguyên = 0: số mũ âm theo số chữ số 0 dẫn đầu của phần lẻ.
		f := strings.TrimLeft(frac, "0")
		exp = -(len(frac) - len(f))
		digits = f
	} else {
		digits += frac
	}
	digits = strings.TrimRight(digits, "0")
	if digits == "" {
		return append(b, 0x02), nil
	}
	if exp > math.MaxInt16 || exp < math.MinInt16 {
		return nil, fmt.Errorf("decimal %q out of range", s)
	}
	body := binary.BigEndian.AppendUint16(nil, uint16(int16(exp))^(1<<15))
	for i := 0; i < len(digits); i++ {
		body = append(body, digits[i]-'0'+1)
	}
	body = append(body, 0)
	if neg {
		for i := range body {
			body[i] = ^body[i]
		}
		return append(append(b, 0x01), body...), nil
	}
	return append(append(b, 0x03), body...), nil
}

// dateTimeOrdinal: số nguyên giữ thứ tự từ các trường lịch (năm, tháng, ngày, giờ, phút, giây, micro
// giây), không cần lịch hợp lệ (MySQL cho phép ngày 0000-00-00). MySQL đưa DATE/DATETIME/TIMESTAMP dạng
// chuỗi "YYYY-MM-DD[ HH:MM:SS[.ffffff]]" (binlog với TIMESTAMP ở UTC; SELECT với phiên UTC); PG đưa
// time.Time (timestamptz ở UTC).
func dateTimeOrdinal(v model.Value) (int64, error) {
	var y, mo, d, h, mi, sec, us int64
	switch x := v.(type) {
	case time.Time:
		// Mọi nguồn đưa time.Time ở UTC (PG timestamp parse ở UTC, timestamptz phiên UTC; MySQL loc=UTC).
		t := x.UTC()
		y, mo, d = int64(t.Year()), int64(t.Month()), int64(t.Day())
		h, mi, sec, us = int64(t.Hour()), int64(t.Minute()), int64(t.Second()), int64(t.Nanosecond()/1000)
		if t.Nanosecond()%1000 != 0 {
			return 0, fmt.Errorf("timestamp %v has sub-microsecond precision", x)
		}
	case string, []byte:
		s, _ := toText(x)
		var err error
		if y, mo, d, h, mi, sec, us, err = parseMySQLDateTime(s); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("unexpected value type %T for a date/time key", v)
	}
	if y < 0 || y > 9999 {
		return 0, fmt.Errorf("year %d out of range for a key", y)
	}
	return ((((((y*13+mo)*32+d)*24+h)*60+mi)*60+sec)*1_000_000 + us), nil
}

func parseMySQLDateTime(s string) (y, mo, d, h, mi, sec, us int64, err error) {
	// Dạng "YYYY-MM-DD HH:MM:SS[.f]" (binlog, SELECT text) hoặc RFC3339 "…T…Z" (driver với parseTime).
	s = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(s), "Z"), "+00:00")
	datePart, timePart, _ := strings.Cut(s, " ")
	if timePart == "" {
		datePart, timePart, _ = strings.Cut(s, "T")
	}
	ds := strings.Split(datePart, "-")
	if len(ds) != 3 {
		return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("invalid date/time %q", s)
	}
	nums := make([]int64, 0, 7)
	for _, p := range ds {
		n, e := strconv.ParseInt(p, 10, 64)
		if e != nil {
			return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("invalid date/time %q", s)
		}
		nums = append(nums, n)
	}
	if timePart != "" {
		hms, frac, _ := strings.Cut(timePart, ".")
		ts := strings.Split(hms, ":")
		if len(ts) != 3 {
			return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("invalid date/time %q", s)
		}
		for _, p := range ts {
			n, e := strconv.ParseInt(p, 10, 64)
			if e != nil {
				return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("invalid date/time %q", s)
			}
			nums = append(nums, n)
		}
		u, e := fracMicros(frac)
		if e != nil {
			return 0, 0, 0, 0, 0, 0, 0, fmt.Errorf("invalid date/time %q", s)
		}
		nums = append(nums, u)
	} else {
		nums = append(nums, 0, 0, 0, 0)
	}
	return nums[0], nums[1], nums[2], nums[3], nums[4], nums[5], nums[6], nil
}

func fracMicros(frac string) (int64, error) {
	if frac == "" {
		return 0, nil
	}
	if len(frac) > 6 {
		if strings.Trim(frac[6:], "0") != "" {
			return 0, fmt.Errorf("sub-microsecond precision")
		}
		frac = frac[:6]
	}
	frac += strings.Repeat("0", 6-len(frac))
	return strconv.ParseInt(frac, 10, 64)
}

// timeMicros: MySQL TIME "[-]HHH:MM:SS[.ffffff]" thành micro giây có dấu.
func timeMicros(s string) (int64, error) {
	s = strings.TrimSpace(s)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	hms, frac, _ := strings.Cut(s, ".")
	ts := strings.Split(hms, ":")
	if len(ts) != 3 {
		return 0, fmt.Errorf("invalid time %q", s)
	}
	var n [3]int64
	for i, p := range ts {
		v, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid time %q", s)
		}
		n[i] = v
	}
	u, err := fracMicros(frac)
	if err != nil {
		return 0, fmt.Errorf("invalid time %q", s)
	}
	t := ((n[0]*60+n[1])*60+n[2])*1_000_000 + u
	if neg {
		t = -t
	}
	return t, nil
}

// CrossEngineSameOrder (MySQL→PG): từng cột PK thuộc cùng lớp thứ tự độc lập engine — số (INT/UNSIGNED/
// DECIMAL ↔ int/numeric), thời gian, VARBINARY ↔ bytea. Chuỗi có collation không được nhận (PAD SPACE /
// collation MySQL khác "C" của PG).
func CrossEngineSameOrder(src, dst *PKCodec) error {
	class := func(k PKKind) string {
		switch k {
		case PKInt, PKUint, PKDecimal:
			return "number"
		case PKDateTime:
			return "datetime"
		case PKBytes:
			return "bytes"
		}
		return ""
	}
	if len(src.Cols) != len(dst.Cols) {
		return fmt.Errorf("primary key has %d columns on the source and %d on the target", len(src.Cols), len(dst.Cols))
	}
	for i := range src.Cols {
		a, b := src.Cols[i], dst.Cols[i]
		ca := class(a.Kind)
		if a.Kind == PKBytes && a.Collation != "" {
			ca = "" // chuỗi có collation (kể cả _0900_bin): không đối chiếu với text "C" của PG
		}
		if a.Name != b.Name || ca == "" || ca != class(b.Kind) {
			return fmt.Errorf("primary key column %s (%s → %s %s) has no engine-independent order", a.Name, a.Kind, b.Kind, b.SQLType)
		}
	}
	return nil
}

// PadSpaceWeightKey chuyển WEIGHT_STRING w của một collation PAD SPACE thành khóa so byte đúng thứ tự và
// phép bằng của DB (review N04). sp là weight của một space (độ dài = độ rộng một đơn vị weight).
//
// DB so hai mảng weight như thể mảng ngắn hơn được nối thêm vô hạn đơn vị sp. Viết weight thành các đoạn
// (L đơn vị sp liên tiếp, rồi một đơn vị u ≠ sp); hết chuỗi = một đoạn "L = ∞". So hai đoạn đầu tiên khác
// nhau (L1,u1) và (L2,u2):
//   - L1 = L2: so u1 với u2;
//   - L1 < L2: ở vị trí L1, bên 1 là u1, bên 2 là sp ⇒ kết quả = so u1 với sp (u1 ≠ sp);
//   - một bên hết chuỗi (L = ∞): kết quả = so sp với đơn vị kế tiếp của bên kia.
//
// Mã hóa mỗi đoạn: u < sp → 0x01, L tăng dần (uint32 BE), u; u > sp → 0x03, L GIẢM dần (^L), u; hết chuỗi
// → 0x02. Kiểm: L1<L2, u1<sp: 0x01·L1 < 0x01·L2 và 0x01 < 0x03 ⇒ bên 1 nhỏ hơn ✓; u1>sp: 0x03·^L1 > 0x03·^L2
// và 0x03 > 0x01 ⇒ bên 1 lớn hơn ✓; hết chuỗi 0x02 nằm giữa ✓; L bằng nhau → cùng tiền tố, so u ✓. Space
// cuối chuỗi biến mất (đoạn cuối chỉ còn "hết chuỗi") nên 'a' = 'a ' ✓.
func PadSpaceWeightKey(w, sp []byte) ([]byte, error) {
	n := len(sp)
	if n == 0 || len(w)%n != 0 {
		return nil, fmt.Errorf("weight string of %d bytes is not a whole number of %d-byte units", len(w), n)
	}
	out := make([]byte, 0, len(w)+len(w)/n*5+1)
	var run uint32
	for i := 0; i < len(w); i += n {
		u := w[i : i+n]
		c := bytes.Compare(u, sp)
		if c == 0 {
			run++
			continue
		}
		l := run
		cls := byte(0x01)
		if c > 0 {
			cls, l = 0x03, ^run
		}
		out = append(out, cls, byte(l>>24), byte(l>>16), byte(l>>8), byte(l))
		out = append(out, u...)
		run = 0
	}
	return append(out, 0x02), nil
}
