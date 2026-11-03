// Package typemap thực hiện ánh xạ kiểu dữ liệu, collation và chuyển đổi giá trị giữa MySQL và PostgreSQL
// theo đặc tả spec/02-specs.md §8, §11, §12.
package typemap

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// spec: 02 §8 (Kiểu dữ liệu MySQL→PG: zero date, BIGINT UNSIGNED vượt int8, ENUM/SET có policy riêng,
// validate quét giá trị thực tế, time_zone session cố định, TIMESTAMP→timestamptz, DATETIME→timestamp)

// MySQLColumn chứa metadata của một cột trong MySQL.
type MySQLColumn struct {
	Name       string
	DataType   string // vd: "int", "varchar", "datetime", "enum"
	ColumnType string // vd: "int(10) unsigned", "varchar(255)", "enum('a','b')"
	Unsigned   bool
	Charset    string
	Collation  string
	Nullable   bool
	EnumValues []string // các giá trị cho phép nếu là enum hoặc set
}

// ColumnMapping biểu diễn ánh xạ từ cột nguồn MySQL sang cột đích PostgreSQL cùng hàm chuyển đổi giá trị.
type ColumnMapping struct {
	Source     MySQLColumn
	TargetType string
	Convert    func(model.Value) (model.Value, error)
	Policy     string
}

// Options chứa cấu hình chính sách ánh xạ kiểu dữ liệu.
type Options struct {
	// ZeroDatePolicy: "reject" (mặc định, trả lỗi exit 3), "null" (gán NULL), "epoch" (1970-01-01)
	ZeroDatePolicy string

	// BigIntUnsignedPolicy: "numeric" (mặc định -> numeric(20,0)), "int8-checked" (từ chối giá trị > 2^63-1)
	BigIntUnsignedPolicy string

	// EnumPolicy: "check" (mặc định -> text + CHECK), "type" (enum type riêng)
	EnumPolicy string

	// SetPolicy: "text_array" (mặc định -> text[])
	SetPolicy string

	// TimePolicy: "time" (mặc định -> time), "interval" (-> interval)
	TimePolicy string

	// StringPolicy: "preserve" (mặc định -> varchar(n)), "text" (-> text)
	StringPolicy string

	// BitPolicy: "bit varying" (mặc định) | "bytea"
	BitPolicy string

	// ScanLimit: giới hạn số row quét thực tế khi validate (mặc định 1000)
	ScanLimit int
}

// DefaultOptions trả về cấu hình mặc định theo đặc tả spec/02 §8.
func DefaultOptions() Options {
	return Options{
		ZeroDatePolicy:       "reject",
		BigIntUnsignedPolicy: "numeric",
		EnumPolicy:           "check",
		SetPolicy:            "text_array",
		TimePolicy:           "time",
		StringPolicy:         "preserve",
		BitPolicy:            "bit varying",
		ScanLimit:            1000,
	}
}

var (
	paramRegex = regexp.MustCompile(`\(([^)]+)\)`)
)

// extractTypeParams trích xuất tham số trong ngoặc của ColumnType, ví dụ "decimal(10,2)" -> "(10,2)".
func extractTypeParams(colType string) string {
	m := paramRegex.FindString(colType)
	return m
}

// ParseEnumValues phân tích danh sách giá trị từ định nghĩa enum(...) hoặc set(...).
func ParseEnumValues(colType string) []string {
	idx := strings.Index(colType, "(")
	if idx == -1 {
		return nil
	}
	endIdx := strings.LastIndex(colType, ")")
	if endIdx <= idx {
		return nil
	}
	content := colType[idx+1 : endIdx]

	var values []string
	var cur strings.Builder
	inQuote := false
	escaped := false

	for i := 0; i < len(content); i++ {
		ch := content[i]
		if escaped {
			cur.WriteByte(ch)
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == '\'' {
			if inQuote && i+1 < len(content) && content[i+1] == '\'' {
				// Hai dấu nháy đơn liên tiếp biểu thị nháy đơn trong chuỗi
				cur.WriteByte('\'')
				i++
				continue
			}
			inQuote = !inQuote
			continue
		}
		if ch == ',' && !inQuote {
			values = append(values, cur.String())
			cur.Reset()
			continue
		}
		if inQuote {
			cur.WriteByte(ch)
		}
	}
	if cur.Len() > 0 || len(values) > 0 {
		values = append(values, cur.String())
	}
	return values
}

// toInt64 chuyển đổi linh hoạt các kiểu số nguyên sang int64.
func toInt64(v model.Value) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return 0, fmt.Errorf("uint %d overflows int64", x)
		}
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, fmt.Errorf("uint64 %d overflows int64", x)
		}
		return int64(x), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(x), 10, 64)
	case []byte:
		return strconv.ParseInt(strings.TrimSpace(string(x)), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

// toUint64 chuyển đổi linh hoạt các kiểu số nguyên sang uint64.
func toUint64(v model.Value) (uint64, error) {
	switch x := v.(type) {
	case uint64:
		return x, nil
	case uint:
		return uint64(x), nil
	case uint8:
		return uint64(x), nil
	case uint16:
		return uint64(x), nil
	case uint32:
		return uint64(x), nil
	case int:
		if x < 0 {
			return 0, fmt.Errorf("negative int %d cannot convert to uint64", x)
		}
		return uint64(x), nil
	case int8:
		if x < 0 {
			return 0, fmt.Errorf("negative int8 %d cannot convert to uint64", x)
		}
		return uint64(x), nil
	case int16:
		if x < 0 {
			return 0, fmt.Errorf("negative int16 %d cannot convert to uint64", x)
		}
		return uint64(x), nil
	case int32:
		if x < 0 {
			return 0, fmt.Errorf("negative int32 %d cannot convert to uint64", x)
		}
		return uint64(x), nil
	case int64:
		if x < 0 {
			return 0, fmt.Errorf("negative int64 %d cannot convert to uint64", x)
		}
		return uint64(x), nil
	case string:
		return strconv.ParseUint(strings.TrimSpace(x), 10, 64)
	case []byte:
		return strconv.ParseUint(strings.TrimSpace(string(x)), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}

// isZeroDate kiểm tra xem giá trị ngày tháng có phải là zero date ('0000-00-00', hoặc ngày/tháng = 0) hay không.
func isZeroDate(v model.Value) (bool, string) {
	if v == nil {
		return false, ""
	}
	switch x := v.(type) {
	case string:
		s := strings.TrimSpace(x)
		if s == "0000-00-00" || s == "0000-00-00 00:00:00" || strings.HasPrefix(s, "0000-00-00") ||
			strings.Contains(s, "-00-") || strings.HasSuffix(s, "-00") || strings.Contains(s, "-00 ") {
			return true, s
		}
	case []byte:
		s := strings.TrimSpace(string(x))
		if s == "0000-00-00" || s == "0000-00-00 00:00:00" || strings.HasPrefix(s, "0000-00-00") ||
			strings.Contains(s, "-00-") || strings.HasSuffix(s, "-00") || strings.Contains(s, "-00 ") {
			return true, s
		}
	case time.Time:
		if x.IsZero() || x.Year() == 0 || x.Month() == 0 || x.Day() == 0 {
			return true, x.Format("2006-01-02 15:04:05")
		}
	}
	return false, ""
}

// handleZeroDate áp dụng chính sách zero date theo Options (reject | null | epoch).
func handleZeroDate(dateStr string, policy string) (model.Value, error) {
	switch policy {
	case "null":
		return nil, nil
	case "epoch":
		return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), nil
	default: // "reject"
		return nil, exitcode.New(
			exitcode.ErrTypeUnmappable,
			exitcode.Rejected,
			fmt.Sprintf("zero date %q rejected by policy (spec: 02 §8)", dateStr),
			nil,
		)
	}
}

// MapMySQLColumn tạo ánh xạ cột từ MySQL sang PostgreSQL theo đặc tả spec/02 §8.
func MapMySQLColumn(col MySQLColumn, opts Options) (ColumnMapping, error) {
	// Chuẩn hóa cấu hình mặc định nếu trống
	if opts.ZeroDatePolicy == "" {
		opts.ZeroDatePolicy = "reject"
	}
	if opts.BigIntUnsignedPolicy == "" {
		opts.BigIntUnsignedPolicy = "numeric"
	}
	if opts.EnumPolicy == "" {
		opts.EnumPolicy = "check"
	}
	if opts.SetPolicy == "" {
		opts.SetPolicy = "text_array"
	}
	if opts.TimePolicy == "" {
		opts.TimePolicy = "time"
	}
	if opts.StringPolicy == "" {
		opts.StringPolicy = "preserve"
	}
	if opts.BitPolicy == "" {
		opts.BitPolicy = "bit varying"
	}

	dt := strings.ToLower(strings.TrimSpace(col.DataType))
	unsigned := col.Unsigned || strings.Contains(strings.ToLower(col.ColumnType), "unsigned")
	col.Unsigned = unsigned

	// Tự động phân tích EnumValues nếu chưa có sẵn
	if (dt == "enum" || dt == "set") && len(col.EnumValues) == 0 {
		col.EnumValues = ParseEnumValues(col.ColumnType)
	}

	params := extractTypeParams(col.ColumnType)

	mapping := ColumnMapping{
		Source: col,
	}

	switch dt {
	// -------------------------------------------------------------------------
	// 1. Số nguyên (tinyint, smallint, mediumint, int, bigint)
	// -------------------------------------------------------------------------
	case "tinyint":
		// MySQL tinyint: signed -128..127, unsigned 0..255.
		// PG không có int1, dùng smallint (int2: -32768..32767).
		mapping.TargetType = "smallint"
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			if unsigned {
				u, err := toUint64(v)
				if err != nil {
					return nil, err
				}
				if u > 255 {
					return nil, fmt.Errorf("value %d exceeds tinyint unsigned max (255)", u)
				}
				return int16(u), nil
			}
			i, err := toInt64(v)
			if err != nil {
				return nil, err
			}
			if i < -128 || i > 127 {
				return nil, fmt.Errorf("value %d exceeds tinyint signed range (-128..127)", i)
			}
			return int16(i), nil
		}

	case "smallint":
		if unsigned {
			// MySQL smallint unsigned: 0..65535 -> PG integer (int4)
			mapping.TargetType = "integer"
			mapping.Convert = func(v model.Value) (model.Value, error) {
				if v == nil {
					return nil, nil
				}
				u, err := toUint64(v)
				if err != nil {
					return nil, err
				}
				if u > 65535 {
					return nil, fmt.Errorf("value %d exceeds smallint unsigned max (65535)", u)
				}
				return int32(u), nil
			}
		} else {
			// MySQL smallint signed: -32768..32767 -> PG smallint (int2)
			mapping.TargetType = "smallint"
			mapping.Convert = func(v model.Value) (model.Value, error) {
				if v == nil {
					return nil, nil
				}
				i, err := toInt64(v)
				if err != nil {
					return nil, err
				}
				if i < -32768 || i > 32767 {
					return nil, fmt.Errorf("value %d exceeds smallint range (-32768..32767)", i)
				}
				return int16(i), nil
			}
		}

	case "mediumint":
		// MySQL mediumint: signed -8388608..8388607, unsigned 0..16777215 -> PG integer (int4)
		mapping.TargetType = "integer"
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			if unsigned {
				u, err := toUint64(v)
				if err != nil {
					return nil, err
				}
				if u > 16777215 {
					return nil, fmt.Errorf("value %d exceeds mediumint unsigned max (16777215)", u)
				}
				return int32(u), nil
			}
			i, err := toInt64(v)
			if err != nil {
				return nil, err
			}
			if i < -8388608 || i > 8388607 {
				return nil, fmt.Errorf("value %d exceeds mediumint signed range (-8388608..8388607)", i)
			}
			return int32(i), nil
		}

	case "int", "integer":
		if unsigned {
			// MySQL int unsigned: 0..4294967295 -> PG bigint (int8)
			mapping.TargetType = "bigint"
			mapping.Convert = func(v model.Value) (model.Value, error) {
				if v == nil {
					return nil, nil
				}
				u, err := toUint64(v)
				if err != nil {
					return nil, err
				}
				if u > 4294967295 {
					return nil, fmt.Errorf("value %d exceeds int unsigned max (4294967295)", u)
				}
				return int64(u), nil
			}
		} else {
			// MySQL int signed: -2147483648..2147483647 -> PG integer (int4)
			mapping.TargetType = "integer"
			mapping.Convert = func(v model.Value) (model.Value, error) {
				if v == nil {
					return nil, nil
				}
				i, err := toInt64(v)
				if err != nil {
					return nil, err
				}
				if i < -2147483648 || i > 2147483647 {
					return nil, fmt.Errorf("value %d exceeds int signed range (-2147483648..2147483647)", i)
				}
				return int32(i), nil
			}
		}

	case "bigint":
		if unsigned {
			// spec: 02 §8: unsigned bigint → numeric(20,0) mặc định; tùy chọn "int8-checked" từ chối giá trị > 2^63-1
			mapping.Policy = opts.BigIntUnsignedPolicy
			if opts.BigIntUnsignedPolicy == "int8-checked" {
				mapping.TargetType = "bigint"
				mapping.Convert = func(v model.Value) (model.Value, error) {
					if v == nil {
						return nil, nil
					}
					u, err := toUint64(v)
					if err != nil {
						return nil, err
					}
					if u > math.MaxInt64 {
						return nil, exitcode.New(
							exitcode.ErrTypeUnmappable,
							exitcode.Rejected,
							fmt.Sprintf("unsigned bigint %d exceeds int8 max (9223372036854775807) (spec: 02 §8, policy=int8-checked)", u),
							nil,
						)
					}
					return int64(u), nil
				}
			} else {
				// Mặc định: numeric(20,0)
				mapping.TargetType = "numeric(20,0)"
				mapping.Convert = func(v model.Value) (model.Value, error) {
					if v == nil {
						return nil, nil
					}
					u, err := toUint64(v)
					if err != nil {
						return nil, err
					}
					return strconv.FormatUint(u, 10), nil
				}
			}
		} else {
			// MySQL bigint signed: -> PG bigint
			mapping.TargetType = "bigint"
			mapping.Convert = func(v model.Value) (model.Value, error) {
				if v == nil {
					return nil, nil
				}
				return toInt64(v)
			}
		}

	// -------------------------------------------------------------------------
	// 2. Số thực & Số thập phân chính xác
	// -------------------------------------------------------------------------
	case "decimal", "numeric", "dec", "fixed":
		// decimal(p,s) -> numeric(p,s)
		if params != "" {
			mapping.TargetType = "numeric" + params
		} else {
			mapping.TargetType = "numeric"
		}
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case string:
				return strings.TrimSpace(x), nil
			case []byte:
				return strings.TrimSpace(string(x)), nil
			case float64:
				return strconv.FormatFloat(x, 'f', -1, 64), nil
			case float32:
				return strconv.FormatFloat(float64(x), 'f', -1, 32), nil
			case int, int64, int32, int16, int8:
				return fmt.Sprintf("%d", x), nil
			case uint, uint64, uint32, uint16, uint8:
				return fmt.Sprintf("%d", x), nil
			default:
				return fmt.Sprintf("%v", x), nil
			}
		}

	case "float":
		// float -> real (4-byte)
		mapping.TargetType = "real"
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case float32:
				return x, nil
			case float64:
				return float32(x), nil
			case string:
				f, err := strconv.ParseFloat(strings.TrimSpace(x), 32)
				return float32(f), err
			case []byte:
				f, err := strconv.ParseFloat(strings.TrimSpace(string(x)), 32)
				return float32(f), err
			default:
				i, err := toInt64(v)
				if err != nil {
					return nil, fmt.Errorf("cannot convert %T to float32: %w", v, err)
				}
				return float32(i), nil
			}
		}

	case "double", "double precision", "real":
		// double -> double precision (8-byte)
		mapping.TargetType = "double precision"
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case float64:
				return x, nil
			case float32:
				return float64(x), nil
			case string:
				return strconv.ParseFloat(strings.TrimSpace(x), 64)
			case []byte:
				return strconv.ParseFloat(strings.TrimSpace(string(x)), 64)
			default:
				i, err := toInt64(v)
				if err != nil {
					return nil, fmt.Errorf("cannot convert %T to float64: %w", v, err)
				}
				return float64(i), nil
			}
		}

	// -------------------------------------------------------------------------
	// 3. Bit
	// -------------------------------------------------------------------------
	case "bit":
		// bit(n) -> bit varying(n) hoặc bytea theo BitPolicy
		mapping.Policy = opts.BitPolicy
		if opts.BitPolicy == "bytea" {
			mapping.TargetType = "bytea"
			mapping.Convert = func(v model.Value) (model.Value, error) {
				if v == nil {
					return nil, nil
				}
				switch x := v.(type) {
				case []byte:
					return x, nil
				case string:
					return []byte(x), nil
				default:
					u, err := toUint64(v)
					if err != nil {
						return nil, err
					}
					return []byte(strconv.FormatUint(u, 2)), nil
				}
			}
		} else {
			if params != "" {
				mapping.TargetType = "bit varying" + params
			} else {
				mapping.TargetType = "bit varying"
			}
			mapping.Convert = func(v model.Value) (model.Value, error) {
				if v == nil {
					return nil, nil
				}
				switch x := v.(type) {
				case string:
					return strings.TrimSpace(x), nil
				case []byte:
					// Nếu là chuỗi byte ASCII '0'/'1'
					s := strings.TrimSpace(string(x))
					isBinaryStr := true
					for _, c := range s {
						if c != '0' && c != '1' {
							isBinaryStr = false
							break
						}
					}
					if isBinaryStr && len(s) > 0 {
						return s, nil
					}
					// Hoặc byte thô nhị phân -> chuyển sang chuỗi bit
					var sb strings.Builder
					for _, b := range x {
						sb.WriteString(fmt.Sprintf("%08b", b))
					}
					return strings.TrimLeft(sb.String(), "0"), nil
				default:
					u, err := toUint64(v)
					if err != nil {
						return nil, err
					}
					return strconv.FormatUint(u, 2), nil
				}
			}
		}

	// -------------------------------------------------------------------------
	// 4. Chuỗi ký tự (char, varchar, text)
	// -------------------------------------------------------------------------
	case "char":
		if opts.StringPolicy == "text" {
			mapping.TargetType = "text"
		} else if params != "" {
			mapping.TargetType = "char" + params
		} else {
			mapping.TargetType = "text"
		}
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case string:
				return x, nil
			case []byte:
				return string(x), nil
			default:
				return fmt.Sprintf("%v", x), nil
			}
		}

	case "varchar":
		if opts.StringPolicy == "text" {
			mapping.TargetType = "text"
		} else if params != "" {
			mapping.TargetType = "varchar" + params
		} else {
			mapping.TargetType = "text"
		}
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case string:
				return x, nil
			case []byte:
				return string(x), nil
			default:
				return fmt.Sprintf("%v", x), nil
			}
		}

	case "tinytext", "text", "mediumtext", "longtext":
		mapping.TargetType = "text"
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case string:
				return x, nil
			case []byte:
				return string(x), nil
			default:
				return fmt.Sprintf("%v", x), nil
			}
		}

	// -------------------------------------------------------------------------
	// 5. Chuỗi nhị phân & BLOB (binary, varbinary, blob)
	// -------------------------------------------------------------------------
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		mapping.TargetType = "bytea"
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case []byte:
				return x, nil
			case string:
				return []byte(x), nil
			default:
				return nil, fmt.Errorf("cannot convert %T to bytea", v)
			}
		}

	// -------------------------------------------------------------------------
	// 6. Ngày tháng & Thời gian (date, datetime, timestamp, time, year)
	// -------------------------------------------------------------------------
	case "date":
		mapping.TargetType = "date"
		mapping.Policy = opts.ZeroDatePolicy
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			if isZero, s := isZeroDate(v); isZero {
				return handleZeroDate(s, opts.ZeroDatePolicy)
			}
			switch x := v.(type) {
			case time.Time:
				return time.Date(x.Year(), x.Month(), x.Day(), 0, 0, 0, 0, time.UTC), nil
			case string:
				s := strings.TrimSpace(x)
				// Cắt bỏ phần thời gian nếu có
				if idx := strings.IndexAny(s, " T"); idx != -1 {
					s = s[:idx]
				}
				t, err := time.Parse("2006-01-02", s)
				if err != nil {
					return nil, fmt.Errorf("parse date %q: %w", s, err)
				}
				return t, nil
			case []byte:
				s := strings.TrimSpace(string(x))
				if idx := strings.IndexAny(s, " T"); idx != -1 {
					s = s[:idx]
				}
				t, err := time.Parse("2006-01-02", s)
				if err != nil {
					return nil, fmt.Errorf("parse date %q: %w", s, err)
				}
				return t, nil
			default:
				return nil, fmt.Errorf("cannot convert %T to date", v)
			}
		}

	case "datetime":
		// spec: 02 §8: DATETIME→timestamp (without time zone)
		if params != "" {
			mapping.TargetType = "timestamp" + params
		} else {
			mapping.TargetType = "timestamp"
		}
		mapping.Policy = opts.ZeroDatePolicy
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			if isZero, s := isZeroDate(v); isZero {
				return handleZeroDate(s, opts.ZeroDatePolicy)
			}
			switch x := v.(type) {
			case time.Time:
				// DATETIME giữ nguyên wall-clock time
				return time.Date(x.Year(), x.Month(), x.Day(), x.Hour(), x.Minute(), x.Second(), x.Nanosecond(), time.UTC), nil
			case string:
				return parseDateTimeString(strings.TrimSpace(x))
			case []byte:
				return parseDateTimeString(strings.TrimSpace(string(x)))
			default:
				return nil, fmt.Errorf("cannot convert %T to timestamp", v)
			}
		}

	case "timestamp":
		// spec: 02 §8: TIMESTAMP→timestamptz.
		// "Múi giờ: chuyển đổi TIMESTAMP giả định session MySQL time_zone='+00:00' (đúng như pipeline đặt), trả time.Time UTC."
		if params != "" {
			mapping.TargetType = "timestamptz" + params
		} else {
			mapping.TargetType = "timestamptz"
		}
		mapping.Policy = opts.ZeroDatePolicy
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			if isZero, s := isZeroDate(v); isZero {
				return handleZeroDate(s, opts.ZeroDatePolicy)
			}
			switch x := v.(type) {
			case time.Time:
				return x.UTC(), nil
			case string:
				t, err := parseDateTimeString(strings.TrimSpace(x))
				if err != nil {
					return nil, err
				}
				return t.UTC(), nil
			case []byte:
				t, err := parseDateTimeString(strings.TrimSpace(string(x)))
				if err != nil {
					return nil, err
				}
				return t.UTC(), nil
			default:
				return nil, fmt.Errorf("cannot convert %T to timestamptz", v)
			}
		}

	case "time":
		// spec: 02 §8: time→time/interval (giải thích: MySQL TIME nhận từ -838:59:59 đến +838:59:59,
		// PG time chỉ 00:00:00-24:00:00; khi có giá trị âm hoặc vượt quá 24h cần dùng interval).
		mapping.Policy = opts.TimePolicy
		if opts.TimePolicy == "interval" {
			mapping.TargetType = "interval"
		} else {
			if params != "" {
				mapping.TargetType = "time" + params
			} else {
				mapping.TargetType = "time"
			}
		}
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case string:
				return strings.TrimSpace(x), nil
			case []byte:
				return strings.TrimSpace(string(x)), nil
			case time.Duration:
				// Chuyển đổi duration thành chuỗi định dạng HH:MM:SS
				totalSec := int64(x.Seconds())
				sign := ""
				if totalSec < 0 {
					sign = "-"
					totalSec = -totalSec
				}
				h := totalSec / 3600
				m := (totalSec % 3600) / 60
				s := totalSec % 60
				return fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, s), nil
			default:
				return fmt.Sprintf("%v", x), nil
			}
		}

	case "year":
		// MySQL year (1901..2155) -> PG smallint
		mapping.TargetType = "smallint"
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			i, err := toInt64(v)
			if err != nil {
				return nil, err
			}
			return int16(i), nil
		}

	// -------------------------------------------------------------------------
	// 7. JSON
	// -------------------------------------------------------------------------
	case "json":
		mapping.TargetType = "jsonb"
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case string:
				s := strings.TrimSpace(x)
				if !json.Valid([]byte(s)) {
					return nil, fmt.Errorf("invalid json payload: %q", s)
				}
				return s, nil
			case []byte:
				if !json.Valid(x) {
					return nil, fmt.Errorf("invalid json payload: %q", string(x))
				}
				return string(x), nil
			default:
				b, err := json.Marshal(x)
				if err != nil {
					return nil, fmt.Errorf("marshal json: %w", err)
				}
				return string(b), nil
			}
		}

	// -------------------------------------------------------------------------
	// 8. ENUM & SET
	// -------------------------------------------------------------------------
	case "enum":
		// spec: 02 §8: enum→text + CHECK (policy) hoặc enum type
		mapping.Policy = opts.EnumPolicy
		if opts.EnumPolicy == "type" {
			mapping.TargetType = col.Name + "_enum"
		} else {
			mapping.TargetType = "text"
		}
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			var str string
			switch x := v.(type) {
			case string:
				str = strings.TrimSpace(x)
			case []byte:
				str = strings.TrimSpace(string(x))
			default:
				str = fmt.Sprintf("%v", x)
			}
			// Kiểm tra giá trị có nằm trong tập enum hay không
			if len(col.EnumValues) > 0 {
				found := false
				for _, ev := range col.EnumValues {
					if ev == str {
						found = true
						break
					}
				}
				if !found {
					return nil, exitcode.New(
						exitcode.ErrTypeUnmappable,
						exitcode.Rejected,
						fmt.Sprintf("enum value %q not in allowed values %v (spec: 02 §8)", str, col.EnumValues),
						nil,
					)
				}
			}
			return str, nil
		}

	case "set":
		// spec: 02 §8: set→text[] (policy)
		mapping.TargetType = "text[]"
		mapping.Policy = opts.SetPolicy
		mapping.Convert = func(v model.Value) (model.Value, error) {
			if v == nil {
				return nil, nil
			}
			switch x := v.(type) {
			case []string:
				return x, nil
			case string:
				s := strings.TrimSpace(x)
				if s == "" {
					return []string{}, nil
				}
				parts := strings.Split(s, ",")
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				return parts, nil
			case []byte:
				s := strings.TrimSpace(string(x))
				if s == "" {
					return []string{}, nil
				}
				parts := strings.Split(s, ",")
				for i := range parts {
					parts[i] = strings.TrimSpace(parts[i])
				}
				return parts, nil
			default:
				return nil, fmt.Errorf("cannot convert %T to text[]", v)
			}
		}

	// -------------------------------------------------------------------------
	// 9. Không thể ánh xạ (geometry, spatial)
	// -------------------------------------------------------------------------
	case "geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection":
		// spec: 02 §8: geometry → type_unmappable
		return ColumnMapping{}, exitcode.New(
			exitcode.ErrTypeUnmappable,
			exitcode.Rejected,
			fmt.Sprintf("column %s has unmappable type %q: geometry types are not supported (spec: 02 §8, §12)", col.Name, col.DataType),
			nil,
		)

	default:
		// Bất kỳ kiểu lạ nào không nhận diện được đều trả về type_unmappable
		return ColumnMapping{}, exitcode.New(
			exitcode.ErrTypeUnmappable,
			exitcode.Rejected,
			fmt.Sprintf("column %s has unknown/unmappable type %q (spec: 02 §8, §12)", col.Name, col.DataType),
			nil,
		)
	}

	return mapping, nil
}

// parseDateTimeString phân tích chuỗi thời gian hỗ trợ nhiều định dạng MySQL phổ biến.
func parseDateTimeString(s string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999",
		"2006-01-02T15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, f := range formats {
		if t, err := time.ParseInLocation(f, s, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse datetime string %q", s)
}
