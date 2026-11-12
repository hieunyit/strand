package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unicode/utf8"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"golang.org/x/text/encoding/charmap"
)

// Giá trị cột chuỗi trong binlog là byte theo CHARSET CỦA CỘT, còn writer gửi qua kết nối utf8mb4 và
// snapshot đọc bằng SELECT (đã chuyển sang charset kết nối). Cột latin1 có ký tự ngoài ASCII vì vậy bị ghi
// sai (1366 Incorrect string value, hoặc '?' khi sql_mode không strict). convertCharsets đưa giá trị chuỗi
// về UTF-8 theo collation của cột trong TABLE_MAP (binlog_row_metadata=FULL):
//   - utf8mb4 / utf8mb3 / ascii / binary: giữ nguyên;
//   - latin1 (= cp1252 của MySQL; 0x81 0x8D 0x8F 0x90 0x9D giữ thành U+0081…): chuyển sang UTF-8;
//   - charset khác: giá trị chỉ có ASCII giữ nguyên (ASCII trùng nhau ở các charset đó); có byte ≥ 0x80 thì
//     lỗi (dừng thay vì ghi sai). ucs2/utf16/utf32 không tương thích ASCII: lỗi với mọi giá trị khác rỗng.

var mysqlLatin1Undefined = map[byte]bool{0x81: true, 0x8D: true, 0x8F: true, 0x90: true, 0x9D: true}

func decodeMySQLLatin1(b []byte) string {
	var sb strings.Builder
	sb.Grow(len(b) + len(b)/4)
	for _, c := range b {
		switch {
		case c < 0x80:
			sb.WriteByte(c)
		case mysqlLatin1Undefined[c]:
			sb.WriteRune(rune(c))
		default:
			sb.WriteRune(charmap.Windows1252.DecodeByte(c))
		}
	}
	return sb.String()
}

func hasHighByte(b []byte) bool {
	for _, c := range b {
		if c >= 0x80 {
			return true
		}
	}
	return false
}

// convertCharsets chuyển (tại chỗ) giá trị các cột chuỗi của rows sang UTF-8. charsets: collation id ->
// charset (information_schema.COLLATIONS của source); nil = không chuyển (giữ hành vi cũ).
func convertCharsets(tm *replication.TableMapEvent, rows [][]any, charsets map[uint64]string) error {
	if charsets == nil || tm == nil {
		return nil
	}
	cm := tm.CollationMap()
	if len(cm) == 0 {
		return nil
	}
	byIdx := make(map[int]string, len(cm))
	for idx, coll := range cm {
		cs, ok := charsets[coll]
		if !ok {
			return fmt.Errorf("%s.%s: unknown collation id %d for column %d", tm.Schema, tm.Table, coll, idx)
		}
		byIdx[idx] = cs
	}
	return convertCharsetsBy(tm, rows, byIdx)
}

// convertCharsetsBy: lõi của convertCharsets, charset theo chỉ số cột (từ collation của TABLE_MAP hoặc từ
// information_schema khi nguồn MySQL 5.7 không có metadata).
func convertCharsetsBy(tm *replication.TableMapEvent, rows [][]any, byIdx map[int]string) error {
	names := tm.ColumnNameString()
	for idx, cs := range byIdx {
		switch cs {
		case "utf8mb4", "utf8mb3", "utf8", "ascii", "binary":
			continue
		}
		col := fmt.Sprint(idx)
		if idx < len(names) {
			col = names[idx]
		}
		for _, row := range rows {
			if idx >= len(row) || row[idx] == nil {
				continue
			}
			var raw []byte
			switch v := row[idx].(type) {
			case string:
				raw = []byte(v)
			case []byte:
				raw = v
			default:
				continue
			}
			var out string
			switch {
			case cs == "latin1":
				out = decodeMySQLLatin1(raw)
			case cs == "ucs2" || cs == "utf16" || cs == "utf16le" || cs == "utf32":
				if len(raw) == 0 {
					continue
				}
				return fmt.Errorf("%s.%s column %s: character set %s is not supported by the binlog reader", tm.Schema, tm.Table, col, cs)
			case !hasHighByte(raw):
				continue
			default:
				return fmt.Errorf("%s.%s column %s: character set %s with non-ASCII data is not supported by the binlog reader (values would be written wrongly); convert the column to utf8mb4 or latin1",
					tm.Schema, tm.Table, col, cs)
			}
			if !utf8.ValidString(out) {
				return fmt.Errorf("%s.%s column %s: invalid %s value", tm.Schema, tm.Table, col, cs)
			}
			// Luôn trả string: TEXT (giá trị binlog là []byte) mà trả []byte thì writer MySQL gửi dạng _binary,
			// server lưu nguyên byte UTF-8 vào cột latin1 (ký tự bị mã hóa hai lần).
			row[idx] = out
		}
	}
	return nil
}

// binaryCollationID: collation "binary" (charset binary) của MySQL.
const binaryCollationID = 63

// fixedStringLen: độ dài khai báo (byte) của cột MYSQL_TYPE_STRING thật (CHAR/BINARY, không phải
// ENUM/SET) — cùng cách giải mã meta với go-mysql RowsEvent.decodeValue. ok=false nếu không phải.
func fixedStringLen(tm *replication.TableMapEvent, i int) (int, bool) {
	if i >= len(tm.ColumnType) || tm.ColumnType[i] != gomysql.MYSQL_TYPE_STRING {
		return 0, false
	}
	meta := tm.ColumnMeta[i]
	tp, length := byte(gomysql.MYSQL_TYPE_STRING), int(meta)
	if meta >= 256 {
		b0, b1 := uint8(meta>>8), uint8(meta&0xFF)
		if b0&0x30 != 0x30 {
			length = int(uint16(b1) | (uint16((b0&0x30)^0x30) << 4))
			tp = b0 | 0x30
		} else {
			length = int(meta & 0xFF)
			tp = b0
		}
	}
	return length, tp == gomysql.MYSQL_TYPE_STRING
}

// padBinary đệm 0x00 tới độ dài khai báo cho cột BINARY(n) (review F1): MySQL lưu và SELECT trả đủ n
// byte, nhưng binlog ROW cắt các byte đệm cuối (Field_string::pack bỏ pad char, với charset binary là
// 0x00). Không đệm thì cùng một giá trị cho hai byte khác nhau ở stream và snapshot: khóa range-state
// lệch, đọc lại theo khóa trượt, MySQL→PG ghi bytea thiếu byte. Cần collation của cột (metadata FULL);
// binlog_row_metadata=MINIMAL đã ghi charset của cột (COLUMN_CHARSET), nên luôn có với MySQL 8.0+.
func padBinary(tm *replication.TableMapEvent, rows [][]any) {
	if tm == nil {
		return
	}
	var cols []int
	for idx, coll := range tm.CollationMap() {
		if coll == binaryCollationID {
			cols = append(cols, idx)
		}
	}
	padBinaryCols(tm, rows, cols)
}

// padBinaryCols đệm 0x00 cho các cột BINARY(n) có chỉ số trong cols.
func padBinaryCols(tm *replication.TableMapEvent, rows [][]any, cols []int) {
	for _, idx := range cols {
		n, ok := fixedStringLen(tm, idx)
		if !ok {
			continue
		}
		for _, row := range rows {
			if idx >= len(row) || row[idx] == nil {
				continue
			}
			switch v := row[idx].(type) {
			case string:
				if len(v) < n {
					row[idx] = v + string(make([]byte, n-len(v)))
				}
			case []byte:
				if len(v) < n {
					row[idx] = append(append(make([]byte, 0, n), v...), make([]byte, n-len(v))...)
				}
			}
		}
	}
}

// NormalizeRows đưa giá trị row của binlog về đúng dạng SELECT trả: đệm BINARY(n), rồi chuyển chuỗi
// sang UTF-8 (convertCharsets). Dùng cho mọi reader đọc binlog MySQL (trực tiếp và archive).
func NormalizeRows(tm *replication.TableMapEvent, rows [][]any, charsets map[uint64]string) error {
	padBinary(tm, rows)
	fixLegacyTime(tm, rows)
	return convertCharsets(tm, rows, charsets)
}

// NormalizeRowsInfo như NormalizeRows và thêm: phần TABLE_MAP không có metadata (MySQL 5.7, hoặc 8.0
// MINIMAL) lấy từ info (information_schema): số nguyên không dấu, nhãn ENUM/SET, đệm BINARY(n), charset cột.
// info=nil: chỉ dùng metadata của TABLE_MAP (giống NormalizeRows, cộng nhãn ENUM/SET khi có trong event).
func NormalizeRowsInfo(tm *replication.TableMapEvent, info *TableInfo, rows [][]any, charsets map[uint64]string) error {
	if err := applyInfo(tm, info, rows); err != nil {
		return err
	}
	if err := convertMariaDBTypes(tm, info, rows); err != nil {
		return err
	}
	if tm == nil || info == nil || len(tm.CollationMap()) > 0 {
		return NormalizeRows(tm, rows, charsets)
	}
	var bin []int
	byIdx := map[int]string{}
	for i, c := range info.Columns {
		if i >= len(tm.ColumnType) {
			break
		}
		switch c.DataType {
		case "binary":
			bin = append(bin, i)
		case "enum", "set", "json":
		default:
			if c.Charset != "" {
				byIdx[i] = c.Charset
			}
		}
	}
	padBinaryCols(tm, rows, bin)
	if charsets == nil {
		return nil
	}
	return convertCharsetsBy(tm, rows, byIdx)
}

// LoadCollationCharsets: collation id -> charset của source (information_schema.COLLATIONS), cho
// Config.CollationCharsets.
func LoadCollationCharsets(ctx context.Context, db *sql.DB) (map[uint64]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT ID, CHARACTER_SET_NAME FROM information_schema.COLLATIONS")
	if err != nil {
		return nil, fmt.Errorf("read source collations: %w", err)
	}
	defer rows.Close()
	out := map[uint64]string{}
	generic := false
	for rows.Next() {
		var id sql.NullInt64
		var cs sql.NullString
		if err := rows.Scan(&id, &cs); err != nil {
			return nil, err
		}
		if !id.Valid || !cs.Valid {
			// MariaDB 11.x: COLLATIONS có thêm dòng collation "chung" (uca1400_ai_ci) không có id/charset.
			generic = true
			continue
		}
		out[uint64(id.Int64)] = cs.String
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !generic {
		return out, nil
	}
	// Collation đầy đủ (utf8mb4_uca1400_ai_ci...) của MariaDB 11.x nằm ở COLLATION_CHARACTER_SET_APPLICABILITY.
	r2, err := db.QueryContext(ctx, "SELECT ID, CHARACTER_SET_NAME FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY WHERE ID IS NOT NULL")
	if err != nil {
		return nil, fmt.Errorf("read source collations (applicability): %w", err)
	}
	defer r2.Close()
	for r2.Next() {
		var id uint64
		var cs string
		if err := r2.Scan(&id, &cs); err != nil {
			return nil, err
		}
		out[id] = cs
	}
	return out, r2.Err()
}
