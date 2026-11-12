package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"strand/internal/typemap"
)

// MySQL 5.7 không có binlog_row_metadata (8.0.1+): TABLE_MAP chỉ có kiểu cột và meta độ dài, không có
// tên cột, PK, signedness, charset hay nhãn ENUM/SET. Phần thiếu được lấy từ information_schema của
// source qua TableInfoProvider (tableinfo.go), khớp theo VỊ TRÍ cột. Khi TABLE_MAP có metadata
// (8.0 FULL) thì metadata của event được ưu tiên như trước.

// ColumnInfo là mô tả một cột của source (information_schema.COLUMNS).
type ColumnInfo struct {
	Name     string
	DataType string   // DATA_TYPE chữ thường: "int", "varchar", "enum", ...
	Unsigned bool     // COLUMN_TYPE chứa "unsigned"
	Charset  string   // CHARACTER_SET_NAME; rỗng với cột không phải chuỗi/binary
	Values   []string // nhãn của ENUM/SET theo thứ tự khai báo
}

// TableInfo là cột (theo thứ tự ORDINAL_POSITION, gồm cả cột generated) và PK của một bảng source.
type TableInfo struct {
	Columns []ColumnInfo
	PK      []string
	// SysVersioned: bảng MariaDB WITH SYSTEM VERSIONING (TABLE_TYPE = 'SYSTEM VERSIONED'). information_schema
	// ẩn row_start/row_end nhưng TABLE_MAP/ROWS của binlog có thêm hai cột đó ở cuối (sysversion.go).
	SysVersioned bool
}

// TableInfoProvider đọc cấu trúc bảng hiện tại của source. found=false khi bảng không còn tồn tại.
type TableInfoProvider interface {
	LoadTable(ctx context.Context, schema, table string) (info *TableInfo, found bool, err error)
}

type infoSchemaProvider struct{ db *sql.DB }

// NewInfoSchemaProvider trả provider đọc information_schema qua db (kết nối tới source).
func NewInfoSchemaProvider(db *sql.DB) TableInfoProvider { return infoSchemaProvider{db: db} }

func (p infoSchemaProvider) LoadTable(ctx context.Context, schema, table string) (*TableInfo, bool, error) {
	rows, err := p.db.QueryContext(ctx,
		"SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, CHARACTER_SET_NAME FROM information_schema.COLUMNS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", schema, table)
	if err != nil {
		return nil, false, fmt.Errorf("read columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	info := &TableInfo{}
	for rows.Next() {
		var name, dt, ct string
		var cs sql.NullString
		if err := rows.Scan(&name, &dt, &ct, &cs); err != nil {
			return nil, false, err
		}
		c := ColumnInfo{Name: name, DataType: strings.ToLower(dt), Charset: cs.String,
			Unsigned: strings.Contains(strings.ToLower(ct), "unsigned")}
		if c.DataType == "enum" || c.DataType == "set" {
			c.Values = typemap.ParseEnumValues(ct)
		}
		info.Columns = append(info.Columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(info.Columns) == 0 {
		return nil, false, nil
	}
	var tableType string
	if err := p.db.QueryRowContext(ctx, "SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		schema, table).Scan(&tableType); err != nil {
		return nil, false, fmt.Errorf("read table type of %s.%s: %w", schema, table, err)
	}
	info.SysVersioned = strings.EqualFold(tableType, "SYSTEM VERSIONED")
	pk, err := p.db.QueryContext(ctx,
		"SELECT COLUMN_NAME FROM information_schema.STATISTICS "+
			"WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY' ORDER BY SEQ_IN_INDEX", schema, table)
	if err != nil {
		return nil, false, fmt.Errorf("read primary key of %s.%s: %w", schema, table, err)
	}
	defer pk.Close()
	for pk.Next() {
		var n string
		if err := pk.Scan(&n); err != nil {
			return nil, false, err
		}
		info.PK = append(info.PK, n)
	}
	return info, true, pk.Err()
}

// safeKind: cột i là MYSQL_TYPE_STRING mang kiểu thật want (ENUM/SET) theo byte cao của meta; không panic khi
// ColumnMeta thiếu.
func safeKind(tm *replication.TableMapEvent, i int, want byte) bool {
	return i < len(tm.ColumnType) && i < len(tm.ColumnMeta) && tm.ColumnType[i] == gomysql.MYSQL_TYPE_STRING &&
		byte(tm.ColumnMeta[i]>>8) == want
}

// typeOK: kiểu cột trong TABLE_MAP có thể là cột information_schema có DATA_TYPE dt không. Dùng để phát hiện
// bảng đã đổi cấu trúc (ALTER) mà số cột không đổi. Kiểu lạ: coi là khớp.
func typeOK(tm *replication.TableMapEvent, i int, dt string) bool {
	t := tm.ColumnType[i]
	in := func(ts ...byte) bool {
		for _, x := range ts {
			if t == x {
				return true
			}
		}
		return false
	}
	switch dt {
	case "tinyint":
		return in(gomysql.MYSQL_TYPE_TINY)
	case "smallint":
		return in(gomysql.MYSQL_TYPE_SHORT)
	case "mediumint":
		return in(gomysql.MYSQL_TYPE_INT24)
	case "int", "integer":
		return in(gomysql.MYSQL_TYPE_LONG)
	case "bigint":
		return in(gomysql.MYSQL_TYPE_LONGLONG)
	case "decimal":
		return in(gomysql.MYSQL_TYPE_NEWDECIMAL, gomysql.MYSQL_TYPE_DECIMAL)
	case "float":
		return in(gomysql.MYSQL_TYPE_FLOAT)
	case "double":
		return in(gomysql.MYSQL_TYPE_DOUBLE)
	case "year":
		return in(gomysql.MYSQL_TYPE_YEAR)
	case "date":
		return in(gomysql.MYSQL_TYPE_DATE, gomysql.MYSQL_TYPE_NEWDATE)
	case "time":
		return in(gomysql.MYSQL_TYPE_TIME, gomysql.MYSQL_TYPE_TIME2)
	case "datetime":
		return in(gomysql.MYSQL_TYPE_DATETIME, gomysql.MYSQL_TYPE_DATETIME2)
	case "timestamp":
		return in(gomysql.MYSQL_TYPE_TIMESTAMP, gomysql.MYSQL_TYPE_TIMESTAMP2)
	case "varchar", "varbinary":
		return in(gomysql.MYSQL_TYPE_VARCHAR, gomysql.MYSQL_TYPE_VAR_STRING)
	case "char", "binary":
		return in(gomysql.MYSQL_TYPE_STRING) && !safeKind(tm, i, gomysql.MYSQL_TYPE_ENUM) && !safeKind(tm, i, gomysql.MYSQL_TYPE_SET)
	case "enum":
		return in(gomysql.MYSQL_TYPE_STRING) && safeKind(tm, i, gomysql.MYSQL_TYPE_ENUM)
	case "set":
		return in(gomysql.MYSQL_TYPE_STRING) && safeKind(tm, i, gomysql.MYSQL_TYPE_SET)
	case "tinytext", "text", "mediumtext", "longtext", "tinyblob", "blob", "mediumblob", "longblob":
		return in(gomysql.MYSQL_TYPE_BLOB)
	case "json":
		return in(gomysql.MYSQL_TYPE_JSON)
	case "bit":
		return in(gomysql.MYSQL_TYPE_BIT)
	case "inet6", "inet4", "uuid":
		return in(gomysql.MYSQL_TYPE_STRING) // MariaDB: STRING(16)/STRING(4) thô
	}
	return true
}

// infoMismatch trả lý do TABLE_MAP không khớp info ("" nếu khớp): số cột khác nhau, hoặc một cột khác kiểu.
func infoMismatch(tm *replication.TableMapEvent, info *TableInfo) string {
	want := len(info.Columns)
	if info.SysVersioned {
		want += sysVerHidden // row_start, row_end ẩn ở cuối
	}
	if int(tm.ColumnCount) != want || len(tm.ColumnType) != want {
		return fmt.Sprintf("binlog has %d columns, information_schema has %d", tm.ColumnCount, want)
	}
	if info.SysVersioned {
		for i := len(info.Columns); i < want; i++ {
			if t := tm.ColumnType[i]; t != gomysql.MYSQL_TYPE_TIMESTAMP2 && t != gomysql.MYSQL_TYPE_TIMESTAMP {
				return fmt.Sprintf("system-versioned table: hidden column %d has binlog type %d, want TIMESTAMP (explicit or transaction-precise period columns are not supported)", i+1, t)
			}
		}
	}
	for i, c := range info.Columns {
		if !typeOK(tm, i, c.DataType) {
			return fmt.Sprintf("column %d (%s %s) does not match binlog column type %d", i+1, c.Name, c.DataType, tm.ColumnType[i])
		}
	}
	return ""
}

// applyInfo sửa tại chỗ giá trị của rows theo info cho những phần TABLE_MAP không mang metadata:
// signedness (go-mysql giải mã số nguyên có dấu nếu thiếu SignednessBitmap) và nhãn ENUM/SET (go-mysql chỉ
// trả chỉ số/bitmask). Phần TABLE_MAP có metadata thì dùng metadata đó (info có thể nil).
func applyInfo(tm *replication.TableMapEvent, info *TableInfo, rows [][]any) error {
	if tm == nil {
		return nil
	}
	unsigned := map[int]bool{}
	if info != nil && len(tm.SignednessBitmap) == 0 {
		for i, c := range info.Columns {
			if c.Unsigned && i < len(tm.ColumnType) {
				unsigned[i] = true
			}
		}
	}
	enums := tm.EnumStrValueMap()
	sets := tm.SetStrValueMap()
	if info != nil {
		for i, c := range info.Columns {
			if i >= len(tm.ColumnType) {
				break
			}
			if _, ok := enums[i]; !ok && safeKind(tm, i, gomysql.MYSQL_TYPE_ENUM) {
				if enums == nil {
					enums = map[int][]string{}
				}
				enums[i] = c.Values
			}
			if _, ok := sets[i]; !ok && safeKind(tm, i, gomysql.MYSQL_TYPE_SET) {
				if sets == nil {
					sets = map[int][]string{}
				}
				sets[i] = c.Values
			}
		}
	}
	for _, row := range rows {
		for i, v := range row {
			if v == nil {
				continue
			}
			switch {
			case unsigned[i]:
				row[i] = toUnsigned(tm.ColumnType[i], v)
			case enums[i] != nil || sets[i] != nil:
				nv, err := enumSetLabel(tm, i, v, enums[i], sets[i])
				if err != nil {
					return err
				}
				row[i] = nv
			}
		}
	}
	return nil
}

// toUnsigned đưa số nguyên có dấu do go-mysql giải mã về giá trị không dấu của cột.
func toUnsigned(colType byte, v any) any {
	var x int64
	switch n := v.(type) {
	case int8:
		x = int64(n)
	case int16:
		x = int64(n)
	case int32:
		x = int64(n)
	case int64:
		x = n
	default:
		return v // đã là uint hoặc kiểu khác
	}
	switch colType {
	case gomysql.MYSQL_TYPE_TINY:
		return uint8(x)
	case gomysql.MYSQL_TYPE_SHORT:
		return uint16(x)
	case gomysql.MYSQL_TYPE_INT24:
		return uint32(x) & 0xFFFFFF
	case gomysql.MYSQL_TYPE_LONG:
		return uint32(x)
	case gomysql.MYSQL_TYPE_LONGLONG:
		return uint64(x)
	}
	return v
}

func enumSetLabel(tm *replication.TableMapEvent, i int, v any, enum, set []string) (any, error) {
	n, ok := v.(int64)
	if !ok {
		return v, nil // đã là chuỗi (nguồn khác) hoặc kiểu không mong đợi: giữ nguyên
	}
	if safeKind(tm, i, gomysql.MYSQL_TYPE_ENUM) {
		if n == 0 {
			return "", nil // giá trị lỗi '' của ENUM
		}
		if n < 0 || int(n) > len(enum) {
			return nil, fmt.Errorf("%s.%s column %d: ENUM index %d has no label (source schema changed? labels=%d)", tm.Schema, tm.Table, i+1, n, len(enum))
		}
		return enum[n-1], nil
	}
	bits := uint64(n)
	var out []string
	for b := 0; b < 64 && bits>>uint(b) != 0; b++ {
		if bits>>uint(b)&1 == 0 {
			continue
		}
		if b >= len(set) {
			return nil, fmt.Errorf("%s.%s column %d: SET bit %d has no label (source schema changed? labels=%d)", tm.Schema, tm.Table, i+1, b, len(set))
		}
		out = append(out, set[b])
	}
	return strings.Join(out, ","), nil
}
