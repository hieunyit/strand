package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// mariaDBOnlyTypes: kiểu dữ liệu riêng của MariaDB 10.6 mà MySQL 8.x không có (giá trị binlog là byte nhị
// phân, SHOW CREATE TABLE ghi tên kiểu mà target không hiểu).
var mariaDBOnlyTypes = map[string]bool{"inet6": true, "inet4": true, "uuid": true}

// CheckMariaDBTable từ chối (exit 3) bảng MariaDB mà Strand không chuyển được sang MySQL 8.x: SEQUENCE,
// bảng system-versioned (WITH SYSTEM VERSIONING), cột kiểu riêng của MariaDB (INET6...), cột nén
// (COMPRESSED). Gọi ở validate và khi tạo bảng target.
func CheckMariaDBTable(ctx context.Context, db *sql.DB, schema, table string) error {
	var tableType string
	if err := db.QueryRowContext(ctx, `SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
		schema, table).Scan(&tableType); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("inspect MariaDB table %s.%s: %w", schema, table, err)
	}
	switch strings.ToUpper(tableType) {
	case "SEQUENCE":
		return reject("%s.%s is a MariaDB SEQUENCE: sequences are not supported; exclude it (tables.exclude) and recreate it on the target manually", schema, table)
	case "SYSTEM VERSIONED":
		return reject("%s.%s is a MariaDB system-versioned table (WITH SYSTEM VERSIONING): not supported; remove versioning or exclude the table", schema, table)
	}
	// Application-time period (10.4+: PERIOD FOR, 10.5+: WITHOUT OVERLAPS) không có trên MySQL.
	var tn, ddl string
	if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+quote(schema, false)+"."+quote(table, false)).Scan(&tn, &ddl); err != nil {
		return fmt.Errorf("inspect MariaDB table %s.%s: %w", schema, table, err)
	}
	if mariaDBHasPeriod(ddl) {
		return reject("%s.%s uses a MariaDB application-time PERIOD (PERIOD FOR / WITHOUT OVERLAPS): not supported; remove the period or exclude the table", schema, table)
	}
	rows, err := db.QueryContext(ctx, `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IFNULL(EXTRA, '') FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, schema, table)
	if err != nil {
		return fmt.Errorf("inspect MariaDB columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var col, dataType, colType, extra string
		if err := rows.Scan(&col, &dataType, &colType, &extra); err != nil {
			return err
		}
		if mariaDBOnlyTypes[strings.ToLower(dataType)] {
			return reject("%s.%s column %s has MariaDB-only type %s, which MySQL 8.x does not have; change it on the source (e.g. VARCHAR/BINARY) or exclude the table",
				schema, table, col, dataType)
		}
		if strings.Contains(strings.ToLower(colType), "compressed") || strings.Contains(strings.ToLower(extra), "compressed") {
			return reject("%s.%s column %s is a MariaDB COMPRESSED column: not supported; remove the COMPRESSED attribute or exclude the table", schema, table, col)
		}
	}
	return rows.Err()
}

// mariaDBHasPeriod: DDL có `PERIOD FOR` hoặc `WITHOUT OVERLAPS` (ngoài chuỗi/tên trích dẫn). Hàm thuần.
func mariaDBHasPeriod(ddl string) bool {
	ts, err := lex(ddl)
	if err != nil {
		return false
	}
	for i := 0; i+1 < len(ts); i++ {
		if ts[i].keyword(ddl, "PERIOD") && ts[i+1].keyword(ddl, "FOR") ||
			ts[i].keyword(ddl, "WITHOUT") && ts[i+1].keyword(ddl, "OVERLAPS") {
			return true
		}
	}
	return false
}

// mariaDBBlobTypes: kiểu mà MySQL không cho DEFAULT literal (chỉ cho DEFAULT (biểu thức), 8.0.13+).
var mariaDBBlobTypes = map[string]bool{
	"tinytext": true, "text": true, "mediumtext": true, "longtext": true,
	"tinyblob": true, "blob": true, "mediumblob": true, "longblob": true, "json": true,
}

// mariaDBKeepDefaultFuncs: hàm MySQL nhận trực tiếp sau DEFAULT (không cần ngoặc) cho DATETIME/TIMESTAMP.
var mariaDBKeepDefaultFuncs = map[string]bool{"current_timestamp": true, "now": true, "localtime": true, "localtimestamp": true}

// mariaDBDefaults đổi mệnh đề DEFAULT của SHOW CREATE TABLE MariaDB sang cú pháp MySQL 8.x:
//   - DEFAULT hàm(...) (uuid(), rand(), curdate(), concat(...)...) → DEFAULT (hàm(...)); MySQL chỉ nhận biểu
//     thức trong ngoặc, trừ CURRENT_TIMESTAMP/NOW/LOCALTIME[STAMP];
//   - TEXT/BLOB/JSON DEFAULT 'literal' → DEFAULT ('literal'); MySQL không cho literal trên các kiểu này.
//
// Biểu thức đã có ngoặc, số, NULL, b'..' giữ nguyên.
func mariaDBDefaults(ddl string) (string, error) {
	ts, err := lex(ddl)
	if err != nil {
		return "", err
	}
	type span struct{ from, to int }
	var wraps []span
	depth := 0
	colType := ""
	startCol := false
	for i := 0; i < len(ts); i++ {
		t := ts[i]
		isOpen := t.value == "(" && ddl[t.start] == '('
		isClose := t.value == ")" && ddl[t.start] == ')'
		if isOpen {
			depth++
			if depth == 1 {
				startCol = true
			}
			continue
		}
		if isClose {
			depth--
			continue
		}
		if depth != 1 {
			continue
		}
		if t.value == "," && ddl[t.start] == ',' {
			startCol = true
			continue
		}
		if startCol {
			// Token đầu của định nghĩa cột là tên cột, token kế là kiểu.
			startCol = false
			colType = ""
			if i+1 < len(ts) && ts[i+1].ident {
				colType = strings.ToLower(ts[i+1].value)
			}
			continue
		}
		if !t.keyword(ddl, "DEFAULT") || i+1 >= len(ts) {
			continue
		}
		nt := ts[i+1]
		switch {
		case nt.value == "(" && ddl[nt.start] == '(':
			// đã là biểu thức trong ngoặc
		case ddl[nt.start] == '\'':
			if mariaDBBlobTypes[colType] {
				wraps = append(wraps, span{nt.start, nt.end})
			}
		case nt.ident && ddl[nt.start] != '`' && ddl[nt.start] != '"' && i+2 < len(ts) && ts[i+2].value == "(" && ddl[ts[i+2].start] == '(':
			// lời gọi hàm: tìm ngoặc đóng tương ứng
			d, k := 0, i+2
			for ; k < len(ts); k++ {
				if ts[k].value == "(" && ddl[ts[k].start] == '(' {
					d++
				} else if ts[k].value == ")" && ddl[ts[k].start] == ')' {
					d--
					if d == 0 {
						break
					}
				}
			}
			if k >= len(ts) {
				return "", fmt.Errorf("unbalanced parentheses in DEFAULT of MariaDB table definition")
			}
			if !mariaDBKeepDefaultFuncs[strings.ToLower(nt.value)] {
				wraps = append(wraps, span{nt.start, ts[k].end})
			}
			i = k
		}
	}
	for j := len(wraps) - 1; j >= 0; j-- {
		w := wraps[j]
		ddl = ddl[:w.from] + "(" + ddl[w.from:w.to] + ")" + ddl[w.to:]
	}
	return ddl, nil
}
