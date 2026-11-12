package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// mariaDBTextTypes: kiểu MariaDB không có ở MySQL, bảng đích dùng chuỗi có đúng dạng chữ mà SELECT của MariaDB
// trả (binlog mang byte thô, reader đổi sang chữ: internal/reader/mysql/mariadb_types.go). ascii_bin: chữ thường
// chính tắc nên so bằng/duy nhất trên đích trùng với nguồn.
var mariaDBTextTypes = map[string]string{
	"inet6": "varchar(39) CHARACTER SET ascii COLLATE ascii_bin", // dài nhất "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff" = 39
	"inet4": "varchar(15) CHARACTER SET ascii COLLATE ascii_bin", // "255.255.255.255"
	"uuid":  "char(36) CHARACTER SET ascii COLLATE ascii_bin",
}

// CheckMariaDBTable từ chối (exit 3) bảng MariaDB mà Strand không chuyển được sang MySQL 8.x: SEQUENCE, khóa chính
// là cột INET6/INET4/UUID, bảng system-versioned có cột row_start/row_end khai báo tường minh, bảng có PERIOD
// (application-time). Bảng WITH SYSTEM VERSIONING thông thường được nhận (chỉ chép hàng hiện hành). Gọi ở
// validate và khi tạo bảng target.
func CheckMariaDBTable(ctx context.Context, db *sql.DB, schema, table string) error {
	var tableType string
	if err := db.QueryRowContext(ctx, `SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`,
		schema, table).Scan(&tableType); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("inspect MariaDB table %s.%s: %w", schema, table, err)
	}
	if strings.EqualFold(tableType, "SEQUENCE") {
		return reject("%s.%s is a MariaDB SEQUENCE: sequences are not supported; exclude it (tables.exclude) and recreate it on the target manually", schema, table)
	}
	rows, err := db.QueryContext(ctx, `SELECT COLUMN_NAME, DATA_TYPE, IFNULL(GENERATION_EXPRESSION, '') FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, schema, table)
	if err != nil {
		return fmt.Errorf("inspect MariaDB columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	textCols := map[string]string{} // cột INET6/INET4/UUID -> kiểu
	for rows.Next() {
		var col, dataType, genExpr string
		if err := rows.Scan(&col, &dataType, &genExpr); err != nil {
			return err
		}
		if strings.EqualFold(genExpr, "ROW START") || strings.EqualFold(genExpr, "ROW END") {
			return reject("%s.%s declares its system-versioning period columns explicitly (%s is %s): not supported; use plain WITH SYSTEM VERSIONING (implicit row_start/row_end), drop the versioning, or exclude the table",
				schema, table, col, genExpr)
		}
		if _, ok := mariaDBTextTypes[strings.ToLower(dataType)]; ok {
			textCols[col] = strings.ToLower(dataType)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if len(textCols) > 0 {
		pk, err := db.QueryContext(ctx, `SELECT COLUMN_NAME FROM information_schema.STATISTICS
			WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX`, schema, table)
		if err != nil {
			return fmt.Errorf("inspect MariaDB primary key of %s.%s: %w", schema, table, err)
		}
		defer pk.Close()
		for pk.Next() {
			var col string
			if err := pk.Scan(&col); err != nil {
				return err
			}
			if typ, ok := textCols[col]; ok {
				// Thứ tự của INET6/INET4/UUID ở MariaDB (so byte; UUID so theo từng đoạn) khác thứ tự chuỗi ở đích, mà
				// snapshot/verify chia khóa theo khoảng và so cả hai phía.
				return reject("%s.%s: primary key column %s has MariaDB type %s; its sort order differs from the text column used on the target, so key ranges cannot be proven equal; "+
					"change the key to a supported type (e.g. a surrogate BIGINT key) or exclude the table (%s columns outside the primary key are supported)", schema, table, col, typ, typ)
			}
		}
		if err := pk.Err(); err != nil {
			return err
		}
	}
	var name, ddl string
	if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+quote(schema, false)+"."+quote(table, false)).Scan(&name, &ddl); err != nil {
		return fmt.Errorf("inspect MariaDB table %s.%s: %w", schema, table, err)
	}
	if has, err := mariaDBHasPeriod(ddl); err != nil {
		return err
	} else if has {
		return reject("%s.%s has a PERIOD FOR definition (application-time period or explicit system-time columns): not supported on MySQL; drop the period or exclude the table", schema, table)
	}
	return nil
}

// mariaDBHasPeriod: DDL có định nghĩa PERIOD FOR ở mức khai báo bảng.
func mariaDBHasPeriod(ddl string) (bool, error) {
	ts, err := lex(ddl)
	if err != nil {
		return false, err
	}
	depth := 0
	for i, t := range ts {
		switch {
		case t.value == "(" && ddl[t.start] == '(':
			depth++
		case t.value == ")" && ddl[t.start] == ')':
			depth--
		case depth == 1 && t.keyword(ddl, "PERIOD") && i+1 < len(ts) && ts[i+1].keyword(ddl, "FOR"):
			return true, nil
		}
	}
	return false, nil
}

// mariaDBTypes đổi kiểu INET6/INET4/UUID của cột sang kiểu chuỗi của đích (mariaDBTextTypes).
func mariaDBTypes(ddl string) (string, error) {
	ts, err := lex(ddl)
	if err != nil {
		return "", err
	}
	type span struct {
		from, to int
		repl     string
	}
	var edits []span
	depth, startCol := 0, false
	for i := 0; i < len(ts); i++ {
		t := ts[i]
		switch {
		case t.value == "(" && ddl[t.start] == '(':
			depth++
			if depth == 1 {
				startCol = true
			}
			continue
		case t.value == ")" && ddl[t.start] == ')':
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
			startCol = false
			if i+1 < len(ts) && ts[i+1].ident && ddl[ts[i+1].start] != '`' && ddl[ts[i+1].start] != '"' {
				if repl, ok := mariaDBTextTypes[strings.ToLower(ts[i+1].value)]; ok {
					edits = append(edits, span{ts[i+1].start, ts[i+1].end, repl})
				}
			}
		}
	}
	for j := len(edits) - 1; j >= 0; j-- {
		e := edits[j]
		ddl = ddl[:e.from] + e.repl + ddl[e.to:]
	}
	return ddl, nil
}

// mariaDBVersioning bỏ cú pháp system-versioning mà MySQL không có: thuộc tính cột WITHOUT SYSTEM VERSIONING,
// mệnh đề bảng WITH SYSTEM VERSIONING và (nếu có ngay sau) PARTITION BY SYSTEM_TIME ... đến hết câu. Cột
// row_start/row_end ẩn không có trong SHOW CREATE TABLE nên đích không có chúng.
func mariaDBVersioning(ddl string) (string, error) {
	ts, err := lex(ddl)
	if err != nil {
		return "", err
	}
	type span struct{ from, to int }
	var cuts []span
	for i := 0; i+2 < len(ts); i++ {
		if !ts[i+1].keyword(ddl, "SYSTEM") || !ts[i+2].keyword(ddl, "VERSIONING") {
			continue
		}
		switch {
		case ts[i].keyword(ddl, "WITHOUT"):
			cuts = append(cuts, span{ts[i].start, ts[i+2].end})
		case ts[i].keyword(ddl, "WITH"):
			end := ts[i+2].end
			if i+5 < len(ts) && ts[i+3].keyword(ddl, "PARTITION") && ts[i+4].keyword(ddl, "BY") && ts[i+5].keyword(ddl, "SYSTEM_TIME") {
				end = len(ddl)
			}
			cuts = append(cuts, span{ts[i].start, end})
		}
	}
	for j := len(cuts) - 1; j >= 0; j-- {
		c := cuts[j]
		ddl = ddl[:c.from] + ddl[c.to:]
	}
	return ddl, nil
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
