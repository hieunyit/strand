package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"strand/internal/model"
	"strand/internal/mysqlver"
	"strand/internal/typemap"
)

// token chỉ dùng để đổi định danh; không đụng nội dung literal/comment/biểu thức.
type token struct {
	start, end int
	value      string
	ident      bool
}

func (t token) keyword(q, k string) bool {
	return t.ident && q[t.start] != '`' && q[t.start] != '"' && strings.EqualFold(t.value, k)
}
func lex(q string) ([]token, error) {
	var out []token
	for i := 0; i < len(q); {
		if q[i] == ' ' || q[i] == '\n' || q[i] == '\t' || q[i] == '\r' {
			i++
			continue
		}
		start := i
		c := q[i]
		i++
		if c == '`' || c == '"' || c == '\'' {
			var b strings.Builder
			closed := false
			for i < len(q) {
				if q[i] == c {
					if i+1 < len(q) && q[i+1] == c {
						b.WriteByte(c)
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				if q[i] == '\\' && c == '\'' && i+1 < len(q) {
					b.WriteByte(q[i+1])
					i += 2
					continue
				}
				b.WriteByte(q[i])
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated SQL quote")
			}
			out = append(out, token{start, i, b.String(), c != '\''})
			continue
		}
		if c == '/' && i < len(q) && q[i] == '*' {
			end := strings.Index(q[i+1:], "*/")
			if end < 0 {
				return nil, fmt.Errorf("unterminated SQL comment")
			}
			i += end + 3
			continue
		}
		ident := c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 128
		if ident {
			for i < len(q) {
				c = q[i]
				if !(c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c >= 128) {
					break
				}
				i++
			}
		}
		out = append(out, token{start, i, q[start:i], ident})
	}
	return out, nil
}

func mysqlDDL(q string, src, dst model.TableID, rename map[string]string) (string, error) {
	ts, err := lex(q)
	if err != nil {
		return "", err
	}
	open := -1
	close := -1
	depth := 0
	for _, t := range ts {
		if t.value == "(" && q[t.start] == '(' {
			if open < 0 {
				open = t.start
			}
			depth++
		}
		if t.value == ")" && q[t.start] == ')' {
			depth--
			if depth == 0 && close < 0 {
				close = t.end
			}
		}
	}
	if open < 0 || close < 0 {
		return "", fmt.Errorf("invalid SHOW CREATE TABLE output")
	}
	q = "CREATE TABLE " + name(dst, false) + " " + q[open:]
	ts, err = lex(q)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	last := 0
	depth = 0
	tableClosed := false
	for i := 0; i < len(ts); i++ {
		t := ts[i]
		if q[t.start] == '(' && t.value == "(" {
			depth++
		}
		if q[t.start] == ')' && t.value == ")" {
			depth--
			if depth == 0 {
				tableClosed = true
			}
		}
		if tableClosed && depth == 0 && t.ident && strings.EqualFold(t.value, "AUTO_INCREMENT") && i+2 < len(ts) && ts[i+1].value == "=" {
			out.WriteString(q[last:t.start])
			last = ts[i+2].end
			i += 2
			continue
		}
		if src != dst && t.keyword(q, "CONSTRAINT") && i+1 < len(ts) && ts[i+1].ident {
			n := dst.Table + "_" + ts[i+1].value
			if len(n) > 64 {
				return "", reject("mapped constraint name %s exceeds MySQL 64-byte limit", n)
			}
			out.WriteString(q[last:ts[i+1].start])
			out.WriteString(quote(n, false))
			last = ts[i+1].end
			i++
			continue
		}
		if t.keyword(q, "REFERENCES") && i+1 < len(ts) {
			a := ts[i+1]
			if !a.ident {
				continue
			}
			ref := model.TableID{Schema: src.Schema, Table: a.value}
			end := a.end
			j := i + 1
			if j+2 < len(ts) && ts[j+1].value == "." && ts[j+2].ident {
				ref.Schema = a.value
				ref.Table = ts[j+2].value
				end = ts[j+2].end
				j += 2
			}
			if mapped, ok := rename[ref.String()]; ok {
				s, t, _ := strings.Cut(mapped, ".")
				ref = model.TableID{Schema: s, Table: t}

			}
			out.WriteString(q[last:a.start])
			out.WriteString(name(ref, false))
			last = end
			i = j
		}
	}
	out.WriteString(q[last:])
	return out.String(), nil
}

// utf8mb4Default57 là collation mặc định của utf8mb4 ở MySQL 5.7 (8.x mặc định utf8mb4_0900_ai_ci).
const utf8mb4Default57 = "utf8mb4_general_ci"

// pinUTF8MB4Collation57 thêm COLLATE tường minh vào DDL do 5.7 sinh ra: sau mỗi `CHARACTER SET utf8mb4`
// của cột không kèm COLLATE và sau `CHARSET=utf8mb4` của bảng nếu bảng không có `COLLATE=`. Hàm thuần.
func pinUTF8MB4Collation57(ddl string) (string, error) {
	return pinUTF8MB4Collation(ddl, utf8mb4Default57)
}

// pinUTF8MB4Collation như pinUTF8MB4Collation57 nhưng với collation mặc định thật của utf8mb4 trên nguồn
// (MariaDB 11.x mặc định utf8mb4_uca1400_ai_ci, không phải utf8mb4_general_ci).
func pinUTF8MB4Collation(ddl, defColl string) (string, error) {
	ts, err := lex(ddl)
	if err != nil {
		return "", err
	}
	type ins struct {
		at   int
		text string
	}
	var adds []ins
	depth, tableClosed := 0, false
	tableHasCollate := false
	tableCharsetEnd := -1
	for i := 0; i < len(ts); i++ {
		t := ts[i]
		if ddl[t.start] == '(' && t.value == "(" {
			depth++
		}
		if ddl[t.start] == ')' && t.value == ")" {
			depth--
			if depth == 0 {
				tableClosed = true
			}
		}
		if t.keyword(ddl, "COLLATE") && tableClosed && depth == 0 {
			tableHasCollate = true
		}
		var vi int // chỉ số token giá trị charset
		switch {
		case t.keyword(ddl, "CHARACTER") && i+2 < len(ts) && ts[i+1].keyword(ddl, "SET"):
			vi = i + 2
		case t.keyword(ddl, "CHARSET") && i+1 < len(ts):
			vi = i + 1
		default:
			continue
		}
		if vi < len(ts) && ts[vi].value == "=" {
			vi++
		}
		if vi >= len(ts) || !ts[vi].keyword(ddl, "utf8mb4") {
			continue
		}
		if tableClosed && depth == 0 {
			tableCharsetEnd = ts[vi].end
			continue
		}
		if vi+1 < len(ts) && ts[vi+1].keyword(ddl, "COLLATE") {
			continue
		}
		adds = append(adds, ins{ts[vi].end, " COLLATE " + defColl})
	}
	if tableCharsetEnd >= 0 && !tableHasCollate {
		adds = append(adds, ins{tableCharsetEnd, " COLLATE=" + defColl})
	}
	sort.Slice(adds, func(a, b int) bool { return adds[a].at < adds[b].at })
	for i := len(adds) - 1; i >= 0; i-- {
		ddl = ddl[:adds[i].at] + adds[i].text + ddl[adds[i].at:]
	}
	return ddl, nil
}

func mysqlToMySQL(ctx context.Context, r Request, t model.TableID) ([]string, []string, error) {
	// invariant: CP-1 — dữ liệu và checkpoint phải commit nguyên tử trên target.
	// Không sao chép engine phi giao dịch rồi tuyên bố effectively-once.
	var engine string
	if err := r.Source.QueryRowContext(ctx, `SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, t.Schema, t.Table).Scan(&engine); err != nil {
		return nil, nil, err
	}
	if !strings.EqualFold(engine, "InnoDB") {
		return nil, nil, reject("table %s uses nontransactional/unsupported engine %s; target requires InnoDB for CP-1", t, engine)
	}
	if ver, err := mysqlver.Cached(ctx, r.Source); err != nil {
		return nil, nil, err
	} else if ver.IsMariaDB() {
		if err := CheckMariaDBTable(ctx, r.Source, t.Schema, t.Table); err != nil {
			return nil, nil, err
		}
	}
	var table, ddl, charset, collation string
	if err := r.Source.QueryRowContext(ctx, "SHOW CREATE TABLE "+name(t, false)).Scan(&table, &ddl); err != nil {
		return nil, nil, err
	}
	if err := r.Source.QueryRowContext(ctx, `SELECT DEFAULT_CHARACTER_SET_NAME,DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?`, t.Schema).Scan(&charset, &collation); err != nil {
		return nil, nil, err
	}
	dst := r.target(t)
	ddl, err := mysqlDDL(ddl, t, dst, r.Rename)
	if err != nil {
		return nil, nil, err
	}
	ver, err := mysqlver.Cached(ctx, r.Source)
	if err != nil {
		return nil, nil, err
	}
	if ver.IsMariaDB() {
		if ddl, err = mariaDBDefaults(ddl); err != nil {
			return nil, nil, err
		}
	}
	var collWarnings []string
	if ver.Legacy57() {
		// SHOW CREATE TABLE của 5.7 bỏ COLLATE khi là mặc định của charset; target 8.x sẽ lấy mặc định
		// khác (utf8mb4_0900_ai_ci) nên phải ghi rõ collation của nguồn.
		if ddl, err = pinUTF8MB4Collation57(ddl); err != nil {
			return nil, nil, err
		}
	}
	if ver.IsMariaDB() {
		// MariaDB cũng bỏ COLLATE khi là mặc định của charset, mà mặc định thật của nguồn có thể không phải
		// general_ci (11.x: uca1400_ai_ci): đọc từ nguồn rồi ghi rõ; sau đó ánh xạ collation đích không có.
		def, err := MariaDBCharsetDefault(ctx, r.Source, "utf8mb4")
		if err != nil {
			return nil, nil, err
		}
		if ddl, err = pinUTF8MB4Collation(ddl, def); err != nil {
			return nil, nil, err
		}
		has, err := TargetCollations(ctx, r.Target)
		if err != nil {
			return nil, nil, err
		}
		cp, err := PlanMariaDBCollations(ctx, r.Source, has, t.Schema, t.Table)
		if err != nil {
			return nil, nil, err
		}
		if ddl, err = rewriteCollations(ddl, cp.Rewrite); err != nil {
			return nil, nil, err
		}
		collWarnings = cp.Warnings
		var dbw []string
		collation, dbw = MapDatabaseCollation(t.Schema, collation, has, ver.AtLeast(10, 5, 0))
		collWarnings = append(collWarnings, dbw...)
	}
	warnings, err := mysqlOmitted(ctx, r.Source, t)
	if err != nil {
		return nil, nil, err
	}
	warnings = append(warnings, collWarnings...)
	return []string{"CREATE DATABASE IF NOT EXISTS " + quote(dst.Schema, false) + " CHARACTER SET " + quote(charset, false) + " COLLATE " + quote(collation, false), ddl}, warnings, nil
}
func mysqlOmitted(ctx context.Context, db *sql.DB, t model.TableID) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=? ORDER BY TRIGGER_NAME`, t.Schema, t.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var w []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		w = append(w, "trigger omitted: "+t.Schema+"."+n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for _, kind := range []string{"event", "routine"} {
		query := `SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA=? ORDER BY EVENT_NAME`
		if kind == "routine" {
			query = `SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA=? ORDER BY ROUTINE_NAME`
		}
		objects, err := db.QueryContext(ctx, query, t.Schema)
		if err != nil {
			return nil, err
		}
		for objects.Next() {
			var n string
			if err := objects.Scan(&n); err != nil {
				objects.Close()
				return nil, err
			}
			w = append(w, kind+" omitted: "+t.Schema+"."+n)
		}
		objects.Close()
		if err := objects.Err(); err != nil {
			return nil, err
		}
	}

	return w, nil
}

type mysqlExtra struct {
	Default           sql.NullString
	Extra, Expression string
}
type mysqlIndex struct {
	Name    string
	Unique  bool
	Columns []string
}

func mysqlIndexes(ctx context.Context, db *sql.DB, t model.TableID) ([]mysqlIndex, error) {
	rows, err := db.QueryContext(ctx, `SELECT INDEX_NAME,NON_UNIQUE,COLUMN_NAME,SUB_PART,INDEX_TYPE,COLLATION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX`, t.Schema, t.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mysqlIndex
	for rows.Next() {
		var n, typ string
		var col, order sql.NullString
		var nonUnique int
		var prefix sql.NullInt64
		if err := rows.Scan(&n, &nonUnique, &col, &prefix, &typ, &order); err != nil {
			return nil, err
		}
		if !col.Valid || prefix.Valid || typ != "BTREE" {
			return nil, reject("index %s on %s requires manual mapping (expression, prefix or %s)", n, t, typ)
		}
		if len(out) == 0 || out[len(out)-1].Name != n {
			out = append(out, mysqlIndex{Name: n, Unique: nonUnique == 0})
		}
		c := quote(col.String, true)
		if order.String == "D" {
			c += " DESC"
		}
		out[len(out)-1].Columns = append(out[len(out)-1].Columns, c)
	}
	return out, rows.Err()
}

var numericDefault = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?$`)

func mysqlToPG(ctx context.Context, r Request, t model.TableID) ([]string, []string, []string, error) {
	cols, err := typemap.InspectMySQLColumns(ctx, r.Source, t.Schema, t.Table)
	if err != nil {
		return nil, nil, nil, err
	}
	indexes, err := mysqlIndexes(ctx, r.Source, t)
	if err != nil {
		return nil, nil, nil, err
	}
	keys := map[string]bool{}
	for _, idx := range indexes {
		if idx.Unique {
			for _, col := range cols {
				for _, c := range idx.Columns {
					if c == quote(col.Name, true) || c == quote(col.Name, true)+" DESC" {
						keys[col.Name] = true
					}
				}
			}
		}
	}
	extras := map[string]mysqlExtra{}
	rows, err := r.Source.QueryContext(ctx, columnExtrasSelect(ctx, r)+` FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, t.Schema, t.Table)
	if err != nil {
		return nil, nil, nil, err
	}
	for rows.Next() {
		var n string
		var e mysqlExtra
		if err := rows.Scan(&n, &e.Default, &e.Extra, &e.Expression); err != nil {
			rows.Close()
			return nil, nil, nil, err
		}
		extras[n] = e
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	dst := r.target(t)
	var defs, after, warnings []string
	for _, col := range cols {
		m, err := typemap.MapMySQLColumn(col, typemap.DefaultOptions())
		if err != nil {
			return nil, nil, nil, err
		}
		e := extras[col.Name]
		if e.Expression != "" {
			return nil, nil, nil, reject("generated MySQL column %s.%s requires manual mapping", t, col.Name)
		}
		d := quote(col.Name, true) + " " + m.TargetType
		if keys[col.Name] && (m.TargetType == "text" || strings.HasPrefix(m.TargetType, "varchar") || strings.HasPrefix(m.TargetType, "char")) {
			d += ` COLLATE "C"`
		}
		if strings.Contains(e.Extra, "auto_increment") {
			if m.TargetType != "smallint" && m.TargetType != "integer" && m.TargetType != "bigint" {
				return nil, nil, nil, reject("AUTO_INCREMENT %s.%s maps to %s which cannot be a PostgreSQL identity", t, col.Name, m.TargetType)
			}
			d += " GENERATED BY DEFAULT AS IDENTITY"
		} else if e.Default.Valid {
			v := e.Default.String
			switch {
			case numericDefault.MatchString(v) && (col.DataType == "tinyint" || col.DataType == "smallint" || col.DataType == "mediumint" || col.DataType == "int" || col.DataType == "bigint" || col.DataType == "decimal" || col.DataType == "float" || col.DataType == "double"):
				d += " DEFAULT " + v
			case col.DataType == "varchar" || col.DataType == "char" || col.DataType == "enum":
				d += " DEFAULT " + literal(v)
			default:
				warnings = append(warnings, "default omitted (requires manual translation): "+t.String()+"."+col.Name)
			}
		}
		if !col.Nullable {
			d += " NOT NULL"
		}
		if col.DataType == "enum" {
			var vals []string
			for _, v := range col.EnumValues {
				vals = append(vals, literal(v))
			}
			d += " CHECK (" + quote(col.Name, true) + " IN (" + strings.Join(vals, ", ") + "))"
		}
		defs = append(defs, d)
	}
	for _, idx := range indexes {
		if idx.Name == "PRIMARY" {
			defs = append(defs, "PRIMARY KEY ("+strings.Join(idx.Columns, ", ")+")")
			continue
		}
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		// Tên index PostgreSQL dùng chung namespace với bảng; thêm tên bảng để tránh va chạm.
		iname := dst.Table + "_" + idx.Name
		if len(iname) > 63 {
			return nil, nil, nil, reject("mapped index name %s exceeds PostgreSQL 63-byte limit", iname)
		}
		after = append(after, "CREATE "+unique+"INDEX "+quote(iname, true)+" ON "+name(dst, true)+" ("+strings.Join(idx.Columns, ", ")+")")
	}
	ddl := []string{"CREATE SCHEMA IF NOT EXISTS " + quote(dst.Schema, true), "CREATE TABLE " + name(dst, true) + " (\n  " + strings.Join(defs, ",\n  ") + "\n)"}
	ddl = append(ddl, after...)
	w, err := mysqlOmitted(ctx, r.Source, t)
	if err != nil {
		return nil, nil, nil, err
	}
	warnings = append(warnings, w...)
	foreign, err := mysqlForeignKeys(ctx, r, t)
	if err != nil {
		return nil, nil, nil, err
	}
	return ddl, foreign, warnings, nil
}

func mysqlForeignKeys(ctx context.Context, r Request, t model.TableID) ([]string, error) {
	rows, err := r.Source.QueryContext(ctx, `SELECT k.CONSTRAINT_NAME,k.COLUMN_NAME,k.REFERENCED_TABLE_SCHEMA,k.REFERENCED_TABLE_NAME,k.REFERENCED_COLUMN_NAME,rc.UPDATE_RULE,rc.DELETE_RULE FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.REFERENTIAL_CONSTRAINTS rc ON rc.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA AND rc.CONSTRAINT_NAME=k.CONSTRAINT_NAME AND rc.TABLE_NAME=k.TABLE_NAME WHERE k.TABLE_SCHEMA=? AND k.TABLE_NAME=? AND k.REFERENCED_TABLE_NAME IS NOT NULL ORDER BY k.CONSTRAINT_NAME,k.ORDINAL_POSITION`, t.Schema, t.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type fk struct {
		n              string
		ref            model.TableID
		cols, refs     []string
		update, delete string
	}
	var fks []fk
	for rows.Next() {
		var n, c, s, table, ref, u, d string
		if err := rows.Scan(&n, &c, &s, &table, &ref, &u, &d); err != nil {
			return nil, err
		}
		if len(fks) == 0 || fks[len(fks)-1].n != n {
			fks = append(fks, fk{n: n, ref: r.target(model.TableID{Schema: s, Table: table}), update: u, delete: d})
		}
		f := &fks[len(fks)-1]
		f.cols = append(f.cols, quote(c, true))
		f.refs = append(f.refs, quote(ref, true))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []string
	for _, f := range fks {
		out = append(out, "ALTER TABLE "+name(r.target(t), true)+" ADD CONSTRAINT "+quote(f.n, true)+" FOREIGN KEY ("+strings.Join(f.cols, ", ")+") REFERENCES "+name(f.ref, true)+" ("+strings.Join(f.refs, ", ")+") ON UPDATE "+f.update+" ON DELETE "+f.delete)
	}
	return out, nil
}
