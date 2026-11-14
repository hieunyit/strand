package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/model"
	"strand/internal/mysqlver"
	"strand/internal/typemap"
)

// P3-01 — đọc catalog source/target thành TableDef rồi gọi Diff. Source: MySQL (đích MySQL hoặc PG).
// TODO(P3-01): source PG cần phát hiện qua message Relation của pgoutput — chưa làm, pipeline giữ hành vi dừng.

// PlanEvolution so bảng t ở source (schema hiện tại) với bảng đích tương ứng (đã áp r.Rename) và trả các
// thay đổi đã phân loại kèm DDL cho target. Cả hai bảng phải tồn tại.
// Giới hạn đã biết: schema source đọc tại lúc xử lý DDL; nếu source đã đi xa hơn bằng DDL sau đó thì diff
// có thể gồm cả thay đổi sau (additive áp sớm vô hại, destructive thì dừng chờ duyệt).
func PlanEvolution(ctx context.Context, r Request, t model.TableID) (Evolution, error) {
	dstID := r.target(t)
	pg := r.TargetKind == "postgres"
	opts := DiffOptions{PG: pg, Target: dstID}

	if r.SourceKind == "postgres" {
		if !pg {
			return Evolution{}, reject("schema evolution from %s to %s is not supported", r.SourceKind, r.TargetKind)
		}
		opts.LenientWiderTarget, opts.IgnoreExtraIndexes = true, true
		src, err := pgSourceDef(ctx, r.Source, t, dstID)
		if err != nil {
			return Evolution{}, err
		}
		dst, err := pgTargetDef(ctx, r.Target, dstID)
		if err != nil {
			return Evolution{}, err
		}
		return Diff(src, dst, opts), nil
	}

	if r.SourceKind != "mysql" {
		return Evolution{}, reject("schema evolution is not implemented for %s sources", r.SourceKind)
	}
	ver, err := mysqlver.Cached(ctx, r.Source)
	if err != nil {
		return Evolution{}, err
	}
	if ver.IsMariaDB() {
		opts.ForceManual = "MariaDB source: DDL is not translated automatically"
	}
	var src, dst TableDef
	if pg {
		opts.LenientWiderTarget, opts.IgnoreExtraIndexes = true, true
		if src, err = sourceDefForPG(ctx, r, t, dstID); err != nil {
			return Evolution{}, err
		}
		if dst, err = pgTargetDef(ctx, r.Target, dstID); err != nil {
			return Evolution{}, err
		}
	} else {
		if src, err = mysqlTableDef(ctx, r.Source, t, dstID, false, ver); err != nil {
			return Evolution{}, err
		}
		if dst, err = mysqlTableDef(ctx, r.Target, dstID, dstID, true, ver); err != nil {
			return Evolution{}, err
		}
	}
	return Diff(src, dst, opts), nil
}

type mysqlColRow struct {
	name, typ                 string
	nullable                  bool
	def                       sql.NullString
	extra, charset, collation string
}

func readMySQLColumns(ctx context.Context, db *sql.DB, t model.TableID) ([]mysqlColRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT COLUMN_NAME, LOWER(COLUMN_TYPE), IS_NULLABLE, COLUMN_DEFAULT, EXTRA,
		COALESCE(CHARACTER_SET_NAME,''), COALESCE(COLLATION_NAME,'') FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, t.Schema, t.Table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mysqlColRow
	for rows.Next() {
		var c mysqlColRow
		var null string
		if err := rows.Scan(&c.name, &c.typ, &null, &c.def, &c.extra, &c.charset, &c.collation); err != nil {
			return nil, err
		}
		c.nullable = null == "YES"
		c.extra = strings.ToLower(c.extra)
		out = append(out, c)
	}
	return out, rows.Err()
}

// readMySQLIndexDefs đọc index (không gồm PRIMARY) và chữ ký PK. pg: sinh Cols/tên theo PostgreSQL.
func readMySQLIndexDefs(ctx context.Context, db *sql.DB, t, dst model.TableID, pg bool) ([]IndexDef, string, error) {
	rows, err := db.QueryContext(ctx, `SELECT INDEX_NAME,NON_UNIQUE,COLUMN_NAME,SUB_PART,INDEX_TYPE,COLLATION
		FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX`, t.Schema, t.Table)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	type idx struct {
		name   string
		unique bool
		typ    string
		sig    []string
		cols   []string
		manual string
	}
	var all []*idx
	for rows.Next() {
		var n, typ string
		var col, order sql.NullString
		var nonUnique int
		var prefix sql.NullInt64
		if err := rows.Scan(&n, &nonUnique, &col, &prefix, &typ, &order); err != nil {
			return nil, "", err
		}
		if len(all) == 0 || all[len(all)-1].name != n {
			all = append(all, &idx{name: n, unique: nonUnique == 0, typ: typ})
		}
		x := all[len(all)-1]
		if !col.Valid {
			x.manual = "expression index requires manual mapping"
			x.sig = append(x.sig, "<expr>")
			continue
		}
		s := col.String
		c := quote(col.String, pg)
		if prefix.Valid {
			s += fmt.Sprintf("(%d)", prefix.Int64)
			if pg {
				x.manual = "prefix index requires manual mapping"
			} else {
				c += fmt.Sprintf("(%d)", prefix.Int64)
			}
		}
		if order.String == "D" {
			s += " desc"
			c += " DESC"
		}
		x.sig = append(x.sig, s)
		x.cols = append(x.cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	var pk string
	var out []IndexDef
	for _, x := range all {
		if x.name == "PRIMARY" {
			pk = strings.Join(x.sig, ",")
			continue
		}
		kind := "n"
		if x.unique {
			kind = "u"
		}
		typ := ""
		if x.typ != "BTREE" {
			typ = x.typ + ":"
			if x.manual == "" {
				x.manual = "index type " + x.typ + " requires manual mapping"
			}
		}
		d := IndexDef{Name: x.name, Unique: x.unique, Sig: kind + "|" + typ + strings.Join(x.sig, ","), Cols: strings.Join(x.cols, ", "), Manual: x.manual}
		if pg {
			// Tên index PG chung namespace với bảng: schemagen đặt <bảng đích>_<tên index>.
			d.Name = dst.Table + "_" + x.name
			if len(d.Name) > 63 && d.Manual == "" {
				d.Manual = "mapped index name exceeds PostgreSQL 63-byte limit"
			}
		}
		out = append(out, d)
	}
	return out, pk, nil
}

// mysqlTableDef: schema bảng MySQL. withDef=false (phía source) kèm đoạn định nghĩa cột từ SHOW CREATE TABLE.
func mysqlTableDef(ctx context.Context, db *sql.DB, t, dst model.TableID, isTarget bool, ver mysqlver.Version) (TableDef, error) {
	cols, err := readMySQLColumns(ctx, db, t)
	if err != nil {
		return TableDef{}, err
	}
	var defs map[string]string
	if !isTarget {
		var tn, ddl string
		if err := db.QueryRowContext(ctx, "SHOW CREATE TABLE "+name(t, false)).Scan(&tn, &ddl); err != nil {
			return TableDef{}, err
		}
		defs = columnDefsFromCreate(ddl)
		if ver.Legacy57() {
			for k, d := range defs {
				if p, err := pinColumnDef57(d); err == nil {
					defs[k] = p
				}
			}
		}
	}
	var td TableDef
	for _, c := range cols {
		cd := ColumnDef{Name: c.name, Type: c.typ, Nullable: c.nullable, HasDefault: c.def.Valid, Charset: c.charset, Collation: c.collation}
		switch {
		case strings.Contains(c.extra, "generated") && !strings.Contains(c.extra, "default_generated"):
			cd.Unsupported = "generated column requires manual mapping"
		case strings.Contains(c.extra, "auto_increment"):
			cd.Unsupported = "AUTO_INCREMENT column requires manual mapping"
		}
		if !isTarget {
			cd.Def = defs[c.name]
		}
		td.Columns = append(td.Columns, cd)
	}
	if td.Indexes, td.PK, err = readMySQLIndexDefs(ctx, db, t, dst, false); err != nil {
		return TableDef{}, err
	}
	return td, nil
}

// pinColumnDef57 ghi rõ COLLATE mặc định của utf8mb4 (5.7) vào một đoạn định nghĩa cột, như mysqlToMySQL.
func pinColumnDef57(def string) (string, error) {
	const head, tail = "CREATE TABLE `x` (\n", "\n)"
	out, err := pinUTF8MB4Collation57(head + def + tail)
	if err != nil {
		return def, err
	}
	return strings.TrimSuffix(strings.TrimPrefix(out, head), tail), nil
}

// columnDefsFromCreate tách từng dòng định nghĩa cột (không dấu phẩy cuối) khỏi SHOW CREATE TABLE.
func columnDefsFromCreate(ddl string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(ddl, "\n") {
		l := strings.TrimSpace(line)
		if !strings.HasPrefix(l, "`") {
			continue
		}
		var n strings.Builder
		i := 1
		for i < len(l) {
			if l[i] == '`' {
				if i+1 < len(l) && l[i+1] == '`' {
					n.WriteByte('`')
					i += 2
					continue
				}
				break
			}
			n.WriteByte(l[i])
			i++
		}
		out[n.String()] = strings.TrimSuffix(l, ",")
	}
	return out
}

// sourceDefForPG: schema bảng nguồn MySQL ở dạng kiểu PG (qua typemap) cùng định nghĩa cột PG.
func sourceDefForPG(ctx context.Context, r Request, t, dst model.TableID) (TableDef, error) {
	cols, err := typemap.InspectMySQLColumns(ctx, r.Source, t.Schema, t.Table)
	if err != nil {
		return TableDef{}, err
	}
	idx, pk, err := readMySQLIndexDefs(ctx, r.Source, t, dst, true)
	if err != nil {
		return TableDef{}, err
	}
	extras := map[string]mysqlExtra{}
	rows, err := r.Source.QueryContext(ctx, columnExtrasSelect(ctx, r)+` FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?`, t.Schema, t.Table)
	if err != nil {
		return TableDef{}, err
	}
	for rows.Next() {
		var n string
		var e mysqlExtra
		if err := rows.Scan(&n, &e.Default, &e.Extra, &e.Expression); err != nil {
			rows.Close()
			return TableDef{}, err
		}
		extras[n] = e
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return TableDef{}, err
	}
	keyCols := map[string]bool{}
	for _, i := range idx {
		if i.Unique {
			for _, c := range cols {
				if strings.Contains(i.Cols, quote(c.Name, true)) {
					keyCols[c.Name] = true
				}
			}
		}
	}
	td := TableDef{PK: pk, Indexes: idx}
	for _, c := range cols {
		cd := ColumnDef{Name: c.Name, Nullable: c.Nullable}
		e := extras[c.Name]
		cd.HasDefault = e.Default.Valid
		m, err := typemap.MapMySQLColumn(c, typemap.DefaultOptions())
		if err != nil {
			cd.Unsupported = err.Error()
			cd.Type = c.ColumnType
			td.Columns = append(td.Columns, cd)
			continue
		}
		cd.Type = NormalizePGType(m.TargetType)
		def, warn, err := pgColumnDef(t, c, m, e, keyCols[c.Name])
		switch {
		case err != nil:
			cd.Unsupported = err.Error()
		case strings.Contains(e.Extra, "auto_increment"):
			cd.Unsupported = "AUTO_INCREMENT column requires manual mapping"
		case len(warn) > 0 && !c.Nullable:
			cd.Unsupported = "NOT NULL default cannot be translated to PostgreSQL"
		default:
			cd.Def = def
		}
		td.Columns = append(td.Columns, cd)
	}
	return td, nil
}

// pgSourceDef đọc schema bảng nguồn PostgreSQL: cột (kèm DEFAULT, NULL, Def) và index.
func pgSourceDef(ctx context.Context, db *sql.DB, t, dst model.TableID) (TableDef, error) {
	rows, err := db.QueryContext(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull,
		COALESCE(pg_get_expr(d.adbin, d.adrelid), ''), a.attidentity::text, a.attgenerated::text,
		COALESCE(cn.nspname, ''), COALESCE(co.collname, ''), tn.nspname
		FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
		JOIN pg_type ty ON ty.oid=a.atttypid JOIN pg_namespace tn ON tn.oid=ty.typnamespace
		LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
		LEFT JOIN pg_collation co ON co.oid=a.attcollation AND a.attcollation<>ty.typcollation
		LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace
		WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, t.Schema, t.Table)
	if err != nil {
		return TableDef{}, err
	}
	defer rows.Close()
	var cols []ColumnDef
	for rows.Next() {
		var n, typ, def, identity, generated, collschema, coll, typeschema string
		var nullable bool
		if err := rows.Scan(&n, &typ, &nullable, &def, &identity, &generated, &collschema, &coll, &typeschema); err != nil {
			return TableDef{}, err
		}
		cd := ColumnDef{
			Name:       n,
			Type:       NormalizePGType(typ),
			Nullable:   nullable,
			HasDefault: def != "" || identity != "",
		}
		switch {
		case typeschema != "pg_catalog":
			cd.Unsupported = fmt.Sprintf("user-defined type %s on %s.%s requires manual mapping", typ, t, n)
		case identity != "" || generated != "":
			cd.Unsupported = fmt.Sprintf("identity or generated column %s.%s requires manual mapping", t, n)
		case strings.Contains(def, "nextval("):
			cd.Unsupported = fmt.Sprintf("sequence default on %s.%s requires manual mapping", t, n)
		default:
			d := quote(n, true) + " " + typ
			if coll != "" {
				d += " COLLATE " + quote(collschema, true) + "." + quote(coll, true)
			}
			if def != "" {
				d += " DEFAULT " + def
			}
			if !nullable {
				d += " NOT NULL"
			}
			cd.Def = d
		}
		cols = append(cols, cd)
	}
	if err := rows.Err(); err != nil {
		return TableDef{}, err
	}
	pk, idxs, err := readPGIndexes(ctx, db, t)
	if err != nil {
		return TableDef{}, err
	}
	return TableDef{Columns: cols, PK: pk, Indexes: idxs}, nil
}

// readPGIndexes đọc PK và danh sách IndexDef của bảng PostgreSQL.
func readPGIndexes(ctx context.Context, db *sql.DB, t model.TableID) (string, []IndexDef, error) {
	irows, err := db.QueryContext(ctx, `SELECT i.relname, x.indisunique, x.indisprimary, (x.indexprs IS NOT NULL OR x.indpred IS NOT NULL), am.amname,
		COALESCE((SELECT string_agg(a.attname || CASE WHEN (x.indoption[(k.ord-1)::int] & 1)=1 THEN ' desc' ELSE '' END, ',' ORDER BY k.ord)
		   FROM unnest(x.indkey::int2[]) WITH ORDINALITY k(attnum, ord) JOIN pg_attribute a ON a.attrelid=x.indrelid AND a.attnum=k.attnum), '')
		FROM pg_index x JOIN pg_class i ON i.oid=x.indexrelid JOIN pg_class c ON c.oid=x.indrelid
		JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_am am ON am.oid=i.relam
		WHERE n.nspname=$1 AND c.relname=$2`, t.Schema, t.Table)
	if err != nil {
		return "", nil, err
	}
	defer irows.Close()
	var pk string
	var idxs []IndexDef
	for irows.Next() {
		var n, am, cols string
		var unique, primary, partial bool
		if err := irows.Scan(&n, &unique, &primary, &partial, &am, &cols); err != nil {
			return "", nil, err
		}
		if primary {
			pk = cols
			continue
		}
		kind := "n"
		if unique {
			kind = "u"
		}
		sig := kind + "|" + cols
		if am != "btree" {
			sig = kind + "|" + strings.ToUpper(am) + ":" + cols
		}
		if partial {
			sig = kind + "|<expr>:" + n
		}
		idxs = append(idxs, IndexDef{Name: n, Unique: unique, Sig: sig})
	}
	return pk, idxs, irows.Err()
}

// pgTargetDef đọc schema bảng đích PostgreSQL: cột (kiểu chuẩn hoá, NULL) và index (chữ ký theo cột).
func pgTargetDef(ctx context.Context, db *sql.DB, t model.TableID) (TableDef, error) {
	rows, err := db.QueryContext(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull
		FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, t.Schema, t.Table)
	if err != nil {
		return TableDef{}, err
	}
	defer rows.Close()
	var td TableDef
	for rows.Next() {
		var c ColumnDef
		var typ string
		if err := rows.Scan(&c.Name, &typ, &c.Nullable); err != nil {
			return TableDef{}, err
		}
		c.Type = NormalizePGType(typ)
		td.Columns = append(td.Columns, c)
	}
	if err := rows.Err(); err != nil {
		return TableDef{}, err
	}
	pk, idxs, err := readPGIndexes(ctx, db, t)
	if err != nil {
		return TableDef{}, err
	}
	td.PK = pk
	td.Indexes = idxs
	return td, nil
}
