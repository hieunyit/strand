package validate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/rowkey"
)

// missingKeysError gom các gợi ý (rowkey.Suggest) của mọi bảng thiếu khóa thành một lỗi missing_pk (exit 3).
func missingKeysError(missing []string) error {
	if len(missing) == 0 {
		return nil
	}
	return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected,
		fmt.Sprintf("%d table(s) have no usable row key:\n\n%s", len(missing), strings.Join(missing, "\n\n")), nil)
}

// CheckPGRowKey kiểm khóa hàng của các bảng nguồn PostgreSQL: PRIMARY KEY, hoặc (không có PK) index của
// REPLICA IDENTITY USING INDEX (unique, không partial, không deferrable, mọi cột NOT NULL). Bảng thiếu khóa được
// gom vào một lỗi missing_pk kèm `ALTER TABLE ... REPLICA IDENTITY USING INDEX ...` (nếu có index đủ điều kiện)
// hoặc câu thêm cột khóa. REPLICA IDENTITY FULL không PK: chưa hỗ trợ (xem rowkey.Suggest).
func CheckPGRowKey(ctx context.Context, db *sql.DB, tables []model.TableID) (map[model.TableID]string, error) {
	labels, _, err := CheckPGRowKeyOpts(ctx, db, tables, KeylessOpts{})
	return labels, err
}

// CheckPGRowKeyOpts như CheckPGRowKey, kèm chế độ keyless: bảng không khóa vẫn qua nếu keyless được phép
// (seed + kiểu cột + REPLICA IDENTITY FULL); warnings liệt kê các bảng chạy keyless.
func CheckPGRowKeyOpts(ctx context.Context, db *sql.DB, tables []model.TableID, ko KeylessOpts) (labels map[model.TableID]string, warnings []string, err error) {
	labels = map[model.TableID]string{}
	var missing []string
	for _, t := range tables {
		var oid, est int64
		if err := db.QueryRowContext(ctx, `SELECT c.oid::bigint, GREATEST(c.reltuples, 0)::bigint FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2`, t.Schema, t.Table).Scan(&oid, &est); err != nil {
			return nil, nil, fmt.Errorf("inspect table %s: %w", t, err)
		}
		res, err := rowkey.ResolvePG(ctx, db, t, oid)
		if err != nil {
			return nil, nil, fmt.Errorf("inspect table %s: %w", t, err)
		}
		if res.Key != nil {
			labels[t] = res.Key.Label()
			continue
		}
		why := ""
		if ko.On {
			cols, types, err := pgColTypes(ctx, db, oid)
			if err != nil {
				return nil, nil, fmt.Errorf("inspect table %s: %w", t, err)
			}
			if why = ko.Decide(t.String(), cols, types, true, res.Info.RelReplIdent); why == "" {
				labels[t] = rowkey.KeylessLabel
				warnings = append(warnings, KeylessWarnings(t.String())...)
				continue
			}
			if strings.Contains(why, "REPLICA IDENTITY FULL") {
				why += fmt.Sprintf(`: ALTER TABLE "%s"."%s" REPLICA IDENTITY FULL;`, t.Schema, t.Table)
			}
		}
		missing = append(missing, rowkey.Suggest(rowkey.SuggestInput{
			Flavor: "postgres", Table: t, EstRows: est, PGIndex: res.Suggested, Notes: rowkey.Explain(res.Rejected), KeylessWhy: why,
		}))
	}
	return labels, warnings, missingKeysError(missing)
}

// uniqueIndexCovers requires an exact identity, for both engines. A subset unique
// cannot represent all rows allowed by the source key.
func uniqueIndexCovers(idx map[string][]string, key []string, exactSet bool) (string, bool) {
	want := map[string]bool{}
	for _, c := range key {
		want[c] = true
	}
	names := make([]string, 0, len(idx))
	for n := range idx {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		cols := idx[n]
		if len(cols) == 0 || len(cols) != len(want) {
			continue
		}
		ok := true
		for _, c := range cols {
			if !want[c] {
				ok = false
			}
		}
		if ok {
			return n, true
		}
	}
	return "", false
}

// TargetKeyError là lỗi (exit 3) khi bảng target đã có nhưng thiếu unique index khớp khóa hàng của nguồn.
func TargetKeyError(src, dst model.TableID, key rowkey.Key, engine model.Engine) error {
	cols := make([]string, len(key.Columns))
	for i, c := range key.Columns {
		if engine == model.EnginePG {
			cols[i] = `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
		} else {
			cols[i] = "`" + strings.ReplaceAll(c, "`", "``") + "`"
		}
	}
	fix := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD UNIQUE KEY (%s);", dst.Schema, dst.Table, strings.Join(cols, ", "))
	if engine == model.EnginePG {
		fix = fmt.Sprintf(`CREATE UNIQUE INDEX ON "%s"."%s" (%s);`, dst.Schema, dst.Table, strings.Join(cols, ", "))
	}
	return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, fmt.Sprintf(
		"source table %s has no primary key and uses %s as its row key, but target table %s has no matching unique index/constraint: "+
			"upserts would insert duplicate rows instead of updating. Create it on the target (or drop the target table and run `strand schema init`): %s",
		src, key.Label(), dst, fix), nil)
}

// CheckTargetKey: bảng target dst (nếu đã tồn tại) phải có unique index khớp khóa key của nguồn. Chỉ áp dụng khi
// khóa KHÔNG phải PRIMARY KEY (bảng có PK giữ hành vi cũ).
func CheckTargetKey(ctx context.Context, dstDB *sql.DB, engine model.Engine, src, dst model.TableID, key rowkey.Key) error {
	if key.IsPrimary() {
		return nil
	}
	idx := map[string][]string{}
	unusable := map[string]bool{}
	var q string
	if engine == model.EnginePG {
		q = `SELECT ic.relname, a.attname
		  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		  JOIN pg_index i ON i.indrelid = c.oid JOIN pg_class ic ON ic.oid = i.indexrelid
		  JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true
		  JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
		 WHERE n.nspname = $1 AND c.relname = $2 AND i.indisunique AND i.indisvalid AND i.indimmediate
		   AND i.indpred IS NULL AND i.indexprs IS NULL AND k.ord <= i.indnkeyatts
		 ORDER BY ic.relname, k.ord`
	} else {
		// Include every unique constraint: even an unusable arbiter can cause
		// ON DUPLICATE KEY UPDATE to overwrite a different source row.
		q = `SELECT s.INDEX_NAME, s.COLUMN_NAME, s.SUB_PART FROM information_schema.STATISTICS s
         WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.NON_UNIQUE = 0
         ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX`
	}
	args := []any{dst.Schema, dst.Table}
	var exists int
	existsQ := "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	if engine == model.EnginePG {
		existsQ = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2"
	}
	if err := dstDB.QueryRowContext(ctx, existsQ, dst.Schema, dst.Table).Scan(&exists); err != nil {
		return fmt.Errorf("inspect target table %s: %w", dst, err)
	}
	if exists == 0 {
		return nil // sẽ được schema init tạo cùng unique index của nguồn
	}
	rows, err := dstDB.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("inspect target indexes of %s: %w", dst, err)
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		var c sql.NullString
		var sub sql.NullInt64
		var err error
		if engine == model.EnginePG {
			err = rows.Scan(&n, &c)
		} else {
			err = rows.Scan(&n, &c, &sub)
		}
		if err != nil {
			return err
		}
		if !c.Valid || sub.Valid {
			unusable[n] = true
		}
		if c.Valid {
			idx[n] = append(idx[n], c.String)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for n := range unusable {
		delete(idx, n)
	}
	if _, ok := uniqueIndexCovers(idx, key.Columns, true); ok {
		if engine == model.EngineMySQL {
			for n, cols := range idx {
				// A strict subset provably excludes rows allowed by this source
				// identity. Other constraints need source-index metadata to compare;
				// unrelated mirrored unique constraints remain supported.
				if len(cols) > 0 && len(cols) < len(key.Columns) && indexContainsIdentity(key.Columns, cols) {
					return incompatibleTargetIndex(src, dst, n)
				}
			}
		}
		return nil
	}
	return TargetKeyError(src, dst, key, engine)
}

// CheckKeys giải khóa hàng của từng bảng nguồn (MySQL/MariaDB/PostgreSQL), kiểm bảng target đã tồn tại có unique
// index khớp khi khóa không phải PRIMARY KEY, và trả các dòng "table: key ..." để in cho người vận hành.
func CheckKeys(ctx context.Context, src *sql.DB, srcKind string, dst *sql.DB, dstKind string, tables []model.TableID, rename map[string]string, ko KeylessOpts) ([]string, error) {
	var lines []string
	keys := map[model.TableID]*rowkey.Key{}
	if strings.EqualFold(srcKind, "postgres") {
		_, kw, err := CheckPGRowKeyOpts(ctx, src, tables, ko)
		lines = append(lines, prefixWarn(kw)...)
		if err != nil {
			return lines, err
		}
		for _, t := range tables {
			var oid int64
			if err := src.QueryRowContext(ctx, `SELECT c.oid::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = $1 AND c.relname = $2`, t.Schema, t.Table).Scan(&oid); err != nil {
				return nil, err
			}
			res, err := rowkey.ResolvePG(ctx, src, t, oid)
			if err != nil {
				return nil, err
			}
			keys[t] = res.Key
		}
	} else {
		for _, t := range tables {
			res, err := rowkey.ResolveMySQL(ctx, src, t.Schema, t.Table)
			if err != nil {
				return nil, err
			}
			keys[t] = res.Key
		}
	}
	engine := model.EngineMySQL
	if strings.EqualFold(dstKind, "postgres") {
		engine = model.EnginePG
	}
	srcEngine := model.EngineMySQL
	if strings.EqualFold(srcKind, "postgres") {
		srcEngine = model.EnginePG
	}
	for _, t := range tables {
		k := keys[t]
		if k == nil || k.IsPrimary() {
			continue
		}
		lines = append(lines, fmt.Sprintf("table %s: no primary key, row key is %s", t, k.Label()))
		dstID := t
		if to, ok := rename[t.String()]; ok {
			if s, tb, ok := strings.Cut(to, "."); ok {
				dstID = model.TableID{Schema: s, Table: tb}
			}
		}
		if err := CheckTargetKey(ctx, dst, engine, t, dstID, *k); err != nil {
			return lines, err
		}
	}
	if dst != nil && src != nil {
		opts := ConstraintCheckOptions{TableRename: rename}
		if err := CheckConstraintCompatibility(ctx, src, srcEngine, dst, engine, tables, opts); err != nil {
			return lines, err
		}
	}
	return lines, nil
}

// CheckRowKeys validates row keys and constraint compatibility across source and target databases.
func CheckRowKeys(ctx context.Context, src *sql.DB, srcKind string, dst *sql.DB, dstKind string, tables []model.TableID, rename map[string]string, ko KeylessOpts) ([]string, error) {
	return CheckKeys(ctx, src, srcKind, dst, dstKind, tables, rename, ko)
}

// KeylessOpts: cấu hình chế độ keyless (tables.keyless=full_row) cho validate.
type KeylessOpts struct {
	On         bool
	AllowFloat bool
	// Seeded(table "schema.table"): dữ liệu đầu của bảng đến từ seed/start_at (không snapshot logic).
	Seeded func(table string) bool
}

// KeylessWarnings: cảnh báo in cho mỗi bảng sẽ chạy keyless.
func KeylessWarnings(table string) []string {
	return []string{
		fmt.Sprintf("table %s runs in keyless mode (no primary/unique key): rows are matched on ALL columns, one at a time; "+
			"each UPDATE/DELETE scans the target table unless an index on the target helps (add a non-unique index on selective columns)", table),
		fmt.Sprintf("table %s keyless: `strand verify` compares row count and an order-independent checksum only, and cannot repair; "+
			"a mismatch means re-seeding the table", table),
		fmt.Sprintf("table %s keyless: its changes are applied serially (one Txn at a time, in commit order); other tables stay parallel", table),
	}
}

// Decide: bảng không có khóa có chạy keyless được không. why rỗng = được.
func (o KeylessOpts) Decide(table string, cols []string, types map[string]string, pg bool, pgReplIdent string) (why string) {
	if !o.On {
		return ""
	}
	if o.Seeded == nil || !o.Seeded(table) {
		return "tables.keyless=full_row requires the table's data to come from a seed (bootstrap.default: seed | start_at, or bootstrap.per_table[...].mode: seed); a logical snapshot of a keyless table is refused because it cannot split or reconcile key ranges"
	}
	if p := rowkey.KeylessProblem(cols, types, pg, o.AllowFloat); p != "" {
		return p
	}
	if pg && pgReplIdent != "f" {
		return "the source table needs REPLICA IDENTITY FULL"
	}
	return ""
}

func mysqlColTypes(ctx context.Context, db *sql.DB, schema, table string) ([]string, map[string]string, error) {
	rs, err := db.QueryContext(ctx, `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, schema, table)
	if err != nil {
		return nil, nil, err
	}
	defer rs.Close()
	var cols []string
	types := map[string]string{}
	for rs.Next() {
		var c, t string
		if err := rs.Scan(&c, &t); err != nil {
			return nil, nil, err
		}
		cols = append(cols, c)
		types[c] = strings.ToLower(t)
	}
	return cols, types, rs.Err()
}

func pgColTypes(ctx context.Context, db *sql.DB, oid int64) ([]string, map[string]string, error) {
	rs, err := db.QueryContext(ctx, `SELECT a.attname, base.typname FROM pg_attribute a
        JOIN LATERAL (
            WITH RECURSIVE types AS (
                SELECT t.oid, t.typname, t.typbasetype, t.typelem, t.typcategory, 0 AS depth FROM pg_type t WHERE t.oid=a.atttypid
                UNION ALL SELECT t.oid, t.typname, t.typbasetype, t.typelem, t.typcategory, p.depth+1 FROM pg_type t JOIN types p ON t.oid=CASE WHEN p.typbasetype<>0 THEN p.typbasetype WHEN p.typcategory='A' THEN p.typelem ELSE 0 END
            ) SELECT typname FROM types WHERE typbasetype=0 ORDER BY (typname IN ('citext','float4','float8','json','xml','point','line','lseg','box','path','polygon','circle')) DESC, depth LIMIT 1
        ) base ON true
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '' ORDER BY a.attnum`, oid)
	if err != nil {
		return nil, nil, err
	}
	defer rs.Close()
	var cols []string
	types := map[string]string{}
	for rs.Next() {
		var c, t string
		if err := rs.Scan(&c, &t); err != nil {
			return nil, nil, err
		}
		cols = append(cols, c)
		types[c] = t
	}
	return cols, types, rs.Err()
}

// prefixWarn: cảnh báo in kèm tiền tố "warning: " cùng dòng key của validate.
func prefixWarn(ws []string) []string {
	out := make([]string, len(ws))
	for i, w := range ws {
		out[i] = "warning: " + w
	}
	return out
}

func indexContainsIdentity(cols, key []string) bool {
	have := make(map[string]bool, len(cols))
	for _, c := range cols {
		have[c] = true
	}
	for _, c := range key {
		if !have[c] {
			return false
		}
	}
	return true
}
func incompatibleTargetIndex(src, dst model.TableID, name string) error {
	return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, fmt.Sprintf("target table %s: unique index %q can conflict between distinct rows from source %s; align target unique constraints with the complete source row identity", dst, name, src), nil)
}
