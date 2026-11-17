package validate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// ConstraintCheckOptions specifies table and column rename rules for constraint compatibility checking.
type ConstraintCheckOptions struct {
	TableRename  map[string]string            // src "schema.table" -> dst "schema.table"
	ColumnRename map[string]map[string]string // src "schema.table" -> (srcCol -> dstCol)
}

// MapTable returns the target table ID for a source table ID under table rename rules.
func (o ConstraintCheckOptions) MapTable(src model.TableID) model.TableID {
	if o.TableRename != nil {
		if to, ok := o.TableRename[src.String()]; ok {
			if s, t, ok := strings.Cut(to, "."); ok {
				return model.TableID{Schema: s, Table: t}
			}
		}
	}
	return src
}

// MapColumn returns the target column name for a source column under column rename contracts.
func (o ConstraintCheckOptions) MapColumn(src model.TableID, col string) string {
	if o.ColumnRename != nil {
		if cols, ok := o.ColumnRename[src.String()]; ok {
			if dstCol, ok := cols[col]; ok {
				return dstCol
			}
		}
	}
	return col
}

// ColumnDescriptor holds column-level schema metadata relevant to constraints.
type ColumnDescriptor struct {
	Name      string
	Nullable  bool
	Collation string
	DataType  string
	SubPart   int64 // > 0 if prefix index in MySQL
	IsExpr    bool
}

// UniqueConstraint represents a primary key or unique index/constraint on a table.
type UniqueConstraint struct {
	Name             string
	IsPrimary        bool
	Columns          []string // ordered column names
	IsPartial        bool     // PG indpred IS NOT NULL
	HasExpr          bool     // PG indexprs IS NOT NULL or MySQL expression index
	HasPrefix        bool     // MySQL SUB_PART IS NOT NULL
	NullsNotDistinct bool     // PG 15+ indnullsnotdistinct
}

// TableConstraints contains the derived constraint metadata of a table.
type TableConstraints struct {
	Table      model.TableID
	Engine     model.Engine
	PrimaryKey *UniqueConstraint
	Uniques    []UniqueConstraint
	Columns    map[string]ColumnDescriptor
}

// CheckConstraintCompatibility checks source-vs-target constraint compatibility for a slice of tables.
func CheckConstraintCompatibility(ctx context.Context, srcDB *sql.DB, srcEngine model.Engine, dstDB *sql.DB, dstEngine model.Engine, tables []model.TableID, opts ConstraintCheckOptions) error {
	if srcDB == nil || dstDB == nil {
		return nil
	}
	for _, t := range tables {
		dstID := opts.MapTable(t)
		if err := CheckTableConstraintCompatibility(ctx, srcDB, srcEngine, dstDB, dstEngine, t, dstID, opts); err != nil {
			return err
		}
	}
	return nil
}

// CheckTableConstraintCompatibility derives unique metadata from source and target, applies rename contracts,
// and verifies that all target unique constraints are implied by source constraints.
func CheckTableConstraintCompatibility(ctx context.Context, srcDB *sql.DB, srcEngine model.Engine, dstDB *sql.DB, dstEngine model.Engine, srcTable, dstTable model.TableID, opts ConstraintCheckOptions) error {
	if srcDB == nil || dstDB == nil {
		return nil
	}

	// 1. Check if target table exists. If missing, return nil (regression: target missing table is created by schema init).
	dstTC, err := InspectTableConstraints(ctx, dstDB, dstEngine, dstTable)
	if err != nil {
		return fmt.Errorf("inspect target constraints of %s: %w", dstTable, err)
	}
	if dstTC == nil {
		return nil
	}

	// 2. Check source table constraints.
	srcTC, err := InspectTableConstraints(ctx, srcDB, srcEngine, srcTable)
	if err != nil {
		return fmt.Errorf("inspect source constraints of %s: %w", srcTable, err)
	}
	if srcTC == nil {
		return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, fmt.Sprintf("source table %s does not exist", srcTable), nil)
	}

	// 3. Verify compatibility.
	return VerifyConstraintsCompatible(srcTC, dstTC, opts)
}

// VerifyConstraintsCompatible checks if every target unique constraint is implied by at least one source constraint.
func VerifyConstraintsCompatible(srcTC, dstTC *TableConstraints, opts ConstraintCheckOptions) error {
	if dstTC == nil || srcTC == nil {
		return nil
	}

	var allSrc []UniqueConstraint
	if srcTC.PrimaryKey != nil {
		allSrc = append(allSrc, *srcTC.PrimaryKey)
	}
	allSrc = append(allSrc, srcTC.Uniques...)

	var allDst []UniqueConstraint
	if dstTC.PrimaryKey != nil {
		allDst = append(allDst, *dstTC.PrimaryKey)
	}
	allDst = append(allDst, dstTC.Uniques...)

	for _, dstC := range allDst {
		implied := false
		var failureReasons []string
		var strictSubsetFound bool
		var strictSubsetSrc string

		for _, srcC := range allSrc {
			ok, reason, isStrictSub := constraintImplies(srcTC, srcC, dstTC, dstC, opts)
			if ok {
				implied = true
				break
			}
			if isStrictSub {
				strictSubsetFound = true
				strictSubsetSrc = srcC.Name
			}
			if reason != "" {
				failureReasons = append(failureReasons, reason)
			}
		}

		if !implied {
			return formatConstraintIncompatibilityError(srcTC, dstTC, dstC, strictSubsetFound, strictSubsetSrc, failureReasons)
		}
	}

	return nil
}

func constraintImplies(srcTC *TableConstraints, srcC UniqueConstraint, dstTC *TableConstraints, dstC UniqueConstraint, opts ConstraintCheckOptions) (bool, string, bool) {
	// 1. Functional / Expression descriptors:
	if dstC.HasExpr {
		if !srcC.HasExpr {
			return false, fmt.Sprintf("target unique index %q is a functional/expression index not provably equivalent to source", dstC.Name), false
		}
		if !descriptorsMatch(srcC, dstC) {
			return false, fmt.Sprintf("target unique index %q functional expression does not match source index %q", dstC.Name, srcC.Name), false
		}
	}
	if srcC.HasExpr && !dstC.HasExpr {
		return false, fmt.Sprintf("source index %q has expressions and cannot imply standard unique constraint on target", srcC.Name), false
	}

	// 2. Prefix descriptors (SUB_PART):
	if dstC.HasPrefix {
		return false, fmt.Sprintf("target unique index %q uses prefix columns (SUB_PART) which only checks partial values", dstC.Name), false
	}
	if srcC.HasPrefix {
		return false, fmt.Sprintf("source unique index %q uses prefix columns which does not guarantee full value uniqueness", srcC.Name), false
	}

	// 3. Partial indexes (WHERE clause):
	if srcC.IsPartial {
		return false, fmt.Sprintf("source unique index %q is partial (has WHERE predicate) and does not cover all rows", srcC.Name), false
	}
	if dstC.IsPartial {
		return false, fmt.Sprintf("target unique index %q is partial and cannot be safely implied", dstC.Name), false
	}

	// 4. Map source columns to target column namespace:
	mappedSrcCols := make([]string, len(srcC.Columns))
	for i, col := range srcC.Columns {
		mappedSrcCols[i] = opts.MapColumn(srcTC.Table, col)
	}

	srcSet := make(map[string]bool, len(mappedSrcCols))
	for _, c := range mappedSrcCols {
		srcSet[c] = true
	}

	dstSet := make(map[string]bool, len(dstC.Columns))
	for _, c := range dstC.Columns {
		dstSet[c] = true
	}

	// Check if mapped source constraint columns are a subset of target unique columns:
	isSubset := true
	for c := range srcSet {
		if !dstSet[c] {
			isSubset = false
			break
		}
	}

	if !isSubset {
		// Check if target is a strict subset of source constraint:
		isStrictSub := true
		for c := range dstSet {
			if !srcSet[c] {
				isStrictSub = false
				break
			}
		}
		if isStrictSub && len(dstSet) < len(srcSet) {
			return false, fmt.Sprintf("target unique index %q on (%s) is a strict subset of source constraint %q on (%s)",
				dstC.Name, strings.Join(dstC.Columns, ", "), srcC.Name, strings.Join(mappedSrcCols, ", ")), true
		}
		return false, fmt.Sprintf("target unique index %q on (%s) is not covered by source constraint %q on (%s)",
			dstC.Name, strings.Join(dstC.Columns, ", "), srcC.Name, strings.Join(mappedSrcCols, ", ")), false
	}

	// 5. Semantics verification when srcSet is a subset of dstSet:
	isSuperset := len(srcSet) < len(dstSet)

	// a) Nullable semantics:
	for i, origCol := range srcC.Columns {
		mappedCol := mappedSrcCols[i]
		srcDesc := srcTC.Columns[origCol]
		dstDesc := dstTC.Columns[mappedCol]

		if srcDesc.Nullable {
			if isSuperset {
				// Nullable column in source constraint allows duplicate NULLs; cannot guarantee uniqueness of target superset.
				return false, fmt.Sprintf("source constraint column %q allows NULL; cannot safely imply target superset unique index %q", origCol, dstC.Name), false
			}
			// Mirrored constraint:
			if !dstDesc.Nullable && len(dstTC.Columns) > 0 {
				// Target requires NOT NULL while source allows NULL: target will reject rows containing NULL.
				return false, fmt.Sprintf("source column %q allows NULL but target column %q is NOT NULL", origCol, mappedCol), false
			}
			if dstC.NullsNotDistinct {
				return false, fmt.Sprintf("target index %q enforces NULLS NOT DISTINCT while source allows duplicate NULLs", dstC.Name), false
			}
		}
	}

	// b) Collation semantics:
	for i, origCol := range srcC.Columns {
		mappedCol := mappedSrcCols[i]
		srcDesc := srcTC.Columns[origCol]
		dstDesc := dstTC.Columns[mappedCol]

		if srcDesc.Collation != "" && dstDesc.Collation != "" {
			if !strings.EqualFold(srcDesc.Collation, dstDesc.Collation) {
				if isCollationLooser(srcDesc.Collation, dstDesc.Collation) {
					return false, fmt.Sprintf("target column %q collation %q is looser than source collation %q",
						mappedCol, dstDesc.Collation, srcDesc.Collation), false
				}
				return false, fmt.Sprintf("column %q collation differs between source (%s) and target (%s); equivalence not provable",
					mappedCol, srcDesc.Collation, dstDesc.Collation), false
			}
		}
	}

	return true, "", false
}

func descriptorsMatch(a, b UniqueConstraint) bool {
	if a.HasExpr != b.HasExpr || a.HasPrefix != b.HasPrefix || a.IsPartial != b.IsPartial {
		return false
	}
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i] != b.Columns[i] {
			return false
		}
	}
	return true
}

func formatConstraintIncompatibilityError(srcTC, dstTC *TableConstraints, dstC UniqueConstraint, strictSubset bool, strictSubsetSrc string, reasons []string) error {
	dstEngine := dstTC.Engine
	var effectMsg string
	var remediationMsg string

	if dstEngine == model.EngineMySQL {
		effectMsg = "MySQL ON DUPLICATE KEY UPDATE will match this constraint and silently overwrite distinct source rows (silent row merge / data corruption)"
		remediationMsg = fmt.Sprintf("drop unique constraint %q on target table %s or align target unique constraints with source row identity", dstC.Name, dstTC.Table)
	} else {
		effectMsg = "PostgreSQL ON CONFLICT does not arbitrate on this constraint and will abort replication with duplicate key violation error"
		remediationMsg = fmt.Sprintf("drop unique constraint %q on target table %s or create an equivalent unique constraint on source table %s", dstC.Name, dstTC.Table, srcTC.Table)
	}

	detail := "no source constraint implies it"
	if strictSubset {
		detail = fmt.Sprintf("target unique constraint is a strict subset of source constraint %q; distinct source rows will collide on target", strictSubsetSrc)
	} else if len(reasons) > 0 {
		detail = strings.Join(reasons, "; ")
	}

	code := exitcode.ErrMissingPK
	if strings.Contains(detail, "collation") && strings.Contains(detail, "looser") {
		code = exitcode.ErrCollationLooser
	}

	msg := fmt.Sprintf("target table %s: unique constraint %q on columns (%s) is incompatible with source table %s: %s (%s); remediation: %s",
		dstTC.Table, dstC.Name, strings.Join(dstC.Columns, ", "), srcTC.Table, detail, effectMsg, remediationMsg)

	return exitcode.New(code, exitcode.Rejected, msg, nil)
}

func isCollationLooser(srcColl, dstColl string) bool {
	src := strings.ToLower(strings.TrimSpace(srcColl))
	dst := strings.ToLower(strings.TrimSpace(dstColl))
	if src == dst {
		return false
	}
	srcCS := isCaseSensitive(src)
	dstCI := isCaseInsensitive(dst)
	if srcCS && dstCI {
		return true
	}
	srcAS := isAccentSensitive(src)
	dstAI := isAccentInsensitive(dst)
	if srcAS && dstAI {
		return true
	}
	if strings.Contains(src, "nopad") && !strings.Contains(dst, "nopad") {
		return true
	}
	return false
}

func isCaseSensitive(c string) bool {
	return strings.HasSuffix(c, "_bin") || strings.HasSuffix(c, "_cs") || strings.Contains(c, "_as_cs") || c == "binary" || c == "c" || c == "posix"
}

func isCaseInsensitive(c string) bool {
	return strings.HasSuffix(c, "_ci") || strings.Contains(c, "-ci") || strings.Contains(c, "case_insensitive") || strings.Contains(c, "citext")
}

func isAccentSensitive(c string) bool {
	return strings.Contains(c, "_as") || strings.HasSuffix(c, "_cs") || strings.HasSuffix(c, "_bin") || c == "binary"
}

func isAccentInsensitive(c string) bool {
	return strings.Contains(c, "_ai") || strings.Contains(c, "accent_insensitive")
}

// InspectTableConstraints derives unique constraints and column metadata for a table.
func InspectTableConstraints(ctx context.Context, db *sql.DB, engine model.Engine, table model.TableID) (*TableConstraints, error) {
	if db == nil {
		return nil, nil
	}

	exists, err := tableExists(ctx, db, engine, table)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}

	tc := &TableConstraints{
		Table:   table,
		Engine:  engine,
		Columns: make(map[string]ColumnDescriptor),
	}

	cols, err := readColumnDescriptors(ctx, db, engine, table)
	if err == nil && len(cols) > 0 {
		tc.Columns = cols
	}

	if engine == model.EnginePG {
		pk, uniques, err := readPGConstraints(ctx, db, table, tc.Columns)
		if err != nil {
			return nil, err
		}
		tc.PrimaryKey = pk
		tc.Uniques = uniques
	} else {
		pk, uniques, err := readMySQLConstraints(ctx, db, table, tc.Columns)
		if err != nil {
			return nil, err
		}
		tc.PrimaryKey = pk
		tc.Uniques = uniques
	}

	return tc, nil
}

func tableExists(ctx context.Context, db *sql.DB, engine model.Engine, table model.TableID) (bool, error) {
	var count int
	var q string
	if engine == model.EnginePG {
		q = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2"
	} else {
		q = "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	}
	if err := db.QueryRowContext(ctx, q, table.Schema, table.Table).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func readColumnDescriptors(ctx context.Context, db *sql.DB, engine model.Engine, table model.TableID) (map[string]ColumnDescriptor, error) {
	cols := make(map[string]ColumnDescriptor)
	var q string
	if engine == model.EnginePG {
		q = "SELECT column_name, is_nullable, COALESCE(collation_name, ''), data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"
	} else {
		q = "SELECT COLUMN_NAME, IS_NULLABLE, COALESCE(COLLATION_NAME, ''), DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
	}
	rows, err := db.QueryContext(ctx, q, table.Schema, table.Table)
	if err != nil {
		return cols, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, isNull, coll, dt string
		if err := rows.Scan(&name, &isNull, &coll, &dt); err != nil {
			return cols, err
		}
		cols[name] = ColumnDescriptor{
			Name:      name,
			Nullable:  strings.EqualFold(isNull, "YES"),
			Collation: coll,
			DataType:  dt,
		}
	}
	return cols, rows.Err()
}

type mysqlIndexEntry struct {
	name      string
	isPrimary bool
	cols      []string
	hasPrefix bool
	hasExpr   bool
}

func readMySQLConstraints(ctx context.Context, db *sql.DB, table model.TableID, cols map[string]ColumnDescriptor) (*UniqueConstraint, []UniqueConstraint, error) {
	q := `SELECT s.INDEX_NAME, s.COLUMN_NAME, s.SUB_PART, s.NULLABLE
          FROM information_schema.STATISTICS s
          WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.NON_UNIQUE = 0
          ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX`
	rows, err := db.QueryContext(ctx, q, table.Schema, table.Table)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]*mysqlIndexEntry)
	var indexOrder []string

	for rows.Next() {
		var idxName string
		var colName sql.NullString
		var subPart sql.NullInt64
		var nullable sql.NullString

		if err := rows.Scan(&idxName, &colName, &subPart, &nullable); err != nil {
			return nil, nil, err
		}

		entry, ok := indexMap[idxName]
		if !ok {
			entry = &mysqlIndexEntry{
				name:      idxName,
				isPrimary: strings.EqualFold(idxName, "PRIMARY"),
			}
			indexMap[idxName] = entry
			indexOrder = append(indexOrder, idxName)
		}

		if !colName.Valid {
			entry.hasExpr = true
		} else {
			entry.cols = append(entry.cols, colName.String)
			if nullable.Valid && strings.EqualFold(nullable.String, "YES") {
				if d, exists := cols[colName.String]; exists {
					d.Nullable = true
					cols[colName.String] = d
				} else {
					cols[colName.String] = ColumnDescriptor{Name: colName.String, Nullable: true}
				}
			}
		}

		if subPart.Valid && subPart.Int64 > 0 {
			entry.hasPrefix = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var pk *UniqueConstraint
	var uniques []UniqueConstraint

	sort.Strings(indexOrder)
	for _, name := range indexOrder {
		entry := indexMap[name]
		uc := UniqueConstraint{
			Name:      entry.name,
			IsPrimary: entry.isPrimary,
			Columns:   entry.cols,
			HasExpr:   entry.hasExpr,
			HasPrefix: entry.hasPrefix,
		}
		if entry.isPrimary {
			pk = &uc
		} else {
			uniques = append(uniques, uc)
		}
	}

	return pk, uniques, nil
}

type pgIndexEntry struct {
	name             string
	isPrimary        bool
	isPartial        bool
	hasExpr          bool
	nullsNotDistinct bool
	cols             []string
}

func readPGConstraints(ctx context.Context, db *sql.DB, table model.TableID, cols map[string]ColumnDescriptor) (*UniqueConstraint, []UniqueConstraint, error) {
	// First attempt: pg_class and pg_index catalog query
	q := `SELECT
		ic.relname,
		i.indisprimary,
		i.indpred IS NOT NULL AS is_partial,
		i.indexprs IS NOT NULL AS has_expr,
		k.ord,
		COALESCE(a.attname, '') AS attname,
		COALESCE(a.attnotnull, false) AS attnotnull,
		COALESCE(coll.collname, '') AS collname
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	JOIN pg_index i ON i.indrelid = c.oid
	JOIN pg_class ic ON ic.oid = i.indexrelid
	JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true
	LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
	LEFT JOIN pg_collation coll ON coll.oid = a.attcollation
	WHERE n.nspname = $1 AND c.relname = $2 AND i.indisunique AND i.indisvalid
	  AND k.ord <= i.indnkeyatts
	ORDER BY ic.relname, k.ord`

	rows, err := db.QueryContext(ctx, q, table.Schema, table.Table)
	if err != nil {
		// Fallback to information_schema for fake drivers / mocks
		return readPGConstraintsFallback(ctx, db, table, cols)
	}
	defer rows.Close()

	indexMap := make(map[string]*pgIndexEntry)
	var indexOrder []string

	for rows.Next() {
		var relname string
		var isPrimary, isPartial, hasExpr bool
		var ord int
		var attname, collname string
		var attnotnull bool

		if err := rows.Scan(&relname, &isPrimary, &isPartial, &hasExpr, &ord, &attname, &attnotnull, &collname); err != nil {
			return nil, nil, err
		}

		entry, ok := indexMap[relname]
		if !ok {
			entry = &pgIndexEntry{
				name:      relname,
				isPrimary: isPrimary,
				isPartial: isPartial,
				hasExpr:   hasExpr,
			}
			indexMap[relname] = entry
			indexOrder = append(indexOrder, relname)
		}

		if attname == "" {
			entry.hasExpr = true
		} else {
			entry.cols = append(entry.cols, attname)
			if d, exists := cols[attname]; exists {
				d.Nullable = !attnotnull
				if collname != "" {
					d.Collation = collname
				}
				cols[attname] = d
			} else {
				cols[attname] = ColumnDescriptor{
					Name:      attname,
					Nullable:  !attnotnull,
					Collation: collname,
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var pk *UniqueConstraint
	var uniques []UniqueConstraint

	sort.Strings(indexOrder)
	for _, name := range indexOrder {
		entry := indexMap[name]
		uc := UniqueConstraint{
			Name:             entry.name,
			IsPrimary:        entry.isPrimary,
			Columns:          entry.cols,
			IsPartial:        entry.isPartial,
			HasExpr:          entry.hasExpr,
			NullsNotDistinct: entry.nullsNotDistinct,
		}
		if entry.isPrimary {
			pk = &uc
		} else {
			uniques = append(uniques, uc)
		}
	}

	return pk, uniques, nil
}

func readPGConstraintsFallback(ctx context.Context, db *sql.DB, table model.TableID, cols map[string]ColumnDescriptor) (*UniqueConstraint, []UniqueConstraint, error) {
	q := `SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
	      FROM information_schema.table_constraints tc
	      JOIN information_schema.key_column_usage kcu
	        ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name
	      WHERE tc.table_schema = $1 AND tc.table_name = $2 AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
	      ORDER BY tc.constraint_name, kcu.ordinal_position`

	rows, err := db.QueryContext(ctx, q, table.Schema, table.Table)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]*pgIndexEntry)
	var indexOrder []string

	for rows.Next() {
		var cName, cType, colName string
		if err := rows.Scan(&cName, &cType, &colName); err != nil {
			return nil, nil, err
		}
		entry, ok := indexMap[cName]
		if !ok {
			entry = &pgIndexEntry{
				name:      cName,
				isPrimary: strings.EqualFold(cType, "PRIMARY KEY"),
			}
			indexMap[cName] = entry
			indexOrder = append(indexOrder, cName)
		}
		entry.cols = append(entry.cols, colName)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var pk *UniqueConstraint
	var uniques []UniqueConstraint

	sort.Strings(indexOrder)
	for _, name := range indexOrder {
		entry := indexMap[name]
		uc := UniqueConstraint{
			Name:      entry.name,
			IsPrimary: entry.isPrimary,
			Columns:   entry.cols,
		}
		if entry.isPrimary {
			pk = &uc
		} else {
			uniques = append(uniques, uc)
		}
	}
	return pk, uniques, nil
}
