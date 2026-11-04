package typemap

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// PGColumn chứa metadata của một cột trong PostgreSQL.
type PGColumn struct {
	Name          string
	DataType      string
	UDTName       string
	CollationName string
}

// PGCollationMeta chứa thông tin chi tiết về collation trong PostgreSQL.
type PGCollationMeta struct {
	Name            string
	IsDeterministic bool
	ICULocale       string
}

// InspectMySQLColumns đọc danh sách cột từ information_schema.COLUMNS của MySQL.
func InspectMySQLColumns(ctx context.Context, db *sql.DB, schema, table string) ([]MySQLColumn, error) {
	query := `
		SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE,
		       COALESCE(CHARACTER_SET_NAME, ''), COALESCE(COLLATION_NAME, '')
		  FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		 ORDER BY ORDINAL_POSITION`

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("inspect mysql columns for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var cols []MySQLColumn
	for rows.Next() {
		var c MySQLColumn
		var isNullable, charset, collation string
		if err := rows.Scan(&c.Name, &c.DataType, &c.ColumnType, &isNullable, &charset, &collation); err != nil {
			return nil, fmt.Errorf("scan mysql column: %w", err)
		}
		c.DataType = strings.ToLower(c.DataType)
		c.Nullable = strings.EqualFold(isNullable, "YES")
		c.Charset = charset
		c.Collation = collation
		c.Unsigned = strings.Contains(strings.ToLower(c.ColumnType), "unsigned")
		if c.DataType == "enum" || c.DataType == "set" {
			c.EnumValues = ParseEnumValues(c.ColumnType)
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read mysql columns rows: %w", err)
	}
	return cols, nil
}

// InspectMySQLPrimaryKey đọc danh sách tên các cột khóa chính từ MySQL.
func InspectMySQLPrimaryKey(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	query := `
		SELECT kcu.COLUMN_NAME
		  FROM information_schema.TABLE_CONSTRAINTS tc
		  JOIN information_schema.KEY_COLUMN_USAGE kcu
		    ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
		   AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
		   AND tc.TABLE_NAME = kcu.TABLE_NAME
		   AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
		 WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
		   AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		 ORDER BY kcu.ORDINAL_POSITION`

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("inspect mysql primary key for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan mysql primary key: %w", err)
		}
		pks = append(pks, col)
	}
	return pks, rows.Err()
}

// InspectPGColumns đọc danh sách cột từ information_schema.columns của PostgreSQL.
func InspectPGColumns(ctx context.Context, db *sql.DB, schema, table string) ([]PGColumn, error) {
	query := `
		SELECT column_name, data_type, udt_name, COALESCE(collation_name, '')
		  FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = $2
		 ORDER BY ordinal_position`

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("inspect pg columns for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var cols []PGColumn
	for rows.Next() {
		var c PGColumn
		if err := rows.Scan(&c.Name, &c.DataType, &c.UDTName, &c.CollationName); err != nil {
			return nil, fmt.Errorf("scan pg column: %w", err)
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// InspectPGPrimaryKey đọc danh sách cột khóa chính từ PostgreSQL.
func InspectPGPrimaryKey(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	query := `
		SELECT kcu.column_name
		  FROM information_schema.table_constraints tc
		  JOIN information_schema.key_column_usage kcu
		    ON tc.constraint_schema = kcu.constraint_schema
		   AND tc.table_schema = kcu.table_schema
		   AND tc.table_name = kcu.table_name
		   AND tc.constraint_name = kcu.constraint_name
		 WHERE tc.table_schema = $1 AND tc.table_name = $2
		   AND tc.constraint_type = 'PRIMARY KEY'
		 ORDER BY kcu.ordinal_position`

	rows, err := db.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, fmt.Errorf("inspect pg primary key for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan pg primary key: %w", err)
		}
		pks = append(pks, col)
	}
	return pks, rows.Err()
}

// InspectPGCollation kiểm tra thuộc tính của collation trong PostgreSQL (đặc biệt tính deterministic và ICU locale).
// Cột locale tên `colliculocale` (PG 15–16) và `colllocale` (PG 17+): đọc qua to_jsonb để chạy được mọi bản.
// Lưu ý: tra theo tên nên mơ hồ khi cùng tên tồn tại ở nhiều schema/encoding; để kiểm tra cột khóa
// hãy dùng InspectPGColumnCollation (resolve theo OID thực của cột).
func InspectPGCollation(ctx context.Context, db *sql.DB, collationName string) (*PGCollationMeta, error) {
	query := `
		SELECT c.collname, c.collisdeterministic,
		       COALESCE(to_jsonb(c)->>'colllocale', to_jsonb(c)->>'colliculocale', '')
		  FROM pg_collation c
		 WHERE c.collname = $1
		 LIMIT 1`

	var meta PGCollationMeta
	err := db.QueryRowContext(ctx, query, collationName).Scan(&meta.Name, &meta.IsDeterministic, &meta.ICULocale)
	if err != nil {
		if err == sql.ErrNoRows {
			// Collation có thể là libc mặc định hoặc C/POSIX
			return &PGCollationMeta{
				Name:            collationName,
				IsDeterministic: true,
			}, nil
		}
		return nil, fmt.Errorf("inspect pg collation %s: %w", collationName, err)
	}
	return &meta, nil
}

// InspectPGColumnCollation xác định collation THỰC của một cột qua pg_attribute.attcollation (OID) join
// pg_collation, thay vì tra theo tên (tên collation không duy nhất: có thể trùng giữa các schema/encoding).
// Trả nil khi cột không có collation (attcollation = 0, kiểu không collatable). Collation OID mặc định
// (DEFAULT_COLLATION_OID, collname "default") là collation của database, luôn deterministic.
// Lỗi khi không tìm thấy cột: không xác định được thì không kết luận "an toàn".
func InspectPGColumnCollation(ctx context.Context, db *sql.DB, schema, table, column string) (*PGCollationMeta, error) {
	query := `
		SELECT a.attcollation::bigint,
		       COALESCE(co.collname, ''), COALESCE(co.collisdeterministic, true),
		       COALESCE(to_jsonb(co)->>'colllocale', to_jsonb(co)->>'colliculocale', '')
		  FROM pg_catalog.pg_attribute a
		  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		  LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
		 WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3
		   AND a.attnum > 0 AND NOT a.attisdropped`

	var oid int64
	var meta PGCollationMeta
	err := db.QueryRowContext(ctx, query, schema, table, column).Scan(&oid, &meta.Name, &meta.IsDeterministic, &meta.ICULocale)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("inspect pg collation of %s.%s.%s: column not found", schema, table, column)
		}
		return nil, fmt.Errorf("inspect pg collation of %s.%s.%s: %w", schema, table, column, err)
	}
	if oid == 0 {
		return nil, nil
	}
	if meta.Name == "" {
		// OID có trong pg_attribute nhưng không có trong pg_collation: bất thường, không đoán.
		return nil, fmt.Errorf("inspect pg collation of %s.%s.%s: collation oid %d not found in pg_collation", schema, table, column, oid)
	}
	return &meta, nil
}
