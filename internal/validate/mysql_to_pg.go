package validate

import (
	"context"
	"database/sql"

	"strand/internal/typemap"
)

// CheckMySQLToPGTables thực hiện preflight kiểm tra tương thích schema và dữ liệu
// từ MySQL sang PostgreSQL theo spec/02-specs.md §8, §11, §12.
func CheckMySQLToPGTables(ctx context.Context, src *sql.DB, dst *sql.DB, tables []string, opts typemap.Options) ([]string, error) {
	return typemap.CheckMySQLToPGTables(ctx, src, dst, tables, opts)
}
