package validate

import (
	"context"
	"database/sql"
	"strings"

	"strand/internal/schemagen"
)

// CheckSourceCollationsOnTarget kiểm collation của các bảng MariaDB nguồn so với đích MySQL (tables:
// "schema.table"). Collation đích không có được ánh xạ sang collation MySQL gần nhất (schemagen.MapMariaDBCollation):
//   - ánh xạ tương đương hoàn toàn (utf8mb4_nopad_bin → utf8mb4_0900_bin): dùng được cho mọi cột;
//   - ánh xạ gần đúng: chỉ cho cột không thuộc PRIMARY KEY/UNIQUE index, trả về cảnh báo nêu cột và ánh xạ;
//   - cột khóa chỉ có ánh xạ gần đúng, hoặc collation không có ánh xạ: từ chối (exit 3).
func CheckSourceCollationsOnTarget(ctx context.Context, src, dst *sql.DB, tables []string) ([]string, error) {
	has, err := schemagen.TargetCollations(ctx, dst)
	if err != nil {
		return nil, err
	}
	var warnings []string
	for _, t := range tables {
		schema, table, _ := strings.Cut(t, ".")
		p, err := schemagen.PlanMariaDBCollations(ctx, src, has, schema, table)
		if err != nil {
			return warnings, err
		}
		warnings = append(warnings, p.Warnings...)
	}
	return warnings, nil
}
