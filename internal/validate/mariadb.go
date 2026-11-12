package validate

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"strand/internal/exitcode"
)

// CheckSourceCollationsOnTarget từ chối (exit 3) khi cột/bảng nguồn dùng collation mà target MySQL không có
// (vd collation NO PAD riêng của MariaDB như utf8mb4_general_nopad_ci): tạo bảng trên target sẽ lỗi, và
// đổi sang collation khác làm thay đổi phép so sánh khóa. tables: "schema.table".
func CheckSourceCollationsOnTarget(ctx context.Context, src, dst *sql.DB, tables []string) error {
	targetHas := map[string]bool{}
	rows, err := dst.QueryContext(ctx, "SELECT COLLATION_NAME FROM information_schema.COLLATIONS")
	if err != nil {
		return fmt.Errorf("read target collations: %w", err)
	}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			rows.Close()
			return err
		}
		targetHas[strings.ToLower(c)] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	missing := map[string][]string{}
	for _, t := range tables {
		schema, table, _ := strings.Cut(t, ".")
		r, err := src.QueryContext(ctx, `SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLLATION_NAME IS NOT NULL
			UNION SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND TABLE_COLLATION IS NOT NULL`,
			schema, table, schema, table)
		if err != nil {
			return fmt.Errorf("read source collations of %s: %w", t, err)
		}
		for r.Next() {
			var c string
			if err := r.Scan(&c); err != nil {
				r.Close()
				return err
			}
			if !targetHas[strings.ToLower(normalizeCollationName(c))] {
				missing[c] = append(missing[c], t)
			}
		}
		r.Close()
		if err := r.Err(); err != nil {
			return err
		}
	}
	if len(missing) == 0 {
		return nil
	}
	var parts []string
	for c, ts := range missing {
		parts = append(parts, fmt.Sprintf("%s (%s)", c, strings.Join(ts, ", ")))
	}
	sort.Strings(parts)
	return exitcode.New(exitcode.ErrBinlogConfigInvalid, exitcode.Rejected,
		"source collation(s) not available on the target: "+strings.Join(parts, "; ")+
			"; convert these columns/tables on the source to a collation the target has (e.g. utf8mb4_general_ci, utf8mb4_bin) or exclude the tables", nil)
}

// normalizeCollationName: "utf8_*" của nguồn cũ là "utf8mb3_*" trên MySQL 8.x.
func normalizeCollationName(c string) string {
	if len(c) >= 5 && strings.EqualFold(c[:5], "utf8_") {
		return "utf8mb3_" + c[5:]
	}
	return c
}
