package validate

import (
	"context"
	"database/sql"
	"fmt"
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

// ReplicaStatusWarning: governor đọc replica lag bằng SHOW REPLICA/SLAVE STATUS. Thiếu quyền (MariaDB
// 10.5+ cần SLAVE MONITOR, REPLICATION CLIENT không còn đủ) thì tín hiệu replica_lag của breaker bị tắt
// âm thầm: cảnh báo kèm câu GRANT.
func ReplicaStatusWarning(ctx context.Context, db *sql.DB) string {
	var firstErr error
	for _, q := range []string{"SHOW REPLICA STATUS", "SHOW SLAVE STATUS"} {
		rows, err := db.QueryContext(ctx, q)
		if err == nil {
			rows.Close()
			return ""
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return fmt.Sprintf("the strand user cannot read replica status (%v): the breaker's replica_lag signal is disabled; "+
		"grant it: MariaDB 10.5+ GRANT SLAVE MONITOR ON *.* TO 'strand'@'%%'; MySQL GRANT REPLICATION CLIENT ON *.* TO 'strand'@'%%'", firstErr)
}
