package validate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// sourceBinlogVars đọc các biến binlog của source MySQL từng biến một (SHOW GLOBAL VARIABLES LIKE), vì
// một câu SELECT @@GLOBAL.a, @@GLOBAL.b hỏng cả câu khi một biến không tồn tại ở phiên bản cũ:
//   - binlog_row_image: 5.6+ (5.5 luôn FULL) -> mặc định "FULL";
//   - gtid_mode: 5.6.5+ (5.5 không có GTID) -> mặc định "OFF";
//   - binlog_row_value_options: 8.0.3+ -> mặc định "" (không có JSON partial).
func sourceBinlogVars(ctx context.Context, db *sql.DB) (format, image, gtidMode, valueOpts string, err error) {
	get := func(name, def string) (string, error) {
		var n, v string
		e := db.QueryRowContext(ctx, "SHOW GLOBAL VARIABLES LIKE '"+name+"'").Scan(&n, &v)
		if e == sql.ErrNoRows {
			return def, nil
		}
		if e != nil {
			return "", fmt.Errorf("read source variable %s: %w", name, e)
		}
		return v, nil
	}
	if format, err = get("binlog_format", ""); err != nil {
		return
	}
	if image, err = get("binlog_row_image", "FULL"); err != nil {
		return
	}
	if gtidMode, err = get("gtid_mode", "OFF"); err != nil {
		return
	}
	valueOpts, err = get("binlog_row_value_options", "")
	return
}

// sessionBinlogHint: thông điệp khi không đọc được session binlog. MySQL 5.5/5.6 không có
// performance_schema.variables_by_thread (5.7+) nên cấp quyền SELECT cũng không giúp được.
func sessionBinlogHint(qerr error) string {
	if qerr != nil && strings.Contains(qerr.Error(), "variables_by_thread") && strings.Contains(strings.ToLower(qerr.Error()), "doesn't exist") {
		return "performance_schema.variables_by_thread does not exist (MySQL < 5.7 or performance_schema disabled): per-session binlog_format cannot be verified; check open connections manually"
	}
	return ""
}
