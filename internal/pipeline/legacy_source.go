package pipeline

import (
	"context"
	"database/sql"
)

// sourceBinlogRetentionSeconds đọc thời gian giữ binlog của source (giây; <=0 hoặc lỗi = không biết).
// MySQL 8.0+: binlog_expire_logs_seconds. MySQL 5.5–5.7 chỉ có expire_logs_days (ngày, 0 = không tự xóa).
func sourceBinlogRetentionSeconds(ctx context.Context, db *sql.DB) int64 {
	var expire int64
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.binlog_expire_logs_seconds").Scan(&expire); err == nil {
		return expire
	}
	var days int64
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.expire_logs_days").Scan(&days); err == nil && days > 0 {
		return days * 86400
	}
	return 0
}

// mysqlTargetSessionSQLMode: sql_mode cố định cho phiên ghi vào target MySQL. Giữ STRICT (không cắt/đổi
// giá trị âm thầm) nhưng BỎ NO_ZERO_DATE và NO_ZERO_IN_DATE: nguồn MySQL 5.5/5.6/5.7 cho phép
// '0000-00-00' / '2024-00-10' (sql_mode mặc định cũ không cấm) và binlog mang đúng các giá trị đó; target
// 8.x mặc định cấm nên nếu giữ sẽ lỗi 1292 khi áp dụng. Không có NO_BACKSLASH_ESCAPES / ANSI_QUOTES để
// driver escape phía client luôn đúng, bất kể sql_mode toàn cục của target.
const mysqlTargetSessionSQLMode = "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
