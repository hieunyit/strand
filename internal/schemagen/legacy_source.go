package schemagen

import "context"

// columnExtrasSelect trả danh sách cột SELECT của information_schema.COLUMNS trên source. Cột
// GENERATION_EXPRESSION chỉ có từ MySQL 5.7 (5.5/5.6 không có cột generated): nếu thiếu thì dùng chuỗi rỗng.
func columnExtrasSelect(ctx context.Context, r Request) string {
	var n int
	err := r.Source.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='COLUMNS' AND COLUMN_NAME='GENERATION_EXPRESSION'`).Scan(&n)
	if err == nil && n == 0 {
		return `SELECT COLUMN_NAME,COLUMN_DEFAULT,EXTRA,'' AS GENERATION_EXPRESSION`
	}
	return `SELECT COLUMN_NAME,COLUMN_DEFAULT,EXTRA,GENERATION_EXPRESSION`
}
