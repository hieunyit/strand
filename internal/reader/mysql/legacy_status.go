package mysql

import (
	"context"
	"database/sql"
	"fmt"
)

// BinlogStatus là kết quả SHOW BINARY LOG STATUS / SHOW MASTER STATUS của source.
type BinlogStatus struct {
	File        string
	Position    uint64
	ExecutedSet string // Executed_Gtid_Set; rỗng với MySQL 5.5 (không có GTID)
}

// StatusQuerier: *sql.DB hoặc *sql.Conn (đọc vị trí trên đúng kết nối đang giữ khóa).
type StatusQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ReadBinlogStatus đọc vị trí binlog hiện tại của source. MySQL 8.4 bỏ SHOW MASTER STATUS (đổi thành
// SHOW BINARY LOG STATUS, có từ 8.2); 5.5–8.0 chỉ có SHOW MASTER STATUS. Thử cú pháp mới trước rồi lùi về cũ.
// Số cột khác nhau theo phiên bản (5.5: 4 cột, 5.6+: 5 cột) nên đọc theo tên cột.
func ReadBinlogStatus(ctx context.Context, db StatusQuerier) (BinlogStatus, error) {
	var lastErr error
	for _, q := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		st, err := queryBinlogStatus(ctx, db, q)
		if err == nil {
			return st, nil
		}
		lastErr = err
	}
	return BinlogStatus{}, fmt.Errorf("read source binlog status: %w", lastErr)
}

func queryBinlogStatus(ctx context.Context, db StatusQuerier, q string) (BinlogStatus, error) {
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return BinlogStatus{}, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return BinlogStatus{}, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return BinlogStatus{}, err
		}
		return BinlogStatus{}, fmt.Errorf("%s: binary logging is disabled (no row)", q)
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return BinlogStatus{}, err
	}
	var st BinlogStatus
	for i, c := range cols {
		switch c {
		case "File":
			st.File = vals[i].String
		case "Position":
			fmt.Sscan(vals[i].String, &st.Position)
		case "Executed_Gtid_Set":
			st.ExecutedSet = vals[i].String
		}
	}
	return st, nil
}
