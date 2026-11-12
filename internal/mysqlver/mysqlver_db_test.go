package mysqlver_test

import (
	"context"
	"testing"

	"strand/internal/mysqlver"
	"strand/internal/mysqlver/mysql57test"
)

// Trên mysqld 5.7 thật: Detect đọc đúng phiên bản, câu SQL phụ thuộc phiên bản chạy được
// (binlog_row_value_options → ”, expire_logs_days thay binlog_expire_logs_seconds).
func TestMySQL57_DetectAndVersionQueries(t *testing.T) {
	_, db := mysql57test.Start(t, append([]string{"--expire-logs-days=3"}, mysql57test.SourceArgs...)...)
	ctx := context.Background()
	v, err := mysqlver.Cached(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if v.Major != 5 || v.Minor != 7 || v.IsMariaDB() || mysqlver.CheckSupported(v) != nil {
		t.Fatalf("version %+v", v)
	}
	var format, image, gtid, opts string
	if err := db.QueryRowContext(ctx, mysqlver.BinlogSettingsQuery(v)).Scan(&format, &image, &gtid, &opts); err != nil {
		t.Fatalf("binlog settings on 5.7: %v", err)
	}
	if format != "ROW" || image != "FULL" || gtid != "ON" || opts != "" {
		t.Errorf("got %q %q %q %q", format, image, gtid, opts)
	}
	secs, err := mysqlver.BinlogRetentionSeconds(ctx, db)
	if err != nil || secs != 3*86400 {
		t.Fatalf("retention = %d, %v; want %d", secs, err, 3*86400)
	}
	// Truy vấn 8.x phải lỗi trên 5.7 (xác nhận test có ý nghĩa).
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.binlog_expire_logs_seconds").Scan(&secs); err == nil {
		t.Fatal("binlog_expire_logs_seconds should not exist on 5.7")
	}
}
