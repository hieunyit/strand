package pipeline

import (
	"context"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"testing"

	"strand/internal/testutil"
)

// MySQL 5.6 chỉ có expire_logs_days (không có binlog_expire_logs_seconds).
func TestCompatM56_Container_RetentionSeconds56(t *testing.T) {
	db := testutil.LegacyMySQL(t, "mysql:5.6", "--expire-logs-days=3")
	if got := sourceBinlogRetentionSeconds(context.Background(), db); got != 3*86400 {
		t.Errorf("retention = %d, want %d", got, 3*86400)
	}
}

// Target MySQL 8.4 (sql_mode mặc định có NO_ZERO_DATE): phiên ghi theo targetDSN phải nhận zero date từ nguồn cũ,
// còn phiên mặc định thì từ chối (chứng minh test có ý nghĩa), và vẫn STRICT (không cắt chuỗi âm thầm).
func TestCompatM56_Container_TargetAcceptsZeroDate84(t *testing.T) {
	root := testutil.LegacyMySQL(t, "mysql:8.4")
	ctx := context.Background()
	if _, err := root.ExecContext(ctx, "CREATE DATABASE z; CREATE TABLE z.t (id INT PRIMARY KEY, d DATE, dt DATETIME, ts TIMESTAMP NULL, s VARCHAR(3))"); err != nil {
		t.Fatal(err)
	}
	if _, err := root.ExecContext(ctx, "INSERT INTO z.t (id,d) VALUES (1,'0000-00-00')"); err == nil {
		t.Fatal("default session must reject zero date on 8.4")
	}
	addr := testutil.LegacyMySQLAddr(root)
	dsn, err := targetDSN("root:dev@tcp("+addr+")/z", "p1", "i1")
	if err != nil {
		t.Fatal(err)
	}
	w, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.ExecContext(ctx, "INSERT INTO t VALUES (1,'0000-00-00','0000-00-00 00:00:00','0000-00-00 00:00:00','ab'),(2,'2024-05-00','2024-00-10 01:02:03',NULL,'')"); err != nil {
		t.Fatalf("writer session must accept zero dates: %v", err)
	}
	var d string
	if err := w.QueryRowContext(ctx, "SELECT CAST(dt AS CHAR) FROM t WHERE id=2").Scan(&d); err != nil || d != "2024-00-10 01:02:03" {
		t.Errorf("roundtrip: %q %v", d, err)
	}
	if _, err := w.ExecContext(ctx, "INSERT INTO t (id,s) VALUES (3,'toolong')"); err == nil {
		t.Error("session must remain STRICT")
	}
}
