package validate_test

import (
	"context"
	"strings"
	"testing"

	"strand/internal/exitcode"
	"strand/internal/mysqlver/mysql57test"
	"strand/internal/validate"
)

// Nguồn 5.7 cấu hình đúng (GTID ON, ROW, FULL, log-bin): CheckMySQLSource không được lỗi vì thiếu
// @@binlog_row_value_options (8.0.3+), và CheckMySQLSessionBinlog không khuyên SET PERSIST (8.0+).
func TestMySQL57_CheckMySQLSource_AcceptsCorrectConfig(t *testing.T) {
	_, db := mysql57test.Start(t, mysql57test.SourceArgs...)
	ctx := context.Background()
	for _, q := range []string{"CREATE DATABASE app", "CREATE TABLE app.t (id INT PRIMARY KEY, v VARCHAR(10))"} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	if err := validate.CheckMySQLSource(ctx, db, []string{"app.t"}); err != nil {
		t.Fatalf("correctly configured 5.7 must pass: %v", err)
	}
	warns, err := validate.CheckMySQLSessionBinlog(ctx, db)
	if err != nil {
		t.Fatalf("session check: %v", err)
	}
	if len(warns) == 0 || !strings.Contains(strings.Join(warns, "\n"), "binlog_row_metadata is not available") {
		t.Errorf("want the 5.7 binlog_row_metadata note, got %q", warns)
	}
	for _, w := range warns {
		if strings.Contains(w, "SET PERSIST") {
			t.Errorf("SET PERSIST does not exist on 5.7: %s", w)
		}
	}
}

// Cấu hình kiểu 5.6 (GTID tắt, không log-bin) phải bị từ chối rõ ràng (exit 3), không lỗi SQL khó hiểu.
func TestMySQL57_CheckMySQLSource_RejectsMisconfig(t *testing.T) {
	_, db := mysql57test.Start(t, "--innodb-use-native-aio=OFF", "--server-id=1", "--binlog-format=MIXED")
	err := validate.CheckMySQLSource(context.Background(), db, nil)
	if err == nil {
		t.Fatal("5.7 without GTID/ROW must be rejected")
	}
	if exitcode.GetExitCode(err) != exitcode.Rejected || !strings.Contains(err.Error(), "binlog_format must be ROW") {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), "SET GLOBAL binlog_format = 'ROW'"); err != nil {
		t.Fatal(err)
	}
	// GTID tắt được nhận (chế độ vị trí binlog), nhưng không bật log-bin thì vẫn phải bị từ chối rõ ràng.
	err = validate.CheckMySQLSource(context.Background(), db, nil)
	if err == nil || !strings.Contains(err.Error(), "binary logging is disabled") {
		t.Fatalf("want log_bin rejection, got %v", err)
	}
}
