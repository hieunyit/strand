package validate

import (
	"context"
	"strings"
	"testing"

	"strand/internal/testutil"
)

// MySQL 5.5: không có gtid_mode / binlog_row_value_options / binlog_row_image / performance_schema.variables_by_thread.
// Trước đây SELECT @@GLOBAL.<biến thiếu> làm cả câu lỗi; giờ phải ra từ chối rõ ràng "gtid_mode must be ON".
func TestCompatM56_Container_CheckMySQLSource55(t *testing.T) {
	db := testutil.LegacyMySQL(t, "mysql:5.5")
	ctx := context.Background()
	format, image, gtid, vopts, err := sourceBinlogVars(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if format != "ROW" || image != "FULL" || gtid != "OFF" || vopts != "" {
		t.Errorf("vars: %q %q %q %q", format, image, gtid, vopts)
	}
	err = CheckMySQLSource(ctx, db, nil)
	if err == nil || !strings.Contains(err.Error(), "gtid_mode must be ON") {
		t.Errorf("want gtid_mode rejection, got %v", err)
	}
	warns, err := CheckMySQLSessionBinlog(ctx, db)
	if err != nil {
		t.Fatalf("session check must degrade to a warning: %v", err)
	}
	if len(warns) == 0 {
		t.Error("expected a warning")
	}
}

func TestCompatM56_Container_CheckMySQLSource56(t *testing.T) {
	db := testutil.LegacyMySQL(t, "mysql:5.6", "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--log-slave-updates=ON")
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE v56; CREATE TABLE v56.a (id INT PRIMARY KEY) ENGINE=InnoDB; CREATE TABLE v56.n (id INT) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	if err := CheckMySQLSource(ctx, db, []string{"v56.a"}); err != nil {
		t.Errorf("5.6 with GTID+ROW must pass: %v", err)
	}
	if err := CheckMySQLSource(ctx, db, []string{"v56.n"}); err == nil || !strings.Contains(err.Error(), "no primary key") {
		t.Errorf("want missing PK, got %v", err)
	}
	if _, err := CheckMySQLSessionBinlog(ctx, db); err != nil {
		t.Errorf("session check: %v", err)
	}
}
