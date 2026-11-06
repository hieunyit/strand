package validate_test

// Regression tests from the Gate 1 code review (2026-11-02).

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/validate"
)

// lastMySQLAddr là host:port của container MySQL vừa khởi chạy (để mở kết nối bằng user khác).
var lastMySQLAddr string

func startMySQL(t *testing.T) *sql.DB {
	t.Helper()
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.4",
			ExposedPorts: []string{"3306/tcp"},
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev", "MYSQL_DATABASE": "app"},
			Cmd:          []string{"--server-id=1", "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--binlog-format=ROW", "--binlog-row-image=FULL", "--innodb-use-native-aio=OFF"},
			WaitingFor:   wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "3306")
	lastMySQLAddr = net.JoinHostPort(host, port.Port())
	db, err := sql.Open("mysql", fmt.Sprintf("root:dev@tcp(%s)/app", lastMySQLAddr))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60 && db.Ping() != nil; i++ {
		time.Sleep(500 * time.Millisecond)
	}
	return db
}

// Gate 1: "validate từ chối partial JSON". Preflight never connected to the source,
// so binlog settings and missing PKs were never checked.
func TestValidateChecksLiveMySQLSource(t *testing.T) {
	ctx := context.Background()
	db := startMySQL(t)
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE TABLE app.orders (id INT PRIMARY KEY, doc JSON)")
	exec("CREATE TABLE app.nopk (a INT, b INT)")

	if err := validate.CheckMySQLSource(ctx, db, []string{"app.orders"}); err != nil {
		t.Fatalf("correctly configured source rejected: %v", err)
	}

	exec("SET GLOBAL binlog_row_value_options = 'PARTIAL_JSON'")
	err := validate.CheckMySQLSource(ctx, db, []string{"app.orders"})
	if exitcode.GetExitCode(err) != exitcode.Rejected {
		t.Fatalf("partial JSON must be rejected with exit 3, got %v", err)
	}
	exec("SET GLOBAL binlog_row_value_options = ''")

	exec("SET GLOBAL binlog_row_image = 'MINIMAL'")
	if err := validate.CheckMySQLSource(ctx, db, []string{"app.orders"}); exitcode.GetExitCode(err) != exitcode.Rejected {
		t.Fatalf("binlog_row_image=MINIMAL must be rejected, got %v", err)
	}
	exec("SET GLOBAL binlog_row_image = 'FULL'")

	err = validate.CheckMySQLSource(ctx, db, []string{"app.orders", "app.nopk"})
	var se *exitcode.Error
	if exitcode.GetExitCode(err) != exitcode.Rejected || !asExit(err, &se) || se.Code != exitcode.ErrMissingPK {
		t.Fatalf("table without PK must be rejected with missing_pk, got %v", err)
	}

	// review G1: PRIMARY KEY có cột prefix bị từ chối với missing_pk.
	exec("CREATE TABLE app.pkpfx (code VARCHAR(40) NOT NULL, PRIMARY KEY (code(10)))")
	err = validate.CheckMySQLSource(ctx, db, []string{"app.orders", "app.pkpfx"})
	if exitcode.GetExitCode(err) != exitcode.Rejected || !asExit(err, &se) || se.Code != exitcode.ErrMissingPK ||
		!strings.Contains(err.Error(), "prefix part") {
		t.Fatalf("primary key with prefix part must be rejected with missing_pk, got %v", err)
	}

	err = validate.CheckMySQLSource(ctx, db, []string{"app.does_not_exist"})
	if exitcode.GetExitCode(err) != exitcode.Rejected {
		t.Fatalf("missing table must be rejected, got %v", err)
	}
}

func asExit(err error, target **exitcode.Error) bool {
	e, ok := err.(*exitcode.Error)
	if ok {
		*target = e
	}
	return ok
}

// Session cũ còn ghi binlog STATEMENT/MINIMAL (SET GLOBAL không đổi kết nối đang mở) phải bị từ chối;
// user không đọc được performance_schema chỉ nhận cảnh báo.
func TestValidateRejectsOpenSessionWithOldBinlogSettings(t *testing.T) {
	ctx := context.Background()
	db := startMySQL(t)
	if w, err := validate.CheckMySQLSessionBinlog(ctx, db); err != nil {
		t.Fatalf("clean source rejected: %v (warnings %v)", err, w)
	}

	old, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.ExecContext(ctx, "SET SESSION binlog_format = 'STATEMENT'"); err != nil {
		t.Fatal(err)
	}
	_, err = validate.CheckMySQLSessionBinlog(ctx, db)
	if exitcode.GetExitCode(err) != exitcode.Rejected || !strings.Contains(err.Error(), "binlog_format=STATEMENT") {
		t.Fatalf("open STATEMENT session must be rejected with exit 3, got %v", err)
	}
	if _, err := old.ExecContext(ctx, "SET SESSION binlog_format = 'ROW'"); err != nil {
		t.Fatal(err)
	}
	if _, err := old.ExecContext(ctx, "SET SESSION binlog_row_image = 'MINIMAL'"); err != nil {
		t.Fatal(err)
	}
	if _, err = validate.CheckMySQLSessionBinlog(ctx, db); exitcode.GetExitCode(err) != exitcode.Rejected {
		t.Fatalf("open MINIMAL session must be rejected, got %v", err)
	}
	_ = old.Close()
	// Kết nối trả về pool vẫn giữ giá trị session; đóng hẳn mọi kết nối rảnh.
	db.SetMaxIdleConns(0)
	db.SetMaxIdleConns(2)
	if w, err := validate.CheckMySQLSessionBinlog(ctx, db); err != nil {
		t.Fatalf("after reconnect: %v (warnings %v)", err, w)
	}

	for _, q := range []string{
		"CREATE USER 'lim'@'%' IDENTIFIED BY 'lim'",
		"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'lim'@'%'",
		"GRANT SELECT ON app.* TO 'lim'@'%'",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	limDB, err := sql.Open("mysql", fmt.Sprintf("lim:lim@tcp(%s)/app", lastMySQLAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer limDB.Close()
	w, err := validate.CheckMySQLSessionBinlog(ctx, limDB)
	if err != nil {
		t.Fatalf("missing performance_schema privilege must only warn, got %v", err)
	}
	if len(w) == 0 || !strings.Contains(strings.Join(w, " "), "cannot verify per-session binlog settings") {
		t.Fatalf("expected a warning about performance_schema access, got %v", w)
	}
}

// tables.include dùng mẫu glob ("app.*") như mọi ví dụ cấu hình; validate phải mở rộng mẫu thành bảng thật
// thay vì từ chối, và báo lỗi khi một mẫu không khớp bảng nào.
func TestValidateExpandsTablePatterns(t *testing.T) {
	ctx := context.Background()
	db := startMySQL(t)
	for _, q := range []string{
		"CREATE TABLE app.a (id INT PRIMARY KEY)",
		"CREATE TABLE app.b (id INT PRIMARY KEY)",
		"CREATE TABLE app.audit (id INT PRIMARY KEY)",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	got, err := validate.ExpandTables(ctx, db, []string{"app.*"}, []string{"app.audit"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "app.a,app.b" {
		t.Fatalf("expanded = %v, want [app.a app.b]", got)
	}
	if err := validate.CheckMySQLSource(ctx, db, got); err != nil {
		t.Fatalf("expanded tables rejected: %v", err)
	}
	if _, err := validate.ExpandTables(ctx, db, []string{"app.*", "shop.*"}, nil); exitcode.GetExitCode(err) != exitcode.Rejected ||
		!strings.Contains(err.Error(), "shop.*") {
		t.Fatalf("pattern matching nothing must be rejected naming it, got %v", err)
	}
}

// review R26 (cần container): target MySQL có bảng đích MyISAM hoặc bảng _strand_* MEMORY bị từ chối
// target_not_transactional (exit 3); toàn InnoDB thì qua; bảng không đồng bộ không ảnh hưởng.
func TestR26_TargetEngineLiveMySQL(t *testing.T) {
	db := startMySQL(t)
	defer db.Close()
	ctx := context.Background()
	for _, q := range []string{
		"CREATE TABLE app.orders_v2 (id INT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE app._strand_checkpoint (pipeline_id VARCHAR(64) PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE app.unrelated (id INT PRIMARY KEY) ENGINE=MyISAM",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	targets := []model.TableID{{Schema: "app", Table: "orders_v2"}}
	if err := validate.CheckMySQLTargetEngines(ctx, db, targets); err != nil {
		t.Fatalf("all-InnoDB target must pass: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE app.orders_v2 ENGINE=MyISAM"); err != nil {
		t.Fatal(err)
	}
	err := validate.CheckMySQLTargetEngines(ctx, db, targets)
	if exitcode.GetExitCode(err) != exitcode.Rejected || !strings.Contains(err.Error(), "app.orders_v2 (ENGINE=MyISAM)") {
		t.Fatalf("MyISAM target table must be rejected, got %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE app.orders_v2 ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE app._strand_checkpoint ENGINE=MEMORY"); err != nil {
		t.Fatal(err)
	}
	err = validate.CheckMySQLTargetEngines(ctx, db, targets)
	if exitcode.GetExitCode(err) != exitcode.Rejected || !strings.Contains(err.Error(), "app._strand_checkpoint (ENGINE=MEMORY)") {
		t.Fatalf("MEMORY checkpoint table must be rejected, got %v", err)
	}
}
