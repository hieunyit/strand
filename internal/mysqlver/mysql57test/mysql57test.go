// Package mysql57test khởi động container MySQL 5.7 cho các test DB của nguồn 5.7 (chỉ test import).
package mysql57test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// SourceArgs là cấu hình nguồn đúng của Strand trên 5.7: GTID ON, ROW, FULL, log-bin.
var SourceArgs = []string{
	"--server-id=1", "--log-bin=mysql-bin", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
	"--binlog-format=ROW", "--binlog-row-image=FULL", "--log-slave-updates=ON",
	"--innodb-use-native-aio=OFF", // không phụ thuộc giới hạn fs.aio-max-nr khi nhiều MySQL chạy cùng lúc
}

func image() string {
	if v := os.Getenv("STRAND_TEST_MYSQL57_IMAGE"); v != "" {
		return v
	}
	return "mysql:5.7"
}

// Start chạy một mysqld 5.7 với args và trả DSN root (không chọn database) + *sql.DB đã mở. Container bị
// hủy khi test kết thúc.
func Start(t *testing.T, args ...string) (string, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image(),
			ExposedPorts: []string{"3306/tcp"},
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
			Cmd:          args,
			WaitingFor:   wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(240 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start %s: %v", image(), err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatal(err)
	}
	dsn := fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true&parseTime=true", net.JoinHostPort(host, port.Port()))
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// Log "port: 3306" cũng xuất hiện ở lần khởi động tạm của entrypoint (socket-only): chờ TCP thật.
	deadline := time.Now().Add(120 * time.Second)
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mysql 5.7 not reachable: %v", err)
		}
		time.Sleep(time.Second)
	}
	return dsn, db
}
