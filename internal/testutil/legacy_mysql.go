package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// LegacyMySQL chạy một container MySQL cũ (mysql:5.5 / mysql:5.6) với binlog ROW, trả kết nối root.
// Container bị dọn khi test kết thúc.
func LegacyMySQL(t testing.TB, image string, extra ...string) *sql.DB {
	t.Helper()
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()
	cmd := append([]string{"--server-id=1", "--log-bin=mysql-bin", "--binlog-format=ROW", "--innodb-use-native-aio=0"}, extra...)
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        image,
			ExposedPorts: []string{"3306/tcp"},
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
			Cmd:          cmd,
			WaitingFor:   wait.ForLog("ready for connections").WithOccurrence(2).WithStartupTimeout(180 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start %s: %v", image, err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	pn, _ := c.MappedPort(ctx, "3306")
	dsn := fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true", net.JoinHostPort(host, pn.Port()))
	var db *sql.DB
	for i := 0; i < 60; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil && db.PingContext(ctx) == nil {
			t.Cleanup(func() { db.Close() })
			legacyAddr.Store(db, net.JoinHostPort(host, pn.Port()))
			return db
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("connect %s: %v", image, err)
	return nil
}

var legacyAddr sync.Map

// LegacyMySQLAddr: host:port của container đã tạo bởi LegacyMySQL cho db này.
func LegacyMySQLAddr(db *sql.DB) string {
	v, _ := legacyAddr.Load(db)
	s, _ := v.(string)
	return s
}
