package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startTestDB khởi động một container DB riêng cho test (dọn ở t.Cleanup) và trả về DSN.
func startTestDB(t *testing.T, engine string) (*sql.DB, string) {
	t.Helper()
	ctx := context.Background()
	var req testcontainers.ContainerRequest
	var port, driver string
	switch engine {
	case "mysql":
		req = testcontainers.ContainerRequest{Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"},
			Env:        map[string]string{"MYSQL_ROOT_PASSWORD": "dev", "MYSQL_DATABASE": "app"},
			Cmd:        []string{"--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--innodb-use-native-aio=OFF"},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second)}
		port, driver = "3306", "mysql"
	default:
		req = testcontainers.ContainerRequest{Image: "postgres:18", ExposedPorts: []string{"5432/tcp"},
			Env:        map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "app"},
			Cmd:        []string{"-c", "wal_level=logical"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(180 * time.Second)}
		port, driver = "5432", "pgx"
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start %s: %v", engine, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mp, err := c.MappedPort(ctx, nat.Port(port+"/tcp"))
	if err != nil {
		t.Fatal(err)
	}
	var dsn string
	if engine == "mysql" {
		dsn = fmt.Sprintf("root:dev@tcp(%s:%s)/app?multiStatements=true", host, mp.Port())
	} else {
		dsn = fmt.Sprintf("postgres://strand:dev@%s:%s/app?sslmode=disable", host, mp.Port())
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for i := 0; i < 60; i++ {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	return db, dsn
}
