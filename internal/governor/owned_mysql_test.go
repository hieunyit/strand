package governor_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Test chỉ kết nối container vừa tạo; cleanup không đụng compose/dev hoặc container của người khác.
func ownSignalMySQL(t *testing.T) *sql.DB {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "test-only", "MYSQL_DATABASE": "app"},
		Cmd: []string{"--innodb-use-native-aio=OFF"}, WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(90 * time.Second),
	}, Started: true})
	if c != nil {
		t.Cleanup(func() {
			if err := c.Terminate(context.Background()); err != nil {
				t.Errorf("terminate own MySQL: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "3306/tcp")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("mysql", fmt.Sprintf("root:test-only@tcp(%s)/app?timeout=5s&readTimeout=5s&writeTimeout=5s", net.JoinHostPort(host, port.Port())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}
