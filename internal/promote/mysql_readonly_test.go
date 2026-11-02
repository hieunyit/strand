package promote_test

// Regression test from the Gate 1 code review (decision P1-17, câu 1, hướng 1).

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

	"strand/internal/promote"
)

// invariant: PR-1
// Step 2 used SET GLOBAL super_read_only=ON, which also blocks Strand's own user
// (CONNECTION_ADMIN), so step 3 could never apply the remaining log. Step 2 must
// block the application user while Strand can still write; step 5 opens writes.
func TestPR1_HoldReadOnlyBlocksAppButNotStrand(t *testing.T) {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "mysql:8.4",
			ExposedPorts: []string{"3306/tcp"},
			Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev", "MYSQL_DATABASE": "app"},
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
	addr := net.JoinHostPort(host, port.Port())
	open := func(user, pass string) *sql.DB {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/app", user, pass, addr))
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 60 && db.Ping() != nil; i++ {
			time.Sleep(500 * time.Millisecond)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	admin := open("root", "dev")
	for _, q := range []string{
		"CREATE TABLE app.t (id INT PRIMARY KEY)",
		"CREATE USER 'strand'@'%' IDENTIFIED BY 's'",
		"GRANT INSERT, SELECT ON app.* TO 'strand'@'%'",
		"GRANT CONNECTION_ADMIN ON *.* TO 'strand'@'%'",
		"CREATE USER 'appuser'@'%' IDENTIFIED BY 'a'",
		"GRANT INSERT, SELECT ON app.* TO 'appuser'@'%'",
	} {
		if _, err := admin.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	strand, app := open("strand", "s"), open("appuser", "a")

	d := promote.NewMySQLTargetDriver(admin)
	if err := d.HoldReadOnly(ctx); err != nil {
		t.Fatalf("HoldReadOnly: %v", err)
	}
	if ro, err := d.IsReadOnly(ctx); err != nil || !ro {
		t.Fatalf("IsReadOnly after HoldReadOnly = %v, %v", ro, err)
	}
	if _, err := app.ExecContext(ctx, "INSERT INTO app.t VALUES (1)"); err == nil {
		t.Fatalf("application user could write after step 2")
	}
	if _, err := strand.ExecContext(ctx, "INSERT INTO app.t VALUES (2)"); err != nil {
		t.Fatalf("Strand user must still apply in step 3, got: %v", err)
	}

	if err := d.OpenWrites(ctx); err != nil {
		t.Fatalf("OpenWrites: %v", err)
	}
	if _, err := app.ExecContext(ctx, "INSERT INTO app.t VALUES (3)"); err != nil {
		t.Fatalf("application user cannot write after step 5: %v", err)
	}
}
