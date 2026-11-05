package main

// `strand verify --mode full` chạy độc lập (không có pipeline): exit 0 khi khớp, exit 5
// (verify_mismatch) khi lệch; MySQL → MySQL và MySQL → PG (kind: postgres).

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
)

// startContainer chạy container (dừng khi test xong) và trả host:port của cổng đầu tiên trong ExposedPorts.
func startContainer(t *testing.T, req testcontainers.ContainerRequest) string {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start %s: %v", req.Image, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, _ := c.Host(ctx)
	ports, err := c.Ports(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for p, bindings := range ports {
		if string(p) == req.ExposedPorts[0] && len(bindings) > 0 {
			return net.JoinHostPort(host, bindings[0].HostPort)
		}
	}
	t.Fatalf("no mapped port for %s", req.ExposedPorts[0])
	return ""
}

func openWait(t *testing.T, driver, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for i := 0; i < 60; i++ {
		if err = db.Ping(); err == nil {
			return db
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("ping %s: %v", driver, err)
	return nil
}

func mustExecSQL(t *testing.T, db *sql.DB, qs ...string) {
	t.Helper()
	for _, q := range qs {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

func runVerifyFull(t *testing.T, yaml string, args ...string) int {
	t.Helper()
	path := filepath.Join(t.TempDir(), "strand.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	// rootCmd là biến toàn cục: đặt lại mọi cờ dùng ở đây cho mỗi lần chạy.
	rootCmd.SetArgs(append([]string{"verify", "-c", path, "--mode", "full", "--rate", "0", "--resume=false", "--repair", "report"}, args...))
	err := rootCmd.ExecuteContext(context.Background())
	if err != nil {
		t.Logf("verify: %v", err)
	}
	return exitcode.GetExitCode(err)
}

func TestVerifyFullCLIExitCodes(t *testing.T) {
	myAddr := startContainer(t, testcontainers.ContainerRequest{
		Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd:        []string{"--innodb-use-native-aio=OFF"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
	})
	pgAddr := startContainer(t, testcontainers.ContainerRequest{
		Image: "postgres:18", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "strand"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(120 * time.Second),
	})

	root := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/", myAddr))
	mustExecSQL(t, root, "CREATE DATABASE vs", "CREATE DATABASE vd",
		"CREATE TABLE vs.t (id INT PRIMARY KEY, v VARCHAR(20), amount DECIMAL(8,2), ts TIMESTAMP(6) NULL)",
		"CREATE TABLE vd.t LIKE vs.t",
		"INSERT INTO vs.t VALUES (1,'a',1.50,'2026-11-03 10:00:00'),(2,NULL,0,NULL),(3,'c',-2.25,'2026-11-03 11:00:00.5')",
		"INSERT INTO vd.t SELECT * FROM vs.t")
	pg := openWait(t, "pgx", fmt.Sprintf("postgres://strand:dev@%s/strand?sslmode=disable", pgAddr))
	mustExecSQL(t, pg, "CREATE TABLE public.t (id int PRIMARY KEY, v varchar(20), amount numeric(8,2), ts timestamptz)",
		"INSERT INTO public.t VALUES (1,'a',1.50,'2026-11-03 10:00:00Z'),(2,NULL,0,NULL),(3,'c',-2.25,'2026-11-03 11:00:00.5Z')")

	mysqlYAML := fmt.Sprintf(`name: cli-full
source: { kind: mysql, dsn: "root:dev@tcp(%[1]s)/vs" }
target: { kind: mysql, dsn: "root:dev@tcp(%[1]s)/vd" }
tables:
  include: ["vs.t"]
  rename: { "vs.t": "vd.t" }
`, myAddr)
	pgYAML := fmt.Sprintf(`name: cli-full-pg
source: { kind: mysql, dsn: "root:dev@tcp(%s)/vs" }
target: { kind: postgres, dsn: "postgres://strand:dev@%s/strand?sslmode=disable" }
tables:
  include: ["vs.t"]
  rename: { "vs.t": "public.t" }
`, myAddr, pgAddr)

	if code := runVerifyFull(t, mysqlYAML, "--table", "vs.t"); code != exitcode.Success {
		t.Fatalf("mysql->mysql equal: exit %d, want 0", code)
	}
	if code := runVerifyFull(t, pgYAML, "--table", "vs.t"); code != exitcode.Success {
		t.Fatalf("mysql->pg equal: exit %d, want 0", code)
	}

	mustExecSQL(t, root, "UPDATE vd.t SET v = 'x' WHERE id = 3")
	mustExecSQL(t, pg, "UPDATE public.t SET ts = ts + interval '1 second' WHERE id = 1")
	// Không --table: lấy tables.include.
	if code := runVerifyFull(t, mysqlYAML, "--table", ""); code != exitcode.DataError {
		t.Fatalf("mysql->mysql mismatch: exit %d, want %d", code, exitcode.DataError)
	}
	if code := runVerifyFull(t, pgYAML, "--table", "vs.t"); code != exitcode.DataError {
		t.Fatalf("mysql->pg mismatch: exit %d, want %d", code, exitcode.DataError)
	}

	// --repair fix (cùng engine) sửa target rồi lượt sau khớp.
	if code := runVerifyFull(t, mysqlYAML, "--table", "vs.t", "--repair", "fix"); code != exitcode.Success {
		t.Fatalf("mysql->mysql repair fix: exit %d, want 0", code)
	}
	if code := runVerifyFull(t, mysqlYAML, "--table", "vs.t"); code != exitcode.Success {
		t.Fatalf("mysql->mysql after repair: exit %d, want 0", code)
	}
	var n int
	// Lượt đã chạy hết thì tiến độ bị xóa (--resume chỉ chạy tiếp lượt bị ngắt, review R12).
	if err := root.QueryRow("SELECT COUNT(*) FROM vd._strand_verify WHERE pipeline_id = 'cli-full' AND table_name = 'vs.t'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("progress rows in vd._strand_verify after a completed run = %d (%v), want 0", n, err)
	}
	if code := runVerifyFull(t, pgYAML, "--table", "vs.t", "--repair", "fix"); code != exitcode.InvalidSyntax {
		t.Fatalf("cross-engine repair fix: exit %d, want %d", code, exitcode.InvalidSyntax)
	}
}
