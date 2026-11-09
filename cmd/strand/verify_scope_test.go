package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
)

// Full verify không được báo sạch khi bộ lọc chọn sai bảng, và --table phải vẫn kiểm được bảng ngoài pipeline.
func TestVerifyFullUsesPipelineTableScope(t *testing.T) {
	addr := startContainer(t, testcontainers.ContainerRequest{
		Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd:        []string{"--innodb-use-native-aio=OFF"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
	})
	db := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/", addr))
	mustExecSQL(t, db,
		"CREATE DATABASE vs", "CREATE DATABASE vd",
		"CREATE TABLE vs.keep (id BIGINT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE vs.omit (id BIGINT PRIMARY KEY, v INT NOT NULL)",
		"CREATE TABLE vd.keep LIKE vs.keep", "CREATE TABLE vd.omit LIKE vs.omit",
		"INSERT INTO vs.keep VALUES (1, 10)", "INSERT INTO vd.keep VALUES (1, 10)",
		"INSERT INTO vs.omit VALUES (1, 20)", "INSERT INTO vd.omit VALUES (1, 21)")
	config := func(include, exclude string) string {
		return fmt.Sprintf(`name: verify-scope
source: {kind: mysql, dsn: "root:dev@tcp(%s)/vs"}
target: {kind: mysql, dsn: "root:dev@tcp(%s)/vd"}
tables:
  include: %s
  exclude: %s
  rename: {"vs.keep": "vd.keep", "vs.omit": "vd.omit"}
`, addr, addr, include, exclude)
	}
	glob := config(`["vs.*", "vs.keep"]`, `["vs.omit"]`)
	if code := runVerifyFull(t, glob, "--table", ""); code != exitcode.Success {
		t.Fatalf("glob, dedup, exclude, rename: exit %d, want 0", code)
	}
	listed := config(`["vs.keep", "vs.omit"]`, `["vs.omit"]`)
	if code := runVerifyFull(t, listed, "--table", ""); code != exitcode.Success {
		t.Fatalf("explicit include with exclude: exit %d, want 0", code)
	}
	if code := runVerifyFull(t, glob, "--table", "vs.omit"); code != exitcode.DataError {
		t.Fatalf("--table override excluded mismatch: exit %d, want %d", code, exitcode.DataError)
	}
	if code := runVerifyFull(t, config(`["vs.omit"]`, `["vs.omit"]`), "--table", ""); code != exitcode.InvalidSyntax {
		t.Fatalf("empty selected set: exit %d, want %d", code, exitcode.InvalidSyntax)
	}
}

func TestVerifyFullUsesPipelineTableScopePG(t *testing.T) {
	addr := startContainer(t, testcontainers.ContainerRequest{
		Image: "postgres:18", ExposedPorts: []string{"5432/tcp"},
		Env:        map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "strand"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(120 * time.Second),
	})
	dsn := fmt.Sprintf("postgres://strand:dev@%s/strand?sslmode=disable", addr)
	db := openWait(t, "pgx", dsn)
	mustExecSQL(t, db,
		"CREATE SCHEMA vs", "CREATE SCHEMA vd",
		"CREATE TABLE vs.keep (id bigint PRIMARY KEY, v integer NOT NULL)",
		"CREATE TABLE vs.omit (id bigint PRIMARY KEY, v integer NOT NULL)",
		"CREATE TABLE vd.keep (LIKE vs.keep INCLUDING ALL)",
		"CREATE TABLE vd.omit (LIKE vs.omit INCLUDING ALL)",
		"INSERT INTO vs.keep VALUES (1, 10)", "INSERT INTO vd.keep VALUES (1, 10)",
		"INSERT INTO vs.omit VALUES (1, 20)", "INSERT INTO vd.omit VALUES (1, 21)")
	yaml := fmt.Sprintf(`name: verify-scope-pg
source: {kind: postgres, dsn: "%s"}
target: {kind: postgres, dsn: "%s"}
tables:
  include: ["vs.*", "vs.keep"]
  exclude: ["vs.omit"]
  rename: {"vs.keep": "vd.keep", "vs.omit": "vd.omit"}
`, dsn, dsn)
	if code := runVerifyFull(t, yaml, "--table", ""); code != exitcode.Success {
		t.Fatalf("PG glob/exclude/rename: exit %d, want 0", code)
	}
	if code := runVerifyFull(t, yaml, "--table", "vs.omit"); code != exitcode.DataError {
		t.Fatalf("PG --table override excluded mismatch: exit %d, want %d", code, exitcode.DataError)
	}
}
