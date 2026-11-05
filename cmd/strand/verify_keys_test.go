package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
)

// Việc C: `verify --mode full` cho PK không phải một cột số nguyên — MySQL→MySQL (VARCHAR _bin, INT) và
// MySQL→PG (INT, DATETIME): khớp → 0; lệch giá trị, thiếu row, row thừa trước min / sau max của source →
// verify_mismatch (5); --repair fix bị từ chối rõ ràng.
func TestGPK_VerifyFullWithGenericKeys(t *testing.T) {
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
	pg := openWait(t, "pgx", fmt.Sprintf("postgres://strand:dev@%s/strand?sslmode=disable", pgAddr))
	mustExecSQL(t, root, "CREATE DATABASE ks", "CREATE DATABASE kd",
		"CREATE TABLE ks.s (k VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, n INT NOT NULL, v VARCHAR(10), PRIMARY KEY (k, n))",
		"CREATE TABLE kd.s LIKE ks.s",
		"CREATE TABLE ks.d (a INT NOT NULL, d DATETIME NOT NULL, v INT, PRIMARY KEY (a, d))")
	for i := 0; i < 2500; i++ {
		mustExecSQL(t, root, fmt.Sprintf("INSERT INTO ks.s VALUES ('b%03d', %d, 'v%d')", i/5, i%5, i),
			fmt.Sprintf("INSERT INTO ks.d VALUES (%d, '2026-01-%02d 10:00:00', %d)", i/25, 1+i%25, i))
	}
	mustExecSQL(t, root, "INSERT INTO kd.s SELECT * FROM ks.s")
	mustExecSQL(t, pg, "CREATE TABLE public.d (a int4 NOT NULL, d timestamp NOT NULL, v int4, PRIMARY KEY (a, d))")
	rows, err := root.Query("SELECT a, d, v FROM ks.d")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var a, v int
		var d string
		_ = rows.Scan(&a, &d, &v)
		if _, err := pg.Exec("INSERT INTO public.d VALUES ($1, $2, $3)", a, d, v); err != nil {
			t.Fatal(err)
		}
	}
	rows.Close()
	myYAML := fmt.Sprintf("name: vk\nsource: { kind: mysql, dsn: \"root:dev@tcp(%[1]s)/ks\" }\ntarget: { kind: mysql, dsn: \"root:dev@tcp(%[1]s)/kd\" }\ntables:\n  include: [\"ks.s\"]\n  rename: { \"ks.s\": \"kd.s\" }\n", myAddr)
	pgYAML := fmt.Sprintf("name: vkpg\nsource: { kind: mysql, dsn: \"root:dev@tcp(%s)/ks\" }\ntarget: { kind: postgres, dsn: \"postgres://strand:dev@%s/strand?sslmode=disable\" }\ntables:\n  include: [\"ks.d\"]\n  rename: { \"ks.d\": \"public.d\" }\n", myAddr, pgAddr)
	if code := runVerifyFull(t, myYAML, "--table", "ks.s"); code != exitcode.Success {
		t.Fatalf("mysql->mysql equal: exit %d", code)
	}
	if code := runVerifyFull(t, pgYAML, "--table", "ks.d"); code != exitcode.Success {
		t.Fatalf("mysql->pg equal: exit %d", code)
	}
	for _, q := range []string{
		"UPDATE kd.s SET v = 'X' WHERE k = 'b100' AND n = 2",
		"DELETE FROM kd.s WHERE k = 'b250' AND n = 0",
		"INSERT INTO kd.s VALUES ('a', 1, 'below-min'), ('zzz', 9, 'above-max')",
	} {
		mustExecSQL(t, root, q)
	}
	if code := runVerifyFull(t, myYAML, "--table", "ks.s"); code != exitcode.DataError {
		t.Fatalf("mysql->mysql with 4 differences: exit %d, want %d", code, exitcode.DataError)
	}
	mustExecSQL(t, pg, "UPDATE public.d SET v = -1 WHERE a = 40 AND d = '2026-01-05 10:00:00'")
	if code := runVerifyFull(t, pgYAML, "--table", "ks.d"); code != exitcode.DataError {
		t.Fatalf("mysql->pg with a difference: exit %d, want %d", code, exitcode.DataError)
	}
	// repair fix (MySQL→MySQL) sửa cả 4 khác biệt, kể cả row thừa trước min / sau max.
	if code := runVerifyFull(t, myYAML, "--table", "ks.s", "--repair", "fix"); code != exitcode.Success {
		t.Fatalf("repair fix on a composite key: exit %d", code)
	}
	if code := runVerifyFull(t, myYAML, "--table", "ks.s"); code != exitcode.Success {
		t.Fatalf("verify after repair: exit %d", code)
	}
	// sample theo tuple (100%) + repair: không chờ apply (pipeline dừng), sửa row vừa hỏng.
	mustExecSQL(t, root, "UPDATE kd.s SET v = 'Y' WHERE k = 'b333' AND n = 4")
	rootCmd.SetArgs([]string{"verify", "-c", writeYAML(t, myYAML), "--table", "ks.s", "--mode", "sample", "--rate", "1.0", "--repair", "fix"})
	if err := rootCmd.ExecuteContext(t.Context()); err != nil {
		t.Fatalf("sample repair on composite key: %v", err)
	}
	if code := runVerifyFull(t, myYAML, "--table", "ks.s"); code != exitcode.Success {
		t.Fatalf("verify after sample repair: exit %d", code)
	}
	// MySQL→PG: repair không hỗ trợ (khác engine) — từ chối rõ.
	if code := runVerifyFull(t, pgYAML, "--table", "ks.d", "--repair", "fix"); code != exitcode.InvalidSyntax {
		t.Fatalf("cross-engine repair: exit %d, want %d", code, exitcode.InvalidSyntax)
	}
}

func writeYAML(t *testing.T, y string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "strand.yaml")
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Review F6: full verify theo tuple không được trùng khóa giữa hai tuple chuỗi khác nhau — ('x','y,z') và
// ('x,y','z') nối bằng "," cùng là "x,y,z"; row bị đè không được so và lệch của nó bị bỏ sót.
func TestVerifyFullKeysDistinguishesAmbiguousTuples(t *testing.T) {
	addr := startContainer(t, testcontainers.ContainerRequest{
		Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd:        []string{"--innodb-use-native-aio=OFF"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
	})
	root := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/", addr))
	mustExecSQL(t, root, "CREATE DATABASE ss", "CREATE DATABASE dd",
		"CREATE TABLE ss.c (a VARCHAR(8) COLLATE utf8mb4_bin NOT NULL, b VARCHAR(8) COLLATE utf8mb4_bin NOT NULL, v INT, PRIMARY KEY (a, b))",
		"CREATE TABLE dd.c LIKE ss.c",
		"INSERT INTO ss.c VALUES ('x', 'y,z', 1), ('x,y', 'z', 2)",
		"INSERT INTO dd.c SELECT * FROM ss.c",
		"UPDATE dd.c SET v = 9 WHERE a = 'x' AND b = 'y,z'")
	y := fmt.Sprintf("name: amb\nsource: { kind: mysql, dsn: \"root:dev@tcp(%[1]s)/ss\" }\ntarget: { kind: mysql, dsn: \"root:dev@tcp(%[1]s)/dd\" }\ntables:\n  include: [\"ss.c\"]\n  rename: { \"ss.c\": \"dd.c\" }\n", addr)
	if code := runVerifyFull(t, y, "--table", "ss.c"); code != exitcode.DataError {
		t.Fatalf("one differing row hidden by an ambiguous key: exit %d, want %d", code, exitcode.DataError)
	}
	if code := runVerifyFull(t, y, "--table", "ss.c", "--repair", "fix"); code != exitcode.Success {
		t.Fatalf("repair: exit %d", code)
	}
	if code := runVerifyFull(t, y, "--table", "ss.c"); code != exitcode.Success {
		t.Fatalf("verify after repair: exit %d", code)
	}
}
