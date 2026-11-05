package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/exitcode"
)

// Review F2: repair chép row dạng text; TIMESTAMP dạng text phụ thuộc time_zone của phiên. Source +07:00,
// target UTC: row sửa không được lệch 7 giờ (verify sau repair phải sạch, thời điểm tuyệt đối giữ nguyên).
// Cả PK số nguyên (repairRows) và PK tổng quát (repairKeysMySQL).
func TestRepairKeepsTimestampAcrossServerTimeZones(t *testing.T) {
	mysqlReq := func(tz string) testcontainers.ContainerRequest {
		return testcontainers.ContainerRequest{
			Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
			Cmd:        []string{"--innodb-use-native-aio=OFF", "--default-time-zone=" + tz},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
		}
	}
	srcAddr := startContainer(t, mysqlReq("+07:00"))
	dstAddr := startContainer(t, mysqlReq("+00:00"))
	src := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/", srcAddr))
	dst := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/", dstAddr))
	ddl := []string{
		"CREATE DATABASE app", "CREATE DATABASE strand",
		"CREATE TABLE app.i (id INT PRIMARY KEY, ts TIMESTAMP(6) NOT NULL, v INT)",
		"CREATE TABLE app.g (k VARCHAR(8) COLLATE utf8mb4_bin NOT NULL, n INT NOT NULL, ts TIMESTAMP(6) NOT NULL, v INT, PRIMARY KEY (k, n))",
	}
	mustExecSQL(t, src, ddl...)
	mustExecSQL(t, dst, ddl...)
	// Cùng thời điểm tuyệt đối ở hai phía, ghi bằng phiên UTC (DSN riêng: SET time_zone trên pool không
	// chắc cùng kết nối với INSERT).
	utc := "?time_zone=%27%2B00%3A00%27"
	for _, addr := range []string{srcAddr, dstAddr} {
		w := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/%s", addr, utc))
		mustExecSQL(t, w,
			"INSERT INTO app.i VALUES (1, '2026-03-01 10:00:00.123456', 1), (2, '2026-03-01 11:00:00', 2)",
			"INSERT INTO app.g VALUES ('a', 1, '2026-03-01 10:00:00.5', 1), ('b', 2, '2026-03-01 12:00:00', 2)")
	}
	mustExecSQL(t, dst, "UPDATE app.i SET v = -1 WHERE id = 1", "UPDATE app.g SET v = -1 WHERE k = 'a'")

	yaml := fmt.Sprintf("name: tz\nsource: { kind: mysql, dsn: \"root:dev@tcp(%s)/app\" }\ntarget: { kind: mysql, dsn: \"root:dev@tcp(%s)/strand\" }\ntables:\n  include: [\"app.i\", \"app.g\"]\n", srcAddr, dstAddr)
	for _, tbl := range []string{"app.i", "app.g"} {
		if code := runVerifyFull(t, yaml, "--table", tbl, "--repair", "fix"); code != exitcode.Success {
			t.Fatalf("repair %s: exit %d", tbl, code)
		}
		if code := runVerifyFull(t, yaml, "--table", tbl); code != exitcode.Success {
			t.Fatalf("verify %s after repair: exit %d (repaired TIMESTAMP shifted?)", tbl, code)
		}
	}
	for q, want := range map[string]string{
		"SELECT CAST(UNIX_TIMESTAMP(ts) AS CHAR) FROM app.i WHERE id = 1":  "1772359200.123456",
		"SELECT CAST(UNIX_TIMESTAMP(ts) AS CHAR) FROM app.g WHERE k = 'a'": "1772359200.500000",
	} {
		var got string
		if err := dst.QueryRow(q).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s: %s, want %s", q, got, want)
		}
	}
}
