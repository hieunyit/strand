package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

// review R04 (invariant: RS-1, CP-2) và các lỗi cùng đường repair:
//   - --repair fix từ chối khi pipeline đang chạy (giữ khóa) và không đụng target;
//   - khi pipeline dừng: sửa row cha mà không xóa row con qua FK ON DELETE CASCADE, không ghi cột
//     generated, xóa row thừa, và không ghi đè row khác khi trùng UNIQUE phụ.
func TestR04_RepairNeedsStoppedPipelineAndRewritesRowsInPlace(t *testing.T) {
	myAddr := startContainer(t, testcontainers.ContainerRequest{
		Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd:        []string{"--innodb-use-native-aio=OFF"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
	})
	root := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/", myAddr))
	for _, db := range []string{"rs", "rd"} {
		mustExecSQL(t, root, "CREATE DATABASE "+db,
			"CREATE TABLE "+db+".p (id INT PRIMARY KEY, v INT, code VARCHAR(10), g INT AS (v * 2) STORED, UNIQUE KEY uk_code (code))",
			"CREATE TABLE "+db+".c (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES "+db+".p(id) ON DELETE CASCADE)")
	}
	mustExecSQL(t, root,
		"INSERT INTO rs.p (id, v, code) VALUES (1, 10, 'a'), (2, 20, 'b')", "INSERT INTO rs.c VALUES (100, 1)",
		"INSERT INTO rd.p (id, v, code) VALUES (1, 99, 'a'), (2, 20, 'b'), (3, 30, 'z')", "INSERT INTO rd.c VALUES (100, 1)")
	yaml := fmt.Sprintf(`name: repair-r04
source: { kind: mysql, dsn: "root:dev@tcp(%[1]s)/rs" }
target: { kind: mysql, dsn: "root:dev@tcp(%[1]s)/rd" }
tables:
  include: ["rs.p"]
  rename: { "rs.p": "rd.p" }
`, myAddr)

	// Pipeline đang chạy (giữ khóa): từ chối, target không đổi.
	ctx := context.Background()
	release, err := checkpoint.NewMySQLStore(root, "repair-r04", model.EngineMySQL).Lock(ctx, "repair-r04")
	if err != nil {
		t.Fatal(err)
	}
	if code := runVerifyFull(t, yaml, "--table", "rs.p", "--repair", "fix"); code != exitcode.Fenced {
		t.Fatalf("repair while pipeline runs: exit %d, want %d (pipeline_locked)", code, exitcode.Fenced)
	}
	var v int
	if err := root.QueryRow("SELECT v FROM rd.p WHERE id = 1").Scan(&v); err != nil || v != 99 {
		t.Fatalf("target changed while pipeline held the lock: v=%d err=%v", v, err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}

	if code := runVerifyFull(t, yaml, "--table", "rs.p", "--repair", "fix"); code != exitcode.Success {
		t.Fatalf("repair with stopped pipeline: exit %d", code)
	}
	if code := runVerifyFull(t, yaml, "--table", "rs.p"); code != exitcode.Success {
		t.Fatalf("verify after repair: exit %d", code)
	}
	var children, extra, g int
	_ = root.QueryRow("SELECT COUNT(*) FROM rd.c WHERE pid = 1").Scan(&children)
	_ = root.QueryRow("SELECT COUNT(*) FROM rd.p WHERE id = 3").Scan(&extra)
	_ = root.QueryRow("SELECT g FROM rd.p WHERE id = 1").Scan(&g)
	if children != 1 || extra != 0 || g != 20 {
		t.Fatalf("after repair: children of p1=%d (want 1, not cascaded), extra row=%d (want 0), g=%d (want 20)", children, extra, g)
	}

	// Các row lệch đổi chéo giá trị UNIQUE (target tụt sau source, như khi pipeline dừng giữa tải): sửa
	// từng row thì row nào trước cũng va uk_code; sửa cả lô trong một transaction thì được, và không
	// cascade xóa row con của chúng.
	mustExecSQL(t, root,
		"INSERT INTO rs.p (id, v, code) VALUES (10, 1, 'x'), (11, 2, 'y')", "INSERT INTO rs.c VALUES (110, 10), (111, 11)",
		"INSERT INTO rd.p (id, v, code) VALUES (10, 1, 'y'), (11, 2, 'x')", "INSERT INTO rd.c VALUES (110, 10), (111, 11)")
	if code := runVerifyFull(t, yaml, "--table", "rs.p", "--repair", "fix"); code != exitcode.Success {
		t.Fatalf("repair of rows that swap a UNIQUE value: exit %d", code)
	}
	if code := runVerifyFull(t, yaml, "--table", "rs.p"); code != exitcode.Success {
		t.Fatalf("verify after swap repair: exit %d", code)
	}
	_ = root.QueryRow("SELECT COUNT(*) FROM rd.c WHERE pid IN (10, 11)").Scan(&children)
	if children != 2 {
		t.Fatalf("children of swapped parents after repair = %d, want 2 (not cascaded)", children)
	}
}
