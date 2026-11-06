package main

// `strand verify --mode full` (MySQL→MySQL) khi pipeline đang chạy (khóa pipeline đang bị giữ) phải chờ
// target apply tới gtid_executed của source trước khi kết luận một row lệch: row chưa kịp apply (target
// trễ) không bị báo lệch giả. Trước đây full verify luôn bỏ bước chờ (SkipApplyWait: true). Khi không có
// pipeline chạy, vẫn bỏ chờ và báo lệch ngay.

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

func TestVerifyFullWaitsForApplyWhilePipelineRuns(t *testing.T) {
	myAddr := startContainer(t, testcontainers.ContainerRequest{
		Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd:        []string{"--innodb-use-native-aio=OFF", "--gtid-mode=ON", "--enforce-gtid-consistency=ON"},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
	})
	root := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/", myAddr))
	mustExecSQL(t, root, "CREATE DATABASE vs", "CREATE DATABASE vd",
		"CREATE TABLE vs.t (id INT PRIMARY KEY, v VARCHAR(20))", "CREATE TABLE vd.t LIKE vs.t",
		"INSERT INTO vs.t VALUES (1,'a'),(2,'b'),(3,'c')", "INSERT INTO vd.t SELECT * FROM vs.t")
	meta := openWait(t, "mysql", fmt.Sprintf("root:dev@tcp(%s)/vd", myAddr))
	ctx := context.Background()
	const id = "cli-wait"
	store := checkpoint.NewMySQLStore(meta, id, model.EngineMySQL)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	var uuid, before string
	if err := root.QueryRow("SELECT @@server_uuid, @@GLOBAL.gtid_executed").Scan(&uuid, &before); err != nil {
		t.Fatal(err)
	}
	mustExecSQL(t, meta, fmt.Sprintf(`INSERT INTO _strand_pipeline VALUES ('%s', 1, 't', 'h', 1, NOW(6))`, id),
		fmt.Sprintf(`INSERT INTO _strand_checkpoint VALUES ('%s', 1, '%s', NOW(6))`, id, before))
	// Thay đổi ở source mà "pipeline" chưa apply: target còn ảnh cũ, checkpoint chưa phủ GTID của nó.
	mustExecSQL(t, root, "UPDATE vs.t SET v = 'new' WHERE id = 2")

	yaml := fmt.Sprintf(`name: %s
source: { kind: mysql, dsn: "root:dev@tcp(%[2]s)/vs" }
target: { kind: mysql, dsn: "root:dev@tcp(%[2]s)/vd" }
tables:
  include: ["vs.t"]
  rename: { "vs.t": "vd.t" }
`, id, myAddr)

	// Không có pipeline chạy: không chờ, row 2 lệch ngay.
	if code := runVerifyFull(t, yaml, "--table", "vs.t"); code != exitcode.DataError {
		t.Fatalf("no pipeline running: exit %d, want %d (row 2 differs)", code, exitcode.DataError)
	}

	// "Pipeline" đang chạy: giữ khóa pipeline, apply row 2 và dời checkpoint sau 2 giây. Checkpoint phủ cả
	// GTID tương lai vì source và target cùng server (mọi lần ghi ở target cũng sinh GTID).
	release, err := store.Lock(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() {
		time.Sleep(2 * time.Second)
		if _, err := meta.Exec("UPDATE vd.t SET v = 'new' WHERE id = 2"); err != nil {
			done <- err
			return
		}
		_, err := meta.Exec("UPDATE _strand_checkpoint SET applied = ? WHERE pipeline_id = ?", uuid+":1-100000000", id)
		done <- err
	}()
	began := time.Now()
	if code := runVerifyFull(t, yaml, "--table", "vs.t"); code != exitcode.Success {
		t.Fatalf("pipeline running: exit %d, want 0 (row 2 was only lagging; verify must wait for apply)", code)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if el := time.Since(began); el < time.Second {
		t.Fatalf("verify returned after %s: it did not wait for the target to apply", el)
	}
}
