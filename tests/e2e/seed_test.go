package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/network"
)

func sourceTableReads(t *testing.T, n *mysqlNode) int64 {
	t.Helper()
	var v int64
	if err := n.DB.QueryRow(`SELECT COALESCE(SUM(COUNT_READ), 0) FROM performance_schema.table_io_waits_summary_by_table
		WHERE OBJECT_SCHEMA = 'app'`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// Gate 1: seed từ backup của một replica (target mang cấu hình replication và tự chạy
// replication như khi restore backup của replica): `seed apply` phải dừng và xóa replication,
// từ chối nếu target đã trôi khỏi P; seed + catch-up không đọc dữ liệu bảng trên source
// (0 lượt đọc theo performance_schema), rồi stream 0 lệch.
func TestG1_E2E_SeedFromReplicaBackupNoAutoReplicateZeroTableReads(t *testing.T) {
	seed := seedFromEnv(t)
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	src := startMySQL(t, mysqlImage("source"), "src", 1, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nw)
	makeReplica(t, dst, "src")
	createSchema(t, src)

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 5000)
	wl.Start(4, 5000)
	time.Sleep(5 * time.Second)
	wl.Stop()
	waitReplicaCaughtUp(t, src, dst)

	dir := testDir(t)
	backup := filepath.Join(dir, "backup")
	_ = os.MkdirAll(backup, 0o750)
	p := strings.ReplaceAll(dst.gtidExecuted(t), "\n", "")
	if err := os.WriteFile(filepath.Join(backup, "clone_status.txt"), []byte(p), 0o640); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(dir, "seed.json")

	readsBefore := sourceTableReads(t, src)

	out, code := runStrandCLI(t, time.Minute, "seed", "plan", "--type", "clone", "--backup", backup,
		"--source", src.RootDSN, "--target", dst.RootDSN, "--out", manifest)
	if code != 0 {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}
	out, code = runStrandCLI(t, time.Minute, "seed", "apply", "--manifest", manifest, "--target", dst.RootDSN)
	if code != 0 || !strings.Contains(out, "stopped and removed") {
		t.Fatalf("seed apply: exit %d\n%s", code, out)
	}
	var channels int
	_ = dst.DB.QueryRow("SELECT COUNT(*) FROM performance_schema.replication_connection_configuration").Scan(&channels)
	if channels != 0 {
		t.Fatalf("target still has %d replication channel(s) after seed apply", channels)
	}

	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	cfg := strandCfg{Name: "e2e-seed", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: dir, Bootstrap: "seed"}
	sp := startStrand(t, cfg.write(t), "--start-at", "seed", "--seed-manifest", manifest)
	// Đo tới ngay trước lúc verify sau seed bắt đầu (verify đọc ~1% mẫu theo 02 §9, không
	// thuộc phần seed). Pipeline chỉ bắt đầu verify sau khi catch-up xong.
	var readsSeed int64 = -1
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		r := sourceTableReads(t, src)
		if strings.Contains(sp.output.String(), "post-seed verify started") {
			break
		}
		readsSeed = r
		time.Sleep(100 * time.Millisecond)
	}
	if readsSeed < 0 {
		t.Fatalf("post-seed verify never started\n%s", sp.tail())
	}
	if d := readsSeed - readsBefore; d != 0 {
		t.Fatalf("seed + catch-up read %d rows of table data on source; want 0", d)
	}
	t.Logf("seed plan + apply + catch-up: 0 table-data reads on source (before post-seed verify)")
	waitSynced(t, &cfg, sp, 3*time.Minute)

	// Ghi tiếp trên source: target chỉ nhận qua Strand (không còn replication riêng).
	wl2 := newWorkload(src.DB, seed+1)
	wl2.nextID = 1_000_000
	wl2.Start(4, 5000)
	time.Sleep(10 * time.Second)
	wl2.Stop()
	waitCaughtUp(t, src, dst, &cfg, sp, 3*time.Minute)
	if code := sp.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, sp.tail())
	}
	compareFinal(t, src, dst, appTables...)
}

// Nếu target đã tự replicate vượt qua P trước khi apply, seed bị từ chối (exit 3).
func TestG1_E2E_SeedApplyRejectsTargetThatReplicatedPastP(t *testing.T) {
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	src := startMySQL(t, mysqlImage("source"), "src", 1, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nw)
	makeReplica(t, dst, "src")
	createSchema(t, src)
	waitReplicaCaughtUp(t, src, dst)

	dir := testDir(t)
	backup := filepath.Join(dir, "backup")
	_ = os.MkdirAll(backup, 0o750)
	p := strings.ReplaceAll(dst.gtidExecuted(t), "\n", "")
	_ = os.WriteFile(filepath.Join(backup, "clone_status.txt"), []byte(p), 0o640)
	manifest := filepath.Join(dir, "seed.json")
	if out, code := runStrandCLI(t, time.Minute, "seed", "plan", "--type", "clone", "--backup", backup,
		"--source", src.RootDSN, "--out", manifest); code != 0 {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}
	src.exec(t, "INSERT INTO app.events VALUES (424242, 1, 'after P')")
	waitReplicaCaughtUp(t, src, dst)
	out, code := runStrandCLI(t, time.Minute, "seed", "apply", "--manifest", manifest, "--target", dst.RootDSN)
	if code != 3 || !strings.Contains(out, "seed_invariant_failed") {
		t.Fatalf("seed apply on a target past P: exit %d, want 3\n%s", code, out)
	}
}
