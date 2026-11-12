package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startMySQLWithDatadir khởi chạy MySQL container giống startMySQL nhưng gắn bind mount
// hostDatadir:/var/lib/mysql để dùng với công cụ backup vật lý XtraBackup.
func startMySQLWithDatadir(t *testing.T, image, alias string, serverID int, nw *testcontainers.DockerNetwork, hostDatadir string) *mysqlNode {
	t.Helper()
	ctx := context.Background()
	args := mysqlServerFlags(image, serverID)
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"3306/tcp"},
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd:          args,
		WaitingFor:   wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(240 * time.Second),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Binds = append(hc.Binds, hostDatadir+":/var/lib/mysql")
		},
	}
	gcr := testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true}
	if nw != nil {
		if err := network.WithNetwork([]string{alias}, nw)(&gcr); err != nil {
			t.Fatal(err)
		}
	}
	c, err := testcontainers.GenericContainer(ctx, gcr)
	if err != nil {
		t.Fatalf("start %s: %v", image, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "3306")
	n := &mysqlNode{c: c, Alias: alias, Addr: net.JoinHostPort(host, port.Port())}
	n.RootDSN = fmt.Sprintf("root:dev@tcp(%s)/", n.Addr)
	n.DB, err = sql.Open("mysql", n.RootDSN+"?parseTime=false&multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	n.DB.SetMaxOpenConns(64)
	for i := 0; i < 120; i++ {
		if err = n.DB.Ping(); err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("ping %s: %v", alias, err)
	}
	t.Cleanup(func() { _ = n.DB.Close() })
	return n
}

// Gate 1 (01 §6): Seed bảng lớn bằng Percona XtraBackup thật, 0 lượt đọc dữ liệu bảng
// trên source trong pha seed + catch-up của Strand.
func TestG1_E2E_SeedFromXtraBackupZeroTableReads(t *testing.T) {
	seed := seedFromEnv(t)
	ctx := context.Background()

	// 1–2. Chuẩn bị thư mục dữ liệu trên host và dọn bằng container khi kết thúc.
	dir := testDir(t)
	srcdata := filepath.Join(dir, "srcdata")
	backup := filepath.Join(dir, "backup")
	dstdata := filepath.Join(dir, "dstdata")
	for _, d := range []string{srcdata, backup, dstdata} {
		if err := os.MkdirAll(d, 0o777); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(d, 0o777); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, d := range []string{srcdata, backup, dstdata} {
			_ = exec.Command("docker", "run", "--rm", "--entrypoint", "sh",
				"-v", d+":/d", "mysql:8.4", "-c", "rm -rf /d/* /d/.[!.]*").Run()
		}
	})

	// 3. Khởi tạo network và source MySQL với datadir gắn từ host.
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	src := startMySQLWithDatadir(t, mysqlImage("source"), "src", 1, nw, srcdata)
	createSchema(t, src)

	// 4. Tạo bảng lớn app.big và nạp dữ liệu không ghi binlog trên một kết nối riêng.
	src.exec(t, `CREATE TABLE app.big (
		id BIGINT PRIMARY KEY,
		ver BIGINT UNSIGNED NOT NULL,
		pad VARCHAR(1000) NOT NULL
	)`)
	gb := envInt("STRAND_E2E_XB_GB", 20)
	totalRows := int64(gb) * 1_000_000
	loadStart := time.Now()
	conn, err := src.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_log_bin = 0"); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE app.nums (n INT PRIMARY KEY)"); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	for base := 0; base < 10000; base += 1000 {
		var sb strings.Builder
		sb.WriteString("INSERT INTO app.nums VALUES ")
		for i := 0; i < 1000; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "(%d)", base+i)
		}
		if _, err := conn.ExecContext(ctx, sb.String()); err != nil {
			_ = conn.Close()
			t.Fatal(err)
		}
	}
	// Mỗi giá trị a.n kết hợp với 10.000 giá trị b.n tạo 10.000 row (~10 MB);
	// mỗi lô 10 giá trị a.n tạo 100.000 row (~100 MB); 100 giá trị a.n = 1.000.000 row (~1 GB).
	totalA := gb * 100
	for lo := 0; lo < totalA; lo += 10 {
		hi := lo + 10
		if _, err := conn.ExecContext(ctx,
			"INSERT INTO app.big SELECT a.n*10000+b.n, UUID_SHORT(), REPEAT('x', 900) FROM app.nums a JOIN app.nums b WHERE a.n >= ? AND a.n < ?",
			lo, hi); err != nil {
			_ = conn.Close()
			t.Fatalf("load app.big [%d..%d): %v", lo, hi, err)
		}
		if hi%100 == 0 {
			t.Logf("loaded %d/%d GB into app.big (%s)", hi/100, gb, time.Since(loadStart).Round(time.Second))
		}
	}
	// Kết nối trả về pool phải bật lại binlog: nếu không, workload dùng chung pool sẽ ghi dữ liệu
	// KHÔNG vào binlog và source tự lệch khỏi binlog của chính nó (không P nào nhất quán được).
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_log_bin = 1"); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	_ = conn.Close()

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 5000)

	// 5. Chạy XtraBackup thật (--backup rồi --prepare) trong khi source đang chịu tải (tải chạy tới hết restore).
	wl.Start(4, 5000)
	defer wl.Stop()

	backupStart := time.Now()
	xbArgs := append([]string{"run", "--rm", "--user", "root",
		"--network", nw.Name,
		"-v", srcdata + ":/var/lib/mysql",
		"-v", backup + ":/backup",
		"percona/percona-xtrabackup:8.4",
		"xtrabackup", "--backup",
		"--host=src", "--port=3306", "--user=root", "--password=dev",
		"--datadir=/var/lib/mysql", "--target-dir=/backup"}, strings.Fields(os.Getenv("STRAND_E2E_XB_EXTRA"))...)
	if out, err := exec.Command("docker", xbArgs...).CombinedOutput(); err != nil {
		t.Fatalf("xtrabackup --backup: %v\n%s", err, out)
	}
	if out, err := exec.Command("docker", "run", "--rm", "--user", "root",
		"-v", backup+":/backup",
		"percona/percona-xtrabackup:8.4",
		"xtrabackup", "--prepare", "--target-dir=/backup",
	).CombinedOutput(); err != nil {
		t.Fatalf("xtrabackup --prepare: %v\n%s", err, out)
	}
	backupDur := time.Since(backupStart)

	// 6. Copy-back sang datadir của target, xóa auto.cnf để có server_uuid riêng, rồi khởi chạy target.
	restoreStart := time.Now()
	if out, err := exec.Command("docker", "run", "--rm", "--user", "root",
		"-v", backup+":/backup",
		"-v", dstdata+":/var/lib/mysql",
		"percona/percona-xtrabackup:8.4",
		"sh", "-c",
		"xtrabackup --copy-back --target-dir=/backup --datadir=/var/lib/mysql && rm -f /var/lib/mysql/auto.cnf && chown -R 999:999 /var/lib/mysql && chmod -R a+rX /backup",
	).CombinedOutput(); err != nil {
		t.Fatalf("xtrabackup --copy-back: %v\n%s", err, out)
	}
	if bi, err := os.ReadFile(filepath.Join(backup, "xtrabackup_binlog_info")); err == nil {
		t.Logf("xtrabackup_binlog_info: %s", strings.TrimSpace(string(bi)))
	} else {
		t.Logf("read xtrabackup_binlog_info: %v", err)
	}
	dst := startMySQLWithDatadir(t, mysqlImage("target"), "dst", 2, nil, dstdata)
	restoreDur := time.Since(restoreStart)

	// 10 (sắp trước bước 8): Tạo schema metadata trên target trước seed plan/apply.
	// Tắt sql_log_bin trong phiên tạo DB để không sinh thêm GTID cục bộ trên target sau P
	// (nếu gtid_executed của target lệch P, cả seed plan lẫn seed apply đều từ chối theo SD-1).
	dst.exec(t, "SET SESSION sql_log_bin = 0; CREATE DATABASE IF NOT EXISTS strand; SET SESSION sql_log_bin = 1")

	// 7. Dừng tải (các ghi trong lúc backup + restore là phần catch-up phải apply), rồi đo lượt đọc bảng
	// trên source. Tải phải dừng trước cửa sổ đo vì UPDATE/DELETE của workload cũng tăng COUNT_READ.
	wl.Stop()
	readsBefore := sourceTableReads(t, src)

	// 8. Chạy strand seed plan, kiểm tra manifest có prepare_ok: true.
	manifest := filepath.Join(dir, "seed.json")
	out, code := runStrandCLI(t, 2*time.Minute, "seed", "plan",
		"--type", "xtrabackup",
		"--backup", backup,
		"--source", src.RootDSN,
		"--target", dst.RootDSN,
		"--out", manifest)
	if code != 0 {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}
	rawManifest, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if !strings.Contains(string(rawManifest), `"prepare_ok": true`) {
		t.Fatalf("seed manifest missing prepare_ok=true:\n%s", rawManifest)
	}

	// 9. Chạy strand seed apply kèm prefetch vào spool.
	catchupStart := time.Now()
	spoolDir := filepath.Join(dir, "state", "prefetch")
	out, code = runStrandCLI(t, 5*time.Minute, "seed", "apply",
		"--manifest", manifest,
		"--target", dst.RootDSN,
		"--spool", spoolDir,
		"--source", src.RootDSN)
	if code != 0 {
		t.Fatalf("seed apply: exit %d\n%s", code, out)
	}

	// 11. Khởi chạy Strand từ seed manifest và đo lượt đọc bảng trên source tới trước post-seed verify.
	cfg := strandCfg{
		Name:      "e2e-xb",
		SourceDSN: src.RootDSN,
		TargetDSN: dst.RootDSN + "strand",
		Include:   []string{"app.*"},
		Exclude:   []string{"app.ignored", "app.nums"},
		Dir:       dir,
		Bootstrap: "seed",
	}
	p := startStrand(t, cfg.write(t), "--start-at", "seed", "--seed-manifest", manifest)
	// Mẫu lượt đọc được lấy TRƯỚC khi kiểm tra log, nên mẫu cuối cùng trước lúc thấy
	// "post-seed verify started" chắc chắn chưa gồm lượt đọc của verify.
	readsSeed := sourceTableReads(t, src)
	started := false
	deadline := time.Now().Add(5 * time.Minute)
	for !started && time.Now().Before(deadline) {
		r := sourceTableReads(t, src)
		if strings.Contains(p.output.String(), "post-seed verify started") {
			started = true
			break
		}
		readsSeed = r
		time.Sleep(100 * time.Millisecond)
	}
	if !started {
		t.Fatalf("post-seed verify never started\n%s", p.tail())
	}
	if d := readsSeed - readsBefore; d != 0 {
		t.Fatalf("seed + catch-up read %d rows of table data on source; want 0", d)
	}
	catchupDur := time.Since(catchupStart)
	t.Logf("seed plan + apply + catch-up: 0 table-data reads on source (before post-seed verify)")

	// 12. Chờ synced, dừng tải, chờ bắt kịp hoàn toàn, rồi đối chiếu trạng thái cuối.
	waitSynced(t, &cfg, p, 10*time.Minute)
	waitCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, appTables...)

	var tbl string
	var srcSum, dstSum int64
	if err := src.DB.QueryRow("CHECKSUM TABLE app.big").Scan(&tbl, &srcSum); err != nil {
		t.Fatalf("checksum source app.big: %v", err)
	}
	if err := dst.DB.QueryRow("CHECKSUM TABLE app.big").Scan(&tbl, &dstSum); err != nil {
		t.Fatalf("checksum target app.big: %v", err)
	}
	if srcSum != dstSum || srcSum == 0 {
		t.Fatalf("app.big checksum mismatch: source=%d target=%d", srcSum, dstSum)
	}
	t.Logf("PASS: data=%d GB (%d rows), backup=%s, restore=%s, catch-up=%s, app.big checksum=%d",
		gb, totalRows, backupDur.Round(time.Millisecond), restoreDur.Round(time.Millisecond), catchupDur.Round(time.Millisecond), srcSum)
}
