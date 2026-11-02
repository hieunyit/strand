package e2e

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/network"
)

func runDockerCmd(t *testing.T, timeout time.Duration, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}
}

// Gate 1: seed từ file mysqldump tạo bằng công cụ thật trong lúc source đang chịu tải ghi,
// tiếp tục ghi trong suốt lúc restore sang target; `seed plan --type mysqldump` in cảnh báo
// bắt buộc về --single-transaction, `seed apply` prefetch log từ P vào spool, catch-up 0 lượt
// đọc dữ liệu bảng trên source trước verify, chạy tải đợt 2 và hội tụ 0 lệch.
func TestG1_E2E_SeedFromMysqldumpUnderLoad(t *testing.T) {
	seed := seedFromEnv(t)
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	src := startMySQL(t, mysqlImage("source"), "src", 1, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nw)
	createSchema(t, src)

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 5000)
	wl.Start(4, 3000)
	defer wl.Stop()
	time.Sleep(2 * time.Second)

	dir := testDir(t)
	dumpFile := filepath.Join(dir, "dump.sql")
	srcID := src.c.GetContainerID()
	dstID := dst.c.GetContainerID()

	// Chụp mysqldump bằng công cụ thật trong lúc tải đang chạy.
	runDockerCmd(t, 2*time.Minute, "exec", srcID, "sh", "-c",
		"mysqldump -uroot -pdev --single-transaction --source-data=2 --set-gtid-purged=ON --databases app > /tmp/dump.sql")
	runDockerCmd(t, time.Minute, "cp", srcID+":/tmp/dump.sql", dumpFile)

	// Giữ tải chạy tiếp trên source trong lúc restore lên target.
	runDockerCmd(t, time.Minute, "cp", dumpFile, dstID+":/tmp/dump.sql")
	runDockerCmd(t, 2*time.Minute, "exec", dstID, "sh", "-c",
		"mysql -uroot -pdev -e 'RESET BINARY LOGS AND GTIDS' && mysql -uroot -pdev < /tmp/dump.sql")
	dst.exec(t, "SET SESSION sql_log_bin=0; CREATE DATABASE IF NOT EXISTS strand; SET SESSION sql_log_bin=1")

	wl.Stop()
	readsBefore := sourceTableReads(t, src)

	manifest := filepath.Join(dir, "seed.json")
	out, code := runStrandCLI(t, time.Minute, "seed", "plan",
		"--type", "mysqldump",
		"--backup", dumpFile,
		"--source", src.RootDSN,
		"--target", dst.RootDSN,
		"--out", manifest,
	)
	if code != 0 || !strings.Contains(out, "WARNING: mysqldump") {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}

	spoolDir := filepath.Join(dir, "state", "prefetch")
	out, code = runStrandCLI(t, 5*time.Minute, "seed", "apply",
		"--manifest", manifest,
		"--target", dst.RootDSN,
		"--spool", spoolDir,
		"--source", src.RootDSN,
	)
	if code != 0 {
		t.Fatalf("seed apply: exit %d\n%s", code, out)
	}

	cfg := strandCfg{
		Name:      "e2e-mysqldump",
		SourceDSN: src.RootDSN,
		TargetDSN: dst.RootDSN + "strand",
		Include:   []string{"app.*"},
		Exclude:   []string{"app.ignored"},
		Dir:       dir,
		Bootstrap: "seed",
	}
	p := startStrand(t, cfg.write(t), "--start-at", "seed", "--seed-manifest", manifest)

	var readsSeed int64 = -1
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		r := sourceTableReads(t, src)
		if strings.Contains(p.output.String(), "post-seed verify started") {
			if readsSeed < 0 {
				readsSeed = readsBefore
			}
			break
		}
		readsSeed = r
		time.Sleep(100 * time.Millisecond)
	}
	if readsSeed < 0 {
		t.Fatalf("post-seed verify never started\n%s", p.tail())
	}
	if d := readsSeed - readsBefore; d != 0 {
		t.Fatalf("seed + catch-up read %d rows of table data on source; want 0", d)
	}
	t.Logf("seed plan + apply + catch-up: 0 table-data reads on source (before post-seed verify)")

	wl2 := newWorkload(src.DB, seed+1)
	wl2.nextID = 2_000_000
	wl2.Start(4, 3000)
	time.Sleep(20 * time.Second)
	wl2.Stop()

	waitSynced(t, &cfg, p, 3*time.Minute)
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, appTables...)
}
