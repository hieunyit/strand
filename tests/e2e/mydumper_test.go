package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/network"

	"strand/internal/seed"
)

const mydumperImage = "docker.io/mydumper/mydumper:v1.0.3-1"

// spec: 02 §9, design §3.3
// Invariants: SD-1, SD-2, SD-3, SD-4
func TestG1_E2E_SeedFromMydumperUnderLoad(t *testing.T) {
	seedVal := seedFromEnv(t)
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	src := startMySQL(t, mysqlImage("source"), "src", 1, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nw)
	createSchema(t, src)

	wl := newWorkload(src.DB, seedVal)
	wl.Preload(t, 5000)
	wl.Start(4, 3000)
	defer wl.Stop()

	dir := testDir(t)
	dumpDir := filepath.Join(dir, "dump")
	if err := os.MkdirAll(dumpDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dumpDir, 0o777); err != nil {
		t.Fatal(err)
	}

	dumpLog := dumpDir + ".log"
	dumpCmd := exec.Command("docker", "run", "--rm",
		"--network", nw.Name,
		"-v", dumpDir+":/dump",
		mydumperImage,
		"mydumper",
		"--host=src",
		"--user=root",
		"--password=dev",
		"--database=app",
		"--outputdir=/dump",
		"--threads=4",
		"-v", "3",
	)
	dumpOut, err := dumpCmd.CombinedOutput()
	if err := os.WriteFile(dumpLog, dumpOut, 0o644); err != nil {
		t.Fatalf("write mydumper log: %v", err)
	}
	if err != nil {
		t.Fatalf("mydumper failed: %v\n%s", err, dumpOut)
	}

	chmodCmd := exec.Command("docker", "run", "--rm",
		"-v", dumpDir+":/dump",
		mydumperImage,
		"chmod", "-R", "a+rwX", "/dump",
	)
	if out, err := chmodCmd.CombinedOutput(); err != nil {
		t.Fatalf("chmod dumpDir failed: %v\n%s", err, out)
	}

	metaRaw, err := os.ReadFile(filepath.Join(dumpDir, "metadata"))
	if err != nil {
		t.Fatalf("read mydumper metadata: %v", err)
	}
	t.Logf("mydumper metadata:\n%s", string(metaRaw))

	loadCmd := exec.Command("docker", "run", "--rm",
		"--network", nw.Name,
		"-v", dumpDir+":/dump",
		mydumperImage,
		"myloader",
		"--host=dst",
		"--user=root",
		"--password=dev",
		"--directory=/dump",
		"--threads=4",
		"--drop-table=DROP",
	)
	if out, err := loadCmd.CombinedOutput(); err != nil {
		t.Fatalf("myloader failed: %v\n%s", err, out)
	}

	meta, err := seed.ParseMydumperMetadata(dumpDir)
	if err != nil {
		t.Fatalf("parse mydumper metadata: %v", err)
	}
	p := strings.ReplaceAll(meta.GTID, "\n", "")

	// myloader ghi vào binlog/GTID riêng của dst; đặt lại gtid_purged = P để khớp SD-1
	// trước khi chạy seed plan (--target) và seed apply (prepareRestoredTarget).
	dst.exec(t,
		"RESET BINARY LOGS AND GTIDS",
		fmt.Sprintf("SET GLOBAL gtid_purged = '%s'", p),
		"SET SESSION sql_log_bin = 0; CREATE DATABASE IF NOT EXISTS strand; SET SESSION sql_log_bin = 1",
	)

	wl.Stop()
	readsBefore := sourceTableReads(t, src)

	manifest := filepath.Join(dir, "seed.json")
	spoolDir := filepath.Join(dir, "state", "prefetch")

	out, code := runStrandCLI(t, time.Minute, "seed", "plan",
		"--type", "mydumper",
		"--backup", dumpDir,
		"--dump-log", dumpLog,
		"--source", src.RootDSN,
		"--target", dst.RootDSN,
		"--out", manifest,
	)
	if code != 0 {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}

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
		Name:      "e2e-mydumper",
		SourceDSN: src.RootDSN,
		TargetDSN: dst.RootDSN + "strand",
		Include:   []string{"app.*"},
		Exclude:   []string{"app.ignored"},
		Dir:       dir,
		Bootstrap: "seed",
	}
	sp := startStrand(t, cfg.write(t), "--start-at", "seed", "--seed-manifest", manifest)

	readsSeed := readsBefore
	verifySeen := false
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		r := sourceTableReads(t, src)
		if strings.Contains(sp.output.String(), "post-seed verify started") {
			verifySeen = true
			break
		}
		readsSeed = r
		if sp.exited() {
			t.Fatalf("strand exited (code %d) before post-seed verify started\n%s", sp.exitCode(), sp.tail())
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !verifySeen {
		t.Fatalf("post-seed verify never started within 5m\n%s", sp.tail())
	}
	if d := readsSeed - readsBefore; d != 0 {
		t.Fatalf("seed + catch-up read %d rows of table data on source; want 0", d)
	}
	t.Logf("mydumper seed + catch-up: 0 table-data reads on source (before post-seed verify)")

	wl2 := newWorkload(src.DB, seedVal+1)
	wl2.nextID = 2_000_000
	wl2.Start(4, 3000)
	time.Sleep(20 * time.Second)
	wl2.Stop()

	waitSynced(t, &cfg, sp, 3*time.Minute)
	waitCaughtUp(t, src, dst, &cfg, sp, 3*time.Minute)
	if code := sp.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, sp.tail())
	}
	compareFinal(t, src, dst, appTables...)
}

// spec: 02 §9, design §3.3
// Invariants: SD-1
func TestP1_E2E_MydumperNoLockRejected(t *testing.T) {
	seedVal := seedFromEnv(t)
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	src := startMySQL(t, mysqlImage("source"), "src", 1, nw)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nw)
	createSchema(t, src)

	wl := newWorkload(src.DB, seedVal)
	wl.Preload(t, 1000)
	wl.Start(2, 1000)
	defer wl.Stop()

	dir := testDir(t)
	dumpDir := filepath.Join(dir, "dump")
	if err := os.MkdirAll(dumpDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dumpDir, 0o777); err != nil {
		t.Fatal(err)
	}

	dumpLog := dumpDir + ".log"
	dumpCmd := exec.Command("docker", "run", "--rm",
		"--network", nw.Name,
		"-v", dumpDir+":/dump",
		mydumperImage,
		"mydumper",
		"--host=src",
		"--user=root",
		"--password=dev",
		"--database=app",
		"--outputdir=/dump",
		"--threads=4",
		"--sync-thread-lock-mode=NO_LOCK",
		"-v", "3",
	)
	dumpOut, err := dumpCmd.CombinedOutput()
	if err := os.WriteFile(dumpLog, dumpOut, 0o644); err != nil {
		t.Fatalf("write mydumper log: %v", err)
	}
	if err != nil {
		t.Fatalf("mydumper failed: %v\n%s", err, dumpOut)
	}

	chmodCmd := exec.Command("docker", "run", "--rm",
		"-v", dumpDir+":/dump",
		mydumperImage,
		"chmod", "-R", "a+rwX", "/dump",
	)
	if out, err := chmodCmd.CombinedOutput(); err != nil {
		t.Fatalf("chmod dumpDir failed: %v\n%s", err, out)
	}

	manifest := filepath.Join(dir, "seed.json")
	out, code := runStrandCLI(t, time.Minute, "seed", "plan",
		"--type", "mydumper",
		"--backup", dumpDir,
		"--source", src.RootDSN,
		"--target", dst.RootDSN,
		"--dump-log", dumpLog,
		"--out", manifest,
	)
	if code != 3 {
		t.Fatalf("seed plan with NO_LOCK: want exit 3, got %d\n%s", code, out)
	}
	if !strings.Contains(out, "NO_LOCK") {
		t.Fatalf("seed plan output must mention NO_LOCK, got:\n%s", out)
	}
}

// spec: 02 §9, design §3.3
