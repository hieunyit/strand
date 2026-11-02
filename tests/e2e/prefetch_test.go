package e2e

import (
	"context"
	"database/sql"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// archiveBinlogs đóng binlog hiện tại rồi chép mọi file binlog của source ra dir (archive).
func archiveBinlogs(t *testing.T, src *mysqlNode, dir string) {
	t.Helper()
	src.exec(t, "FLUSH BINARY LOGS")
	rows, err := src.DB.Query("SHOW BINARY LOGS")
	if err != nil {
		t.Fatal(err)
	}
	cols, _ := rows.Columns()
	var names []string
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		_ = rows.Scan(ptrs...)
		names = append(names, string(vals[0]))
	}
	rows.Close()
	_ = os.MkdirAll(dir, 0o750)
	for _, n := range names[:len(names)-1] { // file cuối đang mở, chưa có gì quan trọng
		rc, err := src.c.CopyFileFromContainer(context.Background(), "/var/lib/mysql/"+n)
		if err != nil {
			t.Fatalf("copy %s: %v", n, err)
		}
		f, _ := os.Create(filepath.Join(dir, n))
		_, _ = io.Copy(f, rc)
		f.Close()
		rc.Close()
	}
}

type prefetchEnv struct {
	src, dst *mysqlNode
	dir      string
	manifest string
	archive  string
	cfg      strandCfg
}

// setupPurgedAfterP dựng: target là bản sao tại P (replica dừng ở P, đã gỡ replication),
// source chạy tiếp, archive giữ binlog, rồi source PURGE binlog: live stream không còn phục
// vụ được phần sau P.
func setupPurgedAfterP(t *testing.T, seed int64) *prefetchEnv {
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	e := &prefetchEnv{}
	e.src = startMySQL(t, mysqlImage("source"), "src", 1, nw)
	e.dst = startMySQL(t, mysqlImage("target"), "dst", 2, nw)
	makeReplica(t, e.dst, "src")
	createSchema(t, e.src)
	wl := newWorkload(e.src.DB, seed)
	wl.Preload(t, 3000)
	waitReplicaCaughtUp(t, e.src, e.dst)
	e.dst.exec(t, "STOP REPLICA", "RESET REPLICA ALL")
	// Schema metadata của Strand tạo TRƯỚC khi chụp P (mọi ghi cục bộ trên target sau P sẽ
	// làm seed apply từ chối, đúng thiết kế).
	e.dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	p := strings.ReplaceAll(e.dst.gtidExecuted(t), "\n", "")

	wl.Start(4, 3000) // Txn sau P
	time.Sleep(8 * time.Second)
	wl.Stop()

	e.dir = testDir(t)
	e.archive = filepath.Join(e.dir, "archive")
	archiveBinlogs(t, e.src, e.archive)
	archived := strings.ReplaceAll(e.src.gtidExecuted(t), "\n", "")
	// PURGE mọi binlog đã archive: source không còn giữ log từ P.
	rows, _ := e.src.DB.Query("SHOW BINARY LOGS")
	var lastName string
	for rows.Next() {
		cols, _ := rows.Columns()
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		_ = rows.Scan(ptrs...)
		lastName = string(vals[0])
	}
	rows.Close()
	e.src.exec(t, "PURGE BINARY LOGS TO '"+lastName+"'")

	backup := filepath.Join(e.dir, "backup")
	_ = os.MkdirAll(backup, 0o750)
	_ = os.WriteFile(filepath.Join(backup, "clone_status.txt"), []byte(p), 0o640)
	e.manifest = filepath.Join(e.dir, "seed.json")
	if out, code := runStrandCLI(t, time.Minute, "seed", "plan", "--type", "clone", "--backup", backup,
		"--source", e.src.RootDSN, "--archive-until", archived, "--out", e.manifest); code != 0 {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}
	e.cfg = strandCfg{Name: "e2e-prefetch", SourceDSN: e.src.RootDSN, TargetDSN: e.dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: e.dir, Bootstrap: "seed"}
	return e
}

// setupSeedAtP dựng env tại vị trí P: target đã STOP/RESET replica và tạo DB strand trước khi
// chụp P, chạy seed plan --type clone; không archive, không purge.
func setupSeedAtP(t *testing.T, seed int64) *prefetchEnv {
	t.Helper()
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })
	e := &prefetchEnv{}
	e.src = startMySQL(t, mysqlImage("source"), "src", 1, nw)
	e.dst = startMySQL(t, mysqlImage("target"), "dst", 2, nw)
	makeReplica(t, e.dst, "src")
	createSchema(t, e.src)
	wl := newWorkload(e.src.DB, seed)
	wl.Preload(t, 3000)
	waitReplicaCaughtUp(t, e.src, e.dst)
	e.dst.exec(t, "STOP REPLICA", "RESET REPLICA ALL")
	e.dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	p := strings.ReplaceAll(e.dst.gtidExecuted(t), "\n", "")

	e.dir = testDir(t)
	backup := filepath.Join(e.dir, "backup")
	_ = os.MkdirAll(backup, 0o750)
	_ = os.WriteFile(filepath.Join(backup, "clone_status.txt"), []byte(p), 0o640)
	e.manifest = filepath.Join(e.dir, "seed.json")
	if out, code := runStrandCLI(t, time.Minute, "seed", "plan", "--type", "clone", "--backup", backup,
		"--source", e.src.RootDSN, "--out", e.manifest); code != 0 {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}
	e.cfg = strandCfg{Name: "e2e-prefetch", SourceDSN: e.src.RootDSN, TargetDSN: e.dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: e.dir, Bootstrap: "seed"}
	return e
}

// P1-14 / Gate 1: archive bị xóa trong lúc seed apply — prefetch đã đưa log vào spool nên
// catch-up vẫn đủ; 0 lệch.
func TestG1_E2E_PrefetchRescuesDeletedArchive(t *testing.T) {
	e := setupPurgedAfterP(t, seedFromEnv(t))
	spoolDir := filepath.Join(e.dir, "state", "prefetch")
	out, code := runStrandCLI(t, 5*time.Minute, "seed", "apply", "--manifest", e.manifest, "--target", e.dst.RootDSN,
		"--spool", spoolDir, "--archive", e.archive, "--source", e.src.RootDSN)
	if code != 0 || !strings.Contains(out, "prefetch:") {
		t.Fatalf("seed apply with prefetch: exit %d\n%s", code, out)
	}
	t.Logf("%s", strings.TrimSpace(out))
	if err := os.RemoveAll(e.archive); err != nil { // archive bị xóa
		t.Fatal(err)
	}
	p := startStrand(t, e.cfg.write(t), "--start-at", "seed", "--seed-manifest", e.manifest)
	waitSynced(t, &e.cfg, p, 3*time.Minute)
	waitCaughtUp(t, e.src, e.dst, &e.cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, e.src, e.dst, appTables...)
}

// Tắt prefetch thì catch-up phải báo log_gap (exit 5), không chạy tiếp với dữ liệu thiếu.
func TestG1_E2E_NoPrefetchReportsLogGap(t *testing.T) {
	e := setupPurgedAfterP(t, seedFromEnv(t))
	if out, code := runStrandCLI(t, 5*time.Minute, "seed", "apply", "--manifest", e.manifest, "--target", e.dst.RootDSN,
		"--prefetch=false"); code != 0 {
		t.Fatalf("seed apply: exit %d\n%s", code, out)
	}
	_ = os.RemoveAll(e.archive)
	p := startStrand(t, e.cfg.write(t), "--start-at", "seed", "--seed-manifest", e.manifest)
	select {
	case <-p.done:
	case <-time.After(2 * time.Minute):
		p.kill9()
		t.Fatalf("pipeline did not stop on missing log")
	}
	if code := p.exitCode(); code != 5 || !strings.Contains(p.output.String(), "log_gap") {
		t.Fatalf("exit %d, want 5 log_gap\n%s", code, p.tail())
	}
}

// Mọi tầng spool chạm hard limit thì prefetch báo prefetch_exhausted (exit 5).
func TestG1_E2E_PrefetchExhausted(t *testing.T) {
	e := setupPurgedAfterP(t, seedFromEnv(t))
	out, code := runStrandCLI(t, 5*time.Minute, "seed", "apply", "--manifest", e.manifest, "--target", e.dst.RootDSN,
		"--spool", filepath.Join(e.dir, "state", "prefetch"), "--spool-hard", "20000", "--archive", e.archive, "--source", e.src.RootDSN)
	if code != 5 || !strings.Contains(out, "prefetch_exhausted") {
		t.Fatalf("exit %d, want 5 prefetch_exhausted\n%s", code, out)
	}
}

// P1-14 / Gate 1: spool local đầy (restore lâu) thì prefetch tràn sang bucket staging (MinIO);
// catch-up đọc lại được từ cả hai tầng; 0 lệch.
func TestG1_E2E_PrefetchSpoolOverflowsToBucket(t *testing.T) {
	ctx := context.Background()
	mc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "minio:latest", ExposedPorts: []string{"9000/tcp"},
			Env:        map[string]string{"MINIO_ROOT_USER": "minioadmin", "MINIO_ROOT_PASSWORD": "minioadmin"},
			Cmd:        []string{"server", "/data"},
			WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(90 * time.Second),
		}, Started: true})
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	t.Cleanup(func() { _ = mc.Terminate(context.Background()) })
	host, _ := mc.Host(ctx)
	port, _ := mc.MappedPort(ctx, "9000")
	t.Setenv("STRAND_S3_ENDPOINT", "http://"+net.JoinHostPort(host, port.Port()))
	t.Setenv("STRAND_S3_ACCESS_KEY", "minioadmin")
	t.Setenv("STRAND_S3_SECRET_KEY", "minioadmin")

	e := setupPurgedAfterP(t, seedFromEnv(t))
	e.cfg.Overflow = "s3://strand-staging/e2e-prefetch"
	out, code := runStrandCLI(t, 5*time.Minute, "seed", "apply", "--manifest", e.manifest, "--target", e.dst.RootDSN,
		"--spool", filepath.Join(e.dir, "state", "prefetch"), "--spool-local-hard", "50000",
		"--overflow", e.cfg.Overflow, "--archive", e.archive, "--source", e.src.RootDSN)
	if code != 0 || strings.Contains(out, "bucket 0 bytes") {
		t.Fatalf("seed apply with bucket overflow: exit %d (expected some bytes in the bucket tier)\n%s", code, out)
	}
	t.Logf("%s", strings.TrimSpace(out))
	_ = os.RemoveAll(e.archive)
	p := startStrand(t, e.cfg.write(t), "--start-at", "seed", "--seed-manifest", e.manifest)
	waitSynced(t, &e.cfg, p, 3*time.Minute)
	waitCaughtUp(t, e.src, e.dst, &e.cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, e.src, e.dst, appTables...)
}

// P1-14 / Gate 1: restore kéo dài (--restore-cmd), source ghi liên tục và mỗi 5 s FLUSH + PURGE
// binlog; prefetch đọc liên tục từ source vào spool (tràn sang bucket) trong suốt lúc restore,
// cứu phần log đã bị purge; sau đó sync --start-at seed bắt kịp 0 lệch.
func TestG1_E2E_PrefetchDuringLongRestore(t *testing.T) {
	ctx := context.Background()
	mc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "minio:latest", ExposedPorts: []string{"9000/tcp"},
			Env:        map[string]string{"MINIO_ROOT_USER": "minioadmin", "MINIO_ROOT_PASSWORD": "minioadmin"},
			Cmd:        []string{"server", "/data"},
			WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(90 * time.Second),
		}, Started: true})
	if err != nil {
		t.Fatalf("start minio: %v", err)
	}
	t.Cleanup(func() { _ = mc.Terminate(context.Background()) })
	host, _ := mc.Host(ctx)
	port, _ := mc.MappedPort(ctx, "9000")
	t.Setenv("STRAND_S3_ENDPOINT", "http://"+net.JoinHostPort(host, port.Port()))
	t.Setenv("STRAND_S3_ACCESS_KEY", "minioadmin")
	t.Setenv("STRAND_S3_SECRET_KEY", "minioadmin")

	seed := seedFromEnv(t)
	e := setupSeedAtP(t, seed)
	e.cfg.Overflow = "s3://strand-staging/e2e-long-restore"

	wl := newWorkload(e.src.DB, seed+1)
	wl.nextID = 2_000_000
	wl.Start(6, 3000)

	// Restore kết thúc thì tạo file đánh dấu; vòng purge dừng ở đó. Ghi sau lúc restore xong nằm ngoài phạm vi
	// prefetch (sync live đọc tiếp từ source), nên purge chúng trước khi sync chạy là kịch bản không hợp lệ.
	restoreDone := filepath.Join(e.dir, "restore.done")
	stopPurge := make(chan struct{})
	var purgeWG sync.WaitGroup
	purgeWG.Add(1)
	go func() {
		defer purgeWG.Done()
		tk := time.NewTicker(5 * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-stopPurge:
				return
			case <-tk.C:
				if _, err := os.Stat(restoreDone); err == nil {
					return
				}
				// Tránh chạy sát ranh giới giây khiến FLUSH ở giây T còn PURGE ở giây T+1.
				if rem := time.Second - time.Duration(time.Now().Nanosecond()); rem < 150*time.Millisecond {
					select {
					case <-stopPurge:
						return
					case <-time.After(rem + 10*time.Millisecond):
					}
				}
				_, _ = e.src.DB.Exec("FLUSH BINARY LOGS")
				_, _ = e.src.DB.Exec("PURGE BINARY LOGS BEFORE NOW()")
			}
		}
	}()

	spoolDir := filepath.Join(e.dir, "state", "prefetch")
	out, code := runStrandCLI(t, 5*time.Minute, "seed", "apply", "--manifest", e.manifest, "--target", e.dst.RootDSN,
		"--spool", spoolDir, "--spool-local-hard", "200000", "--overflow", e.cfg.Overflow,
		"--source", e.src.RootDSN, "--restore-cmd", "sleep 40 && touch "+restoreDone)
	close(stopPurge)
	purgeWG.Wait()
	wl.Stop()
	if code != 0 || !strings.Contains(out, "bucket ") || strings.Contains(out, "bucket 0 bytes") {
		t.Fatalf("seed apply with long restore: exit %d (expected some bytes in the bucket tier)\n%s", code, out)
	}
	t.Logf("%s", strings.TrimSpace(out))

	p := startStrand(t, e.cfg.write(t), "--start-at", "seed", "--seed-manifest", e.manifest)
	waitSynced(t, &e.cfg, p, 3*time.Minute)
	waitCaughtUp(t, e.src, e.dst, &e.cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, e.src, e.dst, appTables...)
}
