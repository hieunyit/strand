package e2e

// Phase 2: snapshot logic từ source PostgreSQL (02 §5, design §6.1) và `--start-at seed` từ seed.json
// PG (02 §9, strand export).

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// pgPKMover đổi PK của app.events qua lại giữa các range (khóa đích ngẫu nhiên trong [1, 2·span]):
// nửa tombstone và nửa upsert thường rơi vào hai range khác trạng thái.
func pgPKMover(db *sql.DB, seed, span int64, stop *atomic.Bool, wg *sync.WaitGroup, moves *atomic.Int64) {
	defer wg.Done()
	r := rand.New(rand.NewSource(seed))
	for !stop.Load() {
		from, to := 1+r.Int63n(span*2), 1+r.Int63n(span*2)
		res, err := db.Exec("UPDATE app.events SET id = $1, ver = nextval('app.ver_seq') WHERE id = $2", to, from)
		if err != nil {
			continue
		}
		if n, _ := res.RowsAffected(); n == 1 {
			moves.Add(1)
		}
	}
}

// setupPGLogical: source có lược đồ + dữ liệu (synchronous_commit=on: commit record được flush và có
// thể được stream TRƯỚC khi Txn visible — đúng race mà watermark PG phải xử lý), target chỉ có lược
// đồ rỗng + trigger đếm apply.
func setupPGLogical(t *testing.T, pub string, items, accounts, events int) (*pgNode, *pgNode) {
	src := startPG(t, "pgsrc", "-c", "synchronous_commit=on")
	dst := startPG(t, "pgdst")
	src.exec(t, pgSchemaSQL...)
	dst.exec(t, pgSchemaSQL...)
	pgPreload(t, src, items, accounts, events)
	dst.exec(t, pgApplyLogSQL...)
	src.exec(t, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')", pub, strings.Join(pgAppTables, ", ")))
	return src, dst
}

// Phase 2 (02 §5): bootstrap logic PG→PG — 3 bảng có dữ liệu (items có cột TOAST > 2 KB, accounts có
// UNIQUE ngoài PK, events bị đổi PK qua các range) được snapshot theo chunk song song với stream dưới
// tải dày (nhiều commit đồng thời quanh mỗi lần đọc chunk, PREPARE TRANSACTION, Txn lớn bị stream).
// Lặp lại STRAND_E2E_PG_SNAPSHOT_ITERATIONS lần (mặc định 5), mỗi lần cặp container mới:
// trạng thái cuối giống hệt, doc không NULL, không phiên bản row nào bị ghi hai lần, ver không giảm.
func TestP2_E2E_PGLogicalSnapshotUnderLoad(t *testing.T) {
	seed := seedFromEnv(t)
	n := envInt("STRAND_E2E_PG_SNAPSHOT_ITERATIONS", 5)
	for i := 1; i <= n; i++ {
		t.Run(fmt.Sprintf("run%d", i), func(t *testing.T) { runPGLogicalSnapshot(t, seed+int64(i)*7919, false) })
	}
}

// Phase 2 (02 §5): kill -9 Strand khi snapshot logic PG đang chạy dở (đã flush vài chunk), khởi động
// lại: plan snapshot được làm tiếp (không đọc lại từ đầu), trạng thái cuối giống hệt, không phiên bản
// row nào bị ghi hai lần, ver không giảm.
func TestP2_E2E_PGLogicalSnapshotResumeAfterKill(t *testing.T) {
	runPGLogicalSnapshot(t, seedFromEnv(t), true)
}

// snapshotInProgress: state cục bộ có plan đã flush ít nhất một chunk mà bảng chưa xong.
func snapshotInProgress(dir string) bool {
	raw, err := os.ReadFile(filepath.Join(dir, "state", "pipeline-state.json"))
	if err != nil {
		return false
	}
	var st struct {
		Plans map[string]struct {
			MinPK  int64 `json:"min_pk"`
			NextPK int64 `json:"next_pk"`
		} `json:"snapshot_plans"`
	}
	if json.Unmarshal(raw, &st) != nil {
		return false
	}
	for _, p := range st.Plans {
		if p.NextPK > p.MinPK {
			return true
		}
	}
	return false
}

func runPGLogicalSnapshot(t *testing.T, seed int64, kill bool) {
	const span = 12000
	slot, pub := "strand_e2e_logical", "strand_e2e_pub"
	src, dst := setupPGLogical(t, pub, span, span, 2*span)

	wl := newPGWorkload(src.DB, seed)
	wl.Start(3, span)
	var stop atomic.Bool
	var wg sync.WaitGroup
	var moves atomic.Int64
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go pgPKMover(src.DB, seed+int64(i), span, &stop, &wg, &moves)
	}
	time.Sleep(time.Second)

	cfg := strandCfg{Name: "e2e-pg-logical", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s, create_slot: true", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "logical", SourceIOPS: 20000}
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	if kill {
		deadline := time.Now().Add(3 * time.Minute)
		durable := func() bool { // đã có checkpoint: lần chạy sau làm tiếp, không bootstrap lại
			got, err := checkpoint.LoadAppliedSnapshotPG(context.Background(), dst.DB, cfg.Name, model.EnginePG)
			return err == nil && got.Lwd().LSN > 0
		}
		for !snapshotInProgress(cfg.Dir) || !durable() {
			if p.exited() || time.Now().After(deadline) {
				t.Fatalf("snapshot never in progress\n%s", p.tail())
			}
			time.Sleep(20 * time.Millisecond)
		}
		p.kill9()
		raw, _ := os.ReadFile(filepath.Join(cfg.Dir, "state", "pipeline-state.json"))
		t.Logf("killed mid-snapshot, state: %s", raw)
		p = startStrand(t, cfgPath)
	}
	waitSynced(t, &cfg, p, 5*time.Minute)
	time.Sleep(3 * time.Second) // stream tiếp sau khi mọi range đã "Xong"

	stop.Store(true)
	wg.Wait()
	wl.Stop()
	t.Logf("workload: %d ops, %d client-side errors, %d two-phase txns, %d big txns, %d PK moves",
		wl.ops.Load(), wl.errs.Load(), wl.prepared.Load(), wl.bigTxns.Load(), moves.Load())
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	out := p.output.String()
	if !kill && !strings.Contains(out, "snapshot visibility gate: exported snapshot of the new slot") {
		t.Fatalf("expected the exported-snapshot visibility gate to be used\n%s", p.tail())
	}
	if c := strings.Count(out, `"snapshot done"`); !kill && c < 4 {
		t.Fatalf("expected 4 tables snapshotted, log shows %d", c)
	}
	if kill && !strings.Contains(out, "snapshot resumes from saved plan") {
		t.Fatalf("restart after kill -9 did not resume the saved snapshot plan\n%s", p.tail())
	}
	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	if kill {
		// kill -9 giữa flush chunk và lưu plan: range được đọc lại, snapshot ghi lại phiên bản đã có.
		checkPGInvariantsSnapshotKilled(t, dst)
	} else {
		checkPGInvariants(t, dst)
	}
}

// Phase 2 (02 §10): `strand snapshot add` một bảng PG khi stream đang chạy dưới tải, có đổi PK qua
// các range của chính bảng đó.
func TestP2_E2E_PGSnapshotAddWhileStreaming(t *testing.T) {
	seed := seedFromEnv(t)
	slot, pub := "strand_e2e_snapadd", "strand_e2e_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)
	dir := testDir(t)
	witness := beginF11Witness(t, src, dir, lsn)
	// Thêm dữ liệu cho events ở source (target có cùng preload; phần thêm chỉ có ở source) để bảng có
	// nhiều chunk.
	src.exec(t, "INSERT INTO app.events SELECT g, nextval('app.ver_seq'), 'pre' FROM generate_series(3001, 20000) g")

	wl := newPGWorkload(src.DB, seed)
	wl.Start(3, 3000)
	var stop atomic.Bool
	var wg sync.WaitGroup
	var moves atomic.Int64
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go pgPKMover(src.DB, seed+int64(i), 10000, &stop, &wg, &moves)
	}

	cfg := strandCfg{Name: "e2e-pg-snapadd", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.items", "app.accounts", "app.marker"}, Dir: dir, Bootstrap: "start_at", SourceIOPS: 20000}
	p := startStrand(t, cfg.write(t), "--start-at", lsn)
	waitSynced(t, &cfg, p, time.Minute)
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")
	time.Sleep(2 * time.Second)

	out, code := runStrandCLI(t, 30*time.Second, "snapshot", "add", "--table", "app.events", "--socket", cfg.Socket)
	if code != 0 {
		t.Fatalf("snapshot add: exit %d: %s", code, out)
	}
	time.Sleep(time.Second)
	waitSynced(t, &cfg, p, 5*time.Minute)
	time.Sleep(3 * time.Second)

	stop.Store(true)
	wg.Wait()
	wl.Stop()
	t.Logf("workload: %d ops, %d errors, %d PK moves during snapshot add", wl.ops.Load(), wl.errs.Load(), moves.Load())
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	if !strings.Contains(p.output.String(), `"snapshot done"`) {
		t.Fatalf("snapshot add did not finish\n%s", p.tail())
	}
	witness.capture(t, dst, &cfg)
	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariants(t, dst)
}

// pgDumpInContainer tạo script thay pg_dump (host không có pg_dump): chạy pg_dump trong container
// source, đổi host:port trong --dbname thành localhost:5432, -f <file> thành redirect ra file host.
func pgDumpInContainer(t *testing.T, n *pgNode) string {
	t.Helper()
	host, _ := n.c.Host(context.Background())
	port, _ := n.c.MappedPort(context.Background(), "5432")
	hostPort := fmt.Sprintf("%s:%s", host, port.Port())
	id := n.c.GetContainerID()
	script := fmt.Sprintf(`#!/bin/bash
args=(); out=""
while [ $# -gt 0 ]; do
  case "$1" in
    -f) out="$2"; shift 2; continue ;;
    --dbname=*) args+=("${1/%s/localhost:5432}") ;;
    *) args+=("$1") ;;
  esac
  shift
done
if [ -n "$out" ]; then
  exec docker exec -e PGPASSWORD="$PGPASSWORD" %s pg_dump "${args[@]}" > "$out"
fi
exec docker exec -e PGPASSWORD="$PGPASSWORD" %s pg_dump "${args[@]}"
`, hostPort, id, id)
	p := filepath.Join(t.TempDir(), "pg_dump")
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// Phase 2 (02 §9): seed PG→PG bằng `strand export` (slot SNAPSHOT 'export' + pg_dump --snapshot) khi
// source đang ghi liên tục → pg_restore vào target → `strand sync --start-at seed`. Ranh giới P: Txn có
// commit LSN < P chỉ đến từ dump, ≥ P chỉ đến từ slot — trigger đếm apply ở target bắt được Txn bị
// apply hai lần (thừa ở P) và so sánh cuối bắt được Txn thiếu.
func TestP2_E2E_PGSeedExportStartAtSeed(t *testing.T) {
	seed := seedFromEnv(t)
	pub := "strand_e2e_pub"
	src := startPG(t, "pgsrc", "-c", "synchronous_commit=on")
	dst := startPG(t, "pgdst")
	src.exec(t, pgSchemaSQL...)
	dst.exec(t, pgSchemaSQL...)
	pgPreload(t, src, 5000, 2000, 10000)
	dst.exec(t, pgApplyLogSQL...)
	src.exec(t, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')", pub, strings.Join(pgAppTables, ", ")))

	wl := newPGWorkload(src.DB, seed)
	wl.Start(4, 3000)
	time.Sleep(2 * time.Second)

	dir := testDir(t)
	dump := filepath.Join(dir, "seed.dump")
	manifest := filepath.Join(dir, "seed.json")
	out, code := runStrandCLI(t, 5*time.Minute, "export", "--source", src.DSN, "--tables", strings.Join(pgAppTables, ","),
		"--out", dump, "-F", "c", "--slot", "strand_seed_slot", "--pg-dump", pgDumpInContainer(t, src), "--manifest", manifest)
	if code != 0 {
		t.Fatalf("strand export: exit %d\n%s", code, out)
	}
	raw, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("seed.json: %s", raw)

	// Restore dữ liệu (lược đồ đã có) vào target trong lúc source vẫn ghi.
	f, err := os.Open(dump)
	if err != nil {
		t.Fatal(err)
	}
	restore := exec.Command("docker", "exec", "-i", dst.c.GetContainerID(), "pg_restore", "-U", "strand", "-d", "strand", "--data-only")
	restore.Stdin = f
	if o, err := restore.CombinedOutput(); err != nil {
		t.Fatalf("pg_restore: %v\n%s", err, o)
	}
	_ = f.Close()

	cfg := strandCfg{Name: "e2e-pg-seed", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN, SourceExtra: fmt.Sprintf(", publication: %s", pub),
		Include: []string{"app.*"}, Dir: dir, Bootstrap: "seed"}
	cfgPath := cfg.write(t)
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")
	p := startStrand(t, cfgPath, "--start-at", "seed", "--seed-manifest", manifest)
	time.Sleep(10 * time.Second)
	// Restart giữa chừng: slot của seed được nhớ (cấu hình không đặt source.slot).
	if code := p.stop(t); code != 0 {
		t.Fatalf("first run exit %d\n%s", code, p.tail())
	}
	firstOut := p.output.String()
	p = startStrand(t, cfgPath)
	time.Sleep(5 * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops, %d errors, %d two-phase txns", wl.ops.Load(), wl.errs.Load(), wl.prepared.Load())
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariants(t, dst)

	// Verify sau seed tự động cho PG→PG (02 §9): pipeline chỉ synced sau khi post-seed verify báo 0 lệch
	// (trạng thái verify pending giữ qua restart, nên lượt chạy nào cũng có thể là lượt verify).
	all := firstOut + p.output.String()
	if !strings.Contains(all, "post-seed verify started") || !strings.Contains(all, "post-seed verify passed: pipeline synced") {
		t.Fatalf("post-seed verify did not run/pass for the PG seed\n%s", p.tail())
	}
	if strings.Contains(all, "MySQL→MySQL only") {
		t.Fatalf("stale MySQL→MySQL-only post-seed warning\n%s", p.tail())
	}
	// Đối chiếu thêm: verify --mode full báo 0 lệch.
	for _, tb := range []string{"app.items", "app.accounts", "app.events"} {
		vout, vcode := runStrandCLI(t, 5*time.Minute, "verify", "-c", cfgPath, "--mode", "full", "--table", tb)
		if vcode != 0 {
			t.Fatalf("verify --mode full --table %s: exit %d\n%s", tb, vcode, vout)
		}
		t.Logf("verify --mode full --table %s: %s", tb, strings.TrimSpace(vout))
	}
}

// Phase 2 (02 §5, RS-1): khuếch đại đúng race của watermark PG. Source có standby đồng bộ là
// pg_receivewal --synchronous (synchronous_standby_names = 'rw'): mỗi commit chờ pg_receivewal fsync
// WAL SAU khi commit record đã được flush — và logical decoding đã có thể stream nó — nhưng TRƯỚC
// ProcArrayEndTransaction. Vì vậy Txn thường xuyên được stream (và route) khi còn CHƯA visible với
// snapshot của SELECT. Low watermark theo LSN sẽ bỏ event của những Txn này trong khi row snapshot
// không có chúng (mất cập nhật); low theo snapshot MVCC + chờ xid đã bỏ visible phải cho trạng thái
// cuối giống hệt.
// (Walsender của chính Strand KHÔNG được làm standby đồng bộ: Strand xác nhận commit LSN — đầu record —
// của Txn đã apply, còn sync rep chờ cuối record; Txn cuối cùng trước barrier sẽ treo.)
func TestP2_E2E_PGLogicalSnapshotSyncRepRace(t *testing.T) {
	seed := seedFromEnv(t)
	const span = 6000
	slot, pub := "strand_e2e_syncrep", "strand_e2e_pub"
	src, dst := setupPGLogical(t, pub, span, span, 2*span)
	rw := exec.Command("docker", "exec", "-u", "postgres", src.c.GetContainerID(), "bash", "-c",
		"mkdir -p /tmp/rw && exec pg_receivewal -D /tmp/rw --synchronous --no-loop -d 'host=localhost user=strand password=dev application_name=rw'")
	if err := rw.Start(); err != nil {
		t.Fatalf("start pg_receivewal: %v", err)
	}
	t.Cleanup(func() { _ = rw.Process.Kill(); _ = rw.Wait() })
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		_ = src.DB.QueryRow("SELECT count(*) FROM pg_stat_replication WHERE application_name = 'rw'").Scan(&n)
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pg_receivewal did not connect")
		}
		time.Sleep(200 * time.Millisecond)
	}
	src.exec(t, "ALTER SYSTEM SET synchronous_standby_names = 'rw'", "SELECT pg_reload_conf()")
	t.Cleanup(func() {
		_, _ = src.DB.Exec("ALTER SYSTEM RESET synchronous_standby_names")
		_, _ = src.DB.Exec("SELECT pg_reload_conf()")
	})

	wl := newPGWorkload(src.DB, seed)
	wl.Start(8, span)
	var stop atomic.Bool
	var wg sync.WaitGroup
	var moves atomic.Int64
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go pgPKMover(src.DB, seed+int64(i), span, &stop, &wg, &moves)
	}
	time.Sleep(time.Second)
	var syncState string
	_ = src.DB.QueryRow("SELECT sync_state FROM pg_stat_replication WHERE application_name = 'rw'").Scan(&syncState)
	if syncState != "sync" {
		t.Fatalf("pg_receivewal is not the synchronous standby (sync_state=%q)", syncState)
	}

	cfg := strandCfg{Name: "e2e-pg-syncrep", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s, create_slot: true", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "logical", SourceIOPS: 20000}
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 10*time.Minute)
	time.Sleep(3 * time.Second)

	stop.Store(true)
	wg.Wait()
	wl.Stop()
	t.Logf("workload (synchronous standby pg_receivewal): %d ops, %d client-side errors, %d two-phase txns, %d PK moves",
		wl.ops.Load(), wl.errs.Load(), wl.prepared.Load(), moves.Load())
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariants(t, dst)
}
