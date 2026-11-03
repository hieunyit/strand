package seed_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/reader"
	"strand/internal/reader/pg"
	"strand/internal/seed"
	"strand/schema"
)

// Ryuk tắt trong dự án: mọi container tạo ở đây được ghi lại và TestMain dọn khi kết thúc.
func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

var (
	trackMu    sync.Mutex
	containers []testcontainers.Container
)

func TestMain(m *testing.M) {
	code := m.Run()
	trackMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	for _, c := range containers {
		_ = c.Terminate(ctx)
	}
	cancel()
	trackMu.Unlock()
	os.Exit(code)
}

// startPG18 khởi động postgres:18 với wal_level=logical (container được TestMain dọn).
func startPG18(t *testing.T) (testcontainers.Container, string, string) {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:18",
			ExposedPorts: []string{"5432/tcp"},
			Env:          map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "strand"},
			Cmd:          []string{"postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"},
			WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if c != nil {
		trackMu.Lock()
		containers = append(containers, c)
		trackMu.Unlock()
	}
	if err != nil {
		t.Fatalf("start postgres:18: %v", err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatal(err)
	}
	hostPort := fmt.Sprintf("%s:%s", host, port.Port())
	return c, hostPort, fmt.Sprintf("postgres://strand:dev@%s/strand?sslmode=disable", hostPort)
}

// pgDumpWrapper tạo script thay cho pg_dump: chạy pg_dump trong container (host không có pg_dump),
// đổi host:port trong --dbname thành localhost:5432 và chuyển -f <file> thành redirect stdout ra file host.
func pgDumpWrapper(t *testing.T, containerID, hostPort string) string {
	t.Helper()
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
`, hostPort, containerID, containerID)
	p := filepath.Join(t.TempDir(), "pg_dump")
	if err := os.WriteFile(p, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func asInt(v any) int64 {
	n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
	return n
}

// readInserts đọc reader tới khi thấy đủ want id (bảng orders), trả id → commit LSN cho từng bảng,
// và commit LSN nhỏ nhất đã phát.
func readInserts(t *testing.T, ctx context.Context, events <-chan reader.Event, want int) (map[string]map[int64]uint64, uint64) {
	t.Helper()
	got := map[string]map[int64]uint64{"orders": {}, "users": {}}
	var minLSN uint64
	deadline := time.After(60 * time.Second)
	for len(got["orders"]) < want || len(got["users"]) < want {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("event channel closed after %d/%d rows", len(got["orders"]), want)
			}
			if ev.Type == reader.EventError {
				t.Fatalf("reader error: %v", ev.Err)
			}
			if ev.Type != reader.EventRows || ev.Op != model.OpInsert {
				continue
			}
			if minLSN == 0 || ev.Pos.LSN < minLSN {
				minLSN = ev.Pos.LSN
			}
			m, ok := got[ev.Table]
			if !ok {
				continue
			}
			for _, row := range ev.Rows {
				id := asInt(row[0])
				if _, dup := m[id]; dup {
					t.Fatalf("%s id %d streamed twice", ev.Table, id)
				}
				m[id] = ev.Pos.LSN
			}
		case <-deadline:
			t.Fatalf("timeout: streamed %d orders / %d users, want %d", len(got["orders"]), len(got["users"]), want)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	return got, minLSN
}

// TestSD1_Export_ConsistentPointIntegration (AC P2-05): `strand export` 2 bảng trong khi có ghi đồng thời,
// restore dump vào target, rồi chứng minh dump cắt đúng tại P = consistent_point của slot:
//   - slot "witness" tạo trước mọi ghi cho commit LSN của từng Txn;
//   - Txn có commit LSN < P nằm trong dump, Txn có commit LSN ≥ P không nằm trong dump;
//   - đọc slot export từ P bằng reader/pg: phát đúng và chỉ đúng các Txn không có trong dump
//     (không trùng, không hổng); bộ đếm cập nhật trong mọi Txn khớp số Txn trước P.
func TestSD1_Export_ConsistentPointIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c, hostPort, dsn := startPG18(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE public.orders (id bigint PRIMARY KEY, v bigint NOT NULL);
		CREATE TABLE public.users  (id bigint PRIMARY KEY, name text NOT NULL);
		CREATE TABLE public.other  (id bigint PRIMARY KEY);
		CREATE TABLE public.counter (k int PRIMARY KEY, n bigint NOT NULL);
		INSERT INTO public.counter VALUES (1, 0);
		CREATE PUBLICATION strand_pub FOR TABLE public.orders, public.users, public.counter;`); err != nil {
		t.Fatal(err)
	}

	// Slot witness: tạo trước mọi ghi; mặc định SNAPSHOT 'nothing' (không export).
	witness, err := pg.New(pg.Config{DSN: dsn, Slot: "witness", Publication: "strand_pub", CreateSlot: true})
	if err != nil {
		t.Fatal(err)
	}
	defer witness.Close()
	if witness.SnapshotName() != "" {
		t.Fatalf("default reader must not export a snapshot, got %q", witness.SnapshotName())
	}

	// Ghi đồng thời: 3 writer, mỗi Txn chèn cùng id vào orders + users và tăng counter.
	var (
		mu        sync.Mutex
		committed = map[int64]bool{}
		nextID    int64
		stop      = make(chan struct{})
		wg        sync.WaitGroup
	)
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				nextID++
				id := nextID
				mu.Unlock()
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					continue
				}
				_, e1 := tx.ExecContext(ctx, "INSERT INTO public.orders VALUES ($1, $2)", id, id*10)
				_, e2 := tx.ExecContext(ctx, "INSERT INTO public.users VALUES ($1, $2)", id, fmt.Sprintf("u%d", id))
				_, e3 := tx.ExecContext(ctx, "UPDATE public.counter SET n = n + 1 WHERE k = 1")
				if e1 != nil || e2 != nil || e3 != nil {
					_ = tx.Rollback()
					continue
				}
				if tx.Commit() == nil {
					mu.Lock()
					committed[id] = true
					mu.Unlock()
				}
			}
		}()
	}
	time.Sleep(300 * time.Millisecond)

	work := t.TempDir()
	dumpFile := filepath.Join(work, "export.dump")
	manifestPath := filepath.Join(work, "seed.json")
	res, err := seed.RunExport(ctx, seed.ExportConfig{
		SourceDSN:   dsn,
		Tables:      []string{"public.orders", "public.users"},
		Out:         dumpFile,
		SlotName:    "strand_orders",
		Format:      "c",
		PgDumpPath:  pgDumpWrapper(t, c.GetContainerID(), hostPort),
		ManifestOut: manifestPath,
	})
	time.Sleep(300 * time.Millisecond) // tiếp tục ghi sau P
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatalf("RunExport: %v", err)
	}
	P := res.ConsistentPoint
	mu.Lock()
	total := len(committed)
	mu.Unlock()
	t.Logf("P = %s, snapshot %s, %d Txn committed", checkpoint.FormatLSN(P), res.SnapshotName, total)

	// seed.json: start_position.lsn = P dạng X/Y, slot, systemid thật, khớp schema.
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var m seed.SeedManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	var sysID string
	if err := db.QueryRowContext(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&sysID); err != nil {
		t.Fatal(err)
	}
	if m.StartPosition.LSN != checkpoint.FormatLSN(P) || !strings.Contains(m.StartPosition.LSN, "/") ||
		m.StartPosition.Slot != "strand_orders" || m.Kind != "strand-export" || m.Engine != "pg" {
		t.Fatalf("manifest start_position/kind = %+v %s %s", m.StartPosition, m.Kind, m.Engine)
	}
	if m.SourceIdentity != sysID {
		t.Fatalf("source_identity = %q, want system_identifier %q", m.SourceIdentity, sysID)
	}
	schemaBytes, _ := os.ReadFile("../../schema/seed.schema.json")
	val, err := schema.NewSeedSchemaValidator(schemaBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := val.Validate(raw); err != nil {
		t.Fatalf("seed.json schema: %v", err)
	}
	// Snapshot đã được giải phóng: slot còn nhưng không còn walsender giữ nó.
	for i := 0; ; i++ {
		var active bool
		if err := db.QueryRowContext(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name = 'strand_orders'").Scan(&active); err != nil {
			t.Fatalf("export slot must remain: %v", err)
		}
		if !active {
			break
		}
		if i > 50 {
			t.Fatal("export slot still held by a walsender after RunExport returned")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Restore dump vào database target.
	if _, err := db.ExecContext(ctx, "CREATE DATABASE seedtarget"); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(dumpFile)
	if err != nil {
		t.Fatal(err)
	}
	restore := exec.CommandContext(ctx, "docker", "exec", "-i", c.GetContainerID(), "pg_restore", "-U", "strand", "-d", "seedtarget")
	restore.Stdin = f
	if out, err := restore.CombinedOutput(); err != nil {
		t.Fatalf("pg_restore: %v\n%s", err, out)
	}
	_ = f.Close()
	tdb, err := sql.Open("pgx", strings.Replace(dsn, "/strand?", "/seedtarget?", 1))
	if err != nil {
		t.Fatal(err)
	}
	defer tdb.Close()
	inDump := map[string]map[int64]bool{"orders": {}, "users": {}}
	for tbl := range inDump {
		rows, err := tdb.QueryContext(ctx, "SELECT id FROM public."+tbl)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			inDump[tbl][id] = true
		}
		rows.Close()
	}
	var otherExists bool
	_ = tdb.QueryRowContext(ctx, "SELECT to_regclass('public.other') IS NOT NULL").Scan(&otherExists)
	if otherExists {
		t.Fatal("only the chosen tables may be dumped")
	}

	// Commit LSN của mọi Txn theo slot witness.
	wev, err := witness.Start(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	commitLSN, _ := readInserts(t, ctx, wev, total)
	before, atP := 0, 0
	for tbl, ids := range commitLSN {
		for id, lsn := range ids {
			if !committed[id] {
				t.Fatalf("%s id %d streamed but not committed by the writer", tbl, id)
			}
			if lsn != commitLSN["orders"][id] {
				t.Fatalf("id %d: orders/users in different Txn", id)
			}
			// Biên của snapshot export: Txn commit trước P có trong dump, từ P trở đi thì không.
			if want := lsn < P; inDump[tbl][id] != want {
				t.Fatalf("%s id %d: commit LSN %s, P %s, in dump = %v", tbl, id,
					checkpoint.FormatLSN(lsn), checkpoint.FormatLSN(P), inDump[tbl][id])
			}
			if tbl == "orders" {
				if lsn < P {
					before++
				}
				if lsn == P {
					atP++
				}
			}
		}
		for id := range inDump[tbl] {
			if _, ok := ids[id]; !ok {
				t.Fatalf("%s id %d in dump but never committed", tbl, id)
			}
		}
	}
	if before == 0 || before == total {
		t.Fatalf("writes must straddle P: %d of %d Txn before P", before, total)
	}
	t.Logf("%d Txn before P (in dump), %d after; %d with commit LSN == P", before, total-before, atP)

	// Đọc slot export từ P bằng reader/pg (applied set rỗng → bắt đầu tại confirmed_flush = P).
	er, err := pg.New(pg.Config{DSN: dsn, Slot: res.SlotName, Publication: "strand_pub"})
	if err != nil {
		t.Fatal(err)
	}
	defer er.Close()
	eev, err := er.Start(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	streamed, minLSN := readInserts(t, ctx, eev, total-before)
	if minLSN < P {
		t.Fatalf("slot streamed a Txn with commit LSN %s < P %s", checkpoint.FormatLSN(minLSN), checkpoint.FormatLSN(P))
	}
	for tbl, ids := range streamed {
		for id, lsn := range ids {
			if inDump[tbl][id] {
				t.Fatalf("duplicate: %s id %d (commit %s) is in the dump and streamed from P", tbl, id, checkpoint.FormatLSN(lsn))
			}
		}
		for id := range committed {
			if !inDump[tbl][id] {
				if _, ok := ids[id]; !ok {
					t.Fatalf("gap: %s id %d is neither in the dump nor streamed from P", tbl, id)
				}
			}
		}
	}
	if len(inDump["orders"])+len(streamed["orders"]) != total {
		t.Fatalf("dump %d + stream %d != committed %d", len(inDump["orders"]), len(streamed["orders"]), total)
	}

	// Bộ đếm trên source (không có trong dump) = tổng Txn; dump chứa đúng `before` hàng.
	var n int64
	if err := db.QueryRowContext(ctx, "SELECT n FROM public.counter WHERE k = 1").Scan(&n); err != nil || n != int64(total) {
		t.Fatalf("counter = %d (%v), want %d", n, err, total)
	}
}

// TestSD1_Export_FailureDropsSlot: pg_dump lỗi → RunExport trả lỗi và không để lại slot giữ WAL;
// ExportSnapshot trên slot đã có bị từ chối.
func TestSD1_Export_FailureDropsSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, hostPort, dsn := startPG18(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = seed.RunExport(ctx, seed.ExportConfig{
		SourceDSN:   dsn,
		Tables:      []string{"public.no_such_table"},
		Out:         filepath.Join(t.TempDir(), "x.dump"),
		SlotName:    "failing_export",
		Format:      "c",
		PgDumpPath:  pgDumpWrapper(t, c.GetContainerID(), hostPort),
		ManifestOut: filepath.Join(t.TempDir(), "seed.json"),
	})
	if err == nil {
		t.Fatal("expected pg_dump failure")
	}
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'failing_export'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("slot left behind after failed export: n=%d err=%v", n, err)
	}

	if _, err := db.ExecContext(ctx, "SELECT pg_create_logical_replication_slot('existing', 'pgoutput')"); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.New(pg.Config{DSN: dsn, Slot: "existing", CreateSlot: true, ExportSnapshot: true}); err == nil {
		t.Fatal("ExportSnapshot on an existing slot must be rejected")
	}
}
