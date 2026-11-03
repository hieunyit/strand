package pg_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/reader"
	"strand/internal/reader/pg"
)

// Dự án tắt Ryuk (xem các package test khác); vì vậy mọi container/network tạo ở đây được ghi lại và
// TestMain dọn sạch khi kết thúc, kể cả khi test fail.
func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

var (
	trackMu    sync.Mutex
	containers []testcontainers.Container
	networks   []*testcontainers.DockerNetwork
)

func track(c testcontainers.Container) {
	trackMu.Lock()
	containers = append(containers, c)
	trackMu.Unlock()
}

func trackNet(n *testcontainers.DockerNetwork) {
	trackMu.Lock()
	networks = append(networks, n)
	trackMu.Unlock()
}

func cleanupAll() {
	trackMu.Lock()
	defer trackMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for i := len(containers) - 1; i >= 0; i-- {
		_ = containers[i].Terminate(ctx)
	}
	containers = nil
	for _, n := range networks {
		_ = n.Remove(ctx)
	}
	networks = nil
}

func TestMain(m *testing.M) {
	code := m.Run()
	cleanupAll()
	os.Exit(code)
}

// pgArgs là cấu hình server dùng chung: wal_level=logical, cho phép PREPARE TRANSACTION,
// logical_decoding_work_mem nhỏ để Txn lớn bị stream, wal_sender_timeout=10s cho AC4.
var pgArgs = []string{
	"-c", "wal_level=logical",
	"-c", "max_wal_senders=20",
	"-c", "max_replication_slots=20",
	"-c", "max_prepared_transactions=20",
	"-c", "logical_decoding_work_mem=64kB",
	"-c", "wal_sender_timeout=10s",
}

type pgInst struct {
	c   testcontainers.Container
	dsn string
	db  *sql.DB
}

// startContainer khởi động container theo req, ghi lại để dọn, trả DSN tới cổng 5432.
func startContainer(ctx context.Context, req testcontainers.ContainerRequest) (*pgInst, error) {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if c != nil {
		track(c)
	}
	if err != nil {
		return nil, err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("postgres://strand:dev@%s:%s/strand?sslmode=disable", host, port.Port())
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(60 * time.Second)
	for {
		if err = db.PingContext(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("ping %s: %w", dsn, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	return &pgInst{c: c, dsn: dsn, db: db}, nil
}

// primaryRequest tạo request cho một server PostgreSQL thường (có thể gắn network + alias).
func primaryRequest(image string, args []string, netName, alias string) testcontainers.ContainerRequest {
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "strand",
			"POSTGRES_PASSWORD": "dev",
			"POSTGRES_DB":       "strand",
		},
		Cmd: append([]string{"postgres"}, args...),
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(3 * time.Minute),
	}
	if netName != "" {
		req.Networks = []string{netName}
		req.NetworkAliases = map[string][]string{netName: {alias}}
		// Cho phép kết nối replication vật lý từ standby trong cùng network.
		req.Files = []testcontainers.ContainerFile{{
			Reader:            strings.NewReader("#!/bin/bash\necho 'host replication all all scram-sha-256' >> \"$PGDATA/pg_hba.conf\"\n"),
			ContainerFilePath: "/docker-entrypoint-initdb.d/10-replication.sh",
			FileMode:          0o755,
		}}
	}
	return req
}

var (
	sharedOnce sync.Once
	shared     *pgInst
	sharedErr  error
)

// sharedPG trả server postgres:18 dùng chung cho AC1–AC5.
func sharedPG(t *testing.T) *pgInst {
	t.Helper()
	sharedOnce.Do(func() {
		shared, sharedErr = startContainer(context.Background(), primaryRequest("postgres:18", pgArgs, "", ""))
	})
	if sharedErr != nil {
		t.Fatalf("không khởi động được postgres:18: %v", sharedErr)
	}
	return shared
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// setupTables tạo bảng dữ liệu (ddl), bảng marker và publication chứa cả hai.
func setupTables(t *testing.T, db *sql.DB, prefix, ddl string, tables ...string) string {
	t.Helper()
	mustExec(t, db, ddl)
	mustExec(t, db, fmt.Sprintf("CREATE TABLE %s_marker (id serial PRIMARY KEY, tag text NOT NULL)", prefix))
	pub := prefix + "_pub"
	all := append(append([]string{}, tables...), prefix+"_marker")
	mustExec(t, db, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, strings.Join(all, ", ")))
	return pub
}

// dropSlotLater xóa slot khi test kết thúc (thử lại vì walsender có thể chưa thoát hẳn).
func dropSlotLater(t *testing.T, db *sql.DB, slot string) {
	t.Cleanup(func() {
		for i := 0; i < 100; i++ {
			_, err := db.Exec(`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1`, slot)
			if err == nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
}

func confirmedFlush(t *testing.T, db *sql.DB, slot string) uint64 {
	t.Helper()
	var s sql.NullString
	if err := db.QueryRow(`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&s); err != nil {
		t.Fatalf("đọc confirmed_flush_lsn: %v", err)
	}
	if !s.Valid {
		return 0
	}
	l, err := pglogrepl.ParseLSN(s.String)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(l)
}

// waitFor thăm dò cond tới khi đúng hoặc hết hạn.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("hết thời gian chờ: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// txnReader ghép Event thành Txn bằng Assembler thật và kiểm hợp đồng luồng Event:
// mọi Event trong một Txn mang cùng Pos (commit LSN) và commit LSN tăng dần giữa các Txn.
type txnReader struct {
	t       *testing.T
	ch      <-chan reader.Event
	asm     *assemble.Assembler
	lastLSN uint64
	inTxn   bool
	txnPos  model.Pos
}

func newTxnReader(t *testing.T, ch <-chan reader.Event) *txnReader {
	return &txnReader{t: t, ch: ch, asm: assemble.New(assemble.WithSpillDir(t.TempDir()))}
}

func (tr *txnReader) next(timeout time.Duration) *model.Txn {
	tr.t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-tr.ch:
			if !ok {
				tr.t.Fatalf("kênh Event bị đóng bất ngờ")
			}
			if ev.Type == reader.EventError {
				tr.t.Fatalf("EventError: %v", ev.Err)
			}
			if ev.Pos.Engine != model.EnginePG || ev.Pos.LSN == 0 {
				tr.t.Fatalf("Event %s có Pos không hợp lệ: %+v", ev.Type, ev.Pos)
			}
			if ev.Type == reader.EventQuery && ev.Query == "BEGIN" {
				if tr.inTxn {
					tr.t.Fatalf("BEGIN lồng nhau")
				}
				tr.inTxn, tr.txnPos = true, ev.Pos
			} else if !tr.inTxn {
				tr.t.Fatalf("Event %s nằm ngoài BEGIN/COMMIT", ev.Type)
			} else if ev.Pos != tr.txnPos {
				tr.t.Fatalf("Event %s có Pos %v khác commit LSN của Txn %v", ev.Type, ev.Pos, tr.txnPos)
			}
			txn, err := tr.asm.Push(ev)
			if err != nil {
				tr.t.Fatalf("assembler: %v", err)
			}
			if txn != nil {
				tr.inTxn = false
				if txn.SourcePos.LSN <= tr.lastLSN {
					tr.t.Fatalf("commit LSN không tăng: %d sau %d", txn.SourcePos.LSN, tr.lastLSN)
				}
				tr.lastLSN = txn.SourcePos.LSN
				return txn
			}
		case <-timer.C:
			tr.t.Fatalf("hết %v chờ Txn kế tiếp", timeout)
		}
	}
}

// untilMarker đọc Txn tới khi gặp Txn chứa marker tag; trả các Txn trước marker và Txn marker.
func (tr *txnReader) untilMarker(tag string, timeout time.Duration) ([]*model.Txn, *model.Txn) {
	tr.t.Helper()
	var got []*model.Txn
	for {
		txn := tr.next(timeout)
		if markerTag(tr.t, txn) == tag {
			return got, txn
		}
		got = append(got, txn)
	}
}

func insertMarker(t *testing.T, db *sql.DB, prefix, tag string) {
	t.Helper()
	mustExec(t, db, fmt.Sprintf("INSERT INTO %s_marker (tag) VALUES ($1)", prefix), tag)
}

func markerTag(t *testing.T, txn *model.Txn) string {
	rows := txnRows(t, txn)
	if len(rows) == 1 && strings.HasSuffix(rows[0].Table.Table, "_marker") {
		return col(t, rows[0], "tag").(string)
	}
	return ""
}

func txnRows(t *testing.T, txn *model.Txn) []model.RowChange {
	t.Helper()
	var out []model.RowChange
	if err := txn.ForEachRow(func(rc *model.RowChange) error {
		out = append(out, *rc)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func colValue(t *testing.T, rc model.RowChange, name string) model.ColValue {
	t.Helper()
	for _, c := range rc.Cols {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("không có cột %q trong %+v", name, rc.Cols)
	return model.ColValue{}
}

func col(t *testing.T, rc model.RowChange, name string) model.Value {
	t.Helper()
	return colValue(t, rc, name).Value
}

func newReader(t *testing.T, cfg pg.Config) *pg.Reader {
	t.Helper()
	r, err := pg.New(cfg)
	if err != nil {
		t.Fatalf("pg.New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}
