package promote_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/promote"
)

// =============================================================================
// Hạ tầng test: container postgres:18 dùng chung + database riêng cho từng test
// =============================================================================

// pgServer là một container PostgreSQL; mỗi test tạo database riêng trên đó để không rò trạng thái.
// Fence (ALTER SYSTEM) là cấp cluster nên test nào fence source phải gỡ fence ở t.Cleanup
// (unfenceOnCleanup) hoặc dùng container riêng.
type pgServer struct {
	container testcontainers.Container
	addr      string
	admin     *sql.DB
}

func (s *pgServer) dsn(user, pass, db string) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, s.addr, db)
}

func (s *pgServer) terminate() {
	if s == nil {
		return
	}
	_ = s.admin.Close()
	_ = s.container.Terminate(context.Background())
}

func startPGServer(ctx context.Context) (*pgServer, error) {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:18",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "strand",
				"POSTGRES_PASSWORD": "dev",
				"POSTGRES_DB":       "postgres",
			},
			Cmd: []string{"postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=10", "-c", "max_replication_slots=10"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}
	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	s := &pgServer{container: c, addr: net.JoinHostPort(host, port.Port())}
	s.admin, err = sql.Open("pgx", s.dsn("strand", "dev", "postgres"))
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	for i := 0; i < 60; i++ {
		if err = s.admin.PingContext(ctx); err == nil {
			return s, nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.terminate()
	return nil, fmt.Errorf("postgres not reachable: %w", err)
}

var (
	pgSrcOnce, pgTgtOnce sync.Once
	pgSrc, pgTgt         *pgServer
	pgSrcErr, pgTgtErr   error
	pgDBSeq              atomic.Int64
)

func sharedPG(t *testing.T, once *sync.Once, srv **pgServer, errp *error) *pgServer {
	t.Helper()
	once.Do(func() { *srv, *errp = startPGServer(context.Background()) })
	if *srv == nil {
		t.Fatalf("PostgreSQL container could not be started: %v", *errp)
	}
	return *srv
}

func sourcePG(t *testing.T) *pgServer { return sharedPG(t, &pgSrcOnce, &pgSrc, &pgSrcErr) }
func targetPG(t *testing.T) *pgServer { return sharedPG(t, &pgTgtOnce, &pgTgt, &pgTgtErr) }

func TestMain(m *testing.M) {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	code := m.Run()
	pgSrc.terminate()
	pgTgt.terminate()
	os.Exit(code)
}

// adminExec chạy lệnh quản trị trên một session riêng đã tắt read-only ở mức session, để không bị
// ảnh hưởng bởi fence cấp cluster (kể cả khi SIGHUP gỡ fence chưa tới session đó).
func (s *pgServer) adminExec(ctx context.Context, q string) error {
	conn, err := s.admin.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET default_transaction_read_only = off"); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, q)
	return err
}

// newTestDB tạo một database mới trên server cho test hiện tại, trả về kết nối (user strand) và DSN.
func newTestDB(t *testing.T, s *pgServer) (*sql.DB, string) {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("t%d_%d", time.Now().UnixNano()%1e9, pgDBSeq.Add(1))
	if err := s.adminExec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	dsn := s.dsn("strand", "dev", name)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		if err := s.adminExec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
	})
	return db, dsn
}

// unfenceOnCleanup gỡ fence cấp cluster (ALTER SYSTEM default_transaction_read_only) sau test và chờ
// tới khi session mới thấy default_transaction_read_only = off, để test sau không bị rò trạng thái.
func unfenceOnCleanup(t *testing.T, s *pgServer) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		if err := s.adminExec(ctx, "ALTER SYSTEM RESET default_transaction_read_only"); err != nil {
			t.Errorf("unfence: %v", err)
			return
		}
		if _, err := s.admin.ExecContext(ctx, "SELECT pg_reload_conf()"); err != nil {
			t.Errorf("unfence reload: %v", err)
			return
		}
		probe, err := sql.Open("pgx", s.dsn("strand", "dev", "postgres"))
		if err != nil {
			t.Errorf("unfence probe: %v", err)
			return
		}
		defer probe.Close()
		probe.SetMaxIdleConns(0)
		for i := 0; i < 100; i++ {
			var v string
			if err := probe.QueryRowContext(ctx, "SHOW default_transaction_read_only").Scan(&v); err == nil && v == "off" {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Errorf("source still read-only after unfence")
	})
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// createAppRole tạo role ứng dụng (cấp cluster) với tên duy nhất cho test, xóa ở cleanup.
func createAppRole(t *testing.T, s *pgServer, base string) string {
	t.Helper()
	role := fmt.Sprintf("%s_%d", base, pgDBSeq.Add(1))
	if err := s.adminExec(context.Background(), fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD 'secret'", role)); err != nil {
		t.Fatalf("create role %s: %v", role, err)
	}
	// Role không bị xóa: nó còn quyền trên database của test (bị xóa sau), tên đã duy nhất,
	// và container bị hủy khi hết package.
	return role
}

func openAs(t *testing.T, s *pgServer, dsn, role string) *sql.DB {
	t.Helper()
	appDSN := strings.Replace(dsn, "strand:dev@", role+":secret@", 1)
	db, err := sql.Open("pgx", appDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// simulatePipeline đóng vai pipeline đang chạy trong lúc promote: liên tục xác nhận (vouch) checkpoint
// trên target tới vị trí WAL hiện tại của source, như pipeline thật làm với keepalive khi không còn Txn.
// Nhờ đó bước 3 (ApplyRemaining) xác minh được target đã theo kịp.
func simulatePipeline(t *testing.T, src, tgt *sql.DB, pipelineID string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	store := checkpoint.NewPGStore(tgt, pipelineID, model.EnginePG)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceEpoch(ctx, pipelineID, "test", "hash"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			var lsn string
			if err := src.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err == nil {
				if l, err := checkpoint.ParseLSN(lsn); err == nil {
					_ = store.WriteGlobal(ctx, checkpoint.Global{
						PipelineID: pipelineID, Epoch: store.Epoch(),
						Applied: checkpoint.NewPGAppliedSet(l).String(),
					})
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-done })
}

func seqLastValue(t *testing.T, db *sql.DB, seq string) int64 {
	t.Helper()
	var v int64
	if err := db.QueryRowContext(context.Background(), "SELECT last_value FROM "+seq).Scan(&v); err != nil {
		t.Fatalf("read %s: %v", seq, err)
	}
	return v
}

func findSeq(t *testing.T, seqs []promote.SequenceInfo, name string) promote.SequenceInfo {
	t.Helper()
	for _, s := range seqs {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("sequence %s not discovered (got %+v)", name, seqs)
	return promote.SequenceInfo{}
}

// hookStore bọc StateStore để chạy hook sau mỗi lần lưu tiến trình.
type hookStore struct {
	promote.StateStore
	afterSave func(p *promote.Progress)
}

func (h *hookStore) SaveProgress(ctx context.Context, p *promote.Progress) error {
	if err := h.StateStore.SaveProgress(ctx, p); err != nil {
		return err
	}
	if h.afterSave != nil {
		h.afterSave(p)
	}
	return nil
}

// =============================================================================
// Test không cần DB
// =============================================================================

// Công thức margin theo 02 §8: 2·cache·increment + 2·r_peak·Δt_sync·increment.
func TestCalculateMarginFormula(t *testing.T) {
	cases := []struct {
		cache, inc int64
		r, dt      float64
		want       int64
	}{
		{1000, 1, 500, 2, 2*1000*1 + 2*500*2*1},
		{20, 3, 100, 1.5, 2*20*3 + 2*100*1.5*3},
		{1, 1, 0, 0, 2},
		{10, 2, 0.25, 1, 40 + 1}, // phần r_peak·Δt_sync làm tròn lên
	}
	for _, c := range cases {
		if got := promote.CalculateMargin(c.cache, c.inc, c.r, c.dt); got != c.want {
			t.Errorf("CalculateMargin(%d,%d,%v,%v) = %d, want %d", c.cache, c.inc, c.r, c.dt, got, c.want)
		}
	}
}

// invariant: PR-1 — như MySQL: bước 3 không được thành công khi không có cách xác minh target đã theo kịp.
func TestPGApplyRemainingWithoutSourceRefuses(t *testing.T) {
	d := promote.NewPGTargetDriver(nil, "app").WithPipeline("p")
	if err := d.ApplyRemaining(context.Background()); err == nil {
		t.Fatalf("ApplyRemaining must not succeed without a source connection")
	}
}

// Đồng bộ định kỳ không được ghi đè tiến trình khi promote đã bắt đầu.
func TestSyncIndependentSequencesDoesNotTouchRunningPromote(t *testing.T) {
	ctx := context.Background()
	store := promote.NewMemoryStateStore()
	src := newMockSourceDriver(true)
	src.lastValues["s"] = 777
	seqs := []promote.SequenceInfo{{Name: "s", Type: promote.SequenceTypeIndependent, CacheSize: 1, IncrementBy: 1}}
	if _, err := promote.SyncIndependentSequences(ctx, src, store, "p", seqs); err != nil {
		t.Fatal(err)
	}
	p, _ := store.LoadProgress(ctx, "p")
	if p.Sequences[0].LastSyncedValue != 777 {
		t.Fatalf("LastSyncedValue = %d", p.Sequences[0].LastSyncedValue)
	}
	p.State = promote.StatePromoting
	p.CompletedSteps = []promote.PromoteStep{promote.StepFenceSource}
	_ = store.SaveProgress(ctx, p)
	src.lastValues["s"] = 999
	if _, err := promote.SyncIndependentSequences(ctx, src, store, "p", seqs); err != nil {
		t.Fatal(err)
	}
	p2, _ := store.LoadProgress(ctx, "p")
	if !p2.IsStepCompleted(promote.StepFenceSource) || p2.Sequences[0].LastSyncedValue != 777 {
		t.Fatalf("sync overwrote a running promote: %+v", p2)
	}
}

// =============================================================================
// Test tích hợp postgres:18
// =============================================================================

// DiscoverPGSequences phân loại: sequence độc lập (kể cả khi dùng làm DEFAULT mà không OWNED BY),
// serial (deptype 'a'), identity (deptype 'i'), và sequence ở schema khác public.
func TestDiscoverPGSequences(t *testing.T) {
	ctx := context.Background()
	db, _ := newTestDB(t, targetPG(t))
	mustExec(t, db, `
		CREATE SEQUENCE test_disc_indep CACHE 50 INCREMENT BY 5;
		CREATE TABLE test_disc_default (id BIGINT PRIMARY KEY DEFAULT nextval('test_disc_indep'));
		CREATE TABLE test_disc_serial (id SERIAL PRIMARY KEY, name TEXT);
		CREATE TABLE test_disc_ident (id BIGINT GENERATED ALWAYS AS IDENTITY (CACHE 20 INCREMENT 3) PRIMARY KEY, title TEXT);
		CREATE SCHEMA other;
		CREATE TABLE other.items (id BIGSERIAL PRIMARY KEY);
	`)

	seqs, err := promote.DiscoverPGSequences(ctx, db, []string{"public", "other"})
	if err != nil {
		t.Fatalf("DiscoverPGSequences: %v", err)
	}
	if len(seqs) != 4 {
		t.Fatalf("expected 4 sequences, got %+v", seqs)
	}
	indep := findSeq(t, seqs, "test_disc_indep")
	if indep.Type != promote.SequenceTypeIndependent || indep.CacheSize != 50 || indep.IncrementBy != 5 {
		t.Fatalf("independent sequence: %+v", indep)
	}
	if len(indep.Feeds) != 1 || indep.Feeds[0] != (promote.ColumnRef{Table: "test_disc_default", Column: "id"}) {
		t.Fatalf("independent sequence feeds (DEFAULT nextval): %+v", indep.Feeds)
	}
	serial := findSeq(t, seqs, "test_disc_serial_id_seq")
	if serial.Type != promote.SequenceTypeColumn || serial.Table != "test_disc_serial" || serial.Column != "id" {
		t.Fatalf("serial sequence: %+v", serial)
	}
	ident := findSeq(t, seqs, "test_disc_ident_id_seq")
	if ident.Type != promote.SequenceTypeColumn || ident.Table != "test_disc_ident" || ident.Column != "id" ||
		ident.CacheSize != 20 || ident.IncrementBy != 3 {
		t.Fatalf("identity sequence: %+v", ident)
	}
	other := findSeq(t, seqs, "other.items_id_seq")
	if other.Type != promote.SequenceTypeColumn || other.Table != "other.items" || other.Column != "id" {
		t.Fatalf("other-schema sequence: %+v", other)
	}

	only, err := promote.DiscoverPGSequences(ctx, db, []string{"other"})
	if err != nil || len(only) != 1 || only[0].Name != "other.items_id_seq" {
		t.Fatalf("schema filter: %+v %v", only, err)
	}
}

// decodingPipeline đóng vai pipeline thật hơn simulatePipeline: đọc slot test_decoding trên source và
// chỉ dời lwd tới vị trí của những gì logical decoding thực sự phát (COMMIT, message) — không bao giờ
// tới WAL không thuộc transaction (checkpoint, RUNNING_XACTS) mà pgoutput không gửi.
func decodingPipeline(t *testing.T, src *sql.DB, store *checkpoint.PGStore, pipelineID string) {
	t.Helper()
	slot := fmt.Sprintf("promote_td_%d", pgDBSeq.Add(1))
	mustExec(t, src, "SELECT pg_create_logical_replication_slot($1, 'test_decoding')", slot)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			rows, err := src.QueryContext(ctx, "SELECT lsn::text, data FROM pg_logical_slot_get_changes($1, NULL, NULL)", slot)
			if err == nil {
				var last uint64
				for rows.Next() {
					var lsn, data string
					if rows.Scan(&lsn, &data) != nil {
						continue
					}
					if strings.HasPrefix(data, "COMMIT") || strings.HasPrefix(data, "message:") {
						if l, err := checkpoint.ParseLSN(lsn); err == nil && l > last {
							last = l
						}
					}
				}
				rows.Close()
				if last > 0 {
					_ = store.WriteGlobal(ctx, checkpoint.Global{PipelineID: pipelineID, Epoch: store.Epoch(),
						Applied: checkpoint.NewPGAppliedSet(last).String()})
				}
			}
			select {
			case <-ctx.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_, _ = src.ExecContext(context.Background(), "SELECT pg_drop_replication_slot($1)", slot)
	})
}

// invariant: PR-1 — bước 3 trên target PG chỉ qua khi tập đã apply bao phủ mọi commit của source.
// Hồi quy: lwd của pipeline chỉ tới vị trí commit/message đã giải mã, còn pg_current_wal_lsn() đã vượt
// qua đó (ở đây do CHECKPOINT ghi WAL không thuộc transaction) — so với nó thì bước 3 không bao giờ qua.
func TestPGApplyRemainingWaitsForCatchUp(t *testing.T) {
	ctx := context.Background()
	src, _ := newTestDB(t, sourcePG(t))
	tgt, _ := newTestDB(t, targetPG(t))
	pid := "catchup"
	store := checkpoint.NewPGStore(tgt, pid, model.EnginePG)
	if err := store.EnsureTables(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceEpoch(ctx, pid, "test", "h"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, src, "CREATE TABLE w (id int)")

	// Pipeline chưa chạy: target không theo kịp, bước 3 không được qua.
	d := promote.NewPGTargetDriver(tgt, "app").WithCatchUpCheck(src, pid, 1*time.Second, nil)
	if err := d.ApplyRemaining(ctx); err == nil || !strings.Contains(err.Error(), "still missing") {
		t.Fatalf("lagging target must not pass step 3, got %v", err)
	}

	// Transaction còn mở (đã có xid) trên source có thể commit sau marker: bước 3 phải chờ nó.
	open, err := src.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := open.ExecContext(ctx, "INSERT INTO w VALUES (0)"); err != nil {
		t.Fatal(err)
	}
	if err := d.ApplyRemaining(ctx); err == nil || !strings.Contains(err.Error(), "could commit after the fence") {
		t.Fatalf("open source transaction must block step 3, got %v", err)
	}
	if err := open.Commit(); err != nil {
		t.Fatal(err)
	}

	decodingPipeline(t, src, store, pid)
	mustExec(t, src, "INSERT INTO w VALUES (1)")
	mustExec(t, src, "CHECKPOINT") // WAL sau commit cuối mà logical decoding không bao giờ phát
	d2 := promote.NewPGTargetDriver(tgt, "app").WithCatchUpCheck(src, pid, 20*time.Second, nil)
	if err := d2.ApplyRemaining(ctx); err != nil {
		t.Fatalf("caught-up target must pass step 3: %v", err)
	}
	var n int
	if err := src.QueryRowContext(ctx, "SELECT count(*) FROM w").Scan(&n); err != nil || n != 2 {
		t.Fatalf("source rows = %d (%v), want 2", n, err)
	}
}

// AC1: sequence độc lập + CACHE 1000, giá trị cấp qua nhiều session ở source. Sau promote, sequence
// target ≥ last_value của source + margin, và ứng dụng INSERT ngay sau promote không trùng key.
func TestPR1_AC1_IndependentSequenceCache1000(t *testing.T) {
	ctx := context.Background()
	srcSrv, tgtSrv := sourcePG(t), targetPG(t)
	srcDB, _ := newTestDB(t, srcSrv)
	tgtDB, tgtDSN := newTestDB(t, tgtSrv)
	unfenceOnCleanup(t, srcSrv)

	mustExec(t, srcDB, `CREATE SEQUENCE ac1_global_seq CACHE 1000;
		CREATE TABLE ac1_orders (id BIGINT PRIMARY KEY DEFAULT nextval('ac1_global_seq'), val TEXT)`)
	// Mỗi session giữ một block cache 1000 riêng.
	var maxIssued int64
	for i := 0; i < 3; i++ {
		conn, err := srcDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var id int64
		if err := conn.QueryRowContext(ctx, "INSERT INTO ac1_orders (val) VALUES ('src') RETURNING id").Scan(&id); err != nil {
			t.Fatal(err)
		}
		if id > maxIssued {
			maxIssued = id
		}
		defer conn.Close()
	}
	srcLast := seqLastValue(t, srcDB, "ac1_global_seq")
	t.Logf("source issued up to %d, source last_value = %d", maxIssued, srcLast)

	role := createAppRole(t, tgtSrv, "ac1_app")
	mustExec(t, tgtDB, `CREATE SEQUENCE ac1_global_seq CACHE 1000;
		CREATE TABLE ac1_orders (id BIGINT PRIMARY KEY DEFAULT nextval('ac1_global_seq'), val TEXT)`)
	// Dữ liệu đã replicate từ source sang target.
	rows, err := srcDB.QueryContext(ctx, "SELECT id, val FROM ac1_orders")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		var v string
		_ = rows.Scan(&id, &v)
		mustExec(t, tgtDB, "INSERT INTO ac1_orders VALUES ($1, $2)", id, v)
	}
	rows.Close()
	mustExec(t, tgtDB, fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s; GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", role, role))

	pid := "ac1-pipeline"
	simulatePipeline(t, srcDB, tgtDB, pid)
	seqs, err := promote.DiscoverPGSequences(ctx, tgtDB, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	seq := findSeq(t, seqs, "ac1_global_seq")
	if seq.Type != promote.SequenceTypeIndependent || seq.CacheSize != 1000 {
		t.Fatalf("discovered: %+v", seq)
	}
	cfg := promote.Config{PipelineID: pid, PeakRate: 500, SyncIntervalSec: 2}
	margin := promote.CalculateMargin(1000, 1, 500, 2)

	sourceDriver := promote.NewPGSourceDriver(srcDB)
	targetDriver := promote.NewPGTargetDriver(tgtDB, role).WithPipeline(pid).WithCatchUpCheck(srcDB, pid, 30*time.Second, nil)
	progress, err := promote.NewPromoter(cfg, promote.NewPGStateStore(tgtDB), sourceDriver, targetDriver, seqs, nil).Run(ctx)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	if progress.State != promote.StatePromoted {
		t.Fatalf("state = %s", progress.State)
	}

	got := seqLastValue(t, tgtDB, "ac1_global_seq")
	if got != srcLast+margin {
		t.Fatalf("AC1: target sequence = %d, want source last_value %d + margin %d = %d", got, srcLast, margin, srcLast+margin)
	}

	// Source đã bị fence: không ghi được nữa.
	if _, err := srcDB.ExecContext(ctx, "INSERT INTO ac1_orders (val) VALUES ('after fence')"); err == nil {
		t.Fatalf("source still writable after fence")
	}

	// Ứng dụng ghi ngay sau promote, bằng role của ứng dụng: không trùng key.
	app := openAs(t, tgtSrv, tgtDSN, role)
	for i := 0; i < 5; i++ {
		var id int64
		if err := app.QueryRowContext(ctx, "INSERT INTO ac1_orders (val) VALUES ('post-promote') RETURNING id").Scan(&id); err != nil {
			t.Fatalf("AC1: app insert after promote: %v", err)
		}
		if id <= srcLast {
			t.Fatalf("AC1: new id %d not above source last_value %d", id, srcLast)
		}
	}
}

// AC2: sequence gắn cột (identity) lấy đúng max(cột) + margin.
func TestPR1_AC2_ColumnSequenceSetval(t *testing.T) {
	ctx := context.Background()
	srcSrv, tgtSrv := sourcePG(t), targetPG(t)
	srcDB, _ := newTestDB(t, srcSrv)
	tgtDB, tgtDSN := newTestDB(t, tgtSrv)
	unfenceOnCleanup(t, srcSrv)

	role := createAppRole(t, tgtSrv, "ac2_app")
	mustExec(t, tgtDB, `
		CREATE TABLE ac2_products (id BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 10 INCREMENT 2) PRIMARY KEY, title TEXT);
		INSERT INTO ac2_products (title) VALUES ('item 1'), ('item 2');
		INSERT INTO ac2_products VALUES (150, 'replicated row 150');`)
	mustExec(t, tgtDB, fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s; GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", role, role))

	pid := "ac2-pipeline"
	simulatePipeline(t, srcDB, tgtDB, pid)
	seqs, err := promote.DiscoverPGSequences(ctx, tgtDB, nil)
	if err != nil {
		t.Fatal(err)
	}
	seq := findSeq(t, seqs, "ac2_products_id_seq")
	if seq.Type != promote.SequenceTypeColumn {
		t.Fatalf("discovered: %+v", seq)
	}
	cfg := promote.Config{PipelineID: pid, PeakRate: 200, SyncIntervalSec: 1.5}
	margin := promote.CalculateMargin(10, 2, 200, 1.5)
	want := int64(150) + margin

	targetDriver := promote.NewPGTargetDriver(tgtDB, role).WithPipeline(pid).WithCatchUpCheck(srcDB, pid, 30*time.Second, nil)
	progress, err := promote.NewPromoter(cfg, promote.NewPGStateStore(tgtDB), promote.NewPGSourceDriver(srcDB), targetDriver, seqs, nil).Run(ctx)
	if err != nil || progress.State != promote.StatePromoted {
		t.Fatalf("promote: %v (%+v)", err, progress)
	}
	if got := seqLastValue(t, tgtDB, "ac2_products_id_seq"); got != want {
		t.Fatalf("AC2: setval = %d, want max(col) 150 + margin %d = %d", got, margin, want)
	}

	app := openAs(t, tgtSrv, tgtDSN, role)
	var newID int64
	if err := app.QueryRowContext(ctx, "INSERT INTO ac2_products (title) VALUES ('post') RETURNING id").Scan(&newID); err != nil {
		t.Fatalf("app insert: %v", err)
	}
	if newID != want+2 {
		t.Fatalf("new id = %d, want %d", newID, want+2)
	}
}

// AC3: source mất kết nối SAU khi fence: promote trả exit 10 promote_needs_decision kèm giá trị gợi ý
// (lần đồng bộ cuối + margin), target vẫn chặn ghi; chạy lại với --override-sequences thì dùng đúng giá trị.
// Source là container riêng và bị docker stop thật ngay sau bước fence.
func TestPR1_AC3_SourceUnreachableAfterFence_Exit10(t *testing.T) {
	ctx := context.Background()
	src, err := startPGServer(ctx)
	if err != nil {
		t.Fatalf("start source: %v", err)
	}
	t.Cleanup(src.terminate)
	srcDB, err := sql.Open("pgx", src.dsn("strand", "dev", "postgres"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srcDB.Close() })

	tgtSrv := targetPG(t)
	tgtDB, tgtDSN := newTestDB(t, tgtSrv)
	role := createAppRole(t, tgtSrv, "ac3_app")
	mustExec(t, srcDB, "CREATE SEQUENCE ac3_seq CACHE 20; SELECT setval('ac3_seq', 10000, true)")
	mustExec(t, tgtDB, "CREATE SEQUENCE ac3_seq CACHE 20; CREATE TABLE ac3_t (id BIGINT PRIMARY KEY DEFAULT nextval('ac3_seq'))")
	mustExec(t, tgtDB, fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s; GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s", role, role))

	pid := "ac3-pipeline"
	simulatePipeline(t, srcDB, tgtDB, pid)
	seqs, err := promote.DiscoverPGSequences(ctx, tgtDB, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	pgStore := promote.NewPGStateStore(tgtDB)
	sourceDriver := promote.NewPGSourceDriver(srcDB)

	// Lần đồng bộ định kỳ cuối cùng trước sự cố ghi nhận last_value = 10000.
	if _, err := promote.SyncIndependentSequences(ctx, sourceDriver, pgStore, pid, seqs); err != nil {
		t.Fatal(err)
	}
	// Sau lần đồng bộ đó source còn cấp thêm (giá trị Strand không thấy).
	mustExec(t, srcDB, "SELECT setval('ac3_seq', 10150, true)")

	stopped := false
	store := &hookStore{StateStore: pgStore, afterSave: func(p *promote.Progress) {
		if !stopped && p.IsStepCompleted(promote.StepFenceSource) {
			stopped = true
			timeout := 2 * time.Second
			if err := src.container.Stop(ctx, &timeout); err != nil {
				t.Errorf("stop source: %v", err)
			}
		}
	}}

	cfg := promote.Config{PipelineID: pid, PeakRate: 100, SyncIntervalSec: 5}
	margin := promote.CalculateMargin(20, 1, 100, 5)
	wantSuggested := int64(10000) + margin
	targetDriver := promote.NewPGTargetDriver(tgtDB, role).WithPipeline(pid).WithCatchUpCheck(srcDB, pid, 30*time.Second, nil)

	progress, err := promote.NewPromoter(cfg, store, sourceDriver, targetDriver, seqs, nil).Run(ctx)
	if !stopped {
		t.Fatalf("source was never stopped after fence (err=%v)", err)
	}
	var exitErr *exitcode.Error
	if !errors.As(err, &exitErr) || exitErr.ExitCode != exitcode.NeedsDecision || exitErr.Code != exitcode.ErrPromoteNeedsDecision {
		t.Fatalf("AC3: want exit 10 promote_needs_decision, got %v", err)
	}
	if progress.State != promote.StateAwaitingOperator || progress.CurrentStep != promote.StepSetval {
		t.Fatalf("AC3: state=%s step=%s", progress.State, progress.CurrentStep)
	}
	if got := progress.SuggestedSequences["ac3_seq"]; got != wantSuggested {
		t.Fatalf("AC3: suggested = %d, want last sync 10000 + margin %d = %d", got, margin, wantSuggested)
	}
	// Trạng thái chờ quyết định đã lưu vào _strand_promote.
	saved, err := pgStore.LoadProgress(ctx, pid)
	if err != nil || saved.State != promote.StateAwaitingOperator || saved.SuggestedSequences["ac3_seq"] != wantSuggested {
		t.Fatalf("AC3: persisted progress %+v %v", saved, err)
	}
	// Target vẫn chặn ghi của ứng dụng.
	app := openAs(t, tgtSrv, tgtDSN, role)
	if _, err := app.ExecContext(ctx, "INSERT INTO ac3_t DEFAULT VALUES"); err == nil {
		t.Fatalf("AC3: app could write while promote awaits a decision")
	}
	if got := seqLastValue(t, tgtDB, "ac3_seq"); got >= wantSuggested {
		t.Fatalf("AC3: sequence changed before a decision: %d", got)
	}

	// Người vận hành nhập giá trị cao hơn gợi ý rồi chạy lại: tiếp tục từ bước setval.
	override := wantSuggested + 1000
	cfg.SequenceOverrides = map[string]int64{"ac3_seq": override}
	progress, err = promote.NewPromoter(cfg, pgStore, sourceDriver, targetDriver, seqs, nil).Run(ctx)
	if err != nil || progress.State != promote.StatePromoted {
		t.Fatalf("AC3: re-run with override: %v (%+v)", err, progress)
	}
	if got := seqLastValue(t, tgtDB, "ac3_seq"); got != override {
		t.Fatalf("AC3: sequence = %d, want override %d", got, override)
	}
	var id int64
	app = openAs(t, tgtSrv, tgtDSN, role) // session cũ bị bước 5 ngắt để kết nối lại với mặc định read-write
	if err := app.QueryRowContext(ctx, "INSERT INTO ac3_t DEFAULT VALUES RETURNING id").Scan(&id); err != nil || id != override+1 {
		t.Fatalf("AC3: app insert after promote: id=%d err=%v", id, err)
	}
}

// failingSetval làm setval của một sequence lỗi một lần để mô phỏng promote bị ngắt giữa bước 4.
type failingSetval struct {
	*promote.PGTargetDriver
	failOn string
	failed bool
}

func (f *failingSetval) Setval(ctx context.Context, seq promote.SequenceInfo, val int64) error {
	if seq.Name == f.failOn && !f.failed {
		f.failed = true
		return errors.New("simulated crash during setval")
	}
	return f.PGTargetDriver.Setval(ctx, seq, val)
}

// AC4: promote bị ngắt giữa bước setval; chạy lại (source đã mất kết nối) làm tiếp từ bước đó,
// dùng giá trị đã chốt và lưu ở lần trước, không hỏi lại người vận hành và không setval lùi.
func TestPR1_AC4_ResumeInterruptedPromote_NoBackwardSetval(t *testing.T) {
	ctx := context.Background()
	srcSrv, tgtSrv := sourcePG(t), targetPG(t)
	srcDB, _ := newTestDB(t, srcSrv)
	tgtDB, _ := newTestDB(t, tgtSrv)
	unfenceOnCleanup(t, srcSrv)
	role := createAppRole(t, tgtSrv, "ac4_app")

	mustExec(t, srcDB, `CREATE SEQUENCE ac4_a CACHE 10; SELECT setval('ac4_a', 5000, true);
		CREATE SEQUENCE ac4_b CACHE 10; SELECT setval('ac4_b', 7000, true);`)
	mustExec(t, tgtDB, `CREATE SEQUENCE ac4_a CACHE 10;
		CREATE SEQUENCE ac4_b CACHE 10; SELECT setval('ac4_b', 1000000, true);
		CREATE TABLE ac4_t (id BIGSERIAL PRIMARY KEY); INSERT INTO ac4_t VALUES (300);`)

	pid := "ac4-pipeline"
	simulatePipeline(t, srcDB, tgtDB, pid)
	seqs, err := promote.DiscoverPGSequences(ctx, tgtDB, []string{"public"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := promote.Config{PipelineID: pid, PeakRate: 100, SyncIntervalSec: 2}
	store := promote.NewPGStateStore(tgtDB)
	sourceDriver := promote.NewPGSourceDriver(srcDB)
	pgTarget := promote.NewPGTargetDriver(tgtDB, role).WithPipeline(pid).WithCatchUpCheck(srcDB, pid, 30*time.Second, nil)
	m10 := promote.CalculateMargin(10, 1, 100, 2)
	m1 := promote.CalculateMargin(1, 1, 100, 2)

	// Lần 1: bị ngắt khi setval ac4_b (ac4_a đã setval xong).
	target := &failingSetval{PGTargetDriver: pgTarget, failOn: "ac4_b"}
	if _, err := promote.NewPromoter(cfg, store, sourceDriver, target, seqs, nil).Run(ctx); err == nil {
		t.Fatalf("expected the simulated crash")
	}
	saved, err := store.LoadProgress(ctx, pid)
	if err != nil || saved.CurrentStep != promote.StepSetval || saved.IsStepCompleted(promote.StepSetval) {
		t.Fatalf("progress after crash: %+v %v", saved, err)
	}
	if got := seqLastValue(t, tgtDB, "ac4_a"); got != 5000+m10 {
		t.Fatalf("ac4_a after run 1 = %d, want %d", got, 5000+m10)
	}

	// Sau đó source mất kết nối, và ai đó lỡ đặt sequence target cao hơn kế hoạch.
	sourceDriver.SetReachable(false)
	mustExec(t, tgtDB, "SELECT setval('ac4_a', 9000, true)")

	// Lần 2: chạy lại đúng lệnh (không override): tiếp tục, không exit 10, không lùi.
	progress, err := promote.NewPromoter(cfg, store, sourceDriver, target, seqs, nil).Run(ctx)
	if err != nil || progress.State != promote.StatePromoted {
		t.Fatalf("AC4: resume: %v (%+v)", err, progress)
	}
	if got := seqLastValue(t, tgtDB, "ac4_a"); got != 9000 {
		t.Fatalf("AC4: ac4_a moved backwards/changed: %d, want 9000", got)
	}
	if got := seqLastValue(t, tgtDB, "ac4_b"); got != 1000000 {
		t.Fatalf("AC4: ac4_b moved backwards: %d, want 1000000", got)
	}
	if got := seqLastValue(t, tgtDB, "ac4_t_id_seq"); got != 300+m1 {
		t.Fatalf("AC4: column sequence = %d, want %d", got, 300+m1)
	}
	for _, s := range progress.Sequences {
		if s.Name == "ac4_b" && (s.FinalValue != 7000+m10 || !s.Decided) {
			t.Fatalf("AC4: ac4_b decided value lost: %+v", s)
		}
	}
}

// Đồng bộ định kỳ last_value của sequence độc lập vào _strand_promote, không bao giờ giảm.
func TestSyncIndependentSequences_PeriodicSync(t *testing.T) {
	ctx := context.Background()
	srcDB, _ := newTestDB(t, sourcePG(t))
	tgtDB, _ := newTestDB(t, targetPG(t))
	mustExec(t, srcDB, "CREATE SEQUENCE sync_api_seq CACHE 1; SELECT setval('sync_api_seq', 100, true)")

	sourceDriver := promote.NewPGSourceDriver(srcDB)
	store := promote.NewPGStateStore(tgtDB)
	pid := "sync-api-pipe"
	seqs := []promote.SequenceInfo{
		{Name: "sync_api_seq", Type: promote.SequenceTypeIndependent, CacheSize: 1, IncrementBy: 1},
		{Name: "col_seq", Type: promote.SequenceTypeColumn, Table: "t", Column: "id", CacheSize: 1, IncrementBy: 1},
	}
	lastSynced := func() int64 {
		p, err := store.LoadProgress(ctx, pid)
		if err != nil || p == nil {
			t.Fatalf("load progress: %v", err)
		}
		for _, s := range p.Sequences {
			if s.Name == "sync_api_seq" {
				return s.LastSyncedValue
			}
		}
		t.Fatalf("sequence missing from progress")
		return 0
	}

	if _, err := promote.SyncIndependentSequences(ctx, sourceDriver, store, pid, seqs); err != nil {
		t.Fatal(err)
	}
	if got := lastSynced(); got != 100 {
		t.Fatalf("LastSyncedValue = %d, want 100", got)
	}
	mustExec(t, srcDB, "SELECT setval('sync_api_seq', 350, true)")
	if _, err := promote.SyncIndependentSequences(ctx, sourceDriver, store, pid, seqs); err != nil {
		t.Fatal(err)
	}
	if got := lastSynced(); got != 350 {
		t.Fatalf("LastSyncedValue = %d, want 350", got)
	}
	mustExec(t, srcDB, "SELECT setval('sync_api_seq', 50, true)")
	if _, err := promote.SyncIndependentSequences(ctx, sourceDriver, store, pid, seqs); err != nil {
		t.Fatal(err)
	}
	if got := lastSynced(); got != 350 {
		t.Fatalf("LastSyncedValue went backwards: %d", got)
	}

	// Vòng lặp định kỳ thấy giá trị mới và dừng sạch.
	mustExec(t, srcDB, "SELECT setval('sync_api_seq', 900, true)")
	stop, done := promote.StartPeriodicSequenceSync(ctx, sourceDriver, store, pid, seqs, 50*time.Millisecond, nil)
	deadline := time.Now().Add(5 * time.Second)
	for lastSynced() != 900 {
		if time.Now().After(deadline) {
			t.Fatalf("periodic sync did not pick up 900")
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("periodic sync goroutine did not stop")
	}
}

// Bước 2 chặn ghi của ứng dụng (cả session đang mở, cả khi ứng dụng tự tắt read-only, cả nextval)
// nhưng role của Strand vẫn ghi và setval được; bước 5 cấp lại đủ quyền ghi trên bảng và sequence,
// kể cả quyền ứng dụng có từ trước (TRUNCATE, UPDATE trên sequence), và không cấp quyền trên _strand_*.
func TestPGDriver_HoldReadOnlyAndOpenWrites(t *testing.T) {
	ctx := context.Background()
	tgtSrv := targetPG(t)
	tgtDB, tgtDSN := newTestDB(t, tgtSrv)
	role := createAppRole(t, tgtSrv, "ro_app")
	strandRole := createAppRole(t, tgtSrv, "strand_apply")

	mustExec(t, tgtDB, fmt.Sprintf(`
		CREATE TABLE ro_t (id SERIAL PRIMARY KEY, info TEXT);
		CREATE TABLE _strand_checkpoint_like (x int);
		CREATE SEQUENCE ro_indep;
		GRANT USAGE ON SCHEMA public TO %[1]s;
		GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ro_t TO %[1]s;
		GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO %[1]s;
		GRANT USAGE ON SCHEMA public TO %[2]s;
		GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %[2]s;
		GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO %[2]s;`, role, strandRole))

	app := openAs(t, tgtSrv, tgtDSN, role)
	app.SetMaxOpenConns(1)
	app.SetMaxIdleConns(1)
	// Session của ứng dụng mở từ trước bước 2.
	if _, err := app.ExecContext(ctx, "INSERT INTO ro_t (info) VALUES ('before hold')"); err != nil {
		t.Fatalf("app write before hold: %v", err)
	}

	d := promote.NewPGTargetDriver(tgtDB, role).WithSchemas("public").WithPipeline("ro-pipe")
	if ro, err := d.IsReadOnly(ctx); err != nil || ro {
		t.Fatalf("IsReadOnly before hold = %v, %v", ro, err)
	}
	if err := d.HoldReadOnly(ctx); err != nil {
		t.Fatalf("HoldReadOnly: %v", err)
	}
	if err := d.HoldReadOnly(ctx); err != nil { // idempotent khi chạy lại
		t.Fatalf("HoldReadOnly (again): %v", err)
	}
	if ro, err := d.IsReadOnly(ctx); err != nil || !ro {
		t.Fatalf("IsReadOnly after hold = %v, %v", ro, err)
	}

	for _, q := range []string{
		"INSERT INTO ro_t (info) VALUES ('during hold')",
		"SELECT nextval('ro_indep')",
		"SELECT setval('ro_indep', 1, true)",
		"TRUNCATE ro_t",
	} {
		if _, err := app.ExecContext(ctx, q); err == nil {
			t.Fatalf("existing app session could run %q during hold", q)
		}
	}
	app2 := openAs(t, tgtSrv, tgtDSN, role)
	conn, err := app2.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ro string
	if err := conn.QueryRowContext(ctx, "SHOW default_transaction_read_only").Scan(&ro); err != nil || ro != "on" {
		t.Fatalf("new app session default_transaction_read_only = %q (%v)", ro, err)
	}
	var holdPID int
	if err := conn.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&holdPID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SET default_transaction_read_only = off"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO ro_t (info) VALUES ('sneaky')"); err == nil {
		t.Fatalf("app bypassed hold with SET default_transaction_read_only = off")
	}
	_ = conn.Close()

	// Role của Strand (không phải superuser) vẫn apply và setval được trong lúc giữ.
	strandDB := openAs(t, tgtSrv, tgtDSN, strandRole)
	if _, err := strandDB.ExecContext(ctx, "INSERT INTO ro_t (id, info) VALUES (100, 'applied by strand')"); err != nil {
		t.Fatalf("strand apply during hold: %v", err)
	}
	if _, err := strandDB.ExecContext(ctx, "SELECT setval('ro_t_id_seq', 200, true)"); err != nil {
		t.Fatalf("strand setval during hold: %v", err)
	}

	if err := d.OpenWrites(ctx); err != nil {
		t.Fatalf("OpenWrites: %v", err)
	}
	if err := d.OpenWrites(ctx); err != nil { // idempotent
		t.Fatalf("OpenWrites (again): %v", err)
	}
	if ro, err := d.IsReadOnly(ctx); err != nil || ro {
		t.Fatalf("IsReadOnly after open = %v, %v", ro, err)
	}

	app3 := openAs(t, tgtSrv, tgtDSN, role)
	var id int64
	if err := app3.QueryRowContext(ctx, "INSERT INTO ro_t (info) VALUES ('after open') RETURNING id").Scan(&id); err != nil || id != 201 {
		t.Fatalf("app insert after open: id=%d err=%v", id, err)
	}
	if _, err := app3.ExecContext(ctx, "SELECT nextval('ro_indep')"); err != nil {
		t.Fatalf("app nextval after open: %v", err)
	}
	// Session ứng dụng mở trong lúc giữ (mặc định read-only) đã bị ngắt ở bước 5.
	var still bool
	if err := tgtDB.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", holdPID).Scan(&still); err != nil || still {
		t.Fatalf("read-only app session opened during hold survived OpenWrites (%v)", err)
	}
	var truncate, seqUpdate, strandTbl bool
	if err := tgtDB.QueryRowContext(ctx, `SELECT has_table_privilege($1, 'ro_t', 'TRUNCATE'),
		has_sequence_privilege($1, 'ro_indep', 'UPDATE'),
		has_table_privilege($1, '_strand_checkpoint_like', 'INSERT')`, role).Scan(&truncate, &seqUpdate, &strandTbl); err != nil {
		t.Fatal(err)
	}
	if !truncate || !seqUpdate {
		t.Fatalf("pre-existing privileges not restored: TRUNCATE=%v sequence UPDATE=%v", truncate, seqUpdate)
	}
	if strandTbl {
		t.Fatalf("app role must not get write access to _strand_* tables")
	}
}

// Bước 2 phải từ chối khi role ứng dụng vẫn ghi được qua PUBLIC: không được báo "đã chặn" sai.
func TestPGDriver_HoldReadOnlyDetectsPublicGrant(t *testing.T) {
	ctx := context.Background()
	tgtSrv := targetPG(t)
	tgtDB, _ := newTestDB(t, tgtSrv)
	role := createAppRole(t, tgtSrv, "pub_app")
	mustExec(t, tgtDB, "CREATE TABLE pub_t (id int); GRANT INSERT ON pub_t TO PUBLIC")
	err := promote.NewPGTargetDriver(tgtDB, role).HoldReadOnly(ctx)
	if err == nil || !strings.Contains(err.Error(), "pub_t") {
		t.Fatalf("HoldReadOnly must fail when the app can still write via PUBLIC, got %v", err)
	}
	if err := promote.NewPGTargetDriver(tgtDB).HoldReadOnly(ctx); err == nil {
		t.Fatalf("HoldReadOnly without an app role must fail")
	}
}
