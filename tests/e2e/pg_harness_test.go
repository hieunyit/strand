package e2e

// Harness e2e Phase 2 cho PostgreSQL: container postgres:18 (ghi lại để TestMain dọn), bộ sinh
// tải PG (TOAST, unique hoán đổi trong một Txn, đổi PK, Txn lớn bị stream, PREPARE TRANSACTION),
// trigger trên target đếm số lần một phiên bản row được ghi (phát hiện apply trùng), so sánh
// cuối từng row.

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
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
	"strand/internal/model"
)

var (
	trackMu sync.Mutex
	tracked []testcontainers.Container
)

func trackContainer(c testcontainers.Container) {
	trackMu.Lock()
	tracked = append(tracked, c)
	trackMu.Unlock()
}

// cleanupTracked dừng mọi container do test PG tạo (kể cả khi test fail giữa chừng).
func cleanupTracked() {
	trackMu.Lock()
	defer trackMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for i := len(tracked) - 1; i >= 0; i-- {
		_ = tracked[i].Terminate(ctx)
	}
	tracked = nil
}

func pgImage() string {
	if v := os.Getenv("STRAND_E2E_PG_IMAGE"); v != "" {
		return v
	}
	return "postgres:18"
}

type pgNode struct {
	c     testcontainers.Container
	Alias string
	DSN   string
	DB    *sql.DB
}

// startPG khởi động postgres với wal_level=logical, PREPARE TRANSACTION, logical_decoding_work_mem
// nhỏ (Txn lớn bị stream).
// extra: tham số "-c k=v" thêm (đứng sau nên ghi đè mặc định).
func startPG(t *testing.T, alias string, extra ...string) *pgNode {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        pgImage(),
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "strand"},
		Cmd: append([]string{"postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=20", "-c", "max_replication_slots=20",
			"-c", "max_prepared_transactions=50", "-c", "logical_decoding_work_mem=64kB", "-c", "max_connections=300",
			"-c", "fsync=off", "-c", "synchronous_commit=off"}, extra...),
		WaitingFor: wait.ForAll(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		).WithDeadline(3 * time.Minute),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if c != nil {
		trackContainer(c)
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatalf("start %s: %v", pgImage(), err)
	}
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432")
	n := &pgNode{c: c, Alias: alias}
	n.DSN = fmt.Sprintf("postgres://strand:dev@%s/strand?sslmode=disable", net.JoinHostPort(host, port.Port()))
	if n.DB, err = sql.Open("pgx", n.DSN); err != nil {
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

func (n *pgNode) exec(t *testing.T, qs ...string) {
	t.Helper()
	for _, q := range qs {
		if _, err := n.DB.Exec(q); err != nil {
			t.Fatalf("%s: %s: %v", n.Alias, q, err)
		}
	}
}

// --------------------------------------------------------------------------
// Lược đồ PG→PG
// --------------------------------------------------------------------------

var pgSchemaSQL = []string{
	"CREATE SCHEMA app",
	"CREATE SEQUENCE app.ver_seq START 1000000",
	// doc: cột TOAST lưu ngoài dòng, không nén (STORAGE EXTERNAL) — luôn > 2 KB.
	`CREATE TABLE app.items (id bigint PRIMARY KEY, ver bigint NOT NULL, qty int NOT NULL, price numeric(12,2) NOT NULL,
		note text, doc text, tags jsonb, updated_at timestamptz)`,
	"ALTER TABLE app.items ALTER COLUMN doc SET STORAGE EXTERNAL",
	// accounts: UNIQUE ngoài PK, giá trị được hoán đổi trong một Txn.
	"CREATE TABLE app.accounts (id bigint PRIMARY KEY, code text NOT NULL UNIQUE, ver bigint NOT NULL, balance bigint NOT NULL)",
	"CREATE TABLE app.events (id bigint PRIMARY KEY, ver bigint NOT NULL, body text)",
	"CREATE TABLE app.marker (id bigint PRIMARY KEY, tag text)",
}

var pgAppTables = []string{"app.items", "app.accounts", "app.events", "app.marker"}

// pgPreload nạp dữ liệu ban đầu tất định — chạy y hệt trên source và target để target khớp source
// tại consistent_point của slot tạo SAU bước này.
func pgPreload(t *testing.T, n *pgNode, items, accounts, events int) {
	n.exec(t,
		fmt.Sprintf(`INSERT INTO app.items SELECT g, g, g %% 100, (g * 1.25)::numeric(12,2), 'n' || g, repeat(md5(g::text), 100),
			jsonb_build_object('k', g), '2026-01-01 00:00:00+00'::timestamptz + g * interval '1 second'
			FROM generate_series(1, %d) g`, items),
		fmt.Sprintf(`INSERT INTO app.accounts SELECT g, 'C' || lpad(g::text, 8, '0'), g, g * 10 FROM generate_series(1, %d) g`, accounts),
		fmt.Sprintf(`INSERT INTO app.events SELECT g, g, 'e' FROM generate_series(1, %d) g`, events),
	)
}

// pgApplyLog: trên target, mỗi lần một phiên bản row (bảng, ver) được ghi thì tăng cnt. Mọi lần ghi
// ở source đều đặt ver = nextval(...) mới, nên cnt > 1 nghĩa là một Txn bị apply hai lần (CP-2).
var pgApplyLogSQL = []string{
	"CREATE TABLE public.apply_log (tbl text NOT NULL, ver bigint NOT NULL, cnt int NOT NULL DEFAULT 1, PRIMARY KEY (tbl, ver))",
	`CREATE FUNCTION public.log_apply() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		INSERT INTO public.apply_log (tbl, ver) VALUES (TG_TABLE_NAME, NEW.ver)
		ON CONFLICT (tbl, ver) DO UPDATE SET cnt = public.apply_log.cnt + 1;
		RETURN NEW;
	END $$`,
	"CREATE TRIGGER t_items AFTER INSERT OR UPDATE ON app.items FOR EACH ROW EXECUTE FUNCTION public.log_apply()",
	"CREATE TRIGGER t_accounts AFTER INSERT OR UPDATE ON app.accounts FOR EACH ROW EXECUTE FUNCTION public.log_apply()",
	"CREATE TRIGGER t_events AFTER INSERT OR UPDATE ON app.events FOR EACH ROW EXECUTE FUNCTION public.log_apply()",
}

// setupPGPair dựng source/target PG→PG: lược đồ, preload giống hệt, publication, trigger đếm apply
// ở target, rồi tạo slot (two_phase) và trả consistent_point.
func setupPGPair(t *testing.T, slot, pub string) (*pgNode, *pgNode, string) {
	src := startPG(t, "pgsrc")
	dst := startPG(t, "pgdst")
	src.exec(t, pgSchemaSQL...)
	dst.exec(t, pgSchemaSQL...)
	pgPreload(t, src, 3000, 1000, 3000)
	pgPreload(t, dst, 3000, 1000, 3000)
	dst.exec(t, pgApplyLogSQL...)
	src.exec(t, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')", pub, strings.Join(pgAppTables, ", ")))
	var lsn string
	if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', false, true)", slot).Scan(&lsn); err != nil {
		t.Fatalf("create slot: %v", err)
	}
	t.Logf("slot %s consistent_point %s", slot, lsn)
	return src, dst, lsn
}

// --------------------------------------------------------------------------
// Bộ sinh tải PG
// --------------------------------------------------------------------------

type pgWorkload struct {
	db       *sql.DB
	rng      *rand.Rand
	nextID   atomic.Int64
	ops      atomic.Int64
	errs     atomic.Int64
	prepared atomic.Int64
	bigTxns  atomic.Int64
	stopped  atomic.Bool
	wg       sync.WaitGroup
	gid      atomic.Int64
}

func newPGWorkload(db *sql.DB, seed int64) *pgWorkload {
	w := &pgWorkload{db: db, rng: rand.New(rand.NewSource(seed))}
	w.nextID.Store(10_000_000)
	return w
}

func (w *pgWorkload) id() int64 { return w.nextID.Add(1) }

func (w *pgWorkload) Start(clients int, span int64) {
	for c := 0; c < clients; c++ {
		w.wg.Add(1)
		r := rand.New(rand.NewSource(w.rng.Int63()))
		go func() {
			defer w.wg.Done()
			for !w.stopped.Load() {
				if err := w.one(r, span); err != nil {
					w.errs.Add(1)
				}
				w.ops.Add(1)
			}
		}()
	}
}

func (w *pgWorkload) Stop() {
	w.stopped.Store(true)
	w.wg.Wait()
}

func hot(r *rand.Rand, n int64) int64 {
	if r.Intn(2) == 0 {
		return 1 + r.Int63n(10)
	}
	return 1 + r.Int63n(n)
}

// BigTxn chèn n row events trong MỘT Txn (> logical_decoding_work_mem → server stream Txn).
func (w *pgWorkload) BigTxn(n int) error {
	base := w.nextID.Add(int64(n)+10) - int64(n) - 9 // dải [base, base+n) dành riêng cho Txn này
	_, err := w.db.Exec(fmt.Sprintf(`INSERT INTO app.events SELECT %d + g, nextval('app.ver_seq'), repeat('z', 100)
		FROM generate_series(0, %d) g`, base, n-1))
	if err == nil {
		w.bigTxns.Add(1)
	}
	return err
}

func (w *pgWorkload) one(r *rand.Rand, span int64) error {
	ctx := context.Background()
	if r.Intn(1500) == 0 { // thỉnh thoảng một Txn lớn (bị server stream)
		return w.BigTxn(3000)
	}
	k := r.Intn(100)
	if k >= 84 && k < 88 {
		return w.twoPhase(ctx, r, span)
	}
	if k >= 57 && k < 59 {
		return w.moveCode(ctx, r, span)
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch {
	case k < 30: // cập nhật item KHÔNG chạm cột TOAST doc
		if _, err := tx.Exec(`UPDATE app.items SET qty = qty + 1, price = price + 0.5, ver = nextval('app.ver_seq'),
			tags = jsonb_set(coalesce(tags, '{}'), '{n}', to_jsonb(qty)), updated_at = now() WHERE id = $1`, hot(r, span)); err != nil {
			return err
		}
	case k < 38: // chèn item có doc lớn
		if _, err := tx.Exec(`INSERT INTO app.items VALUES ($1, nextval('app.ver_seq'), 1, 9.99, 'new',
			repeat(md5(random()::text), 100), '{"a": 1}', now())`, w.id()); err != nil {
			return err
		}
	case k < 42: // đổi chính cột TOAST
		if _, err := tx.Exec(`UPDATE app.items SET doc = repeat(md5(random()::text), 120), ver = nextval('app.ver_seq') WHERE id = $1`,
			1+r.Int63n(span)); err != nil {
			return err
		}
	case k < 47: // xóa item
		if _, err := tx.Exec(`DELETE FROM app.items WHERE id = $1`, 1+r.Int63n(span*2)); err != nil {
			return err
		}
	case k < 57: // hoán đổi UNIQUE code giữa hai account trong một Txn
		a, b := hot(r, span/3), 1+r.Int63n(span/3)
		if a == b {
			return nil
		}
		lo, hi := a, b
		if lo > hi {
			lo, hi = hi, lo
		}
		var ca, cb string
		if err := tx.QueryRow(`SELECT code FROM app.accounts WHERE id = $1 FOR UPDATE`, lo).Scan(&ca); err != nil {
			return nil
		}
		if err := tx.QueryRow(`SELECT code FROM app.accounts WHERE id = $1 FOR UPDATE`, hi).Scan(&cb); err != nil {
			return nil
		}
		if _, err := tx.Exec(`UPDATE app.accounts SET code = $1, ver = nextval('app.ver_seq') WHERE id = $2`, "tmp-"+ca, lo); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE app.accounts SET code = $1, ver = nextval('app.ver_seq'), balance = balance + 1 WHERE id = $2`, ca, hi); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE app.accounts SET code = $1, ver = nextval('app.ver_seq') WHERE id = $2`, cb, lo); err != nil {
			return err
		}
	case k < 61: // chèn account
		id := w.id()
		if _, err := tx.Exec(`INSERT INTO app.accounts VALUES ($1, $2, nextval('app.ver_seq'), 0)`, id, fmt.Sprintf("N%012d", id)); err != nil {
			return err
		}
	case k < 74: // upsert events
		if _, err := tx.Exec(`INSERT INTO app.events VALUES ($1, nextval('app.ver_seq'), 'x')
			ON CONFLICT (id) DO UPDATE SET ver = EXCLUDED.ver, body = 'y'`, hot(r, span)); err != nil {
			return err
		}
	case k < 79: // đổi PK của events
		if _, err := tx.Exec(`UPDATE app.events SET id = $1, ver = nextval('app.ver_seq') WHERE id = $2`, w.id(), 1+r.Int63n(span)); err != nil {
			return err
		}
	default: // Txn nhiều bảng, nhiều row
		id := hot(r, span)
		if _, err := tx.Exec(`UPDATE app.items SET qty = qty - 1, ver = nextval('app.ver_seq') WHERE id = $1`, id); err != nil {
			return err
		}
		for j := 0; j < 1+r.Intn(5); j++ {
			if _, err := tx.Exec(`INSERT INTO app.events VALUES ($1, nextval('app.ver_seq'), 'm')`, w.id()); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`UPDATE app.accounts SET balance = balance - 1, ver = nextval('app.ver_seq') WHERE id = $1`, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// moveCode: Txn 1 đổi code của account a (giải phóng giá trị X), Txn 2 (commit sau) gán X cho
// account c. pgoutput không gửi giá trị cũ X của Txn 1 nên writeset không thấy hai Txn xung đột:
// target phải tự xử lý khi Txn 2 bị apply trước Txn 1 (unique_violation tạm thời).
func (w *pgWorkload) moveCode(ctx context.Context, r *rand.Rand, span int64) error {
	a, c := 1+r.Int63n(span/3), 1+r.Int63n(span/3)
	if a == c {
		return nil
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	var x string
	if err := tx.QueryRow(`SELECT code FROM app.accounts WHERE id = $1 FOR UPDATE`, a).Scan(&x); err != nil {
		_ = tx.Rollback()
		return nil
	}
	if _, err := tx.Exec(`UPDATE app.accounts SET code = $1, ver = nextval('app.ver_seq') WHERE id = $2`,
		fmt.Sprintf("M%d-%d", a, w.id()), a); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_, err = w.db.ExecContext(ctx, `UPDATE app.accounts SET code = $1, ver = nextval('app.ver_seq') WHERE id = $2`, x, c)
	return err
}

// twoPhase: BEGIN … PREPARE TRANSACTION, rồi COMMIT PREPARED (90%) hoặc ROLLBACK PREPARED.
func (w *pgWorkload) twoPhase(ctx context.Context, r *rand.Rand, span int64) error {
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	gid := fmt.Sprintf("strand_e2e_%d", w.gid.Add(1))
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	stmts := []string{
		fmt.Sprintf(`UPDATE app.items SET qty = qty + 7, ver = nextval('app.ver_seq') WHERE id = %d`, hot(r, span)),
		fmt.Sprintf(`INSERT INTO app.events VALUES (%d, nextval('app.ver_seq'), '2pc')`, w.id()),
		fmt.Sprintf(`INSERT INTO app.events VALUES (%d, nextval('app.ver_seq'), '2pc')`, w.id()),
	}
	for _, q := range stmts {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
			return err
		}
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PREPARE TRANSACTION '%s'", gid)); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	time.Sleep(time.Duration(r.Intn(30)) * time.Millisecond)
	verb := "COMMIT PREPARED"
	if r.Intn(10) == 0 {
		verb = "ROLLBACK PREPARED"
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("%s '%s'", verb, gid)); err != nil {
		return err
	}
	w.prepared.Add(1)
	return nil
}

// resolvePrepared commit mọi Txn PREPARE còn treo (client lỗi giữa hai bước).
func resolvePrepared(t *testing.T, n *pgNode) {
	rows, err := n.DB.Query("SELECT gid FROM pg_prepared_xacts")
	if err != nil {
		t.Fatal(err)
	}
	var gids []string
	for rows.Next() {
		var g string
		_ = rows.Scan(&g)
		gids = append(gids, g)
	}
	rows.Close()
	for _, g := range gids {
		n.exec(t, fmt.Sprintf("COMMIT PREPARED '%s'", g))
	}
}

// --------------------------------------------------------------------------
// Chờ theo kịp, so sánh, kiểm tra
// --------------------------------------------------------------------------

// waitPGCaughtUp chèn một marker ở source rồi chờ target có marker và lwd đã xác nhận (row toàn cục)
// vượt LSN trước commit của marker: mọi Txn trước đó đã apply bền vững.
func waitPGCaughtUp(t *testing.T, src, dst *pgNode, cfg *strandCfg, p *strandProc, timeout time.Duration) {
	t.Helper()
	resolvePrepared(t, src)
	markerID := time.Now().UnixNano()
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var before string
	if err := tx.QueryRow("SELECT pg_current_wal_lsn()::text").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO app.marker VALUES ($1, 'caught-up')", markerID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	l0, _ := checkpoint.ParseLSN(before)
	deadline := time.Now().Add(timeout)
	for {
		if p != nil && p.exited() {
			t.Fatalf("strand exited (code %d) before catching up\n%s", p.exitCode(), p.tail())
		}
		var n int
		_ = dst.DB.QueryRow("SELECT count(*) FROM app.marker WHERE id = $1", markerID).Scan(&n)
		got, err := checkpoint.LoadAppliedSnapshotPG(context.Background(), dst.DB, cfg.Name, model.EnginePG)
		if err == nil && n == 1 && got.Lwd().LSN > l0 && pipelineSynced(cfg.Socket) {
			return
		}
		if time.Now().After(deadline) {
			lwd := "?"
			if got != nil {
				lwd = checkpoint.FormatLSN(got.Lwd().LSN)
			}
			t.Fatalf("target not caught up after %s (marker=%d, lwd=%s, want > %s, err=%v)\n%s", timeout, n, lwd, before, err, tailOf(p))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// dumpPG trả id → JSON chuẩn của row (to_jsonb), theo thứ tự id.
func dumpPG(t *testing.T, db *sql.DB, table string) map[string]string {
	rows, err := db.Query(fmt.Sprintf("SELECT id::text, to_jsonb(t)::text FROM %s t", table))
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, js string
		if err := rows.Scan(&id, &js); err != nil {
			t.Fatal(err)
		}
		out[id] = js
	}
	return out
}

// comparePG so sánh từng row của mọi bảng giữa source và target.
func comparePG(t *testing.T, src, dst *pgNode, tables ...string) {
	t.Helper()
	total := 0
	var bad []string
	for _, tb := range tables {
		s, d := dumpPG(t, src.DB, tb), dumpPG(t, dst.DB, tb)
		total += len(s)
		for id, sv := range s {
			dv, ok := d[id]
			switch {
			case !ok:
				bad = append(bad, fmt.Sprintf("%s id=%s missing on target", tb, id))
			case dv != sv:
				bad = append(bad, fmt.Sprintf("%s id=%s differs:\n  src %.300s\n  dst %.300s", tb, id, sv, dv))
			}
		}
		for id := range d {
			if _, ok := s[id]; !ok {
				bad = append(bad, fmt.Sprintf("%s id=%s extra on target", tb, id))
			}
		}
	}
	if len(bad) > 0 {
		if len(bad) > 15 {
			bad = bad[:15]
		}
		t.Logf("diagnostics:\n%s", pgMismatchDiagnostics(t, src, dst, tables))
		t.Fatalf("final state mismatch:\n%s", strings.Join(bad, "\n"))
	}
	t.Logf("final state: %d rows in %d tables identical", total, len(tables))
}

// pgMismatchDiagnostics: với từng row lệch/thiếu — xid (xmin) và ver ở source, và apply_log của target
// cho đúng ver đó (vắng = phiên bản chưa từng được ghi ở target; có = đã ghi rồi bị đè).
func pgMismatchDiagnostics(t *testing.T, src, dst *pgNode, tables []string) string {
	var out []string
	for _, tb := range tables {
		s, d := dumpPG(t, src.DB, tb), dumpPG(t, dst.DB, tb)
		for id, sv := range s {
			if d[id] == sv {
				continue
			}
			var xmin, ver string
			_ = src.DB.QueryRow(fmt.Sprintf("SELECT xmin::text, ver::text FROM %s WHERE id = $1", tb), id).Scan(&xmin, &ver)
			var dver string
			_ = dst.DB.QueryRow(fmt.Sprintf("SELECT ver::text FROM %s WHERE id = $1", tb), id).Scan(&dver)
			var cnt sql.NullInt64
			_ = dst.DB.QueryRow("SELECT cnt FROM public.apply_log WHERE tbl = $1 AND ver = $2::bigint", strings.TrimPrefix(tb, "app."), ver).Scan(&cnt)
			out = append(out, fmt.Sprintf("%s id=%s src xmin=%s ver=%s | dst ver=%q apply_log(src ver)=%v", tb, id, xmin, ver, dver, cnt))
			if len(out) > 30 {
				return strings.Join(out, "\n")
			}
		}
	}
	return strings.Join(out, "\n")
}

// checkPGInvariants: cột TOAST không bao giờ NULL ở target; không phiên bản row nào bị ghi hai lần.
func checkPGInvariants(t *testing.T, dst *pgNode) {
	t.Helper()
	var nulls, dups, logged int
	if err := dst.DB.QueryRow("SELECT count(*) FROM app.items WHERE doc IS NULL").Scan(&nulls); err != nil {
		t.Fatal(err)
	}
	if nulls != 0 {
		t.Fatalf("TOAST column doc is NULL on %d target rows", nulls)
	}
	if err := dst.DB.QueryRow("SELECT count(*) FILTER (WHERE cnt > 1), count(*) FROM public.apply_log").Scan(&dups, &logged); err != nil {
		t.Fatal(err)
	}
	if dups != 0 {
		rows, _ := dst.DB.Query("SELECT tbl, ver, cnt FROM public.apply_log WHERE cnt > 1 LIMIT 10")
		var ex []string
		for rows != nil && rows.Next() {
			var tb string
			var v, c int64
			_ = rows.Scan(&tb, &v, &c)
			ex = append(ex, fmt.Sprintf("%s ver=%d x%d", tb, v, c))
		}
		t.Fatalf("duplicate apply: %d row versions written more than once on target (CP-2), e.g. %v", dups, ex)
	}
	t.Logf("invariants: doc never NULL, %d row versions applied exactly once", logged)
}

// pgVerSampler: ver của mỗi khóa ở target không bao giờ giảm (RS-1, không apply lại Txn cũ).
type pgVerSampler struct {
	stop       atomic.Bool
	wg         sync.WaitGroup
	mu         sync.Mutex
	last       map[string]int64
	violations []string
	samples    int
}

func startPGVerSampler(dst *pgNode, tables ...string) *pgVerSampler {
	vs := &pgVerSampler{last: map[string]int64{}}
	vs.wg.Add(1)
	go func() {
		defer vs.wg.Done()
		for !vs.stop.Load() {
			for _, tb := range tables {
				rows, err := dst.DB.Query(fmt.Sprintf("SELECT id, ver FROM %s", tb))
				if err != nil {
					continue
				}
				vs.mu.Lock()
				for rows.Next() {
					var id, v int64
					_ = rows.Scan(&id, &v)
					k := fmt.Sprintf("%s/%d", tb, id)
					if prev, ok := vs.last[k]; ok && v < prev {
						if len(vs.violations) < 50 {
							vs.violations = append(vs.violations, fmt.Sprintf("%s ver %d -> %d", k, prev, v))
						}
					} else {
						vs.last[k] = v
					}
				}
				vs.mu.Unlock()
				rows.Close()
			}
			vs.mu.Lock()
			vs.samples++
			vs.mu.Unlock()
			time.Sleep(300 * time.Millisecond)
		}
	}()
	return vs
}

func (vs *pgVerSampler) check(t *testing.T) {
	vs.stop.Store(true)
	vs.wg.Wait()
	if len(vs.violations) > 0 {
		t.Fatalf("ver decreased on target: %v", vs.violations)
	}
	t.Logf("ver monotonic over %d samples", vs.samples)
}

// slotConfirmed đọc confirmed_flush_lsn của slot ở source.
func slotConfirmed(t *testing.T, src *pgNode, slot string) uint64 {
	var s sql.NullString
	if err := src.DB.QueryRow("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&s); err != nil {
		t.Fatal(err)
	}
	l, _ := checkpoint.ParseLSN(s.String)
	return l
}
