package e2e

// Harness e2e Gate 1 (04 §2, §3): MySQL thật (testcontainers), binary `strand` chạy như
// process riêng, bộ sinh tải ghi vào source, checker độc lập đọc thẳng source/target
// (không dùng code decode của Strand).

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/admin"
	"strand/internal/checkpoint"
	"strand/tests/checker"
)

var strandBin string

func TestMain(m *testing.M) {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	dir, err := os.MkdirTemp("", "strand-e2e-bin-")
	if err != nil {
		panic(err)
	}
	strandBin = filepath.Join(dir, "strand")
	build := exec.Command("go", "build", "-o", strandBin, "../../cmd/strand")
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	if err := build.Run(); err != nil {
		panic("build strand: " + err.Error())
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func seedFromEnv(t *testing.T) int64 {
	if s := os.Getenv("SEED"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			t.Logf("SEED=%d", v)
			return v
		}
	}
	v := time.Now().UnixNano()
	t.Logf("SEED=%d (đặt SEED để chạy lại đúng kịch bản)", v)
	return v
}

func envInt(name string, def int) int {
	if s := os.Getenv(name); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			return v
		}
	}
	return def
}

func mysqlImage(role string) string {
	if v := os.Getenv("STRAND_E2E_" + strings.ToUpper(role) + "_IMAGE"); v != "" {
		return v
	}
	if v := os.Getenv("STRAND_E2E_MYSQL_IMAGE"); v != "" {
		return v
	}
	return "mysql:8.4"
}

// --------------------------------------------------------------------------
// MySQL containers
// --------------------------------------------------------------------------

type mysqlNode struct {
	c       testcontainers.Container
	Alias   string // tên trong docker network
	Addr    string // host:port từ máy chạy test
	RootDSN string
	DB      *sql.DB
}

func startMySQL(t *testing.T, image, alias string, serverID int, nw *testcontainers.DockerNetwork, extra ...string) *mysqlNode {
	t.Helper()
	ctx := context.Background()
	args := []string{
		fmt.Sprintf("--server-id=%d", serverID),
		"--gtid-mode=ON", "--enforce-gtid-consistency=ON",
		"--binlog-format=ROW", "--binlog-row-image=FULL", "--binlog-row-metadata=FULL",
		"--log-replica-updates=ON", "--max-connections=500",
		// Không dùng native AIO: máy test chạy nhiều MySQL cùng lúc dễ cạn fs.aio-max-nr khiến mysqld không khởi động.
		"--innodb-use-native-aio=OFF",
	}
	args = append(args, extra...)
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"3306/tcp"},
		Env:          map[string]string{"MYSQL_ROOT_PASSWORD": "dev"},
		Cmd:          args,
		WaitingFor:   wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(240 * time.Second),
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

func (n *mysqlNode) exec(t *testing.T, qs ...string) {
	t.Helper()
	for _, q := range qs {
		if _, err := n.DB.Exec(q); err != nil {
			t.Fatalf("%s: %s: %v", n.Alias, q, err)
		}
	}
}

func (n *mysqlNode) gtidExecuted(t *testing.T) string {
	var s string
	if err := n.DB.QueryRow("SELECT @@GLOBAL.gtid_executed").Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// Cột ver luôn = UUID_SHORT() (tăng đơn điệu toàn server), nên mọi lần ghi một khóa — kể cả
// xóa rồi chèn lại — đều có ver lớn hơn: phép kiểm "ver không giảm" (04 §3) hợp lệ.
// schemaSQL là lược đồ test: PK số, unique key phụ, FK cha–con, cột ver, bảng bị loại.
var schemaSQL = []string{
	"CREATE DATABASE IF NOT EXISTS app",
	`CREATE TABLE IF NOT EXISTS app.accounts (
		id BIGINT PRIMARY KEY,
		code VARCHAR(32) NOT NULL,
		ver BIGINT UNSIGNED NOT NULL,
		balance BIGINT NOT NULL,
		note VARCHAR(255),
		payload JSON,
		updated_at DATETIME(6),
		UNIQUE KEY uk_code (code))`,
	`CREATE TABLE IF NOT EXISTS app.orders (
		id BIGINT PRIMARY KEY,
		account_id BIGINT NOT NULL,
		ver BIGINT UNSIGNED NOT NULL,
		amount DECIMAL(18,2) NOT NULL,
		KEY (account_id),
		CONSTRAINT fk_acc FOREIGN KEY (account_id) REFERENCES app.accounts(id))`,
	`CREATE TABLE IF NOT EXISTS app.events (
		id BIGINT PRIMARY KEY,
		ver BIGINT UNSIGNED NOT NULL,
		body VARCHAR(64))`,
	`CREATE TABLE IF NOT EXISTS app.ignored (id BIGINT PRIMARY KEY, v BIGINT)`,
}

func createSchema(t *testing.T, nodes ...*mysqlNode) {
	for _, n := range nodes {
		n.exec(t, schemaSQL...)
	}
}

// --------------------------------------------------------------------------
// Strand process
// --------------------------------------------------------------------------

// socket: unix socket cần đường dẫn ngắn (< 108 byte), nên đặt trong t.TempDir() riêng.
func (c *strandCfg) socket(t *testing.T) string { return c.Socket }

type strandCfg struct {
	Socket      string
	Name        string
	SourceDSN   string
	TargetDSN   string
	Include     []string
	Exclude     []string
	Dir         string
	CommitOrder string
	Workers     int
	LagSLO      string
	Bootstrap   string
	// PostSeedWindow: cửa sổ verify sau seed (mặc định 5s trong test; production 30m).
	PostSeedWindow string
	VerifyDelta    string
	SourceIOPS     int
	BreakerLag     string // governor.breaker.replica_lag.max
	MetricsAddr    string // observability.metrics, ví dụ 127.0.0.1:19464
	Overflow       string // resources.spool.overflow (s3://bucket/prefix)
}

func (c *strandCfg) write(t *testing.T) string {
	t.Helper()
	if c.Socket == "" {
		c.Socket = filepath.Join(t.TempDir(), "admin.sock")
	}
	if c.CommitOrder == "" {
		// STRAND_E2E_COMMIT_ORDER=strict chạy toàn bộ e2e ở chế độ strict (mặc định của spec).
		c.CommitOrder = os.Getenv("STRAND_E2E_COMMIT_ORDER")
		if c.CommitOrder == "" {
			c.CommitOrder = "relaxed"
		}
	}
	if c.Workers == 0 {
		c.Workers = 8
	}
	if c.LagSLO == "" {
		c.LagSLO = "5s"
	}
	if c.Bootstrap == "" {
		c.Bootstrap = "logical"
	}
	if c.PostSeedWindow == "" {
		c.PostSeedWindow = "5s"
	}
	if c.VerifyDelta == "" {
		c.VerifyDelta = "off"
	}
	if c.SourceIOPS == 0 {
		c.SourceIOPS = 600
	}
	if c.BreakerLag == "" {
		c.BreakerLag = "1h"
	}
	q := func(xs []string) string {
		var out []string
		for _, x := range xs {
			out = append(out, strconv.Quote(x))
		}
		return "[" + strings.Join(out, ", ") + "]"
	}
	yaml := fmt.Sprintf(`version: 1
name: %s
source: { kind: mysql, dsn: %q, server_id: %d, read_from: primary }
target:
  kind: mysql
  dsn: %q
  write_mode: upsert
  commit_order: %s
  batch: { max_rows: 2000, max_bytes: 4MiB, max_wait: 50ms }
tables:
  include: %s
  exclude: %s
bootstrap: { default: %s }
checkpoint: { store: target, flush_every: 200ms }
resources:
  spool: { path: %q, soft: 1GiB, hard: 2GiB, overflow: %q }
  source: { iops: { soft: %d, hard: %d } }
  workers: { min: 1, max: %d }
governor: { algorithm: onoff, tick: 1s, lag_slo: %s, breaker: { strand_query_p99: { max_ratio: 1000 }, replica_lag: { max: %s } } }
verify: { delta: %s, post_seed_window: %s }
observability: { metrics: %q, admin_socket: %q, log: { level: info, format: json } }
`, c.Name, c.SourceDSN, 40000+rand.Intn(20000), c.TargetDSN, c.CommitOrder, q(c.Include), q(c.Exclude),
		c.Bootstrap, filepath.Join(c.Dir, "state"), c.Overflow, c.SourceIOPS, c.SourceIOPS*3/2, c.Workers, c.LagSLO, c.BreakerLag, c.VerifyDelta, c.PostSeedWindow, c.MetricsAddr, c.socket(t))
	p := filepath.Join(c.Dir, "pipeline.yaml")
	if err := os.WriteFile(p, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

type strandProc struct {
	cmd    *exec.Cmd
	done   chan struct{}
	err    error
	log    string
	output *lockedBuf
}

type lockedBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

var runCounter atomic.Int64

func startStrand(t *testing.T, cfgPath string, extra ...string) *strandProc {
	t.Helper()
	args := append([]string{"sync", "-c", cfgPath}, extra...)
	cmd := exec.Command(strandBin, args...)
	logPath := filepath.Join(filepath.Dir(cfgPath), fmt.Sprintf("strand-%04d.log", runCounter.Add(1)))
	f, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	buf := &lockedBuf{}
	cmd.Stdout = f
	cmd.Stderr = ioMulti(f, buf)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &strandProc{cmd: cmd, done: make(chan struct{}), log: logPath, output: buf}
	go func() {
		p.err = cmd.Wait()
		f.Close()
		close(p.done)
	}()
	return p
}

type multiW struct {
	ws []interface{ Write([]byte) (int, error) }
}

func (m multiW) Write(p []byte) (int, error) {
	for _, w := range m.ws {
		_, _ = w.Write(p)
	}
	return len(p), nil
}
func ioMulti(ws ...interface{ Write([]byte) (int, error) }) multiW { return multiW{ws} }

func (p *strandProc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *strandProc) exitCode() int {
	<-p.done
	if p.cmd.ProcessState == nil {
		return -1
	}
	return p.cmd.ProcessState.ExitCode()
}

func (p *strandProc) kill9() {
	_ = p.cmd.Process.Signal(syscall.SIGKILL)
	<-p.done
}

func (p *strandProc) stop(t *testing.T) int {
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
	case <-time.After(150 * time.Second):
		p.kill9()
		t.Fatalf("strand did not stop within 150s after SIGTERM\n%s", p.tail())
	}
	return p.cmd.ProcessState.ExitCode()
}

func (p *strandProc) tail() string {
	s := p.output.String()
	if len(s) > 4000 {
		s = s[len(s)-4000:]
	}
	return s
}

// waitCaughtUp chờ tập đã apply trên target phủ toàn bộ gtid_executed của source.
func waitCaughtUp(t *testing.T, src, dst *mysqlNode, cfg *strandCfg, p *strandProc, timeout time.Duration) {
	pipelineID := cfg.Name
	t.Helper()
	executed := src.gtidExecuted(t)
	want, err := checkpoint.ParseMySQLGTIDSet(executed)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := sql.Open("mysql", dst.RootDSN+"strand")
	if err != nil {
		t.Fatal(err)
	}
	defer meta.Close()
	deadline := time.Now().Add(timeout)
	for {
		if p != nil && p.exited() {
			t.Fatalf("strand exited (code %d) before catching up\n%s", p.exitCode(), p.tail())
		}
		got, err := checkpoint.LoadAppliedSnapshotMySQL(context.Background(), meta, pipelineID, "mysql")
		if err == nil {
			miss := want.Clone().(*checkpoint.MySQLAppliedSet)
			miss.Subtract(got.(*checkpoint.MySQLAppliedSet))
			if miss.String() == "" && pipelineSynced(cfg.Socket) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("target not caught up after %s; missing %s\n%s", timeout, miss.String(), tailOf(p))
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("load applied: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func tailOf(p *strandProc) string {
	if p == nil {
		return ""
	}
	return p.tail()
}

// pipelineSynced hỏi trạng thái qua admin socket: bootstrap (snapshot) phải xong.
func pipelineSynced(socket string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	top, err := admin.NewClient(socket).Top(ctx)
	return err == nil && top.State == "synced"
}

// --------------------------------------------------------------------------
// Bộ sinh tải (04 §3): khóa nóng, đổi PK, unique key phụ, FK cha–con, Txn nhiều row
// --------------------------------------------------------------------------

type workload struct {
	db      *sql.DB
	rng     *rand.Rand
	mu      sync.Mutex
	nextID  int64
	ops     atomic.Int64
	errs    atomic.Int64
	stopped atomic.Bool
	wg      sync.WaitGroup
}

func newWorkload(db *sql.DB, seed int64) *workload {
	return &workload{db: db, rng: rand.New(rand.NewSource(seed)), nextID: 1}
}

func (w *workload) id() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextID++
	return w.nextID
}

func (w *workload) pickHot(r *rand.Rand, n int64) int64 {
	// ~Zipf: 50% truy cập dồn vào 10 khóa đầu.
	if r.Intn(2) == 0 {
		return 1 + r.Int63n(10)
	}
	return 1 + r.Int63n(n)
}

// Preload chèn n account và n order ban đầu.
func (w *workload) Preload(t *testing.T, n int) {
	tx, err := w.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if _, err := tx.Exec("INSERT INTO app.accounts VALUES (?, ?, UUID_SHORT(), ?, ?, ?, NOW(6))",
			i, fmt.Sprintf("C%08d", i), i*10, fmt.Sprintf("note %d", i), fmt.Sprintf(`{"k": %d, "s": "x"}`, i)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("INSERT INTO app.orders VALUES (?, ?, UUID_SHORT(), ?)", i, i, float64(i)+0.25); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("INSERT INTO app.events VALUES (?, UUID_SHORT(), 'e')", i); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec("INSERT INTO app.ignored VALUES (?, 1)", i); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	if int64(n) > w.nextID {
		w.nextID = int64(n) + 1000
	}
	w.mu.Unlock()
}

func (w *workload) Start(clients int, span int64) {
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

func (w *workload) Stop() {
	w.stopped.Store(true)
	w.wg.Wait()
}

func (w *workload) one(r *rand.Rand, span int64) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch k := r.Intn(100); {
	case k < 40: // update hot account + order
		id := w.pickHot(r, span)
		if _, err := tx.Exec("UPDATE app.accounts SET ver = UUID_SHORT(), balance = balance + ?, updated_at = NOW(6), payload = JSON_SET(COALESCE(payload, '{}'), '$.n', ver) WHERE id = ?", r.Intn(100), id); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE app.orders SET ver = UUID_SHORT(), amount = amount + 1.5 WHERE account_id = ?", id); err != nil {
			return err
		}
	case k < 55: // insert account + orders (FK cha trước con)
		id := w.id()
		if _, err := tx.Exec("INSERT INTO app.accounts VALUES (?, ?, UUID_SHORT(), 0, NULL, NULL, NOW(6))", id, fmt.Sprintf("N%010d", id)); err != nil {
			return err
		}
		for j := 0; j < 1+r.Intn(3); j++ {
			if _, err := tx.Exec("INSERT INTO app.orders VALUES (?, ?, UUID_SHORT(), ?)", w.id(), id, r.Intn(1000)); err != nil {
				return err
			}
		}
	case k < 65: // delete account (xóa con trước)
		id := 1 + r.Int63n(span*2)
		if _, err := tx.Exec("DELETE FROM app.orders WHERE account_id = ?", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM app.accounts WHERE id = ?", id); err != nil {
			return err
		}
	case k < 72: // đổi PK của events (có thể sang range khác)
		old := 1 + r.Int63n(span)
		if _, err := tx.Exec("UPDATE app.events SET id = ?, ver = UUID_SHORT() WHERE id = ?", w.id(), old); err != nil {
			return err
		}
	case k < 78: // đổi chéo unique key giữa hai account (cần writeset theo unique key)
		a, b := w.pickHot(r, span), 1+r.Int63n(span)
		if a == b {
			return nil
		}
		var ca, cb string
		if err := tx.QueryRow("SELECT code FROM app.accounts WHERE id = ? FOR UPDATE", a).Scan(&ca); err != nil {
			return nil
		}
		if err := tx.QueryRow("SELECT code FROM app.accounts WHERE id = ? FOR UPDATE", b).Scan(&cb); err != nil {
			return nil
		}
		if _, err := tx.Exec("UPDATE app.accounts SET code = ?, ver = UUID_SHORT() WHERE id = ?", "tmp-"+ca, a); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE app.accounts SET code = ?, ver = UUID_SHORT() WHERE id = ?", ca, b); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE app.accounts SET code = ?, ver = UUID_SHORT() WHERE id = ?", cb, a); err != nil {
			return err
		}
	case k < 85: // bảng bị loại khỏi pipeline
		if _, err := tx.Exec("INSERT INTO app.ignored VALUES (?, 1) ON DUPLICATE KEY UPDATE v = v + 1", 1+r.Int63n(span)); err != nil {
			return err
		}
	case k < 88: // Txn nhiều row
		base := w.id() * 1000
		for j := int64(0); j < 200; j++ {
			if _, err := tx.Exec("INSERT INTO app.events VALUES (?, UUID_SHORT(), 'bulk')", base+j); err != nil {
				return err
			}
		}
	default: // events insert/update
		id := w.pickHot(r, span)
		if _, err := tx.Exec("INSERT INTO app.events VALUES (?, UUID_SHORT(), 'x') ON DUPLICATE KEY UPDATE ver = UUID_SHORT(), body = 'y'", id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// --------------------------------------------------------------------------
// Checker trên DB thật
// --------------------------------------------------------------------------

func dumpTable(t *testing.T, db *sql.DB, table string) map[string]checker.RowState {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY 1", table))
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := map[string]checker.RowState{}
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		m := map[string]interface{}{}
		var ver int64
		for i, c := range cols {
			if vals[i] == nil {
				m[c] = nil
			} else {
				m[c] = string(vals[i])
			}
			if c == "ver" {
				ver, _ = strconv.ParseInt(string(vals[i]), 10, 64)
			}
		}
		pk := string(vals[0])
		out[pk] = checker.RowState{Table: table, PK: pk, Ver: ver, Values: m}
	}
	return out
}

func compareFinal(t *testing.T, src, dst *mysqlNode, tables ...string) {
	t.Helper()
	s := map[string]map[string]checker.RowState{}
	d := map[string]map[string]checker.RowState{}
	total := 0
	for _, tb := range tables {
		s[tb] = dumpTable(t, src.DB, tb)
		d[tb] = dumpTable(t, dst.DB, tb)
		total += len(s[tb])
	}
	r := checker.NewChecker().CheckFinalState(s, d)
	if !r.Passed {
		v := r.Violations
		if len(v) > 20 {
			v = v[:20]
		}
		t.Fatalf("final state mismatch (%d violations), first: %v", len(r.Violations), v)
	}
	t.Logf("final state: %d rows in %d tables identical", total, len(tables))
}

// verSampler lấy mẫu ver trên target liên tục (phép kiểm "ver không giảm", RS-1).
// Chỉ giữ ver lớn nhất đã thấy của mỗi khóa (bộ nhớ O(số khóa)), ghi vi phạm ngay khi gặp.
type verSampler struct {
	stop       atomic.Bool
	wg         sync.WaitGroup
	mu         sync.Mutex
	last       map[string]int64
	violations []string
	samples    int
}

func startVerSampler(dst *mysqlNode, tables ...string) *verSampler {
	vs := &verSampler{last: map[string]int64{}}
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
					var id string
					var v int64
					_ = rows.Scan(&id, &v)
					k := tb + "/" + id
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

func (vs *verSampler) check(t *testing.T) {
	vs.stop.Store(true)
	vs.wg.Wait()
	if len(vs.violations) > 0 {
		t.Fatalf("ver decreased on target (RS-1): %v", vs.violations)
	}
	t.Logf("ver monotonic over %d samples", vs.samples)
}

var appTables = []string{"app.accounts", "app.orders", "app.events"}

// checkNoOrphans: ràng buộc FK phải đúng ở target sau khi đồng bộ (04 §3, phép kiểm "Ràng buộc").
func checkNoOrphans(t *testing.T, dst *mysqlNode) {
	t.Helper()
	var n int
	if err := dst.DB.QueryRow(`SELECT COUNT(*) FROM app.orders o LEFT JOIN app.accounts a ON a.id = o.account_id WHERE a.id IS NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("FK violated on target: %d orphan orders", n)
	}
}

// testDir: thư mục làm việc của test; đặt STRAND_E2E_KEEP_DIR=1 để giữ lại log sau khi chạy.
func testDir(t *testing.T) string {
	keep := os.Getenv("STRAND_E2E_KEEP_DIR")
	if keep == "" {
		return t.TempDir()
	}
	parent := ""
	if keep != "1" {
		parent = keep // STRAND_E2E_KEEP_DIR=<thư mục cha>
	}
	d, err := os.MkdirTemp(parent, "strand-e2e-"+strings.ReplaceAll(t.Name(), "/", "_")+"-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("keeping test dir %s", d)
	return d
}
