package e2e

import (
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// P3-01 — schema evolution: thay đổi cộng thêm tự áp lên target; thay đổi phá hủy (hoặc additive: pause)
// dừng schema_change_pending, `strand schema diff` liệt kê, `strand schema approve <id>` áp, chạy lại đi tiếp.
//
// Các test dùng bảng riêng (evo) thay vì app.accounts/... vì workload chuẩn INSERT theo vị trí cột (không
// chịu được ADD COLUMN). evoWriter ghi bằng danh sách cột tường minh và chỉ dùng thêm cột khi cờ tương ứng
// đã bật (sau khi DDL đã chạy ở source).

const (
	evoAddedCols = 1 << iota // đã có cột extra, flag
	evoWidened               // v VARCHAR(100), cnt BIGINT
)

type evoWriter struct {
	db    *sql.DB
	table string
	rng   *rand.Rand
	next  atomic.Int64
	flags atomic.Int32
	stop  atomic.Bool
	ops   atomic.Int64
	wg    sync.WaitGroup
}

func newEvoWriter(db *sql.DB, table string, seed, firstID int64) *evoWriter {
	w := &evoWriter{db: db, table: table, rng: rand.New(rand.NewSource(seed))}
	w.next.Store(firstID)
	return w
}

func (w *evoWriter) Start(clients int, span int64) {
	for c := 0; c < clients; c++ {
		w.wg.Add(1)
		r := rand.New(rand.NewSource(w.rng.Int63()))
		go func() {
			defer w.wg.Done()
			for !w.stop.Load() {
				w.one(r, span)
				w.ops.Add(1)
				time.Sleep(time.Millisecond)
			}
		}()
	}
}

func (w *evoWriter) Stop() { w.stop.Store(true); w.wg.Wait() }

func (w *evoWriter) one(r *rand.Rand, span int64) {
	f := w.flags.Load()
	v := strings.Repeat("a", 5+r.Intn(10))
	var cnt any = r.Intn(1000)
	if f&evoWidened != 0 {
		v = strings.Repeat("w", 60+r.Intn(30))
		cnt = int64(5_000_000_000) + int64(r.Intn(1000))
	}
	switch k := r.Intn(100); {
	case k < 55:
		id := w.next.Add(1)
		cols, ph, args := "id, ver, v, cnt", "?, UUID_SHORT(), ?, ?", []any{id, v, cnt}
		if f&evoAddedCols != 0 {
			cols += ", extra, flag"
			ph += ", ?, 1"
			args = append(args, r.Intn(100))
		}
		_, _ = w.db.Exec(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", w.table, cols, ph), args...)
	case k < 90:
		_, _ = w.db.Exec(fmt.Sprintf("UPDATE %s SET ver = UUID_SHORT(), v = ?, cnt = ? WHERE id = ?", w.table), v, cnt, 1+r.Int63n(span))
	default:
		_, _ = w.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE id = ?", w.table), 1+r.Int63n(span))
	}
}

const evoMySQLTable = `CREATE TABLE app.evo (
	id BIGINT PRIMARY KEY,
	ver BIGINT UNSIGNED NOT NULL,
	v VARCHAR(20) NOT NULL,
	cnt INT NOT NULL DEFAULT 0,
	junk VARCHAR(10) NULL)`

func evoPreload(t *testing.T, db *sql.DB, table string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id, ver, v, cnt, junk) VALUES (?, UUID_SHORT(), ?, ?, 'j')", table), i, fmt.Sprintf("v%d", i), i); err != nil {
			t.Fatal(err)
		}
	}
}

func waitExitPending(t *testing.T, p *strandProc, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for !p.exited() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if !p.exited() || p.exitCode() != 10 || !strings.Contains(p.output.String(), "schema_change_pending") {
		t.Fatalf("%s: want exit 10 schema_change_pending, exited=%v\n%s", what, p.exited(), p.tail())
	}
}

func mustStayRunning(t *testing.T, p *strandProc, what string) {
	t.Helper()
	if p.exited() {
		t.Fatalf("%s: strand exited (code %d) but the change must be applied automatically\n%s", what, p.exitCode(), p.tail())
	}
}

// mysqlColType trả COLUMN_TYPE của cột ở target MySQL ("" nếu không có cột).
func mysqlColType(t *testing.T, db *sql.DB, schema, table, col string) string {
	t.Helper()
	var typ string
	err := db.QueryRow(`SELECT LOWER(COLUMN_TYPE) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, schema, table, col).Scan(&typ)
	if err == sql.ErrNoRows {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return typ
}

func mysqlHasIndex(t *testing.T, db *sql.DB, schema, table, idx string) bool {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME=?`, schema, table, idx).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

var reChangeID = regexp.MustCompile(`#(\d+)`)

// pendingID chạy `strand schema diff` và trả id thay đổi đầu tiên cùng toàn bộ output.
func pendingID(t *testing.T, cfgPath string, mustContain ...string) (string, string) {
	t.Helper()
	out, code := runStrandCLI(t, time.Minute, "schema", "diff", "-c", cfgPath)
	if code != 0 {
		t.Fatalf("schema diff exit %d:\n%s", code, out)
	}
	for _, s := range mustContain {
		if !strings.Contains(out, s) {
			t.Fatalf("schema diff output lacks %q:\n%s", s, out)
		}
	}
	m := reChangeID.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("schema diff lists no change id:\n%s", out)
	}
	return m[1], out
}

type evoMySQLEnv struct {
	src, dst *mysqlNode
	cfg      strandCfg
	cfgPath  string
	wl       *workload
	ew       *evoWriter
	p        *strandProc
}

func evoStartMySQL(t *testing.T, name, additive string) *evoMySQLEnv {
	t.Helper()
	seed := seedFromEnv(t)
	e := &evoMySQLEnv{}
	e.src = startMySQL(t, mysqlImage("source"), "src", 1, nil)
	e.dst = startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	createSchema(t, e.src, e.dst)
	e.src.exec(t, evoMySQLTable) // target tự tạo bằng bootstrap.create_schema: auto
	e.wl = newWorkload(e.src.DB, seed)
	e.wl.Preload(t, 1000)
	evoPreload(t, e.src.DB, "app.evo", 500)
	e.wl.Start(3, 1000)
	t.Cleanup(e.wl.Stop)
	e.ew = newEvoWriter(e.src.DB, "app.evo", seed+1, 10000)
	e.ew.Start(3, 500)
	t.Cleanup(e.ew.Stop)
	e.dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	e.cfg = strandCfg{Name: name, SourceDSN: e.src.RootDSN, TargetDSN: e.dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t), SchemaAdditive: additive}
	e.cfgPath = e.cfg.write(t)
	e.p = startStrand(t, e.cfgPath)
	waitSynced(t, &e.cfg, e.p, 3*time.Minute)
	return e
}

// finish: dừng tải, đợi bắt kịp, dừng Strand (exit 0) và so từng row (0 mismatch).
func (e *evoMySQLEnv) finish(t *testing.T) {
	t.Helper()
	e.ew.Stop()
	e.wl.Stop()
	waitCaughtUp(t, e.src, e.dst, &e.cfg, e.p, 3*time.Minute)
	if code := e.p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, e.p.tail())
	}
	compareFinal(t, e.src, e.dst, append(append([]string{}, appTables...), "app.evo")...)
}

// (a) ADD COLUMN (nullable, NOT NULL DEFAULT) và ADD INDEX không unique dưới tải: tự áp, pipeline không dừng.
func TestP3_E2E_SchemaEvolution_AddColumnAuto_MySQL(t *testing.T) {
	e := evoStartMySQL(t, "e2e-evo-add", "")
	e.src.exec(t, "ALTER TABLE app.evo ADD COLUMN extra INT NULL")
	e.src.exec(t, "ALTER TABLE app.evo ADD COLUMN flag TINYINT NOT NULL DEFAULT 0")
	e.ew.flags.Or(evoAddedCols)
	time.Sleep(time.Second)
	e.src.exec(t, "ALTER TABLE app.evo ADD INDEX iv (v)")
	deadline := time.Now().Add(2 * time.Minute)
	for !mysqlHasIndex(t, e.dst.DB, "app", "evo", "iv") || mysqlColType(t, e.dst.DB, "app", "evo", "flag") == "" {
		mustStayRunning(t, e.p, "additive DDL")
		if time.Now().After(deadline) {
			t.Fatalf("additive change not applied on target\n%s", e.p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	mustStayRunning(t, e.p, "additive DDL")
	time.Sleep(2 * time.Second)
	e.finish(t)
	if got := mysqlColType(t, e.dst.DB, "app", "evo", "extra"); got != "int" {
		t.Fatalf("target extra type = %q, want int", got)
	}
	if !strings.Contains(e.p.output.String(), "additive schema change applied") {
		t.Fatalf("log lacks the auto-apply line\n%s", e.p.tail())
	}
}

// (b) Nới VARCHAR và INT → BIGINT: tự áp; sau đó ghi giá trị dài/lớn tới target không lỗi.
func TestP3_E2E_SchemaEvolution_WidenAuto_MySQL(t *testing.T) {
	e := evoStartMySQL(t, "e2e-evo-widen", "")
	e.src.exec(t, "ALTER TABLE app.evo MODIFY COLUMN v VARCHAR(100) NOT NULL")
	e.src.exec(t, "ALTER TABLE app.evo MODIFY COLUMN cnt BIGINT NOT NULL DEFAULT 0")
	e.ew.flags.Or(evoWidened)
	deadline := time.Now().Add(2 * time.Minute)
	for mysqlColType(t, e.dst.DB, "app", "evo", "v") != "varchar(100)" || mysqlColType(t, e.dst.DB, "app", "evo", "cnt") != "bigint" {
		mustStayRunning(t, e.p, "widen DDL")
		if time.Now().After(deadline) {
			t.Fatalf("widen not applied on target\n%s", e.p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	mustStayRunning(t, e.p, "widen DDL")
	e.finish(t)
	var n int
	if err := e.dst.DB.QueryRow("SELECT COUNT(*) FROM app.evo WHERE CHAR_LENGTH(v) >= 60 AND cnt > 4000000000").Scan(&n); err != nil || n == 0 {
		t.Fatalf("no widened rows reached the target (n=%d, err=%v)", n, err)
	}
}

// (c) DROP COLUMN: dừng schema_change_pending, diff liệt kê, approve (dry-run rồi thật, idempotent), chạy lại đi tiếp.
func TestP3_E2E_SchemaEvolution_DropColumnApprove_MySQL(t *testing.T) {
	e := evoStartMySQL(t, "e2e-evo-drop", "")
	e.src.exec(t, "ALTER TABLE app.evo DROP COLUMN junk")
	waitExitPending(t, e.p, "DROP COLUMN")
	if !strings.Contains(e.p.output.String(), "strand schema approve") {
		t.Fatalf("message must tell the operator how to approve\n%s", e.p.tail())
	}
	if mysqlColType(t, e.dst.DB, "app", "evo", "junk") == "" {
		t.Fatal("DROP COLUMN was applied to the target without approval")
	}
	id, _ := pendingID(t, e.cfgPath, "destructive", "DROP COLUMN `junk`", "app.evo")
	// Chạy lại khi chưa duyệt: vẫn dừng, không ghi trùng bản ghi.
	if c := startStrand(t, e.cfgPath).exitCode(); c != 10 {
		t.Fatalf("restart before approve: exit %d, want 10", c)
	}
	if _, out := pendingID(t, e.cfgPath); strings.Count(out, "proposed target DDL:") != 1 {
		t.Fatalf("restart must reuse the pending record:\n%s", out)
	}
	out, code := runStrandCLI(t, time.Minute, "schema", "approve", id, "-c", e.cfgPath, "--dry-run")
	if code != 0 || !strings.Contains(out, "DROP COLUMN `junk`") {
		t.Fatalf("approve --dry-run exit %d:\n%s", code, out)
	}
	if mysqlColType(t, e.dst.DB, "app", "evo", "junk") == "" {
		t.Fatal("approve --dry-run changed the target")
	}
	out, code = runStrandCLI(t, time.Minute, "schema", "approve", id, "-c", e.cfgPath)
	if code != 0 || !strings.Contains(out, "applied") {
		t.Fatalf("approve exit %d:\n%s", code, out)
	}
	if mysqlColType(t, e.dst.DB, "app", "evo", "junk") != "" {
		t.Fatal("approve did not drop the column on the target")
	}
	out, code = runStrandCLI(t, time.Minute, "schema", "approve", id, "-c", e.cfgPath)
	if code != 0 || !strings.Contains(out, "already applied") {
		t.Fatalf("second approve must be a no-op, exit %d:\n%s", code, out)
	}
	if out, _ = runStrandCLI(t, time.Minute, "schema", "diff", "-c", e.cfgPath); !strings.Contains(out, "No pending") {
		t.Fatalf("nothing should be pending after approve:\n%s", out)
	}
	e.p = startStrand(t, e.cfgPath)
	waitSynced(t, &e.cfg, e.p, 3*time.Minute)
	e.finish(t)
}

// (d) additive: pause — ngay cả ADD COLUMN cũng dừng chờ duyệt.
func TestP3_E2E_SchemaEvolution_AdditivePause_MySQL(t *testing.T) {
	e := evoStartMySQL(t, "e2e-evo-pause", "pause")
	e.src.exec(t, "ALTER TABLE app.evo ADD COLUMN extra INT NULL")
	e.src.exec(t, "ALTER TABLE app.evo ADD COLUMN flag TINYINT NOT NULL DEFAULT 0")
	waitExitPending(t, e.p, "additive with pause")
	if mysqlColType(t, e.dst.DB, "app", "evo", "extra") != "" {
		t.Fatal("additive: pause must not touch the target")
	}
	id, _ := pendingID(t, e.cfgPath, "additive", "ADD COLUMN `extra`")
	if out, code := runStrandCLI(t, time.Minute, "schema", "approve", id, "-c", e.cfgPath); code != 0 {
		t.Fatalf("approve exit %d:\n%s", code, out)
	}
	e.ew.flags.Or(evoAddedCols)
	e.p = startStrand(t, e.cfgPath)
	waitSynced(t, &e.cfg, e.p, 3*time.Minute)
	e.finish(t)
}

// --------------------------------------------------------------------------
// MySQL → PostgreSQL
// --------------------------------------------------------------------------

const evoPGSourceTable = `CREATE TABLE evo.t (
	id BIGINT PRIMARY KEY,
	ver BIGINT UNSIGNED NOT NULL,
	v VARCHAR(20) NOT NULL,
	cnt INT NOT NULL DEFAULT 0,
	junk VARCHAR(10) NULL)`

type evoPGEnv struct {
	src     *mysqlNode
	dst     *pgNode
	cfg     strandCfg
	cfgPath string
	ew      *evoWriter
	p       *strandProc
}

func evoStartPG(t *testing.T, name string) *evoPGEnv {
	t.Helper()
	seed := seedFromEnv(t)
	e := &evoPGEnv{}
	e.src = startMySQL(t, mysqlImage("source"), "src", 1, nil)
	e.dst = startPG(t, "pgdst")
	e.src.exec(t, "CREATE DATABASE evo", evoPGSourceTable)
	evoPreload(t, e.src.DB, "evo.t", 500)
	e.ew = newEvoWriter(e.src.DB, "evo.t", seed, 10000)
	e.ew.Start(3, 500)
	t.Cleanup(e.ew.Stop)
	e.cfg = strandCfg{Name: name, TargetKind: "postgres", SourceDSN: e.src.RootDSN, TargetDSN: e.dst.DSN,
		Include: []string{"evo.*"}, Dir: testDir(t)}
	e.cfgPath = e.cfg.write(t)
	e.p = startStrand(t, e.cfgPath)
	waitSynced(t, &e.cfg, e.p, 3*time.Minute)
	return e
}

func (e *evoPGEnv) finish(t *testing.T, cols ...string) {
	t.Helper()
	e.ew.Stop()
	waitMySQLPGCaughtUp(t, e.src, e.dst, &e.cfg, e.p, 3*time.Minute)
	if code := e.p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, e.p.tail())
	}
	my, pg := make([]string, len(cols)), make([]string, len(cols))
	for i, c := range cols {
		my[i], pg[i] = "CAST("+c+" AS CHAR)", c+"::text"
	}
	s := dumpCanon(t, e.src.DB, "SELECT "+strings.Join(my, ", ")+" FROM evo.t", -1)
	d := dumpCanon(t, e.dst.DB, "SELECT "+strings.Join(pg, ", ")+" FROM evo.t", -1)
	if len(s) == 0 || !reflect.DeepEqual(s, d) {
		t.Fatalf("final state mismatch: source %d rows, target %d rows", len(s), len(d))
	}
	t.Logf("final state: %d rows identical (columns %v)", len(s), cols)
}

func pgColType(t *testing.T, db *sql.DB, schema, table, col string) string {
	t.Helper()
	var typ string
	err := db.QueryRow(`SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
		JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND a.attname=$3 AND NOT a.attisdropped`, schema, table, col).Scan(&typ)
	if err == sql.ErrNoRows {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return typ
}

// (a) MySQL→PG: ADD COLUMN, nới VARCHAR/INT, ADD INDEX tự áp lên PG.
func TestP3_E2E_SchemaEvolution_AdditiveAuto_MySQLToPG(t *testing.T) {
	e := evoStartPG(t, "e2e-evo-pg-add")
	e.src.exec(t, "ALTER TABLE evo.t ADD COLUMN extra INT NULL")
	e.src.exec(t, "ALTER TABLE evo.t ADD COLUMN flag TINYINT NOT NULL DEFAULT 0")
	e.ew.flags.Or(evoAddedCols)
	time.Sleep(time.Second)
	e.src.exec(t, "ALTER TABLE evo.t ADD INDEX iv (v)")
	e.src.exec(t, "ALTER TABLE evo.t MODIFY COLUMN v VARCHAR(100) NOT NULL")
	e.src.exec(t, "ALTER TABLE evo.t MODIFY COLUMN cnt BIGINT NOT NULL DEFAULT 0")
	e.ew.flags.Or(evoWidened)
	deadline := time.Now().Add(2 * time.Minute)
	for pgColType(t, e.dst.DB, "evo", "t", "v") != "character varying(100)" || pgColType(t, e.dst.DB, "evo", "t", "cnt") != "bigint" {
		mustStayRunning(t, e.p, "additive DDL to PG")
		if time.Now().After(deadline) {
			t.Fatalf("additive change not applied on PG target\n%s", e.p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	mustStayRunning(t, e.p, "additive DDL to PG")
	e.finish(t, "id", "ver", "v", "cnt", "extra", "flag")
	var n int
	if err := e.dst.DB.QueryRow(`SELECT count(*) FROM pg_indexes WHERE schemaname='evo' AND tablename='t' AND indexname='t_iv'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("index t_iv missing on PG target (n=%d, err=%v)", n, err)
	}
}

// (c) MySQL→PG: DROP COLUMN dừng chờ duyệt; approve áp lên PG; chạy lại đi tiếp.
func TestP3_E2E_SchemaEvolution_DropColumnApprove_MySQLToPG(t *testing.T) {
	e := evoStartPG(t, "e2e-evo-pg-drop")
	e.src.exec(t, "ALTER TABLE evo.t DROP COLUMN junk")
	waitExitPending(t, e.p, "DROP COLUMN to PG")
	if pgColType(t, e.dst.DB, "evo", "t", "junk") == "" {
		t.Fatal("DROP COLUMN was applied to the PG target without approval")
	}
	id, _ := pendingID(t, e.cfgPath, "destructive", `DROP COLUMN "junk"`)
	out, code := runStrandCLI(t, time.Minute, "schema", "approve", id, "-c", e.cfgPath)
	if code != 0 || !strings.Contains(out, "applied") {
		t.Fatalf("approve exit %d:\n%s", code, out)
	}
	if pgColType(t, e.dst.DB, "evo", "t", "junk") != "" {
		t.Fatal("approve did not drop the column on PG")
	}
	if out, code = runStrandCLI(t, time.Minute, "schema", "approve", id, "-c", e.cfgPath); code != 0 || !strings.Contains(out, "already applied") {
		t.Fatalf("second approve must be a no-op, exit %d:\n%s", code, out)
	}
	e.p = startStrand(t, e.cfgPath)
	waitSynced(t, &e.cfg, e.p, 3*time.Minute)
	e.finish(t, "id", "ver", "v", "cnt")
}
