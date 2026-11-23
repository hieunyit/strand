package e2e

// Chế độ keyless (tables.keyless: full_row, "C-lite"): bảng KHÔNG có PK/unique key, dữ liệu đầu đến từ seed /
// --start-at (target đã khớp source tại vị trí bắt đầu), UPDATE/DELETE khớp ảnh trước theo MỌI cột (NULL-safe,
// LIMIT 1) và được áp tuần tự theo thứ tự commit.
//
// Chạy: STRAND_E2E_COMMIT_ORDER=strict|relaxed go test ./tests/e2e -run 'TestKL_E2E' -count=1
//   TestKL_E2E_MySQLToMySQL_Keyless
//   TestKL_E2E_MariaDBToMySQL_Keyless
//   TestKL_E2E_MySQLToPG_Keyless
//   TestKL_E2E_PGToPG_KeylessReplicaIdentityFull
//   TestKL_E2E_LogicalSnapshotOfKeylessTableRefused
//   TestKL_E2E_DivergenceFailsClosed

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/admin"
)

const klKeylessCfg = "  keyless: full_row\n"

// klWorkload: INSERT (kể cả hàng trùng hệt), UPDATE một hàng, UPDATE nhiều hàng, DELETE một hàng, Txn nhiều câu.
type klWorkload struct {
	db    *sql.DB
	pg    bool
	table string
	ops   atomic.Int64
	stop  atomic.Bool
	wg    sync.WaitGroup
}

func (w *klWorkload) exec(q string, args ...any) error {
	_, err := w.db.Exec(ukBind(w.pg, q), args...)
	return err
}

func (w *klWorkload) one(kind string) string {
	if w.pg {
		return "ctid = (SELECT ctid FROM " + w.table + " WHERE a = ? AND b = ? LIMIT 1)"
	}
	return "a = ? AND b = ? LIMIT 1"
}

func (w *klWorkload) Start(clients int, seed int64) {
	for c := 0; c < clients; c++ {
		w.wg.Add(1)
		go func(s int64) {
			defer w.wg.Done()
			r := rand.New(rand.NewSource(s))
			for !w.stop.Load() {
				a, b := r.Intn(40), fmt.Sprintf("s%d", r.Intn(5))
				var c any
				if r.Intn(3) > 0 {
					c = r.Intn(7)
				}
				var err error
				var sv any // twins that differ only by case / trailing space / accent: byte-exact matching is required
				if r.Intn(4) > 0 {
					sv = []string{"abc", "ABC", "abc ", "Abc", "ábc"}[r.Intn(5)]
				}
				switch r.Intn(8) {
				case 6: // update exactly one twin chosen by its exact bytes
					if w.pg {
						err = w.exec("UPDATE "+w.table+" SET c = ? WHERE ctid = (SELECT ctid FROM "+w.table+" WHERE s = ? LIMIT 1)", c, sv)
					} else {
						err = w.exec("UPDATE "+w.table+" SET c = ? WHERE BINARY s = BINARY ? LIMIT 1", c, sv)
					}
				case 7: // delete exactly one twin
					if w.pg {
						err = w.exec("DELETE FROM "+w.table+" WHERE ctid = (SELECT ctid FROM "+w.table+" WHERE s = ? LIMIT 1)", sv)
					} else {
						err = w.exec("DELETE FROM "+w.table+" WHERE BINARY s = BINARY ? LIMIT 1", sv)
					}
				case 0, 1:
					err = w.exec("INSERT INTO "+w.table+" (a, b, c, s) VALUES (?, ?, ?, ?)", a, b, c, sv)
				case 2:
					if w.pg {
						err = w.exec("UPDATE "+w.table+" SET c = ? WHERE "+w.one(""), c, a, b)
					} else {
						err = w.exec("UPDATE "+w.table+" SET c = ? WHERE "+w.one(""), c, a, b)
					}
				case 3:
					err = w.exec("UPDATE "+w.table+" SET c = COALESCE(c, 0) + 1 WHERE a = ?", a)
				case 4:
					if w.pg {
						err = w.exec("DELETE FROM "+w.table+" WHERE "+w.one(""), a, b)
					} else {
						err = w.exec("DELETE FROM "+w.table+" WHERE "+w.one(""), a, b)
					}
				default:
					tx, e := w.db.Begin()
					if e != nil {
						continue
					}
					_, e1 := tx.Exec(ukBind(w.pg, "INSERT INTO "+w.table+" (a, b, c, s) VALUES (?, ?, ?, ?)"), a, b, c, sv)
					_, e2 := tx.Exec(ukBind(w.pg, "INSERT INTO "+w.table+" (a, b, c, s) VALUES (?, ?, ?, ?)"), a, b, c, sv) // identical twin
					if e1 != nil || e2 != nil {
						_ = tx.Rollback()
						continue
					}
					err = tx.Commit()
				}
				if err == nil {
					w.ops.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}(seed + int64(c))
	}
}

func (w *klWorkload) Stop() { w.stop.Store(true); w.wg.Wait() }

// klPreload nạp cùng một tập hàng (có hàng trùng hệt và NULL) vào db.
func klPreload(t *testing.T, db *sql.DB, pg bool, table string) {
	t.Helper()
	for i := 0; i < 400; i++ {
		var c any
		if i%4 != 0 {
			c = i % 7
		}
		var sv any
		if i%6 != 0 {
			sv = []string{"abc", "ABC", "abc ", "Abc", "ábc"}[i%5]
		}
		if _, err := db.Exec(ukBind(pg, "INSERT INTO "+table+" (a, b, c, s) VALUES (?, ?, ?, ?)"), i%40, fmt.Sprintf("s%d", i%5), c, sv); err != nil {
			t.Fatal(err)
		}
	}
}

// klMulti: các hàng của truy vấn thành danh sách chuỗi đã sắp (giữ hàng trùng: so sánh multiset).
func klMulti(db *sql.DB, q string) ([]string, error) {
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		parts := make([]string, len(cols))
		for i, v := range vals {
			parts[i] = "<NULL>"
			if v.Valid {
				parts[i] = v.String
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	sort.Strings(out)
	return out, rows.Err()
}

func klDiff(srcDB, dstDB *sql.DB, q string) string {
	s, err := klMulti(srcDB, q)
	if err != nil {
		return "source: " + err.Error()
	}
	d, err := klMulti(dstDB, q)
	if err != nil {
		return "target: " + err.Error()
	}
	if reflect.DeepEqual(s, d) {
		return ""
	}
	return fmt.Sprintf("multiset differs: %d src rows vs %d dst rows", len(s), len(d))
}

// klRun: pipeline từ nguồn (MySQL/MariaDB/PG) với bảng keyless đã seed giống hệt ở hai phía; tải đồng thời; cuối cùng
// multiset giống hệt và `verify --mode full` (số hàng + checksum) khớp; làm lệch đích một hàng thì verify báo lệch.
type klEnv struct {
	srcDB, dstDB *sql.DB
	pgSrc, pgDst bool
	cfg          strandCfg
	startAt      string
}

func klRun(t *testing.T, env klEnv) {
	t.Helper()
	cfg := env.cfg
	cfg.Bootstrap = "start_at"
	cfg.StartAt = env.startAt
	cfg.TablesExtra = klKeylessCfg
	path := cfg.write(t)

	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 0 || !strings.Contains(out, "keyless") {
		t.Fatalf("validate with tables.keyless=full_row + start_at: exit %d, want 0 with keyless warnings\n%s", code, out)
	}
	for _, want := range []string{"scans the target table", "count and an order-independent checksum", "applied serially"} {
		if !strings.Contains(out, want) {
			t.Fatalf("validate output lacks the keyless warning %q\n%s", want, out)
		}
	}

	if !env.pgSrc {
		out, code = runStrandCLI(t, time.Minute, "plan", "-c", path)
		if code != 0 || !strings.Contains(out, "full row (keyless)") || strings.Contains(out, "missing_pk") {
			t.Fatalf("plan of a seeded keyless table: exit %d, want full-row matching\n%s", code, out)
		}
	}

	wl := &klWorkload{db: env.srcDB, pg: env.pgSrc, table: "app.kl"}
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	top, err := admin.NewClient(cfg.Socket).Top(ctx)
	cancel()
	if err != nil || top.Verification == nil || top.Verification.Status != "checked" {
		t.Fatalf("expected checked post-seed evidence, got %+v, err=%v\n%s", top, err, p.tail())
	}
	if tables := top.Verification.Tables; len(tables) != 1 || tables[0].Method != "count-and-checksum" || tables[0].Position == "" {
		t.Fatalf("missing keyless verification scope or watermark: %+v", tables)
	}
	wl.Start(4, 11)
	time.Sleep(8 * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops", wl.ops.Load())
	q := "SELECT a, b, c, s FROM app.kl"
	ukWaitEqual(t, p, 3*time.Minute, func() string { return klDiff(env.srcDB, env.dstDB, q) })
	if !strings.Contains(p.output.String(), "running in keyless mode") {
		t.Fatalf("log does not mention keyless mode\n%s", p.tail())
	}
	if env.pgSrc == env.pgDst {
		p.kill9()
		if _, err := env.srcDB.Exec("INSERT INTO app.kl (a,b,c) VALUES (1010,'restart',NULL),(1010,'restart',NULL)"); err != nil {
			t.Fatal(err)
		}
		restarted := startStrand(t, path)
		defer restarted.kill9()
		waitSynced(t, &cfg, restarted, time.Minute)
		restartCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		evidence, err := admin.NewClient(cfg.Socket).Top(restartCtx)
		stop()
		if err != nil || !reflect.DeepEqual(evidence.Verification, top.Verification) {
			t.Fatalf("post-seed evidence changed after restart: %+v, err=%v", evidence, err)
		}
		ukWaitEqual(t, restarted, time.Minute, func() string { return klDiff(env.srcDB, env.dstDB, q) })
		if code := restarted.stop(t); code != 0 {
			t.Fatalf("restart exit %d\n%s", code, restarted.tail())
		}
	} else if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	if d := klDiff(env.srcDB, env.dstDB, q); d != "" {
		t.Fatalf("final state differs: %s", d)
	}
	out, code = runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--mode", "full", "--require-complete")
	if code != 0 || !strings.Contains(out, "keyless") || !strings.Contains(out, "match=true") {
		t.Fatalf("verify --mode full of the keyless table: exit %d\n%s", code, out)
	}
	// Làm lệch đích: verify phải báo lệch (exit != 0) và không sửa (repair fix bị từ chối).
	if _, err := env.dstDB.Exec(ukBind(env.pgDst, "INSERT INTO app.kl (a, b, c) VALUES (?, ?, ?)"), 999, "zz", 1); err != nil {
		t.Fatal(err)
	}
	out, code = runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--mode", "full", "--require-complete")
	if code == 0 || !strings.Contains(out, "match=false") {
		t.Fatalf("verify must report the divergence: exit %d\n%s", code, out)
	}
	out, code = runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--table", "app.kl", "--mode", "full", "--repair", "fix")
	if code == 0 || !(strings.Contains(out, "not supported for keyless") || strings.Contains(out, "same engine")) {
		t.Fatalf("repair of a keyless table must be refused with a clear message: exit %d\n%s", code, out)
	}
}

const klMySQLDDL = "CREATE TABLE app.kl (a INT NOT NULL, b VARCHAR(20) NOT NULL, c INT NULL, s VARCHAR(20) NULL) ENGINE=InnoDB"
const klPGDDL = "CREATE TABLE app.kl (a int NOT NULL, b varchar(20) NOT NULL, c int, s varchar(20))"

func TestKL_E2E_MySQLToMySQL_Keyless(t *testing.T) {
	src, dst := newPair(t)
	src.exec(t, klMySQLDDL)
	dst.exec(t, klMySQLDDL)
	klPreload(t, src.DB, false, "app.kl")
	klPreload(t, dst.DB, false, "app.kl")
	start := strings.ReplaceAll(src.gtidExecuted(t), "\n", "")
	cfg := strandCfg{Name: "e2e-kl", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand", Include: []string{"app.kl"}, Dir: testDir(t)}
	klRun(t, klEnv{srcDB: src.DB, dstDB: dst.DB, cfg: cfg, startAt: start})
}

func TestKL_E2E_MariaDBToMySQL_Keyless(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app", klMySQLDDL)
	dst.exec(t, klMySQLDDL)
	klPreload(t, src.DB, false, "app.kl")
	klPreload(t, dst.DB, false, "app.kl")
	file, pos := src.masterPos(t)
	cfg := strandCfg{Name: "e2e-kl-mdb", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.kl"}, Dir: testDir(t)}
	// A rejected logical bootstrap must let the same pipeline retry from a restored copy.
	logical := cfg
	logical.TablesExtra = klKeylessCfg
	path := logical.write(t)
	for _, command := range []string{"validate", "plan", "sync"} {
		out, code := runStrandCLI(t, time.Minute, command, "-c", path)
		if code != 3 || !strings.Contains(out, "missing_pk") || !strings.Contains(out, "logical snapshot") {
			t.Fatalf("%s with a keyless logical snapshot: exit %d, want missing_pk\n%s", command, code, out)
		}
	}
	klRun(t, klEnv{srcDB: src.DB, dstDB: dst.DB, cfg: cfg, startAt: fmt.Sprintf("%s:%d", file, pos)})
}

func TestKL_E2E_MySQLToPG_Keyless(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app", klMySQLDDL)
	dst.exec(t, "CREATE SCHEMA app", klPGDDL)
	klPreload(t, src.DB, false, "app.kl")
	klPreload(t, dst.DB, true, "app.kl")
	start := strings.ReplaceAll(src.gtidExecuted(t), "\n", "")
	cfg := strandCfg{Name: "e2e-kl-pg", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN, Include: []string{"app.kl"}, Dir: testDir(t)}
	klRun(t, klEnv{srcDB: src.DB, dstDB: dst.DB, pgDst: true, cfg: cfg, startAt: start})
}

// PG→PG: bảng không khóa cần REPLICA IDENTITY FULL; mặc định (DEFAULT) validate từ chối và in lệnh ALTER.
func TestKL_E2E_PGToPG_KeylessReplicaIdentityFull(t *testing.T) {
	src := startPG(t, "pgsrc")
	dst := startPG(t, "pgdst")
	src.exec(t, "CREATE SCHEMA app", klPGDDL)
	dst.exec(t, "CREATE SCHEMA app", klPGDDL)
	klPreload(t, src.DB, true, "app.kl")
	klPreload(t, dst.DB, true, "app.kl")
	src.exec(t, "CREATE PUBLICATION kl_pub FOR TABLE app.kl WITH (publish = 'insert, update, delete')")
	var lsn string
	if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot('kl_slot', 'pgoutput', false, true)").Scan(&lsn); err != nil {
		t.Fatal(err)
	}
	cfg := strandCfg{Name: "e2e-kl-pgpg", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: ", slot: kl_slot, publication: kl_pub", Include: []string{"app.kl"}, Dir: testDir(t), SourceIOPS: 20000,
		Bootstrap: "start_at", TablesExtra: klKeylessCfg}
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t))
	if code != 3 || !strings.Contains(out, "missing_pk") || !strings.Contains(out, `REPLICA IDENTITY FULL`) {
		t.Fatalf("validate with REPLICA IDENTITY DEFAULT: exit %d, want 3 naming REPLICA IDENTITY FULL\n%s", code, out)
	}
	src.exec(t, "ALTER TABLE app.kl REPLICA IDENTITY FULL")
	klRun(t, klEnv{srcDB: src.DB, dstDB: dst.DB, pgSrc: true, pgDst: true, cfg: cfg, startAt: lsn})
}

// Snapshot logic của bảng keyless bị từ chối (cần khóa để chia và đối soát range): validate và sync exit 3 missing_pk,
// kèm gợi ý thêm khóa và nói rõ chế độ keyless chỉ dùng với seed.
func TestKL_E2E_LogicalSnapshotOfKeylessTableRefused(t *testing.T) {
	src, dst := newPair(t)
	src.exec(t, klMySQLDDL)
	klPreload(t, src.DB, false, "app.kl")
	cfg := strandCfg{Name: "e2e-kl-refuse", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand", Include: []string{"app.kl"},
		Dir: testDir(t), TablesExtra: klKeylessCfg} // bootstrap logical (mặc định)
	path := cfg.write(t)
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 3 || !strings.Contains(out, "missing_pk") || !strings.Contains(out, "seed") || !strings.Contains(out, "ALTER TABLE `app`.`kl` ADD COLUMN") {
		t.Fatalf("validate: exit %d, want 3 missing_pk with the seed note and the ALTER suggestion\n%s", code, out)
	}
	p := startStrand(t, path)
	select {
	case <-p.done:
	case <-time.After(2 * time.Minute):
		p.kill9()
		t.Fatalf("sync did not stop\n%s", p.tail())
	}
	if p.exitCode() != 3 || !strings.Contains(p.output.String(), "missing_pk") {
		t.Fatalf("sync: exit %d, want 3 missing_pk\n%s", p.exitCode(), p.tail())
	}
}

// UPDATE/DELETE không khớp hàng nào ở target (target lệch) → dừng với keyless_row_missing (exit 5), không bỏ qua.
func TestKL_E2E_DivergenceFailsClosed(t *testing.T) {
	src, dst := newPair(t)
	src.exec(t, klMySQLDDL)
	dst.exec(t, klMySQLDDL)
	for _, db := range []*mysqlNode{src, dst} {
		db.exec(t, "CREATE TABLE app.control (id INT PRIMARY KEY)")
	}
	src.exec(t, "INSERT INTO app.kl (a,b,c) VALUES (1,'x',1),(2,'y',2)")
	dst.exec(t, "INSERT INTO app.kl (a,b,c) VALUES (1,'x',1),(2,'y',2)")
	start := strings.ReplaceAll(src.gtidExecuted(t), "\n", "")
	cfg := strandCfg{Name: "e2e-kl-div", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand", Include: []string{"app.kl", "app.control"},
		Dir: testDir(t), Bootstrap: "start_at", TablesExtra: klKeylessCfg}
	p := startStrand(t, cfg.write(t), "--start-at", start)
	waitSynced(t, &cfg, p, 3*time.Minute)
	dst.exec(t, "DELETE FROM app.kl WHERE a = 2 LIMIT 1")
	readCheckpoint := func() string {
		t.Helper()
		var applied string
		if err := dst.DB.QueryRow("SELECT applied FROM strand._strand_checkpoint WHERE pipeline_id=?", cfg.Name).Scan(&applied); err != nil {
			t.Fatal(err)
		}
		return applied
	}
	before := readCheckpoint()
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("INSERT INTO app.control VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("DELETE FROM app.kl WHERE a = 2 LIMIT 1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(2 * time.Minute):
		p.kill9()
		t.Fatalf("strand did not stop on the diverged row\n%s", p.tail())
	}
	if p.exitCode() != 5 || !strings.Contains(p.output.String(), "keyless_row_missing") {
		t.Fatalf("exit %d, want 5 keyless_row_missing\n%s", p.exitCode(), p.tail())
	}
	var count int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.control").Scan(&count); err != nil || count != 0 {
		t.Fatalf("mixed transaction was partially applied: count=%d err=%v", count, err)
	}
	if after := readCheckpoint(); after != before {
		t.Fatalf("checkpoint advanced despite rollback: %q → %q", before, after)
	}
}

func TestKL_E2E_PostSeedDetectsKeylessDrift(t *testing.T) {
	src, dst := newPair(t)
	for _, db := range []*mysqlNode{src, dst} {
		db.exec(t, klMySQLDDL)
		db.exec(t, "CREATE TABLE app.control (id INT PRIMARY KEY, v INT)", "INSERT INTO app.control VALUES (1, 1)")
		db.exec(t, "INSERT INTO app.kl (a,b,c) VALUES (1,'x',NULL),(1,'x',NULL)")
	}
	dst.exec(t, "UPDATE app.kl SET b = 'y' LIMIT 1")
	start := strings.ReplaceAll(src.gtidExecuted(t), "\n", "")
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprintf("mixed=%t", mixed), func(t *testing.T) {
			tables := []string{"app.kl"}
			if mixed {
				tables = append(tables, "app.control")
			}
			cfg := strandCfg{Name: fmt.Sprintf("e2e-kl-seed-%t", mixed), SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
				Include: tables, Dir: testDir(t), Bootstrap: "start_at", TablesExtra: klKeylessCfg, PostSeedWindow: "1s"}
			p := startStrand(t, cfg.write(t), "--start-at", start)
			select {
			case <-p.done:
			case <-time.After(time.Minute):
				p.kill9()
				t.Fatalf("post-seed did not stop on mismatch\n%s", p.tail())
			}
			if p.exitCode() != 5 || !strings.Contains(p.output.String(), "keyless checksum found a mismatch") {
				t.Fatalf("exit %d, want keyless seed mismatch\n%s", p.exitCode(), p.tail())
			}
		})
	}
}

func TestKL_E2E_MissingBeforeImageFailsClosed(t *testing.T) {
	src, dst := newPair(t)
	for _, db := range []*mysqlNode{src, dst} {
		db.exec(t, "CREATE TABLE app.images (id INT, payload BLOB)",
			"INSERT INTO app.images VALUES (1, X'FF')",
			"CREATE TABLE app.marker (id INT PRIMARY KEY)")
	}
	start := strings.ReplaceAll(src.gtidExecuted(t), "\n", "")
	cfg := strandCfg{Name: "kl-partial-image", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.images", "app.marker"}, Dir: testDir(t), Bootstrap: "start_at", TablesExtra: klKeylessCfg,
		CommitOrder: "strict"}
	p := startStrand(t, cfg.write(t), "--start-at", start)
	waitSynced(t, &cfg, p, time.Minute)
	readCheckpoint := func() string {
		t.Helper()
		var applied string
		if err := dst.DB.QueryRow("SELECT applied FROM strand._strand_checkpoint WHERE pipeline_id=?", cfg.Name).Scan(&applied); err != nil {
			t.Fatal(err)
		}
		return applied
	}
	before := readCheckpoint()

	// Override the setting on a single source connection after startup validation.
	// NOBLOB omits the unchanged binary payload from the row images.
	conn, err := src.DB.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "SET SESSION binlog_row_image='NOBLOB'"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "SET SESSION binlog_row_image='FULL'")
	tx, err := conn.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("INSERT INTO app.marker VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE app.images SET id=2 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		p.kill9()
		t.Fatalf("pipeline did not reject the partial row image\n%s", p.tail())
	}
	if p.exitCode() != 3 || !strings.Contains(p.output.String(), "has a partial row image") {
		t.Fatalf("exit %d, want partial row image error\n%s", p.exitCode(), p.tail())
	}
	var id, markers int
	if err := dst.DB.QueryRow("SELECT id, (SELECT COUNT(*) FROM app.marker) FROM app.images").Scan(&id, &markers); err != nil {
		t.Fatal(err)
	}
	if after := readCheckpoint(); after != before || id != 1 || markers != 0 {
		t.Fatalf("target advanced despite rejected image: id=%d markers=%d checkpoint %q → %q", id, markers, before, after)
	}
}
