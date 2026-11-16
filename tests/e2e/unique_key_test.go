package e2e

// Bảng không có PRIMARY KEY nhưng có UNIQUE index NOT NULL ("primary key equivalent" / REPLICA IDENTITY USING
// INDEX ở PG) làm khóa hàng, và gợi ý thêm khóa cho bảng không có khóa nào (internal/rowkey).
//
// Chạy: STRAND_E2E_COMMIT_ORDER=strict|relaxed go test ./tests/e2e -run 'TestUK_E2E' -count=1
//   TestUK_E2E_MySQLToMySQL_UniqueKey
//   TestUK_E2E_MariaDBToMySQL_UniqueKey
//   TestUK_E2E_MySQLToPG_UniqueKey
//   TestUK_E2E_PGToPG_UniqueKeyReplicaIdentity
//   TestUK_E2E_NoKeyValidateSuggestsThenInvisibleKeySyncs_MariaDB
//   TestUK_E2E_NoKeyValidateSuggestsThenInvisibleKeySyncs_MySQL
//   TestUK_E2E_TargetWithoutMatchingUniqueIndexRejected_MySQL
//   TestUK_E2E_TargetWithoutMatchingUniqueIndexRejected_PG

import (
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
)

// ukBind đổi các "?" thành $1, $2... cho PostgreSQL.
func ukBind(pg bool, q string) string {
	if !pg {
		return q
	}
	var b strings.Builder
	n := 0
	for _, r := range q {
		if r == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// ukWorkload: INSERT / UPDATE (không đổi khóa) / UPDATE đổi khóa / DELETE đồng thời trên bảng (k, a, v, note)
// với k là khóa duy nhất (không phải PK). Lỗi trùng khóa phía client bị bỏ qua.
type ukWorkload struct {
	db    *sql.DB
	pg    bool
	table string
	span  int
	ops   atomic.Int64
	moves atomic.Int64
	stop  atomic.Bool
	wg    sync.WaitGroup
}

func (w *ukWorkload) exec(q string, args ...any) (int64, error) {
	res, err := w.db.Exec(ukBind(w.pg, q), args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (w *ukWorkload) Start(clients int, seed int64) {
	for c := 0; c < clients; c++ {
		w.wg.Add(1)
		go func(s int64) {
			defer w.wg.Done()
			r := rand.New(rand.NewSource(s))
			for !w.stop.Load() {
				k := 1 + r.Intn(w.span)
				var err error
				switch r.Intn(5) {
				case 0, 1:
					ins := "INSERT IGNORE INTO " + w.table + " (k, a, v, note) VALUES (?, ?, ?, ?)"
					if w.pg {
						ins = "INSERT INTO " + w.table + " (k, a, v, note) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING"
					}
					var note any
					if r.Intn(3) > 0 {
						note = fmt.Sprintf("n%d", r.Intn(1000))
					}
					_, err = w.exec(ins, k, fmt.Sprintf("a%d", k), r.Intn(100), note)
				case 2:
					_, err = w.exec("UPDATE "+w.table+" SET v = v + 1, note = ? WHERE k = ?", fmt.Sprintf("u%d", r.Intn(1000)), k)
				case 3:
					upd := "UPDATE IGNORE " + w.table + " SET k = ? WHERE k = ?"
					if w.pg {
						upd = "UPDATE " + w.table + " SET k = ? WHERE k = ?"
					}
					var n int64
					n, err = w.exec(upd, w.span+1+r.Intn(w.span), k) // key moves out of the preload range and back over time
					if err == nil && n == 1 {
						w.moves.Add(1)
					}
					if err == nil {
						_, err = w.exec(upd, k, w.span+1+r.Intn(w.span))
					}
				default:
					_, err = w.exec("DELETE FROM "+w.table+" WHERE k = ?", k)
				}
				if err == nil {
					w.ops.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}(seed + int64(c))
	}
}

func (w *ukWorkload) Stop() { w.stop.Store(true); w.wg.Wait() }

// ukPreload nạp n hàng (k = 1..n) vào bảng (k, a, v, note) của db.
func ukPreload(t *testing.T, db *sql.DB, pg bool, table string, n int) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if _, err := tx.Exec(ukBind(pg, "INSERT INTO "+table+" (k, a, v, note) VALUES (?, ?, ?, ?)"), i, fmt.Sprintf("a%d", i), i%100, fmt.Sprintf("n%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// ukDump: các hàng của truy vấn q thành tập chuỗi (NULL = "<NULL>"), khóa theo cột đầu.
func ukDump(db *sql.DB, q string) (map[string]string, error) {
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := map[string]string{}
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
		out[parts[0]] = strings.Join(parts, "|")
	}
	return out, rows.Err()
}

// ukDiff so hai truy vấn (cột đầu là khóa); trả mô tả lệch ("" = giống hệt).
func ukDiff(srcDB *sql.DB, srcQ string, dstDB *sql.DB, dstQ string) string {
	s, err := ukDump(srcDB, srcQ)
	if err != nil {
		return "source: " + err.Error()
	}
	d, err := ukDump(dstDB, dstQ)
	if err != nil {
		return "target: " + err.Error()
	}
	if reflect.DeepEqual(s, d) {
		return ""
	}
	var bad []string
	for k, sv := range s {
		if dv, ok := d[k]; !ok {
			bad = append(bad, "missing on target: "+sv)
		} else if dv != sv {
			bad = append(bad, fmt.Sprintf("differs: src %s dst %s", sv, dv))
		}
	}
	for k, dv := range d {
		if _, ok := s[k]; !ok {
			bad = append(bad, "extra on target: "+dv)
		}
	}
	sort.Strings(bad)
	if len(bad) > 10 {
		bad = bad[:10]
	}
	return fmt.Sprintf("%d src rows, %d dst rows:\n%s", len(s), len(d), strings.Join(bad, "\n"))
}

// ukWaitEqual chờ diff() rỗng (target bắt kịp) tối đa timeout; strand thoát giữa chừng là lỗi.
func ukWaitEqual(t *testing.T, p *strandProc, timeout time.Duration, diff func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var d string
	for {
		if p != nil && p.exited() {
			t.Fatalf("strand exited (code %d) before the target caught up\n%s", p.exitCode(), p.tail())
		}
		if d = diff(); d == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("target not equal to source after %s:\n%s\n%s", timeout, d, p.tail())
		}
		time.Sleep(500 * time.Millisecond)
	}
}

const ukMySQLTables = `
	CREATE TABLE app.uk1 (k INT NOT NULL, a VARCHAR(20) NOT NULL, v INT NOT NULL, note VARCHAR(30) NULL,
		UNIQUE KEY uk_k (k), UNIQUE KEY a_nullable_note (note)) ENGINE=InnoDB;`

// Bảng uk1: hai unique index — a_nullable_note (cột NULL, không đủ điều kiện) và uk_k (NOT NULL): khóa phải là uk_k.
// Bảng uk2: unique 2 cột (a, b) NOT NULL, không PK: khóa tổng quát nhiều cột (snapshot theo tuple).
const ukMySQLTable2 = `CREATE TABLE app.uk2 (a INT NOT NULL, b VARCHAR(16) NOT NULL, v INT NOT NULL, UNIQUE KEY uk_ab (a, b)) ENGINE=InnoDB`

// ukRunMySQLSource: chạy pipeline từ nguồn MySQL/MariaDB (src) sang đích (dst: MySQL hoặc PG) với bảng
// unique-key dưới tải (INSERT/UPDATE/UPDATE đổi khóa/DELETE) trong lúc snapshot logic; cuối cùng so hai phía.
func ukRunMySQLSource(t *testing.T, src *mysqlNode, srcDSN string, dstMy *mysqlNode, dstDSN string, dstPG *pgNode) {
	t.Helper()
	pgTarget := dstPG != nil
	src.exec(t, ukMySQLTables, ukMySQLTable2)
	ukPreload(t, src.DB, false, "app.uk1", 3000)
	for i := 1; i <= 500; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.uk2 VALUES (%d, 'x%d', %d)", i%50, i, i))
	}
	targetDSN := dstDSN
	if pgTarget {
		targetDSN = dstPG.DSN
	}
	cfg := strandCfg{Name: "e2e-uk", SourceDSN: srcDSN, TargetDSN: targetDSN, Include: []string{"app.uk1", "app.uk2"},
		Dir: testDir(t), CreateSchema: "auto", ChunkRows: "100", SourceIOPS: 600}
	if pgTarget {
		cfg.TargetKind = "postgres"
	}
	path := cfg.write(t)
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 0 {
		t.Fatalf("validate: exit %d\n%s", code, out)
	}
	for _, want := range []string{"table app.uk1: no primary key, row key is UNIQUE uk_k (k)", "UNIQUE uk_ab (a, b)"} {
		if !strings.Contains(out, want) {
			t.Fatalf("validate output lacks %q:\n%s", want, out)
		}
	}

	wl := &ukWorkload{db: src.DB, table: "app.uk1", span: 3000}
	wl.Start(4, 1)
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 5*time.Minute)
	time.Sleep(5 * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops, %d key moves", wl.ops.Load(), wl.moves.Load())
	// uk2 cũng nhận thay đổi trong lúc stream.
	src.exec(t, "UPDATE app.uk2 SET v = v + 1 WHERE a < 10", "DELETE FROM app.uk2 WHERE a = 11", "UPDATE app.uk2 SET b = CONCAT(b, 'm') WHERE a = 12")

	dstDB := (*sql.DB)(nil)
	if pgTarget {
		dstDB = dstPG.DB
	} else {
		dstDB = dstMy.DB
	}
	diff := func() string {
		if d := ukDiff(src.DB, "SELECT k, a, v, note FROM app.uk1", dstDB, "SELECT k, a, v, note FROM app.uk1"); d != "" {
			return "uk1: " + d
		}
		if d := ukDiff(src.DB, "SELECT CONCAT(a,'/',b), a, b, v FROM app.uk2", dstDB, concatSQL(pgTarget, "app.uk2")); d != "" {
			return "uk2: " + d
		}
		return ""
	}
	ukWaitEqual(t, p, 3*time.Minute, diff)
	if !strings.Contains(p.output.String(), "using a NOT NULL unique index as the row key") {
		t.Fatalf("log does not say which key is used\n%s", p.tail())
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	if d := diff(); d != "" {
		t.Fatalf("final state differs: %s", d)
	}
	// verify --mode full theo khóa unique (checksum theo khóa), 0 lệch.
	for _, tb := range []string{"app.uk1", "app.uk2"} {
		out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--table", tb, "--mode", "full")
		if code != 0 {
			t.Fatalf("verify --mode full %s: exit %d\n%s", tb, code, out)
		}
	}
	if !pgTarget {
		// Bảng đích do schema init tạo phải có unique index khớp khóa (upsert dựa vào đó).
		var n int
		if err := dstMy.DB.QueryRow("SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='app' AND TABLE_NAME='uk1' AND INDEX_NAME='uk_k' AND NON_UNIQUE=0").Scan(&n); err != nil || n != 1 {
			t.Fatalf("target uk1 lacks unique index uk_k (n=%d err=%v)", n, err)
		}
	} else {
		var n int
		if err := dstPG.DB.QueryRow(`SELECT COUNT(*) FROM pg_indexes WHERE schemaname='app' AND tablename='uk1' AND indexdef LIKE 'CREATE UNIQUE INDEX%(k)'`).Scan(&n); err != nil || n != 1 {
			t.Fatalf("PG target uk1 lacks a unique index on (k) (n=%d err=%v)", n, err)
		}
	}
}

func concatSQL(pg bool, table string) string {
	if pg {
		return "SELECT a::text || '/' || b, a, b, v FROM " + table
	}
	return "SELECT CONCAT(a,'/',b), a, b, v FROM " + table
}

func TestUK_E2E_MySQLToMySQL_UniqueKey(t *testing.T) {
	src, dst := newPair(t)
	ukRunMySQLSource(t, src, src.RootDSN, dst, dst.RootDSN+"strand", nil)
}

func TestUK_E2E_MariaDBToMySQL_UniqueKey(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app")
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	ukRunMySQLSource(t, src, fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr), dst, fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), nil)
}

func TestUK_E2E_MySQLToPG_UniqueKey(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app")
	ukRunMySQLSource(t, src, src.RootDSN, nil, "", dst)
}

// PG→PG: bảng không PK, unique index NOT NULL. Chưa có REPLICA IDENTITY thì validate từ chối (exit 3) và in đúng
// lệnh ALTER ... REPLICA IDENTITY USING INDEX; sau khi áp, snapshot logic + stream (UPDATE đổi khóa) giống hệt.
func TestUK_E2E_PGToPG_UniqueKeyReplicaIdentity(t *testing.T) {
	src := startPG(t, "pgsrc")
	dst := startPG(t, "pgdst")
	src.exec(t, "CREATE SCHEMA app",
		`CREATE TABLE app.uk1 (k int NOT NULL, a text NOT NULL, v int NOT NULL, note text)`,
		`CREATE UNIQUE INDEX uk1_nullable ON app.uk1 (note)`,
		`CREATE UNIQUE INDEX uk1_k ON app.uk1 (k)`)
	ukPreload(t, src.DB, true, "app.uk1", 3000)
	src.exec(t, "CREATE PUBLICATION uk_pub FOR TABLE app.uk1 WITH (publish = 'insert, update, delete')")
	cfg := strandCfg{Name: "e2e-uk-pg", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: ", slot: uk_slot, publication: uk_pub, create_slot: true", Include: []string{"app.uk1"},
		Dir: testDir(t), CreateSchema: "auto", ChunkRows: "100", SourceIOPS: 20000}
	path := cfg.write(t)

	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 3 || !strings.Contains(out, "missing_pk") || !strings.Contains(out, `ALTER TABLE "app"."uk1" REPLICA IDENTITY USING INDEX "uk1_k";`) {
		t.Fatalf("validate without replica identity: exit %d, want 3 with the exact ALTER line\n%s", code, out)
	}
	src.exec(t, `ALTER TABLE app.uk1 REPLICA IDENTITY USING INDEX uk1_k`)
	out, code = runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 0 || !strings.Contains(out, "row key is UNIQUE uk1_k (k)") {
		t.Fatalf("validate after ALTER: exit %d\n%s", code, out)
	}

	wl := &ukWorkload{db: src.DB, pg: true, table: "app.uk1", span: 3000}
	wl.Start(4, 7)
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 5*time.Minute)
	time.Sleep(5 * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops, %d key moves", wl.ops.Load(), wl.moves.Load())
	q := "SELECT k, a, v, note FROM app.uk1"
	ukWaitEqual(t, p, 3*time.Minute, func() string { return ukDiff(src.DB, q, dst.DB, q) })
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	if d := ukDiff(src.DB, q, dst.DB, q); d != "" {
		t.Fatalf("final state differs: %s", d)
	}
	out, code = runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--table", "app.uk1", "--mode", "full")
	if code != 0 {
		t.Fatalf("verify --mode full: exit %d\n%s", code, out)
	}
	var n int
	if err := dst.DB.QueryRow(`SELECT COUNT(*) FROM pg_indexes WHERE schemaname='app' AND tablename='uk1' AND indexdef LIKE 'CREATE UNIQUE INDEX%(k)'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("target lacks the unique index on (k): n=%d err=%v", n, err)
	}
}

// ukFirstAlter: dòng "ALTER TABLE ..." đầu tiên của đầu ra validate (câu gợi ý thêm khóa).
func ukFirstAlter(out string) string {
	for _, l := range strings.Split(out, "\n") {
		if l = strings.TrimSpace(l); strings.HasPrefix(l, "ALTER TABLE ") {
			return l
		}
	}
	return ""
}

// Bảng không có khóa nào: validate exit 3 kèm câu ALTER thêm cột khóa INVISIBLE; áp đúng câu đó ở source,
// validate qua, pipeline đồng bộ, cột INVISIBLE có ở đích và dữ liệu (liệt kê cột tường minh) giống hệt.
func ukNoKeyThenInvisible(t *testing.T, src *mysqlNode, srcDSN string, dst *mysqlNode, dstDSN string, wantCol string) {
	t.Helper()
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app",
		"CREATE TABLE app.nokey (a INT NOT NULL, b VARCHAR(20)) ENGINE=InnoDB",
		"INSERT INTO app.nokey VALUES (1,'x'),(1,'x'),(2,NULL),(3,'z')")
	cfg := strandCfg{Name: "e2e-uk-nokey", SourceDSN: srcDSN, TargetDSN: dstDSN, Include: []string{"app.nokey"}, Dir: testDir(t), CreateSchema: "auto"}
	path := cfg.write(t)
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	alter := ukFirstAlter(out)
	if code != 3 || !strings.Contains(out, "missing_pk") || alter == "" || !strings.Contains(alter, "INVISIBLE") || !strings.Contains(alter, wantCol) {
		t.Fatalf("validate of a keyless table: exit %d, want 3 with an invisible-key ALTER (%s)\n%s", code, wantCol, out)
	}
	out2, code2 := runStrandCLI(t, time.Minute, "plan", "-c", path)
	if !strings.Contains(out2, "NONE (missing_pk)") || !strings.Contains(out2, alter) {
		t.Fatalf("plan must show the same suggestion (exit %d)\n%s", code2, out2)
	}
	src.exec(t, strings.TrimSuffix(alter, ";"))
	if out, code = runStrandCLI(t, time.Minute, "validate", "-c", path); code != 0 {
		t.Fatalf("validate after applying the suggestion: exit %d\n%s", code, out)
	}
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)
	src.exec(t, "INSERT INTO app.nokey (a, b) VALUES (4,'w'),(4,'w')", "UPDATE app.nokey SET b = 'y' WHERE a = 3", "DELETE FROM app.nokey WHERE a = 2")
	q := fmt.Sprintf("SELECT %s, a, b FROM app.nokey", wantCol) // cột INVISIBLE phải nêu tên: SELECT * không thấy nó
	ukWaitEqual(t, p, 2*time.Minute, func() string { return ukDiff(src.DB, q, dst.DB, q) })
	var n int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='app' AND TABLE_NAME='nokey' AND COLUMN_NAME=?", wantCol).Scan(&n); err != nil || n != 1 {
		t.Fatalf("target lacks column %s (n=%d err=%v)", wantCol, n, err)
	}
	var cnt int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM (SELECT * FROM app.nokey) x").Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	var visible int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='app' AND TABLE_NAME='nokey' AND (EXTRA NOT LIKE '%INVISIBLE%' OR EXTRA IS NULL)").Scan(&visible); err == nil && visible > 2 {
		t.Fatalf("the surrogate key must stay INVISIBLE on the target (visible columns: %d)", visible)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	if out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--table", "app.nokey", "--mode", "full"); code != 0 {
		t.Fatalf("verify --mode full: exit %d\n%s", code, out)
	}
}

func TestUK_E2E_NoKeyValidateSuggestsThenInvisibleKeySyncs_MariaDB(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	mdbSourceUsers(t, src)
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app")
	mdbTargetSetup(t, dst)
	ukNoKeyThenInvisible(t, src, fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr), dst, fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), "_strand_id")
}

func TestUK_E2E_NoKeyValidateSuggestsThenInvisibleKeySyncs_MySQL(t *testing.T) {
	src, dst := newPair(t)
	var major int
	if err := src.DB.QueryRow("SELECT CAST(SUBSTRING_INDEX(VERSION(), '.', 1) AS UNSIGNED)").Scan(&major); err != nil || major < 8 {
		t.Fatalf("this e2e needs MySQL 8.0.23+ as the source image (major=%d err=%v)", major, err)
	}
	ukNoKeyThenInvisible(t, src, src.RootDSN, dst, dst.RootDSN+"strand", "my_row_id")
}

// Bảng đích đã tồn tại nhưng không có unique index khớp khóa của nguồn: upsert sẽ nhân đôi hàng — validate exit 3.
func TestUK_E2E_TargetWithoutMatchingUniqueIndexRejected_MySQL(t *testing.T) {
	src, dst := newPair(t)
	src.exec(t, ukMySQLTables)
	dst.exec(t, `CREATE TABLE app.uk1 (k INT NOT NULL, a VARCHAR(20) NOT NULL, v INT NOT NULL, note VARCHAR(30) NULL) ENGINE=InnoDB`)
	cfg := strandCfg{Name: "e2e-uk-tgt", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand", Include: []string{"app.uk1"}, Dir: testDir(t)}
	path := cfg.write(t)
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 3 || !strings.Contains(out, "no matching unique index") || !strings.Contains(out, "uk_k (k)") {
		t.Fatalf("validate: exit %d, want 3 naming the missing target index\n%s", code, out)
	}
	dst.exec(t, "ALTER TABLE app.uk1 ADD UNIQUE KEY (k)")
	if out, code = runStrandCLI(t, time.Minute, "validate", "-c", path); code != 0 {
		t.Fatalf("validate after adding the target unique key: exit %d\n%s", code, out)
	}
}

func TestUK_E2E_TargetWithoutMatchingUniqueIndexRejected_PG(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t, "CREATE DATABASE IF NOT EXISTS app", ukMySQLTables)
	dst.exec(t, "CREATE SCHEMA app", `CREATE TABLE app.uk1 (k int NOT NULL, a varchar(20) NOT NULL, v int NOT NULL, note varchar(30))`)
	cfg := strandCfg{Name: "e2e-uk-tgt-pg", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN, Include: []string{"app.uk1"}, Dir: testDir(t)}
	path := cfg.write(t)
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", path)
	if code != 3 || !strings.Contains(out, "no matching unique index") {
		t.Fatalf("validate: exit %d, want 3\n%s", code, out)
	}
	dst.exec(t, "CREATE UNIQUE INDEX ON app.uk1 (k)")
	if out, code = runStrandCLI(t, time.Minute, "validate", "-c", path); code != 0 {
		t.Fatalf("validate after creating the unique index: exit %d\n%s", code, out)
	}
}
