package e2e

// Nhóm test nguồn MariaDB 10.6 (chế độ vị trí binlog, không GTID) → MySQL 8.4.
// Image nguồn: STRAND_E2E_MDB_IMAGE (mặc định mariadb:10.6); đích luôn là mysql:8.4.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func mdbImage() string {
	if v := os.Getenv("STRAND_E2E_MDB_IMAGE"); v != "" {
		return v
	}
	return "mariadb:10.6"
}

// startMariaDB: nguồn MariaDB (env MARIADB_ROOT_PASSWORD, không GTID); kết nối dữ liệu dùng charset utf8mb4.
func startMariaDB(t *testing.T, alias string, serverID int) *mysqlNode {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        mdbImage(),
		ExposedPorts: []string{"3306/tcp"},
		Env:          map[string]string{"MARIADB_ROOT_PASSWORD": "dev"},
		Cmd: []string{"--log-bin=mysql-bin", fmt.Sprintf("--server-id=%d", serverID), "--binlog-format=ROW",
			"--binlog-row-image=FULL", "--max-connections=500", "--innodb-use-native-aio=0"},
		// Server tạm của entrypoint log "port: 0"; chỉ server thật mới "port: 3306".
		WaitingFor: wait.ForLog("port: 3306").WithStartupTimeout(240 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if c != nil {
		t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatalf("start %s: %v", mdbImage(), err)
	}
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "3306")
	n := &mysqlNode{c: c, Alias: alias, Addr: net.JoinHostPort(host, port.Port())}
	n.RootDSN = fmt.Sprintf("root:dev@tcp(%s)/", n.Addr)
	n.DB, err = sql.Open("mysql", n.RootDSN+"?charset=utf8mb4&parseTime=false&multiStatements=true")
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
	var ver string
	if err := n.DB.QueryRow("SELECT @@version").Scan(&ver); err != nil || !strings.Contains(ver, "MariaDB") {
		t.Fatalf("nguồn không phải MariaDB: version=%q err=%v", ver, err)
	}
	t.Logf("nguồn %s: %s", alias, ver)
	return n
}

const mdbPass = "strandpw"

// mdbSourceUsers: user strand với đúng bộ quyền tối thiểu trong tài liệu (REPLICATION SLAVE, REPLICATION
// CLIENT, SELECT, LOCK TABLES) và user app (cho promote) trên nguồn. MariaDB 10.6 bản mới ẩn
// information_schema.TABLE_CONSTRAINTS với bộ quyền này (xem TestMDB106_E2E_ValidateMinimalGrants).
func mdbSourceUsers(t *testing.T, src *mysqlNode, _ ...bool) {
	src.exec(t, "CREATE USER 'strand'@'%' IDENTIFIED BY '"+mdbPass+"'")
	src.exec(t,
		"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'strand'@'%'",
		"GRANT SELECT, LOCK TABLES ON app.* TO 'strand'@'%'",
		"CREATE USER 'appuser'@'%' IDENTIFIED BY 'apppw'",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON app.* TO 'appuser'@'%'")
}

func mdbTargetSetup(t *testing.T, dst *mysqlNode) {
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand", "CREATE DATABASE IF NOT EXISTS app",
		"CREATE USER 'strand'@'%' IDENTIFIED BY '"+mdbPass+"'",
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP ON app.* TO 'strand'@'%'",
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP ON strand.* TO 'strand'@'%'",
		"GRANT CONNECTION_ADMIN ON *.* TO 'strand'@'%'")
}

var mdbSchemaSQL = []string{
	"CREATE DATABASE IF NOT EXISTS app",
	// code: VARCHAR utf8mb4 mặc định collation utf8mb4_general_ci; meta JSON = alias LONGTEXT của MariaDB.
	`CREATE TABLE app.acct (
		code VARCHAR(40) CHARACTER SET utf8mb4 NOT NULL PRIMARY KEY,
		bal DECIMAL(20,4) NOT NULL,
		created DATETIME(3) NOT NULL,
		big BIGINT UNSIGNED NOT NULL,
		kind ENUM('new','active','closed') NOT NULL,
		meta JSON,
		bl BLOB,
		n INT NOT NULL DEFAULT 0) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE app.notes (
		id BIGINT AUTO_INCREMENT PRIMARY KEY,
		body TEXT CHARACTER SET latin1,
		tag VARCHAR(20) CHARACTER SET utf8mb4) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
}

var mdbTables = []string{"app.acct", "app.notes"}

// mdbDump: toàn bộ kết quả truy vấn thành chuỗi (mỗi dòng một row, cột nối bằng '|').
func mdbDump(db *sql.DB, q string) (string, error) {
	rows, err := db.Query(q)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	vals := make([]sql.RawBytes, len(cols))
	ptr := make([]any, len(cols))
	for i := range ptr {
		ptr[i] = &vals[i]
	}
	var sb strings.Builder
	for rows.Next() {
		if err := rows.Scan(ptr...); err != nil {
			return "", err
		}
		for i, v := range vals {
			if i > 0 {
				sb.WriteByte('|')
			}
			if v == nil {
				sb.WriteString("<NULL>")
			} else {
				sb.Write(v)
			}
		}
		sb.WriteByte('\n')
	}
	return sb.String(), rows.Err()
}

// Truy vấn so sánh tất định: HEX() cho cột nhị phân/văn bản; text latin1 quy về utf8mb4 trước khi HEX
// (đích có thể lưu charset khác nhưng nội dung ký tự phải y hệt).
func mdbQuery(table string, notesExtra bool) string {
	if table == "app.acct" {
		return "SELECT HEX(code), bal, created, big, kind, CAST(meta AS CHAR CHARACTER SET utf8mb4), HEX(bl), n FROM app.acct ORDER BY HEX(code)"
	}
	extra := ""
	if notesExtra {
		extra = ", extra"
	}
	return "SELECT id, HEX(CONVERT(body USING utf8mb4)), tag" + extra + " FROM app.notes ORDER BY id"
}

// mdbDiff: "" nếu mọi bảng source == target, ngược lại mô tả bảng/dòng lệch đầu tiên.
func mdbDiff(src, dst *sql.DB, notesExtra bool) string {
	for _, tb := range mdbTables {
		q := mdbQuery(tb, notesExtra)
		a, e1 := mdbDump(src, q)
		b, e2 := mdbDump(dst, q)
		if e1 != nil || e2 != nil {
			return fmt.Sprintf("%s: query error src=%v dst=%v", tb, e1, e2)
		}
		if a != b {
			as, bs := strings.Split(a, "\n"), strings.Split(b, "\n")
			for i := 0; i < len(as) || i < len(bs); i++ {
				var x, y string
				if i < len(as) {
					x = as[i]
				}
				if i < len(bs) {
					y = bs[i]
				}
				if x != y {
					return fmt.Sprintf("%s differs (rows src=%d dst=%d) at line %d:\n src: %s\n dst: %s", tb, len(as)-1, len(bs)-1, i, x, y)
				}
			}
		}
	}
	return ""
}

func mdbWaitEqual(t *testing.T, src, dst *mysqlNode, p *strandProc, notesExtra bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		d := mdbDiff(src.DB, dst.DB, notesExtra)
		if d == "" {
			return
		}
		if p != nil && p.exited() {
			t.Fatalf("strand thoát (code %d) trước khi bắt kịp: %s\n%s", p.exitCode(), d, p.tail())
		}
		if time.Now().After(deadline) {
			t.Fatalf("target chưa hội tụ: %s\n%s", d, tailOf(p))
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func mdbCount(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

// mdbWriter: tải ghi liên tục (insert/update/delete/txn nhiều câu/update TEXT latin1).
type mdbWriter struct {
	db   *sql.DB
	next atomic.Int64
	ops  atomic.Int64
	stop atomic.Bool
	wg   sync.WaitGroup
}

func (w *mdbWriter) Start(clients int) {
	for c := 0; c < clients; c++ {
		r := rand.New(rand.NewSource(int64(1000 + c)))
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for !w.stop.Load() {
				if err := w.one(r); err == nil {
					w.ops.Add(1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}
}

func (w *mdbWriter) Stop() { w.stop.Store(true); w.wg.Wait() }

func (w *mdbWriter) one(r *rand.Rand) error {
	id := w.next.Add(1)
	old := r.Int63n(id) + 1
	switch r.Intn(6) {
	case 0, 1:
		_, err := w.db.Exec("INSERT INTO app.acct (code, bal, created, big, kind, meta, bl, n) VALUES (?, ?, NOW(3), ?, 'active', ?, ?, 0)",
			fmt.Sprintf("w%07d-é", id), fmt.Sprintf("%d.%04d", r.Intn(100000), r.Intn(10000)), uint64(18446744073709551615)-uint64(r.Intn(1000)),
			fmt.Sprintf(`{"i": %d}`, id), []byte{0, byte(r.Intn(256)), 0xff, 0})
		return err
	case 2:
		_, err := w.db.Exec("UPDATE app.acct SET bal = bal + 1.5, n = n + 1, kind = 'closed' WHERE code = ?", fmt.Sprintf("w%07d-é", old))
		return err
	case 3:
		_, err := w.db.Exec("DELETE FROM app.acct WHERE code = ?", fmt.Sprintf("w%07d-é", old))
		return err
	case 4:
		tx, err := w.db.Begin()
		if err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			if _, err = tx.Exec("INSERT INTO app.notes (body, tag) VALUES (?, ?)", fmt.Sprintf("tx café %d naïve", id), "tx"); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		if _, err = tx.Exec("UPDATE app.notes SET tag = 'txu' WHERE id = ?", old); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	default:
		_, err := w.db.Exec("UPDATE app.notes SET body = ? WHERE id = ?", fmt.Sprintf("upd café %d", id), old)
		return err
	}
}

func TestMDB106_E2E_SnapshotStreamDDLRestartVerify(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, mdbSchemaSQL...)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)

	// Dữ liệu snapshot (kết nối utf8mb4): ký tự non-ASCII vào latin1 TEXT, uint64 max, BLOB có 0x00.
	for _, q := range []string{
		`INSERT INTO app.acct VALUES ('Zoë-1', 12345678901234.5678, '2024-02-29 23:59:59.123', 18446744073709551615, 'active', '{"a": 1, "b": "é"}', 0x00FF0001, 1)`,
		`INSERT INTO app.acct VALUES ('plain', -0.0001, '1999-12-31 00:00:00.000', 0, 'new', NULL, NULL, 2)`,
		`INSERT INTO app.acct VALUES ('日本語', 0, '2000-01-01 00:00:00.001', 9223372036854775808, 'closed', '[1, 2]', 0x00, 3)`,
		`INSERT INTO app.notes (body, tag) VALUES ('café', 'a'), ('naïve', 'b'), ('ascii', NULL)`,
	} {
		src.exec(t, q)
	}

	cfg := strandCfg{Name: "e2e-mdb106", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.*"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)

	out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfgPath)
	if code != 0 {
		t.Fatalf("validate: exit %d, want 0\n%s", code, out)
	}

	p := startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)
	if !strings.Contains(p.output.String(), "BINLOG POSITION MODE") {
		t.Fatalf("thiếu cảnh báo chế độ vị trí binlog\n%s", p.tail())
	}

	// 1. Snapshot: giống source và đúng giá trị cụ thể.
	if d := mdbDiff(src.DB, dst.DB, false); d != "" {
		t.Fatalf("snapshot lệch: %s", d)
	}
	var coll string
	if err := dst.DB.QueryRow("SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='app' AND TABLE_NAME='acct' AND COLUMN_NAME='code'").Scan(&coll); err != nil || coll != "utf8mb4_general_ci" {
		t.Fatalf("collation cột code ở đích = %q (%v), want utf8mb4_general_ci", coll, err)
	}
	got, err := mdbDump(dst.DB, "SELECT code, bal, created, big, kind, HEX(bl) FROM app.acct WHERE code = 'Zoë-1'")
	if want := "Zoë-1|12345678901234.5678|2024-02-29 23:59:59.123|18446744073709551615|active|00FF0001\n"; err != nil || got != want {
		t.Fatalf("row Zoë-1 ở đích = %q (%v), want %q", got, err, want)
	}
	for body, hex := range map[string]string{"café": "636166C3A9", "naïve": "6E61C3AF7665"} {
		var h string
		if err := dst.DB.QueryRow("SELECT HEX(CONVERT(body USING utf8mb4)) FROM app.notes WHERE tag = ?", map[string]string{"café": "a", "naïve": "b"}[body]).Scan(&h); err != nil || h != hex {
			t.Fatalf("latin1 TEXT %q ở đích hex=%q (%v), want %q", body, h, err, hex)
		}
	}
	if n := mdbCount(t, dst.DB, "SELECT COUNT(*) FROM app.acct"); n != 3 {
		t.Fatalf("snapshot acct %d row, want 3", n)
	}

	// 2. Stream: INSERT/UPDATE/DELETE + Txn nhiều câu + UPDATE TEXT latin1.
	src.exec(t,
		`INSERT INTO app.acct VALUES ('s1', 1.2500, NOW(3), 18446744073709551614, 'new', '{"k": "v"}', 0x610062, 0)`,
		`INSERT INTO app.acct VALUES ('s2', 2.5000, NOW(3), 5, 'active', NULL, 0x00, 0)`,
		`UPDATE app.acct SET bal = bal * 2, n = n + 1, kind = 'closed' WHERE code = 's1'`,
		`DELETE FROM app.acct WHERE code = 'plain'`,
		`UPDATE app.notes SET body = 'crème brûlée' WHERE tag = 'a'`)
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`INSERT INTO app.notes (body, tag) VALUES ('txn é 1', 'tx')`,
		`INSERT INTO app.notes (body, tag) VALUES ('txn ï 2', 'tx')`,
		`UPDATE app.acct SET n = 99 WHERE code = 's2'`,
		`DELETE FROM app.notes WHERE tag = 'b'`} {
		if _, err := tx.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	mdbWaitEqual(t, src, dst, p, false, 2*time.Minute)
	var h string
	if err := dst.DB.QueryRow("SELECT HEX(CONVERT(body USING utf8mb4)) FROM app.notes WHERE tag = 'a'").Scan(&h); err != nil || h != "6372C3A86D65206272C3BB6CC3A965" {
		t.Fatalf("UPDATE latin1 TEXT ở đích hex=%q (%v)", h, err)
	}
	if n := mdbCount(t, dst.DB, "SELECT COUNT(*) FROM app.acct WHERE code='plain'") + mdbCount(t, dst.DB, "SELECT COUNT(*) FROM app.notes WHERE tag='b'"); n != 0 {
		t.Fatalf("DELETE chưa tới đích (%d)", n)
	}
	if n := mdbCount(t, dst.DB, "SELECT COUNT(*) FROM app.notes WHERE tag='tx'"); n != 2 {
		t.Fatalf("Txn nhiều câu: %d row tag=tx, want 2", n)
	}

	// 3. DDL: ADD COLUMN ở source → sync dừng exit 10, thông điệp có DDL; DBA áp ở đích; chạy lại.
	src.exec(t, "ALTER TABLE app.notes ADD COLUMN extra INT NULL")
	src.exec(t, "INSERT INTO app.notes (body, tag, extra) VALUES ('after ddl é', 'ddl', 7)")
	select {
	case <-p.done:
	case <-time.After(2 * time.Minute):
		t.Fatalf("sync không dừng sau ALTER\n%s", p.tail())
	}
	if p.exitCode() != 10 || !strings.Contains(p.output.String(), "schema_change_pending") ||
		!strings.Contains(strings.ToUpper(p.output.String()), "ADD COLUMN EXTRA") {
		t.Fatalf("mong exit 10 schema_change_pending kèm DDL, got code=%d\n%s", p.exitCode(), p.tail())
	}
	dst.exec(t, "ALTER TABLE app.notes ADD COLUMN extra INT NULL")
	// Ghi khi Strand đang dừng.
	for i := 0; i < 20; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.notes (body, tag, extra) VALUES ('stopped café %d', 'stopped', %d)", i, i),
			fmt.Sprintf("INSERT INTO app.acct VALUES ('stopped%d', %d.5000, NOW(3), %d, 'new', '{\"i\": %d}', 0x00ff, 0)", i, i, i, i))
	}
	p = startStrand(t, cfgPath)
	mdbWaitEqual(t, src, dst, p, true, 2*time.Minute)
	if n := mdbCount(t, dst.DB, "SELECT COUNT(*) FROM app.notes WHERE tag='stopped' OR tag='ddl'"); n != 21 {
		t.Fatalf("row ghi khi dừng/sau DDL ở đích: %d, want 21", n)
	}
	if n := mdbCount(t, dst.DB, "SELECT extra FROM app.notes WHERE tag='ddl'"); n != 7 {
		t.Fatalf("cột mới extra = %d, want 7", n)
	}

	// 4. Tải liên tục + SIGKILL giữa chừng + khởi động lại.
	w := &mdbWriter{db: src.DB}
	w.Start(4)
	time.Sleep(3 * time.Second)
	p.kill9()
	time.Sleep(time.Second)
	p = startStrand(t, cfgPath)
	time.Sleep(3 * time.Second)
	if p.exited() {
		w.Stop()
		t.Fatalf("strand thoát sau restart (code %d)\n%s", p.exitCode(), p.tail())
	}
	w.Stop()
	if w.ops.Load() < 50 {
		t.Fatalf("writer chỉ ghi %d op", w.ops.Load())
	}
	t.Logf("writer: %d op", w.ops.Load())
	mdbWaitEqual(t, src, dst, p, true, 3*time.Minute)

	// 5. verify full từng bảng thành công; cài lệch vào đích thì verify báo mismatch.
	for _, tb := range mdbTables {
		out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", cfgPath, "--table", tb, "--mode", "full")
		if code != 0 {
			t.Fatalf("verify full %s: exit %d\n%s", tb, code, out)
		}
	}
	dst.exec(t, "UPDATE app.acct SET bal = bal + 1 ORDER BY code LIMIT 1")
	out, code = runStrandCLI(t, 5*time.Minute, "verify", "-c", cfgPath, "--table", "app.acct", "--mode", "full")
	if code == 0 || !strings.Contains(strings.ToLower(out), "mismatch") {
		t.Fatalf("lệch cài vào đích không bị phát hiện: exit %d\n%s", code, out)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("stop: exit %d\n%s", code, p.tail())
	}
}

func TestMDB106_E2E_ValidateRejectsUnsupported(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, "CREATE DATABASE app",
		"CREATE TABLE app.t_inet (id INT PRIMARY KEY, ip INET6) ENGINE=InnoDB",
		"CREATE TABLE app.t_comp (id INT PRIMARY KEY, body TEXT COMPRESSED) ENGINE=InnoDB",
		"CREATE TABLE app.t_sv (id INT PRIMARY KEY, v INT) ENGINE=InnoDB WITH SYSTEM VERSIONING",
		"CREATE TABLE app.t_nopad (id INT PRIMARY KEY, name VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_nopad_ci) ENGINE=InnoDB",
		"CREATE TABLE app.t_ok (id INT PRIMARY KEY, v INT) ENGINE=InnoDB",
		"CREATE SEQUENCE app.seq1")
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)

	validate := func(name string, include []string, exclude ...string) (string, int) {
		cfg := strandCfg{Name: name, SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
			TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: include, Exclude: exclude, Dir: testDir(t)}
		return runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t))
	}
	for _, c := range []struct{ table, want string }{
		{"app.t_inet", "MariaDB-only type"},
		{"app.t_comp", "COMPRESSED"},
		{"app.t_sv", "system-versioned"},
		{"app.t_nopad", "not available on the target"},
	} {
		out, code := validate("mdb-rej-"+strings.TrimPrefix(c.table, "app."), []string{c.table})
		if code == 0 || !strings.Contains(out, c.want) {
			t.Errorf("%s: exit %d, want non-zero with %q\n%s", c.table, code, c.want, out)
		}
	}
	// SEQUENCE chỉ cảnh báo, không chặn (include app.* gặp cả sequence; loại bốn bảng bị từ chối).
	out, code := validate("mdb-seq", []string{"app.*"}, "app.t_inet", "app.t_comp", "app.t_sv", "app.t_nopad")
	if code != 0 || !strings.Contains(out, "MariaDB sequence omitted") {
		t.Errorf("sequence: exit %d, want 0 with warning %q\n%s", code, "MariaDB sequence omitted", out)
	}

	// MariaDB → PostgreSQL bị từ chối.
	pg := startPG(t, "pgdst")
	pcfg := strandCfg{Name: "mdb-pg", TargetKind: "postgres", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: pg.DSN, Include: []string{"app.t_ok"}, Dir: testDir(t)}
	out, code = runStrandCLI(t, time.Minute, "validate", "-c", pcfg.write(t))
	if code == 0 || !strings.Contains(out, "supported only with a MySQL 8.x target") {
		t.Errorf("MariaDB→postgres: exit %d, want rejection with %q\n%s", code, "supported only with a MySQL 8.x target", out)
	}
}

func TestMDB106_E2E_Promote(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, mdbSchemaSQL...)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	src.exec(t, `INSERT INTO app.notes (body, tag) VALUES ('café', 'a'), ('naïve', 'b')`)

	cfg := strandCfg{Name: "e2e-mdb-promote", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.*"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)

	// Đẩy AUTO_INCREMENT nguồn vượt MAX(id)+1: chèn rồi xóa các id cao nhất.
	for i := 0; i < 20; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.notes (body, tag) VALUES ('n%d', 'x')", i))
	}
	src.exec(t, "INSERT INTO app.notes (body, tag) VALUES ('d1','doomed'),('d2','doomed'),('d3','doomed')", "DELETE FROM app.notes WHERE tag = 'doomed'")
	mdbWaitEqual(t, src, dst, p, false, 2*time.Minute)
	var srcAI, maxID int64
	if err := src.DB.QueryRow("SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='app' AND TABLE_NAME='notes'").Scan(&srcAI); err != nil {
		t.Fatal(err)
	}
	if err := src.DB.QueryRow("SELECT MAX(id) FROM app.notes").Scan(&maxID); err != nil || srcAI < maxID+4 {
		t.Fatalf("tiền điều kiện: AUTO_INCREMENT nguồn %d, MAX(id) %d (%v)", srcAI, maxID, err)
	}

	// Session app đang chạy SELECT SLEEP: phải bị kill khi fence.
	appDB, err := sql.Open("mysql", fmt.Sprintf("appuser:apppw@tcp(%s)/app", src.Addr))
	if err != nil {
		t.Fatal(err)
	}
	defer appDB.Close()
	sleepErr := make(chan error, 1)
	var started atomic.Bool
	go func() {
		started.Store(true)
		var s int
		sleepErr <- appDB.QueryRow("SELECT SLEEP(300)").Scan(&s)
	}()
	for i := 0; i < 50 && mdbCount(t, src.DB, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE USER='appuser' AND INFO LIKE 'SELECT SLEEP%'") == 0; i++ {
		time.Sleep(100 * time.Millisecond)
	}
	if mdbCount(t, src.DB, "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE USER='appuser' AND INFO LIKE 'SELECT SLEEP%'") == 0 {
		t.Fatal("session app chưa chạy SLEEP")
	}

	buf := &bytes.Buffer{}
	cmd := exec.Command(strandBin, "promote", "--target", dst.RootDSN+"strand", "--fence", src.RootDSN,
		"--pipeline", cfg.Name, "--source-app-roles", "appuser", "--fence-timeout", "30s", "--catchup-timeout", "150s")
	cmd.Stdout, cmd.Stderr = buf, buf
	pdone := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { pdone <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	select {
	case err := <-pdone:
		if err != nil {
			t.Fatalf("promote: %v\n%s\n%s", err, buf.String(), p.tail())
		}
	case <-time.After(4 * time.Minute):
		t.Fatalf("promote không xong\n%s\n%s", buf.String(), p.tail())
	}

	select {
	case err := <-sleepErr:
		if err == nil {
			t.Fatal("session app SLEEP kết thúc không lỗi: không bị kill")
		}
		t.Logf("session app bị kill: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("session app vẫn còn sau promote")
	}
	var ro int
	if err := src.DB.QueryRow("SELECT @@GLOBAL.read_only").Scan(&ro); err != nil || ro != 1 {
		t.Fatalf("source read_only=%d (%v), want 1", ro, err)
	}
	if _, err := appDB.Exec("INSERT INTO app.notes (body, tag) VALUES ('x','after')"); err == nil {
		t.Fatal("app vẫn ghi được vào source sau promote")
	}
	if !p.exited() { // promote có thể đã làm pipeline tự dừng
		if code := p.stop(t); code != 0 {
			t.Fatalf("stop: exit %d\n%s", code, p.tail())
		}
	}

	// AUTO_INCREMENT ở đích >= nguồn (information_schema không cache) và id mới không trùng.
	conn, err := dst.DB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var dstAI int64
	if _, err := conn.ExecContext(context.Background(), "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRowContext(context.Background(), "SELECT AUTO_INCREMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA='app' AND TABLE_NAME='notes'").Scan(&dstAI); err != nil || dstAI < srcAI {
		t.Fatalf("AUTO_INCREMENT đích %d (%v) < nguồn %d", dstAI, err, srcAI)
	}
	res, err := dst.DB.Exec("INSERT INTO app.notes (body, tag) VALUES ('on target', 'new')")
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := res.LastInsertId(); id < srcAI {
		t.Fatalf("id mới ở đích %d < AUTO_INCREMENT nguồn %d", id, srcAI)
	}
}

// Bộ quyền tối thiểu của user strand trên nguồn MariaDB (SELECT, LOCK TABLES trên app.*) phải qua validate.
func TestMDB106_E2E_ValidateMinimalGrants(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, mdbSchemaSQL...)
	mdbSourceUsers(t, src, true)
	mdbTargetSetup(t, dst)
	cfg := strandCfg{Name: "e2e-mdb-mingrant", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.*"}, Dir: testDir(t)}
	out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t))
	if code != 0 {
		t.Fatalf("validate với quyền tối thiểu: exit %d, want 0\n%s", code, out)
	}
}
