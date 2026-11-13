package e2e

// MariaDB 10.6 → MySQL 8.4: chuyển view, procedure, function, trigger, event, user bằng `strand objects`.
// Quy tắc đúng đắn: trigger KHÔNG có ở target khi đang sync (binlog ROW đã chứa dòng trigger ghi ở source), event
// không chạy ở target trước cutover; promote --objects-post tạo trigger và bật event trước khi mở ghi.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func dstCount(t *testing.T, db *sql.DB, q string, args ...any) int {
	return mdbCount(t, db, q, args...)
}

func mdbObjectsSourceSetup(t *testing.T, src *mysqlNode) {
	src.exec(t,
		`CREATE TABLE app.audit (id BIGINT AUTO_INCREMENT PRIMARY KEY, code VARCHAR(40) CHARACTER SET utf8mb4, note VARCHAR(20)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		// view-on-view đi ngược thứ tự bảng chữ cái: v_a_top phụ thuộc v_z_base
		"CREATE VIEW app.v_z_base AS SELECT code, bal FROM app.acct WHERE kind = 'active'",
		"CREATE VIEW app.v_a_top AS SELECT code FROM app.v_z_base WHERE bal > 100",
		"CREATE PROCEDURE app.bump(IN c VARCHAR(40)) UPDATE app.acct SET n = n + 1 WHERE code = c",
		"CREATE FUNCTION app.double_n(x INT) RETURNS INT DETERMINISTIC RETURN x * 2",
		"CREATE TRIGGER app.acct_ai AFTER INSERT ON app.acct FOR EACH ROW INSERT INTO app.audit (code, note) VALUES (NEW.code, 'ins')",
		"CREATE EVENT app.ev_daily ON SCHEDULE EVERY 1 DAY COMMENT 'daily' DO UPDATE app.notes SET tag = 'ev' WHERE id = 0",
		// role MariaDB + user dùng role; user có quyền chỉ MariaDB có
		"CREATE ROLE app_ro",
		"GRANT SELECT ON app.* TO app_ro",
		"CREATE USER 'rouser'@'%' IDENTIFIED BY 'ropw'",
		"GRANT app_ro TO 'rouser'@'%'",
		"SET DEFAULT ROLE app_ro FOR 'rouser'@'%'",
		"CREATE USER 'monuser'@'%' IDENTIFIED BY 'monpw'",
		"GRANT SELECT, DELETE HISTORY ON app.* TO 'monuser'@'%'",
		"GRANT EXECUTE ON PROCEDURE app.bump TO 'appuser'@'%'",
		// user chỉ có quyền ở schema khác: không được chuyển
		"CREATE DATABASE other",
		"CREATE USER 'otheruser'@'%' IDENTIFIED BY 'otherpw'",
		"GRANT SELECT ON other.* TO 'otheruser'@'%'",
	)
	// BINLOG MONITOR (quyền riêng MariaDB) có từ 10.5.2; bản cũ hơn chỉ có REPLICATION CLIENT (cùng ánh xạ).
	if mdbAtLeast(t, src.DB, 10, 5) {
		src.exec(t, "GRANT BINLOG MONITOR ON *.* TO 'monuser'@'%'")
	} else {
		src.exec(t, "GRANT REPLICATION CLIENT ON *.* TO 'monuser'@'%'")
	}
}

// countAction đếm dòng kết quả bắt đầu bằng action (bỏ dòng tổng kết "phase ...").
func countAction(out, action string) int {
	n := 0
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, action+" ") {
			n++
		}
	}
	return n
}

func objCLI(t *testing.T, args ...string) string {
	t.Helper()
	out, code := runStrandCLI(t, 2*time.Minute, args...)
	if code != 0 {
		t.Fatalf("strand %s: exit %d\n%s", strings.Join(args, " "), code, out)
	}
	return out
}

// mysqlClientRun chạy tệp SQL bằng client mysql thật trong container đích.
func mysqlClientRun(t *testing.T, n *mysqlNode, file string) {
	t.Helper()
	ctx := context.Background()
	if err := n.c.CopyFileToContainer(ctx, file, "/tmp/objects.sql", 0o644); err != nil {
		t.Fatal(err)
	}
	code, r, err := n.c.Exec(ctx, []string{"sh", "-c", "mysql -uroot -pdev --default-character-set=utf8mb4 < /tmp/objects.sql 2>&1"})
	var buf bytes.Buffer
	if r != nil {
		_, _ = buf.ReadFrom(r)
	}
	if err != nil || code != 0 {
		t.Fatalf("mysql < %s: code=%d err=%v\n%s", filepath.Base(file), code, err, buf.String())
	}
}

func TestMDB106_E2E_Objects(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil, "--mysql-native-password=ON")
	src.exec(t, mdbSchemaSQL...)
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	mdbObjectsSourceSetup(t, src)
	// validate chỉ đếm được đối tượng mà tài khoản strand nhìn thấy (information_schema lọc theo quyền).
	src.exec(t, "GRANT SHOW VIEW, TRIGGER, EVENT, EXECUTE ON app.* TO 'strand'@'%'")
	src.exec(t, `INSERT INTO app.acct VALUES ('Zoë-1', 500.0000, '2024-02-29 23:59:59.123', 1, 'active', NULL, NULL, 1)`) // snapshot: trigger source ghi audit
	src.exec(t, "SET GLOBAL event_scheduler = ON")

	cfg := strandCfg{Name: "e2e-mdb-objects", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.*"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	rootSrc := "--source-dsn=" + src.RootDSN
	rootDst := "--target-dsn=" + dst.RootDSN

	// validate cảnh báo đối tượng không tự chuyển.
	out := objCLI(t, "validate", "-c", cfgPath)
	for _, want := range []string{"2 views", "1 procedure", "1 function", "1 trigger", "1 event", "not migrated automatically", "strand objects"} {
		if !strings.Contains(out, want) {
			t.Fatalf("validate thiếu %q:\n%s", want, out)
		}
	}

	// list
	out = objCLI(t, "objects", "list", "-c", cfgPath, rootSrc)
	for _, want := range []string{"view", "app.v_a_top", "app.v_z_base", "procedure  app.bump", "function   app.double_n", "trigger    app.acct_ai  ON acct", "event      app.ev_daily",
		"user       'appuser'@'%'", "user       'rouser'@'%'", "role       `app_ro`", "user       'monuser'@'%'", "total: 2 view, 1 procedure, 1 function, 1 trigger, 1 event"} {
		if !strings.Contains(out, want) {
			t.Fatalf("list thiếu %q:\n%s", want, out)
		}
	}
	for _, l := range strings.Split(out, "\n") {
		if (strings.HasPrefix(l, "user") || strings.HasPrefix(l, "role")) && (strings.Contains(l, "otheruser") || strings.Contains(l, "'strand'@") || strings.Contains(l, "root")) {
			t.Fatalf("list chứa user không được chuyển: %s\n%s", l, out)
		}
	}

	p := startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)
	if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM app.audit"); n != 1 {
		t.Fatalf("snapshot: audit ở đích = %d, want 1 (hàng do trigger source ghi)", n)
	}

	// export → các tệp.
	dir := filepath.Join(testDir(t), "objs")
	objCLI(t, "objects", "export", "-c", cfgPath, rootSrc, "--out", dir)
	read := func(n string) string {
		b, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	views := read("01-views-routines.sql")
	if i, j := strings.Index(views, "VIEW `app`.`v_z_base`"), strings.Index(views, "VIEW `app`.`v_a_top`"); i < 0 || j < 0 || i > j {
		t.Fatalf("view nền phải đứng trước view phụ thuộc (%d, %d):\n%s", i, j, views)
	}
	if strings.Contains(views, "DEFINER=") {
		t.Fatalf("DEFINER chưa bị bỏ:\n%s", views)
	}
	if s := read("02-events-disabled.sql"); !strings.Contains(s, "DISABLE") || !strings.Contains(s, "ev_daily") {
		t.Fatalf("02:\n%s", s)
	}
	if s := read("03-triggers.sql"); !strings.Contains(s, "acct_ai") {
		t.Fatalf("03:\n%s", s)
	}
	if s := read("04-events-enable.sql"); !strings.Contains(s, "ALTER EVENT `app`.`ev_daily` ENABLE") {
		t.Fatalf("04:\n%s", s)
	}
	users := read("users.sql")
	for _, want := range []string{"mysql_native_password=ON", "CREATE USER IF NOT EXISTS 'appuser'@'%' IDENTIFIED WITH mysql_native_password AS '*", "GRANT `app_ro` TO 'rouser'@'%'",
		"SET DEFAULT ROLE `app_ro` TO 'rouser'@'%'", "DELETE HISTORY dropped", "global privileges NOT copied", "GRANT EXECUTE ON PROCEDURE `app`.`bump` TO 'appuser'@'%'"} {
		if !strings.Contains(users, want) {
			t.Fatalf("users.sql thiếu %q:\n%s", want, users)
		}
	}
	// Chỉ xét câu lệnh thật (bỏ dòng chú thích "-- skipped ...").
	for _, ln := range strings.Split(users, "\n") {
		if strings.HasPrefix(strings.TrimSpace(ln), "--") {
			continue
		}
		for _, bad := range []string{"otheruser", "'root'", "mariadb.sys", "'strand'", "healthcheck"} {
			if strings.Contains(ln, bad) {
				t.Fatalf("users.sql có câu lệnh cho user không được chuyển (%s): %s", bad, ln)
			}
		}
	}

	// apply --phase pre: dry-run không tạo gì; thật thì tạo view/routine/event (DISABLED), KHÔNG có trigger.
	out = objCLI(t, "objects", "apply", "-c", cfgPath, rootSrc, rootDst, "--phase", "pre", "--dry-run")
	if !strings.Contains(out, "would-create") || dstCount(t, dst.DB, "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA='app'") != 0 {
		t.Fatalf("dry-run:\n%s", out)
	}
	out = objCLI(t, "objects", "apply", "-c", cfgPath, rootSrc, rootDst, "--phase", "pre")
	if countAction(out, "created") != 5 {
		t.Fatalf("pre:\n%s", out)
	}
	checkPre := func() {
		t.Helper()
		if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA='app'"); n != 2 {
			t.Fatalf("views ở đích = %d, want 2", n)
		}
		if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='app'"); n != 2 {
			t.Fatalf("routines ở đích = %d, want 2", n)
		}
		var st string
		if err := dst.DB.QueryRow("SELECT STATUS FROM information_schema.EVENTS WHERE EVENT_SCHEMA='app' AND EVENT_NAME='ev_daily'").Scan(&st); err != nil || st != "DISABLED" {
			t.Fatalf("event ở đích = %q (%v), want DISABLED", st, err)
		}
		if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='app'"); n != 0 {
			t.Fatalf("trigger tồn tại ở đích khi đang sync: %d", n)
		}
	}
	checkPre()
	var dbl int
	if err := dst.DB.QueryRow("SELECT app.double_n(21)").Scan(&dbl); err != nil || dbl != 42 {
		t.Fatalf("function ở đích: %d %v", dbl, err)
	}
	if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM app.v_a_top"); n != 1 {
		t.Fatalf("view-on-view trả %d dòng, want 1", n)
	}
	// idempotent: lần hai mọi đối tượng "identical", không lỗi, không tạo mới.
	out = objCLI(t, "objects", "apply", "-c", cfgPath, rootSrc, rootDst, "--phase", "pre")
	if countAction(out, "identical") != 5 || countAction(out, "created") != 0 || countAction(out, "failed") != 0 {
		t.Fatalf("apply lần hai không idempotent:\n%s", out)
	}
	// khác định nghĩa → lỗi rõ, không ghi đè (exit 1), các đối tượng khác vẫn xử lý.
	dst.exec(t, "DROP FUNCTION app.double_n", "CREATE FUNCTION app.double_n(x INT) RETURNS INT DETERMINISTIC RETURN x * 3")
	out, code := runStrandCLI(t, time.Minute, "objects", "apply", "-c", cfgPath, rootSrc, rootDst, "--phase", "pre")
	if code != 1 || !strings.Contains(out, "DIFFERENT definition") || !strings.Contains(out, "function app.double_n") || countAction(out, "identical") != 4 {
		t.Fatalf("khác định nghĩa: exit %d\n%s", code, out)
	}
	dst.exec(t, "DROP FUNCTION app.double_n")

	// Tệp .sql chạy được bằng client mysql thật; xong apply lại thì mọi thứ giống bản do apply tạo.
	dst.exec(t, "DROP VIEW app.v_a_top", "DROP VIEW app.v_z_base", "DROP PROCEDURE app.bump", "DROP EVENT app.ev_daily")
	mysqlClientRun(t, dst, filepath.Join(dir, "01-views-routines.sql"))
	mysqlClientRun(t, dst, filepath.Join(dir, "02-events-disabled.sql"))
	checkPre()
	out = objCLI(t, "objects", "apply", "-c", cfgPath, rootSrc, rootDst, "--phase", "pre")
	if countAction(out, "identical") != 5 {
		t.Fatalf("tệp chạy bằng client khác bản apply:\n%s", out)
	}

	// users: apply rồi đăng nhập thật; users.sql chạy lại bằng client (idempotent).
	objCLI(t, "objects", "apply", "-c", cfgPath, rootSrc, rootDst, "--phase", "users")
	mysqlClientRun(t, dst, filepath.Join(dir, "users.sql"))
	login := func(user, pw string) *sql.DB {
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/app", user, pw, dst.Addr))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	app := login("appuser", "apppw")
	if n := dstCount(t, app, "SELECT COUNT(*) FROM app.acct"); n < 1 {
		t.Fatalf("appuser không đọc được: %d", n)
	}
	ro := login("rouser", "ropw") // quyền qua role + SET DEFAULT ROLE
	if n := dstCount(t, ro, "SELECT COUNT(*) FROM app.acct"); n < 1 {
		t.Fatalf("rouser không đọc được qua role: %d", n)
	}
	if _, err := ro.Exec("DELETE FROM app.acct"); err == nil {
		t.Fatal("rouser xóa được dữ liệu: role cấp thừa quyền")
	}
	mon := login("monuser", "monpw")
	if n := dstCount(t, mon, "SELECT COUNT(*) FROM app.acct"); n < 1 {
		t.Fatalf("monuser: %d", n)
	}
	if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM mysql.user WHERE User IN ('otheruser','strand') AND User <> 'strand'"); n != 0 {
		t.Fatalf("otheruser được tạo ở đích")
	}

	// Ghi trên source khi đang sync: trigger CHẠY Ở SOURCE; đích phải có đúng các dòng audit đó, không nhân đôi.
	const inserts = 25
	for i := 0; i < inserts; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.acct VALUES ('w%03d', 10.0000, NOW(3), 1, 'new', NULL, NULL, 0)", i))
	}
	src.exec(t, "UPDATE app.acct SET bal = bal + 1 WHERE code LIKE 'w00%'", "DELETE FROM app.acct WHERE code = 'w024'")
	mdbWaitEqual(t, src, dst, p, false, 2*time.Minute)
	auditQ := "SELECT id, code, note FROM app.audit ORDER BY id"
	deadline := time.Now().Add(time.Minute)
	for {
		a, _ := mdbDump(src.DB, auditQ)
		b, _ := mdbDump(dst.DB, auditQ)
		if a == b && a != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("audit lệch:\nsrc:\n%s\ndst:\n%s", a, b)
		}
		time.Sleep(300 * time.Millisecond)
	}
	wantAudit := 1 + inserts
	if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM app.audit"); n != wantAudit {
		t.Fatalf("audit ở đích = %d, want %d (trigger ở đích chạy lần nữa?)", n, wantAudit)
	}

	// promote --objects-post lỗi: trigger cùng tên khác định nghĩa ở đích → dừng TRƯỚC khi mở ghi, target vẫn read-only.
	dst.exec(t, "CREATE TRIGGER app.acct_ai AFTER INSERT ON app.acct FOR EACH ROW SET @x = 1")
	promoteArgs := []string{"promote", "-c", cfgPath, "--objects-post", "--target", dst.RootDSN + "strand", "--fence", src.RootDSN,
		"--pipeline", cfg.Name, "--source-app-roles", "appuser", "--fence-timeout", "30s", "--catchup-timeout", "150s"}
	pout, pcode := runStrandCLI(t, 4*time.Minute, promoteArgs...)
	if pcode == 0 || !strings.Contains(pout, "NOT opened") || !strings.Contains(pout, "acct_ai") {
		t.Fatalf("promote phải dừng vì trigger khác định nghĩa: exit %d\n%s", pcode, pout)
	}
	var ro1 int
	if err := dst.DB.QueryRow("SELECT @@GLOBAL.read_only").Scan(&ro1); err != nil || ro1 != 1 {
		t.Fatalf("target read_only=%d (%v) sau promote lỗi, want 1", ro1, err)
	}
	var sched string
	if err := src.DB.QueryRow("SELECT @@GLOBAL.event_scheduler").Scan(&sched); err != nil || sched != "OFF" {
		t.Fatalf("event_scheduler ở source = %q (%v), want OFF sau khi promote --objects-post", sched, err)
	}

	// Khắc phục (xóa trigger lạ) rồi chạy lại đúng lệnh cũ: tiếp tục từ bước mở ghi.
	dst.exec(t, "DROP TRIGGER app.acct_ai")
	pout, pcode = runStrandCLI(t, 4*time.Minute, promoteArgs...)
	if pcode != 0 {
		t.Fatalf("promote chạy lại: exit %d\n%s", pcode, pout)
	}
	if !p.exited() {
		if code := p.stop(t); code != 0 {
			t.Fatalf("stop: exit %d\n%s", code, p.tail())
		}
	}
	if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='app' AND TRIGGER_NAME='acct_ai'"); n != 1 {
		t.Fatalf("trigger chưa có ở đích sau promote")
	}
	var st string
	if err := dst.DB.QueryRow("SELECT STATUS FROM information_schema.EVENTS WHERE EVENT_SCHEMA='app' AND EVENT_NAME='ev_daily'").Scan(&st); err != nil || st != "ENABLED" {
		t.Fatalf("event ở đích = %q (%v), want ENABLED", st, err)
	}
	var ro2 int
	if err := dst.DB.QueryRow("SELECT @@GLOBAL.read_only").Scan(&ro2); err != nil || ro2 != 0 {
		t.Fatalf("target read_only=%d sau promote, want 0", ro2)
	}

	// Sau promote: ghi vào đích → trigger chạy đúng một lần ở đích.
	before := dstCount(t, dst.DB, "SELECT COUNT(*) FROM app.audit")
	dst.exec(t, "INSERT INTO app.acct VALUES ('after-promote', 1.0000, NOW(3), 1, 'new', NULL, NULL, 0)")
	if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM app.audit WHERE code = 'after-promote'"); n != 1 {
		t.Fatalf("trigger ở đích chạy %d lần với dòng mới, want 1", n)
	}
	if n := dstCount(t, dst.DB, "SELECT COUNT(*) FROM app.audit"); n != before+1 {
		t.Fatalf("audit = %d, want %d", n, before+1)
	}

	// post lần nữa (sau promote) idempotent.
	out = objCLI(t, "objects", "apply", "-c", cfgPath, rootSrc, rootDst, "--phase", "post", "--from", dir)
	if countAction(out, "failed") != 0 || countAction(out, "created") != 0 || countAction(out, "identical") != 2 {
		t.Fatalf("post lần hai:\n%s", out)
	}
}

// objects từ chối cặp không phải MySQL → MySQL.
func TestObjects_RejectsNonMySQLTarget(t *testing.T) {
	cfg := strandCfg{Name: "objects-reject", SourceDSN: "u:p@tcp(127.0.0.1:1)/", TargetDSN: "postgres://u:p@127.0.0.1:1/db", Include: []string{"app.*"},
		TargetKind: "postgres", Dir: testDir(t)}
	cfgPath := cfg.write(t)
	out, code := runStrandCLI(t, time.Minute, "objects", "list", "-c", cfgPath)
	if code != 3 || !strings.Contains(out, "objects_unsupported") {
		t.Fatalf("exit %d\n%s", code, out)
	}
}
