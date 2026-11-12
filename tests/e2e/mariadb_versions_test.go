package e2e

// Khác biệt giữa các phiên bản MariaDB (10.3 … 11.4) và ánh xạ collation MariaDB → MySQL.
// Chạy theo phiên bản: STRAND_E2E_MDB_IMAGE=mariadb:10.3 go test . -run TestMDB

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"strand/internal/mysqlver"
	readermysql "strand/internal/reader/mysql"
)

// Bảng có tính năng khác nhau giữa các bản: cột sinh (VIRTUAL/PERSISTENT), INVISIBLE, CHECK, charset utf8
// (tên utf8 cũ ≤ 10.5, utf8mb3 từ 10.6), DEFAULT hàm. Snapshot + stream phải giống hệt nguồn trên mọi bản.
func TestMDB_E2E_VersionQuirks(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, "CREATE DATABASE app",
		"CREATE TABLE app.t_gen (id INT PRIMARY KEY, a INT NOT NULL, v INT AS (a * 2) VIRTUAL, p INT AS (a + 1) PERSISTENT) ENGINE=InnoDB",
		"CREATE TABLE app.t_inv (id INT PRIMARY KEY, shown INT, hid INT INVISIBLE DEFAULT 5) ENGINE=InnoDB",
		"CREATE TABLE app.t_utf8 (name VARCHAR(30) CHARACTER SET utf8 PRIMARY KEY, note VARCHAR(30) CHARACTER SET utf8mb4, "+
			"n INT, CONSTRAINT chk_n CHECK (n >= 0), ts TIMESTAMP NOT NULL DEFAULT current_timestamp(), u VARCHAR(40) DEFAULT uuid()) ENGINE=InnoDB DEFAULT CHARSET=utf8")
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	src.exec(t,
		"INSERT INTO app.t_gen (id, a) VALUES (1, 10), (2, 20)",
		"INSERT INTO app.t_inv (id, shown, hid) VALUES (1, 1, 11), (2, 2, 22)",
		"INSERT INTO app.t_utf8 (name, note, n) VALUES ('café', 'ünï', 1), ('abc', '😀', 2)")

	// Retention đọc được trên mọi bản (trước đây MariaDB < 10.6.1 lỗi vì không có binlog_expire_logs_seconds).
	secs, err := mysqlver.BinlogRetentionSeconds(context.Background(), src.DB)
	if err != nil || secs != 864000 {
		t.Fatalf("BinlogRetentionSeconds = %d, %v; want 864000 (mặc định 10 ngày)", secs, err)
	}

	cfg := strandCfg{Name: "e2e-mdb-quirks", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.*"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	if out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfgPath); code != 0 {
		t.Fatalf("validate: exit %d\n%s", code, out)
	}
	p := startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)

	queries := []string{
		"SELECT id, a, v, p FROM app.t_gen ORDER BY id",
		"SELECT id, shown, hid FROM app.t_inv ORDER BY id",
		"SELECT HEX(CONVERT(name USING utf8mb4)), HEX(note), n FROM app.t_utf8 ORDER BY 1",
	}
	// INVISIBLE: SELECT * không thấy, nhưng đã chép (đọc tên cột tường minh).
	same := func(when string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Minute)
		for {
			bad := ""
			for _, q := range queries {
				a, e1 := mdbDump(src.DB, q)
				b, e2 := mdbDump(dst.DB, q)
				if e1 != nil || e2 != nil || a != b {
					bad = fmt.Sprintf("%s: src=%q (%v) dst=%q (%v)", q, a, e1, b, e2)
					break
				}
			}
			if bad == "" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: lệch: %s\n%s", when, bad, p.tail())
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
	same("snapshot")
	src.exec(t,
		"INSERT INTO app.t_gen (id, a) VALUES (3, 30)",
		"UPDATE app.t_gen SET a = 11 WHERE id = 1",
		"INSERT INTO app.t_inv (id, shown, hid) VALUES (3, 3, 33)",
		"UPDATE app.t_utf8 SET note = 'ñ' WHERE name = 'abc'",
		"INSERT INTO app.t_utf8 (name, note, n) VALUES ('日本', 'x', 3)")
	same("stream")
	if got, err := mdbDump(dst.DB, "SELECT v, p FROM app.t_gen WHERE id = 1"); err != nil || got != "22|12\n" {
		t.Fatalf("cột sinh ở đích = %q (%v), want 22|12", got, err)
	}
	out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", cfgPath, "--table", "app.t_utf8", "--mode", "full")
	if code != 0 {
		t.Fatalf("verify t_utf8: exit %d\n%s", code, out)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("stop: exit %d\n%s", code, p.tail())
	}
}

// utf8mb4_nopad_bin (MariaDB) và utf8mb4_0900_bin (MySQL) cho cùng kết quả so sánh và sắp xếp, kể cả dấu cách
// cuối: cơ sở để ánh xạ "exact" dùng được cho khóa. Mỗi bên có bảng PK với collation tương ứng.
func TestMDB_E2E_CollationExactEquivalence(t *testing.T) {
	src := startMariaDB(t, "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	// MariaDB 10.3/10.4 coi 'a' = 'a\x00' trên nopad_bin (lỗi sửa ở 10.5): ở các bản đó ánh xạ chỉ là gần đúng
	// (xem TestPlanCollationsNopadBinOldMariaDB), nên test này chỉ áp dụng từ 10.5.
	if !mdbAtLeast(t, src.DB, 10, 5) {
		src.exec(t, "CREATE DATABASE app", "CREATE TABLE app.k (s VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin PRIMARY KEY) ENGINE=InnoDB",
			"INSERT INTO app.k VALUES ('a')")
		if _, err := src.DB.Exec("INSERT INTO app.k VALUES (?)", "a\x00"); err == nil {
			t.Fatal("giả định sai: nopad_bin của MariaDB < 10.5 phân biệt 'a' và 'a\\0'")
		}
		return
	}
	src.exec(t, "CREATE DATABASE app", "CREATE TABLE app.k (s VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin PRIMARY KEY) ENGINE=InnoDB")
	dst.exec(t, "CREATE DATABASE app", "CREATE TABLE app.k (s VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin PRIMARY KEY) ENGINE=InnoDB")
	vals := []string{"a", "a ", "a  ", "A", "b", "B ", "é", "e", "z", "Z", "\t", " ", "", "  ", "a\t", "ab", "a b", "日本", "日", "😀", "é ", "ß", "ss", "ǅ", "a\x00", "a\x00b"}
	for _, db := range []*mysqlNode{src, dst} {
		for _, v := range vals {
			if _, err := db.DB.Exec("INSERT INTO app.k VALUES (?)", v); err != nil {
				t.Fatalf("%s insert %q: %v", db.Alias, v, err)
			}
		}
	}
	// Thứ tự ORDER BY giống nhau.
	a, e1 := mdbDump(src.DB, "SELECT HEX(s) FROM app.k ORDER BY s")
	b, e2 := mdbDump(dst.DB, "SELECT HEX(s) FROM app.k ORDER BY s")
	if e1 != nil || e2 != nil || a != b {
		t.Fatalf("ORDER BY khác nhau:\n src=%s (%v)\n dst=%s (%v)", a, e1, b, e2)
	}
	if n := strings.Count(a, "\n"); n != len(vals) {
		t.Fatalf("PK giữ %d giá trị, want %d (không giá trị nào bị gộp)", n, len(vals))
	}
	// Ma trận so sánh: với mọi cặp (x, y) kết quả =, <, > giống nhau.
	// So sánh trực tiếp bằng collation trên cột (tham số chuỗi dùng collation kết nối, nên ép COLLATE tường minh).
	coll := map[*mysqlNode]string{src: "utf8mb4_nopad_bin", dst: "utf8mb4_0900_bin"}
	for _, x := range vals {
		for _, y := range vals {
			var res [2]string
			for i, db := range []*mysqlNode{src, dst} {
				q := fmt.Sprintf("SELECT CASE WHEN CONVERT(? USING utf8mb4) COLLATE %[1]s = CONVERT(? USING utf8mb4) COLLATE %[1]s THEN '=' "+
					"WHEN CONVERT(? USING utf8mb4) COLLATE %[1]s < CONVERT(? USING utf8mb4) COLLATE %[1]s THEN '<' ELSE '>' END", coll[db])
				if err := db.DB.QueryRow(q, x, y, x, y).Scan(&res[i]); err != nil {
					t.Fatalf("%s: %v", db.Alias, err)
				}
			}
			if res[0] != res[1] {
				t.Errorf("so sánh %q vs %q: MariaDB nopad_bin %s, MySQL 0900_bin %s", x, y, res[0], res[1])
			}
		}
	}
}

// MariaDB 11.4: collation mặc định utf8mb4 là uca1400_ai_ci (đích không có). Cột không khóa được ánh xạ gần
// đúng kèm cảnh báo; nopad_bin ánh xạ tương đương dùng được cho PK; collation gần đúng trên PK/UNIQUE bị từ chối.
func TestMDB_E2E_CollationMapping(t *testing.T) {
	src := startMariaDBImage(t, "mariadb:11.4", "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, "CREATE DATABASE app",
		// mặc định utf8mb4 của 11.4 = uca1400_ai_ci: name/note là cột không khóa.
		"CREATE TABLE app.t_default (id INT PRIMARY KEY, name VARCHAR(30), note TEXT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		// PK dùng nopad_bin (exact), cột thường dùng general_nopad_ci (gần đúng, không khóa).
		"CREATE TABLE app.t_nopadpk (code VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin PRIMARY KEY, "+
			"label VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_nopad_ci, "+
			"ac VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_uca1400_as_cs) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"CREATE TABLE app.t_badpk (name VARCHAR(20) PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		"CREATE TABLE app.t_baduq (id INT PRIMARY KEY, name VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_nopad_ci, UNIQUE KEY uq (name)) ENGINE=InnoDB")
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	var def string
	if err := src.DB.QueryRow("SELECT @@character_set_collations").Scan(&def); err != nil || !strings.Contains(def, "utf8mb4_uca1400_ai_ci") {
		t.Fatalf("giả định sai: mặc định utf8mb4 của 11.4 = %q (%v)", def, err)
	}

	// Hồi quy: MariaDB 11.x có dòng COLLATIONS với ID/CHARACTER_SET_NAME NULL làm LoadCollationCharsets lỗi Scan
	// (sync thoát exit 1). Collation id 2304 = utf8mb4_uca1400_ai_ci.
	if cs, err := readermysql.LoadCollationCharsets(context.Background(), src.DB); err != nil || cs[2304] != "utf8mb4" || cs[45] != "utf8mb4" {
		t.Fatalf("LoadCollationCharsets: id2304=%q id45=%q err=%v", cs[2304], cs[45], err)
	}

	validate := func(name string, include ...string) (string, int) {
		cfg := strandCfg{Name: name, SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
			TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: include, Dir: testDir(t)}
		return runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t))
	}
	for _, tb := range []string{"app.t_badpk", "app.t_baduq"} {
		out, code := validate("mdb-coll-"+strings.TrimPrefix(tb, "app."), tb)
		if code == 0 || !strings.Contains(out, "PRIMARY KEY or UNIQUE") || !strings.Contains(out, "name") {
			t.Errorf("%s: exit %d, want rejection naming the key column\n%s", tb, code, out)
		}
	}
	out, code := validate("mdb-coll-ok", "app.t_default", "app.t_nopadpk")
	if code != 0 {
		t.Fatalf("validate bảng ánh xạ được: exit %d\n%s", code, out)
	}
	for _, w := range []string{
		"app.t_default column name: source collation utf8mb4_uca1400_ai_ci mapped to utf8mb4_0900_ai_ci",
		"app.t_default column note: source collation utf8mb4_uca1400_ai_ci mapped to utf8mb4_0900_ai_ci",
		"app.t_nopadpk column label: source collation utf8mb4_general_nopad_ci mapped to utf8mb4_general_ci",
		"app.t_nopadpk column ac: source collation utf8mb4_uca1400_as_cs mapped to utf8mb4_0900_as_cs",
	} {
		if !strings.Contains(out, w) {
			t.Errorf("validate thiếu cảnh báo %q\n%s", w, out)
		}
	}
	if strings.Contains(out, "code: source collation") {
		t.Errorf("nopad_bin là ánh xạ tương đương, không được cảnh báo\n%s", out)
	}

	src.exec(t,
		"INSERT INTO app.t_default VALUES (1, 'Zoë', 'one'), (2, 'zoe', 'two')",
		"INSERT INTO app.t_nopadpk VALUES ('a', 'L1', 'x'), ('a ', 'L2', 'y'), ('A', 'L3', 'Z')")
	cfg := strandCfg{Name: "e2e-mdb-coll", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.t_default", "app.t_nopadpk"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)

	colColl := func(table, col string) string {
		var c string
		if err := dst.DB.QueryRow("SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='app' AND TABLE_NAME=? AND COLUMN_NAME=?", table, col).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return c
	}
	for _, c := range []struct{ table, col, want string }{
		{"t_default", "name", "utf8mb4_0900_ai_ci"}, {"t_default", "note", "utf8mb4_0900_ai_ci"},
		{"t_nopadpk", "code", "utf8mb4_0900_bin"}, {"t_nopadpk", "label", "utf8mb4_general_ci"}, {"t_nopadpk", "ac", "utf8mb4_0900_as_cs"},
	} {
		if got := colColl(c.table, c.col); got != c.want {
			t.Errorf("%s.%s collation ở đích = %s, want %s", c.table, c.col, got, c.want)
		}
	}
	// Stream: PK có dấu cách cuối ('a' khác 'a ') chạy đúng UPDATE/DELETE theo khóa.
	src.exec(t,
		"INSERT INTO app.t_nopadpk VALUES ('b  ', 'L4', 'q'), ('b', 'L5', 'r')",
		"UPDATE app.t_nopadpk SET label = 'L2u' WHERE code = 'a '",
		"DELETE FROM app.t_nopadpk WHERE code = 'a'",
		"INSERT INTO app.t_default VALUES (3, 'Zoé', 'three')")
	queries := []string{
		"SELECT HEX(code), label, ac FROM app.t_nopadpk ORDER BY HEX(code)",
		"SELECT id, name, note FROM app.t_default ORDER BY id",
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		bad := ""
		for _, q := range queries {
			a, e1 := mdbDump(src.DB, q)
			b, e2 := mdbDump(dst.DB, q)
			if e1 != nil || e2 != nil || a != b {
				bad = fmt.Sprintf("%s: src=%q (%v) dst=%q (%v)", q, a, e1, b, e2)
				break
			}
		}
		if bad == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lệch: %s\n%s", bad, p.tail())
		}
		time.Sleep(300 * time.Millisecond)
	}
	for _, tb := range []string{"app.t_nopadpk", "app.t_default"} {
		out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", cfgPath, "--table", tb, "--mode", "full")
		if code != 0 {
			t.Fatalf("verify %s: exit %d\n%s", tb, code, out)
		}
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("stop: exit %d\n%s", code, p.tail())
	}
}
