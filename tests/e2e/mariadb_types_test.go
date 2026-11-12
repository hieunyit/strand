package e2e

// Kiểu/thuộc tính chỉ có ở MariaDB: INET6/INET4/UUID, cột COMPRESSED, bảng WITH SYSTEM VERSIONING. Mỗi test dựng
// nguồn MariaDB (image cố định theo phiên bản tối thiểu của tính năng) → MySQL 8.4 và chứng minh: snapshot, stream
// INSERT/UPDATE/DELETE, kill -9 + chạy lại, ghi khi Strand dừng, `verify --mode full` đạt, giá trị ở đích bằng nguồn.

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// Nguồn 11.x mặc định utf8mb4_uca1400_ai_ci (đích MySQL không có): test kiểu dữ liệu ghim collation tường minh.
const mdbUTF8Default = "CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"

type mdbTypesRig struct {
	src, dst *mysqlNode
	cfg      strandCfg
	cfgPath  string
	tables   []string
	queries  []string // truy vấn so sánh: chạy y hệt ở nguồn và đích
}

func newMDBTypesRig(t *testing.T, image string, schema []string, tables, queries []string) *mdbTypesRig {
	t.Helper()
	r := &mdbTypesRig{tables: tables, queries: queries}
	r.src = startMariaDBImage(t, image, "src", 1)
	r.dst = startMySQL(t, "mysql:8.4", "dst", 2, nil)
	r.src.exec(t, append([]string{"CREATE DATABASE IF NOT EXISTS app " + mdbUTF8Default}, schema...)...)
	mdbSourceUsers(t, r.src)
	mdbTargetSetup(t, r.dst)
	r.cfg = strandCfg{Name: "e2e-mdbtypes", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, r.src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, r.dst.Addr), Include: []string{"app.*"}, Dir: testDir(t)}
	r.cfgPath = r.cfg.write(t)
	return r
}

func (r *mdbTypesRig) diff() string {
	for _, q := range r.queries {
		a, e1 := mdbDump(r.src.DB, q)
		b, e2 := mdbDump(r.dst.DB, q)
		if e1 != nil || e2 != nil {
			return fmt.Sprintf("%s: query error src=%v dst=%v", q, e1, e2)
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
					return fmt.Sprintf("%s differs (rows src=%d dst=%d) at line %d:\n src: %.300s\n dst: %.300s", q, len(as)-1, len(bs)-1, i, x, y)
				}
			}
		}
	}
	return ""
}

func (r *mdbTypesRig) waitEqual(t *testing.T, p *strandProc, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for {
		d := r.diff()
		if d == "" {
			return
		}
		if p.exited() {
			t.Fatalf("%s: strand thoát (code %d) trước khi bắt kịp: %s\n%s", what, p.exitCode(), d, p.tail())
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: target chưa hội tụ: %s\n%s", what, d, p.tail())
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// run: validate → snapshot (nguồn đã có dữ liệu) → stream (stream) → kill -9 → ghi khi dừng (stopped) → chạy lại →
// verify full từng bảng → lệch cài vào đích phải bị verify phát hiện (corrupt là câu SQL chạy ở đích). Trả về
// output của validate. between được gọi sau mỗi giai đoạn đã hội tụ để test kiểm giá trị cụ thể.
func (r *mdbTypesRig) run(t *testing.T, stream, stopped func(), corrupt string, check func(stage string)) string {
	t.Helper()
	vout, code := runStrandCLI(t, time.Minute, "validate", "-c", r.cfgPath)
	if code != 0 {
		t.Fatalf("validate: exit %d\n%s", code, vout)
	}
	p := startStrand(t, r.cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &r.cfg, p, 3*time.Minute)
	r.waitEqual(t, p, "snapshot")
	check("snapshot")

	stream()
	r.waitEqual(t, p, "stream")
	check("stream")

	p.kill9()
	time.Sleep(time.Second)
	stopped()
	p = startStrand(t, r.cfgPath)
	r.waitEqual(t, p, "restart")
	check("restart")

	for _, tb := range r.tables {
		out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", r.cfgPath, "--table", tb, "--mode", "full")
		if code != 0 {
			t.Fatalf("verify full %s: exit %d\n%s", tb, code, out)
		}
	}
	r.dst.exec(t, corrupt)
	out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", r.cfgPath, "--table", r.tables[0], "--mode", "full")
	if code == 0 || !strings.Contains(strings.ToLower(out), "mismatch") {
		t.Fatalf("lệch cài vào đích không bị phát hiện: exit %d\n%s", code, out)
	}
	if code := p.stop(t); code != 0 {
		t.Fatalf("stop: exit %d\n%s", code, p.tail())
	}
	return vout
}

func (r *mdbTypesRig) dstColumnType(t *testing.T, table, col string) string {
	t.Helper()
	var ct string
	if err := r.dst.DB.QueryRow("SELECT COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='app' AND TABLE_NAME=? AND COLUMN_NAME=?", table, col).Scan(&ct); err != nil {
		t.Fatalf("cột %s.%s ở đích: %v", table, col, err)
	}
	return ct
}

func (r *mdbTypesRig) dstScalar(t *testing.T, q string) string {
	t.Helper()
	var s sql.NullString
	if err := r.dst.DB.QueryRow(q).Scan(&s); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	if !s.Valid {
		return "<NULL>"
	}
	return s.String
}

// Địa chỉ INET6 đại diện cho các dạng viết tắt (IPv4-mapped/compatible, dãy 0 dài nhất, dãy 0 đơn, toàn 0/1...).
var mdbInet6Samples = []string{
	"::", "::1", "::2", "::1.2.3.4", "::ffff:1.2.3.4", "::ffff:0.0.0.0", "::ffff:0:1", "1::", "1::2", "fe80::a",
	"2001:db8::", "2001:db8:0:0:1:0:0:1", "1:0:0:2:0:0:0:3", "1:0:2:0:3:0:4:0", "0:0:0:0:0:1:0:0", "0:0:0:0:0:0:1:2",
	"1:2:3:4:5:6:0:8", "1:2:3:4:5:6:7:8", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "::ffff:255.255.255.255",
	"0:0:0:0:0:ffff:0:0", "1:2:0:0:0:0:0:0", "0:0:3:0:0:0:0:0", "0:1:0:0:0:0:0:0", "::0.1.0.2", "100::", "::100",
}

func randInet6(r *rand.Rand) string {
	var w [8]uint16
	for i := range w {
		switch r.Intn(4) {
		case 0:
		case 1:
			w[i] = uint16(r.Intn(256))
		case 2:
			w[i] = uint16(r.Intn(65536))
		default:
			w[i] = []uint16{0, 1, 0xffff, 0x0102}[r.Intn(4)]
		}
	}
	return fmt.Sprintf("%x:%x:%x:%x:%x:%x:%x:%x", w[0], w[1], w[2], w[3], w[4], w[5], w[6], w[7])
}

// INET6 (10.5+) → VARCHAR(39): chữ giống hệt SELECT của MariaDB, cả ở snapshot và stream (binlog mang 16 byte thô).
func TestMDB_Types_Inet6(t *testing.T) {
	r := newMDBTypesRig(t, "mariadb:10.6", []string{
		"CREATE TABLE app.ip (id INT PRIMARY KEY, ip INET6, ip2 VARCHAR(45), KEY k_ip (ip))  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
	}, []string{"app.ip"}, []string{"SELECT id, ip, ip2, HEX(INET6_ATON(ip)) FROM app.ip ORDER BY id"})
	rnd := rand.New(rand.NewSource(7))
	next := 1
	insert := func(vals []string) {
		ids := make([]int, len(vals))
		for i := range ids {
			ids[i] = next
			next++
		}
		// ip2 là chữ do người dùng gõ; ip2 = ip chuẩn hoá bằng chính MariaDB (cột INET6 tự chuẩn hoá khi lưu).
		var sb strings.Builder
		for i, v := range vals {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "(%d, '%s', NULL)", ids[i], v)
		}
		r.src.exec(t, "INSERT INTO app.ip (id, ip, ip2) VALUES "+sb.String(), "UPDATE app.ip SET ip2 = ip WHERE ip2 IS NULL")
	}
	insert(mdbInet6Samples)
	r.src.exec(t, "INSERT INTO app.ip (id, ip) VALUES (900000, NULL)")
	var many []string
	for i := 0; i < 1500; i++ {
		many = append(many, randInet6(rnd))
	}
	insert(many)

	r.run(t, func() {
		var more []string
		for i := 0; i < 400; i++ {
			more = append(more, randInet6(rnd))
		}
		insert(more)
		r.src.exec(t, "UPDATE app.ip SET ip = '::ffff:9.9.9.9' WHERE id = 3", "UPDATE app.ip SET ip = NULL WHERE id = 4",
			"UPDATE app.ip SET ip = '1:0:0:2:0:0:0:3' WHERE id = 900000", "DELETE FROM app.ip WHERE id BETWEEN 10 AND 30",
			"UPDATE app.ip SET ip2 = ip WHERE id IN (3, 4, 900000)")
	}, func() {
		insert(mdbInet6Samples)
		r.src.exec(t, "UPDATE app.ip SET ip = '::1.2.3.4' WHERE id = 5", "DELETE FROM app.ip WHERE id = 6", "UPDATE app.ip SET ip2 = ip WHERE id = 5")
	}, "UPDATE app.ip SET ip = '::5' ORDER BY id LIMIT 1", func(stage string) {
		if ct := r.dstColumnType(t, "ip", "ip"); ct != "varchar(39)" {
			t.Fatalf("%s: kiểu cột ip ở đích = %s, want varchar(39)", stage, ct)
		}
		// Chữ cụ thể (đối chiếu với SELECT thật của MariaDB).
		if stage == "snapshot" {
			for id, want := range map[int]string{1: "::", 2: "::1", 3: "::2", 4: "::1.2.3.4", 5: "::ffff:1.2.3.4", 6: "::ffff:0.0.0.0", 7: "::ffff:0.0.0.1",
				8: "1::", 13: "1:0:0:2::3", 14: "1::2:0:3:0:4:0"} {
				if got := r.dstScalar(t, fmt.Sprintf("SELECT ip FROM app.ip WHERE id = %d", id)); got != want {
					t.Fatalf("ip id %d = %q, want %q", id, got, want)
				}
			}
		}
		if stage == "snapshot" {
			if got := r.dstScalar(t, "SELECT ip FROM app.ip WHERE id = 900000"); got != "<NULL>" {
				t.Fatalf("NULL ở đích = %q", got)
			}
		}
		if stage == "stream" {
			if got := r.dstScalar(t, "SELECT ip FROM app.ip WHERE id = 3"); got != "::ffff:9.9.9.9" {
				t.Fatalf("id 3 = %q, want ::ffff:9.9.9.9", got)
			}
			if got := r.dstScalar(t, "SELECT ip FROM app.ip WHERE id = 900000"); got != "1:0:0:2::3" {
				t.Fatalf("id 1000 = %q, want 1:0:0:2::3", got)
			}
		}
	})
}

// INET4 (10.10+) → VARCHAR(15); UUID (10.7+) → CHAR(36). Binlog UUID lưu theo thứ tự byte tự nhiên (không đổi thứ tự đoạn).
func TestMDB_Types_Inet4UUID(t *testing.T) {
	r := newMDBTypesRig(t, "mariadb:11.4", []string{
		"CREATE TABLE app.t (id INT PRIMARY KEY, a INET4, u UUID, KEY k_u (u), UNIQUE KEY uk_a (a))  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
	}, []string{"app.t"}, []string{"SELECT id, a, u, INET_ATON(a) FROM app.t ORDER BY id"})
	uuids := []string{
		"550e8400-e29b-41d4-a716-446655440000", // v4
		"6fa459ea-ee8a-11e2-8d5b-0800200c9a66", // v1
		"00000000-0000-0000-0000-000000000000", // nil
		"ffffffff-ffff-ffff-ffff-ffffffffffff", // max
		"a8098c1a-f86e-11da-bd1a-00112444be1e", // v1
		"9073926b-929f-31c2-abc9-fad77ae3e8eb", // v3
		"12345678-1234-4234-8234-123456789abc", // v4
		"00000001-0000-1000-8000-000000000000", // đoạn đầu nhỏ, v1
		"0f000000-0000-4000-8000-0000000000f0", // byte 0x00/0xf0
		"01234567-89ab-1def-8123-456789abcd00", // 0x00 cuối
		"019b0000-0000-7000-8000-000000000000", // v7
	}
	ips := []string{"0.0.0.0", "0.0.0.1", "1.2.3.4", "10.0.0.0", "127.0.0.1", "192.168.0.0", "255.255.255.255", "100.64.0.0", "1.0.0.0", "0.255.0.0", "8.8.8.8"}
	for i := range uuids {
		r.src.exec(t, fmt.Sprintf("INSERT INTO app.t VALUES (%d, '%s', '%s')", i+1, ips[i], uuids[i]))
	}
	r.src.exec(t, "INSERT INTO app.t VALUES (100, NULL, NULL)")
	r.run(t, func() {
		r.src.exec(t, "INSERT INTO app.t VALUES (101, '9.9.9.9', UUID())", "INSERT INTO app.t VALUES (102, '172.16.0.1', '7c9e6679-7425-40de-944b-e07fc1f90ae7')",
			"UPDATE app.t SET u = '00000000-0000-0000-0000-000000000001', a = '1.1.1.1' WHERE id = 1",
			"UPDATE app.t SET u = NULL, a = NULL WHERE id = 2", "DELETE FROM app.t WHERE id IN (3, 4)",
			"UPDATE app.t SET u = '550e8400-e29b-41d4-a716-446655440000' WHERE id = 100")
	}, func() {
		r.src.exec(t, "INSERT INTO app.t VALUES (103, '3.3.3.3', '00000000-0000-1000-8000-0000000000ff')", "DELETE FROM app.t WHERE id = 5",
			"UPDATE app.t SET a = '2.2.2.2' WHERE id = 6")
	}, "UPDATE app.t SET u = '' ORDER BY id LIMIT 1", func(stage string) {
		if ct := r.dstColumnType(t, "t", "a"); ct != "varchar(15)" {
			t.Fatalf("%s: kiểu cột a = %s, want varchar(15)", stage, ct)
		}
		if ct := r.dstColumnType(t, "t", "u"); ct != "char(36)" {
			t.Fatalf("%s: kiểu cột u = %s, want char(36)", stage, ct)
		}
		if stage == "snapshot" {
			for id, want := range map[int]string{1: "550e8400-e29b-41d4-a716-446655440000", 2: "6fa459ea-ee8a-11e2-8d5b-0800200c9a66", 3: "00000000-0000-0000-0000-000000000000"} {
				if got := r.dstScalar(t, fmt.Sprintf("SELECT u FROM app.t WHERE id = %d", id)); got != want {
					t.Fatalf("uuid id %d = %q, want %q", id, got, want)
				}
			}
			if got := r.dstScalar(t, "SELECT a FROM app.t WHERE id = 7"); got != "255.255.255.255" {
				t.Fatalf("inet4 = %q", got)
			}
		}
		if stage == "stream" {
			if got := r.dstScalar(t, "SELECT u FROM app.t WHERE id = 102"); got != "7c9e6679-7425-40de-944b-e07fc1f90ae7" {
				t.Fatalf("uuid v4 stream = %q", got)
			}
			if got := r.dstScalar(t, "SELECT a FROM app.t WHERE id = 1"); got != "1.1.1.1" {
				t.Fatalf("inet4 update = %q", got)
			}
		}
	})
}

// Khóa chính là INET6/INET4/UUID không chứng minh được thứ tự khoảng khóa: từ chối rõ; INET ngoài khóa chính vẫn nhận.
func TestMDB_Types_KeyRejected(t *testing.T) {
	src := startMariaDBImage(t, "mariadb:11.4", "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, "CREATE DATABASE app "+mdbUTF8Default,
		"CREATE TABLE app.pk_u (id UUID PRIMARY KEY, v INT)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"CREATE TABLE app.pk_i6 (id INET6 PRIMARY KEY, v INT)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"CREATE TABLE app.pk_i4 (a INT, b INET4, PRIMARY KEY (a, b))  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"CREATE TABLE app.ok (id INT PRIMARY KEY, u UUID, i INET4, j INET6)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci")
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	for _, tb := range []string{"pk_u", "pk_i6", "pk_i4"} {
		cfg := strandCfg{Name: "rej-" + tb, SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
			TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app." + tb}, Dir: testDir(t)}
		out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t))
		if code == 0 || !strings.Contains(out, "primary key column") || !strings.Contains(out, "key ranges cannot be proven equal") {
			t.Errorf("%s: exit %d, want rejection about primary key column\n%s", tb, code, out)
		}
	}
	cfg := strandCfg{Name: "rej-ok", SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
		TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app.ok"}, Dir: testDir(t)}
	if out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t)); code != 0 {
		t.Errorf("app.ok: exit %d, want 0\n%s", code, out)
	}
}

// randText: chữ UTF-8 hợp lệ gồm n ký tự ngẫu nhiên (nén kém).
func randText(r *rand.Rand, n int) string {
	rs := make([]rune, n)
	for i := range rs {
		rs[i] = rune(0x20 + r.Intn(0x2000))
	}
	return string(rs)
}

// Cột COMPRESSED: binlog mang giá trị nén; đích nhận giá trị gốc ở cột không nén.
func TestMDB_Types_Compressed(t *testing.T) {
	r := newMDBTypesRig(t, "mariadb:10.6", []string{
		`CREATE TABLE app.c (id INT PRIMARY KEY, tx MEDIUMTEXT COMPRESSED, vc VARCHAR(300) COMPRESSED, vs VARCHAR(40) COMPRESSED CHARACTER SET latin1,
			bl BLOB COMPRESSED, mb MEDIUMBLOB COMPRESSED, tail INT)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`,
	}, []string{"app.c"}, []string{"SELECT id, MD5(tx), CHAR_LENGTH(tx), MD5(vc), HEX(CONVERT(vs USING utf8mb4)), MD5(bl), LENGTH(bl), MD5(mb), tail, tx IS NULL, vc IS NULL FROM app.c ORDER BY id"})
	rnd := rand.New(rand.NewSource(3))
	random := func(n int) []byte { b := make([]byte, n); rnd.Read(b); return b }
	big := strings.Repeat("Strand nén dữ liệu ", 5000) // ~100KB lặp, nén tốt (độ dài gốc 3 byte)
	ins := func(id int, tx, vc, vs any, bl, mb []byte, tail int) {
		if _, err := r.src.DB.Exec("INSERT INTO app.c VALUES (?,?,?,?,?,?,?)", id, tx, vc, vs, bl, mb, tail); err != nil {
			t.Fatal(err)
		}
	}
	ins(1, big, strings.Repeat("ab", 140), "café "+strings.Repeat("é", 20), bytes.Repeat([]byte{0, 1, 2}, 4000), bytes.Repeat([]byte{7}, 300000), 11)
	ins(2, "small", "x", "é", []byte{1}, []byte{2}, 12)                       // dưới ngưỡng nén
	ins(3, nil, nil, nil, nil, nil, 13)                                       // NULL
	ins(4, "", "", "", []byte{}, []byte{}, 14)                                // rỗng
	ins(5, strings.Repeat("z", 99), "y", "z", random(2000), random(5000), 15) // ~ngưỡng; ngẫu nhiên không nén được
	ins(6, strings.Repeat("0123456789", 10), strings.Repeat("q", 100), strings.Repeat("a", 40), bytes.Repeat([]byte("x"), 100), bytes.Repeat([]byte("y"), 101), 16)
	ins(7, strings.Repeat("日本語", 3000), strings.Repeat("é", 150), "latin é", random(4000), bytes.Repeat([]byte{0xff}, 70000), 17)
	r.src.exec(t, "INSERT INTO app.c VALUES (8, 'NUL', 'tail', 'x', NULL, NULL, 18)")

	r.run(t, func() {
		ins(20, big+"stream", strings.Repeat("v", 250), strings.Repeat("w", 40), bytes.Repeat([]byte{9, 8}, 30000), random(300), 21)
		ins(21, randText(rnd, 3000), "r", "s", random(100), random(100), 22)
		r.src.exec(t, "UPDATE app.c SET tx = REPEAT('upd ', 2000), tail = tail + 1 WHERE id = 1", "UPDATE app.c SET tx = NULL, bl = NULL WHERE id = 2",
			"UPDATE app.c SET tx = 'now small', vc = '' WHERE id = 7", "UPDATE app.c SET tail = 99 WHERE id = 6", "DELETE FROM app.c WHERE id IN (3, 5)")
	}, func() {
		// column_compression_zlib_wrap=ON: luồng nén bọc zlib (header khác).
		conn, err := r.src.DB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(t.Context(), "SET SESSION column_compression_zlib_wrap=ON"); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(t.Context(), "INSERT INTO app.c VALUES (30, REPEAT('wrap on ', 3000), REPEAT('w', 200), 'z', REPEAT('b', 5000), REPEAT('m', 6000), 31)"); err != nil {
			t.Fatal(err)
		}
		r.src.exec(t, "UPDATE app.c SET tx = REPEAT('after ', 100) WHERE id = 20", "DELETE FROM app.c WHERE id = 21", "INSERT INTO app.c VALUES (31, 'p', 'q', 'r', NULL, NULL, 32)")
	}, "UPDATE app.c SET tail = tail + 1 ORDER BY id LIMIT 1", func(stage string) {
		for _, col := range []string{"tx", "vc", "vs", "bl", "mb"} {
			if got := r.dstScalar(t, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='app' AND TABLE_NAME='c' AND COLUMN_NAME='"+col+"' AND (COLUMN_TYPE LIKE '%compress%' OR EXTRA LIKE '%compress%')"); got != "0" {
				t.Fatalf("%s: cột %s ở đích còn COMPRESSED", stage, col)
			}
		}
		if stage == "snapshot" {
			if got := r.dstScalar(t, "SELECT CHAR_LENGTH(tx) FROM app.c WHERE id = 1"); got != fmt.Sprint(len([]rune(big))) {
				t.Fatalf("độ dài tx id 1 = %s", got)
			}
			if got := r.dstScalar(t, "SELECT tx FROM app.c WHERE id = 2"); got != "small" {
				t.Fatalf("tx id 2 = %q", got)
			}
		}
	})
}

// Bảng WITH SYSTEM VERSIONING: chỉ hàng hiện hành sang bảng thường (không row_start/row_end, không lịch sử).
func TestMDB_Types_SystemVersioned(t *testing.T) {
	for _, image := range []string{"mariadb:10.6", "mariadb:11.4"} {
		t.Run(strings.ReplaceAll(image, ":", "_"), func(t *testing.T) { testMDBSystemVersioned(t, image) })
	}
}

func testMDBSystemVersioned(t *testing.T, image string) {
	r := newMDBTypesRig(t, image, []string{
		`CREATE TABLE app.sv (id INT PRIMARY KEY, name VARCHAR(30), n INT, ip INET6 NULL, UNIQUE KEY uk_name (name))  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci WITH SYSTEM VERSIONING`,
		`CREATE TABLE app.svp (id BIGINT AUTO_INCREMENT PRIMARY KEY, v VARCHAR(20), n INT)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci WITH SYSTEM VERSIONING
			PARTITION BY SYSTEM_TIME INTERVAL 1 HOUR (PARTITION p_hist HISTORY, PARTITION p_cur CURRENT)`,
		`CREATE TABLE app.svx (a INT, b INT, v INT, PRIMARY KEY (a, b))  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci WITH SYSTEM VERSIONING`,
	}, []string{"app.sv", "app.svp", "app.svx"}, []string{
		"SELECT id, name, n, ip FROM app.sv ORDER BY id",
		"SELECT id, v, n FROM app.svp ORDER BY id",
		"SELECT a, b, v FROM app.svx ORDER BY a, b",
	})
	// Lịch sử có sẵn trước snapshot (đích không được thấy lịch sử).
	r.src.exec(t,
		"INSERT INTO app.sv VALUES (1, 'a', 1, '::1'), (2, 'b', 2, NULL), (3, 'c', 3, '::ffff:1.2.3.4'), (4, 'd', 4, NULL)",
		"UPDATE app.sv SET n = n + 10 WHERE id IN (1, 2)", "UPDATE app.sv SET name = 'a2' WHERE id = 1", "DELETE FROM app.sv WHERE id = 4",
		"INSERT INTO app.svp (v, n) VALUES ('x', 1), ('y', 2), ('z', 3)", "UPDATE app.svp SET n = 20 WHERE v = 'y'", "DELETE FROM app.svp WHERE v = 'z'",
		"INSERT INTO app.svx VALUES (1,1,1), (1,2,2), (2,1,3)", "UPDATE app.svx SET v = v * 5 WHERE a = 1", "DELETE FROM app.svx WHERE a = 2")
	var hist int
	if err := r.src.DB.QueryRow("SELECT COUNT(*) FROM app.sv FOR SYSTEM_TIME ALL").Scan(&hist); err != nil || hist <= 3 {
		t.Fatalf("nguồn không có lịch sử: %d %v", hist, err)
	}

	vout := r.run(t, func() {
		r.src.exec(t,
			"INSERT INTO app.sv VALUES (5, 'e', 5, '::2'), (6, 'f', 6, NULL)",
			"UPDATE app.sv SET n = n * 2, ip = '::7' WHERE id IN (1, 5)",                             // nhiều hàng một câu
			"UPDATE app.sv SET name = 'c2' WHERE id = 3", "UPDATE app.sv SET n = n + 1 WHERE id = 3", // update liên tiếp cùng khóa
			"DELETE FROM app.sv WHERE id = 2",
			"INSERT INTO app.sv VALUES (2, 'b-again', 22, NULL)", // xóa rồi chèn lại cùng khóa
			"UPDATE app.sv SET n = n WHERE id = 6",               // update không đổi giá trị
			"INSERT INTO app.svp (v, n) VALUES ('w', 4)", "UPDATE app.svp SET n = n + 1", "DELETE FROM app.svp WHERE v = 'x'",
			"INSERT INTO app.svx VALUES (3,1,1), (3,2,2)", "UPDATE app.svx SET a = 9 WHERE a = 3 AND b = 1", // đổi khóa chính
			"DELETE FROM app.svx WHERE a = 1 AND b = 2")
		tx, err := r.src.DB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{"INSERT INTO app.sv VALUES (7, 'g', 7, NULL)", "UPDATE app.sv SET n = 70 WHERE id = 7", "DELETE FROM app.sv WHERE id = 7",
			"INSERT INTO app.sv VALUES (8, 'h', 8, NULL)", "UPDATE app.sv SET n = 80 WHERE id = 8"} {
			if _, err := tx.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}, func() {
		r.src.exec(t, "UPDATE app.sv SET n = 1000 WHERE id = 8", "DELETE FROM app.sv WHERE id = 6", "INSERT INTO app.sv VALUES (9, 'i', 9, '::9')",
			"INSERT INTO app.svp (v, n) VALUES ('late', 9)", "UPDATE app.svx SET v = 77 WHERE a = 9",
			"DELETE HISTORY FROM app.sv") // xóa lịch sử: không ảnh hưởng đích
	}, "UPDATE app.sv SET n = n + 1 ORDER BY id LIMIT 1", func(stage string) {
		for _, tb := range []string{"sv", "svp", "svx"} {
			if got := r.dstScalar(t, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='app' AND TABLE_NAME='"+tb+"' AND COLUMN_NAME IN ('row_start','row_end')"); got != "0" {
				t.Fatalf("%s: bảng %s ở đích có cột row_start/row_end", stage, tb)
			}
			if got := r.dstScalar(t, "SELECT TABLE_TYPE FROM information_schema.TABLES WHERE TABLE_SCHEMA='app' AND TABLE_NAME='"+tb+"'"); got != "BASE TABLE" {
				t.Fatalf("%s: %s ở đích là %s", stage, tb, got)
			}
		}
		var cur, dcnt int
		_ = r.src.DB.QueryRow("SELECT COUNT(*) FROM app.sv").Scan(&cur)
		_ = r.dst.DB.QueryRow("SELECT COUNT(*) FROM app.sv").Scan(&dcnt)
		if cur != dcnt || cur == 0 {
			t.Fatalf("%s: sv hiện hành nguồn=%d đích=%d", stage, cur, dcnt)
		}
		if stage == "stream" {
			if got := r.dstScalar(t, "SELECT CONCAT(name, '/', n, '/', ip) FROM app.sv WHERE id = 1"); got != "a2/22/::7" {
				t.Fatalf("sv id 1 = %q, want a2/22/::7", got)
			}
			if got := r.dstScalar(t, "SELECT COUNT(*) FROM app.sv WHERE id IN (7)"); got != "0" {
				t.Fatalf("hàng sv 7 (chèn rồi xóa trong Txn) còn ở đích")
			}
		}
	})
	if !strings.Contains(vout, "only current rows") {
		t.Errorf("validate không cảnh báo lịch sử không được chuyển:\n%s", vout)
	}
}

// Hình dạng system-versioning chưa chuyển đúng được: từ chối rõ ràng.
func TestMDB_Types_SystemVersionedShapesRejected(t *testing.T) {
	src := startMariaDBImage(t, "mariadb:10.6", "src", 1)
	dst := startMySQL(t, "mysql:8.4", "dst", 2, nil)
	src.exec(t, "CREATE DATABASE app "+mdbUTF8Default,
		"CREATE TABLE app.explicit_p (id INT PRIMARY KEY, v INT, s TIMESTAMP(6) GENERATED ALWAYS AS ROW START, e TIMESTAMP(6) GENERATED ALWAYS AS ROW END, PERIOD FOR SYSTEM_TIME(s, e))  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci WITH SYSTEM VERSIONING",
		"CREATE TABLE app.app_time (id INT PRIMARY KEY, s DATE, e DATE, PERIOD FOR p(s, e))  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci")
	mdbSourceUsers(t, src)
	mdbTargetSetup(t, dst)
	for _, c := range []struct{ tb, want string }{{"explicit_p", "system-versioning period columns explicitly"}, {"app_time", "PERIOD FOR"}} {
		cfg := strandCfg{Name: "svrej-" + c.tb, SourceDSN: fmt.Sprintf("strand:%s@tcp(%s)/", mdbPass, src.Addr),
			TargetDSN: fmt.Sprintf("strand:%s@tcp(%s)/strand", mdbPass, dst.Addr), Include: []string{"app." + c.tb}, Dir: testDir(t)}
		out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfg.write(t))
		if code == 0 || !strings.Contains(out, c.want) {
			t.Errorf("%s: exit %d, want rejection with %q\n%s", c.tb, code, c.want, out)
		}
	}
}
