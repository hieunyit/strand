package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// Nhóm test nguồn MySQL 5.7 (GTID ON, ROW, FULL, không có binlog_row_metadata) → MySQL 8.4 / PostgreSQL.
// Image nguồn: STRAND_E2E_M57_IMAGE (mặc định mysql:5.7); image đích MySQL: STRAND_E2E_M57_TARGET_IMAGE
// (mặc định mysql:8.4). Không dùng mysqlImage() vì biến STRAND_E2E_MYSQL_IMAGE có thể trỏ cả hai về 5.7.

func m57TargetImage() string {
	if v := os.Getenv("STRAND_E2E_M57_TARGET_IMAGE"); v != "" {
		return v
	}
	return "mysql:8.4"
}

// startM57Source khởi động nguồn 5.7 và xác nhận đúng dòng phiên bản + cấu hình binlog.
func startM57Source(t *testing.T, alias string, serverID int, nw *testcontainers.DockerNetwork, extra ...string) *mysqlNode {
	t.Helper()
	n := startMySQL(t, mysql57Image(), alias, serverID, nw, extra...)
	var ver string
	if err := n.DB.QueryRow("SELECT @@version").Scan(&ver); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ver, "5.7") {
		t.Fatalf("nguồn không phải MySQL 5.7: version=%s image=%s", ver, mysql57Image())
	}
	t.Logf("nguồn %s: MySQL %s", alias, ver)
	return n
}

// ----------------------------------------------------------------------------
// Lược đồ m57: kiểu dữ liệu 5.7, khóa tổ hợp, khóa VARCHAR (đường PK tổng quát), UNIQUE phụ
// ----------------------------------------------------------------------------

var m57SchemaSQL = []string{
	"CREATE DATABASE IF NOT EXISTS m57",
	`CREATE TABLE IF NOT EXISTS m57.types (
		id BIGINT PRIMARY KEY,
		i32 INT NOT NULL,
		u32 INT UNSIGNED NOT NULL,
		i64 BIGINT NOT NULL,
		u64 BIGINT UNSIGNED NOT NULL,
		su SMALLINT UNSIGNED,
		ti TINYINT,
		dec1 DECIMAL(20,4),
		dec2 DECIMAL(10,0),
		dt DATETIME(6),
		ts TIMESTAMP(6) NULL,
		d DATE,
		tm TIME(3),
		vc VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
		lat VARCHAR(50) CHARACTER SET latin1,
		en ENUM('small','medium','large'),
		st SET('a','b','c','d'),
		js JSON,
		bl BLOB,
		ucode VARCHAR(32) CHARACTER SET ascii NOT NULL,
		ver BIGINT UNSIGNED NOT NULL,
		UNIQUE KEY uk_ucode (ucode)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE IF NOT EXISTS m57.cpk (
		a INT NOT NULL,
		b VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
		c BIGINT,
		d DECIMAL(10,2),
		ver BIGINT UNSIGNED NOT NULL,
		PRIMARY KEY (a, b)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE IF NOT EXISTS m57.vpk (
		code VARCHAR(60) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY,
		note VARCHAR(100) CHARACTER SET latin1,
		n BIGINT UNSIGNED,
		ver BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
}

// m57PKCols: vị trí cột khóa của từng bảng trong SELECT * (dùng để ghép khóa so sánh).
var m57PKCols = map[string][]int{
	"m57.types": {0},
	"m57.cpk":   {0, 1},
	"m57.vpk":   {0},
}

var m57Tables = []string{"m57.types", "m57.cpk", "m57.vpk"}

// m57Gen sinh dữ liệu biên: uint64 > MaxInt64, số âm, đa byte, latin1, BLOB có byte 0x00...
type m57Gen struct{}

func (m57Gen) u64(r *rand.Rand) string {
	switch r.Intn(5) {
	case 0:
		return "18446744073709551615"
	case 1:
		return "9223372036854775808"
	case 2:
		return strconv.FormatUint(1<<63+uint64(r.Int63n(1<<40)), 10)
	case 3:
		return "0"
	}
	return strconv.FormatUint(r.Uint64(), 10)
}

func (m57Gen) i64(r *rand.Rand) int64 {
	switch r.Intn(6) {
	case 0:
		return 9223372036854775807
	case 1:
		return -9223372036854775808
	}
	return r.Int63() - r.Int63()
}

func (m57Gen) dec(r *rand.Rand) string {
	sign := ""
	if r.Intn(2) == 0 {
		sign = "-"
	}
	return fmt.Sprintf("%s%d%08d.%04d", sign, r.Intn(10000000), r.Intn(100000000), r.Intn(10000))
}

func (m57Gen) dt(r *rand.Rand) string {
	switch r.Intn(8) {
	case 0:
		return "1000-01-01 00:00:00.000000"
	case 1:
		return "9999-12-31 23:59:59.999999"
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", 1971+r.Intn(60), 1+r.Intn(12), 1+r.Intn(28), r.Intn(24), r.Intn(60), r.Intn(60), r.Intn(1000000))
}

func (g m57Gen) ts(r *rand.Rand) any {
	switch r.Intn(6) {
	case 0:
		return nil
	case 1:
		return "1970-01-01 00:00:01.000000"
	case 2:
		return "2038-01-19 03:14:07.999999"
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", 1971+r.Intn(60), 1+r.Intn(12), 1+r.Intn(28), r.Intn(24), r.Intn(60), r.Intn(60), r.Intn(1000000))
}

func (m57Gen) vc(r *rand.Rand) string {
	pool := []string{"", "plain ascii", "Tiếng Việt có dấu", "日本語テキスト", "emoji 😀🚀 ok", "quote ' \" \\ back", "tab\tnl\nx"}
	return pool[r.Intn(len(pool))] + fmt.Sprintf("-%d", r.Intn(1000))
}

func (m57Gen) lat(r *rand.Rand) any {
	pool := []string{"café", "naïve façade", "Zoë ü ö ä ß", "", "ascii only"}
	if r.Intn(10) == 0 {
		return nil
	}
	return pool[r.Intn(len(pool))]
}

func (m57Gen) en(r *rand.Rand) any {
	if r.Intn(10) == 0 {
		return nil
	}
	return []string{"small", "medium", "large"}[r.Intn(3)]
}

func (m57Gen) st(r *rand.Rand) string {
	var parts []string
	for _, v := range []string{"a", "b", "c", "d"} {
		if r.Intn(2) == 0 {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, ",")
}

func (m57Gen) js(r *rand.Rand) any {
	switch r.Intn(6) {
	case 0:
		return nil
	case 1:
		return `[1, 2.5, "x", null, true, {"k": "v"}]`
	case 2:
		return fmt.Sprintf(`{"big": 18446744073709551615, "neg": -%d, "s": "ü😀"}`, r.Intn(1000))
	}
	return fmt.Sprintf(`{"a": {"b": [%d, %d]}, "t": "x%d", "f": %d.5}`, r.Intn(100), r.Intn(100), r.Intn(50), r.Intn(9))
}

func (m57Gen) bl(r *rand.Rand) any {
	n := r.Intn(300)
	if r.Intn(8) == 0 {
		return []byte{}
	}
	b := make([]byte, n)
	r.Read(b)
	if n > 3 {
		b[1] = 0
	}
	return b
}

const m57TypesInsert = `INSERT INTO m57.types (id,i32,u32,i64,u64,su,ti,dec1,dec2,dt,ts,d,tm,vc,lat,en,st,js,bl,ucode,ver)
	VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,UUID_SHORT())`

func m57TypesArgs(r *rand.Rand, id int64) []any {
	var g m57Gen
	return []any{id, int32(r.Uint32()), strconv.FormatUint(uint64(r.Uint32()), 10), g.i64(r), g.u64(r),
		r.Intn(65536), r.Intn(256) - 128, g.dec(r), strconv.Itoa(r.Intn(1000000000)),
		g.dt(r), g.ts(r), fmt.Sprintf("%04d-%02d-%02d", 1971+r.Intn(60), 1+r.Intn(12), 1+r.Intn(28)),
		fmt.Sprintf("%02d:%02d:%02d.%03d", r.Intn(800)-400+400, r.Intn(60), r.Intn(60), r.Intn(1000)),
		g.vc(r), g.lat(r), g.en(r), g.st(r), g.js(r), g.bl(r), fmt.Sprintf("U%d", id)}
}

func m57Preload(t *testing.T, db *sql.DB, seed int64, types, cpk, vpk int) {
	t.Helper()
	r := rand.New(rand.NewSource(seed))
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= types; i++ {
		if _, err := tx.Exec(m57TypesInsert, m57TypesArgs(r, int64(i))...); err != nil {
			t.Fatalf("preload m57.types %d: %v", i, err)
		}
	}
	for i := 1; i <= cpk; i++ {
		if _, err := tx.Exec("INSERT INTO m57.cpk VALUES (?, ?, ?, ?, UUID_SHORT())", i%50, fmt.Sprintf("k%d-é", i), i*7, fmt.Sprintf("%d.%02d", i, i%100)); err != nil {
			t.Fatalf("preload m57.cpk: %v", err)
		}
	}
	for i := 1; i <= vpk; i++ {
		if _, err := tx.Exec("INSERT INTO m57.vpk VALUES (?, ?, ?, UUID_SHORT())", fmt.Sprintf("code-%d-ü😀", i), "café", strconv.FormatUint(1<<63+uint64(i), 10)); err != nil {
			t.Fatalf("preload m57.vpk: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// m57Writer: tải ghi song song lên 3 bảng m57 (chèn, sửa, xóa, đổi PK).
type m57Writer struct {
	db      *sql.DB
	rng     *rand.Rand
	nextID  atomic.Int64
	nextK   atomic.Int64
	ops     atomic.Int64
	errs    atomic.Int64
	stopped atomic.Bool
	wg      sync.WaitGroup
}

func newM57Writer(db *sql.DB, seed int64, firstID int64) *m57Writer {
	w := &m57Writer{db: db, rng: rand.New(rand.NewSource(seed))}
	w.nextID.Store(firstID)
	w.nextK.Store(firstID)
	return w
}

func (w *m57Writer) Start(clients int, span int64) {
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

func (w *m57Writer) Stop() {
	w.stopped.Store(true)
	w.wg.Wait()
}

func (w *m57Writer) one(r *rand.Rand, span int64) error {
	var g m57Gen
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	id := 1 + r.Int63n(span)
	switch k := r.Intn(100); {
	case k < 15:
		nid := w.nextID.Add(1)
		_, err = tx.Exec(m57TypesInsert, m57TypesArgs(r, nid)...)
	case k < 40:
		_, err = tx.Exec(`UPDATE m57.types SET i64 = ?, u64 = ?, dec1 = ?, dt = ?, ts = ?, vc = ?, lat = ?, en = ?, st = ?, js = ?, bl = ?, ver = UUID_SHORT() WHERE id = ?`,
			g.i64(r), g.u64(r), g.dec(r), g.dt(r), g.ts(r), g.vc(r), g.lat(r), g.en(r), g.st(r), g.js(r), g.bl(r), id)
	case k < 47:
		_, err = tx.Exec(`UPDATE m57.types SET js = JSON_SET(COALESCE(js, '{}'), '$.n', ?), ver = UUID_SHORT() WHERE id = ?`, r.Intn(1000), id)
	case k < 52:
		_, err = tx.Exec("DELETE FROM m57.types WHERE id = ?", 1+r.Int63n(w.nextID.Load()))
	case k < 62: // cpk: upsert / sửa
		a := r.Intn(50)
		_, err = tx.Exec("INSERT INTO m57.cpk VALUES (?, ?, ?, ?, UUID_SHORT()) ON DUPLICATE KEY UPDATE c = VALUES(c) + 1, d = VALUES(d), ver = UUID_SHORT()",
			a, fmt.Sprintf("k%d-é", r.Int63n(span)), r.Int63n(1<<40), fmt.Sprintf("%d.%02d", r.Intn(1000), r.Intn(100)))
	case k < 68: // cpk: đổi PK
		_, err = tx.Exec("UPDATE m57.cpk SET b = ?, ver = UUID_SHORT() WHERE a = ? LIMIT 1", fmt.Sprintf("m%d-é", w.nextK.Add(1)), r.Intn(50))
	case k < 72:
		_, err = tx.Exec("DELETE FROM m57.cpk WHERE a = ? LIMIT 2", r.Intn(50))
	case k < 82: // vpk: chèn / sửa
		_, err = tx.Exec("INSERT INTO m57.vpk VALUES (?, ?, ?, UUID_SHORT()) ON DUPLICATE KEY UPDATE n = n + 1, note = VALUES(note), ver = UUID_SHORT()",
			fmt.Sprintf("code-%d-ü😀", r.Int63n(span)), g.lat(r), g.u64(r))
	case k < 88: // vpk: đổi PK
		_, err = tx.Exec("UPDATE m57.vpk SET code = ?, ver = UUID_SHORT() WHERE code = ?",
			fmt.Sprintf("moved-%d-é", w.nextK.Add(1)), fmt.Sprintf("code-%d-ü😀", r.Int63n(span)))
	case k < 92:
		_, err = tx.Exec("DELETE FROM m57.vpk WHERE code = ?", fmt.Sprintf("code-%d-ü😀", r.Int63n(span)))
	default: // Txn nhiều bảng
		for j := 0; j < 3 && err == nil; j++ {
			_, err = tx.Exec("UPDATE m57.types SET u32 = u32 DIV 2, i32 = -i32, ver = UUID_SHORT() WHERE id = ?", 1+r.Int63n(span))
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

// m57Dump đọc toàn bảng: khóa = giá trị các cột PK nối lại, giá trị = mọi cột dạng text/byte.
func m57Dump(t *testing.T, db *sql.DB, table string, pk []int) map[string][]string {
	t.Helper()
	rows, err := db.Query("SELECT * FROM " + table)
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	out := map[string][]string{}
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			if v.Valid {
				row[i] = v.String
			} else {
				row[i] = "<NULL>"
			}
		}
		var key []string
		for _, i := range pk {
			key = append(key, row[i])
		}
		out[strings.Join(key, "\x00")] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// m57Compare so từng row giữa nguồn 5.7 và đích MySQL 8.x theo khóa, in tối đa 15 lệch.
func m57Compare(t *testing.T, src, dst *mysqlNode, tables ...string) {
	t.Helper()
	total := 0
	var bad []string
	for _, tb := range tables {
		pk := m57PKCols[tb]
		s := m57Dump(t, src.DB, tb, pk)
		d := m57Dump(t, dst.DB, tb, pk)
		total += len(s)
		for k, sv := range s {
			dv, ok := d[k]
			switch {
			case !ok:
				bad = append(bad, fmt.Sprintf("%s %q missing on target", tb, k))
			case strings.Join(sv, "\x01") != strings.Join(dv, "\x01"):
				bad = append(bad, fmt.Sprintf("%s %q differs:\n  src %q\n  dst %q", tb, k, sv, dv))
			}
		}
		for k := range d {
			if _, ok := s[k]; !ok {
				bad = append(bad, fmt.Sprintf("%s %q extra on target", tb, k))
			}
		}
	}
	if len(bad) > 0 {
		sort.Strings(bad)
		n := len(bad)
		if n > 15 {
			bad = bad[:15]
		}
		t.Fatalf("m57 final state mismatch (%d lệch), first:\n%s", n, strings.Join(bad, "\n"))
	}
	t.Logf("m57 final state: %d rows identical in %d tables", total, len(tables))
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

// Smoke của harness: nguồn 5.7 lên được với cờ riêng của 5.7 và đúng cấu hình binlog.
func TestM57_E2E_HarnessSmoke(t *testing.T) {
	src := startM57Source(t, "src", 1, nil)
	var gtid, format, image string
	if err := src.DB.QueryRow("SELECT @@GLOBAL.gtid_mode, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image").Scan(&gtid, &format, &image); err != nil {
		t.Fatal(err)
	}
	if gtid != "ON" || format != "ROW" || image != "FULL" {
		t.Fatalf("cấu hình binlog 5.7: gtid_mode=%s binlog_format=%s binlog_row_image=%s", gtid, format, image)
	}
	var v string
	if err := src.DB.QueryRow("SELECT @@GLOBAL.log_bin").Scan(&v); err != nil || v != "1" {
		t.Fatalf("log_bin=%q err=%v", v, err)
	}
	createSchema(t, src)
	src.exec(t, m57SchemaSQL...)
	m57Preload(t, src.DB, 1, 50, 20, 20)
	if g := src.gtidExecuted(t); g == "" {
		t.Fatalf("gtid_executed rỗng sau khi ghi")
	}
}

// 5.7 → 8.4: snapshot logic dưới tải + CDC; mọi nhóm kiểu 5.7; so từng row; verify --mode full sạch.
func TestM57_E2E_CDCAndSnapshotToMySQL8(t *testing.T) {
	seed := seedFromEnv(t)
	src := startM57Source(t, "src", 1, nil)
	dst := startMySQL(t, m57TargetImage(), "dst", 2, nil)
	createSchema(t, src, dst) // app: có sẵn ở cả hai phía; m57: để Strand tự tạo ở đích (create_schema auto)
	src.exec(t, m57SchemaSQL...)

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 2000)
	const nTypes = 3000
	m57Preload(t, src.DB, seed, nTypes, 600, 600)
	wl.Start(4, 2000)
	defer wl.Stop()
	mw := newM57Writer(src.DB, seed+7, nTypes+1000)
	mw.Start(4, nTypes)
	defer mw.Stop()

	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	cfg := strandCfg{Name: "e2e-m57", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*", "m57.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	waitSynced(t, &cfg, p, 5*time.Minute)
	t.Logf("snapshot xong dưới tải; đang stream")
	time.Sleep(time.Duration(envInt("STRAND_E2E_LOAD_SECONDS", 20)) * time.Second)
	wl.Stop()
	mw.Stop()
	t.Logf("tải: app %d ops (%d lỗi client), m57 %d ops (%d lỗi client)", wl.ops.Load(), wl.errs.Load(), mw.ops.Load(), mw.errs.Load())

	waitCaughtUp(t, src, dst, &cfg, p, 4*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d khi SIGTERM\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, appTables...)
	m57Compare(t, src, dst, m57Tables...)
	checkNoOrphans(t, dst)
	var n int
	_ = dst.DB.QueryRow("SELECT COUNT(*) FROM app.ignored").Scan(&n)
	if n != 0 {
		t.Fatalf("bảng bị loại app.ignored có %d row ở đích", n)
	}

	out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", cfgPath, "--mode", "full", "--rate", "2000")
	if code != 0 {
		t.Fatalf("strand verify --mode full exit %d:\n%s", code, out)
	}
	t.Logf("verify --mode full: %s", strings.TrimSpace(lastLine(out)))
}

// DDL trên nguồn 5.7 trong lúc stream: hợp đồng (internal/pipeline/ddl.go, 02 §12) là dừng
// schema_change_pending (exit 10) khi schema đích chưa khớp; sau khi DBA áp DDL ở đích và chạy lại
// thì đi tiếp đúng từ DDL đó, không mất row. Giai đoạn DROP COLUMN có row cũ + row mới nằm
// chung binlog quanh DDL (nguồn 5.7 không có metadata trong TABLE_MAP).
func TestM57_E2E_AlterTableWhileStreaming(t *testing.T) {
	seed := seedFromEnv(t)
	src := startM57Source(t, "src", 1, nil)
	dst := startMySQL(t, m57TargetImage(), "dst", 2, nil)
	createSchema(t, src, dst)
	const altDDL = "CREATE TABLE app.alt (id BIGINT PRIMARY KEY, v VARCHAR(20) NOT NULL, u BIGINT UNSIGNED NOT NULL)"
	src.exec(t, altDDL)
	dst.exec(t, altDDL)
	for i := 1; i <= 300; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.alt VALUES (%d, 'v%d', 18446744073709551615)", i, i))
	}
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 1000)
	wl.Start(3, 1000)
	defer wl.Stop()

	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	cfg := strandCfg{Name: "e2e-m57-ddl", CreateSchema: "off", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	waitSynced(t, &cfg, p, 3*time.Minute)

	waitPending := func(p *strandProc, what string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Minute)
		for !p.exited() && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}
		if !p.exited() || p.exitCode() != 10 || !strings.Contains(p.output.String(), "schema_change_pending") {
			t.Fatalf("%s: mong exit 10 schema_change_pending, exited=%v code=%d\n%s", what, p.exited(), p.exitCode(), p.tail())
		}
	}

	// 1. ADD COLUMN khi đích chưa áp: dừng exit 10; chạy lại vẫn dừng.
	src.exec(t, "ALTER TABLE app.alt ADD COLUMN w INT NULL")
	src.exec(t, "INSERT INTO app.alt VALUES (1001, 'b', 1, 7)", "UPDATE app.alt SET w = 5 WHERE id < 20")
	waitPending(p, "ADD COLUMN khi đích chưa áp")
	p2 := startStrand(t, cfgPath)
	if c := p2.exitCode(); c != 10 {
		t.Fatalf("chạy lại trước khi đích áp DDL: exit %d, want 10\n%s", c, p2.tail())
	}
	// DBA áp ADD COLUMN ở đích: đi tiếp, row có cột mới tới đủ.
	dst.exec(t, "ALTER TABLE app.alt ADD COLUMN w INT NULL")
	p3 := startStrand(t, cfgPath)
	waitCaughtUp(t, src, dst, &cfg, p3, 3*time.Minute)
	compareFinal(t, src, dst, "app.alt")

	// 2. DROP COLUMN: dừng Strand, ghi row theo schema cũ, DDL, rồi ghi row theo schema mới.
	if code := p3.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p3.tail())
	}
	for i := 2001; i <= 2100; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.alt VALUES (%d, 'old%d', %d, %d)", i, i, i, i%9))
	}
	src.exec(t, "ALTER TABLE app.alt DROP COLUMN w")
	for i := 3001; i <= 3100; i++ {
		src.exec(t, fmt.Sprintf("INSERT INTO app.alt VALUES (%d, 'new%d', %d)", i, i, i))
	}
	p4 := startStrand(t, cfgPath)
	waitPending(p4, "DROP COLUMN khi đích chưa áp")
	// Row trước DDL phải đã tới đích (đi tiếp đúng đến DDL, không vượt qua).
	var cnt int
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.alt WHERE id BETWEEN 2001 AND 2100").Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 100 {
		t.Fatalf("row trước DROP COLUMN ở đích: %d, want 100\n%s", cnt, p4.tail())
	}
	if err := dst.DB.QueryRow("SELECT COUNT(*) FROM app.alt WHERE id BETWEEN 3001 AND 3100").Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 0 {
		t.Fatalf("row sau DROP COLUMN đã tới đích (%d) dù DDL chưa được áp", cnt)
	}
	dst.exec(t, "ALTER TABLE app.alt DROP COLUMN w")
	p5 := startStrand(t, cfgPath)
	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p5, 3*time.Minute)
	if code := p5.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p5.tail())
	}
	compareFinal(t, src, dst, append(append([]string{}, appTables...), "app.alt")...)
}

// Seed bằng mysqldump của chính 5.7 (--master-data=2, --set-gtid-purged=ON) rồi CDC bắt kịp:
// 0 lượt đọc dữ liệu bảng trên nguồn trong pha seed + catch-up, trạng thái cuối khớp.
func TestM57_E2E_SeedMysqldumpToMySQL8(t *testing.T) {
	seed := seedFromEnv(t)
	ctx := context.Background()
	nw, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nw.Remove(context.Background()) })

	src := startM57Source(t, "src", 1, nw)
	dst := startMySQL(t, m57TargetImage(), "dst", 2, nw)
	createSchema(t, src)
	src.exec(t, m57SchemaSQL...)
	const nTypes = 2000
	m57Preload(t, src.DB, seed, nTypes, 300, 300)

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 3000)
	wl.Start(4, 3000)
	defer wl.Stop()
	time.Sleep(2 * time.Second)

	dir := testDir(t)
	dumpFile := filepath.Join(dir, "dump.sql")
	srcID, dstID := src.c.GetContainerID(), dst.c.GetContainerID()
	// mysqldump 5.7: --master-data (không có --source-data như 8.0.26+).
	runDockerCmd(t, 2*time.Minute, "exec", srcID, "sh", "-c",
		"mysqldump -uroot -pdev --single-transaction --master-data=2 --set-gtid-purged=ON --databases app m57 > /tmp/dump.sql")
	runDockerCmd(t, time.Minute, "cp", srcID+":/tmp/dump.sql", dumpFile)
	runDockerCmd(t, time.Minute, "cp", dumpFile, dstID+":/tmp/dump.sql")
	runDockerCmd(t, 2*time.Minute, "exec", dstID, "sh", "-c",
		"mysql -uroot -pdev -e 'RESET BINARY LOGS AND GTIDS' && mysql -uroot -pdev < /tmp/dump.sql")
	dst.exec(t, "SET SESSION sql_log_bin=0; CREATE DATABASE IF NOT EXISTS strand; SET SESSION sql_log_bin=1")

	wl.Stop()
	readsBefore := sourceTableReads(t, src)

	manifest := filepath.Join(dir, "seed.json")
	out, code := runStrandCLI(t, time.Minute, "seed", "plan", "--type", "mysqldump", "--backup", dumpFile,
		"--source", src.RootDSN, "--target", dst.RootDSN, "--out", manifest)
	if code != 0 || !strings.Contains(out, "WARNING: mysqldump") {
		t.Fatalf("seed plan: exit %d\n%s", code, out)
	}
	out, code = runStrandCLI(t, 5*time.Minute, "seed", "apply", "--manifest", manifest, "--target", dst.RootDSN,
		"--spool", filepath.Join(dir, "state", "prefetch"), "--source", src.RootDSN)
	if code != 0 {
		t.Fatalf("seed apply: exit %d\n%s", code, out)
	}

	cfg := strandCfg{Name: "e2e-m57-dump", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*", "m57.*"}, Exclude: []string{"app.ignored"}, Dir: dir, Bootstrap: "seed"}
	p := startStrand(t, cfg.write(t), "--start-at", "seed", "--seed-manifest", manifest)

	var readsSeed int64 = -1
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		r := sourceTableReads(t, src)
		if strings.Contains(p.output.String(), "post-seed verify started") {
			if readsSeed < 0 {
				readsSeed = readsBefore
			}
			break
		}
		readsSeed = r
		time.Sleep(100 * time.Millisecond)
	}
	if readsSeed < 0 {
		t.Fatalf("post-seed verify không bắt đầu\n%s", p.tail())
	}
	if d := readsSeed - readsBefore; d != 0 {
		t.Fatalf("seed + catch-up đọc %d row dữ liệu bảng trên nguồn; want 0", d)
	}
	t.Logf("seed plan + apply + catch-up: 0 lượt đọc dữ liệu bảng trên nguồn 5.7")

	wl2 := newWorkload(src.DB, seed+1)
	wl2.nextID = 2_000_000
	wl2.Start(4, 3000)
	mw := newM57Writer(src.DB, seed+2, nTypes+1000)
	mw.Start(3, nTypes)
	time.Sleep(20 * time.Second)
	wl2.Stop()
	mw.Stop()

	waitSynced(t, &cfg, p, 3*time.Minute)
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, appTables...)
	m57Compare(t, src, dst, m57Tables...)
}

// Nguồn 5.7 → PostgreSQL: snapshot logic dưới tải + stream, giá trị đổi theo typemap (02 §8).
// Dùng lược đồ/tải/so sánh của test MySQL→PG nhưng với collation có ở 5.7 (utf8mb4_bin, tương ứng "C").
func TestM57_E2E_ToPostgres(t *testing.T) {
	seed := seedFromEnv(t)
	src := startM57Source(t, "src", 1, nil)
	dst := startPG(t, "pgdst")
	srcSQL := make([]string, len(myPGSourceSQL))
	for i, q := range myPGSourceSQL {
		srcSQL[i] = strings.ReplaceAll(q, "utf8mb4_0900_ai_ci", "utf8mb4_bin")
	}
	src.exec(t, srcSQL...)
	dst.exec(t, myPGTargetSQL...)
	src.exec(t, "SET GLOBAL time_zone = '+00:00'")

	const preload = 10000
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= preload; i++ {
		if _, err := tx.Exec(`INSERT INTO shop.products VALUES (?, ?, ?, ?, ?, ?, ?, UUID_SHORT())`,
			i, fmt.Sprintf("SKU-%d", i), fmt.Sprintf("%d.%02d", i%9999, i%100), strconv.FormatUint(1<<63+uint64(i), 10),
			fmt.Sprintf(`{"color": "c%d", "dims": {"w": %d, "h": 2}}`, i%7, i),
			fmt.Sprintf("2025-0%d-1%d 10:20:30.%06d", 1+i%9, i%10, i%1000000), "2025-06-01 00:00:00.5"); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	cfg := strandCfg{Name: "e2e-m57-pg", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		Include: []string{"shop.*"}, Dir: testDir(t), SourceIOPS: 20}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	deadline := time.Now().Add(2 * time.Minute)
	for !strings.Contains(p.output.String(), "joins the pipeline without snapshot") {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("strand không nhận bảng PK varchar rỗng (exited=%v)\n%s", p.exited(), p.tail())
		}
		time.Sleep(100 * time.Millisecond)
	}
	wl := &myPGWorkload{db: src.DB, rng: rand.New(rand.NewSource(seed))}
	wl.nextID.Store(preload + 1000)
	wl.Start(6, preload)
	defer wl.Stop()
	waitSynced(t, &cfg, p, 5*time.Minute)
	time.Sleep(15 * time.Second)
	wl.Stop()
	t.Logf("tải: %d ops, %d lỗi client, %d users", wl.ops.Load(), wl.errs.Load(), wl.nextU.Load())

	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit %d\n%s", code, p.tail())
	}
	compareMySQLPG(t, src, dst)
	out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", cfgPath, "--mode", "full", "--table", "shop.products", "--rate", "1000")
	if code != 0 {
		t.Fatalf("strand verify --mode full exit %d:\n%s", code, out)
	}
}

// Nguồn kiểu 5.6 (gtid_mode=OFF) phải bị từ chối rõ ràng: validate exit 3 (Rejected,
// binlog_config_invalid) nêu gtid_mode; sync không được chạy tiếp (exit khác 0, nêu gtid).
func TestM57_E2E_RejectMySQL56Style(t *testing.T) {
	src := startM57Source(t, "src", 1, nil, "--gtid-mode=OFF", "--enforce-gtid-consistency=OFF")
	dst := startMySQL(t, m57TargetImage(), "dst", 2, nil)
	var mode string
	if err := src.DB.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&mode); err != nil || mode != "OFF" {
		t.Fatalf("gtid_mode nguồn=%q err=%v, want OFF", mode, err)
	}
	src.exec(t, "CREATE DATABASE app", "CREATE TABLE app.t (id BIGINT PRIMARY KEY, v INT)", "INSERT INTO app.t VALUES (1, 1)")
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	cfg := strandCfg{Name: "e2e-m57-nogtid", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)

	out, code := runStrandCLI(t, time.Minute, "validate", "-c", cfgPath)
	if code != 3 {
		t.Fatalf("validate: exit %d, want 3 (Rejected)\n%s", code, out)
	}
	if !strings.Contains(strings.ToLower(out), "gtid_mode") {
		t.Fatalf("validate không nêu gtid_mode:\n%s", out)
	}

	p := startStrand(t, cfgPath)
	deadline := time.Now().Add(time.Minute)
	for !p.exited() && time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
	}
	if !p.exited() {
		p.kill9()
		t.Fatalf("sync vẫn chạy với nguồn gtid_mode=OFF\n%s", p.tail())
	}
	if p.exitCode() == 0 || !strings.Contains(strings.ToLower(p.output.String()), "gtid") {
		t.Fatalf("sync: exit %d, want khác 0 và nêu gtid\n%s", p.exitCode(), p.tail())
	}
	t.Logf("sync với gtid_mode=OFF: exit %d (validate: 3)", p.exitCode())
}
