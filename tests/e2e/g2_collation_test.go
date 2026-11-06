package e2e

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Gate 2 "collation _ci (MySQL) → C (PG), 0 mismatch", binary thật: hai bảng PK chuỗi có dữ liệu sẵn —
// VARCHAR utf8mb4_0900_ai_ci (NO PAD, không phân biệt hoa thường và dấu) và (VARCHAR utf8mb4_general_ci
// PAD SPACE, INT) — sang PG varchar COLLATE "C" (thứ tự và phép bằng khác hẳn). Snapshot logic chạy dưới
// tải có đổi khóa sang biến thể CÙNG lớp của source ('abc' → 'ABC', 'e' → 'é', 'abc' → 'abc ' với PAD
// SPACE), đổi sang khóa khác, chèn, xóa; kill -9 lặp lại giữa chừng snapshot (chạy tiếp plan dở: dọn phần
// chưa đọc theo khóa source). Cuối: dump từng byte khóa giống hệt, `strand verify --mode full` 0 lệch cho
// cả hai bảng; rồi đổi một khóa ở target sang biến thể hoa/thường thì verify full phải báo lệch.
func TestG2_E2E_MySQLCIToPGCSnapshotKill9AndVerify(t *testing.T) {
	seed := seedFromEnv(t)
	iterations := envInt("STRAND_E2E_G2_KILL9_ITERATIONS", 25)
	rng := rand.New(rand.NewSource(seed))
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t, "CREATE DATABASE shop",
		`CREATE TABLE shop.ci_ai (k VARCHAR(32) COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY, v INT NOT NULL)
			DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
		`CREATE TABLE shop.ci_gen (k VARCHAR(32) COLLATE utf8mb4_general_ci NOT NULL, n INT NOT NULL, v INT NOT NULL,
			PRIMARY KEY (k, n)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci`)
	dst.exec(t, "CREATE SCHEMA shop",
		`CREATE TABLE shop.ci_ai (k varchar(32) COLLATE "C" NOT NULL PRIMARY KEY, v int NOT NULL)`,
		`CREATE TABLE shop.ci_gen (k varchar(32) COLLATE "C" NOT NULL, n int NOT NULL, v int NOT NULL, PRIMARY KEY (k, n))`)

	// Khóa: chữ hoa/thường, có dấu, ký tự đặc biệt sắp khác nhau giữa _ci và "C" ('_', 'Z' < 'a' ở C).
	alpha := []string{"a", "A", "b", "B", "e", "é", "E", "z", "Z", "_", "-", "ß", "ss"}
	key := func(r *rand.Rand) string {
		var b strings.Builder
		for i := 1 + r.Intn(3); i > 0; i-- {
			b.WriteString(alpha[r.Intn(len(alpha))])
		}
		return fmt.Sprintf("%s%03d", b.String(), r.Intn(400))
	}
	for i := 0; i < 4000; i++ {
		_, _ = src.DB.Exec("INSERT IGNORE INTO shop.ci_ai VALUES (?, ?)", key(rng), i)
		_, _ = src.DB.Exec("INSERT IGNORE INTO shop.ci_gen VALUES (?, ?, ?)", key(rng)+strings.Repeat(" ", rng.Intn(2)), rng.Intn(3), i)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ops, renames atomic.Int64
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(s int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(s))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k, n := key(r), r.Intn(3)
				var err error
				switch r.Intn(9) {
				case 0:
					_, err = src.DB.Exec("INSERT IGNORE INTO shop.ci_ai VALUES (?, 1)", k)
				case 1:
					_, err = src.DB.Exec("DELETE FROM shop.ci_ai WHERE k = ?", k)
				case 2: // biến thể cùng lớp ai_ci: hoa/thường, e ↔ é
					var res interface{ RowsAffected() (int64, error) }
					res, err = src.DB.Exec(`UPDATE shop.ci_ai SET k = IF(BINARY k = LOWER(k), UPPER(k), REPLACE(LOWER(k), 'e', 'é')), v = v + 1 WHERE k = ?`, k)
					if err == nil {
						if n, _ := res.RowsAffected(); n > 0 {
							renames.Add(1)
						}
					}
				case 3: // sang khóa khác
					_, err = src.DB.Exec("UPDATE IGNORE shop.ci_ai SET k = ? WHERE k = ?", key(r), k)
				case 4:
					_, err = src.DB.Exec("UPDATE shop.ci_ai SET v = v + 1 WHERE k = ?", k)
				case 5:
					_, err = src.DB.Exec("INSERT IGNORE INTO shop.ci_gen VALUES (?, ?, 1)", k, n)
				case 6: // PAD SPACE: thêm/bớt space cuối, đổi hoa/thường — cùng lớp
					_, err = src.DB.Exec(`UPDATE shop.ci_gen SET k = IF(k LIKE '% ', UPPER(TRIM(TRAILING ' ' FROM k)), CONCAT(LOWER(k), ' ')), v = v + 1
						WHERE k = ? AND n = ?`, k, n)
					renames.Add(1)
				case 7:
					_, err = src.DB.Exec("DELETE FROM shop.ci_gen WHERE k = ? AND n = ?", k, n)
				default:
					_, err = src.DB.Exec("UPDATE IGNORE shop.ci_gen SET k = ?, v = v + 1 WHERE k = ? AND n = ?", key(r), k, n)
				}
				if err == nil {
					ops.Add(1)
				}
				time.Sleep(time.Millisecond)
			}
		}(seed + int64(w))
	}
	cfg := strandCfg{Name: "e2e-g2-ci", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		Include: []string{"shop.*"}, Dir: testDir(t), ChunkRows: "40"}
	path := cfg.write(t)
	for i := 1; i <= iterations; i++ {
		p := startStrand(t, path)
		wait := time.Duration(100+rng.Intn(1900)) * time.Millisecond
		select {
		case <-p.done:
			t.Fatalf("iteration %d: strand exited by itself with code %d before kill\n%s", i, p.exitCode(), p.tail())
		case <-time.After(wait):
		}
		p.kill9()
	}
	p := startStrand(t, path)
	waitSynced(t, &cfg, p, 10*time.Minute)
	time.Sleep(2 * time.Second) // stream sau snapshot (range "Xong")
	close(stop)
	wg.Wait()
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 10*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("final run exit %d\n%s", code, p.tail())
	}
	// Trước Gate 2, bảng PK _ci có dữ liệu bị từ chối ngay khi khởi động (exit 3) — vòng kill -9 ở trên
	// đã fail ("exited by itself"); tới đây là đường snapshot theo khóa chính xác đã chạy hết.

	// Từng byte khóa (MySQL trả đúng byte đã lưu; HEX để so cả space cuối).
	for _, q := range [][2]string{
		{"SELECT HEX(k), CAST(v AS CHAR) FROM shop.ci_ai", "SELECT upper(encode(convert_to(k, 'UTF8'), 'hex')), v::text FROM shop.ci_ai"},
		{"SELECT CONCAT(HEX(k), '/', n), CAST(v AS CHAR) FROM shop.ci_gen", "SELECT upper(encode(convert_to(k, 'UTF8'), 'hex')) || '/' || n, v::text FROM shop.ci_gen"},
	} {
		s, d := dumpCanon(t, src.DB, q[0], -1), dumpCanon(t, dst.DB, q[1], -1)
		var bad []string
		for k, sv := range s {
			if dv, ok := d[k]; !ok || strings.Join(dv, "|") != strings.Join(sv, "|") {
				bad = append(bad, fmt.Sprintf("%s: src %v dst %v", k, sv, dv))
			}
		}
		for k := range d {
			if _, ok := s[k]; !ok {
				bad = append(bad, fmt.Sprintf("%s extra on target", k))
			}
		}
		if len(bad) > 0 {
			if len(bad) > 15 {
				bad = bad[:15]
			}
			t.Fatalf("%s: %d rows on source, target differs:\n%s", q[0], len(s), strings.Join(bad, "\n"))
		}
	}
	for _, tb := range []string{"shop.ci_ai", "shop.ci_gen"} {
		out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--table", tb, "--mode", "full", "--rate", "1000")
		if code != 0 || !strings.Contains(out, " 0 mismatches") {
			t.Fatalf("verify --mode full %s: exit %d\n%s", tb, code, out)
		}
		t.Logf("verify %s: %s", tb, strings.TrimSpace(lastLine(out)))
	}

	// Biến thể hoa/thường ở target (cùng lớp ở source, khác byte) phải bị verify full bắt.
	var k string
	if err := dst.DB.QueryRow("SELECT k FROM shop.ci_ai WHERE k <> upper(k) LIMIT 1").Scan(&k); err != nil {
		t.Fatal(err)
	}
	dst.exec(t, fmt.Sprintf("UPDATE shop.ci_ai SET k = upper(k) WHERE k = '%s'", strings.ReplaceAll(k, "'", "''")))
	out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", path, "--table", "shop.ci_ai", "--mode", "full", "--rate", "1000")
	if code == 0 || !strings.Contains(out, fmt.Sprintf("%d:%s", len(k), k)) {
		t.Fatalf("verify --mode full after changing %q to upper case on the target: exit %d\n%s", k, code, out)
	}
	t.Logf("PASS: %d kill -9 during the _ci → C snapshot, %d ops, %d same-class renames", iterations, ops.Load(), renames.Load())
}
