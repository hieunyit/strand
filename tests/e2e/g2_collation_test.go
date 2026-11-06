package e2e

// Gate 2 (G2-5, 02 §8 "MySQL `_ci` sang PG `C`: an toàn", rủi ro #10 của 01 §7): bảng PK VARCHAR collation
// `_ci` (utf8mb4_0900_ai_ci NO PAD và utf8mb4_general_ci PAD SPACE) sang PG COLLATE "C" có DỮ LIỆU SẴN,
// stream chạy trong và sau lúc chép dữ liệu ban đầu, với các giá trị chỉ khác hoa/thường, dấu, space cuối.

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var ciTables = []struct{ name, collation string }{
	{"shop.ci_ai", "utf8mb4_0900_ai_ci"},  // NO PAD: 'a' ≠ 'a ', 'a' = 'A' = 'á'
	{"shop.ci_pad", "utf8mb4_general_ci"}, // PAD SPACE: 'a' = 'a ' = 'A' = 'á'
}

func ciSourceSQL() []string {
	out := []string{"CREATE DATABASE shop"}
	for _, tb := range ciTables {
		out = append(out, fmt.Sprintf(`CREATE TABLE %s (k VARCHAR(64) CHARACTER SET utf8mb4 COLLATE %s NOT NULL PRIMARY KEY,
			v INT NOT NULL, ver BIGINT UNSIGNED NOT NULL) DEFAULT CHARSET=utf8mb4`, tb.name, tb.collation))
	}
	return out
}

func ciTargetSQL() []string {
	out := []string{"CREATE SCHEMA shop"}
	for _, tb := range ciTables {
		out = append(out, fmt.Sprintf(`CREATE TABLE %s (k varchar(64) COLLATE "C" PRIMARY KEY, v integer NOT NULL, ver numeric(20,0) NOT NULL)`, tb.name))
	}
	return out
}

// ciWords: thứ tự theo collation `_ci` (a/á/A < b/B < … < z/Z) khác hẳn thứ tự byte của "C" (chữ hoa
// trước chữ thường, chữ có dấu và CJK/emoji sau ASCII) — biên chunk/range theo hai thứ tự này lệch nhau.
var ciWords = []string{"a", "A", "á", "b", "B", "c", "É", "e", "é", "m", "N", "o", "Ö", "z", "Z", "中", "ñ", "Ü"}

func ciKey(r *rand.Rand) string {
	return fmt.Sprintf("%s%s%d", ciWords[r.Intn(len(ciWords))], ciWords[r.Intn(len(ciWords))], r.Intn(80))
}

// ciVariant trả một giá trị BẰNG k theo collation nhưng khác byte: đổi hoa/thường, thêm/bỏ dấu, và với
// PAD SPACE thêm/bỏ space cuối.
func ciVariant(r *rand.Rand, k string, pad bool) string {
	switch r.Intn(3) {
	case 0:
		if strings.ToUpper(k) != k {
			return strings.ToUpper(k)
		}
		return strings.ToLower(k)
	case 1:
		repl := strings.NewReplacer("a", "á", "á", "a", "e", "é", "é", "e", "o", "ö", "ö", "o", "A", "Á", "Á", "A", "E", "É", "É", "E")
		return repl.Replace(k)
	default:
		if pad {
			if strings.HasSuffix(k, " ") {
				return strings.TrimRight(k, " ")
			}
			return k + "  "
		}
		return strings.ToLower(k)
	}
}

// ciWriter ghi liên tục vào một bảng: upsert (khóa trùng theo collation giữ byte cũ), đổi PK sang biến thể
// bằng theo collation, đổi PK sang khóa khác, xóa rồi chèn lại biến thể khác hoa/thường trong hai Txn liền
// nhau, xóa.
func ciWriter(db *sql.DB, table string, pad bool, seed int64, stop *atomic.Bool, wg *sync.WaitGroup, ops *atomic.Int64) {
	defer wg.Done()
	r := rand.New(rand.NewSource(seed))
	for !stop.Load() {
		k := ciKey(r)
		var err error
		switch x := r.Intn(10); {
		case x < 3:
			_, err = db.Exec(fmt.Sprintf("INSERT INTO %s VALUES (?, 1, UUID_SHORT()) ON DUPLICATE KEY UPDATE v = v + 1, ver = UUID_SHORT()", table), k)
		case x < 6:
			_, err = db.Exec(fmt.Sprintf("UPDATE %s SET k = ?, v = v + 1, ver = UUID_SHORT() WHERE k = ?", table), ciVariant(r, k, pad), k)
		case x < 7:
			_, err = db.Exec(fmt.Sprintf("UPDATE IGNORE %s SET k = ?, ver = UUID_SHORT() WHERE k = ?", table), ciKey(r), k)
		case x < 9:
			if _, err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE k = ?", table), k); err == nil {
				_, err = db.Exec(fmt.Sprintf("INSERT IGNORE INTO %s VALUES (?, 7, UUID_SHORT())", table), ciVariant(r, k, pad))
			}
		default:
			_, err = db.Exec(fmt.Sprintf("DELETE FROM %s WHERE k = ?", table), k)
		}
		if err == nil {
			ops.Add(1)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func ciPreload(t *testing.T, src *mysqlNode, seed int64, n int) {
	r := rand.New(rand.NewSource(seed))
	for _, tb := range ciTables {
		tx, err := src.DB.Begin()
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			k := ciKey(r)
			if tb.name == "shop.ci_pad" && r.Intn(5) == 0 {
				k += " "
			}
			if _, err := tx.Exec(fmt.Sprintf("INSERT IGNORE INTO %s VALUES (?, ?, UUID_SHORT())", tb.name), k, i); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

// ciDump: khóa (byte chính xác) → "v|ver", và danh sách khóa theo ORDER BY của chính DB đó.
func ciDump(t *testing.T, db *sql.DB, q string) (map[string]string, []string) {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	out := map[string]string{}
	var order []string
	for rows.Next() {
		var k, v, ver string
		if err := rows.Scan(&k, &v, &ver); err != nil {
			t.Fatal(err)
		}
		out[k] = v + "|" + ver
		order = append(order, k)
	}
	return out, order
}

// Gate 2 (G2-5).
//
//   - logical_snapshot_rejected: bảng PK chuỗi `_ci` có dữ liệu cần snapshot logic thì Strand từ chối rõ
//     ràng (exit 3, type_unmappable), vì flush xóa range khóa ở target bằng cận đọc từ source mà hai thứ tự
//     (`_ci` của MySQL, "C" của PG) khác nhau (pipeline/generated.go checkSnapshotKey,
//     snapshot.CrossEngineSameOrder; docs/USAGE.md: khóa chuỗi MySQL→PG chỉ nhận khi bảng rỗng).
//   - seeded_then_stream: dữ liệu sẵn được chép như một công cụ backup (FLUSH TABLES WITH READ LOCK +
//     START TRANSACTION WITH CONSISTENT SNAPSHOT, ghi lại gtid_executed G) trong khi source vẫn ghi liên tục,
//     nạp vào PG "C", rồi `strand sync --start-at G` (chế độ C). Tải trên cả hai bảng: đổi PK sang biến
//     thể chỉ khác hoa/thường/dấu/space cuối, upsert trùng theo collation, xóa rồi chèn lại biến thể. Cuối:
//     từng khóa giống hệt TỪNG BYTE hai phía (target "C" phân biệt mọi biến thể nên một event áp sai khóa
//     sẽ lộ ra), thứ tự ORDER BY hai phía khác nhau (dữ liệu thật sự kiểm được chỗ hai thứ tự lệch), và
//     `verify --mode full` từ chối rõ ràng (cùng lý do) thay vì báo 0 lệch giả.
func TestG2_E2E_MySQLToPGCICollationVarcharPK(t *testing.T) {
	t.Run("logical_snapshot_rejected", func(t *testing.T) {
		src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
		dst := startPG(t, "pgdst")
		src.exec(t, ciSourceSQL()...)
		dst.exec(t, ciTargetSQL()...)
		ciPreload(t, src, 1, 500)
		cfg := strandCfg{Name: "e2e-ci-reject", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
			Include: []string{"shop.*"}, Dir: testDir(t)}
		p := startStrand(t, cfg.write(t))
		select {
		case <-p.done:
		case <-time.After(2 * time.Minute):
			p.kill9()
			t.Fatalf("strand did not reject the populated varchar _ci primary key table\n%s", p.tail())
		}
		out := p.output.String()
		if code := p.exitCode(); code != 3 || !strings.Contains(out, "needs a logical snapshot but its primary key is not supported") ||
			!strings.Contains(out, "type_unmappable") {
			t.Fatalf("want exit 3 type_unmappable for the populated _ci varchar PK table, got exit %d\n%s", code, p.tail())
		}
		t.Logf("logical snapshot of a populated _ci varchar PK table to PG \"C\" rejected clearly (exit 3)")
	})

	t.Run("seeded_then_stream", func(t *testing.T) {
		seed := seedFromEnv(t)
		src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
		dst := startPG(t, "pgdst")
		src.exec(t, ciSourceSQL()...)
		dst.exec(t, ciTargetSQL()...)
		ciPreload(t, src, seed, 4000)

		var stop atomic.Bool
		var wg sync.WaitGroup
		var ops atomic.Int64
		for i, tb := range ciTables {
			for w := 0; w < 2; w++ {
				wg.Add(1)
				go ciWriter(src.DB, tb.name, tb.collation == "utf8mb4_general_ci", seed+int64(i*10+w), &stop, &wg, &ops)
			}
		}
		defer func() { stop.Store(true); wg.Wait() }()
		time.Sleep(2 * time.Second)

		// "Backup" nhất quán tại G trong khi source vẫn ghi.
		ctx := context.Background()
		conn, err := src.DB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var g string
		for _, q := range []string{"FLUSH TABLES WITH READ LOCK", "START TRANSACTION WITH CONSISTENT SNAPSHOT"} {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&g); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			t.Fatal(err)
		}
		g = strings.ReplaceAll(g, "\n", "")
		copied := 0
		for _, tb := range ciTables {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT k, v, CAST(ver AS CHAR) FROM %s", tb.name))
			if err != nil {
				t.Fatal(err)
			}
			type row struct{ k, v, ver string }
			var rs []row
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.k, &r.v, &r.ver); err != nil {
					t.Fatal(err)
				}
				rs = append(rs, r)
			}
			rows.Close()
			tx, err := dst.DB.Begin()
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range rs {
				if _, err := tx.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1, $2, $3)", tb.name), r.k, r.v, r.ver); err != nil {
					t.Fatalf("load %s %q into PG \"C\": %v", tb.name, r.k, err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			copied += len(rs)
		}
		_, _ = conn.ExecContext(ctx, "COMMIT")
		_ = conn.Close()
		t.Logf("copied %d rows at gtid_executed %s while the source kept writing", copied, g)

		cfg := strandCfg{Name: "e2e-ci-seeded", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
			Include: []string{"shop.*"}, Dir: testDir(t), Bootstrap: "start_at"}
		cfgPath := cfg.write(t)
		p := startStrand(t, cfgPath, "--start-at", g)
		waitSynced(t, &cfg, p, 3*time.Minute)
		time.Sleep(time.Duration(envInt("STRAND_E2E_CI_LOAD_SECONDS", 20)) * time.Second)
		stop.Store(true)
		wg.Wait()
		t.Logf("workload: %d writes (case/accent/trailing-space PK variants, upserts, delete+reinsert)", ops.Load())
		waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
		if code := p.stop(t); code != 0 {
			t.Fatalf("strand exit code %d\n%s", code, p.tail())
		}

		for _, tb := range ciTables {
			s, so := ciDump(t, src.DB, fmt.Sprintf("SELECT k, CAST(v AS CHAR), CAST(ver AS CHAR) FROM %s ORDER BY k", tb.name))
			d, do := ciDump(t, dst.DB, fmt.Sprintf("SELECT k, v::text, ver::text FROM %s ORDER BY k", tb.name))
			var bad []string
			for k, sv := range s {
				if dv, ok := d[k]; !ok {
					bad = append(bad, fmt.Sprintf("%q (%x) missing on target", k, k))
				} else if dv != sv {
					bad = append(bad, fmt.Sprintf("%q differs: src %s dst %s", k, sv, dv))
				}
			}
			for k := range d {
				if _, ok := s[k]; !ok {
					bad = append(bad, fmt.Sprintf("%q (%x) extra on target", k, k))
				}
			}
			if len(bad) > 0 {
				sort.Strings(bad)
				if len(bad) > 20 {
					bad = bad[:20]
				}
				t.Fatalf("%s: %d source rows vs %d target rows, byte-exact mismatch:\n%s", tb.name, len(s), len(d), strings.Join(bad, "\n"))
			}
			diff := 0
			for i := range so {
				if so[i] != do[i] {
					diff++
				}
			}
			if diff == 0 {
				t.Fatalf("%s: source (%s) and target (\"C\") ORDER BY agree on all %d keys — the data does not exercise the order difference", tb.name, tb.collation, len(so))
			}
			t.Logf("%s (%s → \"C\"): %d rows byte-identical; %d positions differ between the two ORDER BY", tb.name, tb.collation, len(s), diff)

			vout, vcode := runStrandCLI(t, 2*time.Minute, "verify", "-c", cfgPath, "--mode", "full", "--table", tb.name)
			if vcode != 3 || !strings.Contains(vout, "order the primary key differently") {
				t.Fatalf("verify --mode full %s: want a clear rejection (exit 3, different key order), got exit %d\n%s", tb.name, vcode, vout)
			}
			t.Logf("verify --mode full %s rejected as documented: %s", tb.name, strings.TrimSpace(lastLine(vout)))
		}
	})
}
