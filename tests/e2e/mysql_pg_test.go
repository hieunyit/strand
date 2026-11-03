package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Lược đồ MySQL→PG: products (PK số, sku utf8mb4_0900_ai_ci UNIQUE, DECIMAL, BIGINT UNSIGNED,
// JSON, DATETIME(6), TIMESTAMP(6)) được snapshot logic dưới tải; users (PK VARCHAR
// utf8mb4_0900_ai_ci) rỗng lúc khởi động và được lấp đầy chỉ bằng stream.
var myPGSourceSQL = []string{
	"CREATE DATABASE shop",
	`CREATE TABLE shop.products (
		id BIGINT PRIMARY KEY,
		sku VARCHAR(64) COLLATE utf8mb4_0900_ai_ci NOT NULL,
		price DECIMAL(12,2) NOT NULL,
		qty BIGINT UNSIGNED NOT NULL,
		attrs JSON,
		created DATETIME(6),
		modified TIMESTAMP(6) NULL,
		ver BIGINT UNSIGNED NOT NULL,
		UNIQUE KEY uk_sku (sku)) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
	`CREATE TABLE shop.users (
		email VARCHAR(191) COLLATE utf8mb4_0900_ai_ci PRIMARY KEY,
		name VARCHAR(100),
		score DECIMAL(10,3),
		meta JSON,
		joined DATETIME,
		seen TIMESTAMP NULL,
		big BIGINT UNSIGNED,
		ver BIGINT UNSIGNED NOT NULL) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`,
}

var myPGTargetSQL = []string{
	"CREATE SCHEMA shop",
	`CREATE TABLE shop.products (id bigint PRIMARY KEY, sku varchar(64) COLLATE "C" NOT NULL UNIQUE, price numeric(12,2) NOT NULL,
		qty numeric(20,0) NOT NULL, attrs jsonb, created timestamp(6), modified timestamptz(6), ver numeric(20,0) NOT NULL)`,
	`CREATE TABLE shop.users (email varchar(191) COLLATE "C" PRIMARY KEY, name varchar(100), score numeric(10,3), meta jsonb,
		joined timestamp, seen timestamptz, big numeric(20,0), ver numeric(20,0) NOT NULL)`,
}

type myPGWorkload struct {
	db      *sql.DB
	rng     *rand.Rand
	nextID  atomic.Int64
	nextU   atomic.Int64
	ops     atomic.Int64
	errs    atomic.Int64
	stopped atomic.Bool
	wg      sync.WaitGroup
}

func (w *myPGWorkload) Start(clients int, span int64) {
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

func (w *myPGWorkload) Stop() {
	w.stopped.Store(true)
	w.wg.Wait()
}

func (w *myPGWorkload) email(r *rand.Rand) string {
	n := w.nextU.Load()
	if n == 0 {
		return "user0@example.com"
	}
	return fmt.Sprintf("user%d@example.com", r.Int63n(n))
}

func (w *myPGWorkload) one(r *rand.Rand, span int64) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	switch k := r.Intn(100); {
	case k < 30: // cập nhật product (khóa nóng)
		if _, err := tx.Exec(`UPDATE shop.products SET price = price + 0.05, qty = qty + ?, ver = UUID_SHORT(),
			attrs = JSON_SET(COALESCE(attrs, '{}'), '$.n', qty), modified = NOW(6) WHERE id = ?`, r.Intn(1000), hot(r, span)); err != nil {
			return err
		}
	case k < 40: // chèn product
		id := w.nextID.Add(1)
		if _, err := tx.Exec(`INSERT INTO shop.products VALUES (?, ?, ?, ?, ?, NOW(6), NOW(6), UUID_SHORT())`,
			id, fmt.Sprintf("SKU-%d", id), fmt.Sprintf("%d.%02d", r.Intn(10000), r.Intn(100)), uint64(1)<<40+uint64(r.Int63n(1<<20)),
			fmt.Sprintf(`{"color": "c%d", "tags": ["a", "b"], "w": %d}`, r.Intn(9), r.Intn(500))); err != nil {
			return err
		}
	case k < 46: // xóa product
		if _, err := tx.Exec(`DELETE FROM shop.products WHERE id = ?`, 1+r.Int63n(span*2)); err != nil {
			return err
		}
	case k < 54: // hoán đổi sku (UNIQUE) giữa hai product trong một Txn
		a, b := hot(r, span), 1+r.Int63n(span)
		if a == b {
			return nil
		}
		var sa, sb string
		if err := tx.QueryRow(`SELECT sku FROM shop.products WHERE id = ? FOR UPDATE`, a).Scan(&sa); err != nil {
			return nil
		}
		if err := tx.QueryRow(`SELECT sku FROM shop.products WHERE id = ? FOR UPDATE`, b).Scan(&sb); err != nil {
			return nil
		}
		for _, st := range []struct {
			sku string
			id  int64
		}{{"tmp-" + sa, a}, {sa, b}, {sb, a}} {
			if _, err := tx.Exec(`UPDATE shop.products SET sku = ?, ver = UUID_SHORT() WHERE id = ?`, st.sku, st.id); err != nil {
				return err
			}
		}
	case k < 70: // chèn user (PK varchar)
		n := w.nextU.Add(1)
		if _, err := tx.Exec(`INSERT INTO shop.users VALUES (?, ?, ?, ?, NOW(), NOW(), ?, UUID_SHORT())`,
			fmt.Sprintf("user%d@example.com", n-1), fmt.Sprintf("Người dùng %d é", n), fmt.Sprintf("%d.%03d", r.Intn(1000), r.Intn(1000)),
			fmt.Sprintf(`{"n": %d, "s": "ü"}`, n), uint64(1)<<62+uint64(n)); err != nil {
			return err
		}
	case k < 82: // cập nhật user
		if _, err := tx.Exec(`UPDATE shop.users SET score = score + 1.5, seen = NOW(6), ver = UUID_SHORT(),
			meta = JSON_SET(COALESCE(meta, '{}'), '$.x', ?) WHERE email = ?`, r.Intn(100), w.email(r)); err != nil {
			return err
		}
	case k < 88: // đổi PK sang giá trị bằng nhau theo ai_ci nhưng khác byte (đổi hoa/thường)
		e := w.email(r)
		if _, err := tx.Exec(`UPDATE shop.users SET email = IF(BINARY email = LOWER(email), UPPER(email), LOWER(email)), ver = UUID_SHORT()
			WHERE email = ?`, e); err != nil {
			return err
		}
	case k < 92: // xóa user
		if _, err := tx.Exec(`DELETE FROM shop.users WHERE email = ?`, w.email(r)); err != nil {
			return err
		}
	default: // Txn nhiều row
		for j := 0; j < 1+r.Intn(4); j++ {
			if _, err := tx.Exec(`UPDATE shop.products SET qty = qty + 1, ver = UUID_SHORT() WHERE id = ?`, 1+r.Int63n(span)); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// myPGSelect: câu SELECT đưa mỗi row về cùng một dạng text ở hai engine (thời gian UTC, 6 chữ số
// lẻ của giây; JSON so theo ngữ nghĩa ở phía Go).
var myPGSelect = map[string][2]string{
	"shop.products": {
		"SELECT CAST(id AS CHAR), sku, CAST(price AS CHAR), CAST(qty AS CHAR), attrs, DATE_FORMAT(created, '%Y-%m-%d %H:%i:%s.%f'), DATE_FORMAT(modified, '%Y-%m-%d %H:%i:%s.%f'), CAST(ver AS CHAR) FROM shop.products",
		"SELECT id::text, sku, price::text, qty::text, attrs::text, to_char(created, 'YYYY-MM-DD HH24:MI:SS.US'), to_char(modified AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), ver::text FROM shop.products",
	},
	"shop.users": {
		"SELECT email, name, CAST(score AS CHAR), meta, DATE_FORMAT(joined, '%Y-%m-%d %H:%i:%s'), DATE_FORMAT(seen, '%Y-%m-%d %H:%i:%s.%f'), CAST(big AS CHAR), CAST(ver AS CHAR) FROM shop.users",
		"SELECT email, name, score::text, meta::text, to_char(joined, 'YYYY-MM-DD HH24:MI:SS'), to_char(seen AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), big::text, ver::text FROM shop.users",
	},
}

// jsonCols: vị trí cột JSON (so theo ngữ nghĩa).
var myPGJSONCol = map[string]int{"shop.products": 4, "shop.users": 3}

func dumpCanon(t *testing.T, db *sql.DB, q string, jsonCol int) map[string][]string {
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("dump %q: %v", q, err)
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
			if !v.Valid {
				row[i] = "<NULL>"
				continue
			}
			row[i] = v.String
			if i == jsonCol {
				var x any
				if json.Unmarshal([]byte(v.String), &x) == nil {
					b, _ := json.Marshal(x) // khóa được sắp, khoảng trắng chuẩn hóa
					row[i] = string(b)
				}
			}
		}
		out[row[0]] = row
	}
	return out
}

func compareMySQLPG(t *testing.T, src *mysqlNode, dst *pgNode) {
	t.Helper()
	total := 0
	var bad []string
	for tb, qs := range myPGSelect {
		s := dumpCanon(t, src.DB, qs[0], myPGJSONCol[tb])
		d := dumpCanon(t, dst.DB, qs[1], myPGJSONCol[tb])
		total += len(s)
		for k, sv := range s {
			dv, ok := d[k]
			if !ok {
				bad = append(bad, fmt.Sprintf("%s %s missing on target", tb, k))
			} else if !reflect.DeepEqual(sv, dv) {
				bad = append(bad, fmt.Sprintf("%s %s differs:\n  src %v\n  dst %v", tb, k, sv, dv))
			}
		}
		for k := range d {
			if _, ok := s[k]; !ok {
				bad = append(bad, fmt.Sprintf("%s %s extra on target", tb, k))
			}
		}
	}
	if len(bad) > 0 {
		if len(bad) > 15 {
			bad = bad[:15]
		}
		t.Fatalf("final state mismatch after type mapping:\n%s", strings.Join(bad, "\n"))
	}
	t.Logf("final state: %d rows identical after MySQL→PG type mapping", total)
}

// waitMySQLPGCaughtUp: gtid_executed của source ⊆ tập đã apply trên target PG, và pipeline synced.
func waitMySQLPGCaughtUp(t *testing.T, src *mysqlNode, dst *pgNode, cfg *strandCfg, p *strandProc, timeout time.Duration) {
	t.Helper()
	want, err := checkpoint.ParseMySQLGTIDSet(src.gtidExecuted(t))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(timeout)
	for {
		if p.exited() {
			t.Fatalf("strand exited (code %d) before catching up\n%s", p.exitCode(), p.tail())
		}
		got, err := checkpoint.LoadAppliedSnapshotPG(context.Background(), dst.DB, cfg.Name, model.EngineMySQL)
		if err == nil {
			miss := want.Clone().(*checkpoint.MySQLAppliedSet)
			miss.Subtract(got.(*checkpoint.MySQLAppliedSet))
			if miss.String() == "" && pipelineSynced(cfg.Socket) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("target not caught up after %s; missing %s\n%s", timeout, miss.String(), p.tail())
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("load applied: %v\n%s", err, p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Phase 2: MySQL→PG — snapshot logic + stream dưới tải, giá trị đổi theo typemap (02 §8), trạng
// thái cuối giống hệt; sau đó `strand verify --mode full` exit 0 cho bảng PK số.
// Bảng PK VARCHAR (utf8mb4_0900_ai_ci) rỗng lúc bắt đầu: snapshot/verify full chưa hỗ trợ PK không
// phải số nguyên, nên bảng này chỉ đi qua stream và được so ở bước so sánh từng row.
func TestP2_E2E_MySQLToPGSnapshotAndStream(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t, myPGSourceSQL...)
	dst.exec(t, myPGTargetSQL...)
	// Phiên test đọc TIMESTAMP ở UTC (giống phiên của Strand).
	src.exec(t, "SET GLOBAL time_zone = '+00:00'")

	const preload = 20000
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= preload; i++ {
		if _, err := tx.Exec(`INSERT INTO shop.products VALUES (?, ?, ?, ?, ?, ?, ?, UUID_SHORT())`,
			i, fmt.Sprintf("SKU-%d", i), fmt.Sprintf("%d.%02d", i%9999, i%100), uint64(1)<<40+uint64(i),
			fmt.Sprintf(`{"color": "c%d", "dims": {"w": %d, "h": 2}}`, i%7, i),
			fmt.Sprintf("2025-0%d-1%d 10:20:30.%06d", 1+i%9, i%10, i%1000000), "2025-06-01 00:00:00.5"); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Snapshot giãn nhịp (~3 s/chunk) để tải chạy song song với snapshot.
	cfg := strandCfg{Name: "e2e-mysql-pg", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		Include: []string{"shop.*"}, Dir: testDir(t), SourceIOPS: 20}
	cfgPath := cfg.write(t)
	p := startStrand(t, cfgPath)
	// shop.users phải còn rỗng tại lúc Strand chốt vị trí stream và kiểm bảng: chỉ bắt đầu tải sau đó.
	deadline := time.Now().Add(2 * time.Minute)
	for !strings.Contains(p.output.String(), "joins the pipeline without snapshot") {
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("strand did not accept the empty varchar-PK table (exited=%v)\n%s", p.exited(), p.tail())
		}
		time.Sleep(100 * time.Millisecond)
	}
	wl := &myPGWorkload{db: src.DB, rng: rand.New(rand.NewSource(seed))}
	wl.nextID.Store(preload + 1000)
	wl.Start(6, preload)
	if pipelineSynced(cfg.Socket) {
		t.Fatalf("snapshot finished before the load started; it must run under load")
	}
	waitSynced(t, &cfg, p, 5*time.Minute)
	t.Logf("snapshot finished under load; streaming")
	time.Sleep(time.Duration(envInt("STRAND_E2E_PG_LOAD_SECONDS", 30)/2) * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops, %d client-side errors, %d users", wl.ops.Load(), wl.errs.Load(), wl.nextU.Load())

	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	compareMySQLPG(t, src, dst)

	out, code := runStrandCLI(t, 5*time.Minute, "verify", "-c", cfgPath, "--mode", "full", "--table", "shop.products", "--rate", "1000")
	if code != 0 {
		t.Fatalf("strand verify --mode full exit %d:\n%s", code, out)
	}
	t.Logf("verify --mode full: %s", strings.TrimSpace(lastLine(out)))
}

func lastLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		return s[i+1:]
	}
	return s
}
