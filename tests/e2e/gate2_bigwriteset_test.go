package e2e

// G2-8 (01 §6 Gate 2, HANDOFF §8): 100 transaction × 49.999 khóa không OOM — ngay dưới
// table_level_threshold = 50.000 (sched.DefaultConfig, 02 §6), nên Txn không tự được nâng thành khóa
// cấp bảng theo ngưỡng; 100 Txn cộng lại ≈ 5 triệu khóa, vượt writeset_budget = 2.000.000 entry.
// PG→PG và MySQL→PG. Các Txn được commit khi Strand dừng, rồi Strand khởi động lại và đọc dồn cả
// backlog (áp lực RAM lớn nhất: mọi Txn tới cùng lúc). Đo RSS đỉnh (VmHWM của process strand), ghi
// số, và kiểm có giới hạn; dữ liệu cuối đúng từng row (kiểm bằng aggregate trên DB, không nạp
// 5 triệu row vào RAM của test — bài học §7.6).
//
// Giới hạn RSS mặc định 1 GiB (STRAND_E2E_BIGWS_RSS_MB để đổi), suy từ các trần RAM của cấu hình
// mặc định (resources.memory chưa được áp — config/unimplemented.go):
//   writeset_budget 2.000.000 entry ≈ 100 MB (ước lượng của 02 §6)
//   + sched MaxPendingBytes 512 MiB (Txn chờ trong DAG)
//   + reader PG MaxBufferedBytes 64 MiB (phần thân Txn streaming)
//   + batch writer 8 worker × max_bytes 4 MiB = 32 MiB
//   + nền process Go/driver/headroom GC ≈ 256 MiB
//   ≈ 964 MiB → làm tròn 1 GiB. Dữ liệu thô của 100 Txn ≈ 5 triệu row; nếu Strand giữ cả backlog trong
// RAM (dạng model.RowChange, vài trăm byte mỗi row) thì RSS vượt xa 1 GiB — đó là OOM mà test chặn.
//
// "writeset_budget hoạt động": quan sát được qua strand_writeset_entries (tổng theo bảng) không vượt
// writeset_budget + một Txn, và qua file *.spill trong state: assembler đổ Txn > 10.000 row ra đĩa
// (assemble.New, maxInMem 10.000) và scheduler coi Txn tràn đĩa là khóa cấp bảng — đây là cơ chế
// thực sự giữ RAM ở đường này (xem báo cáo: Txn 49.999 khóa không bao giờ tới writeset cấp row).

import (
	"database/sql"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/sched"
)

const (
	bigWSKeys     = 49_999  // ngay dưới table_level_threshold
	bigWSIDStride = 100_000 // Txn k ghi id [(k+1)·stride, (k+1)·stride + bigWSKeys)
)

func bigWSTxns() int { return envInt("STRAND_E2E_BIGWS_TXNS", 100) }

// runBigWSTxns chạy n Txn (mỗi Txn một câu lệnh ghi bigWSKeys row) trên conc goroutine.
func runBigWSTxns(t *testing.T, n, conc int, one func(base int64) error) {
	t.Helper()
	if th := sched.DefaultConfig().TableLevelThreshold; bigWSKeys >= th {
		t.Fatalf("test premise broken: %d keys per txn is not below table_level_threshold %d", bigWSKeys, th)
	}
	start := time.Now()
	var next atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	var errs []string
	for c := 0; c < conc; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				k := next.Add(1) - 1
				if k >= int64(n) {
					return
				}
				if err := one((k + 1) * bigWSIDStride); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("txn %d: %v", k, err))
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("%d of %d big transactions failed on the source, first: %s", len(errs), n, errs[0])
	}
	t.Logf("source: %d transactions × %d keys committed in %s (%d concurrent)", n, bigWSKeys, time.Since(start).Round(time.Second), conc)
}

// bigWSMonitor: đỉnh RSS của process strand, đỉnh strand_writeset_entries (tổng các bảng), đỉnh
// strand_holes, đỉnh số file *.spill trong state.
type bigWSMonitor struct {
	rss     *rssSampler
	pid     int
	stop    atomic.Bool
	wg      sync.WaitGroup
	mu      sync.Mutex
	ws      float64
	holes   float64
	spill   int
	scrapes int
}

func startBigWSMonitor(p *strandProc, cfg *strandCfg) *bigWSMonitor {
	m := &bigWSMonitor{pid: p.cmd.Process.Pid}
	m.rss = startRSSSampler(m.pid)
	stateDir := filepath.Join(cfg.Dir, "state")
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for !m.stop.Load() {
			n := countSpillFiles(stateDir)
			text, err := scrapeMetrics(cfg.MetricsAddr)
			m.mu.Lock()
			m.spill = max(m.spill, n)
			if err == nil {
				ws, _ := metricSum(text, "strand_writeset_entries", map[string]string{"pipeline": cfg.Name})
				h, _ := metricSum(text, "strand_holes", map[string]string{"pipeline": cfg.Name})
				m.ws, m.holes = max(m.ws, ws), max(m.holes, h)
				m.scrapes++
			}
			m.mu.Unlock()
			time.Sleep(250 * time.Millisecond)
		}
	}()
	return m
}

// finishAndCheck: gọi khi strand còn sống (VmHWM chỉ đọc được khi process tồn tại).
func (m *bigWSMonitor) finishAndCheck(t *testing.T, catchUp time.Duration) {
	t.Helper()
	hwm, rss, samples := m.rss.finish(m.pid)
	m.stop.Store(true)
	m.wg.Wait()
	limitMB := int64(envInt("STRAND_E2E_BIGWS_RSS_MB", 1024))
	budget := float64(sched.DefaultConfig().WritesetBudget)
	rows := float64(bigWSTxns() * bigWSKeys)
	t.Logf("strand RSS peak (VmHWM) %d MiB, max sampled VmRSS %d MiB over %d samples; limit %d MiB",
		hwm>>10, rss>>10, samples, limitMB)
	t.Logf("catch-up of %.0f rows took %s (%.0f rows/s); strand_writeset_entries peak %.0f (budget %.0f); strand_holes peak %.0f; *.spill files peak %d; %d metric scrapes",
		rows, catchUp.Round(time.Second), rows/math.Max(catchUp.Seconds(), 1), m.ws, budget, m.holes, m.spill, m.scrapes)
	if samples < 4 || m.scrapes < 4 {
		t.Fatalf("too few samples (rss %d, metrics %d): the catch-up finished before it could be observed", samples, m.scrapes)
	}
	if hwm>>10 > limitMB {
		t.Errorf("strand RSS peak %d MiB exceeds the bound %d MiB derived from the default memory caps", hwm>>10, limitMB)
	}
	if m.ws > budget+bigWSKeys {
		t.Errorf("strand_writeset_entries peaked at %.0f, above writeset_budget %.0f + one transaction", m.ws, budget)
	}
	if m.holes > cp4MaxHoles {
		t.Errorf("strand_holes peaked at %.0f (> %d)", m.holes, cp4MaxHoles)
	}
	if m.spill == 0 {
		t.Errorf("no *.spill file was ever seen under the state dir: transactions of %d rows were held in RAM (assembler spill at 10,000 rows did not engage)", bigWSKeys)
	}
}

// checkBigWSTarget kiểm bảng đích trên PG bằng aggregate: đủ n·bigWSKeys row, mỗi Txn đủ bigWSKeys
// row, giá trị đúng công thức sinh (v = 7·id + 3, pad = 'p' || id).
func checkBigWSTarget(t *testing.T, db *sql.DB, table string, n int) {
	t.Helper()
	var total, bad, buckets, shortBuckets int64
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*), count(*) FILTER (WHERE v <> id * 7 + 3 OR pad <> 'p' || id::text),
		count(DISTINCT id / %d) FROM %s`, bigWSIDStride, table)).Scan(&total, &bad, &buckets); err != nil {
		t.Fatalf("check %s: %v", table, err)
	}
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM (SELECT id / %[1]d AS b, count(*) AS c, min(id) AS lo, max(id) AS hi
		FROM %[2]s GROUP BY 1) x WHERE c <> %[3]d OR lo <> b * %[1]d OR hi <> b * %[1]d + %[4]d`,
		bigWSIDStride, table, bigWSKeys, bigWSKeys-1)).Scan(&shortBuckets); err != nil {
		t.Fatalf("check %s per transaction: %v", table, err)
	}
	want := int64(n) * bigWSKeys
	if total != want || bad != 0 || buckets != int64(n) || shortBuckets != 0 {
		t.Fatalf("target %s: %d rows (want %d), %d rows with wrong values, %d transactions present (want %d), %d transactions incomplete",
			table, total, want, bad, buckets, n, shortBuckets)
	}
	t.Logf("target %s: %d rows of %d transactions, every value as generated", table, total, n)
}

// G2-8, PG→PG.
func TestG2_E2E_BigWritesetNoOOMPG(t *testing.T) {
	n := bigWSTxns()
	src := startPG(t, "pgsrc")
	dst := startPG(t, "pgdst")
	schema := []string{
		"CREATE SCHEMA app",
		"CREATE TABLE app.bulk (id bigint PRIMARY KEY, v bigint NOT NULL, pad text NOT NULL)",
		"CREATE TABLE app.marker (id bigint PRIMARY KEY, tag text)",
	}
	src.exec(t, schema...)
	dst.exec(t, schema...)
	slot, pub := "strand_g2_bigws", "strand_g2_bigws_pub"
	src.exec(t, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE app.bulk, app.marker", pub))
	var lsn string
	if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', false, true)", slot).Scan(&lsn); err != nil {
		t.Fatalf("create slot: %v", err)
	}
	cfg := strandCfg{Name: "g2-bigws-pg", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN, MetricsAddr: freeAddr(t),
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at"}
	cfgPath := cfg.write(t)
	t.Logf("commit_order %s", cfg.CommitOrder)
	p := startStrand(t, cfgPath, "--start-at", lsn)
	waitPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}

	runBigWSTxns(t, n, envInt("STRAND_E2E_BIGWS_CONCURRENCY", 10), func(base int64) error {
		_, err := src.DB.Exec(`INSERT INTO app.bulk SELECT $1::bigint + g, ($1::bigint + g) * 7 + 3, 'p' || ($1::bigint + g)
			FROM generate_series(0, $2::int - 1) g`, base, bigWSKeys)
		return err
	})

	p = startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	mon := startBigWSMonitor(p, &cfg)
	start := time.Now()
	waitPGCaughtUp(t, src, dst, &cfg, p, envDuration("STRAND_E2E_BIGWS_CATCHUP", 20*time.Minute))
	mon.finishAndCheck(t, time.Since(start))
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	checkBigWSTarget(t, dst.DB, "app.bulk", n)
}

// G2-8, MySQL→PG.
func TestG2_E2E_BigWritesetNoOOMMySQLToPG(t *testing.T) {
	n := bigWSTxns()
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t,
		"CREATE DATABASE bulk",
		"CREATE TABLE bulk.items (id BIGINT PRIMARY KEY, v BIGINT NOT NULL, pad VARCHAR(64) NOT NULL)",
		// aux.nums: dãy 0..49.998 để mỗi Txn là một INSERT … SELECT (aux nằm ngoài pipeline).
		"CREATE DATABASE aux",
		"CREATE TABLE aux.nums (n INT PRIMARY KEY)",
		fmt.Sprintf(`SET SESSION cte_max_recursion_depth = 100000;
			INSERT INTO aux.nums WITH RECURSIVE s(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM s WHERE n < %d) SELECT n FROM s`, bigWSKeys-1),
	)
	var nums int
	if err := src.DB.QueryRow("SELECT COUNT(*) FROM aux.nums").Scan(&nums); err != nil || nums != bigWSKeys {
		t.Fatalf("aux.nums: %d rows, err %v", nums, err)
	}
	dst.exec(t,
		"CREATE SCHEMA bulk",
		"CREATE TABLE bulk.items (id bigint PRIMARY KEY, v bigint NOT NULL, pad varchar(64) NOT NULL)",
	)
	cfg := strandCfg{Name: "g2-bigws-mypg", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		MetricsAddr: freeAddr(t), Include: []string{"bulk.*"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	t.Logf("commit_order %s", cfg.CommitOrder)
	p := startStrand(t, cfgPath)
	waitSynced(t, &cfg, p, 3*time.Minute)
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}

	runBigWSTxns(t, n, envInt("STRAND_E2E_BIGWS_CONCURRENCY", 10), func(base int64) error {
		_, err := src.DB.Exec("INSERT INTO bulk.items SELECT ? + n, (? + n) * 7 + 3, CONCAT('p', ? + n) FROM aux.nums", base, base, base)
		return err
	})
	var srcRows int64
	if err := src.DB.QueryRow("SELECT COUNT(*) FROM bulk.items").Scan(&srcRows); err != nil || srcRows != int64(n)*bigWSKeys {
		t.Fatalf("source bulk.items: %d rows, err %v", srcRows, err)
	}

	p = startStrand(t, cfgPath)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	mon := startBigWSMonitor(p, &cfg)
	start := time.Now()
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, envDuration("STRAND_E2E_BIGWS_CATCHUP", 20*time.Minute))
	mon.finishAndCheck(t, time.Since(start))
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	checkBigWSTarget(t, dst.DB, "bulk.items", n)
}
