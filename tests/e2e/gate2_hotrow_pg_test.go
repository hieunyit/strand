package e2e

// G2-11 trên đường PG (01 §6 Gate 2, 04 §5 "hot row 1.000 update/s báo unverifiable_hot"): PG→PG và
// MySQL→PG. Row id=1 được UPDATE ~1.000 lần/giây ở source suốt lúc verify; một row khác (không ai ghi)
// bị sửa thẳng ở target. Với pipeline đang chạy:
//   - `verify --mode full` và `--mode sample --rate 1.0`: exit 5 (verify_mismatch), lệch đúng một row
//     (row cài lệch), row nóng không bao giờ lọt vào danh sách lệch, unverifiable_hot ≤ 1 và = 1 ở ít
//     nhất một trong 3 lượt (lượt so range có thể đọc trúng lúc target bằng source);
//   - `--mode delta` (khóa CDC đã chạm = row nóng): exit 0, metric
//     strand_verify_unverifiable_hot_total của bảng tăng.
// Cuối: dừng tải, trả row cài lệch về giá trị source, trạng thái cuối giống hệt.
//
// Helper đặt tên riêng (pgHot*) để không trùng với helper của gate2_hotrow_test.go (MySQL→MySQL, nhánh
// khác) khi gộp nhánh.

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/exitcode"
)

const (
	pgHotRowID   = 1
	pgPlantedID  = 777
	pgHotRate    = 1000 // update/giây
	pgHotRateMin = 800  // nhịp tối thiểu để kết quả có nghĩa
)

var (
	pgHotFullRe   = regexp.MustCompile(`(\d+) rows compared, (\d+) mismatches, (\d+) unverifiable_hot`)
	pgHotSampleRe = regexp.MustCompile(`(\d+) rows, (\d+) mismatches, (\d+) unverifiable_hot`)
)

// pgHotUpdater: workers goroutine chia nhau nhịp rate lần chạy stmt/giây (nhịp chung theo đồng hồ).
type pgHotUpdater struct {
	stopped atomic.Bool
	wg      sync.WaitGroup
	n, errs atomic.Int64
	lastErr atomic.Value
	start   time.Time
	end     time.Time
}

func startPGHotUpdater(db *sql.DB, stmt string, id int64, rate, workers int) *pgHotUpdater {
	h := &pgHotUpdater{start: time.Now()}
	var ticket atomic.Int64
	for w := 0; w < workers; w++ {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			for !h.stopped.Load() {
				k := ticket.Add(1) - 1
				if d := time.Until(h.start.Add(time.Duration(k) * time.Second / time.Duration(rate))); d > 0 {
					time.Sleep(d)
				}
				if _, err := db.Exec(stmt, id); err != nil {
					h.errs.Add(1)
					h.lastErr.Store(err.Error())
					continue
				}
				h.n.Add(1)
			}
		}()
	}
	return h
}

func (h *pgHotUpdater) rate() float64 { return float64(h.n.Load()) / time.Since(h.start).Seconds() }

func (h *pgHotUpdater) Stop() float64 {
	if !h.stopped.Swap(true) {
		h.wg.Wait()
		h.end = time.Now()
	}
	return float64(h.n.Load()) / h.end.Sub(h.start).Seconds()
}

func pgHotMetric(t *testing.T, addr, name string, want map[string]string) float64 {
	t.Helper()
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	var sum float64
	sc := bufio.NewScanner(strings.NewReader(string(b)))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		ok := true
		for k, v := range want {
			if !strings.Contains(line, k+`="`+v+`"`) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		if v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64); err == nil {
			sum += v
		}
	}
	return sum
}

// pgHotVerifyRuns chạy verify (args) tối đa 3 lượt khi tải nóng đang chạy, kiểm từng lượt và trả về khi
// row nóng được báo unverifiable_hot.
func pgHotVerifyRuns(t *testing.T, mode string, re *regexp.Regexp, minRows int, hot *pgHotUpdater, p *strandProc, args ...string) {
	t.Helper()
	for attempt := 1; attempt <= 3; attempt++ {
		began := time.Now()
		out, code := runStrandCLI(t, 10*time.Minute, args...)
		m := re.FindStringSubmatch(out)
		if m == nil {
			t.Fatalf("%s attempt %d: no summary line (exit %d, hot rate %.0f/s):\n%s\n%s", mode, attempt, code, hot.rate(), out, p.tail())
		}
		rows, _ := strconv.Atoi(m[1])
		mism, _ := strconv.Atoi(m[2])
		hotN, _ := strconv.Atoi(m[3])
		t.Logf("%s attempt %d (%s): exit %d, %d rows, %d mismatches, %d unverifiable_hot", mode, attempt,
			time.Since(began).Round(time.Millisecond), code, rows, mism, hotN)
		if code != exitcode.DataError || !strings.Contains(out, exitcode.ErrVerifyMismatch) {
			t.Fatalf("%s attempt %d: planted mismatch not reported: exit %d, want %d %s\n%s", mode, attempt, code, exitcode.DataError, exitcode.ErrVerifyMismatch, out)
		}
		if mism != 1 || !strings.Contains(out, fmt.Sprintf("(pk %d)", pgPlantedID)) {
			t.Fatalf("%s attempt %d: want exactly one mismatch, pk %d (hot row %d must be unverifiable_hot, not a mismatch):\n%s",
				mode, attempt, pgPlantedID, pgHotRowID, out)
		}
		if mode == "full" && rows == 0 {
			t.Fatalf("full attempt %d: no rows compared (the chunk with the planted row must mismatch)", attempt)
		}
		if mode == "sample" && rows < minRows {
			t.Fatalf("sample attempt %d: --rate 1.0 compared only %d rows of %d", attempt, rows, minRows)
		}
		if hotN > 1 {
			t.Fatalf("%s attempt %d: %d rows reported unverifiable_hot, only row %d is hot:\n%s", mode, attempt, hotN, pgHotRowID, out)
		}
		if hotN == 1 {
			return
		}
	}
	t.Fatalf("%s: hot row %d (%.0f updates/s) was never reported unverifiable_hot in 3 runs", mode, pgHotRowID, hot.rate())
}

// pgHotDelta: delta (chỉ khóa đã chạm = row nóng) không báo lệch giả, metric unverifiable_hot tăng.
func pgHotDelta(t *testing.T, cfg *strandCfg, path, table string, p *strandProc) {
	t.Helper()
	labels := map[string]string{"pipeline": cfg.Name, "table": table}
	const metric = "strand_verify_unverifiable_hot_total"
	base := pgHotMetric(t, cfg.MetricsAddr, metric, labels)
	for attempt := 1; attempt <= 3 && pgHotMetric(t, cfg.MetricsAddr, metric, labels) <= base; attempt++ {
		out, code := runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--mode", "delta", "--socket", cfg.Socket)
		t.Logf("delta attempt %d: exit %d: %s", attempt, code, strings.TrimSpace(out))
		if code != 0 {
			t.Fatalf("verify delta on the hot row reported a mismatch or failed: exit %d\n%s\n%s", code, out, p.tail())
		}
	}
	if got := pgHotMetric(t, cfg.MetricsAddr, metric, labels); got <= base {
		t.Fatalf("%s{table=%s} did not increase over 3 delta runs (%.0f)", metric, table, got)
	} else {
		t.Logf("%s{table=%s}: %.0f -> %.0f", metric, table, base, got)
	}
}

func TestG2_E2E_PGToPGHotRowUnverifiable(t *testing.T) {
	slot, pub := "strand_g2_hot", "strand_g2_hot_pub"
	src, dst, lsn := setupPGPair(t, slot, pub)
	cfg := strandCfg{Name: "g2-hotrow-pg", SourceKind: "postgres", TargetKind: "postgres",
		SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s", slot, pub),
		Include:     []string{"app.*"}, Dir: testDir(t), Bootstrap: "start_at", VerifyDelta: "on", MetricsAddr: freeAddr(t)}
	path := cfg.write(t)
	p := startStrand(t, path, "--start-at", lsn)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)

	var srcBal int64
	if err := src.DB.QueryRow("SELECT balance FROM app.accounts WHERE id = $1", pgPlantedID).Scan(&srcBal); err != nil {
		t.Fatal(err)
	}
	dst.exec(t, fmt.Sprintf("UPDATE app.accounts SET balance = -424242 WHERE id = %d", pgPlantedID))

	hot := startPGHotUpdater(src.DB, "UPDATE app.accounts SET ver = nextval('app.ver_seq'), balance = balance + 1 WHERE id = $1", pgHotRowID, pgHotRate, 4)
	defer hot.Stop()
	time.Sleep(5 * time.Second)
	if r := hot.rate(); r < pgHotRateMin {
		t.Fatalf("hot row updater reached only %.0f updates/s (want ~%d); errors %d, last %v", r, pgHotRate, hot.errs.Load(), hot.lastErr.Load())
	}

	pgHotVerifyRuns(t, "full", pgHotFullRe, 0, hot, p, "verify", "-c", path, "--mode", "full", "--table", "app.accounts", "--rate", "1000")
	pgHotVerifyRuns(t, "sample", pgHotSampleRe, 1000, hot, p, "verify", "-c", path, "--mode", "sample", "--table", "app.accounts", "--rate", "1.0")
	pgHotDelta(t, &cfg, path, "app.accounts", p)

	if r := hot.Stop(); r < pgHotRateMin {
		t.Fatalf("hot row rate over the whole run %.0f/s < %d", r, pgHotRateMin)
	}
	waitPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	var dstBal int64
	if err := dst.DB.QueryRow("SELECT balance FROM app.accounts WHERE id = $1", pgPlantedID).Scan(&dstBal); err != nil {
		t.Fatal(err)
	}
	if dstBal != -424242 {
		t.Fatalf("planted row %d changed on target to %d", pgPlantedID, dstBal)
	}
	dst.exec(t, fmt.Sprintf("UPDATE app.accounts SET balance = %d WHERE id = %d", srcBal, pgPlantedID))
	comparePG(t, src, dst, pgAppTables...)
}

func TestG2_E2E_MySQLToPGHotRowUnverifiable(t *testing.T) {
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startPG(t, "pgdst")
	src.exec(t, "SET GLOBAL innodb_flush_log_at_trx_commit = 2", "SET GLOBAL sync_binlog = 0",
		"CREATE DATABASE hot",
		"CREATE TABLE hot.accounts (id BIGINT PRIMARY KEY, note VARCHAR(64) NOT NULL, balance BIGINT NOT NULL, ver BIGINT NOT NULL)")
	dst.exec(t, "CREATE SCHEMA hot",
		"CREATE TABLE hot.accounts (id bigint PRIMARY KEY, note varchar(64) NOT NULL, balance bigint NOT NULL, ver bigint NOT NULL)")
	const rows = 2000
	tx, err := src.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= rows; i++ {
		if _, err := tx.Exec("INSERT INTO hot.accounts VALUES (?, ?, ?, ?)", i, fmt.Sprintf("n%d", i), i*10, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	cfg := strandCfg{Name: "g2-hotrow-mypg", TargetKind: "postgres", SourceDSN: src.RootDSN, TargetDSN: dst.DSN,
		Include: []string{"hot.*"}, Dir: testDir(t), VerifyDelta: "on", MetricsAddr: freeAddr(t)}
	path := cfg.write(t)
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 2*time.Minute)

	dst.exec(t, fmt.Sprintf("UPDATE hot.accounts SET note = 'PLANTED-MISMATCH' WHERE id = %d", pgPlantedID))
	hot := startPGHotUpdater(src.DB, "UPDATE hot.accounts SET ver = UUID_SHORT(), balance = balance + 1 WHERE id = ?", pgHotRowID, pgHotRate, 4)
	defer hot.Stop()
	time.Sleep(5 * time.Second)
	if r := hot.rate(); r < pgHotRateMin {
		t.Fatalf("hot row updater reached only %.0f updates/s (want ~%d); errors %d, last %v", r, pgHotRate, hot.errs.Load(), hot.lastErr.Load())
	}

	pgHotVerifyRuns(t, "full", pgHotFullRe, 0, hot, p, "verify", "-c", path, "--mode", "full", "--table", "hot.accounts", "--rate", "1000")
	pgHotVerifyRuns(t, "sample", pgHotSampleRe, rows, hot, p, "verify", "-c", path, "--mode", "sample", "--table", "hot.accounts", "--rate", "1.0")
	pgHotDelta(t, &cfg, path, "hot.accounts", p)

	if r := hot.Stop(); r < pgHotRateMin {
		t.Fatalf("hot row rate over the whole run %.0f/s < %d", r, pgHotRateMin)
	}
	waitMySQLPGCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	var note string
	if err := dst.DB.QueryRow("SELECT note FROM hot.accounts WHERE id = $1", pgPlantedID).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if note != "PLANTED-MISMATCH" {
		t.Fatalf("planted row %d changed on target to %q", pgPlantedID, note)
	}
	dst.exec(t, fmt.Sprintf("UPDATE hot.accounts SET note = 'n%d' WHERE id = %d", pgPlantedID, pgPlantedID))
	// Trạng thái cuối giống hệt: mọi cột đều là số / chuỗi ASCII, so theo text.
	dump := func(db *sql.DB, q string) map[string]string {
		rs, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer rs.Close()
		out := map[string]string{}
		for rs.Next() {
			var k, v string
			if err := rs.Scan(&k, &v); err != nil {
				t.Fatal(err)
			}
			out[k] = v
		}
		return out
	}
	s := dump(src.DB, "SELECT CAST(id AS CHAR), CONCAT(note, '|', CAST(balance AS CHAR), '|', CAST(ver AS CHAR)) FROM hot.accounts")
	d := dump(dst.DB, "SELECT id::text, note || '|' || balance::text || '|' || ver::text FROM hot.accounts")
	if len(s) != rows || len(d) != len(s) {
		t.Fatalf("row count source %d target %d, want %d", len(s), len(d), rows)
	}
	for k, v := range s {
		if d[k] != v {
			t.Fatalf("row %s differs: source %q target %q", k, v, d[k])
		}
	}
}
