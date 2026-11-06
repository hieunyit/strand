package e2e

// G2-11 (01 §6 Gate 2, HANDOFF §8): hot row 1.000 update/giây được báo unverifiable_hot, nhưng lệch
// thật cài vào một row khác vẫn bị phát hiện.
//
// Đường báo unverifiable_hot trong code hiện tại (verify.LiveVerifier.recheck: đọc S1 ở source, chờ
// target apply gtid_executed, đọc target, đọc lại S2; S1 ≠ S2 ở mọi lần thử → hot) chỉ có cho
// MySQL→MySQL: `strand verify --mode sample` (CLI, cmd/strand/verify.go) và `--mode delta` (trong
// pipeline, internal/pipeline/verify.go). `--mode full` với PG ở một phía so row thẳng, không có kiểm
// lại nên không bao giờ báo unverifiable_hot (xem báo cáo). Vì vậy test chạy trên MySQL→MySQL.
//
// Kịch bản: account id=1 được UPDATE ~1.000 lần/giây ở source suốt lúc verify; account id=777 (không
// ai ghi) bị sửa thẳng ở target. Kiểm:
//   - sample --rate 1.0: exit 5 (exitcode.DataError, verify_mismatch), mismatch đúng một row là 777,
//     row nóng không bao giờ bị báo lệch, và unverifiable_hot = 1 (row nóng) — tối đa 3 lượt, vì có
//     thể lượt so range đầu tiên đọc trúng lúc target bằng source.
//   - delta (khóa CDC đã chạm = row nóng): không báo lệch giả, metric
//     strand_verify_unverifiable_hot_total{table="app.accounts"} tăng.
//   - Cuối: trả row 777 về đúng giá trị source, trạng thái cuối giống hệt (row nóng không mất bản ghi nào).

import (
	"fmt"
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
	hotRowID     = 1
	plantedRowID = 777
	hotRowRate   = 1000 // update/giây
	hotRowMinOK  = 800  // nhịp đạt được tối thiểu để kết quả có nghĩa
)

var sampleCountsRe = regexp.MustCompile(`(\d+) rows, (\d+) mismatches, (\d+) unverifiable_hot`)

// hotUpdater: workers goroutine chia nhau đúng nhịp rate update/giây trên một row (nhịp chung theo
// đồng hồ, worker nào rảnh thì lấy lượt — bù độ trễ của một lần commit).
type hotUpdater struct {
	stopped atomic.Bool
	wg      sync.WaitGroup
	n       atomic.Int64
	errs    atomic.Int64
	lastErr atomic.Value
	start   time.Time
	end     time.Time
}

func startHotUpdater(src *mysqlNode, id int64, rate, workers int) *hotUpdater {
	h := &hotUpdater{start: time.Now()}
	var ticket atomic.Int64
	for w := 0; w < workers; w++ {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			for !h.stopped.Load() {
				k := ticket.Add(1) - 1
				due := h.start.Add(time.Duration(k) * time.Second / time.Duration(rate))
				if d := time.Until(due); d > 0 {
					time.Sleep(d)
				}
				if _, err := src.DB.Exec("UPDATE app.accounts SET ver = UUID_SHORT(), balance = balance + 1, updated_at = NOW(6) WHERE id = ?", id); err != nil {
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

// rate trả nhịp đạt được (update/giây) tính tới lúc gọi.
func (h *hotUpdater) rate() float64 {
	return float64(h.n.Load()) / time.Since(h.start).Seconds()
}

func (h *hotUpdater) Stop() float64 {
	if !h.stopped.Swap(true) {
		h.wg.Wait()
		h.end = time.Now()
	}
	return float64(h.n.Load()) / h.end.Sub(h.start).Seconds()
}

func TestG2_E2E_HotRowUnverifiableAndPlantedMismatch(t *testing.T) {
	src, dst := newPair(t)
	// 1.000 commit/giây nối tiếp trên cùng một row: không fsync mỗi commit (cả hai phía), nếu không
	// nhịp bị chặn bởi độ trễ fsync của máy test chứ không phải bởi Strand.
	for _, n := range []*mysqlNode{src, dst} {
		n.exec(t, "SET GLOBAL innodb_flush_log_at_trx_commit = 2", "SET GLOBAL sync_binlog = 0")
	}
	wl := newWorkload(src.DB, 1)
	wl.Preload(t, 2000)
	cfg := strandCfg{Name: "g2-hotrow", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t), VerifyDelta: "on", MetricsAddr: freeAddr(t)}
	path := cfg.write(t)
	p := startStrand(t, path)
	defer func() {
		if !p.exited() {
			p.kill9()
		}
	}()
	waitSynced(t, &cfg, p, 3*time.Minute)
	waitCaughtUp(t, src, dst, &cfg, p, 2*time.Minute)

	// Lệch thật: row 777 không ai ghi ở source, bị sửa thẳng ở target.
	var srcNote string
	if err := src.DB.QueryRow("SELECT note FROM app.accounts WHERE id = ?", plantedRowID).Scan(&srcNote); err != nil {
		t.Fatal(err)
	}
	dst.exec(t, fmt.Sprintf("UPDATE app.accounts SET note = 'PLANTED-MISMATCH' WHERE id = %d", plantedRowID))

	hot := startHotUpdater(src, hotRowID, hotRowRate, 4)
	defer hot.Stop()
	time.Sleep(5 * time.Second)
	if r := hot.rate(); r < hotRowMinOK {
		t.Fatalf("hot row updater reached only %.0f updates/s (want ~%d, at least %d); errors %d, last %v",
			r, hotRowRate, hotRowMinOK, hot.errs.Load(), hot.lastErr.Load())
	}
	t.Logf("hot row id=%d: %.0f updates/s on the source", hotRowID, hot.rate())

	// 1. sample trên toàn bảng (rate 1.0): row 777 lệch thật, row nóng unverifiable_hot.
	sawHot := false
	for attempt := 1; attempt <= 3 && !sawHot; attempt++ {
		began := time.Now()
		out, code := runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--table", "app.accounts", "--mode", "sample", "--rate", "1.0")
		m := sampleCountsRe.FindStringSubmatch(out)
		if m == nil {
			t.Fatalf("attempt %d: verify output has no summary line (exit %d, hot rate %.0f/s):\n%s\n%s", attempt, code, hot.rate(), out, p.tail())
		}
		rows, _ := strconv.Atoi(m[1])
		mism, _ := strconv.Atoi(m[2])
		hotN, _ := strconv.Atoi(m[3])
		t.Logf("attempt %d (%s): exit %d, %d rows, %d mismatches, %d unverifiable_hot", attempt, time.Since(began).Round(time.Millisecond), code, rows, mism, hotN)
		if code != exitcode.DataError || !strings.Contains(out, exitcode.ErrVerifyMismatch) {
			t.Fatalf("attempt %d: planted mismatch not reported: exit %d, want %d %s\n%s", attempt, code, exitcode.DataError, exitcode.ErrVerifyMismatch, out)
		}
		// Chỉ row 777 là lệch thật; row nóng (id 1) không được lọt vào danh sách lệch.
		if mism != 1 || !strings.Contains(out, fmt.Sprintf("(pk %d)", plantedRowID)) {
			t.Fatalf("attempt %d: want exactly one mismatch, pk %d (hot row %d must be unverifiable_hot, not a mismatch):\n%s",
				attempt, plantedRowID, hotRowID, out)
		}
		if rows < 2000 {
			t.Fatalf("attempt %d: sample --rate 1.0 compared only %d rows of 2000", attempt, rows)
		}
		if hotN > 1 {
			t.Fatalf("attempt %d: %d rows reported unverifiable_hot, only row %d is hot:\n%s", attempt, hotN, hotRowID, out)
		}
		sawHot = hotN == 1
	}
	if !sawHot {
		t.Fatalf("hot row %d (%.0f updates/s) was never reported unverifiable_hot in 3 sample runs", hotRowID, hot.rate())
	}

	// 2. delta: khóa CDC đã chạm chỉ có row nóng — không lệch giả, metric unverifiable_hot tăng.
	unverifiable := func() float64 {
		text, err := scrapeMetrics(cfg.MetricsAddr)
		if err != nil {
			t.Fatalf("scrape metrics: %v", err)
		}
		v, _ := metricSum(text, "strand_verify_unverifiable_hot_total", map[string]string{"pipeline": cfg.Name, "table": "app.accounts"})
		return v
	}
	base := unverifiable()
	for attempt := 1; attempt <= 3 && unverifiable() <= base; attempt++ {
		out, code := runStrandCLI(t, 10*time.Minute, "verify", "-c", path, "--mode", "delta", "--socket", cfg.Socket)
		t.Logf("delta attempt %d: exit %d: %s", attempt, code, strings.TrimSpace(out))
		if code != 0 {
			t.Fatalf("verify delta on the hot row reported a mismatch or failed: exit %d\n%s\n%s", code, out, p.tail())
		}
	}
	if got := unverifiable(); got <= base {
		t.Fatalf("strand_verify_unverifiable_hot_total{table=app.accounts} did not increase over 3 delta runs (%.0f)", got)
	} else {
		t.Logf("strand_verify_unverifiable_hot_total{table=app.accounts}: %.0f -> %.0f", base, got)
	}

	achieved := hot.Stop()
	t.Logf("hot row updater: %d updates, %.0f/s overall, %d errors", hot.n.Load(), achieved, hot.errs.Load())
	if achieved < hotRowMinOK {
		t.Fatalf("hot row rate over the whole run %.0f/s < %d", achieved, hotRowMinOK)
	}
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d\n%s", code, p.tail())
	}
	// Pipeline không chạm row 777: lệch cài vào vẫn còn nguyên cho tới khi sửa tay.
	var dstNote string
	if err := dst.DB.QueryRow("SELECT note FROM app.accounts WHERE id = ?", plantedRowID).Scan(&dstNote); err != nil {
		t.Fatal(err)
	}
	if dstNote != "PLANTED-MISMATCH" {
		t.Fatalf("planted row %d changed on target to %q", plantedRowID, dstNote)
	}
	if _, err := dst.DB.Exec("UPDATE app.accounts SET note = ? WHERE id = ?", srcNote, plantedRowID); err != nil {
		t.Fatal(err)
	}
	compareFinal(t, src, dst, appTables...)
}
