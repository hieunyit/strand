package e2e

// Helper dùng chung cho các test bắt buộc Gate 2 (01 §6, HANDOFF §8: G2-6, G2-8, G2-11): đọc
// metric Prometheus của Strand, đo RSS của process strand, theo dõi kích thước checkpoint trên target
// PG, chạy tải giãn nhịp.

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// Ngưỡng CP-4 (02 §2, §6): |holes| ≤ 4.096 (max_holes), row delta ≤ 64 KB.
const (
	cp4MaxHoles      = 4096
	cp4MaxDeltaBytes = 64 * 1024
)

// envDuration đọc biến môi trường dạng time.Duration ("30m", "90s"); sai hoặc rỗng thì dùng def.
func envDuration(name string, def time.Duration) time.Duration {
	if s := os.Getenv(name); s != "" {
		if v, err := time.ParseDuration(s); err == nil && v > 0 {
			return v
		}
	}
	return def
}

// --------------------------------------------------------------------------
// Metric Prometheus
// --------------------------------------------------------------------------

func scrapeMetrics(addr string) (string, error) {
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://" + addr + "/metrics")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

// metricSum cộng giá trị mọi series của metric name có đủ các label trong want (định dạng text của
// Prometheus: `name{k="v",...} value`). ok = có ít nhất một series khớp.
func metricSum(text, name string, want map[string]string) (sum float64, ok bool) {
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		match := true
		for k, v := range want {
			if !strings.Contains(line, k+`="`+v+`"`) {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		v, err := strconv.ParseFloat(line[i+1:], 64)
		if err != nil {
			continue
		}
		sum += v
		ok = true
	}
	return sum, ok
}

// --------------------------------------------------------------------------
// RSS của process strand (/proc/<pid>/status)
// --------------------------------------------------------------------------

// procMemKB đọc VmHWM (RSS đỉnh của process từ lúc khởi động) và VmRSS hiện tại, đơn vị KiB.
func procMemKB(pid int) (hwm, rss int64, err error) {
	f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), ":")
		if !ok || (k != "VmHWM" && k != "VmRSS") {
			continue
		}
		n, _ := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
		if k == "VmHWM" {
			hwm = n
		} else {
			rss = n
		}
	}
	return hwm, rss, sc.Err()
}

// rssSampler lấy mẫu VmHWM/VmRSS của một process strand: VmHWM đơn điệu trong đời process nhưng mất
// khi process thoát, nên phải giữ giá trị lớn nhất đã đọc.
type rssSampler struct {
	stop    atomic.Bool
	wg      sync.WaitGroup
	mu      sync.Mutex
	hwmKB   int64
	rssKB   int64
	samples int
}

func startRSSSampler(pid int) *rssSampler {
	s := &rssSampler{}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for !s.stop.Load() {
			if hwm, rss, err := procMemKB(pid); err == nil {
				s.mu.Lock()
				s.hwmKB = max(s.hwmKB, hwm)
				s.rssKB = max(s.rssKB, rss)
				s.samples++
				s.mu.Unlock()
			}
			time.Sleep(250 * time.Millisecond)
		}
	}()
	return s
}

// finish dừng lấy mẫu (gọi khi process còn sống để có lần đọc VmHWM cuối) và trả đỉnh KiB.
func (s *rssSampler) finish(pid int) (hwmKB, rssKB int64, samples int) {
	if hwm, rss, err := procMemKB(pid); err == nil {
		s.mu.Lock()
		s.hwmKB, s.rssKB = max(s.hwmKB, hwm), max(s.rssKB, rss)
		s.mu.Unlock()
	}
	s.stop.Store(true)
	s.wg.Wait()
	return s.hwmKB, s.rssKB, s.samples
}

// countSpillFiles đếm file *.spill dưới thư mục state (spill của assembler, Txn đã route, buffer
// reader PG).
func countSpillFiles(dir string) int {
	n := 0
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".spill") {
			n++
		}
		return nil
	})
	return n
}

// --------------------------------------------------------------------------
// Checkpoint trên target PG: kích thước row, số holes (CP-4)
// --------------------------------------------------------------------------

// appliedHoles đếm holes trong cột applied (mã hóa theo engine của source, 02 §4):
//   - PG: JSON {"lwd","high","holes","applied"} → len(holes); extra = số LSN > high (applied).
//   - MySQL: GTID set "uuid:1-5:7-9,..." (có thể có tag) → mỗi nhóm (uuid, tag) có n khoảng thì có
//     n−1 lỗ.
func appliedHoles(engine model.Engine, applied string) (holes, extra int, err error) {
	applied = strings.TrimSpace(applied)
	if applied == "" {
		return 0, 0, nil
	}
	if engine == model.EnginePG {
		var pj checkpoint.PGAppliedJSON
		if err := json.Unmarshal([]byte(applied), &pj); err != nil {
			return 0, 0, err
		}
		return len(pj.Holes), len(pj.Applied), nil
	}
	for _, part := range strings.Split(strings.ReplaceAll(applied, "\n", ""), ",") {
		fields := strings.Split(strings.TrimSpace(part), ":")
		group := 0
		for _, f := range fields[1:] {
			if f == "" {
				continue
			}
			if f[0] < '0' || f[0] > '9' { // tag: bắt đầu nhóm mới
				holes += max(0, group-1)
				group = 0
				continue
			}
			group++
		}
		holes += max(0, group-1)
	}
	return holes, 0, nil
}

// ckptStats: giá trị lớn nhất quan sát được trong lúc chạy.
type ckptStats struct {
	GlobalBytes   int     // octet_length(applied) của row toàn cục
	DeltaBytes    int     // octet_length(applied) lớn nhất trong các row delta
	GlobalHoles   int     // holes trong row toàn cục
	DeltaHoles    int     // holes lớn nhất trong một row delta
	DeltaExtra    int     // (PG) số LSN > high lớn nhất trong một row delta
	MetricHoles   float64 // strand_holes
	LagSeconds    float64 // strand_lag_seconds{phase="synced"}
	Samples       int
	QueryErrors   int
	LastQueryErr  string
	WorstDeltaRow string // 300 byte đầu của row delta lớn nhất (chẩn đoán)
}

// ckptMonitor lấy mẫu _strand_checkpoint / _strand_worker_delta trên target PG và metric holes, lag
// mỗi giây, giữ giá trị lớn nhất (02 §6: checkpoint không được phình theo thời gian).
type ckptMonitor struct {
	stop atomic.Bool
	wg   sync.WaitGroup
	mu   sync.Mutex
	max  ckptStats
}

func startCkptMonitor(dst *sql.DB, pipelineID string, srcEngine model.Engine, metricsAddr string) *ckptMonitor {
	m := &ckptMonitor{}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for !m.stop.Load() {
			m.sample(dst, pipelineID, srcEngine, metricsAddr)
			time.Sleep(time.Second)
		}
	}()
	return m
}

func (m *ckptMonitor) sample(dst *sql.DB, pipelineID string, srcEngine model.Engine, metricsAddr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fail := func(err error) {
		m.mu.Lock()
		m.max.QueryErrors++
		m.max.LastQueryErr = err.Error()
		m.mu.Unlock()
	}
	var global string
	if err := dst.QueryRowContext(ctx, "SELECT applied FROM _strand_checkpoint WHERE pipeline_id = $1", pipelineID).Scan(&global); err != nil && err != sql.ErrNoRows {
		fail(err)
		return
	}
	gh, _, err := appliedHoles(srcEngine, global)
	if err != nil {
		fail(fmt.Errorf("parse global applied: %w", err))
	}
	rows, err := dst.QueryContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id = $1", pipelineID)
	if err != nil {
		fail(err)
		return
	}
	var dBytes, dHoles, dExtra int
	var worst string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			fail(err)
			continue
		}
		if len(a) > dBytes {
			dBytes, worst = len(a), a
		}
		h, x, err := appliedHoles(srcEngine, a)
		if err != nil {
			fail(fmt.Errorf("parse delta applied: %w", err))
		}
		dHoles, dExtra = max(dHoles, h), max(dExtra, x)
	}
	rows.Close()
	var mh, lag float64
	if text, err := scrapeMetrics(metricsAddr); err == nil {
		mh, _ = metricSum(text, "strand_holes", map[string]string{"pipeline": pipelineID})
		lag, _ = metricSum(text, "strand_lag_seconds", map[string]string{"pipeline": pipelineID, "phase": "synced"})
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := &m.max
	s.Samples++
	s.GlobalBytes, s.GlobalHoles = max(s.GlobalBytes, len(global)), max(s.GlobalHoles, gh)
	if dBytes > s.DeltaBytes {
		s.DeltaBytes = dBytes
		if len(worst) > 300 {
			worst = worst[:300]
		}
		s.WorstDeltaRow = worst
	}
	s.DeltaHoles, s.DeltaExtra = max(s.DeltaHoles, dHoles), max(s.DeltaExtra, dExtra)
	s.MetricHoles, s.LagSeconds = max(s.MetricHoles, mh), max(s.LagSeconds, lag)
}

func (m *ckptMonitor) snapshot() ckptStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.max
}

func (m *ckptMonitor) finish() ckptStats {
	m.stop.Store(true)
	m.wg.Wait()
	return m.snapshot()
}

// checkCP4 áp ngưỡng CP-4 lên giá trị lớn nhất quan sát được.
func checkCP4(t *testing.T, s ckptStats) {
	t.Helper()
	t.Logf("checkpoint over %d samples (%d query errors): global row max %d B / %d holes; delta row max %d B / %d holes / %d LSN>high; strand_holes max %.0f; lag max %.1fs",
		s.Samples, s.QueryErrors, s.GlobalBytes, s.GlobalHoles, s.DeltaBytes, s.DeltaHoles, s.DeltaExtra, s.MetricHoles, s.LagSeconds)
	if s.Samples < 10 || s.Samples-s.QueryErrors < 10 {
		t.Fatalf("checkpoint monitor took too few good samples (%d, %d errors, last %q)", s.Samples, s.QueryErrors, s.LastQueryErr)
	}
	if s.DeltaBytes > cp4MaxDeltaBytes {
		t.Errorf("CP-4: a _strand_worker_delta row reached %d bytes (> %d); head: %s", s.DeltaBytes, cp4MaxDeltaBytes, s.WorstDeltaRow)
	}
	if s.GlobalHoles > cp4MaxHoles || s.DeltaHoles > cp4MaxHoles || s.MetricHoles > cp4MaxHoles {
		t.Errorf("CP-4: holes exceeded %d (global %d, delta %d, strand_holes %.0f)", cp4MaxHoles, s.GlobalHoles, s.DeltaHoles, s.MetricHoles)
	}
}

// --------------------------------------------------------------------------
// Tải giãn nhịp
// --------------------------------------------------------------------------

// pacedLoad chạy fn trên clients goroutine với tổng nhịp ≈ opsPerSec (mỗi client một nhịp riêng) —
// test dài (G2-6) không được để tải tự do làm dữ liệu phình tới mức bước so sánh cuối hết RAM.
type pacedLoad struct {
	stopped atomic.Bool
	wg      sync.WaitGroup
	ops     atomic.Int64
	errs    atomic.Int64
	lastErr atomic.Value
}

func startPacedLoad(seed int64, clients int, opsPerSec float64, fn func(r *rand.Rand) error) *pacedLoad {
	l := &pacedLoad{}
	gap := time.Duration(float64(time.Second) * float64(clients) / opsPerSec)
	rng := rand.New(rand.NewSource(seed))
	for c := 0; c < clients; c++ {
		r := rand.New(rand.NewSource(rng.Int63()))
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			next := time.Now()
			for !l.stopped.Load() {
				if err := fn(r); err != nil {
					l.errs.Add(1)
					l.lastErr.Store(err.Error())
				}
				l.ops.Add(1)
				next = next.Add(gap)
				if d := time.Until(next); d > 0 {
					time.Sleep(d)
				} else if d < -time.Second {
					next = time.Now() // tụt quá xa thì không dồn bù
				}
			}
		}()
	}
	return l
}

func (l *pacedLoad) Stop() {
	l.stopped.Store(true)
	l.wg.Wait()
}

func (l *pacedLoad) String() string {
	s := fmt.Sprintf("%d ops, %d client-side errors", l.ops.Load(), l.errs.Load())
	if e, ok := l.lastErr.Load().(string); ok {
		s += fmt.Sprintf(" (last: %.200s)", e)
	}
	return s
}

// Kiểm helper đọc checkpoint / metric (không cần container).
func TestG2_HelperParsers(t *testing.T) {
	for _, c := range []struct {
		engine        model.Engine
		in            string
		holes, extras int
	}{
		{model.EngineMySQL, "", 0, 0},
		{model.EngineMySQL, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", 0, 0},
		{model.EngineMySQL, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:7-9:11,\n4E11FA47-71CA-11E1-9E33-C80AA9429562:1-3", 2, 0},
		{model.EngineMySQL, "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:7:tag1:1-2:4", 2, 0},
		{model.EnginePG, `{"lwd":"0/10","high":"0/40","holes":["0/20","0/30"],"applied":["0/50"]}`, 2, 1},
		{model.EnginePG, `{"lwd":"0/10","high":"0/10","holes":[]}`, 0, 0},
	} {
		h, x, err := appliedHoles(c.engine, c.in)
		if err != nil || h != c.holes || x != c.extras {
			t.Errorf("appliedHoles(%s, %q) = %d, %d, %v; want %d, %d", c.engine, c.in, h, x, err, c.holes, c.extras)
		}
	}
	text := "# HELP x\nstrand_holes{pipeline=\"p\"} 3\nstrand_writeset_entries{pipeline=\"p\",table=\"a.b\"} 10\n" +
		"strand_writeset_entries{pipeline=\"p\",table=\"a.c\"} 2.5e+01\nstrand_writeset_entries{pipeline=\"q\",table=\"a.c\"} 7\n"
	if v, ok := metricSum(text, "strand_writeset_entries", map[string]string{"pipeline": "p"}); !ok || v != 35 {
		t.Errorf("metricSum writeset = %v, %v; want 35", v, ok)
	}
	if v, ok := metricSum(text, "strand_holes", map[string]string{"pipeline": "p"}); !ok || v != 3 {
		t.Errorf("metricSum holes = %v, %v; want 3", v, ok)
	}
	if _, ok := metricSum(text, "strand_holes", map[string]string{"pipeline": "zz"}); ok {
		t.Errorf("metricSum matched a missing label")
	}
	if hwm, rss, err := procMemKB(os.Getpid()); err != nil || hwm <= 0 || rss <= 0 || rss > hwm {
		t.Errorf("procMemKB(self) = %d, %d, %v", hwm, rss, err)
	}
}
