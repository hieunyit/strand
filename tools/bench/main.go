// Command bench chạy benchmark Gate 2 so sánh ba governor (static, onoff, grace-v2) theo giao thức
// reports/GATE2-BENCH-PROTOCOL.md (spec/04-testing.md §5). Mỗi lần chạy: dựng lại dữ liệu tất định ở
// source, khởi động lại hai container MySQL, chạy tải ứng dụng sóng vuông, khởi động `strand sync`
// snapshot bảng cố định, lấy mẫu metrics mỗi giây, so khớp cuối, rồi tổng hợp theo quy tắc quyết định.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

type options struct {
	Strand, SourceDSN, TargetDSN, SourceContainer, TargetContainer string
	Governors                                                      string
	Repeats                                                        int
	Seed                                                           int64
	BigRows, AppRows                                               int
	Clients                                                        int
	LowRate, HighRate                                              float64
	Period, SpikeEvery, SpikeDur                                   time.Duration
	SpikeMult                                                      float64
	Warmup, StreamAfter, CatchupTimeout, SnapshotTimeout           time.Duration
	SoftIOPS, HardIOPS                                             int
	LagSLO                                                         string
	MetricsAddr                                                    string
	Out                                                            string
	Only                                                           string // chạy một lần với governor này (thử nhanh), không tổng hợp
}

func main() {
	var o options
	flag.StringVar(&o.Strand, "strand", "./strand", "binary strand")
	flag.StringVar(&o.SourceDSN, "source", "", "DSN root của source MySQL (không kèm database), vd root:p@tcp(127.0.0.1:33061)/")
	flag.StringVar(&o.TargetDSN, "target", "", "DSN root của target MySQL (không kèm database)")
	flag.StringVar(&o.SourceContainer, "source-container", "", "tên container source để khởi động lại mỗi lần (rỗng = không khởi động lại)")
	flag.StringVar(&o.TargetContainer, "target-container", "", "tên container target để khởi động lại mỗi lần")
	flag.StringVar(&o.Governors, "governors", "static,onoff,grace-v2", "danh sách governor")
	flag.IntVar(&o.Repeats, "repeats", 3, "số lần mỗi governor")
	flag.Int64Var(&o.Seed, "seed", 20261105, "seed thứ tự chạy và dữ liệu")
	flag.IntVar(&o.BigRows, "big-rows", 2_000_000, "số row bảng snapshot bench.big")
	flag.IntVar(&o.AppRows, "app-rows", 100_000, "số row bảng ứng dụng bench.sbtest")
	flag.IntVar(&o.Clients, "clients", 16, "số client tải ứng dụng")
	flag.Float64Var(&o.LowRate, "low", 50, "tải mức thấp (txn/s)")
	flag.Float64Var(&o.HighRate, "high", 200, "tải mức cao (txn/s)")
	flag.DurationVar(&o.Period, "period", 10*time.Minute, "chu kỳ sóng vuông")
	flag.DurationVar(&o.SpikeEvery, "spike-every", 3*time.Minute, "khoảng cách spike")
	flag.DurationVar(&o.SpikeDur, "spike-dur", 10*time.Second, "độ dài spike")
	flag.Float64Var(&o.SpikeMult, "spike-mult", 2, "spike = mult × mức cao")
	flag.DurationVar(&o.Warmup, "warmup", 120*time.Second, "tải chạy trước khi khởi động strand")
	flag.DurationVar(&o.StreamAfter, "stream-after", 120*time.Second, "stream thêm sau snapshot")
	flag.DurationVar(&o.CatchupTimeout, "catchup-timeout", 10*time.Minute, "chờ target bắt kịp")
	flag.DurationVar(&o.SnapshotTimeout, "snapshot-timeout", 3*time.Hour, "giới hạn thời gian snapshot")
	flag.IntVar(&o.SoftIOPS, "soft-iops", 400, "resources.source.iops.soft")
	flag.IntVar(&o.HardIOPS, "hard-iops", 600, "resources.source.iops.hard")
	flag.StringVar(&o.LagSLO, "lag-slo", "5s", "governor.lag_slo")
	flag.StringVar(&o.MetricsAddr, "metrics-addr", "127.0.0.1:19190", "địa chỉ metrics của strand")
	flag.StringVar(&o.Out, "out", "bench-out", "thư mục kết quả")
	flag.StringVar(&o.Only, "only", "", "chạy thử một lần với governor này")
	flag.Parse()
	if o.SourceDSN == "" || o.TargetDSN == "" {
		log.Fatal("--source và --target là bắt buộc")
	}
	if err := os.MkdirAll(o.Out, 0o755); err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	type job struct {
		Gov string `json:"governor"`
		Rep int    `json:"repeat"`
	}
	var jobs []job
	if o.Only != "" {
		jobs = []job{{o.Only, 1}}
	} else {
		for _, g := range strings.Split(o.Governors, ",") {
			for r := 1; r <= o.Repeats; r++ {
				jobs = append(jobs, job{strings.TrimSpace(g), r})
			}
		}
		rng := rand.New(rand.NewSource(o.Seed))
		rng.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })
	}
	log.Printf("order (seed %d): %v", o.Seed, jobs)

	rep := report{Options: o, Started: time.Now().UTC()}
	for _, j := range jobs {
		rep.Order = append(rep.Order, fmt.Sprintf("%s#%d", j.Gov, j.Rep))
	}
	for i, j := range jobs {
		name := fmt.Sprintf("%02d-%s-%d", i+1, j.Gov, j.Rep)
		log.Printf("=== run %s", name)
		res := runOnce(ctx, o, name, j.Gov)
		res.Governor, res.Repeat = j.Gov, j.Rep
		log.Printf("=== run %s: snapshot=%.1fs p99=%.1fms hard_exceed=%d p2p_median=%.3f ok=%v err=%s",
			name, res.SnapshotSeconds, res.AppP99Ms, res.HardLimitExceed, res.P2PLogRMedian, res.OK, res.Error)
		rep.Runs = append(rep.Runs, res)
		writeJSON(filepath.Join(o.Out, "results.json"), rep) // ghi dần: lần chạy dài không mất kết quả
	}
	if o.Only == "" {
		rep.Summary, rep.Decision = decide(rep.Runs)
	}
	rep.Finished = time.Now().UTC()
	writeJSON(filepath.Join(o.Out, "results.json"), rep)
	log.Printf("decision: %s", rep.Decision)
}

type report struct {
	Options  options               `json:"options"`
	Started  time.Time             `json:"started"`
	Finished time.Time             `json:"finished"`
	Order    []string              `json:"order"`
	Runs     []runResult           `json:"runs"`
	Summary  map[string]govSummary `json:"summary,omitempty"`
	Decision string                `json:"decision,omitempty"`
}

type runResult struct {
	Name             string    `json:"name"`
	Governor         string    `json:"governor"`
	Repeat           int       `json:"repeat"`
	Start            time.Time `json:"start"`
	SnapshotSeconds  float64   `json:"snapshot_seconds"`
	AppP99Ms         float64   `json:"app_p99_ms_during_snapshot"`
	AppTxns          int       `json:"app_txns_during_snapshot"`
	AppErrors        int64     `json:"app_errors"`
	HardLimitExceed  int       `json:"hard_limit_exceed_samples"`
	P2PLogRMedian    float64   `json:"p2p_log_r_median"`
	P2PLogRMax       float64   `json:"p2p_log_r_max"`
	P2PWindows       int       `json:"p2p_windows"`
	P2PZeroWindows   int       `json:"p2p_windows_with_r_zero"`
	Samples          int       `json:"samples"`
	StrandExit       int       `json:"strand_exit_code"`
	CompareOK        bool      `json:"compare_ok"`
	CompareDetail    string    `json:"compare_detail"`
	LoadBefore       string    `json:"uptime_before"`
	LoadAfter        string    `json:"uptime_after"`
	OK               bool      `json:"ok"`
	Error            string    `json:"error,omitempty"`
	TimeseriesCSV    string    `json:"timeseries_csv"`
	SnapshotStartRel float64   `json:"snapshot_start_rel_s"`
}

type govSummary struct {
	Runs                  int     `json:"runs"`
	OKRuns                int     `json:"ok_runs"`
	MedianSnapshotSeconds float64 `json:"median_snapshot_seconds"`
	MedianAppP99Ms        float64 `json:"median_app_p99_ms"`
	HardLimitExceedTotal  int     `json:"hard_limit_exceed_total"`
	MaxP2PMedian          float64 `json:"max_p2p_log_r_median"`
}

func writeJSON(path string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		log.Printf("write %s: %v", path, err)
	}
}

func uptime() string {
	b, _ := os.ReadFile("/proc/loadavg")
	return strings.TrimSpace(string(b))
}

// ---------------------------------------------------------------------------
// Một lần chạy
// ---------------------------------------------------------------------------

func runOnce(ctx context.Context, o options, name, gov string) (res runResult) {
	res.Name = name
	res.Start = time.Now().UTC()
	res.LoadBefore = uptime()
	defer func() { res.LoadAfter = uptime() }()
	fail := func(err error) runResult {
		res.Error = err.Error()
		return res
	}
	runDir, err := filepath.Abs(filepath.Join(o.Out, name))
	if err != nil {
		return fail(err)
	}
	_ = os.RemoveAll(runDir)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return fail(err)
	}

	// 1. Dữ liệu tất định + target sạch.
	if err := prepare(ctx, o); err != nil {
		return fail(fmt.Errorf("prepare: %w", err))
	}
	// 2. Khởi động lại MySQL (buffer pool lạnh như nhau).
	for _, c := range []string{o.SourceContainer, o.TargetContainer} {
		if c == "" {
			continue
		}
		if out, err := exec.Command("docker", "restart", c).CombinedOutput(); err != nil {
			return fail(fmt.Errorf("docker restart %s: %v %s", c, err, out))
		}
	}
	src, err := waitDB(ctx, o.SourceDSN, 3*time.Minute)
	if err != nil {
		return fail(err)
	}
	defer src.Close()
	dst, err := waitDB(ctx, o.TargetDSN+"strand", 3*time.Minute)
	if err != nil {
		return fail(err)
	}
	defer dst.Close()

	// 3. Tải ứng dụng.
	ld := newLoad(src, o)
	ld.start()
	stopLoad := sync.OnceFunc(ld.stop)
	defer stopLoad()
	time.Sleep(o.Warmup)

	// 4. Strand.
	cfgPath := filepath.Join(runDir, "pipeline.yaml")
	pipelineID := "bench"
	if err := os.WriteFile(cfgPath, []byte(pipelineYAML(o, gov, runDir)), 0o644); err != nil {
		return fail(err)
	}
	logF, err := os.Create(filepath.Join(runDir, "strand.log"))
	if err != nil {
		return fail(err)
	}
	defer logF.Close()
	cmd := exec.Command(o.Strand, "sync", "-c", cfgPath)
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	bootDone := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(pr)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		once := sync.Once{}
		for sc.Scan() {
			line := sc.Text()
			fmt.Fprintln(logF, line)
			if strings.Contains(line, "bootstrap finished") {
				once.Do(func() { close(bootDone) })
			}
		}
	}()
	strandStart := time.Now()
	res.SnapshotStartRel = strandStart.Sub(ld.t0).Seconds()
	if err := cmd.Start(); err != nil {
		return fail(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait(); _ = pw.Close() }()

	smp := newSampler(o.MetricsAddr, pipelineID)
	sctx, scancel := context.WithCancel(ctx)
	go smp.run(sctx, ld, strandStart)

	var snapEnd time.Time
	select {
	case <-bootDone:
		snapEnd = time.Now()
	case err := <-exited:
		scancel()
		return fail(fmt.Errorf("strand exited before bootstrap finished: %v", err))
	case <-time.After(o.SnapshotTimeout):
		_ = cmd.Process.Signal(syscall.SIGTERM)
		scancel()
		return fail(fmt.Errorf("snapshot did not finish within %s", o.SnapshotTimeout))
	}
	res.SnapshotSeconds = snapEnd.Sub(strandStart).Seconds()
	time.Sleep(o.StreamAfter)
	scancel()
	stopLoad()
	res.AppErrors = ld.errs.Load()

	// 5. Chờ bắt kịp rồi dừng strand.
	if err := waitCaughtUp(ctx, src, dst, pipelineID, o.CatchupTimeout); err != nil {
		res.Error = err.Error()
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-exited:
		res.StrandExit = exitCode(err)
	case <-time.After(2 * time.Minute):
		_ = cmd.Process.Kill()
		res.StrandExit = -1
	}

	// 6. Số liệu.
	lat := ld.latenciesBetween(strandStart, snapEnd)
	res.AppTxns = len(lat)
	res.AppP99Ms = percentile(lat, 0.99)
	samples := smp.snapshot()
	res.Samples = len(samples)
	for _, s := range samples {
		if !s.T.After(snapEnd) && s.R > float64(o.HardIOPS)+1e-9 {
			res.HardLimitExceed++
		}
	}
	res.P2PLogRMedian, res.P2PLogRMax, res.P2PWindows, res.P2PZeroWindows = p2pLogR(samples, ld, strandStart, snapEnd, o)
	res.TimeseriesCSV = filepath.Join(runDir, "timeseries.csv")
	writeCSV(res.TimeseriesCSV, samples, ld, strandStart, snapEnd)

	// 7. So khớp.
	res.CompareOK, res.CompareDetail = compare(ctx, src, o.TargetDSN)
	res.OK = res.Error == "" && res.StrandExit == 0 && res.CompareOK && res.SnapshotSeconds > 0
	return res
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}

func pipelineYAML(o options, gov, runDir string) string {
	return fmt.Sprintf(`version: 1
name: bench
source: { kind: mysql, dsn: %q, server_id: 47999 }
target: { kind: mysql, dsn: %q }
tables: { include: ["bench.*"] }
bootstrap: { default: logical }
checkpoint: { store: target, flush_every: 200ms }
resources:
  source: { iops: { soft: %d, hard: %d } }
  spool: { path: %q, hard: 2GiB }
governor: { algorithm: %s, tick: 1s, lag_slo: %s }
observability: { metrics: %q, log: { level: info, format: json } }
`, o.SourceDSN, o.TargetDSN+"strand", o.SoftIOPS, o.HardIOPS, filepath.Join(runDir, "state"), gov, o.LagSLO, o.MetricsAddr)
}

// ---------------------------------------------------------------------------
// Dữ liệu
// ---------------------------------------------------------------------------

const bigDDL = `CREATE TABLE bench.big (id BIGINT PRIMARY KEY, a INT NOT NULL, b BIGINT NOT NULL,
  c VARCHAR(100) NOT NULL, d VARCHAR(100) NOT NULL, ver INT NOT NULL DEFAULT 0, KEY ka (a)) ENGINE=InnoDB`
const appDDL = `CREATE TABLE bench.sbtest (id BIGINT PRIMARY KEY, k INT NOT NULL, c CHAR(120) NOT NULL,
  pad CHAR(60) NOT NULL, KEY kk (k)) ENGINE=InnoDB`

func prepare(ctx context.Context, o options) error {
	src, err := waitDB(ctx, o.SourceDSN, time.Minute)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := waitDB(ctx, o.TargetDSN, time.Minute)
	if err != nil {
		return err
	}
	defer dst.Close()
	// Một kết nối: biến session (cte, digits tạm) phải nằm cùng phiên.
	conn, err := src.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	digits := "(SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)"
	gen := func(n int) string {
		return fmt.Sprintf(`SELECT a.d+10*b.d+100*c.d+1000*e.d+10000*f.d+100000*g.d+1000000*h.d+1 AS n
		  FROM %[1]s a, %[1]s b, %[1]s c, %[1]s e, %[1]s f, %[1]s g, %[1]s h
		 WHERE a.d+10*b.d+100*c.d+1000*e.d+10000*f.d+100000*g.d+1000000*h.d < %[2]d`, digits, n)
	}
	if o.BigRows > 10_000_000 || o.AppRows > 10_000_000 {
		return fmt.Errorf("big-rows/app-rows tối đa 10.000.000")
	}
	stmts := []string{
		"DROP DATABASE IF EXISTS bench", "CREATE DATABASE bench", bigDDL, appDDL,
		// Nội dung chỉ phụ thuộc n và seed → giống hệt ở mọi lần (tương đương khôi phục cùng snapshot).
		fmt.Sprintf(`INSERT INTO bench.big (id,a,b,c,d) SELECT n, CRC32(CONCAT(n,'a%[1]d')) %% 100000, CRC32(CONCAT(n,'b%[1]d'))*7,
		   CONCAT(MD5(CONCAT(n,'c%[1]d')),MD5(CONCAT(n,'x'))), MD5(CONCAT(n,'d%[1]d')) FROM (%[2]s) t`, o.Seed, gen(o.BigRows)),
		fmt.Sprintf(`INSERT INTO bench.sbtest (id,k,c,pad) SELECT n, CRC32(CONCAT(n,'k%[1]d')) %% %[3]d,
		   RPAD(MD5(CONCAT(n,'c%[1]d')),120,'#'), RPAD(MD5(CONCAT(n,'p%[1]d')),60,'@') FROM (%[2]s) t`, o.Seed, gen(o.AppRows), o.AppRows),
		"RESET BINARY LOGS AND GTIDS", // pipeline mới mỗi lần: giữ binlog nhỏ
	}
	for _, q := range stmts {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("source %q: %w", firstLine(q), err)
		}
	}
	for _, q := range []string{"DROP DATABASE IF EXISTS bench", "DROP DATABASE IF EXISTS strand", "CREATE DATABASE strand",
		"CREATE DATABASE bench", bigDDL, appDDL, "RESET BINARY LOGS AND GTIDS"} {
		if _, err := dst.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("target %q: %w", firstLine(q), err)
		}
	}
	return nil
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return s[:i]
	}
	return s
}

func waitDB(ctx context.Context, dsn string, timeout time.Duration) (*sql.DB, error) {
	deadline := time.Now().Add(timeout)
	for {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			if err = db.PingContext(ctx); err == nil {
				db.SetMaxOpenConns(64)
				return db, nil
			}
			db.Close()
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("connect %s: %w", redact(dsn), err)
		}
		time.Sleep(time.Second)
	}
}

func redact(dsn string) string {
	if i := strings.Index(dsn, "@"); i > 0 {
		if j := strings.Index(dsn[:i], ":"); j >= 0 {
			return dsn[:j+1] + "***" + dsn[i:]
		}
	}
	return dsn
}

func waitCaughtUp(ctx context.Context, src, dst *sql.DB, pipelineID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		var executed string
		if err := src.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
			return err
		}
		want, err := checkpoint.ParseMySQLGTIDSet(executed)
		if err != nil {
			return err
		}
		gotSet, err := checkpoint.LoadAppliedSnapshotMySQL(ctx, dst, pipelineID, model.EngineMySQL)
		if err == nil {
			got, _ := gotSet.(*checkpoint.MySQLAppliedSet)
			miss := want.Clone().(*checkpoint.MySQLAppliedSet)
			if got != nil {
				miss.Subtract(got)
				if strings.TrimSpace(miss.String()) == "" {
					return nil
				}
				last = fmt.Errorf("missing %s", miss.String())
			}
		} else {
			last = err
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("target not caught up after %s: %v", timeout, last)
}

func compare(ctx context.Context, src *sql.DB, targetRoot string) (bool, string) {
	dst, err := sql.Open("mysql", targetRoot)
	if err != nil {
		return false, err.Error()
	}
	defer dst.Close()
	q := map[string]string{
		"bench.big":    "SELECT COUNT(*), COALESCE(BIT_XOR(CRC32(CONCAT_WS('#',id,a,b,c,d,ver))),0) FROM bench.big",
		"bench.sbtest": "SELECT COUNT(*), COALESCE(BIT_XOR(CRC32(CONCAT_WS('#',id,k,c,pad))),0) FROM bench.sbtest",
	}
	ok := true
	var parts []string
	for _, t := range []string{"bench.big", "bench.sbtest"} {
		var sn, dn int64
		var sc, dc uint64
		e1 := src.QueryRowContext(ctx, q[t]).Scan(&sn, &sc)
		e2 := dst.QueryRowContext(ctx, q[t]).Scan(&dn, &dc)
		if e1 != nil || e2 != nil || sn != dn || sc != dc {
			ok = false
		}
		parts = append(parts, fmt.Sprintf("%s src=%d/%x dst=%d/%x %v %v", t, sn, sc, dn, dc, e1, e2))
	}
	return ok, strings.Join(parts, "; ")
}

// ---------------------------------------------------------------------------
// Tải ứng dụng (kiểu sysbench oltp_read_write, rate theo sóng vuông + spike)
// ---------------------------------------------------------------------------

type latSample struct {
	at time.Time
	us int64
}

type load struct {
	db    *sql.DB
	o     options
	t0    time.Time
	stopC chan struct{}
	wg    sync.WaitGroup
	mu    sync.Mutex
	lat   []latSample
	errs  atomic.Int64
	next  atomic.Int64
}

func newLoad(db *sql.DB, o options) *load {
	l := &load{db: db, o: o, stopC: make(chan struct{})}
	l.next.Store(int64(o.AppRows) + 1)
	return l
}

// rateAt: mức tải mục tiêu (txn/s) tại thời điểm t kể từ lúc bắt đầu tải.
func (l *load) rateAt(d time.Duration) float64 {
	r := l.o.LowRate
	if half := l.o.Period / 2; half > 0 && (d/half)%2 == 1 {
		r = l.o.HighRate
	}
	if l.o.SpikeEvery > 0 && d >= l.o.SpikeEvery && d%l.o.SpikeEvery < l.o.SpikeDur {
		r = l.o.HighRate * l.o.SpikeMult
	}
	return r
}

func (l *load) start() {
	l.t0 = time.Now()
	jobs := make(chan struct{}, l.o.Clients*4)
	for i := 0; i < l.o.Clients; i++ {
		l.wg.Add(1)
		rng := rand.New(rand.NewSource(l.o.Seed + int64(i)*7919))
		go func() {
			defer l.wg.Done()
			for range jobs {
				st := time.Now()
				if err := l.txn(rng); err != nil {
					l.errs.Add(1)
					continue
				}
				l.mu.Lock()
				l.lat = append(l.lat, latSample{at: st, us: time.Since(st).Microseconds()})
				l.mu.Unlock()
			}
		}()
	}
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		defer close(jobs)
		due := time.Now()
		for {
			select {
			case <-l.stopC:
				return
			default:
			}
			r := l.rateAt(time.Since(l.t0))
			due = due.Add(time.Duration(float64(time.Second) / r))
			if d := time.Until(due); d > 0 {
				time.Sleep(d)
			} else if -d > time.Second {
				due = time.Now() // tụt quá xa: không dồn burst
			}
			select {
			case jobs <- struct{}{}:
			case <-l.stopC:
				return
			}
		}
	}()
}

func (l *load) stop() {
	close(l.stopC)
	l.wg.Wait()
}

func (l *load) txn(rng *rand.Rand) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n := int64(l.o.AppRows)
	for i := 0; i < 10; i++ {
		var c string
		if err := tx.QueryRowContext(ctx, "SELECT c FROM bench.sbtest WHERE id = ?", 1+rng.Int63n(n)).Scan(&c); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE bench.sbtest SET k = k + 1 WHERE id = ?", 1+rng.Int63n(n)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE bench.sbtest SET c = ? WHERE id = ?", fmt.Sprintf("%0120d", rng.Int63()), 1+rng.Int63n(n)); err != nil {
		return err
	}
	id := 1 + rng.Int63n(n)
	if _, err := tx.ExecContext(ctx, "DELETE FROM bench.sbtest WHERE id = ?", id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO bench.sbtest (id,k,c,pad) VALUES (?,?,?,?)", id, rng.Intn(int(n)),
		fmt.Sprintf("%0120d", rng.Int63()), fmt.Sprintf("%060d", rng.Int63())); err != nil {
		return err
	}
	return tx.Commit()
}

func (l *load) latenciesBetween(a, b time.Time) []float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []float64
	for _, s := range l.lat {
		if !s.at.Before(a) && s.at.Before(b) {
			out = append(out, float64(s.us)/1000)
		}
	}
	return out
}

func percentile(v []float64, p float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	idx := int(math.Ceil(p*float64(len(s)))) - 1
	if idx < 0 {
		idx = 0
	}
	return s[idx]
}

// ---------------------------------------------------------------------------
// Lấy mẫu metrics của strand
// ---------------------------------------------------------------------------

type sample struct {
	T                        time.Time
	R, Zone, P, Breaker, Lag float64
	OK                       bool
}

type sampler struct {
	addr, pipeline string
	mu             sync.Mutex
	s              []sample
}

func newSampler(addr, pipeline string) *sampler { return &sampler{addr: addr, pipeline: pipeline} }

func (s *sampler) run(ctx context.Context, _ *load, _ time.Time) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	cl := &http.Client{Timeout: 900 * time.Millisecond}
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			sm := sample{T: now}
			if resp, err := cl.Get("http://" + s.addr + "/metrics"); err == nil {
				m := parseMetrics(resp.Body, s.pipeline)
				resp.Body.Close()
				sm.R, sm.OK = m[`strand_governor_rate_tokens|source_iops`]
				sm.Zone = m["strand_governor_zone|"]
				sm.P = m[`strand_governor_pressure|total`]
				if sm.P == 0 {
					sm.P = m[`strand_governor_pressure|replica_lag`]
				}
				sm.Breaker = m["strand_breaker_open|"]
				sm.Lag = m["strand_lag_seconds|"]
			}
			s.mu.Lock()
			s.s = append(s.s, sm)
			s.mu.Unlock()
		}
	}
}

func (s *sampler) snapshot() []sample {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]sample(nil), s.s...)
}

// parseMetrics đọc exposition text; khóa = "<tên>|<giá trị nhãn resource/signal/phase nếu có>", chỉ series
// có pipeline = id. strand_lag_seconds lấy series đầu tiên của pipeline (không phân biệt nhãn khác).
func parseMetrics(r io.Reader, pipeline string) map[string]float64 {
	out := map[string]float64{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	want := map[string]string{"strand_governor_rate_tokens": "resource", "strand_governor_zone": "",
		"strand_governor_pressure": "signal", "strand_breaker_open": "", "strand_lag_seconds": ""}
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.IndexAny(line, "{ ")
		if i < 0 {
			continue
		}
		name := line[:i]
		lbl, ok := want[name]
		if !ok {
			continue
		}
		labels := map[string]string{}
		rest := line[i:]
		if strings.HasPrefix(rest, "{") {
			j := strings.Index(rest, "}")
			if j < 0 {
				continue
			}
			for _, kv := range strings.Split(rest[1:j], ",") {
				k, v, ok := strings.Cut(kv, "=")
				if ok {
					labels[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"`)
				}
			}
			rest = rest[j+1:]
		}
		if labels["pipeline"] != pipeline {
			continue
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(strings.Fields(rest)[0]), 64)
		if err != nil {
			continue
		}
		key := name + "|"
		if lbl != "" {
			key += labels[lbl]
		}
		if _, dup := out[key]; !dup {
			out[key] = f
		}
	}
	return out
}

// p2pLogR: với mỗi nửa chu kỳ tải không đổi nằm trọn trong [snapStart, snapEnd], bỏ 60 s đầu, tính
// max(ln R) − min(ln R); trả trung vị, max, số cửa sổ dùng được và số cửa sổ có R = 0 (bị loại).
func p2pLogR(samples []sample, l *load, snapStart, snapEnd time.Time, o options) (median, maxV float64, n, zero int) {
	half := o.Period / 2
	if half <= 0 {
		return math.NaN(), math.NaN(), 0, 0
	}
	var vals []float64
	for k := 0; ; k++ {
		ws := l.t0.Add(time.Duration(k) * half)
		we := ws.Add(half)
		if ws.After(snapEnd) {
			break
		}
		if ws.Before(snapStart) || we.After(snapEnd) {
			continue
		}
		from := ws.Add(60 * time.Second)
		lo, hi := math.Inf(1), math.Inf(-1)
		hasZero, cnt := false, 0
		for _, s := range samples {
			if s.T.Before(from) || !s.T.Before(we) || !s.OK {
				continue
			}
			// Bỏ khoảng spike khỏi trạng thái xác lập.
			if d := s.T.Sub(l.t0); o.SpikeEvery > 0 && d >= o.SpikeEvery && d%o.SpikeEvery < o.SpikeDur+5*time.Second {
				continue
			}
			if s.R <= 0 {
				hasZero = true
				continue
			}
			v := math.Log(s.R)
			lo, hi = math.Min(lo, v), math.Max(hi, v)
			cnt++
		}
		if hasZero {
			zero++
			continue
		}
		if cnt >= 10 {
			vals = append(vals, hi-lo)
		}
	}
	if len(vals) == 0 {
		return math.NaN(), math.NaN(), 0, zero
	}
	sort.Float64s(vals)
	return vals[len(vals)/2], vals[len(vals)-1], len(vals), zero
}

func writeCSV(path string, samples []sample, l *load, snapStart, snapEnd time.Time) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	fmt.Fprintln(w, "t_s,phase,R,lnR,zone,pressure,breaker,lag_s,target_rate,p99_10s_ms")
	for _, s := range samples {
		phase := "snapshot"
		if s.T.After(snapEnd) {
			phase = "stream"
		}
		lnR := math.NaN()
		if s.R > 0 {
			lnR = math.Log(s.R)
		}
		p99 := percentile(l.latenciesBetween(s.T.Add(-10*time.Second), s.T), 0.99)
		fmt.Fprintf(w, "%.0f,%s,%.3f,%.4f,%.0f,%.4f,%.0f,%.3f,%.0f,%.2f\n", s.T.Sub(snapStart).Seconds(), phase,
			s.R, lnR, s.Zone, s.P, s.Breaker, s.Lag, l.rateAt(s.T.Sub(l.t0)), p99)
	}
}

// ---------------------------------------------------------------------------
// Quy tắc quyết định (giao thức §4)
// ---------------------------------------------------------------------------

func median(v []float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

func decide(runs []runResult) (map[string]govSummary, string) {
	sum := map[string]govSummary{}
	snaps, p99s := map[string][]float64{}, map[string][]float64{}
	for _, r := range runs {
		g := sum[r.Governor]
		g.Runs++
		g.HardLimitExceedTotal += r.HardLimitExceed
		if r.OK {
			g.OKRuns++
			snaps[r.Governor] = append(snaps[r.Governor], r.SnapshotSeconds)
			p99s[r.Governor] = append(p99s[r.Governor], r.AppP99Ms)
		}
		if !math.IsNaN(r.P2PLogRMedian) && r.P2PLogRMedian > g.MaxP2PMedian {
			g.MaxP2PMedian = r.P2PLogRMedian
		}
		sum[r.Governor] = g
	}
	for k, g := range sum {
		g.MedianSnapshotSeconds = median(snaps[k])
		g.MedianAppP99Ms = median(p99s[k])
		sum[k] = g
	}
	var why []string
	for k, g := range sum {
		if g.HardLimitExceedTotal > 0 {
			why = append(why, fmt.Sprintf("%s vượt hard limit %d mẫu", k, g.HardLimitExceedTotal))
		}
		if g.OKRuns < g.Runs {
			why = append(why, fmt.Sprintf("%s có %d/%d lần chạy lỗi", k, g.Runs-g.OKRuns, g.Runs))
		}
	}
	gr, on := sum["grace-v2"], sum["onoff"]
	if gr.Runs == 0 || on.Runs == 0 {
		return sum, "không đủ dữ liệu grace-v2/onoff để quyết định"
	}
	ok := len(why) == 0
	if gr.MaxP2PMedian > 0.3 {
		ok = false
		why = append(why, fmt.Sprintf("grace-v2 p2p log R %.3f > 0,3", gr.MaxP2PMedian))
	}
	gain := 1 - gr.MedianSnapshotSeconds/on.MedianSnapshotSeconds
	if !(gain >= 0.30) {
		ok = false
		why = append(why, fmt.Sprintf("snapshot grace-v2 nhanh hơn onoff %.1f%% (< 30%%)", gain*100))
	}
	if !(gr.MedianAppP99Ms <= on.MedianAppP99Ms) {
		ok = false
		why = append(why, fmt.Sprintf("p99 grace-v2 %.1f ms > onoff %.1f ms", gr.MedianAppP99Ms, on.MedianAppP99Ms))
	}
	if ok {
		return sum, fmt.Sprintf("grace-v2 đạt: làm mặc định (snapshot nhanh hơn onoff %.1f%%, p99 %.1f ≤ %.1f ms)", gain*100, gr.MedianAppP99Ms, on.MedianAppP99Ms)
	}
	return sum, "grace-v2 KHÔNG đạt → onoff mặc định, GRACE experimental: " + strings.Join(why, "; ")
}
