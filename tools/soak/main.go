// Công cụ soak test cho Strand CDC (spec/04 §2, §4):
// Chạy bộ sinh tải liên tục trên MySQL source, giám sát tiến trình `strand sync`,
// tiêm lỗi định kỳ (kill -9, SIGTERM, SIGSTOP/SIGCONT), lấy mẫu RSS / độ dài checkpoint / lag
// mỗi 60 giây, kiểm tra trạng thái cuối bằng checker và xuất báo cáo JSON.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"strand/internal/admin"
	"strand/internal/checkpoint"
	"strand/internal/config"
	"strand/internal/model"
	"strand/tests/checker"
)

// Các ngưỡng mặc định cho kết luận soak test (02 §2 CP-4, 04 §2).
const (
	DefaultMaxCheckpointBytes    = 65536
	DefaultMaxRSSGrowthMBPerHour = 5.0
)

// Danh sách bảng ứng dụng cần đối chiếu trạng thái cuối.
var appTables = []string{"app.accounts", "app.orders", "app.events"}

// schemaSQL là lược đồ kiểm thử giống tests/e2e/harness_test.go:
// PK số, unique key phụ, FK cha–con, cột ver = UUID_SHORT(), và bảng bị loại khỏi pipeline.
var schemaSQL = []string{
	"CREATE DATABASE IF NOT EXISTS app",
	`CREATE TABLE IF NOT EXISTS app.accounts (
		id BIGINT PRIMARY KEY,
		code VARCHAR(32) NOT NULL,
		ver BIGINT UNSIGNED NOT NULL,
		balance BIGINT NOT NULL,
		note VARCHAR(255),
		payload JSON,
		updated_at DATETIME(6),
		UNIQUE KEY uk_code (code))`,
	`CREATE TABLE IF NOT EXISTS app.orders (
		id BIGINT PRIMARY KEY,
		account_id BIGINT NOT NULL,
		ver BIGINT UNSIGNED NOT NULL,
		amount DECIMAL(18,2) NOT NULL,
		KEY (account_id),
		CONSTRAINT fk_acc FOREIGN KEY (account_id) REFERENCES app.accounts(id))`,
	`CREATE TABLE IF NOT EXISTS app.events (
		id BIGINT PRIMARY KEY,
		ver BIGINT UNSIGNED NOT NULL,
		body VARCHAR(64))`,
	`CREATE TABLE IF NOT EXISTS app.ignored (id BIGINT PRIMARY KEY, v BIGINT)`,
}

// FaultCounts thống kê số lỗi đã tiêm theo từng loại.
type FaultCounts struct {
	Kill9   int `json:"kill9"`
	SIGTERM int `json:"sigterm"`
	SIGSTOP int `json:"sigstop"`
}

// Sample lưu một mẫu đo định kỳ mỗi 60 giây.
type Sample struct {
	Timestamp               time.Time `json:"timestamp"`
	ElapsedSeconds          float64   `json:"elapsed_seconds"`
	RSSBytes                int64     `json:"rss_bytes"`
	RSSMB                   float64   `json:"rss_mb"`
	CheckpointBytes         int       `json:"checkpoint_bytes"`
	CheckpointAppliedBytes  int       `json:"checkpoint_applied_bytes"`
	WorkerDeltaAppliedBytes int       `json:"worker_delta_applied_bytes"`
	LagGTIDs                int64     `json:"lag_gtids"`
	Restarts                int       `json:"restarts"`
}

// Conclusion tóm tắt các chỉ số đánh giá điều kiện đạt của đợt soak test.
type Conclusion struct {
	RSSGrowth          float64 `json:"rss_growth"`
	CheckpointMaxBytes int     `json:"checkpoint_max_bytes"`
	FinalStateEqual    bool    `json:"final_state_equal"`
	ForbiddenExit      bool    `json:"forbidden_exit"`
	ForbiddenExitCode  int     `json:"forbidden_exit_code,omitempty"`
	Pass               bool    `json:"pass"`
}

// SoakReport là cấu trúc báo cáo JSON xuất ra cuối đợt soak test.
type SoakReport struct {
	Seed               int64       `json:"seed"`
	StartedAt          time.Time   `json:"started_at"`
	FinishedAt         time.Time   `json:"finished_at"`
	Duration           string      `json:"duration"`
	DurationSeconds    float64     `json:"duration_seconds"`
	ConfiguredDuration string      `json:"configured_duration"`
	PipelineID         string      `json:"pipeline_id"`
	FaultsInjected     FaultCounts `json:"faults_injected"`
	Restarts           int         `json:"restarts"`
	WorkloadOps        int64       `json:"workload_ops"`
	WorkloadErrs       int64       `json:"workload_errs"`
	Samples            []Sample    `json:"samples"`
	RSSGrowth          float64     `json:"rss_growth"`
	CheckpointMaxBytes int         `json:"checkpoint_max_bytes"`
	FinalStateEqual    bool        `json:"final_state_equal"`
	ForbiddenExit      bool        `json:"forbidden_exit"`
	ForbiddenExitCode  int         `json:"forbidden_exit_code,omitempty"`
	Pass               bool        `json:"pass"`
	Conclusion         Conclusion  `json:"conclusion"`
	Violations         []string    `json:"violations,omitempty"`
	ErrorReason        string      `json:"error_reason,omitempty"`
}

// PassInput chứa các đầu vào để quyết định đợt soak test đạt hay không.
type PassInput struct {
	FinalStateEqual       bool
	ForbiddenExit         bool
	CheckpointMaxBytes    int
	RSSGrowthMBPerHour    float64
	MaxCheckpointBytes    int
	MaxRSSGrowthMBPerHour float64
}

// isForbiddenExitCode kiểm tra mã thoát có nằm trong tập cấm tự động restart {2, 3, 4, 5, 10}
// theo spec/03 §4 (RestartPreventExitStatus=2 3 4 5 10) hay không.
// spec: 03 §4, 02 §10
func isForbiddenExitCode(code int) bool {
	switch code {
	case 2, 3, 4, 5, 10:
		return true
	default:
		return false
	}
}

// computeRSSGrowthMBPerHour tính tốc độ tăng RSS (MB/giờ) bằng hồi quy tuyến tính
// bình phương tối thiểu (OLS) trên nửa sau thời gian của chuỗi mẫu.
func computeRSSGrowthMBPerHour(samples []Sample) float64 {
	secondHalf := selectSecondHalfSamples(samples)
	if len(secondHalf) < 2 {
		return 0
	}

	baseTime := samples[0].Timestamp
	xs := make([]float64, len(secondHalf))
	ys := make([]float64, len(secondHalf))
	for i, s := range secondHalf {
		xs[i] = sampleHours(s, baseTime, i)
		ys[i] = sampleMB(s)
	}
	return linearRegressionSlope(xs, ys)
}

// selectSecondHalfSamples lọc các mẫu thuộc nửa sau khoảng thời gian quan sát.
func selectSecondHalfSamples(samples []Sample) []Sample {
	n := len(samples)
	if n < 2 {
		return nil
	}
	baseTime := samples[0].Timestamp
	t0 := sampleHours(samples[0], baseTime, 0)
	tLast := sampleHours(samples[n-1], baseTime, n-1)
	if tLast > t0 {
		mid := t0 + (tLast-t0)/2.0
		var filtered []Sample
		for i, s := range samples {
			if sampleHours(s, baseTime, i) >= mid-1e-12 {
				filtered = append(filtered, s)
			}
		}
		if len(filtered) >= 2 {
			return filtered
		}
	}
	return samples[n/2:]
}

func sampleHours(s Sample, baseTime time.Time, fallbackIdx int) float64 {
	if s.ElapsedSeconds > 0 {
		return s.ElapsedSeconds / 3600.0
	}
	if !s.Timestamp.IsZero() && !baseTime.IsZero() {
		return s.Timestamp.Sub(baseTime).Hours()
	}
	if s.ElapsedSeconds == 0 && (!s.Timestamp.IsZero() || fallbackIdx == 0) {
		return 0
	}
	return float64(fallbackIdx) / 60.0
}

func sampleMB(s Sample) float64 {
	if s.RSSMB != 0 {
		return s.RSSMB
	}
	return float64(s.RSSBytes) / (1024.0 * 1024.0)
}

// linearRegressionSlope tính hệ số góc b1 của đường thẳng y = b0 + b1*x theo OLS.
func linearRegressionSlope(xs, ys []float64) float64 {
	n := len(xs)
	if n < 2 || len(ys) != n {
		return 0
	}
	var sumX, sumY float64
	for i := 0; i < n; i++ {
		sumX += xs[i]
		sumY += ys[i]
	}
	meanX := sumX / float64(n)
	meanY := sumY / float64(n)

	var num, den float64
	for i := 0; i < n; i++ {
		dx := xs[i] - meanX
		dy := ys[i] - meanY
		num += dx * dy
		den += dx * dx
	}
	if den == 0 {
		return 0
	}
	return num / den
}

// computeCheckpointMaxBytes tìm độ dài checkpoint lớn nhất (byte) trong chuỗi mẫu.
func computeCheckpointMaxBytes(samples []Sample) int {
	maxBytes := 0
	for _, s := range samples {
		b := s.CheckpointBytes
		if b == 0 && (s.CheckpointAppliedBytes > 0 || s.WorkerDeltaAppliedBytes > 0) {
			b = s.CheckpointAppliedBytes + s.WorkerDeltaAppliedBytes
		}
		if b > maxBytes {
			maxBytes = b
		}
	}
	return maxBytes
}

// evaluatePass quyết định kết quả đạt (PASS) hay trượt (FAIL) của đợt soak test.
func evaluatePass(in PassInput) bool {
	maxCkpt := in.MaxCheckpointBytes
	if maxCkpt <= 0 {
		maxCkpt = DefaultMaxCheckpointBytes
	}
	maxRSS := in.MaxRSSGrowthMBPerHour
	if maxRSS <= 0 {
		maxRSS = DefaultMaxRSSGrowthMBPerHour
	}
	if !in.FinalStateEqual {
		return false
	}
	if in.ForbiddenExit {
		return false
	}
	if in.CheckpointMaxBytes > maxCkpt {
		return false
	}
	if math.IsNaN(in.RSSGrowthMBPerHour) || math.IsInf(in.RSSGrowthMBPerHour, 0) {
		return false
	}
	if in.RSSGrowthMBPerHour > maxRSS {
		return false
	}
	return true
}

// buildConclusion tổng hợp kết luận từ chuỗi mẫu và trạng thái kết thúc.
func buildConclusion(samples []Sample, finalStateEqual, forbiddenExit bool, forbiddenExitCode, maxCheckpointBytes int, maxRSSGrowthMBPerHour float64) Conclusion {
	rssGrowth := computeRSSGrowthMBPerHour(samples)
	ckptMax := computeCheckpointMaxBytes(samples)
	pass := evaluatePass(PassInput{
		FinalStateEqual:       finalStateEqual,
		ForbiddenExit:         forbiddenExit,
		CheckpointMaxBytes:    ckptMax,
		RSSGrowthMBPerHour:    rssGrowth,
		MaxCheckpointBytes:    maxCheckpointBytes,
		MaxRSSGrowthMBPerHour: maxRSSGrowthMBPerHour,
	})
	return Conclusion{
		RSSGrowth:          rssGrowth,
		CheckpointMaxBytes: ckptMax,
		FinalStateEqual:    finalStateEqual,
		ForbiddenExit:      forbiddenExit,
		ForbiddenExitCode:  forbiddenExitCode,
		Pass:               pass,
	}
}

// ---------------------------------------------------------------------------
// Đọc VmRSS từ /proc/<pid>/status và đếm GTID thiếu
// ---------------------------------------------------------------------------

// readProcessRSSBytes đọc VmRSS (tính bằng byte) của tiến trình từ /proc/<pid>/status.
func readProcessRSSBytes(pid int) (int64, error) {
	if pid <= 0 {
		return 0, fmt.Errorf("invalid pid %d", pid)
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	return parseVmRSSBytes(string(raw))
}

// parseVmRSSBytes phân tích trường VmRSS từ nội dung tệp /proc/<pid>/status.
func parseVmRSSBytes(statusContent string) (int64, error) {
	for _, line := range strings.Split(statusContent, "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "VmRSS:"))
		if len(fields) == 0 {
			return 0, fmt.Errorf("malformed VmRSS line: %q", line)
		}
		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse VmRSS value %q: %w", fields[0], err)
		}
		return kb * 1024, nil
	}
	return 0, fmt.Errorf("VmRSS not found in status")
}

// countGTIDSet đếm tổng số GTID trong một MySQLAppliedSet.
func countGTIDSet(set *checkpoint.MySQLAppliedSet) int64 {
	if set == nil {
		return 0
	}
	s := strings.TrimSpace(set.String())
	if s == "" {
		return 0
	}
	var total int64
	for _, seg := range strings.Split(s, ",") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		parts := strings.Split(seg, ":")
		if len(parts) < 2 {
			continue
		}
		// Phần tử đầu là UUID; các phần tử sau là tag hoặc khoảng số "n" / "a-b".
		for _, tok := range parts[1:] {
			tok = strings.TrimSpace(tok)
			if tok == "" || tok[0] < '0' || tok[0] > '9' {
				continue
			}
			aStr, bStr, isRange := strings.Cut(tok, "-")
			start, err := strconv.ParseUint(strings.TrimSpace(aStr), 10, 64)
			if err != nil || start == 0 {
				continue
			}
			end := start
			if isRange {
				end, err = strconv.ParseUint(strings.TrimSpace(bStr), 10, 64)
				if err != nil || end < start {
					continue
				}
			}
			total += int64(end - start + 1)
		}
	}
	return total
}

// measureLagGTIDs tính số GTID có trong gtid_executed của source nhưng chưa có trong tập đã apply ở target.
func measureLagGTIDs(ctx context.Context, srcDB, dstDB *sql.DB, pipelineID string) (int64, error) {
	var executed string
	if err := srcDB.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		return 0, fmt.Errorf("query source gtid_executed: %w", err)
	}
	want, err := checkpoint.ParseMySQLGTIDSet(executed)
	if err != nil {
		return 0, fmt.Errorf("parse source gtid_executed: %w", err)
	}
	gotSet, err := checkpoint.LoadAppliedSnapshotMySQL(ctx, dstDB, pipelineID, model.EngineMySQL)
	if err != nil {
		return 0, fmt.Errorf("load applied snapshot: %w", err)
	}
	got, ok := gotSet.(*checkpoint.MySQLAppliedSet)
	if !ok {
		return 0, fmt.Errorf("unexpected applied set type %T", gotSet)
	}
	miss := want.Clone().(*checkpoint.MySQLAppliedSet)
	miss.Subtract(got)
	return countGTIDSet(miss), nil
}

// measureCheckpointBytes đọc độ dài cột applied của _strand_checkpoint và tổng độ dài _strand_worker_delta.applied.
func measureCheckpointBytes(ctx context.Context, dstDB *sql.DB, pipelineID string) (ckptLen int, deltaLen int, err error) {
	err = dstDB.QueryRowContext(ctx,
		"SELECT COALESCE(OCTET_LENGTH(applied), 0) FROM _strand_checkpoint WHERE pipeline_id = ?",
		pipelineID,
	).Scan(&ckptLen)
	if errors.Is(err, sql.ErrNoRows) {
		ckptLen = 0
		err = nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("query _strand_checkpoint length: %w", err)
	}

	err = dstDB.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(OCTET_LENGTH(applied)), 0) FROM _strand_worker_delta WHERE pipeline_id = ?",
		pipelineID,
	).Scan(&deltaLen)
	if errors.Is(err, sql.ErrNoRows) {
		deltaLen = 0
		err = nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("query _strand_worker_delta length: %w", err)
	}
	return ckptLen, deltaLen, nil
}

// ---------------------------------------------------------------------------
// Bộ sinh tải liên tục trên source (04 §3)
// ---------------------------------------------------------------------------

type workload struct {
	db      *sql.DB
	rng     *rand.Rand
	mu      sync.Mutex
	nextID  int64
	ops     atomic.Int64
	errs    atomic.Int64
	stopped atomic.Bool
	wg      sync.WaitGroup
}

func newWorkload(db *sql.DB, seed int64) *workload {
	return &workload{
		db:     db,
		rng:    rand.New(rand.NewSource(seed)),
		nextID: 1,
	}
}

func (w *workload) id() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextID++
	return w.nextID
}

func (w *workload) pickHot(r *rand.Rand, n int64) int64 {
	if n <= 0 {
		n = 100
	}
	// ~Zipf: 50% truy cập dồn vào 10 khóa đầu.
	if r.Intn(2) == 0 {
		return 1 + r.Int63n(10)
	}
	return 1 + r.Int63n(n)
}

// InitAndPreload khởi tạo nextID từ dữ liệu sẵn có và nạp dữ liệu ban đầu nếu bảng đang trống.
func (w *workload) InitAndPreload(ctx context.Context, n int) error {
	var maxID int64
	for _, tb := range []string{"app.accounts", "app.orders", "app.events"} {
		var m int64
		if err := w.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(MAX(id), 0) FROM %s", tb)).Scan(&m); err != nil {
			return fmt.Errorf("read max id from %s: %w", tb, err)
		}
		if m > maxID {
			maxID = m
		}
	}
	if maxID > 0 {
		w.mu.Lock()
		w.nextID = maxID + 1000
		w.mu.Unlock()
		return nil
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i := 1; i <= n; i++ {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO app.accounts VALUES (?, ?, UUID_SHORT(), ?, ?, ?, NOW(6))",
			i, fmt.Sprintf("C%08d", i), i*10, fmt.Sprintf("note %d", i), fmt.Sprintf(`{"k": %d, "s": "x"}`, i),
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO app.orders VALUES (?, ?, UUID_SHORT(), ?)",
			i, i, float64(i)+0.25,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO app.events VALUES (?, UUID_SHORT(), 'e')",
			i,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO app.ignored VALUES (?, 1)",
			i,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	w.mu.Lock()
	if int64(n) >= w.nextID {
		w.nextID = int64(n) + 1000
	}
	w.mu.Unlock()
	return nil
}

func (w *workload) Start(clients int, span int64) {
	if clients <= 0 {
		clients = 1
	}
	if span <= 0 {
		span = 100000
	}
	for c := 0; c < clients; c++ {
		w.wg.Add(1)
		w.mu.Lock()
		clientSeed := w.rng.Int63()
		w.mu.Unlock()
		r := rand.New(rand.NewSource(clientSeed))
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

func (w *workload) Stop() {
	w.stopped.Store(true)
	w.wg.Wait()
}

func (w *workload) one(r *rand.Rand, span int64) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	switch k := r.Intn(100); {
	case k < 40: // cập nhật khóa nóng account + order
		id := w.pickHot(r, span)
		if _, err := tx.Exec(
			"UPDATE app.accounts SET ver = UUID_SHORT(), balance = balance + ?, updated_at = NOW(6), payload = JSON_SET(COALESCE(payload, '{}'), '$.n', ver) WHERE id = ?",
			r.Intn(100), id,
		); err != nil {
			return err
		}
		if _, err := tx.Exec(
			"UPDATE app.orders SET ver = UUID_SHORT(), amount = amount + 1.5 WHERE account_id = ?",
			id,
		); err != nil {
			return err
		}
	case k < 55: // chèn cha + con (FK cha trước con)
		id := w.id()
		if _, err := tx.Exec(
			"INSERT INTO app.accounts VALUES (?, ?, UUID_SHORT(), 0, NULL, NULL, NOW(6))",
			id, fmt.Sprintf("N%010d", id),
		); err != nil {
			return err
		}
		for j := 0; j < 1+r.Intn(3); j++ {
			if _, err := tx.Exec(
				"INSERT INTO app.orders VALUES (?, ?, UUID_SHORT(), ?)",
				w.id(), id, r.Intn(1000),
			); err != nil {
				return err
			}
		}
	case k < 65: // xóa account (xóa con trước)
		id := 1 + r.Int63n(span*2)
		if _, err := tx.Exec("DELETE FROM app.orders WHERE account_id = ?", id); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM app.accounts WHERE id = ?", id); err != nil {
			return err
		}
	case k < 72: // đổi PK của app.events
		old := 1 + r.Int63n(span)
		if _, err := tx.Exec("UPDATE app.events SET id = ?, ver = UUID_SHORT() WHERE id = ?", w.id(), old); err != nil {
			return err
		}
	case k < 78: // đổi chéo unique key giữa hai account
		a, b := w.pickHot(r, span), 1+r.Int63n(span)
		if a == b {
			return nil
		}
		var ca, cb string
		if err := tx.QueryRow("SELECT code FROM app.accounts WHERE id = ? FOR UPDATE", a).Scan(&ca); err != nil {
			return nil
		}
		if err := tx.QueryRow("SELECT code FROM app.accounts WHERE id = ? FOR UPDATE", b).Scan(&cb); err != nil {
			return nil
		}
		if _, err := tx.Exec("UPDATE app.accounts SET code = ?, ver = UUID_SHORT() WHERE id = ?", "tmp-"+ca, a); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE app.accounts SET code = ?, ver = UUID_SHORT() WHERE id = ?", ca, b); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE app.accounts SET code = ?, ver = UUID_SHORT() WHERE id = ?", cb, a); err != nil {
			return err
		}
	case k < 85: // bảng app.ignored bị loại khỏi pipeline
		if _, err := tx.Exec("INSERT INTO app.ignored VALUES (?, 1) ON DUPLICATE KEY UPDATE v = v + 1", 1+r.Int63n(span)); err != nil {
			return err
		}
	case k < 88: // Txn nhiều row
		base := w.id() * 1000
		for j := int64(0); j < 200; j++ {
			if _, err := tx.Exec("INSERT INTO app.events VALUES (?, UUID_SHORT(), 'bulk')", base+j); err != nil {
				return err
			}
		}
	default: // chèn/cập nhật app.events
		id := w.pickHot(r, span)
		if _, err := tx.Exec("INSERT INTO app.events VALUES (?, UUID_SHORT(), 'x') ON DUPLICATE KEY UPDATE ver = UUID_SHORT(), body = 'y'", id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Supervisor quản lý tiến trình con `strand sync -c <config>`
// ---------------------------------------------------------------------------

type runningProc struct {
	cmd            *exec.Cmd
	pid            int
	done           chan struct{}
	exitCode       int
	expectCleanSig atomic.Bool
	frozen         atomic.Bool
	tail           *ringBuffer
}

type ringBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func newRingBuffer(max int) *ringBuffer {
	return &ringBuffer{max: max}
}

func (r *ringBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.buf = append(r.buf, p...)
	if len(r.buf) > r.max {
		r.buf = append([]byte(nil), r.buf[len(r.buf)-r.max:]...)
	}
	return len(p), nil
}

func (r *ringBuffer) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.buf)
}

type supervisor struct {
	strandBin  string
	configPath string

	mu                sync.Mutex
	cur               *runningProc
	stopping          bool
	restarts          int
	forbiddenExit     bool
	forbiddenExitCode int
	failReason        string

	fatalCh   chan struct{}
	fatalOnce sync.Once
	doneCh    chan struct{}
}

func newSupervisor(strandBin, configPath string) *supervisor {
	return &supervisor{
		strandBin:  strandBin,
		configPath: configPath,
		fatalCh:    make(chan struct{}),
		doneCh:     make(chan struct{}),
	}
}

func (s *supervisor) markFatal(code int, reason string) {
	s.mu.Lock()
	if !s.forbiddenExit {
		s.forbiddenExit = true
		s.forbiddenExitCode = code
		s.failReason = reason
	}
	s.mu.Unlock()
	s.fatalOnce.Do(func() {
		close(s.fatalCh)
	})
}

func (s *supervisor) spawn() (*runningProc, error) {
	cmd := exec.Command(s.strandBin, "sync", "-c", s.configPath)
	rb := newRingBuffer(8192)
	cmd.Stdout = rb
	cmd.Stderr = rb
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s sync -c %s: %w", s.strandBin, s.configPath, err)
	}
	p := &runningProc{
		cmd:      cmd,
		pid:      cmd.Process.Pid,
		done:     make(chan struct{}),
		exitCode: -1,
		tail:     rb,
	}
	go func() {
		_ = cmd.Wait()
		if cmd.ProcessState != nil {
			p.exitCode = cmd.ProcessState.ExitCode()
		}
		close(p.done)
	}()
	return p, nil
}

// Run chạy vòng lặp giám sát và tự động khởi động lại khi mã thoát ∉ {2, 3, 4, 5, 10}.
func (s *supervisor) Run(ctx context.Context) {
	defer close(s.doneCh)
	first := true
	for {
		s.mu.Lock()
		if s.stopping || s.forbiddenExit {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		if !first {
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}

		s.mu.Lock()
		if s.stopping || s.forbiddenExit {
			s.mu.Unlock()
			return
		}
		p, err := s.spawn()
		if err != nil {
			s.mu.Unlock()
			s.markFatal(1, err.Error())
			return
		}
		if !first {
			s.restarts++
		}
		first = false
		s.cur = p
		s.mu.Unlock()

		// Chờ tiến trình con kết thúc.
		<-p.done

		s.mu.Lock()
		stopping := s.stopping
		code := p.exitCode
		expectZero := p.expectCleanSig.Load()
		s.mu.Unlock()

		if stopping {
			return
		}
		if isForbiddenExitCode(code) {
			s.markFatal(code, fmt.Sprintf("strand exited with forbidden restart code %d\n%s", code, p.tail.String()))
			return
		}
		if expectZero && code != 0 {
			s.markFatal(code, fmt.Sprintf("strand exited with code %d after SIGTERM (expected 0)\n%s", code, p.tail.String()))
			return
		}
	}
}

func (s *supervisor) currentProc() *runningProc {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur == nil {
		return nil
	}
	select {
	case <-s.cur.done:
		return nil
	default:
		return s.cur
	}
}

func (s *supervisor) waitRunningProc(ctx context.Context, timeout time.Duration) *runningProc {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p := s.currentProc(); p != nil {
			return p
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.fatalCh:
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil
}

func (s *supervisor) stats() (restarts int, forbidden bool, code int, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restarts, s.forbiddenExit, s.forbiddenExitCode, s.failReason
}

// StopGracefully dừng giám sát và gửi SIGTERM cho tiến trình strand đang chạy.
func (s *supervisor) StopGracefully(timeout time.Duration) error {
	s.mu.Lock()
	s.stopping = true
	p := s.cur
	s.mu.Unlock()

	if p != nil {
		if p.frozen.Load() {
			_ = p.cmd.Process.Signal(syscall.SIGCONT)
			p.frozen.Store(false)
		}
		select {
		case <-p.done:
		default:
			p.expectCleanSig.Store(true)
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-p.done:
				if p.exitCode != 0 {
					return fmt.Errorf("strand exited with code %d on final SIGTERM\n%s", p.exitCode, p.tail.String())
				}
			case <-time.After(timeout):
				_ = p.cmd.Process.Signal(syscall.SIGKILL)
				<-p.done
				return fmt.Errorf("strand did not stop within %s after final SIGTERM\n%s", timeout, p.tail.String())
			}
		}
	}
	<-s.doneCh
	return nil
}

// injectFault chọn ngẫu nhiên 1 trong 3 lỗi: kill -9, SIGTERM (kiểm exit 0), hoặc SIGSTOP 30–120s rồi SIGCONT.
func injectFault(ctx context.Context, sup *supervisor, rng *rand.Rand, counts *FaultCounts, countsMu *sync.Mutex) error {
	p := sup.waitRunningProc(ctx, 30*time.Second)
	if p == nil {
		return nil
	}

	switch choice := rng.Intn(3); choice {
	case 0: // kill -9
		countsMu.Lock()
		counts.Kill9++
		countsMu.Unlock()
		_ = p.cmd.Process.Signal(syscall.SIGKILL)
		select {
		case <-p.done:
		case <-ctx.Done():
		}
		return nil

	case 1: // SIGTERM (dừng an toàn, kiểm exit 0)
		countsMu.Lock()
		counts.SIGTERM++
		countsMu.Unlock()
		p.expectCleanSig.Store(true)
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
			if p.exitCode != 0 {
				err := fmt.Errorf("SIGTERM fault check failed: strand exited with code %d (expected 0)", p.exitCode)
				sup.markFatal(p.exitCode, err.Error())
				return err
			}
		case <-time.After(120 * time.Second):
			_ = p.cmd.Process.Signal(syscall.SIGKILL)
			<-p.done
			err := fmt.Errorf("SIGTERM fault check failed: strand did not exit within 120s")
			sup.markFatal(-1, err.Error())
			return err
		case <-ctx.Done():
		}
		return nil

	default: // SIGSTOP 30–120 s rồi SIGCONT
		countsMu.Lock()
		counts.SIGSTOP++
		countsMu.Unlock()
		freezeDur := 30*time.Second + time.Duration(rng.Int63n(int64(90*time.Second)+1))
		p.frozen.Store(true)
		_ = p.cmd.Process.Signal(syscall.SIGSTOP)
		defer func() {
			_ = p.cmd.Process.Signal(syscall.SIGCONT)
			p.frozen.Store(false)
		}()
		timer := time.NewTimer(freezeDur)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		case <-sup.fatalCh:
		}
		return nil
	}
}

// ---------------------------------------------------------------------------
// Chờ target theo kịp và đối chiếu trạng thái cuối
// ---------------------------------------------------------------------------

func waitCaughtUp(ctx context.Context, srcDB, dstDB *sql.DB, pipelineID, adminSocket string, sup *supervisor, timeout time.Duration) error {
	var executed string
	if err := srcDB.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&executed); err != nil {
		return fmt.Errorf("read source gtid_executed: %w", err)
	}
	want, err := checkpoint.ParseMySQLGTIDSet(executed)
	if err != nil {
		return fmt.Errorf("parse source gtid_executed: %w", err)
	}

	deadline := time.Now().Add(timeout)
	for {
		_, forbidden, code, reason := sup.stats()
		if forbidden {
			return fmt.Errorf("strand exited with forbidden code %d while catching up: %s", code, reason)
		}
		select {
		case <-sup.doneCh:
			return fmt.Errorf("supervisor stopped; strand is not running during final catch-up")
		default:
		}
		if p := sup.currentProc(); p != nil && p.frozen.Load() {
			_ = p.cmd.Process.Signal(syscall.SIGCONT)
			p.frozen.Store(false)
		}

		gotSet, err := checkpoint.LoadAppliedSnapshotMySQL(ctx, dstDB, pipelineID, model.EngineMySQL)
		if err == nil {
			miss := want.Clone().(*checkpoint.MySQLAppliedSet)
			miss.Subtract(gotSet.(*checkpoint.MySQLAppliedSet))
			if miss.String() == "" && isPipelineSyncedIfSocketPresent(adminSocket) {
				return nil
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("target not caught up after %s; missing GTIDs: %s", timeout, miss.String())
			}
		} else if time.Now().After(deadline) {
			return fmt.Errorf("target not caught up after %s (last error: %v)", timeout, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sup.fatalCh:
			_, _, code, reason := sup.stats()
			return fmt.Errorf("strand failed (code %d) before catching up: %s", code, reason)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func isPipelineSyncedIfSocketPresent(socket string) bool {
	if socket == "" {
		return true
	}
	if _, err := os.Stat(socket); err != nil {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	top, err := admin.NewClient(socket).Top(ctx)
	if err != nil {
		return false
	}
	return top.State == "synced"
}

// dumpTable đọc toàn bộ các dòng của bảng theo thứ tự PK giống tests/e2e/harness_test.go.
func dumpTable(ctx context.Context, db *sql.DB, table string) (map[string]checker.RowState, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY 1", table))
	if err != nil {
		return nil, fmt.Errorf("dump %s: %w", table, err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make(map[string]checker.RowState)
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]interface{}, len(cols))
		var ver int64
		for i, c := range cols {
			if vals[i] == nil {
				m[c] = nil
			} else {
				m[c] = string(vals[i])
			}
			if c == "ver" {
				ver, _ = strconv.ParseInt(string(vals[i]), 10, 64)
			}
		}
		pk := string(vals[0])
		out[pk] = checker.RowState{Table: table, PK: pk, Ver: ver, Values: m}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func compareFinalState(ctx context.Context, srcDB, dstDB *sql.DB, tables []string) (bool, []string, error) {
	srcState := make(map[string]map[string]checker.RowState, len(tables))
	dstState := make(map[string]map[string]checker.RowState, len(tables))
	for _, tb := range tables {
		sRows, err := dumpTable(ctx, srcDB, tb)
		if err != nil {
			return false, nil, err
		}
		dRows, err := dumpTable(ctx, dstDB, tb)
		if err != nil {
			return false, nil, err
		}
		srcState[tb] = sRows
		dstState[tb] = dRows
	}
	res := checker.NewChecker().CheckFinalState(srcState, dstState)
	violations := res.Violations
	if len(violations) > 20 {
		violations = violations[:20]
	}
	return res.Passed, violations, nil
}

// ---------------------------------------------------------------------------
// Khởi tạo kết nối MySQL và tạo schema
// ---------------------------------------------------------------------------

func openMySQL(ctx context.Context, dsn string, createDBIfMissing bool) (*sql.DB, error) {
	parsed, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}
	if createDBIfMissing && parsed.DBName != "" {
		dbName := parsed.DBName
		baseCfg := *parsed
		baseCfg.DBName = ""
		if tmpDB, err := sql.Open("mysql", baseCfg.FormatDSN()); err == nil {
			_, _ = tmpDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", strings.ReplaceAll(dbName, "`", "``")))
			_ = tmpDB.Close()
		}
	}
	if parsed.Params == nil {
		parsed.Params = make(map[string]string)
	}
	if _, ok := parsed.Params["parseTime"]; !ok {
		parsed.Params["parseTime"] = "false"
	}
	db, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(64)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func ensureAppSchema(ctx context.Context, db *sql.DB) error {
	for _, q := range schemaSQL {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("exec %q: %w", q, err)
		}
	}
	return nil
}

func writeReportFile(path string, rep *SoakReport) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

// ---------------------------------------------------------------------------
// Hàm main
// ---------------------------------------------------------------------------

func main() {
	os.Exit(runMain(os.Args[1:], os.Stdout, os.Stderr))
}

func runMain(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("soak", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		strandBin             string
		configPath            string
		sourceDSN             string
		targetDSN             string
		duration              time.Duration
		faultEvery            time.Duration
		workloadClients       int
		span                  int64
		reportPath            string
		seedFlag              int64
		maxCheckpointBytes    int
		maxRSSGrowthMBPerHour float64
		sampleEvery           time.Duration
		progressEvery         time.Duration
		catchupTimeout        time.Duration
	)

	fs.StringVar(&strandBin, "strand", "strand", "Đường dẫn binary strand")
	fs.StringVar(&configPath, "config", "", "Đường dẫn tệp cấu hình pipeline.yaml")
	fs.StringVar(&sourceDSN, "source", "", "DSN của MySQL source")
	fs.StringVar(&targetDSN, "target", "", "DSN của MySQL target (kèm schema metadata)")
	fs.DurationVar(&duration, "duration", 72*time.Hour, "Thời lượng chạy soak test")
	fs.DurationVar(&faultEvery, "fault-every", 10*time.Minute, "Chu kỳ tiêm lỗi định kỳ")
	fs.IntVar(&workloadClients, "workload-clients", 8, "Số client sinh tải đồng thời")
	fs.Int64Var(&span, "span", 100000, "Phạm vi khóa cho bộ sinh tải")
	fs.StringVar(&reportPath, "report", "soak-report.json", "Đường dẫn xuất báo cáo JSON")
	fs.Int64Var(&seedFlag, "seed", 0, "Seed ngẫu nhiên (mặc định: thời gian hiện tại)")
	fs.IntVar(&maxCheckpointBytes, "checkpoint-max-bytes", DefaultMaxCheckpointBytes, "Ngưỡng kích thước checkpoint tối đa (byte)")
	fs.IntVar(&maxCheckpointBytes, "max-checkpoint-bytes", DefaultMaxCheckpointBytes, "Ngưỡng kích thước checkpoint tối đa (byte)")
	fs.Float64Var(&maxRSSGrowthMBPerHour, "rss-growth-max", DefaultMaxRSSGrowthMBPerHour, "Ngưỡng tốc độ tăng RSS tối đa (MB/giờ)")
	fs.Float64Var(&maxRSSGrowthMBPerHour, "max-rss-growth", DefaultMaxRSSGrowthMBPerHour, "Ngưỡng tốc độ tăng RSS tối đa (MB/giờ)")
	fs.DurationVar(&sampleEvery, "sample-every", 60*time.Second, "Chu kỳ ghi mẫu")
	fs.DurationVar(&progressEvery, "progress-every", 10*time.Minute, "Chu kỳ in tiến độ ra stdout")
	fs.DurationVar(&catchupTimeout, "catchup-timeout", 30*time.Minute, "Thời gian chờ tối đa để target theo kịp source")

	if err := fs.Parse(args); err != nil {
		return 2
	}
	if configPath == "" {
		fmt.Fprintln(stderr, "thiếu cờ bắt buộc: --config <pipeline.yaml>")
		return 2
	}

	cfg, err := config.LoadFile(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "không đọc được cấu hình %s: %v\n", configPath, err)
		return 2
	}
	if sourceDSN == "" {
		sourceDSN = cfg.Source.DSN
	}
	if targetDSN == "" {
		targetDSN = cfg.Target.DSN
	}
	if sourceDSN == "" || targetDSN == "" {
		fmt.Fprintln(stderr, "thiếu --source hoặc --target DSN")
		return 2
	}

	seed := seedFlag
	if seed == 0 {
		if envSeed := os.Getenv("SEED"); envSeed != "" {
			if v, err := strconv.ParseInt(envSeed, 10, 64); err == nil {
				seed = v
			}
		}
		if seed == 0 {
			seed = time.Now().UnixNano()
		}
	}
	fmt.Fprintf(stdout, "SOAK SEED=%d (dùng --seed %d để chạy lại đúng kịch bản)\n", seed, seed)

	rootCtx, stopSig := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSig()

	srcDB, err := openMySQL(rootCtx, sourceDSN, true)
	if err != nil {
		fmt.Fprintf(stderr, "kết nối source thất bại: %v\n", err)
		return 1
	}
	defer srcDB.Close()

	dstDB, err := openMySQL(rootCtx, targetDSN, true)
	if err != nil {
		fmt.Fprintf(stderr, "kết nối target thất bại: %v\n", err)
		return 1
	}
	defer dstDB.Close()

	// Tạo schema app trên cả source và target nếu chưa có.
	if err := ensureAppSchema(rootCtx, srcDB); err != nil {
		fmt.Fprintf(stderr, "tạo schema trên source thất bại: %v\n", err)
		return 1
	}
	if err := ensureAppSchema(rootCtx, dstDB); err != nil {
		fmt.Fprintf(stderr, "tạo schema trên target thất bại: %v\n", err)
		return 1
	}

	wl := newWorkload(srcDB, seed)
	preloadCount := 1000
	if span > 0 && int(span) < preloadCount {
		preloadCount = int(span)
	}
	if err := wl.InitAndPreload(rootCtx, preloadCount); err != nil {
		fmt.Fprintf(stderr, "khởi tạo dữ liệu ban đầu trên source thất bại: %v\n", err)
		return 1
	}

	startTime := time.Now()
	soakCtx, cancelSoak := context.WithTimeout(rootCtx, duration)
	defer cancelSoak()

	// 1. Bắt đầu sinh tải liên tục trên source.
	wl.Start(workloadClients, span)

	// 2. Chạy supervisor quản lý `strand sync -c <config>`.
	sup := newSupervisor(strandBin, configPath)
	// Supervisor sống theo rootCtx, không theo soakCtx: nếu lỗi tiêm cuối cùng giết strand ngay trước khi
	// hết thời lượng, strand vẫn phải được khởi động lại để bắt kịp ở pha kiểm tra cuối (dừng bằng StopGracefully).
	go sup.Run(rootCtx)

	var (
		samplesMu sync.Mutex
		samples   []Sample
		countsMu  sync.Mutex
		faults    FaultCounts
		bgWG      sync.WaitGroup
	)

	collectSample := func() Sample {
		now := time.Now()
		var rssBytes int64
		if p := sup.currentProc(); p != nil {
			if b, err := readProcessRSSBytes(p.pid); err == nil {
				rssBytes = b
			}
		}
		sCtx, sCancel := context.WithTimeout(rootCtx, 10*time.Second)
		defer sCancel()
		ckptLen, deltaLen, _ := measureCheckpointBytes(sCtx, dstDB, cfg.Name)
		lagGTIDs, _ := measureLagGTIDs(sCtx, srcDB, dstDB, cfg.Name)
		restarts, _, _, _ := sup.stats()

		s := Sample{
			Timestamp:               now,
			ElapsedSeconds:          now.Sub(startTime).Seconds(),
			RSSBytes:                rssBytes,
			RSSMB:                   float64(rssBytes) / (1024.0 * 1024.0),
			CheckpointBytes:         ckptLen + deltaLen,
			CheckpointAppliedBytes:  ckptLen,
			WorkerDeltaAppliedBytes: deltaLen,
			LagGTIDs:                lagGTIDs,
			Restarts:                restarts,
		}
		samplesMu.Lock()
		samples = append(samples, s)
		samplesMu.Unlock()
		return s
	}

	// 3. Goroutine tiêm lỗi định kỳ mỗi --fault-every.
	if faultEvery > 0 {
		bgWG.Add(1)
		faultRNG := rand.New(rand.NewSource(seed ^ 0x5a5a5a5a))
		go func() {
			defer bgWG.Done()
			ticker := time.NewTicker(faultEvery)
			defer ticker.Stop()
			for {
				select {
				case <-soakCtx.Done():
					return
				case <-sup.fatalCh:
					cancelSoak()
					return
				case <-ticker.C:
					_ = injectFault(soakCtx, sup, faultRNG, &faults, &countsMu)
				}
			}
		}()
	}

	// 4. Goroutine lấy mẫu mỗi 60 giây.
	if sampleEvery > 0 {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			ticker := time.NewTicker(sampleEvery)
			defer ticker.Stop()
			for {
				select {
				case <-soakCtx.Done():
					return
				case <-sup.fatalCh:
					return
				case <-ticker.C:
					collectSample()
				}
			}
		}()
	}

	// 7. Goroutine in tiến độ ngắn mỗi 10 phút ra stdout.
	if progressEvery > 0 {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			ticker := time.NewTicker(progressEvery)
			defer ticker.Stop()
			for {
				select {
				case <-soakCtx.Done():
					return
				case <-sup.fatalCh:
					return
				case <-ticker.C:
					samplesMu.Lock()
					var last Sample
					if len(samples) > 0 {
						last = samples[len(samples)-1]
					}
					samplesMu.Unlock()
					countsMu.Lock()
					fc := faults
					countsMu.Unlock()
					restarts, _, _, _ := sup.stats()
					fmt.Fprintf(stdout,
						"[progress %s] ops=%d errs=%d rss=%.2fMB ckpt=%dB lag_gtids=%d restarts=%d faults(kill9=%d,sigterm=%d,sigstop=%d)\n",
						time.Since(startTime).Round(time.Second),
						wl.ops.Load(), wl.errs.Load(),
						last.RSSMB, last.CheckpointBytes, last.LagGTIDs,
						restarts, fc.Kill9, fc.SIGTERM, fc.SIGSTOP,
					)
				}
			}
		}()
	}

	// Chờ hết --duration hoặc khi gặp mã cấm restart.
	select {
	case <-soakCtx.Done():
	case <-sup.fatalCh:
		cancelSoak()
	}

	// 5. Dừng tải và các vòng lặp định kỳ.
	wl.Stop()
	cancelSoak()
	bgWG.Wait()

	// Thu thập thêm một mẫu cuối trước khi dừng strand.
	collectSample()

	restarts, forbiddenExit, forbiddenCode, errReason := sup.stats()
	var (
		finalStateEqual bool
		violations      []string
	)

	if !forbiddenExit {
		// Đảm bảo strand đang chạy để bắt kịp toàn bộ GTID trên source.
		_ = sup.waitRunningProc(rootCtx, 15*time.Second)
		if err := waitCaughtUp(rootCtx, srcDB, dstDB, cfg.Name, cfg.Observability.AdminSocket, sup, catchupTimeout); err != nil {
			errReason = err.Error()
		} else if err := sup.StopGracefully(120 * time.Second); err != nil {
			errReason = err.Error()
		} else {
			eq, v, err := compareFinalState(rootCtx, srcDB, dstDB, appTables)
			if err != nil {
				errReason = err.Error()
			} else {
				finalStateEqual = eq
				violations = v
			}
		}
	} else {
		_ = sup.StopGracefully(30 * time.Second)
	}

	restarts, forbiddenExit, forbiddenCode, supReason := sup.stats()
	if errReason == "" && supReason != "" {
		errReason = supReason
	}

	finishTime := time.Now()
	samplesMu.Lock()
	collectedSamples := append([]Sample(nil), samples...)
	samplesMu.Unlock()

	countsMu.Lock()
	finalFaults := faults
	countsMu.Unlock()

	conclusion := buildConclusion(
		collectedSamples,
		finalStateEqual && errReason == "",
		forbiddenExit,
		forbiddenCode,
		maxCheckpointBytes,
		maxRSSGrowthMBPerHour,
	)

	rep := &SoakReport{
		Seed:               seed,
		StartedAt:          startTime,
		FinishedAt:         finishTime,
		Duration:           finishTime.Sub(startTime).Round(time.Second).String(),
		DurationSeconds:    finishTime.Sub(startTime).Seconds(),
		ConfiguredDuration: duration.String(),
		PipelineID:         cfg.Name,
		FaultsInjected:     finalFaults,
		Restarts:           restarts,
		WorkloadOps:        wl.ops.Load(),
		WorkloadErrs:       wl.errs.Load(),
		Samples:            collectedSamples,
		RSSGrowth:          conclusion.RSSGrowth,
		CheckpointMaxBytes: conclusion.CheckpointMaxBytes,
		FinalStateEqual:    conclusion.FinalStateEqual,
		ForbiddenExit:      conclusion.ForbiddenExit,
		ForbiddenExitCode:  conclusion.ForbiddenExitCode,
		Pass:               conclusion.Pass,
		Conclusion:         conclusion,
		Violations:         violations,
		ErrorReason:        errReason,
	}

	if err := writeReportFile(reportPath, rep); err != nil {
		fmt.Fprintf(stderr, "ghi báo cáo %s thất bại: %v\n", reportPath, err)
		return 1
	}

	fmt.Fprintf(stdout,
		"SOAK RESULT: pass=%v final_state_equal=%v rss_growth=%.3fMB/h checkpoint_max_bytes=%d restarts=%d report=%s\n",
		rep.Pass, rep.FinalStateEqual, rep.RSSGrowth, rep.CheckpointMaxBytes, rep.Restarts, reportPath,
	)
	if !rep.Pass {
		return 1
	}
	return 0
}
