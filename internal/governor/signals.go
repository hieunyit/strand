package governor

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// SignalFilter lọc tín hiệu thô qua median của 3 mẫu gần nhất rồi tính EWMA (α = 0.3).
// spec: 02 §7
type SignalFilter struct {
	mu      sync.Mutex
	history []float64
	ewma    float64
	hasEWMA bool
	alpha   float64
}

func NewSignalFilter(alpha float64) *SignalFilter {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.3
	}
	return &SignalFilter{
		history: make([]float64, 0, 3),
		alpha:   alpha,
	}
}

func (f *SignalFilter) Add(v float64) float64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.history = append(f.history, v)
	if len(f.history) > 3 {
		f.history = f.history[1:]
	}

	// 1. Lấy median của tối đa 3 mẫu
	med := v
	if len(f.history) == 2 {
		med = (f.history[0] + f.history[1]) / 2.0
	} else if len(f.history) == 3 {
		tmp := []float64{f.history[0], f.history[1], f.history[2]}
		sort.Float64s(tmp)
		med = tmp[1]
	}

	// 2. Tính EWMA: new = alpha * med + (1 - alpha) * old
	if !f.hasEWMA {
		f.ewma = med
		f.hasEWMA = true
	} else {
		f.ewma = f.alpha*med + (1.0-f.alpha)*f.ewma
	}
	return f.ewma
}

func (f *SignalFilter) Value() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ewma
}

// LatencyTracker theo dõi thời gian thực thi của các truy vấn Strand để tính p99.
type LatencyTracker struct {
	mu       sync.Mutex
	samples  []time.Duration
	baseline time.Duration
	maxCap   int
}

func NewLatencyTracker(baseline time.Duration) *LatencyTracker {
	if baseline <= 0 {
		baseline = 10 * time.Millisecond
	}
	return &LatencyTracker{
		samples:  make([]time.Duration, 0, 100),
		baseline: baseline,
		maxCap:   500,
	}
}

func (t *LatencyTracker) Record(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.samples) >= t.maxCap {
		t.samples = t.samples[1:]
	}
	t.samples = append(t.samples, d)
}

func (t *LatencyTracker) P99() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.samples) == 0 {
		return t.baseline
	}
	cp := make([]time.Duration, len(t.samples))
	copy(cp, t.samples)
	sort.Slice(cp, func(i, j int) bool {
		return cp[i] < cp[j]
	})
	idx := int(float64(len(cp)) * 0.99)
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func (t *LatencyTracker) Baseline() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.baseline
}

// MySQLDBSignalReader đọc các tín hiệu sức khỏe trực tiếp từ MySQL.
// spec: 02 §7 (không phụ thuộc metrics_source)
type MySQLDBSignalReader struct {
	db             *sql.DB
	latencyTracker *LatencyTracker
	hard           *HardSignalDetector

	// SpoolUsage (tùy chọn) trả về số byte spool local đang dùng và hard limit của nó;
	// dùng cho tín hiệu cứng "spool local ≥ 95% hard limit".
	SpoolUsage func() (used, hard int64)
}

func NewMySQLDBSignalReader(db *sql.DB, tracker *LatencyTracker) *MySQLDBSignalReader {
	if tracker == nil {
		tracker = NewLatencyTracker(10 * time.Millisecond)
	}
	return &MySQLDBSignalReader{
		db:             db,
		latencyTracker: tracker,
		hard:           NewHardSignalDetector(),
	}
}

func (r *MySQLDBSignalReader) ReadSignals(ctx context.Context) (DBSignals, error) {
	signals := DBSignals{
		QueryP99:      r.latencyTracker.P99(),
		QueryBaseline: r.latencyTracker.Baseline(),
	}

	var status map[string]uint64
	if r.db != nil {
		status = readGlobalStatus(ctx, r.db, HardSignalCounters)
	}
	var used, hard int64
	if r.SpoolUsage != nil {
		used, hard = r.SpoolUsage()
	}
	signals.HardSignal, signals.HardReason = r.hard.Evaluate(status, used, hard)

	if r.db == nil {
		return signals, nil
	}

	// Đọc độ trễ replica từ SHOW REPLICA STATUS (MySQL 8.4) hoặc SHOW SLAVE STATUS (MySQL 8.0)
	start := time.Now()
	rows, err := r.db.QueryContext(ctx, "SHOW REPLICA STATUS")
	if err != nil {
		// Fallback sang cú pháp cũ
		rows, err = r.db.QueryContext(ctx, "SHOW SLAVE STATUS")
	}
	r.latencyTracker.Record(time.Since(start))

	if err == nil && rows != nil {
		defer rows.Close()
		cols, _ := rows.Columns()
		if rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err == nil {
				for i, col := range cols {
					if col == "Seconds_Behind_Source" || col == "Seconds_Behind_Master" {
						if vals[i] != nil {
							signals.ReplicaLagSeconds = parseLagValue(vals[i])
						}
						break
					}
				}
			}
		}
	}

	return signals, nil
}

// HardSignalCounters là các counter của SHOW GLOBAL STATUS tạo tín hiệu cứng cho MySQL
// (design: bảng loại tín hiệu, hàng "Cứng"): Δ > 0 giữa hai tick là tác dụng ngay. Deadlock và
// lock wait (Innodb_row_lock_waits) thuộc tín hiệu mềm theo đồng thời, chỉ hạ trần L, nên không ở đây.
var HardSignalCounters = []string{"Innodb_buffer_pool_wait_free", "Innodb_log_waits"}

// SpoolHardSignalRatio: spool local chạm 95% hard limit là tín hiệu cứng.
const SpoolHardSignalRatio = 0.95

// HardSignalDetector biến counter cộng dồn thành tín hiệu cứng bằng delta giữa hai lần đọc
// (02 §7: "Tín hiệu cứng tác dụng ngay, và luôn lấy delta của counter"). Lần đọc đầu của một
// counter chỉ lấy mốc, không bao giờ kích hoạt; counter giảm (server restart, FLUSH STATUS)
// cũng chỉ đặt lại mốc.
type HardSignalDetector struct {
	mu   sync.Mutex
	last map[string]uint64
}

func NewHardSignalDetector() *HardSignalDetector {
	return &HardSignalDetector{last: map[string]uint64{}}
}

// Evaluate nhận giá trị counter hiện tại và mức dùng spool, trả về tín hiệu cứng kèm lý do.
func (d *HardSignalDetector) Evaluate(status map[string]uint64, spoolUsed, spoolHard int64) (bool, string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var reasons []string
	for _, name := range HardSignalCounters {
		v, ok := status[name]
		if !ok {
			continue
		}
		prev, seen := d.last[name]
		d.last[name] = v
		if seen && v > prev {
			reasons = append(reasons, fmt.Sprintf("Δ%s = %d", name, v-prev))
		}
	}
	if SpoolNearFull(spoolUsed, spoolHard) {
		reasons = append(reasons, fmt.Sprintf("local spool %d/%d bytes ≥ %.0f%% of hard limit",
			spoolUsed, spoolHard, SpoolHardSignalRatio*100))
	}
	if len(reasons) == 0 {
		return false, ""
	}
	return true, strings.Join(reasons, "; ")
}

// SpoolNearFull: spool local đã dùng ≥ 95% hard limit (hard ≤ 0 = không cấu hình, không kích hoạt).
func SpoolNearFull(used, hard int64) bool {
	return hard > 0 && float64(used) >= SpoolHardSignalRatio*float64(hard)
}

// readGlobalStatus đọc các counter cần thiết từ SHOW GLOBAL STATUS. Lỗi thì trả map rỗng:
// thiếu một lần đọc chỉ làm lỡ một delta, không được làm hỏng tick của governor.
func readGlobalStatus(ctx context.Context, db *sql.DB, names []string) map[string]uint64 {
	out := map[string]uint64{}
	if len(names) == 0 {
		return out
	}
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = "'" + strings.ReplaceAll(n, "'", "''") + "'"
	}
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ("+strings.Join(quoted, ", ")+")")
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var name, val string
		if err := rows.Scan(&name, &val); err != nil {
			return out
		}
		var v uint64
		if _, err := fmt.Sscanf(strings.TrimSpace(val), "%d", &v); err == nil {
			out[name] = v
		}
	}
	return out
}

// SimulatedSignalReader là reader mô phỏng phục vụ unit test.
type SimulatedSignalReader struct {
	mu             sync.Mutex
	signals        DBSignals
	latencyTracker *LatencyTracker
}

func NewSimulatedSignalReader(initSignals DBSignals) *SimulatedSignalReader {
	tracker := NewLatencyTracker(initSignals.QueryBaseline)
	if initSignals.QueryP99 > 0 {
		tracker.Record(initSignals.QueryP99)
	}
	return &SimulatedSignalReader{
		signals:        initSignals,
		latencyTracker: tracker,
	}
}

func (r *SimulatedSignalReader) SetSignals(sig DBSignals) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.signals = sig
	if sig.QueryP99 > 0 {
		r.latencyTracker.Record(sig.QueryP99)
	}
}

func (r *SimulatedSignalReader) ReadSignals(ctx context.Context) (DBSignals, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sig := r.signals
	if sig.QueryP99 == 0 {
		sig.QueryP99 = r.latencyTracker.P99()
	}
	if sig.QueryBaseline == 0 {
		sig.QueryBaseline = r.latencyTracker.Baseline()
	}
	return sig, nil
}

func parseSecondsBytes(b []byte) float64 {
	var v float64
	fmt.Sscanf(string(b), "%f", &v)
	return v
}

// parseLagValue đọc Seconds_Behind_Source với mọi kiểu driver có thể trả (int64, uint64,
// []byte của text protocol, string...). Trước đây chỉ nhận int64/string nên uint64 thành 0.
func parseLagValue(v any) float64 {
	switch x := v.(type) {
	case int64:
		return float64(x)
	case uint64:
		return float64(x)
	case int32:
		return float64(x)
	case uint32:
		return float64(x)
	case int:
		return float64(x)
	case float64:
		return x
	case []byte:
		return parseSecondsBytes(x)
	case string:
		return parseSecondsBytes([]byte(x))
	}
	return 0
}
