package governor

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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
}

func NewMySQLDBSignalReader(db *sql.DB, tracker *LatencyTracker) *MySQLDBSignalReader {
	if tracker == nil {
		tracker = NewLatencyTracker(10 * time.Millisecond)
	}
	return &MySQLDBSignalReader{
		db:             db,
		latencyTracker: tracker,
	}
}

func (r *MySQLDBSignalReader) ReadSignals(ctx context.Context) (DBSignals, error) {
	signals := DBSignals{
		QueryP99:      r.latencyTracker.P99(),
		QueryBaseline: r.latencyTracker.Baseline(),
	}

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
							if sec, ok := vals[i].(int64); ok {
								signals.ReplicaLagSeconds = float64(sec)
							} else if b, ok := vals[i].([]byte); ok {
								// Text protocol: giá trị cột trả về dạng []byte.
								signals.ReplicaLagSeconds = parseSecondsBytes(b)
							} else if s, ok := vals[i].(string); ok {
								var secFloat float64
								fmt.Sscanf(s, "%f", &secFloat)
								signals.ReplicaLagSeconds = secFloat
							}
						}
						break
					}
				}
			}
		}
	}

	return signals, nil
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
