package web

import (
	"context"
	"sync"
	"time"

	"strand/internal/web/store"
)

// Lịch sử số liệu (chế độ DB): mỗi mẫu của sampler được gom và ghi theo lô vào metric_samples (mặc định mỗi
// 30 giây một INSERT nhiều dòng: N pipeline x 6 mẫu); bảo trì mỗi giờ gộp thành mẫu 1 phút và xóa dữ liệu hết
// hạn (raw 24 giờ, 1 phút 7 ngày). Khởi động lại web: ring 1 giờ trong RAM được nạp lại từ DB. Chế độ file
// giữ nguyên ring 1 giờ trong RAM.

// ChartRange là một khoảng của bộ chọn trên tab Graph.
type ChartRange struct {
	Key   string
	Label string
	Dur   time.Duration
	Step  time.Duration // độ rộng bucket khi đọc từ DB (~720 điểm)
}

var chartRanges = []ChartRange{
	{"1h", "1 h", time.Hour, 0},
	{"6h", "6 h", 6 * time.Hour, 30 * time.Second},
	{"24h", "24 h", 24 * time.Hour, 2 * time.Minute},
	{"7d", "7 d", 7 * 24 * time.Hour, 15 * time.Minute},
}

func chartRangeOf(key string) ChartRange {
	for _, r := range chartRanges {
		if r.Key == key {
			return r
		}
	}
	return chartRanges[0]
}

var (
	metricsFlushEvery = 30 * time.Second
	metricsMaintEvery = time.Hour
)

type metricsBuf struct {
	mu      sync.Mutex
	pending []store.MetricSample
}

func (b *metricsBuf) add(name string, s Sample) {
	b.mu.Lock()
	if len(b.pending) < 200000 { // DB mất lâu: không để RAM phình vô hạn
		b.pending = append(b.pending, store.MetricSample{Pipeline: name, T: s.T.UTC(), State: s.State, Lag: s.Lag,
			Throughput: s.Throughput, ApplyRate: s.ApplyRate, Rows: s.Rows, SnapRows: s.SnapRows, Holes: s.Holes, Writeset: s.Writeset, SpoolBytes: s.SpoolBytes})
	}
	b.mu.Unlock()
}

// flushMetrics ghi các mẫu đang chờ; lỗi thì giữ lại cho lần sau.
func (s *Server) flushMetrics() {
	if s.db == nil {
		return
	}
	b := &s.db.metrics
	b.mu.Lock()
	rows := b.pending
	b.pending = nil
	b.mu.Unlock()
	for len(rows) > 0 {
		n := len(rows)
		if n > 1000 {
			n = 1000
		}
		ctx, cancel := dbCtx()
		err := s.db.st.AppendMetrics(ctx, rows[:n])
		cancel()
		if err != nil {
			s.log.Warn("store metrics history failed (will retry)", "rows", len(rows), "err", err)
			b.mu.Lock()
			b.pending = append(rows, b.pending...)
			b.mu.Unlock()
			return
		}
		rows = rows[n:]
	}
}

func (s *Server) maintainMetrics() {
	if s.db == nil {
		return
	}
	now := s.now().UTC().Truncate(time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	err := s.db.st.Locked(ctx, "metrics-maintenance", func(ctx context.Context) error {
		return s.db.st.MaintainMetrics(ctx, now.Add(-time.Minute), now.Add(-store.MetricsRawRetention), now.Add(-store.MetricsRollupRetention))
	})
	if err != nil {
		s.log.Warn("metrics history maintenance failed", "err", err)
	}
}

// runMetricsHistory: vòng ghi lô + bảo trì, tới khi ctx hủy (ghi nốt khi thoát).
func (s *Server) runMetricsHistory(ctx context.Context) {
	if s.db == nil {
		return
	}
	s.maintainMetrics()
	fl := time.NewTicker(metricsFlushEvery)
	mt := time.NewTicker(metricsMaintEvery)
	defer fl.Stop()
	defer mt.Stop()
	for {
		select {
		case <-ctx.Done():
			return // Serve ghi nốt trước khi đóng kho
		case <-fl.C:
			s.flushMetrics()
		case <-mt.C:
			s.maintainMetrics()
		}
	}
}

// preloadSeries nạp ring 1 giờ từ DB lúc khởi động (biểu đồ không trống sau khi web khởi động lại).
func (s *Server) preloadSeries() {
	if s.db == nil {
		return
	}
	now := s.now()
	for _, p := range s.reg.List() {
		ctx, cancel := dbCtx()
		rows, err := s.db.st.QueryMetrics(ctx, p.Name, now.Add(-time.Hour), now, s.samp.interval, false)
		cancel()
		if err != nil {
			s.log.Warn("load metrics history failed", "pipeline", p.Name, "err", err)
			return
		}
		pts := make([]Sample, 0, len(rows))
		for _, r := range rows {
			pts = append(pts, sampleOf(r, false))
		}
		s.samp.preload(p.Name, pts)
	}
}

func sampleOf(r store.MetricSample, useMax bool) Sample {
	lag := r.Lag
	if useMax {
		lag = r.LagMax // khoảng dài: đỉnh trễ của bucket, không để trung bình che mất
	}
	return Sample{T: r.T, State: r.State, Lag: lag, Throughput: r.Throughput, ApplyRate: r.ApplyRate, Rows: r.Rows, SnapRows: r.SnapRows, Holes: r.Holes,
		Writeset: r.Writeset, SpoolBytes: r.SpoolBytes}
}

// seriesFor trả các điểm của khoảng rng: 1 giờ từ RAM; dài hơn từ DB (nil, false ở chế độ file).
func (s *Server) seriesFor(name string, rng ChartRange) ([]Sample, bool) {
	if rng.Step == 0 {
		return s.samp.Series(name), true
	}
	if s.db == nil {
		return nil, false
	}
	now := s.now().UTC()
	from := now.Add(-rng.Dur)
	rawFrom := now.Add(-store.MetricsRawRetention)
	ctx, cancel := dbCtx()
	defer cancel()
	var rows []store.MetricSample
	if from.Before(rawFrom) {
		old, err := s.db.st.QueryMetrics(ctx, name, from, rawFrom, rng.Step, true)
		if err != nil {
			s.log.Warn("read metrics history failed", "pipeline", name, "err", err)
			return nil, true
		}
		rows = old
		from = rawFrom
	}
	recent, err := s.db.st.QueryMetrics(ctx, name, from, now, rng.Step, false)
	if err != nil {
		s.log.Warn("read metrics history failed", "pipeline", name, "err", err)
		return nil, true
	}
	rows = append(rows, recent...)
	out := make([]Sample, 0, len(rows))
	for _, r := range rows {
		out = append(out, sampleOf(r, true))
	}
	return out, true
}
