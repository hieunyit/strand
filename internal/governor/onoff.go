package governor

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricsInitOnce sync.Once
	zoneMetric      prometheus.Gauge
	pressureMetric  *prometheus.GaugeVec
	breakerMetric   prometheus.Gauge
	rateMetric      *prometheus.GaugeVec
)

func initGovernorMetrics() {
	metricsInitOnce.Do(func() {
		zoneMetric = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "strand_governor_zone",
			Help: "Current governor zone (0 Green, 1 Yellow, 2 Red)",
		})
		pressureMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_governor_pressure",
			Help: "Current governor pressure by signal",
		}, []string{"signal"})
		breakerMetric = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "strand_breaker_open",
			Help: "Circuit breaker open status (0 closed, 1 open)",
		})
		rateMetric = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "strand_governor_rate_tokens",
			Help: "Governor token rate per resource",
		}, []string{"resource"})

		_ = prometheus.Register(zoneMetric)
		_ = prometheus.Register(pressureMetric)
		_ = prometheus.Register(breakerMetric)
		_ = prometheus.Register(rateMetric)
	})
}

// OnoffConfig cấu hình cho Governor onoff theo spec/02 §7, §10.
type OnoffConfig struct {
	SoftRate     float64       // Giới hạn mềm (R), ví dụ 1000 tokens/s
	HardRate     float64       // Giới hạn cứng
	LagSLO       time.Duration // SLO độ trễ (mặc định 5s)
	Breaker      *Breaker
	SignalReader DBSignalReader
}

// OnoffGovernor cài đặt Governor kiểu onoff theo spec/02 §7:
// - Background chỉ có hai trạng thái: chạy ở R = soft limit hoặc dừng hẳn.
// - Dừng khi P >= 1.0, khi có tín hiệu cứng, hoặc khi breaker kích hoạt.
// - Chạy lại khi P < 0.90 trong 3 tick liên tiếp.
type OnoffGovernor struct {
	mu                sync.RWMutex
	cfg               OnoffConfig
	filter            *SignalFilter
	currentRate       float64
	foregroundRate    float64
	backgroundRate    float64
	backgroundRunning bool
	lowPressureTicks  int
	currentPressure   float64
	currentZone       Zone
}

// NewOnoffGovernor khởi tạo OnoffGovernor mới.
func NewOnoffGovernor(cfg OnoffConfig) *OnoffGovernor {
	initGovernorMetrics()
	if cfg.SoftRate <= 0 {
		cfg.SoftRate = 1000.0
	}
	if cfg.HardRate <= 0 {
		cfg.HardRate = cfg.SoftRate * 2.0
	}
	if cfg.LagSLO <= 0 {
		cfg.LagSLO = 5 * time.Second
	}
	if cfg.Breaker == nil {
		cfg.Breaker = NewBreaker(BreakerConfig{})
	}

	return &OnoffGovernor{
		cfg:               cfg,
		filter:            NewSignalFilter(0.3),
		currentRate:       cfg.SoftRate,
		foregroundRate:    cfg.SoftRate,
		backgroundRate:    cfg.SoftRate,
		backgroundRunning: true,
		currentZone:       ZoneGreen,
	}
}

// Tick thực hiện một chu kỳ đánh giá tín hiệu và cập nhật tốc độ R.
func (g *OnoffGovernor) Tick(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// 1. Đọc tín hiệu trực tiếp từ DB
	var sig DBSignals
	if g.cfg.SignalReader != nil {
		var err error
		sig, err = g.cfg.SignalReader.ReadSignals(ctx)
		if err != nil {
			return err
		}
	}

	// 2. Tính áp suất P
	rawPressure := 0.0
	if g.cfg.LagSLO > 0 {
		rawPressure = sig.ReplicaLagSeconds / g.cfg.LagSLO.Seconds()
	}
	if sig.CommitLagSeconds > 0 && g.cfg.LagSLO > 0 {
		cPressure := sig.CommitLagSeconds / g.cfg.LagSLO.Seconds()
		if cPressure > rawPressure {
			rawPressure = cPressure
		}
	}
	filteredPressure := g.filter.Add(rawPressure)
	g.currentPressure = filteredPressure

	// 3. Đánh giá Circuit Breaker
	isBreakerOpen, _ := g.cfg.Breaker.Evaluate(sig)

	// 4. Áp dụng luật onoff và Breaker (spec: 02 §7)
	if isBreakerOpen {
		// Breaker kích hoạt: R giảm ngay về 50% và background dừng!
		g.currentZone = ZoneRed
		g.currentRate = g.cfg.SoftRate * 0.5
		g.foregroundRate = g.currentRate
		g.backgroundRunning = false
		g.backgroundRate = 0
		g.lowPressureTicks = 0
	} else if filteredPressure >= 1.0 || sig.HardSignal {
		// P >= 1.0 hoặc tín hiệu cứng: background dừng hẳn
		g.currentZone = ZoneRed
		g.currentRate = g.cfg.SoftRate
		g.foregroundRate = g.currentRate
		g.backgroundRunning = false
		g.backgroundRate = 0
		g.lowPressureTicks = 0
	} else if filteredPressure < 0.90 {
		// P < 0.90: đếm số tick liên tiếp
		g.lowPressureTicks++
		g.currentRate = g.cfg.SoftRate
		g.foregroundRate = g.currentRate

		if g.lowPressureTicks >= 3 {
			// Đủ 3 tick liên tiếp: background chạy lại!
			g.backgroundRunning = true
			g.backgroundRate = g.cfg.SoftRate
			g.currentZone = ZoneGreen
		} else {
			// Chưa đủ 3 tick: nếu trước đó đang dừng thì tiếp tục dừng
			if !g.backgroundRunning {
				g.currentZone = ZoneYellow
				g.backgroundRate = 0
			} else {
				g.currentZone = ZoneGreen
				g.backgroundRate = g.cfg.SoftRate
			}
		}
	} else {
		// 0.90 <= P < 1.0: Vùng Yellow, reset lowPressureTicks
		g.currentZone = ZoneYellow
		g.lowPressureTicks = 0
		g.currentRate = g.cfg.SoftRate
		g.foregroundRate = g.currentRate
		if g.backgroundRunning {
			g.backgroundRate = g.cfg.SoftRate
		} else {
			g.backgroundRate = 0
		}
	}

	// 5. Cập nhật metric Prometheus
	if zoneMetric != nil {
		zoneMetric.Set(float64(g.currentZone))
	}
	if pressureMetric != nil {
		pressureMetric.WithLabelValues("replica_lag").Set(filteredPressure)
	}
	if breakerMetric != nil {
		if isBreakerOpen {
			breakerMetric.Set(1)
		} else {
			breakerMetric.Set(0)
		}
	}
	if rateMetric != nil {
		rateMetric.WithLabelValues("iops").Set(g.currentRate)
	}

	return nil
}

func (g *OnoffGovernor) CurrentRate() (foregroundRate float64, backgroundRate float64) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.foregroundRate, g.backgroundRate
}

func (g *OnoffGovernor) Zone() Zone {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.currentZone
}

func (g *OnoffGovernor) Pressure() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.currentPressure
}

func (g *OnoffGovernor) BreakerOpen() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.cfg.Breaker.IsOpen()
}

func (g *OnoffGovernor) RecordQueryLatency(d time.Duration) {
	if r, ok := g.cfg.SignalReader.(*MySQLDBSignalReader); ok {
		r.latencyTracker.Record(d)
	} else if r, ok := g.cfg.SignalReader.(*PGDBSignalReader); ok {
		r.latencyTracker.Record(d)
	} else if s, ok := g.cfg.SignalReader.(*SimulatedSignalReader); ok {
		s.latencyTracker.Record(d)
	}
}

// SetSoftRate đổi soft limit nóng (strand throttle set --soft).
func (g *OnoffGovernor) SetSoftRate(r float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r > 0 {
		g.cfg.SoftRate = r
		if g.backgroundRunning {
			g.backgroundRate = r
		}
	}
}

// BackgroundRunning cho biết background (snapshot, prefetch) đang được chạy hay bị dừng.
func (g *OnoffGovernor) BackgroundRunning() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.backgroundRunning
}

// SensitivityUnknown trả về false cho onoff vì onoff không dùng RLS (spec: 02 §7).
func (g *OnoffGovernor) SensitivityUnknown() bool {
	return false
}
