package pipeline

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"strand/internal/governor"
	"strand/internal/model"
)

// Governor (02 §7, P1-16, P2-07): grace-v2, onoff (mặc định) hoặc static; breaker theo tín hiệu đọc thẳng
// từ DB (p99 query của chính Strand, replica lag), không phụ thuộc metrics_source.
// softRate trả soft limit IOPS source hiện hành (0 nếu governor chưa được dựng).
func (p *Pipeline) softRate() float64 { return math.Float64frombits(p.softRateBits.Load()) }

// setSoftRate đổi soft limit nóng: governor (onoff/GRACE) và nhịp static cùng dùng giá trị mới.
func (p *Pipeline) setSoftRate(r float64) {
	p.softRateBits.Store(math.Float64bits(r))
	if p.gov != nil {
		p.gov.SetSoftRate(r)
	}
}

// Governor điều tiết snapshot (background) bằng khoảng cách giữa các chunk.

func (p *Pipeline) setupGovernor() error {
	algo := strings.ToLower(p.cfg.Governor.Algorithm)
	switch algo {
	case "", "onoff", "static", "grace-v2":
	default:
		return fmt.Errorf("unknown governor.algorithm %q", p.cfg.Governor.Algorithm)
	}
	if algo == "" {
		algo = "onoff"
	}
	p.govTick = durationOr(p.cfg.Governor.Tick, time.Second)
	if algo != "static" && p.srcEngine != model.EngineMySQL {
		// Tín hiệu onoff đọc từ MySQL, và governor chỉ điều tiết snapshot logic — thứ source PG
		// chưa có: dùng static.
		p.log.Warn("governor onoff reads MySQL signals; using static for a PostgreSQL source")
		algo = "static"
	}
	p.govAlgo = algo
	soft := float64(p.cfg.Resources.Source.IOPS.Soft)
	if soft <= 0 {
		soft = 600
	}
	p.softRateBits.Store(math.Float64bits(soft))
	hard := float64(p.cfg.Resources.Source.IOPS.Hard)
	bcfg := governor.BreakerConfig{
		MaxRatio:      p.cfg.Governor.Breaker.StrandQueryP99.MaxRatio,
		MaxReplicaLag: durationOr(p.cfg.Governor.Breaker.ReplicaLag.Max, 0),
		// Mở lại sau 60 giây liên tục dưới ngưỡng (02 §7).
		CooldownTicks: int((60 * time.Second) / p.govTick),
	}
	// Tín hiệu cứng (02 §7): delta counter InnoDB trên source và spool local ≥ 95% spool.hard.
	reader := governor.NewMySQLDBSignalReader(p.src, nil)
	reader.SpoolUsage = p.spoolUsage

	if algo == "grace-v2" {
		// GRACE v2 (02 §7); probe: off tắt dither ±1%.
		p.gov = governor.NewGraceGovernor(governor.GraceConfig{
			SoftRate:     soft,
			HardRate:     hard,
			LagSLO:       durationOr(p.cfg.Governor.LagSLO, 5*time.Second),
			Tick:         p.govTick,
			DelayTicks:   1,
			Probe:        !strings.EqualFold(strings.TrimSpace(p.cfg.Governor.Probe), "off"),
			Breaker:      governor.NewBreaker(bcfg),
			SignalReader: reader,
			PipelineID:   p.id,
			Metrics:      p.metrics,
		})
		return nil
	}
	// onoff và static giữ nguyên như P1 (static chỉ bỏ qua governor qua govAlgo).
	p.gov = governor.NewOnoffGovernor(governor.OnoffConfig{
		SoftRate:     soft,
		HardRate:     hard,
		LagSLO:       durationOr(p.cfg.Governor.LagSLO, 5*time.Second),
		Breaker:      governor.NewBreaker(bcfg),
		SignalReader: reader,
	})
	return nil
}

// spoolUsage trả về số byte spool local đang dùng (spool prefetch) và resources.spool.hard.
func (p *Pipeline) spoolUsage() (used, hard int64) {
	if p.prefetch != nil {
		used = p.prefetch.Usage().LocalBytes
	}
	return used, int64(parseBytes(p.cfg.Resources.Spool.Hard))
}

func (p *Pipeline) governorLoop(ctx context.Context) {
	t := time.NewTicker(p.govTick)
	defer t.Stop()
	first := true
	var alerts graceAlertState
	for {
		if !first {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
		first = false
		if p.govAlgo == "static" {
			continue
		}

		gg, isGrace := p.gov.(*governor.GraceGovernor)
		if isGrace {
			p.feedGrace(ctx, gg)
		}
		if err := p.gov.Tick(ctx); err != nil {
			p.log.Warn("governor tick failed", "error", err)
			continue
		}
		if isGrace {
			// GRACE tự xuất metric §11 (zone, pressure theo signal, rate, breaker, sensitivity_unknown).
			p.graceAlerts(gg, &alerts)
			continue
		}
		_, bg := p.gov.CurrentRate()
		p.metrics.GovernorZone.WithLabelValues(p.id).Set(float64(p.gov.Zone()))
		p.metrics.GovernorPressure.WithLabelValues(p.id, "replica_lag").Set(p.gov.Pressure())
		p.metrics.GovernorRateTokens.WithLabelValues(p.id, "source_iops").Set(bg)
		open := 0.0
		if p.gov.BreakerOpen() {
			open = 1
		}
		p.metrics.BreakerOpen.WithLabelValues(p.id).Set(open)
	}
}

// feedGrace truyền pha và P_lag theo pha cho GRACE trước mỗi tick (02 §7 bảng P_lag):
// bootstrap = khoảng cách reader → head của source; synced = độ trễ apply ở target.
func (p *Pipeline) feedGrace(ctx context.Context, gg *governor.GraceGovernor) {
	slo := durationOr(p.cfg.Governor.LagSLO, 5*time.Second).Seconds()
	phase, lag := "synced", p.lagSeconds()
	if p.bootstrap.Load() || p.verifying.Load() {
		phase = "bootstrap"
		lag = 0
		if ts := p.lastRoutedTS.Load(); ts > 0 && !p.caughtUpWithSource(ctx) {
			lag = time.Since(time.Unix(ts, 0)).Seconds()
		}
	}
	gg.SetPhase(phase)
	gg.SetLagPressure(lag / slo)
}

// graceAlerts ghi log cảnh báo của GRACE khi cờ chuyển từ tắt sang bật (02 §7, §12).
// bootstrap_retention_risk do monitor() xử lý (cùng định nghĩa P_lag bootstrap).
type graceAlertState struct{ lagUnsustainable, pressureExternal bool }

func (p *Pipeline) graceAlerts(gg *governor.GraceGovernor, st *graceAlertState) {
	if v := gg.LagUnsustainable(); v != st.lagUnsustainable {
		st.lagUnsustainable = v
		if v {
			p.log.Warn("lag_unsustainable: foreground floor exceeds hard limit; holding hard limit", "code", "lag_unsustainable")
		}
	}
	if v := gg.PressureExternal(); v != st.pressureExternal {
		st.pressureExternal = v
		if v {
			p.log.Warn("pressure_external: sensitivity ~0 with high confidence for > 5m; holding at polite floor", "code", "pressure_external")
		}
	}
}

// inWindows: bootstrap.logical.windows, ví dụ ["22:00-06:00"] (giờ địa phương). Rỗng = luôn được chạy.
func inWindows(windows []string, now time.Time) bool {
	if len(windows) == 0 {
		return true
	}
	mins := now.Hour()*60 + now.Minute()
	for _, w := range windows {
		a, b, ok := strings.Cut(w, "-")
		if !ok {
			continue
		}
		var h1, m1, h2, m2 int
		if _, err := fmt.Sscanf(strings.TrimSpace(a), "%d:%d", &h1, &m1); err != nil {
			continue
		}
		if _, err := fmt.Sscanf(strings.TrimSpace(b), "%d:%d", &h2, &m2); err != nil {
			continue
		}
		s, e := h1*60+m1, h2*60+m2
		if s <= e && mins >= s && mins < e {
			return true
		}
		if s > e && (mins >= s || mins < e) { // qua nửa đêm
			return true
		}
	}
	return false
}
