package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"strand/internal/governor"
)

// Governor (02 §7, P1-16): onoff (mặc định ở P1) hoặc static; breaker theo tín hiệu đọc thẳng
// từ DB (p99 query của chính Strand, replica lag), không phụ thuộc metrics_source.
// Governor điều tiết snapshot (background) bằng khoảng cách giữa các chunk.

func (p *Pipeline) setupGovernor() error {
	algo := strings.ToLower(p.cfg.Governor.Algorithm)
	switch algo {
	case "", "onoff", "static":
	case "grace-v2":
		p.log.Warn("governor grace-v2 is a Phase 2 feature; using onoff (02 §7 fallback)")
		algo = "onoff"
	default:
		return fmt.Errorf("unknown governor.algorithm %q", p.cfg.Governor.Algorithm)
	}
	if algo == "" {
		algo = "onoff"
	}
	p.govAlgo = algo
	p.govTick = durationOr(p.cfg.Governor.Tick, time.Second)
	soft := float64(p.cfg.Resources.Source.IOPS.Soft)
	if soft <= 0 {
		soft = 600
	}
	hard := float64(p.cfg.Resources.Source.IOPS.Hard)
	bcfg := governor.BreakerConfig{
		MaxRatio:      p.cfg.Governor.Breaker.StrandQueryP99.MaxRatio,
		MaxReplicaLag: durationOr(p.cfg.Governor.Breaker.ReplicaLag.Max, 0),
		// Mở lại sau 60 giây liên tục dưới ngưỡng (02 §7).
		CooldownTicks: int((60 * time.Second) / p.govTick),
	}
	p.gov = governor.NewOnoffGovernor(governor.OnoffConfig{
		SoftRate:     soft,
		HardRate:     hard,
		LagSLO:       durationOr(p.cfg.Governor.LagSLO, 5*time.Second),
		Breaker:      governor.NewBreaker(bcfg),
		SignalReader: governor.NewMySQLDBSignalReader(p.src, nil),
	})
	return nil
}

func (p *Pipeline) governorLoop(ctx context.Context) {
	t := time.NewTicker(p.govTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if p.govAlgo == "static" {
			continue
		}
		if err := p.gov.Tick(ctx); err != nil {
			p.log.Warn("governor tick failed", "error", err)
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
