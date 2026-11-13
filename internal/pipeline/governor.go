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

// Governor điều tiết snapshot (background) bằng khoảng cách giữa các chunk (token bucket chung, xem
// pace), cỡ chunk (luật 250 ms, nextChunkRows) và cửa sổ L (windowSaturated). Bảng ánh xạ đầu ra →
// actuator: docs/GOVERNOR-WIRING.md.

// selectGovernorAlgo chuẩn hóa governor.algorithm (rỗng = onoff). Source nào cũng chạy được ba
// governor: MySQL và PostgreSQL đều có bộ đọc tín hiệu (02 §7).
func selectGovernorAlgo(cfg string) (string, error) {
	algo := strings.ToLower(strings.TrimSpace(cfg))
	switch algo {
	case "":
		return "onoff", nil
	case "onoff", "static", "grace-v2":
		return algo, nil
	}
	return "", fmt.Errorf("unknown governor.algorithm %q", cfg)
}

// signalProber là bộ đọc tín hiệu cần đọc thử khi dựng governor (source PG: quyền trên view thống kê).
type signalProber interface {
	governor.DBSignalReader
	Probe(ctx context.Context) ([]string, error)
}

// probeSignals đọc thử bộ đọc tín hiệu: lỗi → cảnh báo và static (governor không được chạy mù);
// cảnh báo (tín hiệu bị ẩn một phần) chỉ ghi log. Trả algo hiệu lực.
func (p *Pipeline) probeSignals(ctx context.Context, algo string, r signalProber) string {
	if algo == "static" {
		return algo
	}
	warns, err := r.Probe(ctx)
	if err != nil {
		p.log.Warn("cannot read source governor signals; using static", "algorithm", algo, "error", err)
		return "static"
	}
	for _, w := range warns {
		p.log.Warn("governor signals degraded", "algorithm", algo, "reason", w)
	}
	return algo
}

func (p *Pipeline) setupGovernor(ctx context.Context) error {
	algo, err := selectGovernorAlgo(p.cfg.Governor.Algorithm)
	if err != nil {
		return err
	}
	p.govTick = durationOr(p.cfg.Governor.Tick, time.Second)
	// Tín hiệu cứng (02 §7): MySQL — delta counter InnoDB; PG — wal_status của slot; cả hai — spool
	// local ≥ 95% spool.hard. Trước đây source PG luôn bị ép về static (backlog #31).
	var reader governor.DBSignalReader
	if p.srcEngine == model.EnginePG {
		slot, _ := p.pgSlotNames()
		pr := governor.NewPGDBSignalReader(p.src, slot, nil)
		pr.SpoolUsage = p.spoolUsage
		// Giữ cả khi static: mẫu PG còn dùng cho P_lag (WAL slot) và "reader đã đuổi kịp" ở bootstrap.
		p.pgSignals = pr
		qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		algo = p.probeSignals(qctx, algo, pr)
		cancel()
		reader = pr
	} else {
		mr := governor.NewMySQLDBSignalReader(p.src, nil)
		mr.SpoolUsage = p.spoolUsage
		reader = mr
	}
	p.govAlgo = algo
	// Luật cỡ chunk 250 ms (02 §7) áp cho onoff và grace-v2 khi chunk_rows = auto; static và chunk_rows
	// cố định giữ nguyên cỡ chunk (mốc so sánh của benchmark).
	p.adaptiveChunks = algo != "static" && chunkRowsAuto(p.cfg.Bootstrap.Logical.ChunkRows)
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
	p.breaker = governor.NewBreaker(bcfg)
	if algo == "grace-v2" {
		// GRACE v2 (02 §7); probe: off tắt dither ±1%.
		p.gov = governor.NewGraceGovernor(governor.GraceConfig{
			SoftRate:     soft,
			HardRate:     hard,
			LagSLO:       durationOr(p.cfg.Governor.LagSLO, 5*time.Second),
			Tick:         p.govTick,
			DelayTicks:   1,
			Probe:        !strings.EqualFold(strings.TrimSpace(p.cfg.Governor.Probe), "off"),
			Breaker:      p.breaker,
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
		Breaker:      p.breaker,
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
	breakerWas, reasonWas := false, ""
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
		breakerWas, reasonWas = p.logBreaker(breakerWas, reasonWas)
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

// feedGrace truyền pha, P_lag theo pha và trạng thái cửa sổ L cho GRACE trước mỗi tick (02 §7 bảng
// P_lag): bootstrap = khoảng cách reader → head của source; synced = độ trễ apply ở target; cả hai pha
// cộng số hạng WAL slot (source PG).
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
	gg.SetLagPressure(math.Max(lag/slo, p.walRetainedPressure()))
	// Green chỉ tăng R khi cửa sổ L đang bị dùng hết (02 §7): có reader snapshot đang chạy, không bị giữ
	// bởi pause / windows / breaker / background dừng.
	gg.SetWindowSaturated(windowSaturated(p.snapReaders.Load(), p.snapGated.Load()))
}

// walRetainedPressure: số hạng WAL của P_lag (02 §7) = WAL slot đang giữ / max_slot_wal_keep_size, từ
// mẫu PG gần nhất. Source MySQL hoặc chưa có mẫu: 0.
func (p *Pipeline) walRetainedPressure() float64 {
	if p.pgSignals == nil {
		return 0
	}
	s, ok := p.pgSignals.Last()
	if !ok {
		return 0
	}
	return s.WALRetainedRatio()
}

// windowSaturated: background chỉ có một reader nối tiếp cho mỗi bảng (không có actuator đổi số reader
// song song), nên L thực tế = số reader đang chạy. Cửa sổ "bị dùng hết" khi có ít nhất một reader không
// bị giữ ở cổng của pace.
func windowSaturated(readers, gated int32) bool { return readers > gated }

// chunkRowsAuto: bootstrap.logical.chunk_rows rỗng hoặc "auto".
func chunkRowsAuto(v string) bool {
	v = strings.TrimSpace(v)
	return v == "" || strings.EqualFold(v, "auto")
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

// logBreaker ghi log khi breaker mở/đóng hoặc đổi lý do (trước đây lý do bị bỏ: người vận hành chỉ thấy
// snapshot đứng mà không biết vì sao).
func (p *Pipeline) logBreaker(was bool, reasonWas string) (bool, string) {
	if p.breaker == nil {
		return was, reasonWas
	}
	open, reason := p.breaker.IsOpen(), p.breaker.Reason()
	switch {
	case open && !was:
		p.log.Warn("source circuit breaker OPEN: snapshot paused to protect the source", "reason", reason,
			"hint", "it closes after 60 s below the thresholds; tune governor.breaker (strand_query_p99.max_ratio, replica_lag.max) if the source is not actually overloaded")
	case open && reason != reasonWas:
		p.log.Warn("source circuit breaker still open", "reason", reason)
	case !open && was:
		p.log.Info("source circuit breaker closed: snapshot resumes")
	}
	return open, reason
}
