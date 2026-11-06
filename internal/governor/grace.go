package governor

import (
	"context"
	"math"
	"sync"
	"time"

	"strand/internal/metrics"
)

// GRACE v2 (spec/02 §7, dòng 300–341; design §5.2–§5.4).
// Actuator duy nhất x = log R; L và chunk suy ra từ R (§7 dòng 304–307).

const (
	// §7 dòng 309: median 3 mẫu rồi EWMA α = 0,3.
	graceEWMAAlpha = 0.3
	// §7 dòng 320: ngưỡng hysteresis.
	thresholdGreenToYellow = 0.70 // Green → Yellow khi P ≥ 0,70
	thresholdYellowToGreen = 0.60 // Yellow → Green khi P < 0,60 trong 3 tick
	yellowToGreenTicks     = 3
	thresholdRedEntry      = 1.0  // vào Red khi P ≥ 1,0 hoặc tín hiệu cứng
	thresholdRedExit       = 0.90 // rời Red khi P < 0,90 và hết cooldown
	// §7 dòng 309: tín hiệu mềm phải xuất hiện ở 2 trong 3 tick.
	softConfirmNeed   = 2
	softConfirmWindow = 3
	// §7 dòng 324: β = clamp(1 − 0,5(P − 1), 0,5, 0,85); tín hiệu cứng β = 0,5.
	betaMin  = 0.5
	betaMax  = 0.85
	betaHard = 0.5
	// §7 dòng 325: P* = 0,85, hoặc 1,0 khi P_lag ≥ 1; dead band |e| < 0,05; s_min (design §5.3).
	setpointNormal = 0.85
	setpointLag    = 1.0
	deadBand       = 0.05
	sMin           = 0.05
	// §7 dòng 326: tăng tối đa 10% mỗi tick → log(1,10); dither ±1% → log(1,01).
	maxUp      = 0.09531017980432493  // math.Log(1.10)
	ditherStep = 0.009950330853168083 // math.Log(1.01)
	// design §5.3: hằng số CUBIC (đơn vị log/tick³).
	cubicC = 0.004
	// §7 dòng 326: W_max hết hạn sau 10 phút.
	wmaxExpiry = 10 * time.Minute
	// §7 dòng 328: RLS hệ số quên 0,95.
	rlsLambda = 0.95
	// §7 dòng 337: ŝ ≈ 0 tin cậy cao hơn 5 phút → pressure_external.
	externalPressureAfter = 5 * time.Minute
	// §7 dòng 334: breaker cắt R về 50%.
	breakerCut = 0.5

	// Trễ nhóm của bộ lọc (median-3 ≈ 1 tick, EWMA α=0,3 ≈ (1−α)/α ≈ 2,3 tick) làm tròn 3 tick.
	// d trong công thức κ và cooldown là độ trễ phản hồi đo trên P đã lọc (design §5.1 hàng 6:
	// τ đo từ step response), nên d = trễ của plant + trễ của bộ lọc. Mô phỏng cho thấy bỏ phần
	// này thì vòng Vàng dao động (limit cycle) ngay cả khi ŝ đúng.
	filterDelayTicks = 3
	// Số tick R đứng yên ở vùng Xanh trước khi bật dither (design §5.1 hàng 4: "R đứng yên quá lâu").
	ditherIdleTicks = 3
	// design §5.1 hàng 14: mù metric quá 3 tick thì đóng băng tăng, quá 10 tick thì giảm một nửa.
	blindFreezeTicks = 3
	blindCutTicks    = 10
	// ŝ mặc định thận trọng khi chưa đo được (design §5.1 hàng 4: probe off → ŝ mặc định thận trọng).
	sConservative = 1.0
	// Nhiễu danh định của P (design §5.4: mục tiêu σ_P < 0,03); dùng làm sàn cho ước lượng nhiễu RLS.
	rlsNoiseFloor = 0.01
	rlsInitCov    = 1e4
	rlsHorizon    = 3 // số tick của sai phân dùng cho RLS
	// Trong bootstrap với P_lag ≥ 1, background được bảo đảm ít nhất phần này của R (không bao giờ 0).
	bootstrapBgShare = 0.5
)

// Tên các tín hiệu mềm theo tải (label `signal` của strand_governor_pressure, §11).
const (
	SignalReplicaLag = "replica_lag"
	SignalCommitLag  = "commit_lag"
	SignalStrandP99  = "strand_query_p99"
)

var softSignalNames = []string{SignalReplicaLag, SignalCommitLag, SignalStrandP99}

// RLSEstimator ước lượng ŝ = ∂P/∂x bằng RLS vô hướng, hệ số quên λ = 0,95, có hiệp phương sai
// (§7 dòng 328). Hiệp phương sai × phương sai nhiễu cho độ lệch chuẩn của ŝ, dùng để phân biệt
// "ŝ nhỏ" (tin cậy) với "chưa đo được" (không đủ kích thích).
type RLSEstimator struct {
	lambda float64
	sHat   float64
	cov    float64
	noise  float64 // phương sai nhiễu ước lượng (EWMA của innovation²)
}

func NewRLSEstimator(lambda float64) *RLSEstimator {
	if lambda <= 0 || lambda > 1 {
		lambda = rlsLambda
	}
	return &RLSEstimator{lambda: lambda, sHat: sConservative, cov: rlsInitCov, noise: rlsNoiseFloor * rlsNoiseFloor}
}

// Update nạp một cặp (u = Δx đã áp dụng, y = ΔP quan sát). u = 0 thì không có thông tin mới:
// hiệp phương sai lớn dần theo 1/λ ("chưa đo được" dần quay lại).
func (r *RLSEstimator) Update(u, y float64) {
	e := y - r.sHat*u
	pPrior := r.cov / r.lambda
	if u != 0 {
		k := pPrior * u / (1 + pPrior*u*u)
		r.sHat += k * e
		pPrior = (1 - k*u) * pPrior
		// Chỉ cập nhật nhiễu khi có kích thích để y = 0 không kéo nhiễu về sàn một cách giả tạo.
		r.noise = 0.9*r.noise + 0.1*e*e
	}
	r.cov = math.Min(pPrior, rlsInitCov)
	if r.noise < rlsNoiseFloor*rlsNoiseFloor {
		r.noise = rlsNoiseFloor * rlsNoiseFloor
	}
}

func (r *RLSEstimator) Estimate() float64   { return r.sHat }
func (r *RLSEstimator) Covariance() float64 { return r.cov }

// StdDev là độ lệch chuẩn của ŝ.
func (r *RLSEstimator) StdDev() float64 { return math.Sqrt(r.cov * r.noise) }

// Unknown: độ bất định lớn so với chính ŝ (hoặc so với s_min/2 khi ŝ nhỏ) → chưa đo được.
func (r *RLSEstimator) Unknown() bool {
	return r.StdDev() > math.Max(0.5*math.Abs(r.sHat), sMin/2)
}

// ConfidentSmall: đo được và ŝ < s_min (Strand gần như không ảnh hưởng tới P).
func (r *RLSEstimator) ConfidentSmall() bool { return !r.Unknown() && r.sHat < sMin }

// ConfidentLarge: đo được và ŝ ≥ s_min.
func (r *RLSEstimator) ConfidentLarge() bool { return !r.Unknown() && r.sHat >= sMin }

// oscDetector: lưới an toàn cuối (design §5.1 hàng 15): peak-to-peak của log R trong 4d tick > 0,3
// hoặc tự tương quan của Δlog R âm → κ giảm một nửa.
type oscDetector struct {
	xs, dxs []float64
	max     int
}

func (o *oscDetector) observe(x, dx float64) {
	o.xs = append(o.xs, x)
	o.dxs = append(o.dxs, dx)
	if len(o.xs) > o.max {
		o.xs = o.xs[1:]
		o.dxs = o.dxs[1:]
	}
}

func (o *oscDetector) peakToPeak(w int) float64 {
	if len(o.xs) == 0 {
		return 0
	}
	if w <= 0 || w > len(o.xs) {
		w = len(o.xs)
	}
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, v := range o.xs[len(o.xs)-w:] {
		lo, hi = math.Min(lo, v), math.Max(hi, v)
	}
	return hi - lo
}

func (o *oscDetector) negAutocorr() bool {
	if len(o.dxs) < 4 {
		return false
	}
	var prod, sq float64
	for i := 1; i < len(o.dxs); i++ {
		prod += o.dxs[i] * o.dxs[i-1]
		sq += o.dxs[i] * o.dxs[i]
	}
	return sq > 1e-8 && prod/sq < -0.5
}

// GraceConfig cấu hình GraceGovernor (§7, §10 khối governor).
type GraceConfig struct {
	SoftRate     float64       // soft limit (token/s): trần thường của R
	HardRate     float64       // hard limit (token/s): trần tuyệt đối
	MinRate      float64       // sàn tuyệt đối của R (mặc định soft/100, tối thiểu 1)
	PoliteFloor  float64       // polite floor khi pressure_external (mặc định soft/4)
	LagSLO       time.Duration // governor.lag_slo (mặc định 5s)
	Tick         time.Duration // governor.tick (mặc định 1s)
	DelayTicks   int           // trễ phản hồi của plant d (tick); <0 coi là 0; mặc định 1
	Probe        bool          // governor.probe: on = dither ±1% ở vùng Xanh
	Breaker      *Breaker
	SignalReader DBSignalReader
	Phase        string // "bootstrap" | "synced" (mặc định synced)
	PipelineID   string // label pipeline cho metric §11
	Metrics      *metrics.Metrics
	Clock        func() time.Time // đồng hồ (test tiêm vào)
}

// GraceGovernor hiện thực interface Governor bằng GRACE v2.
type GraceGovernor struct {
	mu    sync.RWMutex
	cfg   GraceConfig
	clock func() time.Time

	filters map[string]*SignalFilter
	sigP    map[string]float64 // P_r đã lọc theo từng tín hiệu
	sens    *RLSEstimator
	osc     oscDetector
	dEff    int       // d dùng cho κ/cooldown = DelayTicks + filterDelayTicks
	xHist   []float64 // x thực xuất (gồm dither) theo tick; phần tử cuối = x đang có hiệu lực
	rawHist []float64 // P thô theo tick

	x, xMin, xHard, xSoft, xPolite float64
	xEff                           float64 // x thực xuất ra (gồm dither)
	zone                           Zone
	pHist                          []float64 // P đã lọc của các tick gần nhất (xác nhận 2/3)
	lowTicks                       int
	cooldown                       int
	wmax                           float64
	wmaxAt                         time.Time
	cubicT, cubicK                 float64
	kappaScale                     float64
	stillTicks                     int
	ditherSign                     float64
	blindTicks                     int

	p, rawP    float64
	pLag       float64
	phase      string
	hard       bool
	breakerWas bool
	breakerNow bool

	fgDemand, fgFloor float64
	bgWeights         map[string]float64
	windowSaturated   bool
	fgRate, bgRate    float64

	smallSince             time.Time
	bootstrapRetentionRisk bool
	lagUnsustainable       bool
	pressureExternal       bool
}

// NewGraceGovernor khởi tạo GRACE v2; R bắt đầu ở soft limit.
func NewGraceGovernor(cfg GraceConfig) *GraceGovernor {
	if cfg.SoftRate <= 0 {
		cfg.SoftRate = 600
	}
	if cfg.HardRate < cfg.SoftRate {
		cfg.HardRate = cfg.SoftRate * 2
	}
	if cfg.MinRate <= 0 {
		cfg.MinRate = math.Max(1, cfg.SoftRate/100)
	}
	if cfg.MinRate > cfg.SoftRate {
		cfg.MinRate = cfg.SoftRate
	}
	if cfg.PoliteFloor <= 0 {
		cfg.PoliteFloor = cfg.SoftRate / 4
	}
	cfg.PoliteFloor = math.Max(cfg.PoliteFloor, cfg.MinRate)
	if cfg.LagSLO <= 0 {
		cfg.LagSLO = 5 * time.Second
	}
	if cfg.Tick <= 0 {
		cfg.Tick = time.Second
	}
	if cfg.DelayTicks < 0 {
		cfg.DelayTicks = 0
	}
	if cfg.Breaker == nil {
		cfg.Breaker = NewBreaker(BreakerConfig{})
	}
	if cfg.Phase == "" {
		cfg.Phase = "synced"
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	g := &GraceGovernor{
		cfg:             cfg,
		clock:           clock,
		filters:         map[string]*SignalFilter{},
		sigP:            map[string]float64{},
		sens:            NewRLSEstimator(rlsLambda),
		dEff:            cfg.DelayTicks + filterDelayTicks,
		xSoft:           math.Log(cfg.SoftRate),
		xHard:           math.Log(cfg.HardRate),
		xMin:            math.Log(cfg.MinRate),
		xPolite:         math.Log(cfg.PoliteFloor),
		zone:            ZoneGreen,
		kappaScale:      1,
		ditherSign:      1,
		phase:           cfg.Phase,
		windowSaturated: true,
	}
	for _, n := range softSignalNames {
		g.filters[n] = NewSignalFilter(graceEWMAAlpha)
	}
	g.x, g.xEff = g.xSoft, g.xSoft
	g.xHist = []float64{g.xSoft}
	g.wmax, g.wmaxAt = g.xSoft, clock()
	g.osc.max = 4*g.dEff + 8
	g.fgRate, g.bgRate = cfg.SoftRate, cfg.SoftRate
	return g
}

// Kappa = 50% biên ổn định 2 sin(π / (2(2d + 1))) (§7 dòng 325; design §5.4).
func Kappa(d int) float64 {
	if d < 0 {
		d = 0
	}
	return 0.5 * 2 * math.Sin(math.Pi/float64(2*(2*d+1)))
}

// Beta = clamp(1 − 0,5(P − 1), 0,5, 0,85); tín hiệu cứng thì 0,5 (§7 dòng 324).
func Beta(p float64, hard bool) float64 {
	if hard {
		return betaHard
	}
	return clamp(1-0.5*(p-1), betaMin, betaMax)
}

// Tick chạy một chu kỳ điều khiển (design §5.3).
func (g *GraceGovernor) Tick(ctx context.Context) error {
	var sig DBSignals
	var err error
	if g.cfg.SignalReader != nil {
		sig, err = g.cfg.SignalReader.ReadSignals(ctx)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock()
	if err != nil {
		g.blindTick()
		return err
	}
	g.blindTicks = 0

	// 1. Áp suất: mỗi tín hiệu median-3 → EWMA 0,3 (§7 dòng 309), P = max_r P_r (dòng 312).
	raw := g.rawPressures(sig)
	g.rawP, g.p = 0, 0
	for _, n := range softSignalNames {
		g.sigP[n] = g.filters[n].Add(raw[n])
		g.rawP = math.Max(g.rawP, raw[n])
		g.p = math.Max(g.p, g.sigP[n])
	}
	g.pHist = append(g.pHist, g.p)
	if len(g.pHist) > softConfirmWindow {
		g.pHist = g.pHist[1:]
	}
	// Tín hiệu cứng: tác dụng ngay, counter đã lấy delta trong HardSignalDetector (§7 dòng 309).
	g.hard = sig.HardSignal

	// 2. ŝ bằng RLS (§7 dòng 328): y = P_t − P_{t−h} (P thô, không lẫn động học bộ lọc),
	// u = x_{t−d} − x_{t−d−h} (x đã xuất, gồm dither và bước MD — chính là kích thích).
	// Sai phân trên h = 3 tick thay vì 1 tick để bắt phần lớn đáp ứng của plant có quán tính
	// (sai phân 1 tick làm ŝ thấp đi ~2 lần trên plant bậc nhất, gain gấp đôi); h lẻ để dither
	// ±1% xen kẽ vẫn là kích thích.
	g.rawHist = append(g.rawHist, g.rawP)
	if len(g.rawHist) > rlsHorizon+1 {
		g.rawHist = g.rawHist[1:]
	}
	if n, d := len(g.xHist), g.cfg.DelayTicks; len(g.rawHist) > rlsHorizon && n-1-d-rlsHorizon >= 0 {
		u := g.xHist[n-1-d] - g.xHist[n-1-d-rlsHorizon]
		g.sens.Update(u, g.rawHist[len(g.rawHist)-1]-g.rawHist[0])
	}
	g.updateExternal(now)

	// 3. Breaker (§7 dòng 332–337): đứng trên GRACE.
	g.breakerNow, _ = g.cfg.Breaker.Evaluate(sig)

	// 4. Zone với hysteresis (§7 dòng 320).
	prevZone := g.zone
	g.zone = g.nextZone()
	if g.breakerNow {
		g.zone = ZoneRed
	}
	entered := g.zone != prevZone

	// 5. Luật trên x (§7 bảng dòng 322–326).
	dx := 0.0
	switch g.zone {
	case ZoneRed:
		if g.cooldown > 0 { // cooldown d + 1 tick
			g.cooldown--
			break
		}
		if g.breakerNow && !g.hard && g.p < thresholdRedEntry {
			break // breaker mở nhưng P chưa tới Red: chỉ giữ, không MD dây chuyền
		}
		g.wmax, g.wmaxAt = g.x, now // ghi W_max = x trước khi giảm
		dx = math.Log(Beta(g.p, g.hard))
		g.cooldown = g.dEff + 1
	case ZoneYellow:
		// Bumpless: luật tích phân bắt đầu từ x hiện tại, không có trạng thái cũ cần reset.
		e := g.setpoint() - g.p
		if math.Abs(e) >= deadBand {
			dx = g.gain() * e
		}
	case ZoneGreen:
		// W_max hết hạn sau 10 phút → đặt lại gốc CUBIC từ x hiện tại.
		expired := now.Sub(g.wmaxAt) > wmaxExpiry
		if entered || expired { // bumpless: target(0) = x
			if expired || g.wmax < g.x {
				// Giả mã design §5.3 đặt W_max = x mỗi tick khi x > W_max; làm vậy thì target
				// = x + C(t−K)³ tự cộng dồn (tăng nhanh hơn CUBIC, gây chu kỳ Green↔Red trong
				// mô phỏng). Chỉ đặt lại khi tính gốc CUBIC.
				g.wmax, g.wmaxAt = g.x, now
			}
			g.cubicT, g.cubicK = 0, math.Cbrt((g.wmax-g.x)/cubicC)
		}
		if g.windowSaturated { // chỉ tăng khi L đang bị dùng hết
			g.cubicT++
			dx = g.wmax + cubicC*math.Pow(g.cubicT-g.cubicK, 3) - g.x
			// Chặn thêm (chỉ làm bước nhỏ hơn trần 10%): tổng tăng trong d+1 tick, theo ŝ, không được
			// đẩy P vượt ngưỡng rời Xanh 0,70. Không có chặn này, plant nhạy (ŝ≈2) với trễ của bộ lọc
			// làm P vọt từ 0,6 lên >1,0 trước khi Green→Yellow kịp xác nhận → chu kỳ Green↔Red.
			dx = math.Min(dx, math.Max(0, thresholdGreenToYellow-g.p)/(g.gainS()*float64(g.dEff+1)))
		}
	}

	// 6. Giới hạn: tăng ≤ 10%/tick; không tăng khi breaker mở hoặc khi mù metric.
	dx = math.Min(dx, maxUp)
	if g.breakerNow {
		dx = math.Min(dx, 0)
	}
	floor := g.floorX()
	ceil := g.ceilX()
	newX := g.x + dx
	if g.breakerNow && !g.breakerWas {
		// Breaker vừa kích hoạt: R giảm ngay về 50%; không xuyên sàn foreground trừ tín hiệu cứng.
		newX = math.Min(newX, g.x+math.Log(breakerCut))
		if !g.hard {
			newX = math.Max(newX, floor)
		}
	}
	// Polite floor: chỉ chặn bước giảm khi pressure_external VÀ hiệp phương sai xác nhận ŝ nhỏ;
	// chưa đo được thì giữ hành động giảm (§7 dòng 328, 337). Tín hiệu cứng vẫn xuyên qua.
	if newX < g.x && g.pressureExternal && g.sens.ConfidentSmall() && !g.hard {
		newX = math.Max(newX, math.Min(g.x, g.xPolite))
	}
	lo := floor
	if g.hard {
		lo = g.xMin // chỉ tín hiệu cứng mới xuyên qua sàn foreground
	}
	newX = clamp(newX, math.Min(lo, ceil), ceil)
	base := newX - g.x
	g.x = newX
	g.breakerWas = g.breakerNow

	// 7. Dither ±1% quanh x khi ở Xanh và R đứng yên quá lâu; tắt bằng probe: off (§7 dòng 326).
	if math.Abs(base) < 1e-9 {
		g.stillTicks++
	} else {
		g.stillTicks = 0
	}
	xOut := g.x
	if g.cfg.Probe && g.zone == ZoneGreen && g.stillTicks >= ditherIdleTicks && !g.breakerNow {
		xOut = math.Min(g.x+g.ditherSign*ditherStep, ceil)
		g.ditherSign = -g.ditherSign
	}
	g.xEff = xOut
	g.xHist = append(g.xHist, xOut)
	if len(g.xHist) > 64 {
		g.xHist = g.xHist[1:]
	}

	// 8. Lưới an toàn dao động trên x gốc (không tính dither).
	g.osc.observe(g.x, base)
	if g.osc.peakToPeak(4*g.dEff) > 0.3 || g.osc.negAutocorr() {
		g.kappaScale = math.Max(0.125, g.kappaScale*0.5)
	} else {
		g.kappaScale = math.Min(1, g.kappaScale*1.02)
	}

	// 9. Arbiter (§7 dòng 330) và metric (§11).
	g.arbiter(math.Exp(g.xEff))
	g.publish()
	return nil
}

// rawPressures: P_r thô = max(U_r/S_r, (g_r − 1)/(g_crit − 1)) (design §5.2).
func (g *GraceGovernor) rawPressures(sig DBSignals) map[string]float64 {
	slo := g.cfg.LagSLO.Seconds()
	out := map[string]float64{
		SignalReplicaLag: math.Max(0, sig.ReplicaLagSeconds/slo),
		SignalCommitLag:  math.Max(0, sig.CommitLagSeconds/slo),
	}
	if gCrit := g.cfg.Breaker.cfg.MaxRatio; sig.QueryBaseline > 0 && sig.QueryP99 > 0 && gCrit > 1 {
		gr := float64(sig.QueryP99) / float64(sig.QueryBaseline)
		out[SignalStrandP99] = math.Max(0, (gr-1)/(gCrit-1))
	} else {
		out[SignalStrandP99] = 0
	}
	return out
}

// confirmed: tín hiệu mềm vượt ngưỡng ở ít nhất 2 trong 3 tick gần nhất (§7 dòng 309).
func (g *GraceGovernor) confirmed(th float64) bool {
	n := 0
	for _, v := range g.pHist {
		if v >= th {
			n++
		}
	}
	return n >= softConfirmNeed
}

// nextZone: hysteresis §7 dòng 320.
func (g *GraceGovernor) nextZone() Zone {
	if g.hard || g.confirmed(thresholdRedEntry) {
		g.lowTicks = 0
		return ZoneRed
	}
	switch g.zone {
	case ZoneRed:
		if g.p < thresholdRedExit && g.cooldown == 0 {
			g.lowTicks = 0
			if g.p < thresholdYellowToGreen {
				g.lowTicks = 1
			}
			return ZoneYellow
		}
		return ZoneRed
	case ZoneYellow:
		if g.p < thresholdYellowToGreen {
			g.lowTicks++
			if g.lowTicks >= yellowToGreenTicks {
				g.lowTicks = 0
				return ZoneGreen
			}
		} else {
			g.lowTicks = 0
		}
		return ZoneYellow
	default:
		if g.confirmed(thresholdGreenToYellow) {
			return ZoneYellow
		}
		return ZoneGreen
	}
}

// setpoint: P* = 0,85, hoặc 1,0 khi P_lag ≥ 1 (§7 dòng 325).
func (g *GraceGovernor) setpoint() float64 {
	if g.pLag >= 1 {
		return setpointLag
	}
	return setpointNormal
}

// gain: k = κ / max(ŝ, s_min); chưa đo được thì dùng ŝ thận trọng (§7 dòng 325, 328).
func (g *GraceGovernor) gain() float64 {
	return g.kappaScale * Kappa(g.dEff) / g.gainS()
}

// gainS = max(ŝ, s_min); chưa đo được thì không nhỏ hơn ŝ thận trọng.
func (g *GraceGovernor) gainS() float64 {
	s := math.Max(g.sens.Estimate(), sMin)
	if g.sens.Unknown() {
		s = math.Max(s, sConservative)
	}
	return s
}

// floorX: xMin, hoặc sàn foreground khi P_lag ≥ 1 (§7 dòng 318); sàn vượt hard → giữ hard,
// báo lag_unsustainable (§7 dòng 330).
func (g *GraceGovernor) floorX() float64 {
	f := g.xMin
	g.lagUnsustainable = false
	if g.pLag >= 1 && g.fgFloor > 0 {
		f = math.Max(f, math.Log(g.fgFloor))
		if f > g.xHard {
			g.lagUnsustainable = true
			f = g.xHard
		}
	}
	return f
}

// ceilX: soft limit; bootstrap với P_lag ≥ 1 được lên tới hard limit (tăng ưu tiên snapshot/prefetch,
// vẫn trong hard limit, §7 dòng 317); sàn foreground cũng có thể đẩy tới hard.
func (g *GraceGovernor) ceilX() float64 {
	c := g.xSoft
	if g.pLag >= 1 && g.phase == "bootstrap" {
		c = g.xHard
	}
	if g.pLag >= 1 && g.fgFloor > 0 {
		c = math.Max(c, math.Min(math.Log(g.fgFloor), g.xHard))
	}
	return math.Min(c, g.xHard)
}

// updateExternal: pressure_external khi ŝ ≈ 0 với độ tin cậy cao hơn 5 phút (§7 dòng 337).
// Thiếu kích thích (hiệp phương sai lớn dần) không phủ nhận bằng chứng cũ nên không reset đồng hồ;
// chỉ bằng chứng ngược (ŝ ≥ s_min tin cậy) hoặc áp suất hết (P < 0,90) mới reset.
func (g *GraceGovernor) updateExternal(now time.Time) {
	switch {
	case g.sens.ConfidentLarge() || g.p < thresholdRedExit:
		g.smallSince = time.Time{}
	case g.sens.ConfidentSmall() && g.smallSince.IsZero():
		g.smallSince = now
	}
	g.pressureExternal = !g.smallSince.IsZero() && now.Sub(g.smallSince) > externalPressureAfter
}

// blindTick: mù metric (design §5.1 hàng 14): >3 tick đóng băng tăng (x giữ nguyên),
// >10 tick R giảm một nửa nhưng không dưới sàn foreground.
func (g *GraceGovernor) blindTick() {
	g.blindTicks++
	g.rawHist = nil // không ghép sai phân P qua khoảng mù
	if g.blindTicks > blindCutTicks {
		g.x = math.Max(g.x+math.Log(0.5), math.Min(g.floorX(), g.ceilX()))
		g.xEff = g.x
		g.arbiter(math.Exp(g.xEff))
	}
	g.xHist = append(g.xHist, g.xEff)
	if len(g.xHist) > 64 {
		g.xHist = g.xHist[1:]
	}
}

// arbiter: foreground đủ nhu cầu trước, background chia phần còn lại theo weight (§7 dòng 330),
// cộng luật P_lag theo pha (§7 dòng 314–318).
func (g *GraceGovernor) arbiter(r float64) {
	g.bootstrapRetentionRisk = g.pLag >= 1 && g.phase == "bootstrap"
	switch {
	case g.breakerNow: // breaker: background dừng (§7 dòng 334)
		g.fgRate, g.bgRate = r, 0
	case g.pLag >= 1 && g.phase != "bootstrap": // synced: background 0, foreground giữ sàn
		g.fgRate, g.bgRate = r, 0
	case g.bootstrapRetentionRisk: // bootstrap: tăng ưu tiên background, không cắt về 0
		fg := math.Min(math.Max(g.fgDemand, 0), r*(1-bootstrapBgShare))
		g.fgRate, g.bgRate = fg, r-fg
	case g.fgDemand > 0:
		fg := math.Min(g.fgDemand, r)
		g.fgRate, g.bgRate = fg, r-fg
	default: // chưa biết nhu cầu foreground: foreground không bị giữ chỗ, background nhận cả R
		g.fgRate, g.bgRate = r, r
	}
}

func (g *GraceGovernor) publish() {
	m := g.cfg.Metrics
	if m == nil {
		return
	}
	id := g.cfg.PipelineID
	m.GovernorZone.WithLabelValues(id).Set(float64(g.zone))
	for _, n := range softSignalNames {
		m.GovernorPressure.WithLabelValues(id, n).Set(g.sigP[n])
	}
	m.GovernorPressure.WithLabelValues(id, "total").Set(g.p)
	m.GovernorPressure.WithLabelValues(id, "lag").Set(g.pLag)
	m.GovernorRateTokens.WithLabelValues(id, "source_iops").Set(math.Exp(g.xEff))
	m.BreakerOpen.WithLabelValues(id).Set(b2f(g.breakerNow))
	m.SensitivityUnknown.WithLabelValues(id).Set(b2f(g.sens.Unknown()))
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// --- interface Governor ---

func (g *GraceGovernor) CurrentRate() (float64, float64) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.fgRate, g.bgRate
}

func (g *GraceGovernor) Zone() Zone {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.zone
}

func (g *GraceGovernor) Pressure() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.p
}

func (g *GraceGovernor) BreakerOpen() bool { return g.cfg.Breaker.IsOpen() }

func (g *GraceGovernor) RecordQueryLatency(d time.Duration) {
	switch r := g.cfg.SignalReader.(type) {
	case *MySQLDBSignalReader:
		r.latencyTracker.Record(d)
	case *PGDBSignalReader:
		r.latencyTracker.Record(d)
	case *SimulatedSignalReader:
		r.latencyTracker.Record(d)
	}
}

// SetSoftRate đổi soft limit nóng; x bị kẹp về trần mới ở tick sau.
func (g *GraceGovernor) SetSoftRate(r float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r > 0 {
		g.cfg.SoftRate = r
		g.xSoft = math.Min(math.Log(r), g.xHard)
	}
}

func (g *GraceGovernor) BackgroundRunning() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.bgRate > 0
}

func (g *GraceGovernor) SensitivityUnknown() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.sens.Unknown()
}

// --- đầu vào từ pipeline ---

// SetPhase: "bootstrap" (trước synced) hoặc "synced" (§7 dòng 314–318).
func (g *GraceGovernor) SetPhase(phase string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.phase = phase
}

// SetLagPressure đặt P_lag đã tính theo pha (lag/lag_slo, WAL, binlog còn lại; §7 dòng 312).
func (g *GraceGovernor) SetLagPressure(pLag float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.pLag = pLag
}

// SetForegroundDemand: nhu cầu token/s của foreground (streaming, catch-up).
func (g *GraceGovernor) SetForegroundDemand(v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fgDemand = v
}

// SetForegroundFloor: mức token/s đủ để lag không tăng thêm (áp dụng khi P_lag ≥ 1).
func (g *GraceGovernor) SetForegroundFloor(v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.fgFloor = v
}

// SetWindowSaturated: cửa sổ L đang bị dùng hết (Green chỉ tăng khi true).
func (g *GraceGovernor) SetWindowSaturated(v bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.windowSaturated = v
}

// WindowSaturated: giá trị SetWindowSaturated gần nhất (mặc định true).
func (g *GraceGovernor) WindowSaturated() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.windowSaturated
}

// SetBackgroundWeights: weight của từng tác vụ background (snapshot, verify, prefetch...).
func (g *GraceGovernor) SetBackgroundWeights(w map[string]float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.bgWeights = map[string]float64{}
	for k, v := range w {
		if v > 0 {
			g.bgWeights[k] = v
		}
	}
}

// BackgroundShare: phần R background của một tác vụ theo weight (§7 dòng 330).
func (g *GraceGovernor) BackgroundShare(name string) float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if len(g.bgWeights) == 0 {
		return g.bgRate
	}
	var sum float64
	for _, v := range g.bgWeights {
		sum += v
	}
	return g.bgRate * g.bgWeights[name] / sum
}

// --- trạng thái / cảnh báo ---

func (g *GraceGovernor) BootstrapRetentionRisk() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.bootstrapRetentionRisk
}
func (g *GraceGovernor) LagUnsustainable() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.lagUnsustainable
}
func (g *GraceGovernor) PressureExternal() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.pressureExternal
}
func (g *GraceGovernor) LagPressure() float64 { g.mu.RLock(); defer g.mu.RUnlock(); return g.pLag }
func (g *GraceGovernor) Phase() string        { g.mu.RLock(); defer g.mu.RUnlock(); return g.phase }
func (g *GraceGovernor) SensitivityEstimate() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.sens.Estimate()
}
func (g *GraceGovernor) Cooldown() int { g.mu.RLock(); defer g.mu.RUnlock(); return g.cooldown }
func (g *GraceGovernor) WMax() float64 { g.mu.RLock(); defer g.mu.RUnlock(); return math.Exp(g.wmax) }

// EffectiveDelay: d dùng cho κ và cooldown (trễ plant + trễ bộ lọc).
func (g *GraceGovernor) EffectiveDelay() int { return g.dEff }

// TotalRate: R = exp(x) đang xuất (gồm dither).
func (g *GraceGovernor) TotalRate() float64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return math.Exp(g.xEff)
}

// BaseRate: R gốc exp(x), không tính dither.
func (g *GraceGovernor) BaseRate() float64 { g.mu.RLock(); defer g.mu.RUnlock(); return math.Exp(g.x) }

// --- đại lượng suy ra từ R (§7 dòng 304–307) ---

// ConcurrencyWindow: L = ⌈R · T_op · 1,25⌉, chặn bởi limit (trần deadlock / writeset; ≤0 = không chặn).
func ConcurrencyWindow(r float64, top time.Duration, limit int) int {
	l := int(math.Ceil(r * top.Seconds() * 1.25))
	if l < 1 {
		l = 1
	}
	if limit > 0 && l > limit {
		l = limit
	}
	return l
}

// NextChunkRows: rows ← rows · clamp(250 ms / thời gian chunk trước, 0,5, 2), rồi chi phí ước lượng
// rows·tokensPerRow ≤ R · 0,25 s (tokensPerRow ≤ 0 hoặc r ≤ 0 = bỏ qua ràng buộc chi phí).
func NextChunkRows(rows int, last time.Duration, tokensPerRow, r float64) int {
	if rows < 1 {
		rows = 1
	}
	next := float64(rows)
	if last > 0 {
		next *= clamp(float64(250*time.Millisecond)/float64(last), 0.5, 2)
	}
	if tokensPerRow > 0 && r > 0 {
		next = math.Min(next, math.Floor(MaxChunkCostTokens(r)/tokensPerRow))
	}
	if next < 1 {
		return 1
	}
	return int(next)
}

// MaxChunkCostTokens: chi phí tối đa của một chunk = R · 0,25 s.
func MaxChunkCostTokens(r float64) float64 { return r * 0.25 }

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
