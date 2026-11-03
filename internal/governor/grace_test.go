package governor_test

import (
	"context"
	"math"
	"testing"
	"time"

	"strand/internal/governor"
)

// plant mô phỏng DB: trả P (qua ReplicaLagSeconds = P · lag_slo) tính từ lịch sử R của governor.
type plant struct {
	slo   time.Duration
	rates []float64 // R sau mỗi tick (x_t)
	pFn   func(rates []float64) float64
	hard  bool
	lastP float64
}

func (p *plant) ReadSignals(ctx context.Context) (governor.DBSignals, error) {
	p.lastP = p.pFn(p.rates)
	return governor.DBSignals{ReplicaLagSeconds: p.lastP * p.slo.Seconds(), HardSignal: p.hard}, nil
}

// delayed trả R của d tick trước (x_{t−d}); thiếu lịch sử thì dùng r0.
func delayed(rates []float64, d int, r0 float64) float64 {
	if i := len(rates) - 1 - d; i >= 0 {
		return rates[i]
	}
	return r0
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func newGov(t *testing.T, pl *plant, cfg governor.GraceConfig) (*governor.GraceGovernor, func()) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	if cfg.SoftRate == 0 {
		cfg.SoftRate = 1000
	}
	if cfg.HardRate == 0 {
		cfg.HardRate = 2000
	}
	cfg.LagSLO = pl.slo
	cfg.Tick = time.Second
	cfg.SignalReader = pl
	cfg.Clock = clk.now
	if cfg.Breaker == nil {
		cfg.Breaker = governor.NewBreaker(governor.BreakerConfig{CooldownTicks: 1})
	}
	g := governor.NewGraceGovernor(cfg)
	pl.rates = append(pl.rates, g.TotalRate())
	step := func() {
		if err := g.Tick(context.Background()); err != nil {
			t.Fatalf("Tick: %v", err)
		}
		pl.rates = append(pl.rates, g.TotalRate())
		clk.t = clk.t.Add(time.Second)
	}
	return g, step
}

// AC1: luật 3 zone. Chuỗi P cho trước; mô hình tham chiếu độc lập tính zone kỳ vọng từ P đã lọc
// (governor.Pressure()) theo §7 dòng 320 và so từng tick; kiểm β, cooldown d+1, dead band.
func TestGV_AC1_ThreeZoneRules_Hysteresis_Beta_DeadBand(t *testing.T) {
	type seg struct {
		p     float64
		ticks int
		hard  bool
	}
	script := []seg{
		{0.2, 6, false}, {0.8, 12, false}, {0.84, 12, false}, {0.3, 15, false},
		{1.6, 25, false}, {0.3, 30, false}, {0.3, 1, true}, {0.3, 10, false},
	}
	var seq []seg
	for _, s := range script {
		for i := 0; i < s.ticks; i++ {
			seq = append(seq, seg{s.p, 1, s.hard})
		}
	}
	idx := 0
	pl := &plant{slo: 5 * time.Second}
	pl.pFn = func([]float64) float64 { return seq[idx].p }
	g, step := newGov(t, pl, governor.GraceConfig{DelayTicks: 1, Probe: false})
	d := g.EffectiveDelay()

	// Mô hình tham chiếu.
	zone := governor.ZoneGreen
	var hist []float64
	low := 0
	sawYellow, sawRed, sawGreenAgain, sawDeadBand, sawHard := false, false, false, false, false
	mdTicks := []int{}
	for idx = 0; idx < len(seq); idx++ {
		pl.hard = seq[idx].hard
		cdBefore := g.Cooldown()
		rBefore := g.BaseRate()
		zBefore := g.Zone()
		step()
		p := g.Pressure()
		hist = append(hist, p)
		if len(hist) > 3 {
			hist = hist[1:]
		}
		count := func(th float64) int {
			n := 0
			for _, v := range hist {
				if v >= th {
					n++
				}
			}
			return n
		}
		switch {
		case pl.hard || count(1.0) >= 2:
			zone, low = governor.ZoneRed, 0
		case zone == governor.ZoneRed:
			if p < 0.90 && cdBefore == 0 {
				zone, low = governor.ZoneYellow, 0
				if p < 0.60 {
					low = 1
				}
			}
		case zone == governor.ZoneYellow:
			if p < 0.60 {
				low++
				if low >= 3 {
					zone, low = governor.ZoneGreen, 0
				}
			} else {
				low = 0
			}
		default:
			if count(0.70) >= 2 {
				zone = governor.ZoneYellow
			}
		}
		if pl.hard {
			// Breaker cũng mở do tín hiệu cứng: zone vẫn Red; giữ mô hình đồng bộ.
			zone = governor.ZoneRed
		}
		if g.Zone() != zone {
			t.Fatalf("tick %d: P=%.3f zone=%v, kỳ vọng %v", idx, p, g.Zone(), zone)
		}
		ratio := g.BaseRate() / rBefore
		switch zone {
		case governor.ZoneYellow:
			sawYellow = true
			if zBefore == governor.ZoneYellow && math.Abs(0.85-p) < 0.05 {
				sawDeadBand = true
				if math.Abs(ratio-1) > 1e-12 {
					t.Fatalf("tick %d: dead band |e|<0,05 nhưng R đổi %v", idx, ratio)
				}
			}
			if zBefore == governor.ZoneYellow && 0.85-p >= 0.05 && ratio <= 1 && g.BaseRate() < 1000-1e-9 {
				t.Fatalf("tick %d: e>0 ở Yellow phải tăng R", idx)
			}
			if zBefore == governor.ZoneYellow && 0.85-p <= -0.05 && ratio >= 1 {
				t.Fatalf("tick %d: e<0 ở Yellow phải giảm R", idx)
			}
		case governor.ZoneRed:
			sawRed = true
			if ratio < 1-1e-12 {
				want := governor.Beta(p, pl.hard)
				if math.Abs(ratio-want) > 1e-9 {
					t.Fatalf("tick %d: β=%v, kỳ vọng clamp(1−0,5(P−1),0,5,0,85)=%v", idx, ratio, want)
				}
				if pl.hard {
					sawHard = true
					if math.Abs(ratio-0.5) > 1e-9 {
						t.Fatalf("tín hiệu cứng phải β=0,5, có %v", ratio)
					}
				}
				if math.Abs(g.WMax()-rBefore) > 1e-6 {
					t.Fatalf("W_max phải ghi = R trước khi giảm")
				}
				mdTicks = append(mdTicks, idx)
			}
		case governor.ZoneGreen:
			if sawRed {
				sawGreenAgain = true
			}
		}
	}
	if !sawYellow || !sawRed || !sawGreenAgain || !sawDeadBand || !sawHard {
		t.Fatalf("chuỗi chưa phủ đủ: yellow=%v red=%v green=%v deadband=%v hard=%v", sawYellow, sawRed, sawGreenAgain, sawDeadBand, sawHard)
	}
	// Hai lần MD liên tiếp trong cùng đợt Red cách nhau đúng d+2 tick (cooldown d+1 tick).
	gaps := 0
	for i := 1; i < len(mdTicks); i++ {
		if gap := mdTicks[i] - mdTicks[i-1]; gap < d+2 {
			t.Fatalf("MD tại %v: khoảng %d < cooldown d+1=%d", mdTicks, gap, d+1)
		} else if gap == d+2 {
			gaps++
		}
	}
	if gaps == 0 {
		t.Fatalf("không thấy MD lặp sau đúng cooldown d+1: %v", mdTicks)
	}
	if math.Abs(governor.Kappa(1)-0.5) > 1e-12 || math.Abs(governor.Kappa(3)-0.2225) > 1e-3 {
		t.Fatalf("κ sai: κ(1)=%v κ(3)=%v", governor.Kappa(1), governor.Kappa(3))
	}
	if governor.Beta(1.1, false) != 0.85 || governor.Beta(1.4, false) != 0.8 || governor.Beta(3, false) != 0.5 {
		t.Fatal("β clamp sai")
	}
}

// AC2: P_lag theo pha.
func TestGV_AC2_LagPressureByPhase(t *testing.T) {
	run := func(phase string, floor float64) *governor.GraceGovernor {
		pl := &plant{slo: 5 * time.Second, pFn: func([]float64) float64 { return 0.3 }}
		g, step := newGov(t, pl, governor.GraceConfig{DelayTicks: 1})
		g.SetPhase(phase)
		g.SetForegroundDemand(300)
		g.SetForegroundFloor(floor)
		g.SetLagPressure(1.5)
		for i := 0; i < 20; i++ {
			step()
		}
		return g
	}
	// synced + P_lag ≥ 1: background 0, foreground giữ sàn.
	g := run("synced", 1500)
	fg, bg := g.CurrentRate()
	if bg != 0 || g.BackgroundRunning() {
		t.Fatalf("synced P_lag≥1: background phải 0, có %v", bg)
	}
	if fg < 1500-1e-6 {
		t.Fatalf("synced P_lag≥1: foreground phải ≥ sàn 1500, có %v", fg)
	}
	if g.BootstrapRetentionRisk() {
		t.Fatal("synced không báo bootstrap_retention_risk")
	}
	// bootstrap + P_lag ≥ 1: không cắt background, báo bootstrap_retention_risk, vẫn ≤ hard.
	g = run("bootstrap", 0)
	fg, bg = g.CurrentRate()
	if bg <= 0 || !g.BackgroundRunning() {
		t.Fatalf("bootstrap P_lag≥1: background không được về 0 (fg=%v bg=%v)", fg, bg)
	}
	if bg < 0.5*g.TotalRate()-1e-9 {
		t.Fatalf("bootstrap P_lag≥1: background phải được ưu tiên (≥50%% R), bg=%v R=%v", bg, g.TotalRate())
	}
	if !g.BootstrapRetentionRisk() {
		t.Fatal("bootstrap P_lag≥1 phải báo bootstrap_retention_risk")
	}
	if g.TotalRate() > 2000+1e-9 || g.TotalRate() <= 1000 {
		t.Fatalf("bootstrap P_lag≥1: R phải tăng quá soft nhưng ≤ hard, R=%v", g.TotalRate())
	}
	// Sàn foreground vượt hard → giữ hard, lag_unsustainable.
	g = run("synced", 5000)
	if !g.LagUnsustainable() || math.Abs(g.TotalRate()-2000) > 1e-6 {
		t.Fatalf("sàn > hard: phải giữ hard và báo lag_unsustainable (R=%v flag=%v)", g.TotalRate(), g.LagUnsustainable())
	}
	// P_lag < 1: foreground đủ nhu cầu trước, background phần còn lại theo weight.
	g = run("synced", 0)
	g.SetLagPressure(0.2)
	g.SetBackgroundWeights(map[string]float64{"snapshot": 3, "verify": 1})
	_ = g.Tick(context.Background())
	fg, bg = g.CurrentRate()
	if math.Abs(fg-300) > 1e-6 || math.Abs(fg+bg-g.TotalRate()) > 1e-6 {
		t.Fatalf("arbiter: fg=%v bg=%v R=%v", fg, bg, g.TotalRate())
	}
	if s, v := g.BackgroundShare("snapshot"), g.BackgroundShare("verify"); math.Abs(s-3*v) > 1e-6 || math.Abs(s+v-bg) > 1e-6 {
		t.Fatalf("chia background theo weight sai: %v %v", s, v)
	}
}

// AC3: không livelock: snapshot PG (stream chỉ apply sau snapshot nên lag tăng suốt snapshot,
// và tăng thêm theo R), lag_slo nhỏ. Bootstrap phải vẫn cấp token cho background mọi tick, R ≤ hard.
func TestGV_AC3_NoLivelock_PGSnapshotWithSmallLagSLO(t *testing.T) {
	const soft, hard = 1000.0, 2000.0
	lagSLO := 1 * time.Second
	pl := &plant{slo: 5 * time.Second}
	// Áp suất source tỷ lệ với R của Strand (trễ 1 tick).
	pl.pFn = func(r []float64) float64 { return 0.45 * delayed(r, 1, soft) / soft }
	g, step := newGov(t, pl, governor.GraceConfig{SoftRate: soft, HardRate: hard, DelayTicks: 1, Probe: true})
	g.SetPhase("bootstrap")
	lag := 0.0
	var bgTokens, windowTokens float64
	for i := 0; i < 600; i++ {
		g.SetLagPressure(lag / lagSLO.Seconds())
		step()
		_, bg := g.CurrentRate()
		if bg <= 0 {
			t.Fatalf("tick %d: background bị cắt về 0 trong bootstrap (livelock)", i)
		}
		if g.TotalRate() > hard+1e-9 {
			t.Fatalf("tick %d: R=%v vượt hard", i, g.TotalRate())
		}
		bgTokens += bg
		windowTokens += bg
		if i%50 == 49 {
			if windowTokens <= 0 {
				t.Fatalf("snapshot không tiến trong cửa sổ kết thúc ở tick %d", i)
			}
			windowTokens = 0
		}
		lag += 1 + 0.0005*bg // lag tăng theo thời gian và theo R
	}
	if !g.BootstrapRetentionRisk() || bgTokens <= 0 {
		t.Fatalf("phải báo bootstrap_retention_risk và snapshot phải tiến (bg tokens=%v)", bgTokens)
	}
	// Đối chứng: cùng plant ở synced thì background về 0 (lý do bootstrap phải có luật riêng).
	g.SetPhase("synced")
	step()
	if _, bg := g.CurrentRate(); bg != 0 {
		t.Fatalf("synced P_lag≥1 phải cắt background, có %v", bg)
	}
}

// AC4: strand_sensitivity_unknown.
func TestGV_AC4_SensitivityUnknown(t *testing.T) {
	// (a) plant không phản ứng với R (áp suất ngoài, P = 0,95): Yellow liên tục giảm từng bước nhỏ,
	// kích thích không đủ → cờ bật; hành động giảm được giữ (R giảm đơn điệu, không dừng ở polite floor).
	// probe: on — dither chỉ có ở vùng Xanh nên ở (a) (vùng Vàng) không có kích thích dither.
	pl := &plant{slo: 5 * time.Second, pFn: func([]float64) float64 { return 0.95 }}
	g, step := newGov(t, pl, governor.GraceConfig{DelayTicks: 1, Probe: true})
	prev := g.BaseRate()
	for i := 0; i < 120; i++ {
		step()
		if g.BaseRate() > prev+1e-9 {
			t.Fatalf("tick %d: plant không phản ứng nhưng R tăng (%v → %v)", i, prev, g.BaseRate())
		}
		prev = g.BaseRate()
	}
	if !g.SensitivityUnknown() {
		t.Fatalf("plant không phản ứng: strand_sensitivity_unknown phải = 1 (ŝ=%v)", g.SensitivityEstimate())
	}
	if g.BaseRate() >= 1000*0.9 || g.PressureExternal() {
		t.Fatalf("phải giữ hành động giảm: R=%v pressure_external=%v", g.BaseRate(), g.PressureExternal())
	}
	start := g.BaseRate()
	for i := 0; i < 20; i++ {
		step()
	}
	if g.BaseRate() >= start {
		t.Fatalf("chưa đo được ŝ thì vẫn phải tiếp tục giảm: %v → %v", start, g.BaseRate())
	}

	// (b) cùng governor, plant trở nên phản ứng: P = 0,5 + 1,0·(log R − log 1000) trễ 1 tick
	// (kẹp ≥ 0). Áp suất về vùng Xanh, R tăng lại tới trần rồi đứng yên → dither ±1% là kích thích
	// → cờ tắt.
	ref := math.Log(1000)
	pl.pFn = func(r []float64) float64 {
		return math.Max(0, 0.5+1.0*(math.Log(delayed(r, 1, 1000))-ref))
	}
	g.SetWindowSaturated(true)
	off := -1
	for i := 0; i < 400; i++ {
		step()
		if !g.SensitivityUnknown() && off < 0 {
			off = i
		}
	}
	if off < 0 || g.SensitivityUnknown() || g.Zone() != governor.ZoneGreen {
		t.Fatalf("plant phản ứng: cờ phải tắt (off tại %d, ŝ=%v, zone=%v)", off, g.SensitivityEstimate(), g.Zone())
	}
	if s := g.SensitivityEstimate(); s < 0.5 || s > 1.5 {
		t.Fatalf("ŝ phải xấp xỉ 1, có %v", s)
	}
	// (c) probe: off và plant phản ứng nhưng R đứng yên: không có kích thích → hiệp phương sai
	// lớn dần → cờ bật trở lại (chưa đo được, không phải "ŝ nhỏ").
	pl2 := &plant{slo: 5 * time.Second}
	pl2.pFn = func(r []float64) float64 { return 0.5 + (math.Log(delayed(r, 1, 1000)) - ref) }
	g2, step2 := newGov(t, pl2, governor.GraceConfig{DelayTicks: 1, Probe: false})
	for i := 0; i < 200; i++ {
		step2()
	}
	if !g2.SensitivityUnknown() || g2.PressureExternal() {
		t.Fatalf("probe off, không kích thích: cờ unknown phải bật, không pressure_external")
	}
}

// pressure_external: plant không phản ứng, kích thích mạnh (β = 0,5 vì P lớn) → ŝ ≈ 0 tin cậy;
// sau > 5 phút báo pressure_external và không giảm dưới polite floor.
func TestGV_PressureExternal(t *testing.T) {
	pl := &plant{slo: 5 * time.Second, pFn: func([]float64) float64 { return 2.0 }}
	g, step := newGov(t, pl, governor.GraceConfig{DelayTicks: 0, Probe: false, MinRate: 1, PoliteFloor: 100})
	for i := 0; i < 400; i++ {
		step()
	}
	if !g.PressureExternal() {
		t.Fatalf("phải báo pressure_external (ŝ=%v unknown=%v)", g.SensitivityEstimate(), g.SensitivityUnknown())
	}
}

// Ổn định: plant bậc nhất có trễ d tick → x hội tụ, dao động biên độ < 5% sau 100 tick.
func TestGV_Stability_FirstOrderPlantWithDelay(t *testing.T) {
	for _, d := range []int{0, 1, 2, 3} {
		for _, s := range []float64{0.5, 1, 2} {
			pl := &plant{slo: 5 * time.Second}
			x0 := math.Log(1000)
			pp := 1.2
			pl.pFn = func(r []float64) float64 {
				target := 1.2 + s*(math.Log(delayed(r, d, 1000))-x0)
				pp = 0.6*pp + 0.4*target // bậc nhất, hằng số thời gian ≈ 2 tick
				return math.Max(0, pp)
			}
			g, step := newGov(t, pl, governor.GraceConfig{DelayTicks: d, Probe: true})
			lo, hi := math.Inf(1), math.Inf(-1)
			for i := 0; i < 300; i++ {
				step()
				if i >= 100 {
					lo, hi = math.Min(lo, g.TotalRate()), math.Max(hi, g.TotalRate())
				}
			}
			if amp := hi/lo - 1; amp >= 0.05 {
				t.Fatalf("d=%d s=%v: dao động %.2f%% ≥ 5%% (R ∈ [%.1f, %.1f], zone=%v)", d, s, amp*100, lo, hi, g.Zone())
			}
			if p := pl.lastP; p >= 1.0 {
				t.Fatalf("d=%d s=%v: không hội tụ dưới Red, P=%v", d, s, p)
			}
			t.Logf("d=%d s=%v: R hội tụ trong [%.1f, %.1f], P=%.3f zone=%v ŝ=%.2f", d, s, lo, hi, pl.lastP, g.Zone(), g.SensitivityEstimate())
		}
	}
}

// Breaker: cắt 50% đúng một lần khi kích hoạt, background dừng, không cắt dây chuyền khi đang mở.
func TestGV_BreakerCutsOnce(t *testing.T) {
	pl := &plant{slo: 5 * time.Second, pFn: func([]float64) float64 { return 0.2 }}
	br := governor.NewBreaker(governor.BreakerConfig{MaxReplicaLag: 3 * time.Second, CooldownTicks: 5})
	g, step := newGov(t, pl, governor.GraceConfig{DelayTicks: 1, Probe: false, Breaker: br})
	step()
	r0 := g.BaseRate()
	pl.pFn = func([]float64) float64 { return 0.8 } // replica lag 4s > 3s → breaker mở
	step()
	if !g.BreakerOpen() || math.Abs(g.BaseRate()/r0-0.5) > 1e-9 {
		t.Fatalf("breaker phải mở và cắt 50%%: open=%v ratio=%v", g.BreakerOpen(), g.BaseRate()/r0)
	}
	if _, bg := g.CurrentRate(); bg != 0 {
		t.Fatal("breaker mở: background phải dừng")
	}
	r1 := g.BaseRate()
	for i := 0; i < 5; i++ {
		step()
	}
	if g.BaseRate() < r1-1e-9 {
		t.Fatalf("breaker đang mở không được cắt thêm khi P < 1: %v → %v", r1, g.BaseRate())
	}
	pl.pFn = func([]float64) float64 { return 0.2 }
	for i := 0; i < 6; i++ {
		step()
	}
	if g.BreakerOpen() {
		t.Fatal("breaker phải đóng sau cooldown")
	}
	if math.Abs(g.BaseRate()-r1) > r1*0.11 {
		t.Fatalf("khởi động lại bumpless từ R hiện tại: %v → %v", r1, g.BaseRate())
	}
}

func TestGV_ChunkAndConcurrencyWindow(t *testing.T) {
	if l := governor.ConcurrencyWindow(1000, 10*time.Millisecond, 0); l != 13 {
		t.Fatalf("L = ⌈1000·0,01·1,25⌉ = 13, có %d", l)
	}
	if l := governor.ConcurrencyWindow(1000, 10*time.Millisecond, 4); l != 4 {
		t.Fatalf("L bị chặn bởi trần 4, có %d", l)
	}
	if r := governor.NextChunkRows(1000, 500*time.Millisecond, 0, 0); r != 500 {
		t.Fatalf("chunk chậm 2x → 0,5x, có %d", r)
	}
	if r := governor.NextChunkRows(1000, 50*time.Millisecond, 0, 0); r != 2000 {
		t.Fatalf("chunk nhanh → kẹp 2x, có %d", r)
	}
	if r := governor.NextChunkRows(1000, 250*time.Millisecond, 0.5, 400); r != 200 {
		t.Fatalf("chi phí chunk ≤ R·0,25s = 100 token → 200 rows, có %d", r)
	}
}
