package e2e

// Gate 2 (G2-9, 02 §7, rủi ro #9 của 01 §7): GRACE v2 với RLS chưa đủ dữ liệu bật
// `strand_sensitivity_unknown` = 1 (đọc giá trị thật từ /metrics, không chỉ kiểm metric tồn tại), và tắt
// về 0 khi có đủ kích thích.

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// scrapeMetric đọc /metrics và trả giá trị của dòng đầu tiên có tên name (đúng tên, có label) chứa mọi
// chuỗi trong match (vd. `pipeline="x"`). ok = false khi không có dòng nào khớp.
func scrapeMetric(addr, name string, match ...string) (float64, bool, error) {
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://" + addr + "/metrics")
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, err
	}
lines:
	for _, l := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(l, name+"{") {
			continue
		}
		for _, m := range match {
			if !strings.Contains(l, m) {
				continue lines
			}
		}
		i := strings.LastIndexByte(l, ' ')
		v, err := strconv.ParseFloat(strings.TrimSpace(l[i+1:]), 64)
		if err != nil {
			return 0, false, fmt.Errorf("parse %q: %w", l, err)
		}
		return v, true, nil
	}
	return 0, false, nil
}

// graceSample là một lần đọc các gauge của GRACE.
type graceSample struct {
	at                    time.Duration
	unknown, zone, tokens float64
}

func (s graceSample) String() string {
	return fmt.Sprintf("t=%.0fs unknown=%v zone=%v R=%.1f", s.at.Seconds(), s.unknown, s.zone, s.tokens)
}

// Gate 2 (G2-9): MySQL→MySQL với governor grace-v2 (source PG luôn dùng static nên GRACE chỉ chạy được
// với source MySQL), probe: off, lag_slo lớn, source không có replica lag: P ≈ 0 ở vùng Xanh, R đứng yên
// ở soft limit — không có kích thích nào, hiệp phương sai RLS lớn dần, nên GRACE ở đúng trạng thái
// "chưa đo được" (02 §7: giữ hành động giảm và bật strand_sensitivity_unknown).
//
//  1. Chưa đủ dữ liệu: ≥ 20 lần đọc liên tiếp (1 s/lần) đều thấy strand_sensitivity_unknown = 1 (onoff/static
//     luôn xuất 0, nên giá trị 1 chứng minh nhánh GRACE và trạng thái thiếu dữ liệu), R đứng yên ở soft.
//  2. Đủ dữ liệu: đổi soft limit nóng (`strand throttle set --soft`) qua lại 600 ↔ 300 mỗi 3 s — R bị kẹp
//     về trần mới rồi leo lại (CUBIC), là kích thích lớn trong khi P không đổi → RLS tin cậy ŝ ≈ 0 → cờ về 0
//     (mô phỏng với chính governor: ~45–60 tick).
//  3. Hết kích thích (soft cố định): hiệp phương sai lớn dần theo 1/λ → cờ bật lại (mô phỏng: ~36 tick).
//
// Cuối cùng pipeline vẫn đồng bộ đúng (0 lệch) — GRACE chỉ điều tiết, không làm sai dữ liệu.
func TestG2_E2E_GraceSensitivityUnknownMetric(t *testing.T) {
	seed := seedFromEnv(t)
	src, dst := newPair(t)
	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 2000)
	cfg := strandCfg{Name: "e2e-grace-unknown", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t), MetricsAddr: freeAddr(t),
		GovernorAlgo: "grace-v2", GovernorProbe: "off", LagSLO: "10m", SourceIOPS: 600}
	p := startStrand(t, cfg.write(t))
	waitSynced(t, &cfg, p, 3*time.Minute)
	start := time.Now()
	label := `pipeline="` + cfg.Name + `"`
	sample := func() graceSample {
		t.Helper()
		if p.exited() {
			t.Fatalf("strand exited (code %d)\n%s", p.exitCode(), p.tail())
		}
		s := graceSample{at: time.Since(start)}
		for _, m := range []struct {
			name  string
			extra []string
			dst   *float64
		}{
			{"strand_sensitivity_unknown", nil, &s.unknown},
			{"strand_governor_zone", nil, &s.zone},
			{"strand_governor_rate_tokens", []string{`signal="source_iops"`}, &s.tokens},
		} {
			v, ok, err := scrapeMetric(cfg.MetricsAddr, m.name, append([]string{label}, m.extra...)...)
			if err != nil || !ok {
				t.Fatalf("scrape %s{%s}: ok=%v err=%v", m.name, label, ok, err)
			}
			*m.dst = v
		}
		return s
	}
	var hist []graceSample
	history := func() string {
		var b []string
		for _, s := range hist {
			b = append(b, s.String())
		}
		if len(b) > 60 {
			b = b[len(b)-60:]
		}
		return strings.Join(b, "\n")
	}
	throttle := func(iops int) {
		t.Helper()
		out, code := runStrandCLI(t, 15*time.Second, "throttle", "set", "--source-iops", strconv.Itoa(iops), "--soft", "--socket", cfg.Socket)
		if code != 0 {
			t.Fatalf("throttle set --soft %d: exit %d\n%s", iops, code, out)
		}
	}

	// 1. Chưa đủ dữ liệu: chờ 20 lần đọc liên tiếp R đứng yên ở soft, vùng Xanh — tất cả phải là unknown = 1.
	// (Trong bootstrap có thể đã có kích thích, vd. tín hiệu cứng InnoDB → Red → bước MD; dữ liệu đó cũ đi
	// sau vài chục tick — mô phỏng: ≤ ~80 tick kể cả khi nhiễu đã về sàn — nên chỉ đếm khi R đã đứng yên, và
	// chỉ coi là lỗi khi R đứng yên ≥ 100 tick mà cờ vẫn 0.)
	steady, still := 0, 0
	for deadline := time.Now().Add(180 * time.Second); steady < 20; {
		s := sample()
		hist = append(hist, s)
		switch {
		case s.zone != 0 || math.Abs(s.tokens-600) > 0.5:
			steady, still = 0, 0
		case s.unknown != 1:
			if still++; still >= 100 {
				t.Fatalf("R steady at soft for %d scrapes without excitation but strand_sensitivity_unknown = %v\nhistory:\n%s\n%s", still, s.unknown, history(), p.tail())
			}
			steady = 0
		default:
			steady++
			still++
		}
		if time.Now().After(deadline) {
			t.Fatalf("never reached the insufficient-data state (20 scrapes: Green, R = 600, unknown = 1) in 180 s\nhistory:\n%s\n%s", history(), p.tail())
		}
		time.Sleep(time.Second)
	}
	t.Logf("insufficient data: strand_sensitivity_unknown = 1 on %d consecutive scrapes, zone Green, R steady at 600", steady)

	// 2. Kích thích bằng bước soft limit → đủ dữ liệu → 0.
	soft, lastToggle := 600, time.Time{}
	offAt := time.Duration(-1)
	for deadline := time.Now().Add(150 * time.Second); time.Now().Before(deadline); {
		if time.Since(lastToggle) >= 3*time.Second {
			soft = 900 - soft // 600 ↔ 300
			throttle(soft)
			lastToggle = time.Now()
		}
		s := sample()
		hist = append(hist, s)
		if s.unknown == 0 {
			offAt = s.at
			break
		}
		time.Sleep(time.Second)
	}
	if offAt < 0 {
		t.Fatalf("strand_sensitivity_unknown stayed 1 for 150 s of soft-limit steps 600↔300 (RLS never gained confidence)\nhistory:\n%s\n%s", history(), p.tail())
	}
	t.Logf("sufficient data after soft-limit steps: strand_sensitivity_unknown = 0 at %.0f s", offAt.Seconds())

	// 3. Hết kích thích → dữ liệu cũ dần → 1 lại.
	if soft != 600 {
		throttle(600)
	}
	stopAt := time.Since(start)
	onAt := time.Duration(-1)
	for deadline := time.Now().Add(150 * time.Second); time.Now().Before(deadline); {
		s := sample()
		hist = append(hist, s)
		if s.unknown == 1 {
			onAt = s.at
			break
		}
		time.Sleep(time.Second)
	}
	if onAt < 0 {
		t.Fatalf("strand_sensitivity_unknown stayed 0 for 150 s without excitation (covariance must grow back)\nhistory:\n%s\n%s", history(), p.tail())
	}
	t.Logf("no excitation again: strand_sensitivity_unknown back to 1 after %.0f s", (onAt - stopAt).Seconds())
	if strings.Contains(p.output.String(), `"code":"pressure_external"`) {
		t.Fatalf("pressure_external must not be raised: P ≈ 0 and sensitivity was not confidently small under pressure\n%s", p.tail())
	}

	wl.Start(2, 2000)
	time.Sleep(5 * time.Second)
	wl.Stop()
	waitCaughtUp(t, src, dst, &cfg, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("exit %d\n%s", code, p.tail())
	}
	compareFinal(t, src, dst, appTables...)
}
