package main

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type GovSummary struct {
	Runs                  int `json:"runs"`
	OKRuns                int `json:"ok_runs"`
	MedianSnapshotSeconds jf  `json:"median_snapshot_seconds"`
	MedianAppP99Ms        jf  `json:"median_app_p99_ms"`
	HardLimitExceedTotal  int `json:"hard_limit_exceed_total"`
	MaxP2PMedian          jf  `json:"max_p2p_log_r_median"`
	MaxP2P                jf  `json:"max_p2p_log_r"`
}

const (
	StatusGrace      = "grace-v2-default"
	StatusOnoff      = "onoff-default"
	StatusIncomplete = "incomplete"
)

// Verdict: Status chỉ là grace-v2-default / onoff-default khi bằng chứng ĐỦ; thiếu bằng chứng → incomplete
// (không có kết quả, không được hiểu là "GRACE không đạt").
type Verdict struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons"`
	Notes   []string `json:"notes,omitempty"`
}

var requiredGovernors = []string{"static", "onoff", "grace-v2"}

// latestAttempts: mỗi (governor, repeat) lấy lần thử cuối (chạy bù theo giao thức §4.5);
// các lần thử trước bị ghi vào notes, không bị loại âm thầm.
func latestAttempts(runs []RunResult) (map[string]map[int]RunResult, []string, []string) {
	best := map[string]map[int]RunResult{}
	var notes, dups []string
	for _, r := range runs {
		m := best[r.Governor]
		if m == nil {
			m = map[int]RunResult{}
			best[r.Governor] = m
		}
		if old, ok := m[r.Repeat]; ok {
			if old.Attempt == r.Attempt {
				dups = append(dups, fmt.Sprintf("%s#%d attempt %d xuất hiện hai lần", r.Governor, r.Repeat, r.Attempt))
			}
			prev, cur := old, r
			if cur.Attempt < prev.Attempt {
				prev, cur = cur, prev
			}
			notes = append(notes, fmt.Sprintf("%s#%d: attempt %d replaced by attempt %d (first: ok=%v %s)",
				r.Governor, r.Repeat, prev.Attempt, cur.Attempt, prev.OK, firstNonEmpty(prev.Evidence, prev.Error)))
			m[r.Repeat] = cur
			continue
		}
		m[r.Repeat] = r
	}
	return best, notes, dups
}

func firstNonEmpty(a ...string) string {
	for _, s := range a {
		if s != "" {
			return s
		}
	}
	return ""
}

// Decide áp quy tắc spec/04-testing §5 / spec/01-plan.md; ngưỡng không đổi: 0 vượt hard limit,
// grace p2p ≤ 0,3 (mọi cửa sổ), snapshot nhanh ≥ 30% so với onoff, p99 median không xấu hơn onoff.
func Decide(runs []RunResult, repeats int) (map[string]GovSummary, Verdict) {
	best, notes, dups := latestAttempts(runs)
	sum := map[string]GovSummary{}
	snaps, p99s := map[string][]float64{}, map[string][]float64{}
	incomplete := append([]string(nil), dups...)
	var manifest string
	for _, gov := range requiredGovernors {
		reps := best[gov]
		if len(reps) != repeats {
			incomplete = append(incomplete, fmt.Sprintf("%s: cần đúng %d lần đo độc lập, có %d", gov, repeats, len(reps)))
		}
		var keys []int
		for k := range reps {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, k := range keys {
			r := reps[k]
			g := sum[gov]
			g.Runs++
			g.HardLimitExceedTotal += r.HardLimitExceed
			if r.Evidence != "" {
				incomplete = append(incomplete, fmt.Sprintf("%s#%d: %s", gov, k, r.Evidence))
			} else if !r.OK {
				incomplete = append(incomplete, fmt.Sprintf("%s#%d: lần chạy lỗi (%s)", gov, k, r.Error))
			} else {
				g.OKRuns++
				snaps[gov] = append(snaps[gov], float64(r.SnapshotSeconds))
				p99s[gov] = append(p99s[gov], float64(r.AppP99Ms))
				if manifest == "" {
					manifest = r.FixtureManifest
				} else if manifest != r.FixtureManifest {
					incomplete = append(incomplete, fmt.Sprintf("%s#%d: fixture manifest khác các lần chạy khác", gov, k))
				}
			}
			if finite(float64(r.P2PLogRMedian)) && r.P2PLogRMedian > g.MaxP2PMedian {
				g.MaxP2PMedian = r.P2PLogRMedian
			}
			if finite(float64(r.P2PLogRMax)) && r.P2PLogRMax > g.MaxP2P {
				g.MaxP2P = r.P2PLogRMax
			}
			sum[gov] = g
		}
	}
	for gov, g := range sum {
		g.MedianSnapshotSeconds = jf(medianOf(snaps[gov]))
		g.MedianAppP99Ms = jf(medianOf(p99s[gov]))
		sum[gov] = g
	}
	for g := range best {
		known := false
		for _, rg := range requiredGovernors {
			known = known || rg == g
		}
		if !known {
			incomplete = append(incomplete, "governor lạ: "+g)
		}
	}
	if len(incomplete) > 0 {
		return sum, Verdict{Status: StatusIncomplete, Reasons: incomplete, Notes: notes}
	}
	var fail []string
	for _, gov := range requiredGovernors {
		if g := sum[gov]; g.HardLimitExceedTotal > 0 {
			fail = append(fail, fmt.Sprintf("%s vượt hard limit %d mẫu", gov, g.HardLimitExceedTotal))
		}
	}
	gr, on := sum["grace-v2"], sum["onoff"]
	if !(float64(gr.MaxP2P) <= maxP2POfficial) {
		fail = append(fail, fmt.Sprintf("grace-v2 p2p log R max %.3f > 0,3", float64(gr.MaxP2P)))
	}
	gain := 1 - float64(gr.MedianSnapshotSeconds)/float64(on.MedianSnapshotSeconds)
	if !(gain >= 0.30) {
		fail = append(fail, fmt.Sprintf("snapshot grace-v2 nhanh hơn onoff %.1f%% (< 30%%)", gain*100))
	}
	if !(gr.MedianAppP99Ms <= on.MedianAppP99Ms) {
		fail = append(fail, fmt.Sprintf("p99 grace-v2 %.2f ms > onoff %.2f ms", float64(gr.MedianAppP99Ms), float64(on.MedianAppP99Ms)))
	}
	if len(fail) == 0 {
		return sum, Verdict{Status: StatusGrace, Notes: notes, Reasons: []string{fmt.Sprintf(
			"snapshot nhanh hơn onoff %.1f%%, p99 %.2f ≤ %.2f ms", gain*100, float64(gr.MedianAppP99Ms), float64(on.MedianAppP99Ms))}}
	}
	return sum, Verdict{Status: StatusOnoff, Reasons: fail, Notes: notes}
}

func (v Verdict) String() string {
	return v.Status + ": " + strings.Join(v.Reasons, "; ")
}

var _ = math.NaN
