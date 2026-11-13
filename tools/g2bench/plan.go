package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Wave: sóng vuông giữa Low và High (chu kỳ Period, mỗi mức Period/2) + spike
// (SpikeMult × High trong SpikeDur, mỗi SpikeEvery). Mốc 0 là lúc bắt đầu tải; sóng bắt đầu ở mức thấp.
type Wave struct {
	Low, High  float64
	Period     time.Duration
	SpikeEvery time.Duration
	SpikeDur   time.Duration
	SpikeMult  float64
}

// RateAt: mức tải mục tiêu (txn/s) tại d kể từ mốc 0.
func (w Wave) RateAt(d time.Duration) float64 {
	r := w.Low
	if half := w.Period / 2; half > 0 && (d/half)%2 == 1 {
		r = w.High
	}
	if w.inSpike(d) {
		r = w.High * w.SpikeMult
	}
	return r
}

func (w Wave) inSpike(d time.Duration) bool {
	return w.SpikeEvery > 0 && d >= w.SpikeEvery && d%w.SpikeEvery < w.SpikeDur
}

// Segment: một đoạn tải hằng định, chạy bằng một tiến trình client.
type Segment struct {
	Start time.Duration `json:"start_offset_ns"`
	Dur   time.Duration `json:"duration_ns"`
	Rate  float64       `json:"rate"`
}

// NextSegment trả đoạn hằng định bắt đầu tại at: kết thúc ở mốc đổi mức gần nhất
// (cạnh nửa chu kỳ, đầu/cuối spike) hoặc sau maxChunk. Client ngoài không đổi tốc độ khi đang chạy
// và không dừng sạch giữa chừng (log thô cần footer) nên tải được cắt thành các tiến trình nối tiếp.
func (w Wave) NextSegment(at, maxChunk time.Duration) Segment {
	end := at + maxChunk
	cut := func(t time.Duration) {
		if t > at && t < end {
			end = t
		}
	}
	if half := w.Period / 2; half > 0 {
		cut((at/half + 1) * half)
	}
	if w.SpikeEvery > 0 {
		cut((at/w.SpikeEvery + 1) * w.SpikeEvery) // đầu spike kế tiếp
		if at >= w.SpikeEvery {
			k := at / w.SpikeEvery
			cut(k*w.SpikeEvery + w.SpikeDur) // cuối spike hiện tại
		}
	}
	return Segment{Start: at, Dur: end - at, Rate: w.RateAt(at)}
}

// Job là một lần chạy trong thứ tự ngẫu nhiên.
type Job struct {
	Gov string `json:"governor"`
	Rep int    `json:"repeat"`
}

// RunOrder: mọi (governor, repeat) trộn bằng seed — cùng seed cho cùng thứ tự.
func RunOrder(governors []string, repeats int, seed int64) []Job {
	var jobs []Job
	for _, g := range governors {
		for r := 1; r <= repeats; r++ {
			jobs = append(jobs, Job{g, r})
		}
	}
	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(jobs), func(i, j int) { jobs[i], jobs[j] = jobs[j], jobs[i] })
	return jobs
}

func (j Job) String() string { return fmt.Sprintf("%s#%d", j.Gov, j.Rep) }

func splitList(s string) []string {
	var out []string
	for _, x := range strings.Split(s, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}
