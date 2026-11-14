package web

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"strand/internal/admin"
)

// SampleInterval và RingSize: lấy mẫu mỗi 5 giây, giữ 1 giờ trong RAM (plan §2).
const (
	DefaultSampleInterval = 5 * time.Second
	RingSize              = 720
)

// Ring là ring buffer có kích thước cố định (an toàn đồng thời).
type Ring[T any] struct {
	mu   sync.Mutex
	buf  []T
	next int
	full bool
}

func NewRing[T any](n int) *Ring[T] { return &Ring[T]{buf: make([]T, n)} }

// Add thêm phần tử, đè phần tử cũ nhất khi đầy.
func (r *Ring[T]) Add(v T) {
	r.mu.Lock()
	r.buf[r.next] = v
	r.next = (r.next + 1) % len(r.buf)
	if r.next == 0 {
		r.full = true
	}
	r.mu.Unlock()
}

// Snapshot trả các phần tử từ cũ đến mới.
func (r *Ring[T]) Snapshot() []T {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		return append([]T(nil), r.buf[:r.next]...)
	}
	out := make([]T, 0, len(r.buf))
	out = append(out, r.buf[r.next:]...)
	return append(out, r.buf[:r.next]...)
}

// Len: số phần tử hiện có.
func (r *Ring[T]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.full {
		return len(r.buf)
	}
	return r.next
}

// Sample là một điểm của biểu đồ.
type Sample struct {
	T          time.Time `json:"t"`
	State      string    `json:"state"`
	Lag        float64   `json:"lag_seconds"`
	Throughput float64   `json:"throughput"`       // rows/s từ `top`
	ApplyRate  float64   `json:"apply_rows_per_s"` // tính từ strand_apply_rows_total
	Holes      float64   `json:"holes"`
	Writeset   float64   `json:"writeset_entries"`
	SpoolBytes float64   `json:"spool_bytes"`
}

// Snap là trạng thái mới nhất của một pipeline (cho Overview và danh sách).
type Snap struct {
	Name      string             `json:"name"`
	State     string             `json:"state"`
	Reachable bool               `json:"admin_reachable"`
	Top       *admin.TopInfo     `json:"top,omitempty"`
	Runtime   Runtime            `json:"runtime"`
	Metrics   map[string]float64 `json:"metrics,omitempty"`
	At        time.Time          `json:"sampled_at"`
	Err       string             `json:"error,omitempty"`
}

// Các trạng thái hiển thị.
const (
	StateSynced        = "synced"
	StateBootstrapping = "bootstrapping"
	StatePaused        = "paused"
	StateFailed        = "failed"
	StateStopped       = "stopped"
	StateStarting      = "starting"
	StateUnreachable   = "unreachable"
	StateUnknown       = "unknown"
)

// AllStates theo thứ tự hiển thị ở Overview.
var AllStates = []string{StateSynced, StateBootstrapping, StatePaused, StateStarting, StateFailed, StateStopped, StateUnreachable}

// DeriveState gộp trả lời admin socket (nếu có) với trạng thái tiến trình.
func DeriveState(top *admin.TopInfo, rt Runtime) string {
	if top != nil {
		switch top.State {
		case "synced", "bootstrapping", "paused", "failed", "starting":
			return top.State
		case "":
			return StateUnknown
		}
		return top.State
	}
	if rt.Running {
		if rt.ActiveState == "activating" {
			return StateStarting
		}
		return StateUnreachable // tiến trình sống nhưng admin socket không trả lời
	}
	if rt.ActiveState == "failed" || (rt.ExitCode != nil && *rt.ExitCode != 0) {
		return StateFailed
	}
	if rt.Error != "" && rt.ActiveState == "" {
		return StateUnknown
	}
	return StateStopped
}

// Sampler lấy mẫu admin socket và /metrics của mọi pipeline.
type Sampler struct {
	reg      *Registry
	interval time.Duration
	client   *http.Client

	mu     sync.RWMutex
	rings  map[string]*Ring[Sample]
	latest map[string]*Snap
	prev   map[string]promPrev

	// FetchTop thay được trong test; mặc định gọi admin socket.
	FetchTop func(ctx context.Context, socket string) (*admin.TopInfo, error)
	Now      func() time.Time
}

type promPrev struct {
	t    time.Time
	rows float64
}

func NewSampler(reg *Registry, interval time.Duration) *Sampler {
	if interval <= 0 {
		interval = DefaultSampleInterval
	}
	return &Sampler{
		reg: reg, interval: interval,
		client: &http.Client{Timeout: 2 * time.Second},
		rings:  map[string]*Ring[Sample]{}, latest: map[string]*Snap{}, prev: map[string]promPrev{},
		FetchTop: func(ctx context.Context, socket string) (*admin.TopInfo, error) {
			ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			return admin.NewClient(socket).Top(ctx)
		},
		Now: time.Now,
	}
}

// Run lấy mẫu ngay rồi mỗi interval cho tới khi ctx hủy.
func (s *Sampler) Run(ctx context.Context) {
	s.SampleOnce(ctx)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.SampleOnce(ctx)
		}
	}
}

// SampleOnce quét lại pipeline rồi lấy mẫu song song.
func (s *Sampler) SampleOnce(ctx context.Context) {
	s.reg.Refresh(ctx)
	list := s.reg.List()
	var wg sync.WaitGroup
	for _, p := range list {
		wg.Add(1)
		go func(p *Pipeline) {
			defer wg.Done()
			s.sampleOne(ctx, p)
		}(p)
	}
	wg.Wait()
	// Bỏ dữ liệu của pipeline không còn đăng ký.
	keep := map[string]bool{}
	for _, p := range list {
		keep[p.Name] = true
	}
	s.mu.Lock()
	for n := range s.rings {
		if !keep[n] {
			delete(s.rings, n)
			delete(s.latest, n)
			delete(s.prev, n)
		}
	}
	s.mu.Unlock()
}

func (s *Sampler) sampleOne(ctx context.Context, p *Pipeline) {
	now := s.Now()
	snap := &Snap{Name: p.Name, At: now}
	if p.CfgErr != "" {
		snap.Err = p.CfgErr
	}
	snap.Runtime = s.reg.Runtime(ctx, p)
	if sock := p.AdminSocket(); sock != "" { // socket không có thì Dial thất bại ngay
		if top, err := s.FetchTop(ctx, sock); err == nil {
			snap.Top, snap.Reachable = top, true
		}
	}
	if snap.Reachable {
		if url := p.MetricsURL(); url != "" {
			if m, err := s.fetchMetrics(ctx, url); err == nil {
				snap.Metrics = m
			}
		}
	}
	snap.State = DeriveState(snap.Top, snap.Runtime)

	smp := Sample{T: now, State: snap.State}
	if snap.Top != nil {
		smp.Lag, smp.Throughput = snap.Top.LagSeconds, snap.Top.Throughput
	}
	if snap.Metrics != nil {
		smp.Holes = snap.Metrics["strand_holes"]
		smp.Writeset = snap.Metrics["strand_writeset_entries"]
		smp.SpoolBytes = snap.Metrics["strand_spool_bytes"]
	}
	s.mu.Lock()
	if rows, ok := snap.Metrics["strand_apply_rows_total"]; ok {
		if pv, had := s.prev[p.Name]; had && now.After(pv.t) && rows >= pv.rows {
			smp.ApplyRate = (rows - pv.rows) / now.Sub(pv.t).Seconds()
		}
		s.prev[p.Name] = promPrev{t: now, rows: rows}
	}
	r := s.rings[p.Name]
	if r == nil {
		r = NewRing[Sample](RingSize)
		s.rings[p.Name] = r
	}
	s.latest[p.Name] = snap
	s.mu.Unlock()
	r.Add(smp)
}

func (s *Sampler) fetchMetrics(ctx context.Context, url string) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("metrics: HTTP %d", resp.StatusCode)
	}
	return ParseProm(io.LimitReader(resp.Body, 8<<20))
}

// Latest trả trạng thái mới nhất của pipeline (nil nếu chưa có mẫu).
func (s *Sampler) Latest(name string) *Snap {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latest[name]
}

// Series trả các mẫu của pipeline từ cũ đến mới.
func (s *Sampler) Series(name string) []Sample {
	s.mu.RLock()
	r := s.rings[name]
	s.mu.RUnlock()
	if r == nil {
		return nil
	}
	return r.Snapshot()
}

// ParseProm đọc định dạng text Prometheus; cộng dồn mọi nhãn của cùng tên metric.
// Chỉ lấy tên bắt đầu bằng `strand_`.
func ParseProm(r io.Reader) (map[string]float64, error) {
	out := map[string]float64{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		ln := sc.Text()
		if !strings.HasPrefix(ln, "strand_") {
			continue
		}
		name := ln
		if i := strings.IndexAny(ln, "{ "); i > 0 {
			name = ln[:i]
		}
		rest := ln
		if i := strings.LastIndex(ln, "}"); i >= 0 {
			rest = ln[i+1:]
		} else {
			rest = ln[len(name):]
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(f[0], 64)
		if err != nil {
			continue
		}
		out[name] += v
	}
	return out, sc.Err()
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// Overview là số liệu tổng hợp của trang Overview.
type Overview struct {
	Counts         map[string]int `json:"counts"`
	Total          int            `json:"total"`
	MaxLagSeconds  float64        `json:"max_lag_seconds"`
	MaxLagPipeline string         `json:"max_lag_pipeline,omitempty"`
	RowsPerSec     float64        `json:"rows_per_sec"`
	Pipelines      []*Snap        `json:"pipelines"`
}

// BuildOverview tổng hợp từ các Snap mới nhất.
func (s *Sampler) BuildOverview() Overview {
	ov := Overview{Counts: map[string]int{}}
	for _, st := range AllStates {
		ov.Counts[st] = 0
	}
	for _, p := range s.reg.List() {
		snap := s.Latest(p.Name)
		if snap == nil {
			snap = &Snap{Name: p.Name, State: StateUnknown, Err: p.CfgErr}
		}
		ov.Pipelines = append(ov.Pipelines, snap)
		ov.Counts[snap.State]++
		ov.Total++
		if snap.Top != nil {
			if snap.Top.LagSeconds > ov.MaxLagSeconds {
				ov.MaxLagSeconds, ov.MaxLagPipeline = snap.Top.LagSeconds, p.Name
			}
			ov.RowsPerSec += snap.Top.Throughput
		}
	}
	return ov
}
