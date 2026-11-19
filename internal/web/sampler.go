package web

import (
	"bufio"
	"context"
	"errors"
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
	Throughput float64   `json:"throughput"`       // rows/s từ `top` (cửa sổ ngắn của pipeline)
	ApplyRate  float64   `json:"apply_rows_per_s"` // tính từ strand_apply_rows_total
	Rows       float64   `json:"rows_per_s"`       // rows/s đã làm mượt (cửa sổ 30 s), số hiển thị ở UI
	SnapRows   float64   `json:"snapshot_rows_per_s"`
	Holes      float64   `json:"holes"`
	Writeset   float64   `json:"writeset_entries"`
	SpoolBytes float64   `json:"spool_bytes"`
	Stale      bool      `json:"stale,omitempty"` // lần lấy mẫu này thất bại; giá trị là của lần tốt gần nhất
}

// Rates là các tốc độ đã làm mượt (cửa sổ trượt RateWindow trên bộ đếm, hoặc EWMA khi không có bộ đếm).
type Rates struct {
	Rows     float64 `json:"rows_per_s"`          // dòng apply (CDC) mỗi giây
	Txn      float64 `json:"txn_per_s"`           // giao dịch apply mỗi giây
	SnapRows float64 `json:"snapshot_rows_per_s"` // dòng snapshot mỗi giây
	OK       bool    `json:"ok"`                  // đã có ít nhất một giá trị
}

// Snap là trạng thái mới nhất của một pipeline (cho mọi trang và luồng SSE). Một Snap không bao giờ bị sửa
// sau khi lưu: mỗi lần lấy mẫu tạo Snap mới, các con trỏ tới dữ liệu "tốt gần nhất" được chia sẻ (chỉ đọc).
type Snap struct {
	Name      string             `json:"name"`
	State     string             `json:"state"`     // trạng thái hiển thị (sau luật hai mẫu)
	RawState  string             `json:"raw_state"` // trạng thái suy từ mẫu này
	Reachable bool               `json:"admin_reachable"`
	Top       *admin.TopInfo     `json:"top,omitempty"`
	Runtime   Runtime            `json:"runtime"`
	Metrics   map[string]float64 `json:"metrics,omitempty"`
	At        time.Time          `json:"sampled_at"`
	Err       string             `json:"error,omitempty"`

	// Stale: lần gọi admin socket gần nhất thất bại (hết giờ, lỗi) khi tiến trình vẫn chạy; Top, Status,
	// Tables... là của lần tốt gần nhất (LastOK). PollErr nói vì sao.
	Stale   bool      `json:"stale"`
	LastOK  time.Time `json:"last_ok"`
	PollErr string    `json:"poll_error,omitempty"`
	PollMS  float64   `json:"poll_ms"`

	Status        *admin.StatusInfo       `json:"-"`
	StatusErr     string                  `json:"-"`
	Tables        []admin.TableInfo       `json:"-"`
	TablesErr     string                  `json:"-"`
	Checkpoint    *admin.CheckpointResult `json:"-"`
	CheckpointErr string                  `json:"-"`
	Events        []admin.EventRecord     `json:"-"`
	EventsErr     string                  `json:"-"`

	Rates    Rates  `json:"rates"`
	LagLevel string `json:"lag_level"` // "", warn, bad (có trễ pha)
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
var AllStates = []string{StateSynced, StateBootstrapping, StatePaused, StateStarting, StateFailed, StateStopped, StateUnreachable, StateUnknown}

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

// AdminPoller là phần admin socket mà sampler dùng (thay được trong test).
type AdminPoller interface {
	Top(ctx context.Context) (*admin.TopInfo, error)
	Status(ctx context.Context) (*admin.StatusInfo, error)
	Tables(ctx context.Context) ([]admin.TableInfo, error)
	CheckpointShow(ctx context.Context) (*admin.CheckpointResult, error)
	Events(ctx context.Context) ([]admin.EventRecord, error)
}

// MaxConcurrentPolls giới hạn số pipeline được hỏi cùng lúc.
const MaxConcurrentPolls = 8

// pollState là trạng thái riêng của sampler cho một pipeline. Mỗi pipeline có tối đa một lần lấy mẫu đang
// chạy (inflight), nên chỉ goroutine đó chạm vào các trường còn lại.
type pollState struct {
	inflight bool
	lastRaw  string
	rows     rateWindow
	txn      rateWindow
	snapRows rateWindow
	apply    rateWindow
	rates    Rates
	lastT    time.Time
}

// Sampler là thành phần DUY NHẤT gọi admin socket và /metrics của pipeline để hiển thị: mỗi pipeline tối đa
// một lần hỏi mỗi chu kỳ, có hạn giờ, tối đa MaxConcurrentPolls pipeline song song, không bao giờ trên đường
// xử lý request. Trang và luồng SSE chỉ đọc Snap mới nhất.
type Sampler struct {
	reg      *Registry
	interval time.Duration
	timeout  time.Duration
	client   *http.Client
	sem      chan struct{}

	mu     sync.RWMutex
	rings  map[string]*Ring[Sample]
	latest map[string]*Snap
	poll   map[string]*pollState
	names  string // danh sách pipeline của vòng trước (phát hiện thay đổi cấu trúc)

	// Dial mở client admin socket; thay được trong test.
	Dial func(socket string) AdminPoller
	Now  func() time.Time
	// OnSample được gọi sau mỗi lần lấy mẫu một pipeline (từ goroutine lấy mẫu); OnRound sau mỗi vòng,
	// structural=true khi danh sách pipeline đổi. Server dùng hai móc này để phát sự kiện SSE.
	OnSample func(name string, snap *Snap, smp Sample)
	OnRound  func(structural bool)
}

// preload đặt sẵn các mẫu (cũ đến mới) vào ring của pipeline (lịch sử từ DB lúc khởi động).
func (s *Sampler) preload(name string, pts []Sample) {
	if len(pts) == 0 {
		return
	}
	s.mu.Lock()
	r := s.rings[name]
	if r == nil {
		r = NewRing[Sample](RingSize)
		s.rings[name] = r
	}
	s.mu.Unlock()
	for _, p := range pts {
		r.Add(p)
	}
}

func NewSampler(reg *Registry, interval time.Duration) *Sampler {
	if interval <= 0 {
		interval = DefaultSampleInterval
	}
	// Hạn giờ của một lần hỏi: phải xong trước chu kỳ sau, tối đa 4 s.
	to := interval * 8 / 10
	if to > 4*time.Second {
		to = 4 * time.Second
	}
	if to < time.Second {
		to = time.Second
	}
	return &Sampler{
		reg: reg, interval: interval, timeout: to,
		client: &http.Client{Timeout: to},
		sem:    make(chan struct{}, MaxConcurrentPolls),
		rings:  map[string]*Ring[Sample]{}, latest: map[string]*Snap{}, poll: map[string]*pollState{},
		Dial: func(socket string) AdminPoller { return admin.NewClient(socket) },
		Now:  time.Now,
	}
}

// Interval là chu kỳ lấy mẫu.
func (s *Sampler) Interval() time.Duration { return s.interval }

// Run lấy mẫu ngay rồi mỗi interval cho tới khi ctx hủy. Một vòng không chờ pipeline chậm: pipeline còn
// đang được hỏi từ vòng trước thì bỏ qua lần này (Snap của nó già đi và trang đánh dấu stale).
func (s *Sampler) Run(ctx context.Context) {
	s.round(ctx, false)
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.round(ctx, false)
		}
	}
}

// SampleOnce quét lại pipeline, lấy mẫu song song và chờ xong (test).
func (s *Sampler) SampleOnce(ctx context.Context) { s.round(ctx, true) }

// Kick lấy mẫu ngay một pipeline sau thao tác của người dùng (start, pause...): trạng thái mới được nhận
// ngay, không chờ mẫu thứ hai (chính người dùng vừa gây ra thay đổi). Không chặn.
func (s *Sampler) Kick(ctx context.Context, name string) {
	p := s.reg.Get(name)
	if p == nil || !s.claim(p.Name) {
		return
	}
	go func() {
		s.sem <- struct{}{}
		defer func() { <-s.sem }()
		s.sampleOne(ctx, p, true)
	}()
}

// claim đánh dấu pipeline đang được hỏi; false nếu đã có một lần hỏi đang chạy.
func (s *Sampler) claim(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ps := s.poll[name]
	if ps == nil {
		ps = &pollState{}
		s.poll[name] = ps
	}
	if ps.inflight {
		return false
	}
	ps.inflight = true
	return true
}

func (s *Sampler) round(ctx context.Context, wait bool) {
	s.reg.Refresh(ctx)
	list := s.reg.List()
	names := make([]string, 0, len(list))
	keep := map[string]bool{}
	for _, p := range list {
		keep[p.Name] = true
		names = append(names, p.Name)
	}
	sort.Strings(names)
	joined := strings.Join(names, "\x00")
	s.mu.Lock()
	structural := s.names != joined
	s.names = joined
	for n := range s.rings {
		if !keep[n] {
			delete(s.rings, n)
		}
	}
	for n := range s.latest {
		if !keep[n] {
			delete(s.latest, n)
		}
	}
	for n, ps := range s.poll {
		if !keep[n] && !ps.inflight {
			delete(s.poll, n)
		}
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, p := range list {
		if !s.claim(p.Name) {
			if !wait {
				continue // vẫn đang hỏi từ vòng trước: bỏ lượt này
			}
			// SampleOnce: chờ lần hỏi đang chạy (ví dụ Kick sau một thao tác) rồi hỏi lại.
			claimed := false
			for deadline := time.Now().Add(2 * s.timeout); !claimed && time.Now().Before(deadline); {
				time.Sleep(5 * time.Millisecond)
				claimed = s.claim(p.Name)
			}
			if !claimed {
				continue
			}
		}
		wg.Add(1)
		go func(p *Pipeline) {
			defer wg.Done()
			s.sem <- struct{}{}
			defer func() { <-s.sem }()
			s.sampleOne(ctx, p, false)
		}(p)
	}
	finish := func() {
		wg.Wait()
		if s.OnRound != nil {
			s.OnRound(structural)
		}
	}
	if wait {
		finish()
	} else {
		go finish()
	}
}

func (s *Sampler) sampleOne(ctx context.Context, p *Pipeline, force bool) {
	s.mu.RLock()
	ps := s.poll[p.Name]
	prev := s.latest[p.Name]
	s.mu.RUnlock()
	defer func() {
		s.mu.Lock()
		ps.inflight = false
		s.mu.Unlock()
	}()
	start := time.Now()
	now := s.Now()
	snap := &Snap{Name: p.Name, At: now}
	if p.CfgErr != "" {
		snap.Err = p.CfgErr
	}
	rctx, rcancel := context.WithTimeout(ctx, s.timeout)
	snap.Runtime = s.reg.Runtime(rctx, p)
	rcancel()

	actx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var topErr error
	if sock := p.AdminSocket(); sock != "" {
		c := s.Dial(sock)
		var top *admin.TopInfo
		if top, topErr = c.Top(actx); topErr == nil && top != nil {
			snap.Top, snap.Reachable = top, true
			var err error
			if snap.Status, err = c.Status(actx); err != nil {
				snap.StatusErr = notRunningMsg(err)
			}
			if snap.Tables, err = c.Tables(actx); err != nil {
				snap.TablesErr = notRunningMsg(err)
			}
			if snap.Checkpoint, err = c.CheckpointShow(actx); err != nil {
				snap.CheckpointErr = notRunningMsg(err)
			}
			if snap.Events, err = c.Events(actx); err != nil {
				snap.EventsErr = notRunningMsg(err)
			}
		} else if topErr == nil {
			topErr = errors.New("empty reply from the admin socket")
		}
	} else {
		topErr = errNoSocket
	}
	if snap.Reachable {
		if url := p.MetricsURL(); url != "" {
			if m, err := s.fetchMetrics(actx, url); err == nil {
				snap.Metrics = m
			}
		}
		snap.LastOK = now
	} else {
		msg := notRunningMsg(topErr)
		snap.PollErr = msg
		snap.StatusErr, snap.TablesErr, snap.CheckpointErr, snap.EventsErr = msg, msg, msg, msg
	}
	snap.PollMS = float64(time.Since(start).Microseconds()) / 1000
	snap.RawState = DeriveState(snap.Top, snap.Runtime)

	// Lần hỏi thất bại khi tiến trình còn chạy (hết giờ, socket bận...): giữ số liệu tốt gần nhất, đánh dấu stale.
	if !snap.Reachable && snap.Runtime.Running && prev != nil && !prev.LastOK.IsZero() {
		snap.Stale, snap.LastOK = true, prev.LastOK
		snap.Top, snap.Metrics = prev.Top, prev.Metrics
		snap.Status, snap.Tables, snap.Checkpoint, snap.Events = prev.Status, prev.Tables, prev.Checkpoint, prev.Events
		snap.StatusErr, snap.TablesErr, snap.CheckpointErr, snap.EventsErr = prev.StatusErr, prev.TablesErr, prev.CheckpointErr, prev.EventsErr
	}

	shown := ""
	if prev != nil && !force {
		shown = prev.State
	}
	snap.State = nextState(shown, ps.lastRaw, snap.RawState)
	ps.lastRaw = snap.RawState
	if snap.Top != nil {
		prevLevel := ""
		if prev != nil {
			prevLevel = prev.LagLevel
		}
		snap.LagLevel = lagLevel(prevLevel, snap.Top.LagSeconds)
	}

	smp := Sample{T: now, State: snap.State, Stale: snap.Stale}
	if snap.Top != nil {
		smp.Lag, smp.Throughput = snap.Top.LagSeconds, snap.Top.Throughput
	}
	if snap.Metrics != nil {
		smp.Holes = snap.Metrics["strand_holes"]
		smp.Writeset = snap.Metrics["strand_writeset_entries"]
		smp.SpoolBytes = snap.Metrics["strand_spool_bytes"]
	}
	s.smooth(ps, snap, &smp, now)
	smp.Rows, smp.SnapRows = snap.Rates.Rows, snap.Rates.SnapRows

	s.mu.Lock()
	if s.poll[p.Name] != ps { // pipeline vừa bị gỡ: bỏ mẫu
		s.mu.Unlock()
		return
	}
	r := s.rings[p.Name]
	if r == nil {
		r = NewRing[Sample](RingSize)
		s.rings[p.Name] = r
	}
	s.latest[p.Name] = snap
	s.mu.Unlock()
	r.Add(smp)
	if s.OnSample != nil {
		s.OnSample(p.Name, snap, smp)
	}
}

// smooth cập nhật tốc độ đã làm mượt. Bộ đếm (applied_rows, applied_txns, snapshot_rows) cho tốc độ trung
// bình trên RateWindow; không có bộ đếm thì dùng EWMA (hằng số thời gian RateWindow) của tốc độ tức thời của
// `top`. Mẫu stale giữ tốc độ tốt gần nhất. Pipeline không trả lời (dừng): tốc độ về 0, cửa sổ xóa.
func (s *Sampler) smooth(ps *pollState, snap *Snap, smp *Sample, now time.Time) {
	switch {
	case snap.Stale:
		snap.Rates = ps.rates
		return
	case !snap.Reachable:
		ps.rows.reset()
		ps.txn.reset()
		ps.snapRows.reset()
		ps.apply.reset()
		ps.rates, ps.lastT = Rates{}, time.Time{}
		snap.Rates = ps.rates
		return
	}
	dt := now.Sub(ps.lastT)
	had := ps.rates.OK
	r := ps.rates
	if st := snap.Status; st != nil {
		if v, ok := ps.rows.add(now, float64(st.AppliedRows), RateWindow); ok {
			r.Rows = v
		} else {
			r.Rows = ewma(r.Rows, had, snap.Top.Throughput, dt, RateWindow)
		}
		if v, ok := ps.txn.add(now, float64(st.AppliedTxns), RateWindow); ok {
			r.Txn = v
		}
	} else {
		r.Rows = ewma(r.Rows, had, snap.Top.Throughput, dt, RateWindow)
	}
	if v, ok := ps.snapRows.add(now, float64(snap.Top.SnapshotRows), RateWindow); ok {
		r.SnapRows = v
	} else {
		r.SnapRows = ewma(r.SnapRows, had, snap.Top.SnapshotRate, dt, RateWindow)
	}
	if rows, ok := snap.Metrics["strand_apply_rows_total"]; ok {
		if v, ok := ps.apply.add(now, rows, RateWindow); ok {
			smp.ApplyRate = v
		}
	}
	r.OK = true
	ps.rates, ps.lastT = r, now
	snap.Rates = r
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
		}
		ov.RowsPerSec += snap.Rates.Rows
	}
	return ov
}
