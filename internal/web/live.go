package web

import (
	"encoding/json"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// Số liệu "sống" của Web UI: làm mượt tốc độ, trễ có trễ pha (hysteresis) cho màu, trạng thái chỉ đổi khi
// hai mẫu liên tiếp đồng ý, và hub phát sự kiện SSE. Mọi thứ ở đây là hàm thuần hoặc cấu trúc trong RAM;
// chỉ sampler gọi admin socket (docs/WEB-API.md "Live stream").

// RateWindow là cửa sổ trượt cho tốc độ của bộ đếm: rate = (v_cuối - v_đầu) / (t_cuối - t_đầu) trong ~window.
const RateWindow = 30 * time.Second

type ratePt struct {
	t time.Time
	v float64
}

// rateWindow giữ các điểm (t, bộ đếm) của khoảng RateWindow gần nhất.
type rateWindow struct{ pts []ratePt }

// add thêm một điểm và trả tốc độ trung bình trên cửa sổ. ok=false khi chưa đủ hai điểm cách nhau > 0.
// Bộ đếm giảm (tiến trình khởi động lại) thì cửa sổ bắt đầu lại từ điểm này.
func (w *rateWindow) add(t time.Time, v float64, window time.Duration) (float64, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	if n := len(w.pts); n > 0 && (v < w.pts[n-1].v || !t.After(w.pts[n-1].t)) {
		if v < w.pts[n-1].v {
			w.pts = w.pts[:0]
		} else {
			return w.rate()
		}
	}
	w.pts = append(w.pts, ratePt{t, v})
	// Bỏ điểm cũ nhưng giữ đúng một điểm ở (hoặc trước) mép cửa sổ để khoảng đo ≈ window.
	cut := t.Add(-window)
	i := 0
	for i+1 < len(w.pts) && !w.pts[i+1].t.After(cut) {
		i++
	}
	if i > 0 {
		w.pts = append(w.pts[:0], w.pts[i:]...)
	}
	return w.rate()
}

func (w *rateWindow) rate() (float64, bool) {
	if len(w.pts) < 2 {
		return 0, false
	}
	a, b := w.pts[0], w.pts[len(w.pts)-1]
	dt := b.t.Sub(a.t).Seconds()
	if dt <= 0 {
		return 0, false
	}
	return (b.v - a.v) / dt, true
}

func (w *rateWindow) reset() { w.pts = w.pts[:0] }

// ewma là trung bình trượt mũ theo thời gian: hằng số thời gian tau, khoảng dt giữa hai mẫu.
// prevOK=false: chưa có giá trị trước, trả x.
func ewma(prev float64, prevOK bool, x float64, dt, tau time.Duration) float64 {
	if !prevOK || tau <= 0 || math.IsNaN(prev) {
		return x
	}
	if dt <= 0 {
		return prev
	}
	a := 1 - math.Exp(-dt.Seconds()/tau.Seconds())
	return prev + a*(x-prev)
}

// Ngưỡng màu của độ trễ, có trễ pha: vào mức khi vượt ngưỡng, chỉ ra khi xuống dưới 80% ngưỡng, nên số dao
// động quanh ngưỡng không làm ô đổi màu liên tục.
const (
	LagWarnSeconds = 30
	LagBadSeconds  = 300
	lagExitRatio   = 0.8
)

// lagLevel trả "", "warn" hoặc "bad" từ mức trước đó và độ trễ hiện tại.
func lagLevel(prev string, lag float64) string {
	if math.IsNaN(lag) || lag < 0 {
		lag = 0
	}
	switch {
	case lag >= LagBadSeconds:
		return "bad"
	case prev == "bad" && lag >= LagBadSeconds*lagExitRatio:
		return "bad"
	case lag >= LagWarnSeconds:
		return "warn"
	case (prev == "warn" || prev == "bad") && lag >= LagWarnSeconds*lagExitRatio:
		return "warn"
	}
	return ""
}

// nextState áp luật hiển thị trạng thái: trạng thái hiển thị chỉ đổi khi hai mẫu liên tiếp đồng ý
// (raw == lastRaw), trừ failed và stopped (đổi ngay) và lần đầu (chưa có gì để giữ).
func nextState(shown, lastRaw, raw string) string {
	switch {
	case shown == "", raw == shown, raw == StateFailed, raw == StateStopped, raw == lastRaw:
		return raw
	}
	return shown
}

// ---- hub SSE ----

// liveEvent là một sự kiện đã mã hóa. Full: bản đầy đủ (bảng, sự kiện lỗi) cho client đang xem pipeline này;
// Lite: bản gọn cho mọi client khác. Sự kiện không thuộc pipeline nào (overview, structure) chỉ có Full.
type liveEvent struct {
	ID       uint64
	Type     string
	Pipeline string
	Full     []byte
	Lite     []byte
}

func (e liveEvent) payloadFor(pipeline string) []byte {
	if e.Pipeline == "" || e.Pipeline == pipeline || e.Lite == nil {
		return e.Full
	}
	return e.Lite
}

type liveSub struct {
	pipeline string
	ch       chan liveEvent
	lagged   atomic.Bool // kênh đầy, đã rơi sự kiện: handler gửi lại snapshot đầy đủ
}

// liveHub phát sự kiện cho các kết nối SSE và giữ một vòng sự kiện gần nhất để nối lại bằng Last-Event-ID.
type liveHub struct {
	mu   sync.Mutex
	seq  uint64
	ring []liveEvent
	next int
	full bool
	subs map[*liveSub]struct{}
}

const (
	hubRingSize = 512
	subBuffer   = 256
)

func newLiveHub() *liveHub {
	return &liveHub{ring: make([]liveEvent, hubRingSize), subs: map[*liveSub]struct{}{}}
}

// publish mã hóa và phát; không bao giờ chặn (client chậm bị đánh dấu lagged).
func (h *liveHub) publish(typ, pipeline string, full, lite any) uint64 {
	fb, err := json.Marshal(full)
	if err != nil {
		return 0
	}
	var lb []byte
	if lite != nil {
		if lb, err = json.Marshal(lite); err != nil {
			return 0
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seq++
	ev := liveEvent{ID: h.seq, Type: typ, Pipeline: pipeline, Full: fb, Lite: lb}
	h.ring[h.next] = ev
	h.next = (h.next + 1) % len(h.ring)
	if h.next == 0 {
		h.full = true
	}
	for s := range h.subs {
		select {
		case s.ch <- ev:
		default:
			s.lagged.Store(true)
		}
	}
	return ev.ID
}

// subscribe đăng ký; nếu lastID còn trong vòng thì trả các sự kiện bị lỡ (replay, ok=true).
// ok=false: không nối tiếp được (id quá cũ, hoặc lastID=0), client cần snapshot đầy đủ. seq là id mới nhất
// tại lúc đăng ký: mọi sự kiện sau seq sẽ tới qua kênh của sub.
func (h *liveHub) subscribe(pipeline string, lastID uint64) (s *liveSub, replay []liveEvent, ok bool, seq uint64) {
	s = &liveSub{pipeline: pipeline, ch: make(chan liveEvent, subBuffer)}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs[s] = struct{}{}
	seq = h.seq
	if lastID == 0 || lastID > h.seq {
		return s, nil, false, seq
	}
	if lastID == h.seq {
		return s, nil, true, seq
	}
	var evs []liveEvent
	if h.full {
		evs = append(evs, h.ring[h.next:]...)
	}
	evs = append(evs, h.ring[:h.next]...)
	// evs theo thứ tự id tăng dần; id kế tiếp lastID phải còn trong vòng.
	if len(evs) == 0 || evs[0].ID > lastID+1 {
		return s, nil, false, seq
	}
	out := make([]liveEvent, 0, h.seq-lastID)
	for _, e := range evs {
		if e.ID > lastID {
			out = append(out, e)
		}
	}
	return s, out, true, seq
}

func (h *liveHub) unsubscribe(s *liveSub) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

// lastID là id của sự kiện mới nhất (0 nếu chưa có).
func (h *liveHub) lastID() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}

func (h *liveHub) subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}
