package admin

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// DefaultEventCapacity: số bản ghi WARN/ERROR giữ lại cho lệnh `events` (plan web UI §5).
const DefaultEventCapacity = 200

// EventRecord là một bản ghi WARN/ERROR gần nhất của pipeline.
type EventRecord struct {
	Time    time.Time         `json:"time"`
	Level   string            `json:"level"`
	Message string            `json:"message"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

// EventRing giữ N bản ghi WARN/ERROR gần nhất trong RAM. Chỉ quan sát: không đổi log hiện có.
type EventRing struct {
	mu   sync.Mutex
	cap  int
	buf  []EventRecord
	next int
	full bool
}

// NewEventRing tạo ring với sức chứa capacity (<= 0: DefaultEventCapacity).
func NewEventRing(capacity int) *EventRing {
	if capacity <= 0 {
		capacity = DefaultEventCapacity
	}
	return &EventRing{cap: capacity, buf: make([]EventRecord, capacity)}
}

// Add thêm một bản ghi, đè bản ghi cũ nhất khi đầy.
func (r *EventRing) Add(e EventRecord) {
	r.mu.Lock()
	r.buf[r.next] = e
	r.next = (r.next + 1) % r.cap
	if r.next == 0 {
		r.full = true
	}
	r.mu.Unlock()
}

// Snapshot trả các bản ghi từ cũ đến mới. Nhận nil an toàn.
func (r *EventRing) Snapshot() []EventRecord {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.full {
		return append([]EventRecord(nil), r.buf[:r.next]...)
	}
	out := make([]EventRecord, 0, r.cap)
	out = append(out, r.buf[r.next:]...)
	out = append(out, r.buf[:r.next]...)
	return out
}

// Wrap trả slog.Handler ghi mọi bản ghi qua next như cũ, và song song chép bản ghi >= WARN vào ring.
func (r *EventRing) Wrap(next slog.Handler) slog.Handler {
	return &ringHandler{ring: r, next: next}
}

type ringHandler struct {
	ring  *EventRing
	next  slog.Handler
	attrs []slog.Attr
}

func (h *ringHandler) Enabled(ctx context.Context, l slog.Level) bool {
	// Ring cần mọi WARN/ERROR kể cả khi handler đích lọc cao hơn.
	return h.next.Enabled(ctx, l) || l >= slog.LevelWarn
}

func (h *ringHandler) Handle(ctx context.Context, rec slog.Record) error {
	if rec.Level >= slog.LevelWarn {
		e := EventRecord{Time: rec.Time, Level: rec.Level.String(), Message: rec.Message}
		if e.Time.IsZero() {
			e.Time = time.Now()
		}
		add := func(a slog.Attr) {
			if e.Attrs == nil {
				e.Attrs = map[string]string{}
			}
			e.Attrs[a.Key] = fmt.Sprint(a.Value.Resolve().Any())
		}
		for _, a := range h.attrs {
			add(a)
		}
		rec.Attrs(func(a slog.Attr) bool { add(a); return true })
		h.ring.Add(e)
	}
	if h.next.Enabled(ctx, rec.Level) {
		return h.next.Handle(ctx, rec)
	}
	return nil
}

func (h *ringHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	c.next = h.next.WithAttrs(attrs)
	c.attrs = append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return &c
}

func (h *ringHandler) WithGroup(name string) slog.Handler {
	c := *h
	c.next = h.next.WithGroup(name)
	return &c
}
