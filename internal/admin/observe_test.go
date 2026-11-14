package admin_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"strand/internal/admin"
)

// observeHandler thêm ba lệnh quan sát vào mockHandler.
type observeHandler struct {
	*mockHandler
	ring *admin.EventRing
}

func (h *observeHandler) Status(ctx context.Context) (*admin.StatusInfo, error) {
	return &admin.StatusInfo{PipelineID: "orders-sync", State: "synced", Epoch: 7, Workers: 4,
		CommitOrder: "strict", StartPosition: "uuid:1-10",
		Governor: admin.GovernorInfo{Algorithm: "grace-v2", Zone: "green", ForegroundRate: 12.5}}, nil
}

func (h *observeHandler) Tables(ctx context.Context) ([]admin.TableInfo, error) {
	return []admin.TableInfo{
		{Table: "app.a", State: "done", ProgressPct: 100},
		{Table: "app.b", State: "running", Ranges: map[string]int{"Chưa đọc": 3, "Xong": 1}, RowsEstimate: 1000, RowsCopied: 250, ProgressPct: 25},
	}, nil
}

func (h *observeHandler) Events(ctx context.Context) ([]admin.EventRecord, error) {
	return h.ring.Snapshot(), nil
}

func TestWeb_StatusTablesEventsOverSocket(t *testing.T) {
	ring := admin.NewEventRing(10)
	ring.Add(admin.EventRecord{Time: time.Now(), Level: "WARN", Message: "hello"})
	_, client, cleanup := setupTestServer(t, &observeHandler{mockHandler: newMockHandler(), ring: ring})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	st, err := client.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Epoch != 7 || st.Workers != 4 || st.Governor.Algorithm != "grace-v2" || st.StartPosition != "uuid:1-10" {
		t.Fatalf("unexpected status %+v", st)
	}
	ts, err := client.Tables(ctx)
	if err != nil || len(ts) != 2 || ts[1].State != "running" || ts[1].Ranges["Xong"] != 1 || ts[1].ProgressPct != 25 {
		t.Fatalf("unexpected tables %+v err=%v", ts, err)
	}
	ev, err := client.Events(ctx)
	if err != nil || len(ev) != 1 || ev[0].Message != "hello" || ev[0].Level != "WARN" {
		t.Fatalf("unexpected events %+v err=%v", ev, err)
	}
}

// Handler cũ (không cài interface mới) phải trả lỗi rõ ràng, giao thức cũ vẫn chạy.
func TestWeb_NewCommandsBackwardCompatible(t *testing.T) {
	_, client, cleanup := setupTestServer(t, newMockHandler())
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for name, call := range map[string]func() error{
		"status": func() error { _, err := client.Status(ctx); return err },
		"tables": func() error { _, err := client.Tables(ctx); return err },
		"events": func() error { _, err := client.Events(ctx); return err },
	} {
		err := call()
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Errorf("%s: want 'not supported' error, got %v", name, err)
		}
	}
	if _, err := client.Top(ctx); err != nil {
		t.Fatalf("top must still work: %v", err)
	}
}

func TestWeb_EventRingKeepsLastAndWrapsHandler(t *testing.T) {
	ring := admin.NewEventRing(3)
	var sb strings.Builder
	base := slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelWarn})
	log := slog.New(ring.Wrap(base)).With("pipeline", "p1")
	log.Info("ignored info")
	for i := 0; i < 5; i++ {
		log.Warn(fmt.Sprintf("w%d", i), "n", i)
	}
	log.Error("boom", "table", "app.t")
	ev := ring.Snapshot()
	if len(ev) != 3 {
		t.Fatalf("want 3 events (capacity), got %d", len(ev))
	}
	if ev[0].Message != "w3" || ev[1].Message != "w4" || ev[2].Message != "boom" {
		t.Fatalf("wrong order/content: %+v", ev)
	}
	if ev[2].Level != "ERROR" || ev[2].Attrs["table"] != "app.t" || ev[2].Attrs["pipeline"] != "p1" {
		t.Fatalf("attrs lost: %+v", ev[2])
	}
	// Log gốc không đổi: vẫn nhận WARN/ERROR, không nhận INFO.
	out := sb.String()
	if strings.Contains(out, "ignored info") || !strings.Contains(out, "msg=w0") || !strings.Contains(out, "msg=boom") {
		t.Fatalf("underlying handler output changed:\n%s", out)
	}
}

func TestWeb_EventRingCapturesWarnEvenWhenHandlerFiltersHigher(t *testing.T) {
	ring := admin.NewEventRing(5)
	var sb strings.Builder
	log := slog.New(ring.Wrap(slog.NewTextHandler(&sb, &slog.HandlerOptions{Level: slog.LevelError})))
	log.Warn("warn-only")
	if len(ring.Snapshot()) != 1 {
		t.Fatal("warn must reach the ring")
	}
	if strings.Contains(sb.String(), "warn-only") {
		t.Fatal("warn must not reach a handler that filters it")
	}
	var nilRing *admin.EventRing
	if nilRing.Snapshot() != nil {
		t.Fatal("nil ring snapshot must be nil")
	}
}
