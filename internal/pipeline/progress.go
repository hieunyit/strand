package pipeline

import (
	"context"

	"strand/internal/admin"
	"sync"
	"time"

	"strand/internal/model"
)

// rateWindow: tốc độ (đơn vị/giây) trên cửa sổ trượt ~60 s từ các mẫu bộ đếm cộng dồn.
type rateWindow struct {
	mu      sync.Mutex
	samples []rateSample
}

type rateSample struct {
	at time.Time
	v  int64
}

func (w *rateWindow) add(now time.Time, v int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.samples = append(w.samples, rateSample{now, v})
	for len(w.samples) > 2 && now.Sub(w.samples[0].at) > 60*time.Second {
		w.samples = w.samples[1:]
	}
}

func (w *rateWindow) rate() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.samples) < 2 {
		return 0
	}
	a, b := w.samples[0], w.samples[len(w.samples)-1]
	if d := b.at.Sub(a.at).Seconds(); d > 0 && b.v >= a.v {
		return float64(b.v-a.v) / d
	}
	return 0
}

// progressLoop lấy mẫu số row snapshot đã ghi mỗi 5 s (strand top).
func (p *Pipeline) progressLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		if p.snap != nil {
			p.snapRate.add(time.Now(), p.snap.RowsFlushed.Load())
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// estimateRows: tổng số row ước lượng (thống kê của source) của các bảng sẽ snapshot; lỗi thì bỏ qua bảng.
func (p *Pipeline) estimateRows(ctx context.Context, tables []model.TableID) int64 {
	var total int64
	for _, id := range tables {
		var n int64
		var err error
		if p.srcEngine == model.EnginePG {
			err = p.src.QueryRowContext(ctx, `SELECT GREATEST(c.reltuples, 0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2`, id.Schema, id.Table).Scan(&n)
		} else {
			err = p.src.QueryRowContext(ctx, `SELECT IFNULL(TABLE_ROWS, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, id.Schema, id.Table).Scan(&n)
		}
		if err == nil && n > 0 {
			total += n
		}
	}
	return total
}

// snapshotProgress: % và ETA của bootstrap. Ước lượng row lấy từ thống kê source (có thể lệch, và lần
// chạy tiếp không đếm row đã ghi ở lần trước): % giữ dưới 100 tới khi mọi bảng xong.
func snapshotProgress(t *admin.TopInfo) (pct, eta float64) {
	if t.TablesTotal > 0 && t.TablesDone >= t.TablesTotal {
		return 100, 0
	}
	if t.SnapshotRowsTotal > 0 {
		pct = 100 * float64(t.SnapshotRows) / float64(t.SnapshotRowsTotal)
	} else if t.TablesTotal > 0 {
		pct = 100 * float64(t.TablesDone) / float64(t.TablesTotal)
	}
	if pct > 99.9 {
		pct = 99.9
	}
	if t.SnapshotRate > 0 && t.SnapshotRowsTotal > t.SnapshotRows {
		eta = float64(t.SnapshotRowsTotal-t.SnapshotRows) / t.SnapshotRate
	}
	return pct, eta
}
