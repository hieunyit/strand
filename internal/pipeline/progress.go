package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
	var lastRows int64 = -1
	lastChange, lastWarn := time.Now(), time.Time{}
	for {
		if p.snap != nil {
			now, rows := time.Now(), p.snap.RowsFlushed.Load()
			p.snapRate.add(now, rows)
			if rows != lastRows {
				lastRows, lastChange = rows, now
			}
			// Watchdog: bootstrap mà 2 phút không ghi thêm row snapshot nào → log chẩn đoán (mỗi 2 phút).
			if p.bootstrap.Load() && now.Sub(lastChange) >= snapStallAfter && now.Sub(lastWarn) >= snapStallAfter {
				lastWarn = now
				p.logSnapshotStall(ctx, now.Sub(lastChange))
			}
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

// snapStallAfter: snapshot không tiến trong khoảng này thì watchdog ghi log chẩn đoán.
var snapStallAfter = 2 * time.Minute

// logSnapshotStall ghi một dòng WARN nêu mọi lý do thường gặp khiến snapshot không tiến: breaker, governor,
// pause/khung giờ, trạng thái range của bảng đang chạy, và (nguồn MySQL/MariaDB) giao dịch mở lâu nhất /
// phiên đang chờ khóa — LOCK TABLES … READ của snapshot chờ giao dịch đang ghi bảng commit.
func (p *Pipeline) logSnapshotStall(ctx context.Context, idle time.Duration) {
	table, _ := p.snapCurTable.Load().(string)
	attrs := []any{"idle", idle.Round(time.Second).String(), "table", table,
		"governor_rate", p.backgroundRate(), "paused", p.snapPaused.Load(), "gated_readers", p.snapGated.Load(),
		"retention_risk", p.retentionRisk.Load()}
	if p.breaker != nil && p.breaker.IsOpen() {
		attrs = append(attrs, "breaker", "open", "breaker_reason", p.breaker.Reason())
	} else {
		attrs = append(attrs, "breaker", "closed")
	}
	if p.snap != nil {
		if c, ok := p.snap.Manager.StateCounts()[table]; ok {
			attrs = append(attrs, "ranges", fmt.Sprint(c))
		}
	}
	if p.srcEngine != model.EnginePG && p.src != nil {
		qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		attrs = append(attrs, "oldest_source_txns", sourceTxnSummary(qctx, p.src),
			"source_lock_waits", sourceLockWaitSummary(qctx, p.src))
	}
	attrs = append(attrs, "hint", "breaker open → see breaker_reason / governor.breaker; long source transactions block LOCK TABLES READ of the snapshot until they commit")
	p.log.Warn("snapshot made no progress", attrs...)
}

// sourceTxnSummary: 3 giao dịch InnoDB mở lâu nhất trên nguồn ("id=… 3600s state query").
func sourceTxnSummary(ctx context.Context, db *sql.DB) string {
	rows, err := db.QueryContext(ctx, `SELECT trx_mysql_thread_id, TIMESTAMPDIFF(SECOND, trx_started, NOW()), trx_state, IFNULL(LEFT(trx_query, 80), '')
		FROM information_schema.INNODB_TRX ORDER BY trx_started LIMIT 3`)
	if err != nil {
		return "unavailable: " + err.Error()
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, secs int64
		var st, q string
		if rows.Scan(&id, &secs, &st, &q) == nil {
			out = append(out, fmt.Sprintf("id=%d %ds %s %q", id, secs, st, q))
		}
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, "; ")
}

// sourceLockWaitSummary: phiên đang chờ khóa (metadata lock / table lock) trên nguồn.
func sourceLockWaitSummary(ctx context.Context, db *sql.DB) string {
	rows, err := db.QueryContext(ctx, `SELECT ID, USER, TIME, STATE, IFNULL(LEFT(INFO, 80), '') FROM information_schema.PROCESSLIST
		WHERE STATE LIKE '%lock%' ORDER BY TIME DESC LIMIT 5`)
	if err != nil {
		return "unavailable: " + err.Error()
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, t int64
		var u, st, q string
		if rows.Scan(&id, &u, &t, &st, &q) == nil {
			out = append(out, fmt.Sprintf("id=%d %s %ds %s %q", id, u, t, st, q))
		}
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, "; ")
}
