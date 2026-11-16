package pipeline

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"strand/internal/admin"
	"strand/internal/model"
	"strand/internal/schemagen"
	"strand/internal/snapshot"
)

// Các lệnh admin chỉ quan sát cho Web UI (docs/PLAN-WEB-UI.md §5): status, tables, events.
// Không đụng reader, scheduler, writer, checkpoint: chỉ đọc cờ atomic và trạng thái snapshot.

// Status trả thông tin chậm đổi của lần chạy: epoch, uptime, vị trí bắt đầu, governor, số worker.
func (h *adminHandler) Status(ctx context.Context) (*admin.StatusInfo, error) {
	p := h.p
	info := &admin.StatusInfo{
		PipelineID:     p.id,
		State:          h.state(),
		Epoch:          p.epoch,
		StartedAt:      p.startedAt,
		UptimeSeconds:  time.Since(p.startedAt).Seconds(),
		StartAt:        p.opts.StartAt,
		SourceKind:     p.cfg.Source.Kind,
		TargetKind:     p.cfg.Target.Kind,
		CommitOrder:    "relaxed",
		Workers:        len(p.writers),
		Paused:         p.paused.Load(),
		SnapshotPaused: p.snapPaused.Load(),
		AppliedTxns:    p.appliedTxns.Load(),
		AppliedRows:    p.appliedRows.Load(),
	}
	if p.strict {
		info.CommitOrder = "strict"
	}
	if p.applied0 != nil {
		info.StartPosition = fmt.Sprint(p.applied0)
	}
	gi := admin.GovernorInfo{Algorithm: p.govAlgo}
	if h.ready() == nil {
		if p.gov != nil && p.govAlgo != "static" {
			gi.ForegroundRate, gi.BackgroundRate = p.gov.CurrentRate()
			gi.Zone = p.gov.Zone().String()
			gi.Pressure = p.gov.Pressure()
			gi.BreakerOpen = p.gov.BreakerOpen()
		} else {
			gi.BackgroundRate = math.Float64frombits(p.softRateBits.Load())
		}
		if p.breaker != nil && p.breaker.IsOpen() {
			gi.BreakerOpen, gi.BreakerReason = true, p.breaker.Reason()
		}
	}
	info.Governor = gi
	return info, nil
}

// Tables trả trạng thái snapshot từng bảng: done / running / pending, số range theo trạng thái,
// tiến độ theo PK, ước lượng row (thống kê source, cache) và ETA của bảng đang chạy.
func (h *adminHandler) Tables(ctx context.Context) ([]admin.TableInfo, error) {
	if err := h.ready(); err != nil {
		return nil, err
	}
	p := h.p
	var counts map[string]map[string]int
	if p.snap != nil && p.snap.Manager != nil {
		counts = p.snap.Manager.StateCounts()
	}
	p.tablesMu.RLock()
	metas := make([]*tableMeta, 0, len(p.tables))
	for _, m := range p.tables {
		metas = append(metas, m)
	}
	p.tablesMu.RUnlock()
	sort.Slice(metas, func(i, j int) bool { return metas[i].ID.String() < metas[j].ID.String() })

	cur, _ := p.snapCurTable.Load().(string)
	rate := p.snapRate.rate()
	out := make([]admin.TableInfo, 0, len(metas))
	for _, m := range metas {
		name := m.ID.String()
		ti := admin.TableInfo{Table: name, PK: m.PK, GenericPK: !m.PKNumeric, Key: keyLabel(m)}
		rc, active := counts[name]
		switch {
		case p.state.IsDone(name):
			ti.State, ti.ProgressPct = "done", 100
		case active || name == cur:
			ti.State = "running"
		default:
			ti.State = "pending"
		}
		if ti.State != "done" {
			ti.Ranges = rc
			ti.RowsEstimate = h.rowsEstimate(ctx, m.ID)
			ti.ProgressPct = tableProgress(p.state, name, rc)
			ti.RowsCopied = int64(float64(ti.RowsEstimate) * ti.ProgressPct / 100)
			if ti.State == "running" && rate > 0 && ti.RowsEstimate > ti.RowsCopied {
				ti.ETASeconds = float64(ti.RowsEstimate-ti.RowsCopied) / rate
			}
		}
		out = append(out, ti)
	}
	return out, nil
}

func (h *adminHandler) rowsEstimate(ctx context.Context, id model.TableID) int64 {
	if v, ok := h.p.tableRowsEst.Load(id); ok {
		return v.(int64)
	}
	if h.p.src == nil {
		return 0
	}
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	n := h.p.estimateRows(cctx, []model.TableID{id})
	if cctx.Err() == nil {
		h.p.tableRowsEst.Store(id, n)
	}
	return n
}

// tableProgress: % đã flush của một bảng chưa xong. PK số nguyên: theo kế hoạch (NextPK trong
// [MinPK, MaxPK]); khóa tổng quát hoặc chưa có kế hoạch: theo tỉ lệ range Done.
func tableProgress(st *localState, name string, rc map[string]int) float64 {
	if pl, ok := st.Plan(name); ok && !pl.GenericKey {
		switch {
		case pl.Finished:
			return 100
		case !pl.Empty && pl.MaxPK > pl.MinPK && pl.NextPK > pl.MinPK:
			span := float64(pl.MaxPK) - float64(pl.MinPK) + 1
			return clampPct(100 * (float64(pl.NextPK) - float64(pl.MinPK)) / span)
		}
	}
	total, done := 0, 0
	for k, n := range rc {
		total += n
		if k == snapshot.StateDone.String() {
			done += n
		}
	}
	if total == 0 {
		return 0
	}
	return clampPct(100 * float64(done) / float64(total))
}

func clampPct(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 99.9:
		return 99.9
	}
	return v
}

// Events trả tối đa 200 bản ghi WARN/ERROR gần nhất (cũ → mới).
func (h *adminHandler) Events(ctx context.Context) ([]admin.EventRecord, error) {
	return h.p.opts.Events.Snapshot(), nil
}

// SourceTables liệt kê bảng người dùng của source (không đổi trạng thái pipeline), đánh dấu bảng đã thuộc
// pipeline; Web UI dùng để chọn bảng cho `snapshot add`. Chỉ đọc information_schema / pg_catalog.
func (h *adminHandler) SourceTables(ctx context.Context) ([]admin.SourceTable, error) {
	if err := h.ready(); err != nil {
		return nil, err
	}
	p := h.p
	if p.src == nil {
		return nil, fmt.Errorf("source connection is not ready")
	}
	cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	ids, _, err := schemagen.Discover(cctx, p.src, p.cfg.Source.Kind, []string{"*"}, nil)
	if err != nil {
		return nil, err
	}
	p.tablesMu.RLock()
	defer p.tablesMu.RUnlock()
	out := make([]admin.SourceTable, 0, len(ids))
	for _, id := range ids {
		_, in := p.tables[id]
		out = append(out, admin.SourceTable{Table: id.String(), InPipeline: in})
	}
	return out, nil
}
