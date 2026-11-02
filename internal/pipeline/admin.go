package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"strand/internal/admin"
	"strand/internal/checkpoint"
	"strand/internal/exitcode"
)

// adminHandler phục vụ admin socket (02 §10) bằng trạng thái thật của pipeline.
type adminHandler struct {
	p *Pipeline
}

func (h *adminHandler) state() string {
	switch {
	case h.p.failed() != nil:
		return "failed"
	case h.p.paused.Load():
		return "paused"
	case h.p.bootstrap.Load(), h.p.verifying.Load():
		return "bootstrapping"
	default:
		return "synced"
	}
}

func (h *adminHandler) Top(ctx context.Context) (*admin.TopInfo, error) {
	p := h.p
	zone := "green"
	if p.retentionRisk.Load() {
		zone = "bootstrap_retention_risk"
	}
	return &admin.TopInfo{
		PipelineID: p.id,
		State:      h.state(),
		LagSeconds: p.lagSeconds(),
		Throughput: float64(p.appliedRows.Load()),
		Zone:       zone,
	}, nil
}

func (h *adminHandler) Pause(ctx context.Context) (*admin.PauseResult, error) {
	h.p.paused.Store(true)
	return &admin.PauseResult{State: "paused", Paused: true}, nil
}

// PauseSnapshot chỉ tạm dừng phần snapshot logic, giữ stream CDC vẫn chạy (02 §10).
func (h *adminHandler) PauseSnapshot(ctx context.Context) (*admin.PauseResult, error) {
	h.p.snapPaused.Store(true)
	return &admin.PauseResult{State: "snapshot_paused", Paused: true}, nil
}

func (h *adminHandler) Resume(ctx context.Context, snapshotOnly bool) (*admin.ResumeResult, error) {
	if snapshotOnly {
		// Chỉ chạy lại snapshot đã dừng bằng `pause --snapshot`; không đổi trạng thái pause của stream.
		h.p.snapPaused.Store(false)
		return &admin.ResumeResult{State: h.state(), SnapshotOnly: true, Resumed: true}, nil
	}
	h.p.pauseMu.Lock()
	h.p.paused.Store(false)
	h.p.pauseCond.Broadcast()
	h.p.pauseMu.Unlock()
	return &admin.ResumeResult{State: h.state(), Resumed: true}, nil
}

func (h *adminHandler) ThrottleSet(ctx context.Context, req admin.Request) (*admin.ThrottleResult, error) {
	// Đổi soft limit IOPS source của governor nóng (02 §10).
	if req.SourceIOPS > 0 {
		h.p.gov.SetSoftRate(float64(req.SourceIOPS))
	}
	res := &admin.ThrottleResult{}
	if req.Soft {
		res.SourceIOPSSoft, res.TargetIOPSSoft = req.SourceIOPS, req.TargetIOPS
	} else {
		res.SourceIOPSHard, res.TargetIOPSHard = req.SourceIOPS, req.TargetIOPS
	}
	return res, nil
}

func (h *adminHandler) CheckpointShow(ctx context.Context) (*admin.CheckpointResult, error) {
	p := h.p
	var applied string
	var updated time.Time
	err := p.dst.QueryRowContext(ctx,
		"SELECT applied, updated_at FROM _strand_checkpoint WHERE pipeline_id = ?", p.id).Scan(&applied, &updated)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	all, err := p.store.LoadApplied(ctx)
	if err == nil {
		applied = all.(*checkpoint.MySQLAppliedSet).String()
	}
	return &admin.CheckpointResult{PipelineID: p.id, Epoch: p.epoch, Applied: applied, UpdatedAt: updated}, nil
}

// CheckpointRewind tua tập đã apply về `to` (kiểm gtid_purged), rồi dừng pipeline để
// supervisor khởi động lại và stream lại từ vị trí mới (02 §10).
func (h *adminHandler) CheckpointRewind(ctx context.Context, to string) (*admin.CheckpointResult, error) {
	p := h.p
	set, err := checkpoint.ParseMySQLGTIDSet(to)
	if err != nil || set.String() == "" {
		return nil, fmt.Errorf("invalid GTID set %q: %v", to, err)
	}
	if err := p.checkLogAvailable(ctx, set); err != nil {
		return nil, err
	}
	p.paused.Store(true)
	p.routeMu.Lock()
	defer p.routeMu.Unlock()
	if !p.drain(time.Minute) {
		return nil, fmt.Errorf("rewind: pipeline did not drain")
	}
	p.comp.Stop()
	tx, err := p.dst.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, "UPDATE _strand_checkpoint SET applied = ?, updated_at = NOW(6) WHERE pipeline_id = ? AND epoch = ?",
		set.String(), p.id, p.epoch)
	if err != nil {
		return nil, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return nil, exitcode.New(exitcode.ErrFenced, exitcode.Fenced, "rewind fenced: epoch changed", nil)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE _strand_worker_delta SET applied = '', batch_no = batch_no + 1 WHERE pipeline_id = ? AND epoch = ?",
		p.id, p.epoch); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	p.log.Warn("checkpoint rewound; restarting pipeline", "to", set.String())
	go p.fail(fmt.Errorf("checkpoint rewound to %s: restart required", set.String()))
	return &admin.CheckpointResult{PipelineID: p.id, Epoch: p.epoch, Applied: set.String(), RewoundTo: set.String(), UpdatedAt: time.Now()}, nil
}

// SnapshotAdd: `strand snapshot add --table <t>`.
func (h *adminHandler) SnapshotAdd(ctx context.Context, table string) error {
	return h.p.SnapshotAdd(h.p.runCtx, table)
}

// VerifyDelta: `strand verify --mode delta`.
func (h *adminHandler) VerifyDelta(ctx context.Context) (string, error) {
	return h.p.VerifyDelta(ctx)
}
