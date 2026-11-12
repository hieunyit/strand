package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"strand/internal/admin"
	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
)

// adminHandler phục vụ admin socket (02 §10) bằng trạng thái thật của pipeline.
type adminHandler struct {
	p *Pipeline
}

// errNotReady: admin socket mở trước khi scheduler/writer/snapshotter/governor được dựng; lệnh cần
// chúng bị từ chối thay vì nil deref làm chết process (review R12).
var errNotReady = fmt.Errorf("pipeline is starting; try again")

// ready báo các thành phần lệnh admin cần đã dựng xong. p.running được đặt ngay trước log
// "pipeline running", sau khi mọi thành phần (sched, writers, comp, snap, gov) đã khởi tạo (review R12).
func (h *adminHandler) ready() error {
	if !h.p.running.Load() {
		return errNotReady
	}
	return nil
}

func (h *adminHandler) state() string {
	switch {
	case h.p.failed() != nil:
		return "failed"
	case h.p.paused.Load():
		return "paused"
	case !h.p.running.Load():
		return "starting"
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
	// Top vẫn trả lời lúc "starting" (script chờ trạng thái), chỉ bỏ lag khi scheduler chưa có (review R12).
	lag := 0.0
	if h.ready() == nil {
		lag = p.lagSeconds()
	}
	return &admin.TopInfo{
		PipelineID: p.id,
		State:      h.state(),
		LagSeconds: lag,
		Throughput: p.throughput.rate(), // row/giây trong cửa sổ gần nhất (review R18)
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
	// Chỉ báo thành công cho thứ thực sự được thi hành (review R17): soft limit IOPS source, áp cho mọi
	// governor (static dùng chính giá trị này làm nhịp snapshot). IOPS target chưa được thi hành — từ
	// chối thay vì báo đã đặt.
	if err := h.ready(); err != nil {
		return nil, err
	}
	if req.TargetIOPS > 0 {
		return nil, fmt.Errorf("target IOPS limits are not enforced by this version; nothing changed")
	}
	if req.SourceIOPS <= 0 {
		return nil, fmt.Errorf("--source-iops must be > 0")
	}
	// Không có --soft: như các bản trước, giá trị đổi soft limit (hard limit không đổi nóng được) —
	// nhưng phản hồi báo đúng là soft, không còn báo nhầm đã đặt hard.
	h.p.setSoftRate(float64(req.SourceIOPS))
	return &admin.ThrottleResult{SourceIOPSSoft: req.SourceIOPS, SourceIOPSHard: h.p.cfg.Resources.Source.IOPS.Hard}, nil
}

func (h *adminHandler) CheckpointShow(ctx context.Context) (*admin.CheckpointResult, error) {
	p := h.p
	var applied string
	var updated time.Time
	// updated_at đọc dưới dạng epoch micro giây: DSN MySQL của target có thể thiếu parseTime=true, khi đó
	// driver trả TIMESTAMP dạng []byte và Scan vào time.Time lỗi (`strand checkpoint show` hỏng).
	q := "SELECT applied, CAST(UNIX_TIMESTAMP(updated_at) * 1000000 AS SIGNED) FROM _strand_checkpoint WHERE pipeline_id = ?"
	if p.dstEngine == model.EnginePG {
		q = "SELECT applied, (EXTRACT(EPOCH FROM updated_at) * 1000000)::bigint FROM _strand_checkpoint WHERE pipeline_id = $1"
	}
	var micros sql.NullInt64
	err := p.dst.QueryRowContext(ctx, q, p.id).Scan(&applied, &micros)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if micros.Valid {
		updated = time.UnixMicro(micros.Int64).UTC()
	}
	all, err := p.store.LoadApplied(ctx)
	if err == nil {
		applied = all.(fmt.Stringer).String()
	}
	return &admin.CheckpointResult{PipelineID: p.id, Epoch: p.epoch, Applied: applied, UpdatedAt: updated}, nil
}

// CheckpointRewind tua tập đã apply về `to` (kiểm gtid_purged), rồi dừng pipeline để
// supervisor khởi động lại và stream lại từ vị trí mới (02 §10).
func (h *adminHandler) CheckpointRewind(ctx context.Context, to string) (*admin.CheckpointResult, error) {
	p := h.p
	if err := h.ready(); err != nil {
		return nil, err
	}
	if p.srcEngine != model.EngineMySQL || p.dstEngine != model.EngineMySQL {
		return nil, fmt.Errorf("checkpoint rewind is supported for MySQL → MySQL pipelines only")
	}
	if p.posMode {
		return nil, rejectBinlogPos("checkpoint rewind")
	}
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
	if err := h.ready(); err != nil {
		return err
	}
	return h.p.SnapshotAdd(h.p.runCtx, table)
}

// VerifyDelta: `strand verify --mode delta`.
func (h *adminHandler) VerifyDelta(ctx context.Context) (string, error) {
	if err := h.ready(); err != nil {
		return "", err
	}
	return h.p.VerifyDelta(ctx)
}
