package admin_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"strand/internal/admin"
)

// mockHandler cài đặt PipelineAdminHandler để phục vụ kiểm thử.
type mockHandler struct {
	mu           sync.Mutex
	pipelineID   string
	state        string
	lagSeconds   float64
	throughput   float64
	zone         string
	progress     float64
	etaSeconds   float64
	sourceSoft   int
	sourceHard   int
	targetSoft   int
	targetHard   int
	epoch        int64
	applied      string
	purgedPos    string
	snapshotOnly bool
}

func newMockHandler() *mockHandler {
	return &mockHandler{
		pipelineID: "orders-sync",
		state:      "synced",
		lagSeconds: 0.12,
		throughput: 2500.0,
		zone:       "green",
		progress:   85.5,
		etaSeconds: 120.0,
		sourceSoft: 600,
		sourceHard: 900,
		targetSoft: 1500,
		targetHard: 2000,
		epoch:      3,
		applied:    "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000",
		purgedPos:  "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-500",
	}
}

func (h *mockHandler) Top(ctx context.Context) (*admin.TopInfo, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return &admin.TopInfo{
		PipelineID: h.pipelineID,
		State:      h.state,
		LagSeconds: h.lagSeconds,
		Throughput: h.throughput,
		Zone:       h.zone,
		Progress:   h.progress,
		ETASeconds: h.etaSeconds,
	}, nil
}

func (h *mockHandler) Pause(ctx context.Context) (*admin.PauseResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state = "paused"
	return &admin.PauseResult{
		State:  h.state,
		Paused: true,
	}, nil
}

func (h *mockHandler) Resume(ctx context.Context, snapshotOnly bool) (*admin.ResumeResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.state = "synced"
	h.snapshotOnly = snapshotOnly
	return &admin.ResumeResult{
		State:        h.state,
		SnapshotOnly: snapshotOnly,
		Resumed:      true,
	}, nil
}

func (h *mockHandler) ThrottleSet(ctx context.Context, req admin.Request) (*admin.ThrottleResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if req.SourceIOPS > 0 {
		if req.Soft {
			h.sourceSoft = req.SourceIOPS
		} else {
			h.sourceHard = req.SourceIOPS
		}
	}
	if req.TargetIOPS > 0 {
		if req.Soft {
			h.targetSoft = req.TargetIOPS
		} else {
			h.targetHard = req.TargetIOPS
		}
	}
	return &admin.ThrottleResult{
		SourceIOPSSoft: h.sourceSoft,
		SourceIOPSHard: h.sourceHard,
		TargetIOPSSoft: h.targetSoft,
		TargetIOPSHard: h.targetHard,
	}, nil
}

func (h *mockHandler) CheckpointShow(ctx context.Context) (*admin.CheckpointResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return &admin.CheckpointResult{
		PipelineID: h.pipelineID,
		Epoch:      h.epoch,
		Applied:    h.applied,
		UpdatedAt:  time.Now(),
	}, nil
}

func (h *mockHandler) CheckpointRewind(ctx context.Context, to string) (*admin.CheckpointResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if to == "" {
		return nil, fmt.Errorf("rewind position cannot be empty")
	}
	// Kiểm tra vị trí tua lại so với purged (spec/02 §10)
	if to <= h.purgedPos {
		return nil, fmt.Errorf("cannot rewind to %s: position has been purged (gtid_purged: %s)", to, h.purgedPos)
	}
	h.applied = to
	h.epoch++
	return &admin.CheckpointResult{
		PipelineID: h.pipelineID,
		Epoch:      h.epoch,
		Applied:    h.applied,
		RewoundTo:  to,
		UpdatedAt:  time.Now(),
	}, nil
}

func setupTestServer(t *testing.T, handler admin.PipelineAdminHandler) (*admin.Server, *admin.Client, func()) {
	t.Helper()
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "admin.sock")

	srv := admin.NewServer(sockPath, handler, nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("start admin server: %v", err)
	}

	cli := admin.NewClient(sockPath)
	cleanup := func() {
		_ = srv.Close()
	}
	return srv, cli, cleanup
}

// TestAD1_TopOverSocket kiểm tra AC1:
// `top` trả về lag, throughput, zone, tiến độ, ETA qua socket.
func TestAD1_TopOverSocket(t *testing.T) {
	handler := newMockHandler()
	_, client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	top, err := client.Top(ctx)
	if err != nil {
		t.Fatalf("client.Top failed: %v", err)
	}

	if top.PipelineID != "orders-sync" {
		t.Errorf("pipelineID mong muốn 'orders-sync', nhận được '%s'", top.PipelineID)
	}
	if top.LagSeconds != 0.12 {
		t.Errorf("lagSeconds mong muốn 0.12, nhận được %f", top.LagSeconds)
	}
	if top.Throughput != 2500.0 {
		t.Errorf("throughput mong muốn 2500.0, nhận được %f", top.Throughput)
	}
	if top.Zone != "green" {
		t.Errorf("zone mong muốn 'green', nhận được '%s'", top.Zone)
	}
	if top.Progress != 85.5 {
		t.Errorf("progress mong muốn 85.5, nhận được %f", top.Progress)
	}
	if top.ETASeconds != 120.0 {
		t.Errorf("etaSeconds mong muốn 120.0, nhận được %f", top.ETASeconds)
	}
}

// TestAD2_PauseAndResumeOverSocket kiểm tra AC2:
// `pause` và `resume --snapshot-only` hoạt động qua socket.
func TestAD2_PauseAndResumeOverSocket(t *testing.T) {
	handler := newMockHandler()
	_, client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. Gửi pause
	pauseRes, err := client.Pause(ctx)
	if err != nil {
		t.Fatalf("client.Pause failed: %v", err)
	}
	if !pauseRes.Paused || pauseRes.State != "paused" {
		t.Errorf("pause kết quả không đúng: %+v", pauseRes)
	}

	// 2. Gửi resume thường
	resumeRes, err := client.Resume(ctx, false)
	if err != nil {
		t.Fatalf("client.Resume failed: %v", err)
	}
	if !resumeRes.Resumed || resumeRes.State != "synced" || resumeRes.SnapshotOnly {
		t.Errorf("resume kết quả không đúng: %+v", resumeRes)
	}

	// 3. Gửi resume với cờ --snapshot-only
	resumeSnapRes, err := client.Resume(ctx, true)
	if err != nil {
		t.Fatalf("client.Resume snapshot-only failed: %v", err)
	}
	if !resumeSnapRes.Resumed || !resumeSnapRes.SnapshotOnly {
		t.Errorf("resume snapshot-only kết quả không đúng: %+v", resumeSnapRes)
	}
	if !handler.snapshotOnly {
		t.Errorf("handler chưa ghi nhận snapshotOnly = true")
	}
}

// TestAD3_ThrottleSetOverSocket kiểm tra AC3:
// `throttle set` đổi limit nóng qua admin socket.
func TestAD3_ThrottleSetOverSocket(t *testing.T) {
	handler := newMockHandler()
	_, client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Đổi source-iops soft limit lên 800
	res, err := client.ThrottleSet(ctx, 800, 0, true)
	if err != nil {
		t.Fatalf("client.ThrottleSet failed: %v", err)
	}

	if res.SourceIOPSSoft != 800 {
		t.Errorf("source_iops_soft mong muốn 800, nhận được %d", res.SourceIOPSSoft)
	}
	if handler.sourceSoft != 800 {
		t.Errorf("handler.sourceSoft mong muốn 800, nhận được %d", handler.sourceSoft)
	}

	// Đổi source-iops hard limit lên 1200
	res2, err := client.ThrottleSet(ctx, 1200, 0, false)
	if err != nil {
		t.Fatalf("client.ThrottleSet hard limit failed: %v", err)
	}
	if res2.SourceIOPSHard != 1200 {
		t.Errorf("source_iops_hard mong muốn 1200, nhận được %d", res2.SourceIOPSHard)
	}
}

// TestAD4_CheckpointShowAndRewindOverSocket kiểm tra AC4:
// `checkpoint show|rewind` hoạt động qua socket, có kiểm tra vị trí purged.
func TestAD4_CheckpointShowAndRewindOverSocket(t *testing.T) {
	handler := newMockHandler()
	_, client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. Checkpoint show
	showRes, err := client.CheckpointShow(ctx)
	if err != nil {
		t.Fatalf("client.CheckpointShow failed: %v", err)
	}
	if showRes.Applied != handler.applied || showRes.Epoch != handler.epoch {
		t.Errorf("show checkpoint không khớp: %+v", showRes)
	}

	// 2. Checkpoint rewind tới vị trí hợp lệ (> purged)
	validTarget := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-800"
	rewindRes, err := client.CheckpointRewind(ctx, validTarget)
	if err != nil {
		t.Fatalf("client.CheckpointRewind to valid pos failed: %v", err)
	}
	if rewindRes.Applied != validTarget || rewindRes.RewoundTo != validTarget {
		t.Errorf("checkpoint sau rewind không đúng: %+v", rewindRes)
	}
	if rewindRes.Epoch != 4 {
		t.Errorf("epoch mong muốn tăng lên 4, nhận được %d", rewindRes.Epoch)
	}

	// 3. Checkpoint rewind tới vị trí đã bị purged (< purgedPos) -> phải bị từ chối
	purgedTarget := "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-400"
	_, err = client.CheckpointRewind(ctx, purgedTarget)
	if err == nil {
		t.Fatalf("rewind tới vị trí purged phải bị lỗi nhưng lại thành công")
	}
}
