package admin_test

import (
	"context"
	"fmt"
	"net"
	"os"
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

type snapshotPauseMockHandler struct {
	*mockHandler
	snapPaused bool
}

func (h *snapshotPauseMockHandler) PauseSnapshot(ctx context.Context) (*admin.PauseResult, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapPaused = true
	return &admin.PauseResult{
		State:  "snapshot_paused",
		Paused: true,
	}, nil
}

// TestAD5_PauseSnapshotOverSocket kiểm tra `pause_snapshot` qua socket:
// handler cài đặt SnapshotPauser trả Paused=true; handler không cài đặt trả lỗi.
func TestAD5_PauseSnapshotOverSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. Handler cài đặt SnapshotPauser
	withPauseSnap := &snapshotPauseMockHandler{mockHandler: newMockHandler()}
	_, clientOK, cleanupOK := setupTestServer(t, withPauseSnap)
	defer cleanupOK()

	res, err := clientOK.PauseSnapshot(ctx)
	if err != nil {
		t.Fatalf("client.PauseSnapshot failed: %v", err)
	}
	if !res.Paused || res.State != "snapshot_paused" {
		t.Errorf("PauseSnapshot kết quả không đúng: %+v", res)
	}
	if !withPauseSnap.snapPaused {
		t.Errorf("handler chưa ghi nhận snapPaused = true")
	}

	// 2. Handler không cài đặt SnapshotPauser -> trả lỗi
	withoutPauseSnap := newMockHandler()
	_, clientUnsupported, cleanupUnsupported := setupTestServer(t, withoutPauseSnap)
	defer cleanupUnsupported()

	if _, err := clientUnsupported.PauseSnapshot(ctx); err == nil {
		t.Fatalf("client.PauseSnapshot phải trả lỗi khi handler không cài đặt SnapshotPauser")
	}
}

// TestAD6_SocketAndDirPermissions kiểm tra quyền 0700 của thư mục tạo mới và file socket sau Start (spec/03 §4, §7),
// đồng thời không tự chmod thư mục đã tồn tại sẵn.
func TestAD6_SocketAndDirPermissions(t *testing.T) {
	baseDir := t.TempDir()

	// 1. Thư mục chưa tồn tại -> Start tạo mới với 0700, socket có mode 0700.
	newDir := filepath.Join(baseDir, "orders-sync")
	sockPath := filepath.Join(newDir, "admin.sock")

	srv := admin.NewServer(sockPath, newMockHandler(), nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start failed: %v", err)
	}
	defer func() { _ = srv.Close() }()

	dirInfo, err := os.Stat(newDir)
	if err != nil {
		t.Fatalf("stat socket dir: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("quyền thư mục tạo mới mong muốn 0700, nhận được %04o", perm)
	}

	sockInfo, err := os.Lstat(sockPath)
	if err != nil {
		t.Fatalf("lstat socket file: %v", err)
	}
	if perm := sockInfo.Mode().Perm(); perm != 0700 {
		t.Errorf("quyền file socket mong muốn 0700, nhận được %04o", perm)
	}

	// 2. Thư mục đã tồn tại sẵn với quyền 0755 -> Start không đổi quyền thư mục có sẵn, socket vẫn 0700.
	existingDir := filepath.Join(baseDir, "existing-dir")
	if err := os.Mkdir(existingDir, 0755); err != nil {
		t.Fatalf("mkdir existing-dir: %v", err)
	}
	if err := os.Chmod(existingDir, 0755); err != nil {
		t.Fatalf("chmod existing-dir: %v", err)
	}
	sockPath2 := filepath.Join(existingDir, "admin.sock")
	srv2 := admin.NewServer(sockPath2, newMockHandler(), nil)
	if err := srv2.Start(); err != nil {
		t.Fatalf("srv2.Start failed: %v", err)
	}
	defer func() { _ = srv2.Close() }()

	existingInfo, err := os.Stat(existingDir)
	if err != nil {
		t.Fatalf("stat existing-dir: %v", err)
	}
	if perm := existingInfo.Mode().Perm(); perm != 0755 {
		t.Errorf("quyền thư mục có sẵn không được bị đổi (mong muốn 0755, nhận được %04o)", perm)
	}

	sockInfo2, err := os.Lstat(sockPath2)
	if err != nil {
		t.Fatalf("lstat socket file 2: %v", err)
	}
	if perm := sockInfo2.Mode().Perm(); perm != 0700 {
		t.Errorf("quyền file socket 2 mong muốn 0700, nhận được %04o", perm)
	}
}

// TestAD7_StartFailsWhenLiveServerListening kiểm tra Start trả lỗi khi đã có server sống
// đang lắng nghe trên cùng đường dẫn socket và không làm hỏng socket của server đang chạy.
func TestAD7_StartFailsWhenLiveServerListening(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "admin.sock")

	srv1 := admin.NewServer(sockPath, newMockHandler(), nil)
	if err := srv1.Start(); err != nil {
		t.Fatalf("srv1.Start failed: %v", err)
	}
	defer func() { _ = srv1.Close() }()

	srv2 := admin.NewServer(sockPath, newMockHandler(), nil)
	if err := srv2.Start(); err == nil {
		_ = srv2.Close()
		t.Fatalf("srv2.Start phải thất bại khi srv1 đang lắng nghe trên cùng đường dẫn")
	}

	// Đảm bảo srv1 vẫn tiếp tục phục vụ bình thường (socket không bị xóa mất).
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	top, err := admin.NewClient(sockPath).Top(ctx)
	if err != nil {
		t.Fatalf("client.Top tới srv1 sau khi srv2.Start thất bại bị lỗi: %v", err)
	}
	if top.PipelineID != "orders-sync" {
		t.Errorf("pipelineID mong muốn 'orders-sync', nhận được %q", top.PipelineID)
	}
}

// TestAD8_StaleSocketReplacedOnStart kiểm tra file socket cũ không còn listener
// được thay thế thành công khi gọi Start.
func TestAD8_StaleSocketReplacedOnStart(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "admin.sock")

	// Tạo một UNIX socket rồi đóng listener nhưng giữ lại file socket trên đĩa (stale socket).
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix to create stale socket: %v", err)
	}
	if ul, ok := ln.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	if _, err := os.Lstat(sockPath); err != nil {
		t.Fatalf("stale socket file phải còn tồn tại trên đĩa trước khi Start: %v", err)
	}

	srv := admin.NewServer(sockPath, newMockHandler(), nil)
	if err := srv.Start(); err != nil {
		t.Fatalf("srv.Start phải thay thế được stale socket nhưng trả lỗi: %v", err)
	}
	defer func() { _ = srv.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	top, err := admin.NewClient(sockPath).Top(ctx)
	if err != nil {
		t.Fatalf("client.Top sau khi thay thế stale socket thất bại: %v", err)
	}
	if top.PipelineID != "orders-sync" {
		t.Errorf("pipelineID mong muốn 'orders-sync', nhận được %q", top.PipelineID)
	}
}
