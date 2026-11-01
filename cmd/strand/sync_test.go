package main_test

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	main "strand/cmd/strand"
)

// TestSD1_SystemdUnitAndFKEManifest kiểm tra AC1:
// Unit systemd có RestartPreventExitStatus=2 3 4 5 10 và manifest FKE đúng spec/03 §4, §5.
func TestSD1_SystemdUnitAndFKEManifest(t *testing.T) {
	// 1. Kiểm tra unit systemd
	servicePath := "../../deploy/systemd/strand@.service"
	content, err := os.ReadFile(servicePath)
	if err != nil {
		t.Fatalf("không đọc được file %s: %v", servicePath, err)
	}
	s := string(content)

	if !strings.Contains(s, "RestartPreventExitStatus=2 3 4 5 10") {
		t.Errorf("thiếu RestartPreventExitStatus=2 3 4 5 10 trong unit systemd")
	}
	if !strings.Contains(s, "KillSignal=SIGTERM") {
		t.Errorf("thiếu KillSignal=SIGTERM trong unit systemd")
	}
	if !strings.Contains(s, "TimeoutStopSec=120") {
		t.Errorf("thiếu TimeoutStopSec=120 trong unit systemd")
	}

	// 2. Kiểm tra manifest FKE StatefulSet
	fkePath := "../../deploy/fke/statefulset.yaml"
	fkeContent, err := os.ReadFile(fkePath)
	if err != nil {
		t.Fatalf("không đọc được file %s: %v", fkePath, err)
	}
	f := string(fkeContent)

	if !strings.Contains(f, "terminationGracePeriodSeconds: 150") {
		t.Errorf("thiếu terminationGracePeriodSeconds: 150 trong FKE manifest")
	}
	if !strings.Contains(f, "path: /healthz") {
		t.Errorf("thiếu livenessProbe path: /healthz trong FKE manifest")
	}
	if !strings.Contains(f, "readOnlyRootFilesystem: true") {
		t.Errorf("thiếu readOnlyRootFilesystem: true trong FKE manifest")
	}
}

type testFlusher struct {
	flushed atomic.Bool
}

func (f *testFlusher) FlushCheckpoint(ctx context.Context) error {
	f.flushed.Store(true)
	return nil
}

// TestSD2_SIGTERM_FlushesCheckpointAndExitsInTime kiểm tra AC2:
// Khi nhận SIGTERM, coordinator ghi checkpoint rồi thoát an toàn trong thời gian cho phép.
func TestSD2_SIGTERM_FlushesCheckpointAndExitsInTime(t *testing.T) {
	flusher := &testFlusher{}
	timeout := 5 * time.Second
	coord := main.NewSyncCoordinator(flusher, timeout, nil)

	errCh := make(chan error, 1)
	start := time.Now()

	go func() {
		errCh <- coord.Run(context.Background())
	}()

	// Đợi coordinator khởi động xong vòng lặp
	time.Sleep(50 * time.Millisecond)

	// Gửi tín hiệu SIGTERM
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("gửi SIGTERM thất bại: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("coordinator thoát với lỗi: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("coordinator không thoát trong thời gian cho phép sau khi nhận SIGTERM")
	}

	elapsed := time.Since(start)
	if !flusher.flushed.Load() {
		t.Fatalf("chưa thực hiện ghi checkpoint khi nhận SIGTERM")
	}
	t.Logf("SIGTERM graceful shutdown hoàn tất thành công trong %v, checkpoint đã được flush", elapsed)
}
