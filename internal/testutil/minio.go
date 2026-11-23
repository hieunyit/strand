// Package testutil chứa fixture do từng test tạo và dọn; không dùng dịch vụ compose có sẵn.
package testutil

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// PinnedMinIOImage là digest cố định của MinIO container dùng cho fixture kiểm thử.
const PinnedMinIOImage = "bitnamilegacy/minio:2025.4.22-debian-12-r0@sha256:a26e42a4d6550d2fb7708c8ceefc60474b42c124b6f3273a081721ddf1d86b20"

// MinIOEntrypoint runs the pinned MinIO binary directly with test-owned data.
const MinIOEntrypoint = "/opt/bitnami/minio/bin/minio"

// MinIOEndpoint trả endpoint của MinIO riêng, cùng thông tin đăng nhập test minioadmin/minioadmin.
func MinIOEndpoint(t testing.TB) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: PinnedMinIOImage, Entrypoint: []string{MinIOEntrypoint}, ExposedPorts: []string{"9000/tcp"}, Env: map[string]string{"MINIO_ROOT_USER": "minioadmin", "MINIO_ROOT_PASSWORD": "minioadmin"}, Cmd: []string{"server", "/tmp/minio-data"},
		WaitingFor: wait.ForHTTP("/minio/health/ready").WithPort("9000/tcp").WithStartupTimeout(90 * time.Second),
	}, Started: true})
	if c != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := c.Terminate(ctx); err != nil {
				t.Errorf("terminate own MinIO: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + net.JoinHostPort(host, port.Port())

	// Probe ngắn sau /minio/health/ready để đảm bảo S3 subsystem đã sẵn sàng (tránh transient 503 lúc chuyển trạng thái).
	probeClient := &netClient{Timeout: 2 * time.Second}
	probeDeadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := probeClient.Get(endpoint + "/minio/health/ready")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			// Kiểm tra thêm S3 API endpoint
			s3Resp, s3Err := probeClient.Head(endpoint + "/probe-ready")
			if s3Err == nil {
				s3Resp.Body.Close()
				if s3Resp.StatusCode != 503 {
					break
				}
			}
		}
		if time.Now().After(probeDeadline) {
			t.Fatalf("MinIO S3 subsystem did not become ready within probe deadline at %s", endpoint)
		}
		time.Sleep(20 * time.Millisecond)
	}

	return endpoint
}

type netClient = http.Client
