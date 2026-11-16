package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func init() {
	// Dự án tắt Ryuk; container dùng chung được dọn bởi TerminatePostgres trong TestMain.
	if os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}

var pgShared struct {
	once sync.Once
	c    testcontainers.Container
	host string
	err  error
	n    int
	mu   sync.Mutex
}

// NewPostgresDSN trả DSN của một database RỖNG (mới tạo) trên một PostgreSQL dùng chung của process test
// (một container, 1 CPU, tên pgs-test-<pid>). Gọi TerminatePostgres trong TestMain để dọn container.
// Database của từng test bị xóa khi test kết thúc.
func NewPostgresDSN(t testing.TB) string {
	t.Helper()
	pgShared.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:17",
			Name:         fmt.Sprintf("pgs-test-%d", os.Getpid()),
			ExposedPorts: []string{"5432/tcp"},
			Env:          map[string]string{"POSTGRES_USER": "strand", "POSTGRES_PASSWORD": "dev", "POSTGRES_DB": "postgres"},
			Cmd:          []string{"postgres", "-c", "fsync=off", "-c", "max_connections=200"},
			WaitingFor: wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithDeadline(2 * time.Minute),
		}, Started: true})
		pgShared.c, pgShared.err = c, err
		if err != nil {
			return
		}
		// Giới hạn 1 CPU (máy test dùng chung); không phụ thuộc kiểu của docker SDK, lỗi thì bỏ qua.
		_ = exec.Command("docker", "update", "--cpus=1", c.GetContainerID()).Run()
		h, err := c.Host(ctx)
		if err != nil {
			pgShared.err = err
			return
		}
		p, err := c.MappedPort(ctx, "5432/tcp")
		if err != nil {
			pgShared.err = err
			return
		}
		pgShared.host = net.JoinHostPort(h, p.Port())
	})
	if pgShared.err != nil {
		t.Fatalf("start PostgreSQL container: %v", pgShared.err)
	}
	pgShared.mu.Lock()
	pgShared.n++
	name := fmt.Sprintf("t%d_%d", os.Getpid(), pgShared.n)
	pgShared.mu.Unlock()
	admin := "postgres://strand:dev@" + pgShared.host + "/postgres?sslmode=disable"
	db, err := sql.Open("pgx", admin)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err = db.Exec(`CREATE DATABASE ` + name); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("create database: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Cleanup(func() {
		d, err := sql.Open("pgx", admin)
		if err != nil {
			return
		}
		defer d.Close()
		_, _ = d.Exec(`DROP DATABASE IF EXISTS ` + name + ` WITH (FORCE)`)
	})
	return strings.Replace(admin, "/postgres?", "/"+name+"?", 1)
}

// TerminatePostgres dọn container dùng chung (gọi trong TestMain sau m.Run).
func TerminatePostgres() {
	if pgShared.c == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = pgShared.c.Terminate(ctx)
}
