package watermark_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/tests/experiments/watermark"
)

func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

func getIterations() int {
	if s := os.Getenv("WATERMARK_ITERATIONS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 10000
}

func getMySQL84Addr(t *testing.T) (string, func()) {
	t.Helper()
	return startTestContainerMySQL(t, "mysql:8.4")
}

func startTestContainerMySQL(t *testing.T, image string) (string, func()) {
	t.Helper()
	ctx := context.Background()

	cmd := []string{
		"--server-id=101",
		"--gtid-mode=ON",
		"--enforce-gtid-consistency=ON",
		"--binlog-format=ROW",
		"--binlog-row-image=FULL",
		"--binlog-row-metadata=FULL",
	}

	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "dev",
		},
		Cmd:        cmd,
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container %s: %v", image, err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("failed to get container host: %v", err)
	}

	portNat, err := container.MappedPort(ctx, "3306")
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("failed to get mapped port: %v", err)
	}

	addr := net.JoinHostPort(host, portNat.Port())
	dsn := fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true", addr)

	// Đợi kết nối DB sẵn sàng và tạo database test_watermark
	var db *sql.DB
	for i := 0; i < 30; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil && db.Ping() == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if db != nil {
		_, _ = db.Exec("CREATE DATABASE IF NOT EXISTS test_watermark;")
		db.Close()
	}

	cleanup := func() {
		_ = container.Terminate(context.Background())
	}
	return addr, cleanup
}

// AC1: Chạy N vòng (mặc định 10.000) trên MySQL 8.4; xuất JSON {iterations, violations}.
func TestAC1_Watermark_MySQL84(t *testing.T) {
	addr, cleanup := getMySQL84Addr(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outJSON := filepath.Join(tmpDir, "watermark_mysql84.json")
	iters := getIterations()

	t.Logf("Chạy AC1 trên MySQL 8.4 với %d vòng lặp...", iters)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	cfg := watermark.Config{
		Addr:            addr,
		User:            "root",
		Password:        "dev",
		DBName:          "test_watermark",
		Iterations:      iters,
		NumWriters:      4,
		NegativeControl: false,
		OutputPath:      outJSON,
	}

	res, err := watermark.RunWatermarkExperiment(ctx, cfg)
	if err != nil {
		t.Fatalf("Thí nghiệm watermark MySQL 8.4 thất bại: %v", err)
	}

	t.Logf("MySQL 8.4 Kết quả: Iterations=%d, Violations=%d, Duration=%.2fs, Version=%s, Writes=%d",
		res.Iterations, res.Violations, res.DurationSec, res.MySQLVersion, res.TotalWrites)

	if res.Violations > 0 {
		t.Fatalf("AC1 FAIL trên MySQL 8.4: Phát hiện %d vi phạm bất biến RS-2!", res.Violations)
	}

	if _, err := os.Stat(outJSON); err != nil {
		t.Fatalf("Không tìm thấy file JSON kết quả: %v", err)
	}
	t.Logf("AC1 PASS MySQL 8.4: 100%% các GTID trong low watermark đều visible trong SELECT (0 violations)")
}

// AC1: Chạy N vòng (mặc định 10.000) trên MySQL 9.7; xuất JSON {iterations, violations}.
func TestAC1_Watermark_MySQL97(t *testing.T) {
	addr, cleanup := startTestContainerMySQL(t, "mysql:9.7")
	defer cleanup()

	tmpDir := t.TempDir()
	outJSON := filepath.Join(tmpDir, "watermark_mysql97.json")
	iters := getIterations()

	t.Logf("Chạy AC1 trên MySQL 9.7 với %d vòng lặp...", iters)
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()

	cfg := watermark.Config{
		Addr:            addr,
		User:            "root",
		Password:        "dev",
		DBName:          "test_watermark",
		Iterations:      iters,
		NumWriters:      4,
		NegativeControl: false,
		OutputPath:      outJSON,
	}

	res, err := watermark.RunWatermarkExperiment(ctx, cfg)
	if err != nil {
		t.Fatalf("Thí nghiệm watermark MySQL 9.7 thất bại: %v", err)
	}

	t.Logf("MySQL 9.7 Kết quả: Iterations=%d, Violations=%d, Duration=%.2fs, Version=%s, Writes=%d",
		res.Iterations, res.Violations, res.DurationSec, res.MySQLVersion, res.TotalWrites)

	if res.Violations > 0 {
		t.Fatalf("AC1 FAIL trên MySQL 9.7: Phát hiện %d vi phạm bất biến RS-2!", res.Violations)
	}

	if _, err := os.Stat(outJSON); err != nil {
		t.Fatalf("Không tìm thấy file JSON kết quả: %v", err)
	}
	t.Logf("AC1 PASS MySQL 9.7: 100%% các GTID trong low watermark đều visible trong SELECT (0 violations)")
}

// AC2: Tự kiểm: bản cố ý đọc low SAU câu SELECT phải phát hiện violations > 0.
func TestAC2_Watermark_NegativeSelfCheck(t *testing.T) {
	addr, cleanup := getMySQL84Addr(t)
	defer cleanup()

	tmpDir := t.TempDir()
	outJSON := filepath.Join(tmpDir, "watermark_negative.json")

	iters := 1000
	t.Logf("Chạy AC2 Negative Self-Check trên MySQL 8.4 (%d vòng)...", iters)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := watermark.Config{
		Addr:            addr,
		User:            "root",
		Password:        "dev",
		DBName:          "test_watermark",
		Iterations:      iters,
		NumWriters:      4,
		NegativeControl: true, // Cố ý đọc low SAU câu SELECT
		OutputPath:      outJSON,
	}

	res, err := watermark.RunWatermarkExperiment(ctx, cfg)
	if err != nil {
		t.Fatalf("Thí nghiệm Negative Self-Check thất bại: %v", err)
	}

	t.Logf("Negative Self-Check Kết quả: Iterations=%d, Violations=%d, Writes=%d",
		res.Iterations, res.Violations, res.TotalWrites)

	if res.Violations == 0 {
		t.Fatalf("AC2 FAIL: Bản cố ý đọc low SAU câu SELECT KHÔNG phát hiện được vi phạm nào (kỳ vọng violations > 0)!")
	}

	t.Logf("AC2 PASS: Đã bắt được %d vi phạm khi đọc low sau SELECT, tự kiểm tra chứng minh độ nhạy của checker thành công!", res.Violations)
}
