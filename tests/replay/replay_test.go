package replay_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type BenchmarkReport struct {
	Workers      int            `json:"workers"`
	TxnCount     int            `json:"txn_count"`
	RowCount     int            `json:"row_count"`
	DurationSec  float64        `json:"duration_seconds"`
	TxnPerSec    float64        `json:"txn_per_sec"`
	RowsPerSec   float64        `json:"rows_per_sec"`
	DAGWidthHist map[string]int `json:"dag_width_histogram"`
	HotRowRatio  float64        `json:"hot_row_ratio"`
}

// Test replay xóa/tạo bảng, nên luôn dùng container riêng; không dò localhost/compose hay DSN môi trường.
func getMySQLDSNs(t *testing.T) (string, string) {
	t.Helper()
	start := func(serverID int) string {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
			Image: "mysql:8.4", ExposedPorts: []string{"3306/tcp"}, Env: map[string]string{"MYSQL_ROOT_PASSWORD": "test-only", "MYSQL_DATABASE": "app"},
			Cmd:        []string{fmt.Sprintf("--server-id=%d", serverID), "--gtid-mode=ON", "--enforce-gtid-consistency=ON", "--binlog-format=ROW", "--binlog-row-image=FULL", "--innodb-use-native-aio=OFF"},
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(90 * time.Second),
		}, Started: true})
		if c != nil {
			t.Cleanup(func() {
				if err := c.Terminate(context.Background()); err != nil {
					t.Errorf("terminate own replay container: %v", err)
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
		port, err := c.MappedPort(ctx, "3306/tcp")
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("root:test-only@tcp(%s)/app?parseTime=true&timeout=5s&readTimeout=30s&writeTimeout=30s", net.JoinHostPort(host, port.Port()))
	}
	return start(101), start(102)
}

// AC3: Replay tải genload từ mysql-src sang mysql-dst: checksum từng bảng bằng nhau.
// AC4: Chạy được với --workers 4, 8, 16 và xuất file JSON số liệu.
func TestAC3_AC4_ReplayBenchmark(t *testing.T) {
	// Nếu helper vô tình dùng lại DSN môi trường, socket không tồn tại này làm test đỏ trước mọi DDL.
	t.Setenv("MYSQL_SRC_DSN", "root:test-only@unix("+filepath.Join(t.TempDir(), "not-a-db.sock")+")/app")
	t.Setenv("MYSQL_DST_DSN", "root:test-only@unix("+filepath.Join(t.TempDir(), "not-a-db.sock")+")/app")
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srcDSN, dstDSN := getMySQLDSNs(t)
	t.Log("Using owned MySQL source and target test containers")

	srcDB, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatalf("failed to open mysql-src: %v", err)
	}
	defer srcDB.Close()

	dstDB, err := sql.Open("mysql", dstDSN)
	if err != nil {
		t.Fatalf("failed to open mysql-dst: %v", err)
	}
	defer dstDB.Close()

	if err := srcDB.PingContext(ctx); err != nil {
		t.Fatalf("mysql-src không phản hồi: %v", err)
	}
	if err := dstDB.PingContext(ctx); err != nil {
		t.Fatalf("mysql-dst không phản hồi: %v", err)
	}

	// 1. Dọn sạch dữ liệu cũ trên cả hai bên
	_, _ = srcDB.ExecContext(ctx, "DROP TABLE IF EXISTS gen_orders;")
	_, _ = dstDB.ExecContext(ctx, "DROP TABLE IF EXISTS gen_orders;")

	// 2. Ghi nhận vị trí binlog trước khi genload
	var startFile string
	var startPos uint32
	var d1, d2, d3 sql.NullString
	err = srcDB.QueryRowContext(ctx, "SHOW BINARY LOG STATUS").Scan(&startFile, &startPos, &d1, &d2, &d3)
	if err != nil {
		t.Fatalf("failed to query SHOW BINARY LOG STATUS: %v", err)
	}
	t.Logf("Recorded binlog start position: %s:%d", startFile, startPos)

	tmpDir := t.TempDir()

	// 3. Chạy tools/genload để tạo schema và sinh tải có hot row
	genloadOut := filepath.Join(tmpDir, "genload.json")
	genCmd := exec.CommandContext(ctx, "go", "run", "../../tools/genload",
		"-dsn", srcDSN,
		"-count", "300",
		"-hot-ratio", "0.25",
		"-out", genloadOut,
	)
	genOutput, err := genCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("genload failed: %v, output: %s", err, string(genOutput))
	}
	t.Logf("genload output: %s", string(genOutput))

	var srcCount int
	err = srcDB.QueryRowContext(ctx, "SELECT count(*) FROM gen_orders").Scan(&srcCount)
	if err != nil || srcCount == 0 {
		t.Fatalf("genload không sinh hàng nào: count=%d, err=%v", srcCount, err)
	}
	t.Logf("mysql-src có %d rows trong gen_orders", srcCount)

	// 4. Chạy replay với các mức workers: 4, 8, 16 (AC4)
	workerLevels := []int{4, 8, 16}

	for _, workers := range workerLevels {
		// Dọn bảng dst trước mỗi lần replay
		_, _ = dstDB.ExecContext(ctx, "DROP TABLE IF EXISTS gen_orders;")

		outFile := filepath.Join(tmpDir, fmt.Sprintf("benchmark_w%d.json", workers))
		replayCmd := exec.CommandContext(ctx, "go", "run", "../../cmd/replay",
			"-src", srcDSN,
			"-dst", dstDSN,
			"-workers", fmt.Sprintf("%d", workers),
			"-max-txns", "300",
			"-start-file", startFile,
			"-start-pos", fmt.Sprintf("%d", startPos),
			"-out", outFile,
		)
		replayOutput, err := replayCmd.CombinedOutput()
		if err != nil {
			t.Fatalf("replay with workers=%d failed: %v, output: %s", workers, err, string(replayOutput))
		}

		// Kiểm tra file JSON số liệu xuất ra (AC4)
		data, err := os.ReadFile(outFile)
		if err != nil {
			t.Fatalf("không tìm thấy file JSON benchmark %s: %v", outFile, err)
		}

		var rep BenchmarkReport
		if err := json.Unmarshal(data, &rep); err != nil {
			t.Fatalf("JSON benchmark %s không hợp lệ: %v", outFile, err)
		}

		if rep.Workers != workers {
			t.Errorf("expected workers=%d, got %d", workers, rep.Workers)
		}
		if rep.TxnCount == 0 {
			t.Errorf("workers=%d: txnCount is 0", workers)
		}
		if rep.TxnPerSec <= 0 {
			t.Errorf("workers=%d: txn_per_sec <= 0 (%f)", workers, rep.TxnPerSec)
		}
		if rep.RowsPerSec <= 0 {
			t.Errorf("workers=%d: rows_per_sec <= 0 (%f)", workers, rep.RowsPerSec)
		}
		if len(rep.DAGWidthHist) == 0 {
			t.Errorf("workers=%d: dag_width_histogram is empty", workers)
		}
		if rep.HotRowRatio <= 0 {
			t.Errorf("workers=%d: hot_row_ratio <= 0 (%f)", workers, rep.HotRowRatio)
		}

		t.Logf("[Workers %d] Replay thành công: %.1f txn/s, %.1f rows/s, hot_ratio=%.2f, dag_hist=%v",
			workers, rep.TxnPerSec, rep.RowsPerSec, rep.HotRowRatio, rep.DAGWidthHist)

		// 5. Kiểm tra CHECKSUM TABLE giữa src và dst (AC3)
		var srcTable, dstTable string
		var srcChecksum, dstChecksum uint64

		err = srcDB.QueryRowContext(ctx, "CHECKSUM TABLE gen_orders").Scan(&srcTable, &srcChecksum)
		if err != nil {
			t.Fatalf("failed to get checksum on src: %v", err)
		}

		err = dstDB.QueryRowContext(ctx, "CHECKSUM TABLE gen_orders").Scan(&dstTable, &dstChecksum)
		if err != nil {
			t.Fatalf("failed to get checksum on dst: %v", err)
		}

		var dstCount int
		_ = dstDB.QueryRowContext(ctx, "SELECT count(*) FROM gen_orders").Scan(&dstCount)

		if srcChecksum != dstChecksum || srcCount != dstCount {
			t.Fatalf("AC3 thất bại [workers %d]: Checksum hoặc count lệch: src(rows=%d, chk=%d) vs dst(rows=%d, chk=%d)",
				workers, srcCount, srcChecksum, dstCount, dstChecksum)
		}
		t.Logf("AC3 PASS [Workers %d]: Checksum khớp hoàn hảo: %d (rows=%d)", workers, dstChecksum, dstCount)
	}

	t.Log("AC3 & AC4 PASS: Đã kiểm chứng toàn bộ workers 4, 8, 16 với checksum bằng nhau 100%")
}
