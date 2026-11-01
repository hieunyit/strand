package checkpoint_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

var (
	mysqlOnce sync.Once
	mysqlDB   *sql.DB
	mysqlStop func()

	pgOnce sync.Once
	pgDB   *sql.DB
	pgStop func()
)

func getMySQL(t *testing.T) *sql.DB {
	t.Helper()
	mysqlOnce.Do(func() {
		ctx := context.Background()
		cmd := []string{
			"--server-id=1001",
			"--gtid-mode=ON",
			"--enforce-gtid-consistency=ON",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
		}

		req := testcontainers.ContainerRequest{
			Image:        "mysql:8.4",
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": "dev",
				"MYSQL_DATABASE":      "strand",
			},
			Cmd:        cmd,
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Logf("failed to start mysql container: %v", err)
			return
		}

		host, err := container.Host(ctx)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to get mysql host: %v", err)
			return
		}

		portNat, err := container.MappedPort(ctx, "3306")
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to get mapped mysql port: %v", err)
			return
		}

		addr := net.JoinHostPort(host, portNat.Port())
		dsn := fmt.Sprintf("root:dev@tcp(%s)/strand?multiStatements=true&parseTime=true", addr)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to open mysql db: %v", err)
			return
		}

		// Chờ ping thành công
		for i := 0; i < 30; i++ {
			if err := db.Ping(); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		mysqlDB = db
		mysqlStop = func() {
			_ = db.Close()
			_ = container.Terminate(context.Background())
		}
	})

	if mysqlDB == nil {
		t.Fatal("MySQL container could not be started")
	}
	return mysqlDB
}

func getPostgres(t *testing.T) *sql.DB {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		cmd := []string{
			"postgres",
			"-c", "wal_level=logical",
			"-c", "max_wal_senders=10",
			"-c", "max_replication_slots=10",
		}

		req := testcontainers.ContainerRequest{
			Image:        "postgres:18",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "strand",
				"POSTGRES_PASSWORD": "dev",
				"POSTGRES_DB":       "strand",
			},
			Cmd:        cmd,
			WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(120 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Logf("failed to start postgres container: %v", err)
			return
		}

		host, err := container.Host(ctx)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to get pg host: %v", err)
			return
		}

		portNat, err := container.MappedPort(ctx, "5432")
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to get pg mapped port: %v", err)
			return
		}

		port, _ := strconv.Atoi(portNat.Port())
		dsn := fmt.Sprintf("postgres://strand:dev@%s:%d/strand?sslmode=disable", host, port)

		db, err := sql.Open("pgx", dsn)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to open pg db: %v", err)
			return
		}

		for i := 0; i < 30; i++ {
			if err := db.Ping(); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		pgDB = db
		pgStop = func() {
			_ = db.Close()
			_ = container.Terminate(context.Background())
		}
	})

	if pgDB == nil {
		t.Fatal("Postgres container could not be started")
	}
	return pgDB
}

func TestMain(m *testing.M) {
	code := m.Run()
	if mysqlStop != nil {
		mysqlStop()
	}
	if pgStop != nil {
		pgStop()
	}
	os.Exit(code)
}

// AC1: Tạo đủ 3 bảng trên MySQL và PG target
func TestAC1_EnsureMetadataTables(t *testing.T) {
	ctx := context.Background()

	// 1. Kiểm tra trên MySQL target
	t.Run("MySQL", func(t *testing.T) {
		db := getMySQL(t)
		store := checkpoint.NewMySQLStore(db, "test-pipeline-mysql", model.EngineMySQL)

		if err := store.EnsureTables(ctx); err != nil {
			t.Fatalf("MySQL EnsureTables failed: %v", err)
		}

		tables := []string{"_strand_pipeline", "_strand_checkpoint", "_strand_worker_delta"}
		for _, tbl := range tables {
			var count int
			q := fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)
			if err := db.QueryRowContext(ctx, q).Scan(&count); err != nil {
				t.Fatalf("table %s does not exist on MySQL target: %v", tbl, err)
			}
		}
		t.Logf("AC1: Đã xác thực đủ 3 bảng metadata trên MySQL target")
	})

	// 2. Kiểm tra trên PostgreSQL target
	t.Run("PostgreSQL", func(t *testing.T) {
		db := getPostgres(t)
		store := checkpoint.NewPGStore(db, "test-pipeline-pg", model.EnginePG)

		if err := store.EnsureTables(ctx); err != nil {
			t.Fatalf("PG EnsureTables failed: %v", err)
		}

		tables := []string{"_strand_pipeline", "_strand_checkpoint", "_strand_worker_delta"}
		for _, tbl := range tables {
			var count int
			q := fmt.Sprintf("SELECT COUNT(*) FROM %s", tbl)
			if err := db.QueryRowContext(ctx, q).Scan(&count); err != nil {
				t.Fatalf("table %s does not exist on PG target: %v", tbl, err)
			}
		}
		t.Logf("AC1: Đã xác thực đủ 3 bảng metadata trên PostgreSQL target")
	})

	t.Logf("AC1 PASS: Tạo đủ 3 bảng metadata trên cả MySQL và PostgreSQL target")
}

// AC2: TestSG1: instance thứ hai trả pipeline_locked
func TestSG1_PipelineLockSecondInstance(t *testing.T) {
	ctx := context.Background()

	// 1. Kiểm tra trên MySQL target (GET_LOCK)
	t.Run("MySQL_GET_LOCK", func(t *testing.T) {
		db := getMySQL(t)
		pipelineID := "pipeline-sg1-mysql"

		store1 := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)
		store2 := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)

		// Instance 1 lấy khóa thành công
		release1, err := store1.Lock(ctx, pipelineID)
		if err != nil {
			t.Fatalf("Instance 1 Lock failed: %v", err)
		}
		if release1 == nil {
			t.Fatalf("expected non-nil release func from Instance 1")
		}

		// Instance 2 cố gắng lấy khóa trên cùng pipeline -> phải trả về pipeline_locked
		_, err2 := store2.Lock(ctx, pipelineID)
		if err2 == nil {
			t.Fatalf("TestSG1 FAIL: Instance 2 phải bị từ chối khi Instance 1 đang giữ khóa")
		}
		if !errors.Is(err2, checkpoint.ErrPipelineLocked) {
			t.Fatalf("TestSG1 FAIL: Instance 2 expected ErrPipelineLocked ('pipeline_locked'), got: %v", err2)
		}
		t.Logf("Instance 2 bị từ chối chính xác với mã lỗi: %v", err2)

		// Instance 1 giải phóng khóa
		if err := release1(); err != nil {
			t.Fatalf("Instance 1 release failed: %v", err)
		}

		// Instance 2 giờ đây lấy khóa thành công
		release2, err := store2.Lock(ctx, pipelineID)
		if err != nil {
			t.Fatalf("Instance 2 Lock failed after release: %v", err)
		}
		_ = release2()
		t.Logf("Instance 2 lấy khóa thành công sau khi Instance 1 giải phóng")
	})

	// 2. Kiểm tra trên PostgreSQL target (pg_try_advisory_lock)
	t.Run("PostgreSQL_AdvisoryLock", func(t *testing.T) {
		db := getPostgres(t)
		pipelineID := "pipeline-sg1-pg"

		store1 := checkpoint.NewPGStore(db, pipelineID, model.EnginePG)
		store2 := checkpoint.NewPGStore(db, pipelineID, model.EnginePG)

		// Instance 1 lấy khóa thành công
		release1, err := store1.Lock(ctx, pipelineID)
		if err != nil {
			t.Fatalf("Instance 1 PG Lock failed: %v", err)
		}
		if release1 == nil {
			t.Fatalf("expected non-nil release func from Instance 1")
		}

		// Instance 2 cố gắng lấy khóa trên cùng pipeline -> phải trả về pipeline_locked
		_, err2 := store2.Lock(ctx, pipelineID)
		if err2 == nil {
			t.Fatalf("TestSG1 FAIL: Instance 2 PG phải bị từ chối khi Instance 1 đang giữ khóa")
		}
		if !errors.Is(err2, checkpoint.ErrPipelineLocked) {
			t.Fatalf("TestSG1 FAIL: Instance 2 PG expected ErrPipelineLocked, got: %v", err2)
		}
		t.Logf("Instance 2 PG bị từ chối chính xác với mã lỗi: %v", err2)

		// Instance 1 giải phóng khóa
		if err := release1(); err != nil {
			t.Fatalf("Instance 1 PG release failed: %v", err)
		}

		// Instance 2 giờ đây lấy khóa thành công
		release2, err := store2.Lock(ctx, pipelineID)
		if err != nil {
			t.Fatalf("Instance 2 PG Lock failed after release: %v", err)
		}
		_ = release2()
		t.Logf("Instance 2 PG lấy khóa thành công sau khi Instance 1 giải phóng")
	})

	t.Logf("AC2 PASS: TestSG1 instance thứ hai trả pipeline_locked trên cả MySQL và PG target")
}

// AC3: Epoch tăng mỗi lần khởi động
func TestAC3_EpochIncreasesOnStartup(t *testing.T) {
	ctx := context.Background()

	// 1. Kiểm tra trên MySQL target
	t.Run("MySQL", func(t *testing.T) {
		db := getMySQL(t)
		pipelineID := "pipeline-epoch-mysql"
		store := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)

		_ = store.EnsureTables(ctx)

		// Lần 1: Khởi động lần đầu -> epoch = 1
		ep1, err := store.AdvanceEpoch(ctx, pipelineID, "host-pod-1", "hash-1")
		if err != nil {
			t.Fatalf("AdvanceEpoch 1 failed: %v", err)
		}
		if ep1 != 1 || store.Epoch() != 1 {
			t.Fatalf("expected epoch 1 on first startup, got %d", ep1)
		}

		// Lần 2: Khởi động lại -> epoch = 2
		ep2, err := store.AdvanceEpoch(ctx, pipelineID, "host-pod-2", "hash-2")
		if err != nil {
			t.Fatalf("AdvanceEpoch 2 failed: %v", err)
		}
		if ep2 != 2 || store.Epoch() != 2 {
			t.Fatalf("expected epoch 2 on second startup, got %d", ep2)
		}

		// Lần 3: Khởi động lại -> epoch = 3
		ep3, err := store.AdvanceEpoch(ctx, pipelineID, "host-pod-3", "hash-3")
		if err != nil {
			t.Fatalf("AdvanceEpoch 3 failed: %v", err)
		}
		if ep3 != 3 || store.Epoch() != 3 {
			t.Fatalf("expected epoch 3 on third startup, got %d", ep3)
		}

		// Xác thực trên bảng _strand_pipeline và _strand_checkpoint
		var pipelineEpoch int64
		_ = db.QueryRowContext(ctx, "SELECT epoch FROM _strand_pipeline WHERE pipeline_id = ?", pipelineID).Scan(&pipelineEpoch)
		if pipelineEpoch != 3 {
			t.Fatalf("expected _strand_pipeline epoch 3, got %d", pipelineEpoch)
		}

		var checkpointEpoch int64
		_ = db.QueryRowContext(ctx, "SELECT epoch FROM _strand_checkpoint WHERE pipeline_id = ?", pipelineID).Scan(&checkpointEpoch)
		if checkpointEpoch != 3 {
			t.Fatalf("expected _strand_checkpoint epoch 3, got %d", checkpointEpoch)
		}
	})

	// 2. Kiểm tra trên PostgreSQL target
	t.Run("PostgreSQL", func(t *testing.T) {
		db := getPostgres(t)
		pipelineID := "pipeline-epoch-pg"
		store := checkpoint.NewPGStore(db, pipelineID, model.EnginePG)

		_ = store.EnsureTables(ctx)

		// Lần 1: Khởi động lần đầu -> epoch = 1
		ep1, err := store.AdvanceEpoch(ctx, pipelineID, "host-pod-1", "hash-1")
		if err != nil {
			t.Fatalf("PG AdvanceEpoch 1 failed: %v", err)
		}
		if ep1 != 1 || store.Epoch() != 1 {
			t.Fatalf("expected epoch 1 on first startup on PG, got %d", ep1)
		}

		// Lần 2: Khởi động lại -> epoch = 2
		ep2, err := store.AdvanceEpoch(ctx, pipelineID, "host-pod-2", "hash-2")
		if err != nil {
			t.Fatalf("PG AdvanceEpoch 2 failed: %v", err)
		}
		if ep2 != 2 || store.Epoch() != 2 {
			t.Fatalf("expected epoch 2 on second startup on PG, got %d", ep2)
		}

		// Lần 3: Khởi động lại -> epoch = 3
		ep3, err := store.AdvanceEpoch(ctx, pipelineID, "host-pod-3", "hash-3")
		if err != nil {
			t.Fatalf("PG AdvanceEpoch 3 failed: %v", err)
		}
		if ep3 != 3 || store.Epoch() != 3 {
			t.Fatalf("expected epoch 3 on third startup on PG, got %d", ep3)
		}

		// Xác thực trên bảng _strand_pipeline và _strand_checkpoint
		var pipelineEpoch int64
		_ = db.QueryRowContext(ctx, "SELECT epoch FROM _strand_pipeline WHERE pipeline_id = $1", pipelineID).Scan(&pipelineEpoch)
		if pipelineEpoch != 3 {
			t.Fatalf("expected _strand_pipeline epoch 3 on PG, got %d", pipelineEpoch)
		}

		var checkpointEpoch int64
		_ = db.QueryRowContext(ctx, "SELECT epoch FROM _strand_checkpoint WHERE pipeline_id = $1", pipelineID).Scan(&checkpointEpoch)
		if checkpointEpoch != 3 {
			t.Fatalf("expected _strand_checkpoint epoch 3 on PG, got %d", checkpointEpoch)
		}
	})

	t.Logf("AC3 PASS: Epoch tăng đơn điệu mỗi lần khởi động trên cả MySQL và PG target")
}
