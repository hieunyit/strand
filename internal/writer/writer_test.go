package writer_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/writer"
)

func init() {
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

var (
	mysqlOnce sync.Once
	mysqlDB   *sql.DB
	mysqlDSN  string

	pgOnce sync.Once
	pgDB   *sql.DB
)

func getMySQL(t *testing.T) (*sql.DB, string) {
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
		// clientFoundRows=false để kiểm thử hành vi MySQL mặc định
		mysqlDSN = fmt.Sprintf("root:dev@tcp(%s)/strand?multiStatements=true&parseTime=true&clientFoundRows=false", addr)

		db, err := sql.Open("mysql", mysqlDSN)
		if err != nil {
			_ = container.Terminate(ctx)
			t.Logf("failed to open mysql db: %v", err)
			return
		}

		for i := 0; i < 30; i++ {
			if err := db.Ping(); err == nil {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		mysqlDB = db
	})

	if mysqlDB == nil {
		t.Fatal("MySQL container failed to start")
	}
	return mysqlDB, mysqlDSN
}

func getPostgres(t *testing.T) *sql.DB {
	t.Helper()
	pgOnce.Do(func() {
		ctx := context.Background()
		req := testcontainers.ContainerRequest{
			Image:        "postgres:18",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "dev",
				"POSTGRES_DB":       "strand",
			},
			WaitingFor: wait.ForListeningPort("5432/tcp").WithStartupTimeout(180 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Logf("failed to start pg container: %v", err)
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
			t.Logf("failed to get mapped pg port: %v", err)
			return
		}

		addr := net.JoinHostPort(host, portNat.Port())
		dsn := fmt.Sprintf("postgres://postgres:dev@%s/strand?sslmode=disable", addr)

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
	})

	if pgDB == nil {
		t.Fatal("Postgres container failed to start")
	}
	return pgDB
}

// invariant: SG-1
// AC1: TestSG1_StaleEpochRollsBack: epoch cũ thì rollback, trả fenced (exit code 4)
func TestSG1_StaleEpochRollsBack(t *testing.T) {
	ctx := context.Background()

	t.Run("MySQL", func(t *testing.T) {
		db, _ := getMySQL(t)
		pipelineID := fmt.Sprintf("sg1-my-%d", time.Now().UnixNano())
		store := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)
		defer store.Close()

		if err := store.EnsureTables(ctx); err != nil {
			t.Fatalf("ensure tables: %v", err)
		}

		// Tạo bảng ứng dụng items
		_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test_items_sg1_my (id INT PRIMARY KEY, val VARCHAR(64))")
		if err != nil {
			t.Fatalf("create test table: %v", err)
		}
		_, _ = db.ExecContext(ctx, "TRUNCATE TABLE test_items_sg1_my")

		// Khởi tạo epoch = 1
		epoch, err := store.AdvanceEpoch(ctx, pipelineID, "owner-1", "hash-1")
		if err != nil {
			t.Fatalf("advance epoch: %v", err)
		}
		if epoch != 1 {
			t.Fatalf("expected epoch 1, got %d", epoch)
		}

		// Khởi tạo worker 1 trên DB
		w := writer.NewMySQLWriter(db, pipelineID, 1, 1)
		if err := w.EnsureWorkerRow(ctx); err != nil {
			t.Fatalf("ensure worker row: %v", err)
		}

		// Instance khác khởi động và nâng epoch lên 2 trên storage
		epoch2, err := store.AdvanceEpoch(ctx, pipelineID, "owner-2", "hash-2")
		if err != nil {
			t.Fatalf("advance epoch 2: %v", err)
		}
		if epoch2 != 2 {
			t.Fatalf("expected epoch 2, got %d", epoch2)
		}

		// Writer cũ (w) vẫn giữ epoch cũ = 1 cố gắng ApplyBatch
		batch := []*model.Txn{
			{
				Rows: []model.RowChange{
					{
						Table: model.TableID{Schema: "strand", Table: "test_items_sg1_my"},
						Op:    model.OpInsert,
						Key:   []model.Value{100},
						Cols: []model.ColValue{
							{Name: "id", Value: 100},
							{Name: "val", Value: "stale_write"},
						},
					},
				},
			},
		}
		delta := writer.DeltaRow{
			PipelineID: pipelineID,
			WorkerID:   1,
			Epoch:      1, // Stale epoch!
			Applied:    "uuid:1-10",
		}

		applyErr := w.ApplyBatch(ctx, batch, delta)
		if applyErr == nil {
			t.Fatal("kỳ vọng ApplyBatch thất bại với lỗi fenced khi epoch cũ")
		}

		// Kiểm tra exit code = 4 (exitcode.Fenced)
		exitCode := exitcode.GetExitCode(applyErr)
		if exitCode != exitcode.Fenced {
			t.Fatalf("kỳ vọng exit code %d (Fenced), got %d (err: %v)", exitcode.Fenced, exitCode, applyErr)
		}

		// Kiểm tra atomicity: Dữ liệu KHÔNG được ghi vào bảng (đã rollback thành công)
		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_items_sg1_my WHERE id = 100").Scan(&count)
		if err != nil {
			t.Fatalf("query count: %v", err)
		}
		if count != 0 {
			t.Fatalf("vi phạm fencing: dữ liệu vẫn bị ghi vào bảng dù epoch cũ (count=%d)!", count)
		}

		// Kiểm tra row delta trên DB không bị ghi đè
		var dbApplied string
		err = db.QueryRowContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id = ? AND worker_id = ?", pipelineID, 1).Scan(&dbApplied)
		if err != nil {
			t.Fatalf("query worker delta: %v", err)
		}
		if dbApplied == "uuid:1-10" {
			t.Fatal("vi phạm fencing: worker delta bị ghi đè với epoch cũ!")
		}
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := getPostgres(t)
		pipelineID := fmt.Sprintf("sg1-pg-%d", time.Now().UnixNano())
		store := checkpoint.NewPGStore(db, pipelineID, model.EnginePG)
		defer store.Close()

		if err := store.EnsureTables(ctx); err != nil {
			t.Fatalf("ensure tables: %v", err)
		}

		_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test_items_sg1_pg (id INT PRIMARY KEY, val text)")
		if err != nil {
			t.Fatalf("create test table: %v", err)
		}
		_, _ = db.ExecContext(ctx, "TRUNCATE TABLE test_items_sg1_pg")

		epoch, err := store.AdvanceEpoch(ctx, pipelineID, "owner-1", "hash-1")
		if err != nil {
			t.Fatalf("advance epoch: %v", err)
		}
		if epoch != 1 {
			t.Fatalf("expected epoch 1, got %d", epoch)
		}

		w := writer.NewPGWriter(db, pipelineID, 1, 1)
		if err := w.EnsureWorkerRow(ctx); err != nil {
			t.Fatalf("ensure pg worker row: %v", err)
		}

		// Advance epoch lên 2
		_, err = store.AdvanceEpoch(ctx, pipelineID, "owner-2", "hash-2")
		if err != nil {
			t.Fatalf("advance epoch 2: %v", err)
		}

		// Writer cũ gọi ApplyBatch với epoch 1
		batch := []*model.Txn{
			{
				Rows: []model.RowChange{
					{
						Table: model.TableID{Schema: "public", Table: "test_items_sg1_pg"},
						Op:    model.OpInsert,
						Key:   []model.Value{200},
						Cols: []model.ColValue{
							{Name: "id", Value: 200},
							{Name: "val", Value: "stale_write_pg"},
						},
					},
				},
			},
		}
		delta := writer.DeltaRow{
			PipelineID: pipelineID,
			WorkerID:   1,
			Epoch:      1,
			Applied:    "100",
		}

		applyErr := w.ApplyBatch(ctx, batch, delta)
		if applyErr == nil {
			t.Fatal("kỳ vọng PG ApplyBatch thất bại với lỗi fenced khi epoch cũ")
		}

		exitCode := exitcode.GetExitCode(applyErr)
		if exitCode != exitcode.Fenced {
			t.Fatalf("kỳ vọng exit code %d (Fenced), got %d (err: %v)", exitcode.Fenced, exitCode, applyErr)
		}

		var count int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM test_items_sg1_pg WHERE id = 200").Scan(&count)
		if err != nil {
			t.Fatalf("query count: %v", err)
		}
		if count != 0 {
			t.Fatalf("vi phạm fencing PG: dữ liệu vẫn bị ghi vào bảng dù epoch cũ!")
		}
	})
}

// invariant: CP-1
// AC2: TestCP1_DeltaSameTxn: delta ghi cùng transaction với dữ liệu
func TestCP1_DeltaSameTxn(t *testing.T) {
	ctx := context.Background()

	t.Run("MySQL", func(t *testing.T) {
		db, _ := getMySQL(t)
		pipelineID := fmt.Sprintf("cp1-my-%d", time.Now().UnixNano())
		store := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)
		defer store.Close()

		if err := store.EnsureTables(ctx); err != nil {
			t.Fatalf("ensure tables: %v", err)
		}

		_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test_items_cp1_my (id INT PRIMARY KEY, val VARCHAR(64))")
		if err != nil {
			t.Fatalf("create test table: %v", err)
		}
		_, _ = db.ExecContext(ctx, "TRUNCATE TABLE test_items_cp1_my")

		epoch, err := store.AdvanceEpoch(ctx, pipelineID, "owner", "hash")
		if err != nil {
			t.Fatalf("advance epoch: %v", err)
		}

		w := writer.NewMySQLWriter(db, pipelineID, 1, epoch)
		if err := w.EnsureWorkerRow(ctx); err != nil {
			t.Fatalf("ensure worker row: %v", err)
		}

		// 1. Commit thành công: Cả dữ liệu và delta đều được lưu
		batch1 := []*model.Txn{
			{
				Rows: []model.RowChange{
					{
						Table: model.TableID{Schema: "strand", Table: "test_items_cp1_my"},
						Op:    model.OpInsert,
						Key:   []model.Value{1},
						Cols: []model.ColValue{
							{Name: "id", Value: 1},
							{Name: "val", Value: "val-1"},
						},
					},
				},
			},
		}
		delta1 := writer.DeltaRow{
			PipelineID: pipelineID,
			WorkerID:   1,
			Epoch:      epoch,
			Applied:    "uuid:1-1",
		}

		if err := w.ApplyBatch(ctx, batch1, delta1); err != nil {
			t.Fatalf("apply batch 1 thất bại: %v", err)
		}

		// Kiểm tra cả 2 đã được lưu
		var val string
		err = db.QueryRowContext(ctx, "SELECT val FROM test_items_cp1_my WHERE id = 1").Scan(&val)
		if err != nil || val != "val-1" {
			t.Fatalf("kỳ vọng dữ liệu val-1 đã được lưu, got val=%s, err=%v", val, err)
		}
		var applied string
		err = db.QueryRowContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id = ? AND worker_id = ?", pipelineID, 1).Scan(&applied)
		if err != nil || applied != "uuid:1-1" {
			t.Fatalf("kỳ vọng delta uuid:1-1 đã được lưu, got applied=%s, err=%v", applied, err)
		}

		// 2. Thất bại giữa chừng (lỗi SQL trong dữ liệu): Cả dữ liệu và delta đều bị ROLLBACK
		batchErr := []*model.Txn{
			{
				Rows: []model.RowChange{
					{
						Table: model.TableID{Schema: "strand", Table: "table_non_existent_fail"},
						Op:    model.OpInsert,
						Key:   []model.Value{999},
					},
				},
			},
		}
		deltaErr := writer.DeltaRow{
			PipelineID: pipelineID,
			WorkerID:   1,
			Epoch:      epoch,
			Applied:    "uuid:1-999",
		}

		err = w.ApplyBatch(ctx, batchErr, deltaErr)
		if err == nil {
			t.Fatal("kỳ vọng ApplyBatch gặp lỗi SQL")
		}

		// Kiểm tra delta KHÔNG bị thay đổi sang uuid:1-999
		err = db.QueryRowContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id = ? AND worker_id = ?", pipelineID, 1).Scan(&applied)
		if err != nil {
			t.Fatalf("query delta: %v", err)
		}
		if applied != "uuid:1-1" {
			t.Fatalf("vi phạm nguyên tử tính CP-1: delta đã bị cập nhật dù transaction dữ liệu lỗi (applied=%s)", applied)
		}
	})

	t.Run("PostgreSQL", func(t *testing.T) {
		db := getPostgres(t)
		pipelineID := fmt.Sprintf("cp1-pg-%d", time.Now().UnixNano())
		store := checkpoint.NewPGStore(db, pipelineID, model.EnginePG)
		defer store.Close()

		if err := store.EnsureTables(ctx); err != nil {
			t.Fatalf("ensure tables: %v", err)
		}

		_, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS test_items_cp1_pg (id INT PRIMARY KEY, val text)")
		if err != nil {
			t.Fatalf("create test table: %v", err)
		}
		_, _ = db.ExecContext(ctx, "TRUNCATE TABLE test_items_cp1_pg")

		epoch, err := store.AdvanceEpoch(ctx, pipelineID, "owner", "hash")
		if err != nil {
			t.Fatalf("advance epoch: %v", err)
		}

		w := writer.NewPGWriter(db, pipelineID, 1, epoch)
		if err := w.EnsureWorkerRow(ctx); err != nil {
			t.Fatalf("ensure pg worker row: %v", err)
		}

		batch := []*model.Txn{
			{
				Rows: []model.RowChange{
					{
						Table: model.TableID{Schema: "public", Table: "test_items_cp1_pg"},
						Op:    model.OpInsert,
						Key:   []model.Value{10},
						Cols: []model.ColValue{
							{Name: "id", Value: 10},
							{Name: "val", Value: "pg-val-10"},
						},
					},
				},
			},
		}
		delta := writer.DeltaRow{
			PipelineID: pipelineID,
			WorkerID:   1,
			Epoch:      epoch,
			Applied:    "100",
		}

		if err := w.ApplyBatch(ctx, batch, delta); err != nil {
			t.Fatalf("apply pg batch: %v", err)
		}

		var val string
		err = db.QueryRowContext(ctx, "SELECT val FROM test_items_cp1_pg WHERE id = 10").Scan(&val)
		if err != nil || val != "pg-val-10" {
			t.Fatalf("kỳ vọng pg-val-10, got val=%s, err=%v", val, err)
		}

		var applied string
		err = db.QueryRowContext(ctx, "SELECT applied FROM _strand_worker_delta WHERE pipeline_id = $1 AND worker_id = $2", pipelineID, 1).Scan(&applied)
		if err != nil || applied != "100" {
			t.Fatalf("kỳ vọng delta 100, got applied=%s, err=%v", applied, err)
		}
	})
}

// AC3: TestAC3_MySQLClientFoundRows: MySQL không bật CLIENT_FOUND_ROWS vẫn đếm đúng
func TestAC3_MySQLClientFoundRows(t *testing.T) {
	ctx := context.Background()

	// Dùng kết nối MySQL với clientFoundRows=false
	db, _ := getMySQL(t)
	pipelineID := fmt.Sprintf("ac3-my-%d", time.Now().UnixNano())
	store := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)
	defer store.Close()

	if err := store.EnsureTables(ctx); err != nil {
		t.Fatalf("ensure tables: %v", err)
	}

	epoch, err := store.AdvanceEpoch(ctx, pipelineID, "owner", "hash")
	if err != nil {
		t.Fatalf("advance epoch: %v", err)
	}

	w := writer.NewMySQLWriter(db, pipelineID, 1, epoch)
	if err := w.EnsureWorkerRow(ctx); err != nil {
		t.Fatalf("ensure worker row: %v", err)
	}

	// Đặt applied ban đầu là "uuid:1-5" với batch_no = 1
	_, err = db.ExecContext(ctx,
		"UPDATE _strand_worker_delta SET applied = 'uuid:1-5', batch_no = 1 WHERE pipeline_id = ? AND worker_id = ?",
		pipelineID, 1,
	)
	if err != nil {
		t.Fatalf("init delta: %v", err)
	}

	// Batch 1: Ghi LẠI giá trị applied cũ = "uuid:1-5"
	// Trong MySQL thông thường nếu clientFoundRows=false:
	// UPDATE cùng giá trị sẽ trả RowsAffected = 0!
	// NHƯNG vì có "batch_no = batch_no + 1", row luôn luôn bị thay đổi trên đĩa,
	// do đó MySQL BẮT BUỘC trả RowsAffected = 1!
	delta1 := writer.DeltaRow{
		PipelineID: pipelineID,
		WorkerID:   1,
		Epoch:      epoch,
		Applied:    "uuid:1-5", // Đúng giá trị đang có trên DB
	}

	if err := w.ApplyBatch(ctx, nil, delta1); err != nil {
		t.Fatalf("Batch 1 thất bại (có thể bị false fence do clientFoundRows): %v", err)
	}

	// Kiểm tra batch_no đã tăng lên 2
	var batchNo int64
	err = db.QueryRowContext(ctx,
		"SELECT batch_no FROM _strand_worker_delta WHERE pipeline_id = ? AND worker_id = ?",
		pipelineID, 1,
	).Scan(&batchNo)
	if err != nil {
		t.Fatalf("query batch_no: %v", err)
	}
	if batchNo != 2 {
		t.Fatalf("kỳ vọng batch_no = 2, got %d", batchNo)
	}

	// Batch 2: Tiếp tục ghi lại CÙNG giá trị "uuid:1-5" lần thứ 2
	delta2 := writer.DeltaRow{
		PipelineID: pipelineID,
		WorkerID:   1,
		Epoch:      epoch,
		Applied:    "uuid:1-5",
	}

	if err := w.ApplyBatch(ctx, nil, delta2); err != nil {
		t.Fatalf("Batch 2 thất bại: %v", err)
	}

	// Kiểm tra batch_no đã tăng lên 3
	err = db.QueryRowContext(ctx,
		"SELECT batch_no FROM _strand_worker_delta WHERE pipeline_id = ? AND worker_id = ?",
		pipelineID, 1,
	).Scan(&batchNo)
	if err != nil {
		t.Fatalf("query batch_no 2: %v", err)
	}
	if batchNo != 3 {
		t.Fatalf("kỳ vọng batch_no = 3, got %d", batchNo)
	}
}
