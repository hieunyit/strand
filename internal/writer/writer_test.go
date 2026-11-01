package writer_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
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

func ensureMySQLMetadataTables(t *testing.T, db *sql.DB, pipelineID string) {
	t.Helper()
	store := checkpoint.NewMySQLStore(db, pipelineID, model.EngineMySQL)
	defer store.Close()
	if err := store.EnsureTables(context.Background()); err != nil {
		t.Fatalf("ensure metadata tables failed: %v", err)
	}
}

// AC1: multi-row upsert
func TestAC1_MySQLMultiRowUpsert(t *testing.T) {
	db, _ := getMySQL(t)
	if db == nil {
		t.Fatal("không thể kết nối MySQL")
	}

	pipelineID := "pipe-ac1"
	ensureMySQLMetadataTables(t, db, pipelineID)

	ctx := context.Background()
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS multi_items (
		id INT PRIMARY KEY,
		name VARCHAR(50),
		price INT
	)`)
	if err != nil {
		t.Fatalf("create multi_items: %v", err)
	}
	_, _ = db.ExecContext(ctx, "TRUNCATE TABLE multi_items")

	tbl := model.TableID{Schema: "strand", Table: "multi_items"}
	w := writer.NewMySQLWriter(db, pipelineID, 1, 100)
	if err := w.EnsureWorkerRow(ctx); err != nil {
		t.Fatalf("ensure worker row: %v", err)
	}

	// Tạo 1 transaction chứa 30 row insert
	numRows := 30
	rows := make([]model.RowChange, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = model.RowChange{
			Table: tbl,
			Op:    model.OpInsert,
			Cols: []model.ColValue{
				{Name: "id", Value: i + 1},
				{Name: "name", Value: fmt.Sprintf("item_%d", i+1)},
				{Name: "price", Value: (i + 1) * 10},
			},
		}
	}

	txn1 := &model.Txn{Rows: rows}
	delta1 := writer.DeltaRow{
		PipelineID: "pipe-ac1",
		WorkerID:   1,
		Epoch:      100,
		Applied:    "uuid:1",
	}

	if err := w.ApplyBatch(ctx, []*model.Txn{txn1}, delta1); err != nil {
		t.Fatalf("ApplyBatch thất bại: %v", err)
	}

	multiUpserts, totalRows := w.GetStats()
	if multiUpserts < 1 {
		t.Fatalf("kỳ vọng thực thi multi-row upsert (multiUpserts >= 1), got %d", multiUpserts)
	}
	if totalRows != int64(numRows) {
		t.Fatalf("kỳ vọng totalRows == %d, got %d", numRows, totalRows)
	}

	// Xác nhận đủ 30 row trên DB
	var count int
	var sumPrice int
	err = db.QueryRowContext(ctx, "SELECT count(*), sum(price) FROM multi_items").Scan(&count, &sumPrice)
	if err != nil {
		t.Fatalf("query multi_items: %v", err)
	}
	if count != 30 {
		t.Fatalf("kỳ vọng 30 row trong multi_items, got %d", count)
	}
	expectedSum := 0
	for i := 1; i <= 30; i++ {
		expectedSum += i * 10
	}
	if sumPrice != expectedSum {
		t.Fatalf("kỳ vọng tổng price = %d, got %d", expectedSum, sumPrice)
	}

	// Transaction 2: Cập nhật giá cả 30 row bằng ON DUPLICATE KEY UPDATE
	rowsUpdate := make([]model.RowChange, numRows)
	for i := 0; i < numRows; i++ {
		rowsUpdate[i] = model.RowChange{
			Table: tbl,
			Op:    model.OpInsert,
			Cols: []model.ColValue{
				{Name: "id", Value: i + 1},
				{Name: "name", Value: fmt.Sprintf("item_updated_%d", i+1)},
				{Name: "price", Value: (i + 1) * 20},
			},
		}
	}
	txn2 := &model.Txn{Rows: rowsUpdate}
	delta2 := writer.DeltaRow{
		PipelineID: "pipe-ac1",
		WorkerID:   1,
		Epoch:      100,
		Applied:    "uuid:2",
	}
	if err := w.ApplyBatch(ctx, []*model.Txn{txn2}, delta2); err != nil {
		t.Fatalf("ApplyBatch update thất bại: %v", err)
	}

	multiUpserts2, _ := w.GetStats()
	if multiUpserts2 < 2 {
		t.Fatalf("kỳ vọng 2 lần multi-row upsert, got %d", multiUpserts2)
	}

	// Tổng price mới phải gấp đôi
	err = db.QueryRowContext(ctx, "SELECT count(*), sum(price) FROM multi_items").Scan(&count, &sumPrice)
	if err != nil {
		t.Fatalf("query updated: %v", err)
	}
	if count != 30 || sumPrice != expectedSum*2 {
		t.Fatalf("kỳ vọng count=30, sum=%d, got count=%d, sum=%d", expectedSum*2, count, sumPrice)
	}

	t.Logf("AC1 PASS: Multi-row upsert thành công trên %d rows với ON DUPLICATE KEY UPDATE", numRows)
}

// AC2: một Txn nguồn không bị tách
func TestAC2_TxnNeverSplit(t *testing.T) {
	db, _ := getMySQL(t)
	if db == nil {
		t.Fatal("không thể kết nối MySQL")
	}

	ctx := context.Background()

	// 1. Kiểm thử tính nguyên tử: một transaction chứa nhiều thao tác, nếu 1 thao tác lỗi thì toàn bộ Txn rollback
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS txn_orders (
		id INT PRIMARY KEY,
		val INT
	) ENGINE=InnoDB`)
	if err != nil {
		t.Fatalf("create txn_orders: %v", err)
	}
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS txn_items (
		id INT PRIMARY KEY,
		order_id INT,
		FOREIGN KEY (order_id) REFERENCES txn_orders(id)
	) ENGINE=InnoDB`)
	if err != nil {
		t.Fatalf("create txn_items: %v", err)
	}
	_, _ = db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0")
	_, _ = db.ExecContext(ctx, "TRUNCATE TABLE txn_items")
	_, _ = db.ExecContext(ctx, "TRUNCATE TABLE txn_orders")
	_, _ = db.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1")

	pipelineID := "pipe-ac2"
	ensureMySQLMetadataTables(t, db, pipelineID)

	w := writer.NewMySQLWriter(db, pipelineID, 1, 200)
	if err := w.EnsureWorkerRow(ctx); err != nil {
		t.Fatalf("ensure worker row: %v", err)
	}

	tblOrders := model.TableID{Schema: "strand", Table: "txn_orders"}
	tblItems := model.TableID{Schema: "strand", Table: "txn_items"}

	// Txn cố ý cài lỗi FK: row 1 hợp lệ trên txn_orders, nhưng row 2 vi phạm FK trên txn_items
	badTxn := &model.Txn{
		Rows: []model.RowChange{
			{
				Table: tblOrders,
				Op:    model.OpInsert,
				Cols:  []model.ColValue{{Name: "id", Value: 99}, {Name: "val", Value: 100}},
			},
			{
				Table: tblItems,
				Op:    model.OpInsert,
				Cols:  []model.ColValue{{Name: "id", Value: 1}, {Name: "order_id", Value: 999999}}, // order_id 999999 không tồn tại
			},
		},
	}

	deltaBad := writer.DeltaRow{
		PipelineID: "pipe-ac2",
		WorkerID:   1,
		Epoch:      200,
		Applied:    "uuid:bad",
	}

	err = w.ApplyBatch(ctx, []*model.Txn{badTxn}, deltaBad)
	if err == nil {
		t.Fatal("kỳ vọng ApplyBatch thất bại do vi phạm FK, nhưng trả về nil")
	}

	// Đảm bảo không có row nào của badTxn bị commit dở dang (nguyên tử)
	var ordersCount int
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM txn_orders WHERE id = 99").Scan(&ordersCount)
	if ordersCount != 0 {
		t.Fatalf("vi phạm nguyên tử: row trong txn_orders bị commit dù transaction lỗi, count=%d", ordersCount)
	}

	// 2. Kiểm thử GroupCommit BatchTxns: không bao giờ tách đôi một Txn nguồn
	txns := make([]*model.Txn, 5)
	// Txn 0: 500 rows
	txns[0] = &model.Txn{RowCount: 500, Bytes: 1000}
	// Txn 1: 800 rows (500 + 800 = 1300 <= 2000)
	txns[1] = &model.Txn{RowCount: 800, Bytes: 1000}
	// Txn 2: 900 rows (1300 + 900 = 2200 > 2000 => phải mở batch mới, KHÔNG ĐƯỢC chia cắt txn 2)
	txns[2] = &model.Txn{RowCount: 900, Bytes: 1000}
	// Txn 3: 3000 rows (vượt ngưỡng max 2000 của batch một mình, nhưng Txn nguồn vẫn phải trọn vẹn 1 batch)
	txns[3] = &model.Txn{RowCount: 3000, Bytes: 2000}
	// Txn 4: 100 rows
	txns[4] = &model.Txn{RowCount: 100, Bytes: 500}

	cfg := writer.GroupCommitConfig{
		MaxRows:  2000,
		MaxBytes: 1024 * 1024,
	}

	batches := writer.BatchTxns(txns, cfg)
	// Kỳ vọng chia thành 4 batches:
	// Batch 1: [txn0, txn1] (1300 rows)
	// Batch 2: [txn2] (900 rows)
	// Batch 3: [txn3] (3000 rows nguyên vẹn)
	// Batch 4: [txn4] (100 rows)
	if len(batches) != 4 {
		t.Fatalf("kỳ vọng 4 batches, got %d", len(batches))
	}
	if len(batches[0]) != 2 || batches[0][0] != txns[0] || batches[0][1] != txns[1] {
		t.Fatal("batch 0 không đúng kỳ vọng [txn0, txn1]")
	}
	if len(batches[1]) != 1 || batches[1][0] != txns[2] {
		t.Fatal("batch 1 không đúng kỳ vọng [txn2]")
	}
	if len(batches[2]) != 1 || batches[2][0] != txns[3] {
		t.Fatal("batch 2 không đúng kỳ vọng [txn3] (txn 3000 rows phải nguyên vẹn)")
	}
	if len(batches[3]) != 1 || batches[3][0] != txns[4] {
		t.Fatal("batch 3 không đúng kỳ vọng [txn4]")
	}

	t.Log("AC2 PASS: Một Txn nguồn không bao giờ bị tách thành nhiều lần commit ở target")
}

// invariant: CP-5
// AC3: chế độ strict cho đích luôn là prefix của source (TestCP5).
func TestCP5_StrictCommitOrderPrefix(t *testing.T) {
	db, _ := getMySQL(t)
	if db == nil {
		t.Fatal("không thể kết nối MySQL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS strict_prefix_test (
		id INT PRIMARY KEY,
		seq INT,
		worker_id INT
	) ENGINE=InnoDB`)
	if err != nil {
		t.Fatalf("create strict_prefix_test: %v", err)
	}
	_, _ = db.ExecContext(ctx, "TRUNCATE TABLE strict_prefix_test")

	pipelineID := "pipe-cp5"
	ensureMySQLMetadataTables(t, db, pipelineID)

	numTxns := 40
	numWorkers := 6

	for wID := 1; wID <= numWorkers; wID++ {
		w := writer.NewMySQLWriter(db, pipelineID, wID, 500)
		if err := w.EnsureWorkerRow(ctx); err != nil {
			t.Fatalf("ensure worker row %d: %v", wID, err)
		}
	}

	orderer := writer.NewCommitOrderer("strict", 1)

	// Tạo các transaction độc lập hoàn toàn về key (id = 1..40)
	txns := make([]*model.Txn, numTxns)
	tbl := model.TableID{Schema: "strand", Table: "strict_prefix_test"}
	for i := 0; i < numTxns; i++ {
		seq := uint64(i + 1)
		txns[i] = &model.Txn{
			Rows: []model.RowChange{
				{
					Table: tbl,
					Op:    model.OpInsert,
					Cols: []model.ColValue{
						{Name: "id", Value: seq},
						{Name: "seq", Value: seq},
						{Name: "worker_id", Value: 0},
					},
				},
			},
		}
		txns[i].SetSeq(seq)
	}

	// Goroutine quan sát liên tục kiểm tra trạng thái target tại mọi thời điểm
	var violationsCount int64
	var totalChecks int64
	stopObserver := make(chan struct{})

	var observerWg sync.WaitGroup
	observerWg.Add(1)
	go func() {
		defer observerWg.Done()
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopObserver:
				return
			case <-ticker.C:
				rows, err := db.QueryContext(ctx, "SELECT seq FROM strict_prefix_test ORDER BY seq ASC")
				if err != nil {
					continue
				}
				var observedSeqs []int
				for rows.Next() {
					var s int
					if err := rows.Scan(&s); err == nil {
						observedSeqs = append(observedSeqs, s)
					}
				}
				rows.Close()

				atomic.AddInt64(&totalChecks, 1)

				// Kiểm tra tính chất prefix: observedSeqs PHẢI là [1, 2, ..., k] liên tục không có lỗ
				for idx, sVal := range observedSeqs {
					expectedSeq := idx + 1
					if sVal != expectedSeq {
						atomic.AddInt64(&violationsCount, 1)
						t.Errorf("VI PHẠM BẤT BIẾN CP-5: Quan sát thấy sequence có lỗ! Vị trí %d: kỳ vọng seq=%d nhưng thấy seq=%d (observed: %v)",
							idx, expectedSeq, sVal, observedSeqs)
						break
					}
				}
			}
		}
	}()

	// Worker pool áp dụng các transaction song song
	txnCh := make(chan *model.Txn, numTxns)
	for _, tx := range txns {
		txnCh <- tx
	}
	close(txnCh)

	var workersWg sync.WaitGroup
	for wID := 1; wID <= numWorkers; wID++ {
		workersWg.Add(1)
		workerID := wID
		go func() {
			defer workersWg.Done()
			w := writer.NewMySQLWriter(db, pipelineID, workerID, 500)
			w.SetCommitOrderer(orderer)

			for txn := range txnCh {
				// Cố ý thêm jitter ngẫu nhiên để mô phỏng thời gian chuẩn bị dữ liệu khác nhau giữa các worker
				jitter := time.Duration(10+(txn.Seq()%7)*5) * time.Millisecond
				time.Sleep(jitter)

				delta := writer.DeltaRow{
					PipelineID: "pipe-cp5",
					WorkerID:   workerID,
					Epoch:      500,
					Applied:    fmt.Sprintf("uuid:%d", txn.Seq()),
				}

				if err := w.ApplyBatch(ctx, []*model.Txn{txn}, delta); err != nil {
					t.Errorf("Worker %d apply txn %d thất bại: %v", workerID, txn.Seq(), err)
					return
				}
			}
		}()
	}

	workersWg.Wait()
	close(stopObserver)
	observerWg.Wait()

	if v := atomic.LoadInt64(&violationsCount); v > 0 {
		t.Fatalf("CP-5 THẤT BẠI: Có %d lần quan sát vi phạm tính chất prefix", v)
	}

	// Kiểm tra cuối cùng: đủ 40 rows và đúng thứ tự 1..40
	var finalCount int
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM strict_prefix_test").Scan(&finalCount)
	if finalCount != numTxns {
		t.Fatalf("kỳ vọng đủ %d rows trên đích, got %d", numTxns, finalCount)
	}

	t.Logf("AC3 TestCP5 PASS: Đã thực hiện %d lần kiểm tra liên tục trong khi %d workers chạy song song. 0 vi phạm prefix!",
		atomic.LoadInt64(&totalChecks), numWorkers)
}
