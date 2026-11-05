package snapshot_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

var (
	mysqlOnce sync.Once
	srcDSN    string
	dstDSN    string
)

func getMySQLDSNs(t *testing.T) (string, string) {
	t.Helper()
	if envSrc := os.Getenv("MYSQL_SRC_DSN"); envSrc != "" {
		if envDst := os.Getenv("MYSQL_DST_DSN"); envDst != "" {
			return envSrc, envDst
		}
	}

	mysqlOnce.Do(func() {
		ctx := context.Background()
		cmd := []string{
			"--server-id=6001",
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
				"MYSQL_DATABASE":      "app_src",
			},
			Cmd:        cmd,
			WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(180 * time.Second),
		}

		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
		})
		if err != nil {
			t.Fatalf("failed to start mysql container for snapshot tests: %v", err)
		}

		host, err := container.Host(ctx)
		if err != nil {
			t.Fatalf("failed to get mysql host: %v", err)
		}
		portNat, err := container.MappedPort(ctx, "3306")
		if err != nil {
			t.Fatalf("failed to get mapped mysql port: %v", err)
		}
		addr := net.JoinHostPort(host, portNat.Port())
		srcDSN = fmt.Sprintf("root:dev@tcp(%s)/app_src?multiStatements=true&parseTime=true", addr)
		dstDSN = fmt.Sprintf("root:dev@tcp(%s)/app_dst?multiStatements=true&parseTime=true", addr)

		db, err := sql.Open("mysql", fmt.Sprintf("root:dev@tcp(%s)/?multiStatements=true", addr))
		if err != nil {
			t.Fatalf("failed to connect to root mysql: %v", err)
		}
		defer db.Close()
		if _, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS app_dst;"); err != nil {
			t.Fatalf("failed to create app_dst db: %v", err)
		}
	})

	return srcDSN, dstDSN
}

// applyEventToTarget ghi một stream event trực tiếp vào target DB.
func applyEventToTarget(ctx context.Context, db *sql.DB, event *model.RowChange, columnNames []string, pkCol string) error {
	pkVal := event.Key[0]
	if event.Op == model.OpDelete {
		query := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", event.Table.String(), pkCol)
		_, err := db.ExecContext(ctx, query, pkVal)
		return err
	}

	colMap := make(map[string]any)
	for _, c := range event.Cols {
		colMap[c.Name] = c.Value
	}

	var phs []string
	var args []any
	for _, col := range columnNames {
		phs = append(phs, "?")
		args = append(args, colMap[col])
	}

	var updateExprs []string
	for _, col := range columnNames {
		if col != pkCol {
			updateExprs = append(updateExprs, fmt.Sprintf("%s = VALUES(%s)", col, col))
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		event.Table.String(),
		strings.Join(columnNames, ", "),
		strings.Join(phs, ", "),
		strings.Join(updateExprs, ", "),
	)
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

// TestRS1_SnapshotAdd_HeavyConcurrentUpdates kiểm tra AC1:
// Invariant RS-1: snapshot add dưới tải cập nhật dày, 0 lệch.
// Target không bao giờ hiển thị một row cũ hơn trạng thái nó đã có.
func TestRS1_SnapshotAdd_HeavyConcurrentUpdates(t *testing.T) {
	runRS1HeavyConcurrent(t, "rs1_snapshot_test", 64*1024*1024)
}

// invariant: RS-1/RS-2 — review R10, MySQL thật: max_chunk_bytes nhỏ (~6 row) nên mọi chunk 25 row bị
// cắt và đọc tiếp nhiều lần (SplitReading) dưới tải cập nhật/xóa dày; target cuối khớp source.
func TestR10_SnapshotAdd_ByteTruncatedChunksUnderHeavyUpdates(t *testing.T) {
	n := runRS1HeavyConcurrent(t, "r10_snapshot_test", 120)
	if n == 0 {
		t.Fatal("no chunk read was truncated by max_chunk_bytes")
	}
	t.Logf("%d chunk reads stopped at max_chunk_bytes", n)
}

// runRS1HeavyConcurrent chạy SnapshotAdd dưới tải và đối chiếu; trả số lần đọc chunk bị cắt theo byte.
func runRS1HeavyConcurrent(t *testing.T, tableName string, maxChunkBytes int64) int {
	srcDSN, dstDSN := getMySQLDSNs(t)

	srcDB, err := sql.Open("mysql", srcDSN)
	if err != nil {
		t.Fatalf("failed to connect to src db: %v", err)
	}
	defer srcDB.Close()

	dstDB, err := sql.Open("mysql", dstDSN)
	if err != nil {
		t.Fatalf("failed to connect to dst db: %v", err)
	}
	defer dstDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	table := model.TableID{Table: tableName}
	pkCol := "id"
	columnNames := []string{"id", "title", "val"}

	// 1. Tạo bảng trên cả Source và Destination
	createTableSQL := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			title VARCHAR(100) NOT NULL,
			val INT NOT NULL
		) ENGINE=InnoDB;
	`, table.String(), table.String())

	if _, err := srcDB.ExecContext(ctx, createTableSQL); err != nil {
		t.Fatalf("failed to create src table: %v", err)
	}
	if _, err := dstDB.ExecContext(ctx, createTableSQL); err != nil {
		t.Fatalf("failed to create dst table: %v", err)
	}

	// 2. Nạp dữ liệu ban đầu vào Source (100 rows: 1..100)
	var insertVals []string
	for i := 1; i <= 100; i++ {
		insertVals = append(insertVals, fmt.Sprintf("(%d, 'initial-%d', 1)", i, i))
	}
	initQuery := fmt.Sprintf("INSERT INTO %s (id, title, val) VALUES %s", table.String(), strings.Join(insertVals, ", "))
	if _, err := srcDB.ExecContext(ctx, initQuery); err != nil {
		t.Fatalf("failed to insert initial rows: %v", err)
	}

	opts := snapshot.ChunkOptions{
		Timeout:          5 * time.Second,
		MaxChunkBytes:    maxChunkBytes,
		MinChunkRows:     1,
		DefaultChunkRows: 25, // Chia thành 4 chunks [1..25], [26..50], [51..75], [76..100]
	}
	snapshotter := snapshot.NewMySQLSnapshotter(opts)
	var truncations atomic.Int64
	snapshotter.OnTruncatedRead = func() { truncations.Add(1) }

	var streamPos atomic.Value // model.Pos
	initGTID, err := snapshot.GetGlobalGTIDExecuted(srcDB)
	if err != nil {
		t.Fatalf("failed to get init gtid: %v", err)
	}
	streamPos.Store(snapshot.LatestPos(initGTID))

	var streamMu sync.Mutex
	type streamItem struct {
		event *model.RowChange
		pos   model.Pos
	}
	streamQueue := make(chan streamItem, 5000)

	var wg sync.WaitGroup
	var stopUpdater atomic.Bool

	// 3. Goroutine chạy tải cập nhật dày (Heavy concurrent updates/inserts/deletes)
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		versionMap := make(map[int]int)

		for !stopUpdater.Load() {
			targetID := r.Intn(150) + 1 // IDs 1..150 (chạm cả range ban đầu lẫn sau b_last)
			opType := r.Intn(10)

			var op model.Op
			var query string
			var args []any
			var rc model.RowChange
			rc.Table = table
			rc.Key = []model.Value{targetID}

			if opType < 2 {
				// DELETE
				op = model.OpDelete
				query = fmt.Sprintf("DELETE FROM %s WHERE id = ?", table.String())
				args = []any{targetID}
				rc.Op = op
			} else {
				// INSERT hoặc UPDATE
				v := versionMap[targetID] + 1
				versionMap[targetID] = v
				op = model.OpUpdate
				rc.Op = op
				titleVal := fmt.Sprintf("concurrent-%d-v%d", targetID, v)
				rc.Cols = []model.ColValue{
					{Name: "id", Value: targetID},
					{Name: "title", Value: titleVal},
					{Name: "val", Value: v},
				}
				query = fmt.Sprintf("INSERT INTO %s (id, title, val) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE title = VALUES(title), val = VALUES(val)", table.String())
				args = []any{targetID, titleVal, v}
			}

			if _, err := srcDB.Exec(query, args...); err == nil {
				curGTID, err := snapshot.GetGlobalGTIDExecuted(srcDB)
				if err == nil {
					p := snapshot.LatestPos(curGTID)
					streamQueue <- streamItem{event: &rc, pos: p}
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 4. Goroutine Stream Processor: tiêu thụ stream events theo RangeStateManager
	streamDone := make(chan struct{})
	go func() {
		defer close(streamDone)
		for item := range streamQueue {
			streamMu.Lock()
			action, _ := snapshotter.Manager.HandleStreamEvent(item.event, item.pos)
			if action == snapshot.ActionApply {
				_ = applyEventToTarget(ctx, dstDB, item.event, columnNames, pkCol)
			}
			streamPos.Store(item.pos)
			streamMu.Unlock()
		}
	}()

	// Rendezvous waiter: chờ stream đạt hoặc vượt chunk high watermark
	rendezvousWaitFn := func(rCtx context.Context, high checkpoint.AppliedSet) error {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-rCtx.Done():
				return rCtx.Err()
			case <-ticker.C:
				curPos := streamPos.Load().(model.Pos)
				if snapshot.StreamHasReachedHigh(curPos, high) {
					return nil
				}
			}
		}
	}

	// 5. Chạy SnapshotAdd đồng thời với tải cập nhật dày
	err = snapshotter.SnapshotAdd(ctx, srcDB, dstDB, table, pkCol, columnNames, rendezvousWaitFn)
	if err != nil {
		t.Fatalf("SnapshotAdd failed: %v", err)
	}

	// 6. Dừng tải cập nhật và chờ stream bắt kịp
	stopUpdater.Store(true)
	wg.Wait()

	// Chờ thêm để mọi event dồn vào hàng đợi được xử lý
	time.Sleep(200 * time.Millisecond)
	close(streamQueue)
	<-streamDone

	// 7. Đối chiếu kết quả giữa Source và Destination: 0 lệch
	type rowData struct {
		title string
		val   int
	}
	srcRowsMap := make(map[int]rowData)
	dstRowsMap := make(map[int]rowData)

	sRows, err := srcDB.QueryContext(ctx, fmt.Sprintf("SELECT id, title, val FROM %s", table.String()))
	if err != nil {
		t.Fatalf("failed to query src rows: %v", err)
	}
	defer sRows.Close()
	for sRows.Next() {
		var id, val int
		var title string
		if err := sRows.Scan(&id, &title, &val); err != nil {
			t.Fatalf("scan src row failed: %v", err)
		}
		srcRowsMap[id] = rowData{title: title, val: val}
	}

	dRows, err := dstDB.QueryContext(ctx, fmt.Sprintf("SELECT id, title, val FROM %s", table.String()))
	if err != nil {
		t.Fatalf("failed to query dst rows: %v", err)
	}
	defer dRows.Close()
	for dRows.Next() {
		var id, val int
		var title string
		if err := dRows.Scan(&id, &title, &val); err != nil {
			t.Fatalf("scan dst row failed: %v", err)
		}
		dstRowsMap[id] = rowData{title: title, val: val}
	}

	// Kiểm tra 0 lệch
	mismatchCount := 0
	if len(srcRowsMap) != len(dstRowsMap) {
		t.Errorf("Row count mismatch: src=%d, dst=%d", len(srcRowsMap), len(dstRowsMap))
		mismatchCount++
	}

	for id, sRow := range srcRowsMap {
		dRow, ok := dstRowsMap[id]
		if !ok {
			t.Errorf("Missing row id %d in dst", id)
			mismatchCount++
			continue
		}
		if sRow.title != dRow.title || sRow.val != dRow.val {
			t.Errorf("Row value mismatch for id %d: src={%s, %d}, dst={%s, %d}", id, sRow.title, sRow.val, dRow.title, dRow.val)
			mismatchCount++
		}
	}

	for id := range dstRowsMap {
		if _, ok := srcRowsMap[id]; !ok {
			t.Errorf("Extra row id %d in dst", id)
			mismatchCount++
		}
	}

	if mismatchCount > 0 {
		t.Fatalf("TestRS1 FAILED: found %d mismatches between source and target", mismatchCount)
	}
	t.Logf("TestRS1 PASS: snapshot add under heavy concurrent updates completed with EXACTLY 0 mismatch (%d rows)", len(srcRowsMap))
	return int(truncations.Load())
}

// TestRS_ChunkSplitOnTimeout_SingleRowNoTimeout kiểm tra AC2:
// - SELECT > 5 s thì chia đôi chunk.
// - Chunk 1 row thì không áp timeout.
func TestRS_ChunkSplitOnTimeout_SingleRowNoTimeout(t *testing.T) {
	chunker := snapshot.NewChunker()
	table := model.TableID{Schema: "test", Table: "orders"}

	// --- Phần A: SELECT timeout chia đôi chunk ---
	t.Run("SplitOnTimeout", func(t *testing.T) {
		initialChunk := &snapshot.Chunk{
			ID:            1,
			Table:         table,
			PKCol:         "id",
			StartPK:       1,
			EndPK:         10, // 10 rows
			EstimatedRows: 10,
		}

		opts := snapshot.ChunkOptions{
			Timeout:       50 * time.Millisecond,
			MaxChunkBytes: 64 * 1024 * 1024,
			MinChunkRows:  1,
		}

		queryCalls := 0
		mockQuery := func(qCtx context.Context, c *snapshot.Chunk, _ int64) ([]*model.RowChange, int64, error) {
			queryCalls++
			// Nếu chunk lớn hơn 5 rows: cố tình ngủ lâu để gây timeout
			if c.RowCount() > 5 {
				select {
				case <-time.After(100 * time.Millisecond):
					return nil, 0, nil
				case <-qCtx.Done():
					return nil, 0, qCtx.Err() // DeadlineExceeded
				}
			}
			// Chunk nhỏ hơn hoặc bằng 5 rows: thành công ngay
			var rows []*model.RowChange
			for id := c.StartPK; id <= c.EndPK; id++ {
				rows = append(rows, &model.RowChange{
					Table: table,
					Op:    model.OpInsert,
					Key:   []model.Value{id},
				})
			}
			return rows, int64(len(rows) * 50), nil
		}

		read, err := chunker.ReadChunkAdaptive(context.Background(), initialChunk, opts, mockQuery)
		if err != nil {
			t.Fatalf("ReadChunkAdaptive failed: %v", err)
		}
		rows := read.Rows

		if len(rows) != 10 {
			t.Fatalf("expected 10 rows, got %d", len(rows))
		}
		if chunker.SplitCount() == 0 {
			t.Fatalf("expected chunk to be split on timeout, but splitCount is 0")
		}
		t.Logf("PASS: Chunk split on timeout successfully, split count: %d", chunker.SplitCount())
	})

	// --- Phần B: Chunk 1 row không áp timeout ---
	t.Run("SingleRowNoTimeout", func(t *testing.T) {
		singleRowChunk := &snapshot.Chunk{
			ID:            2,
			Table:         table,
			PKCol:         "id",
			StartPK:       5,
			EndPK:         5, // 1 row duy nhất
			EstimatedRows: 1,
		}

		opts := snapshot.ChunkOptions{
			Timeout:       20 * time.Millisecond, // Timeout ngắn 20ms
			MaxChunkBytes: 64 * 1024 * 1024,
			MinChunkRows:  1,
		}

		splitsBefore := chunker.SplitCount()

		mockQuerySingleRow := func(qCtx context.Context, c *snapshot.Chunk, _ int64) ([]*model.RowChange, int64, error) {
			// Kiểm tra context: "chunk 1 row thì không áp timeout"
			// qCtx không có deadline từ opts.Timeout
			if _, hasDeadline := qCtx.Deadline(); hasDeadline {
				t.Errorf("expected no deadline on context for 1-row chunk")
			}
			// Ngủ 50ms (vượt quá 20ms của opts.Timeout)
			time.Sleep(50 * time.Millisecond)

			return []*model.RowChange{
				{
					Table: table,
					Op:    model.OpInsert,
					Key:   []model.Value{c.StartPK},
				},
			}, 100, nil
		}

		read, err := chunker.ReadChunkAdaptive(context.Background(), singleRowChunk, opts, mockQuerySingleRow)
		if err != nil {
			t.Fatalf("ReadChunkAdaptive failed for single row: %v", err)
		}
		rows := read.Rows

		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if chunker.SplitCount() != splitsBefore {
			t.Fatalf("expected no split for single row chunk, but split count increased")
		}
		t.Logf("PASS: Single row chunk ignored timeout and completed successfully without splitting")
	})
}

// TestRS_LargeRowExceedsMaxChunkBytes_NoInfiniteLoop kiểm tra AC3: row lớn hơn max_chunk_bytes không gây
// vòng lặp chia chunk vô hạn. Với trần RAM (review R10): mỗi lần đọc dừng khi chạm trần (vượt tối đa một
// row), phần còn lại đọc ở lần sau; các lần đọc liên tiếp phủ đủ mọi row.
func TestRS_LargeRowExceedsMaxChunkBytes_NoInfiniteLoop(t *testing.T) {
	chunker := snapshot.NewChunker()
	table := model.TableID{Schema: "test", Table: "large_rows"}
	opts := snapshot.ChunkOptions{Timeout: 5 * time.Second, MaxChunkBytes: 1000, MinChunkRows: 1}
	size := func(id int64) int64 {
		if id == 2 {
			return 5000 // hàng lớn vượt max_chunk_bytes
		}
		return 100
	}
	mockQuery := func(qCtx context.Context, c *snapshot.Chunk, maxBytes int64) ([]*model.RowChange, int64, error) {
		var rows []*model.RowChange
		var total int64
		for id := c.StartPK; id <= c.EndPK; id++ {
			total += size(id)
			rows = append(rows, &model.RowChange{Table: table, Op: model.OpInsert, Key: []model.Value{id}})
			if maxBytes > 0 && total >= maxBytes {
				break
			}
		}
		return rows, total, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var got []int64
	for start, reads := int64(1), 0; start <= 4; reads++ {
		if reads > 10 {
			t.Fatal("too many reads: no progress")
		}
		read, err := chunker.ReadChunkAdaptive(ctx, &snapshot.Chunk{ID: 1, Table: table, PKCol: "id", StartPK: start, EndPK: 4}, opts, mockQuery)
		if err != nil {
			t.Fatalf("ReadChunkAdaptive failed on large row: %v", err)
		}
		if read.Bytes > opts.MaxChunkBytes+5000 {
			t.Fatalf("read of [%d,4] kept %d bytes, more than max_chunk_bytes plus one row", start, read.Bytes)
		}
		for _, r := range read.Rows {
			got = append(got, r.Key[0].(int64))
		}
		start = read.Cut + 1
	}
	if fmt.Sprint(got) != "[1 2 3 4]" {
		t.Fatalf("rows read = %v, want [1 2 3 4]", got)
	}
}

// review R10: max_chunk_bytes là trần RAM của kết quả — không chia đôi rồi ghép lại toàn bộ.
func TestR10_ByteBudgetTruncatesInsteadOfReassembling(t *testing.T) {
	c := snapshot.NewChunker()
	opts := snapshot.DefaultChunkOptions()
	opts.MaxChunkBytes = 1024
	calls := 0
	read, err := c.ReadChunkAdaptive(context.Background(), &snapshot.Chunk{StartPK: 1, EndPK: 8}, opts,
		func(_ context.Context, ch *snapshot.Chunk, maxBytes int64) ([]*model.RowChange, int64, error) {
			calls++
			var out []*model.RowChange
			var total int64
			for i := ch.StartPK; i <= ch.EndPK; i++ {
				out = append(out, &model.RowChange{Key: []model.Value{i}})
				total += 512
				if maxBytes > 0 && total >= maxBytes {
					break
				}
			}
			return out, total, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if !read.Truncated || read.Cut != 2 || len(read.Rows) != 2 || read.Bytes > opts.MaxChunkBytes || calls != 1 {
		t.Fatalf("read = {rows %d, bytes %d, cut %d, truncated %v}, calls %d; want 2 rows cut at 2 in one query",
			len(read.Rows), read.Bytes, read.Cut, read.Truncated, calls)
	}
}

// TestRS_TailChunk_HandledCorrectly kiểm tra AC4:
// Chunk đuôi [b_last, +inf) được xử lý đúng:
// Đọc max(pk) sau low; các khóa lớn hơn thuộc range "Xong" ngay từ đầu và apply thẳng từ stream.
func TestRS_TailChunk_HandledCorrectly(t *testing.T) {
	manager := snapshot.NewRangeStateManager()
	table := model.TableID{Schema: "test", Table: "tail_test"}
	pkCol := "id"

	// Giả sử max(pk) lúc đọc sau low là b_last = 50
	bLast := int64(50)

	// Các chunk được chia từ 1 đến 50
	chunks := []*snapshot.Chunk{
		{
			ID:           1,
			Table:        table,
			PKCol:        pkCol,
			StartPK:      1,
			EndPK:        50,
			State:        snapshot.StateReading,
			LowWatermark: checkpoint.NewMySQLAppliedSet("uuid"),
		},
	}
	manager.RegisterTable(table, pkCol, chunks, bLast)

	// Stream event 1: PK = 20 (nằm trong chunk đang đọc [1, 50])
	eventInChunk := &model.RowChange{
		Table: table,
		Op:    model.OpInsert,
		Key:   []model.Value{int64(20)},
	}
	// Pos chưa nằm trong low -> phải được BUFFER
	act1, chunk1 := manager.HandleStreamEvent(eventInChunk, model.Pos{Engine: model.EngineMySQL, GTID: "uuid:100"})
	if act1 != snapshot.ActionBuffer || chunk1 == nil {
		t.Fatalf("expected ActionBuffer for pk=20, got %v", act1)
	}

	// Stream event 2: PK = 75 (nằm sau b_last = 50)
	// spec 02 §5: "Chunk đuôi [b_last, +inf): khóa lớn hơn thuộc range Xong ngay từ đầu"
	eventTail := &model.RowChange{
		Table: table,
		Op:    model.OpInsert,
		Key:   []model.Value{int64(75)},
	}
	act2, chunk2 := manager.HandleStreamEvent(eventTail, model.Pos{Engine: model.EngineMySQL, GTID: "uuid:101"})
	if act2 != snapshot.ActionApply {
		t.Fatalf("expected ActionApply for tail pk=75 (> b_last=50), got %v", act2)
	}
	if chunk2 != nil {
		t.Fatalf("expected nil chunk (belongs to Done range immediately), got %v", chunk2)
	}

	// Stream event 3: PK = 1000 (nằm sau b_last)
	eventTail2 := &model.RowChange{
		Table: table,
		Op:    model.OpUpdate,
		Key:   []model.Value{int64(1000)},
	}
	act3, chunk3 := manager.HandleStreamEvent(eventTail2, model.Pos{Engine: model.EngineMySQL, GTID: "uuid:102"})
	if act3 != snapshot.ActionApply || chunk3 != nil {
		t.Fatalf("expected ActionApply for tail pk=1000, got %v", act3)
	}

	t.Logf("PASS: Tail chunk [b_last, +inf) correctly routed events > b_last to ActionApply immediately")
}
