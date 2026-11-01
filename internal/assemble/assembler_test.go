package assemble_test

import (
	"context"
	"io"
	"os"
	"runtime"
	"testing"

	"strand/internal/assemble"
	"strand/internal/model"
	"strand/internal/reader"
)

// AC1: Lắp đúng Txn nhiều bảng
func TestAC1_MultiTableTxn(t *testing.T) {
	asm := assemble.New()
	reg := asm.Registry()

	reg.RegisterTable("app", "orders", []string{"id"}, []string{"id", "user_id", "total"})
	reg.RegisterTable("app", "order_items", []string{"id"}, []string{"id", "order_id", "item"})
	reg.RegisterTable("app", "users", []string{"id"}, []string{"id", "name", "balance"})

	events := []reader.Event{
		{
			Type: reader.EventGTID,
			Pos:  model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:101"},
		},
		{
			Type:  reader.EventQuery,
			Query: "BEGIN",
		},
		{
			Type:   reader.EventRows,
			Schema: "app",
			Table:  "orders",
			Op:     model.OpInsert,
			Rows: [][]any{
				{1, 10, 500},
			},
		},
		{
			Type:   reader.EventRows,
			Schema: "app",
			Table:  "order_items",
			Op:     model.OpInsert,
			Rows: [][]any{
				{101, 1, "product_A"},
				{102, 1, "product_B"},
			},
		},
		{
			Type:   reader.EventRows,
			Schema: "app",
			Table:  "users",
			Op:     model.OpUpdate,
			Rows: [][]any{
				{10, "Alice", 1000}, // before
				{10, "Alice", 500},  // after
			},
		},
		{
			Type: reader.EventXID,
			XID:  98765,
		},
	}

	var assembledTxn *model.Txn
	for _, ev := range events {
		txn, err := asm.Push(ev)
		if err != nil {
			t.Fatalf("asm.Push failed: %v", err)
		}
		if txn != nil {
			assembledTxn = txn
		}
	}

	if assembledTxn == nil {
		t.Fatalf("AC1 FAIL: không lắp ráp được transaction")
	}

	if assembledTxn.SourcePos.GTID != "3e11fa47-71ca-11e1-9e33-c80aa9429562:101" {
		t.Errorf("expected GTID 3e11fa47-71ca-11e1-9e33-c80aa9429562:101, got %s", assembledTxn.SourcePos.GTID)
	}

	if len(assembledTxn.Rows) != 4 {
		t.Fatalf("expected 4 row changes across 3 tables, got %d", len(assembledTxn.Rows))
	}

	expectedTables := []string{"orders", "order_items", "order_items", "users"}
	expectedOps := []model.Op{model.OpInsert, model.OpInsert, model.OpInsert, model.OpUpdate}

	for i, rc := range assembledTxn.Rows {
		if rc.Table.Table != expectedTables[i] {
			t.Errorf("row %d: expected table %s, got %s", i, expectedTables[i], rc.Table.Table)
		}
		if rc.Op != expectedOps[i] {
			t.Errorf("row %d: expected op %s, got %s", i, expectedOps[i], rc.Op)
		}
	}

	t.Logf("AC1 PASS: Lắp đúng Txn nhiều bảng (%d rows across tables: %v)", len(assembledTxn.Rows), expectedTables)
}

// AC2: UPDATE đổi PK có OldKey
func TestAC2_UpdatePKWithOldKey(t *testing.T) {
	asm := assemble.New()
	reg := asm.Registry()

	reg.RegisterTable("app", "accounts", []string{"id"}, []string{"id", "owner", "val"})

	events := []reader.Event{
		{
			Type: reader.EventGTID,
			Pos:  model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:201"},
		},
		{
			Type:  reader.EventQuery,
			Query: "BEGIN",
		},
		// Case 1: UPDATE đổi PK từ id=10 sang id=20
		{
			Type:   reader.EventRows,
			Schema: "app",
			Table:  "accounts",
			Op:     model.OpUpdate,
			Rows: [][]any{
				{10, "Alice", 100}, // before
				{20, "Alice", 100}, // after (PK đổi 10 -> 20)
			},
		},
		// Case 2: UPDATE không đổi PK (id=30 giữ nguyên)
		{
			Type:   reader.EventRows,
			Schema: "app",
			Table:  "accounts",
			Op:     model.OpUpdate,
			Rows: [][]any{
				{30, "Bob", 200}, // before
				{30, "Bob", 300}, // after (PK không đổi)
			},
		},
		{
			Type: reader.EventXID,
			XID:  88888,
		},
	}

	var assembledTxn *model.Txn
	for _, ev := range events {
		txn, err := asm.Push(ev)
		if err != nil {
			t.Fatalf("Push failed: %v", err)
		}
		if txn != nil {
			assembledTxn = txn
		}
	}

	if assembledTxn == nil || len(assembledTxn.Rows) != 2 {
		t.Fatalf("AC2 FAIL: expected 2 rows, got %v", assembledTxn)
	}

	// Kiểm tra Case 1: UPDATE đổi PK có OldKey
	rc1 := assembledTxn.Rows[0]
	if len(rc1.Key) == 0 || rc1.Key[0] != 20 {
		t.Errorf("Case 1: expected new Key [20], got %v", rc1.Key)
	}
	if len(rc1.OldKey) == 0 || rc1.OldKey[0] != 10 {
		t.Errorf("Case 1: expected OldKey [10], got %v", rc1.OldKey)
	}

	// Kiểm tra Case 2: UPDATE không đổi PK thì OldKey nil
	rc2 := assembledTxn.Rows[1]
	if len(rc2.Key) == 0 || rc2.Key[0] != 30 {
		t.Errorf("Case 2: expected Key [30], got %v", rc2.Key)
	}
	if rc2.OldKey != nil {
		t.Errorf("Case 2: expected OldKey nil khi không đổi PK, got %v", rc2.OldKey)
	}

	t.Logf("AC2 PASS: UPDATE đổi PK có OldKey=%v, UPDATE giữ PK có OldKey=%v", rc1.OldKey, rc2.OldKey)
}

// AC3: Txn 50 triệu row không giữ hết trong RAM
func TestAC3_LargeTxnSpill_50MillionRows(t *testing.T) {
	// Giới hạn maxInMemRows = 10,000 dòng để kích hoạt spill ngay lập tức
	tempDir := t.TempDir()
	asm := assemble.New(
		assemble.WithMaxInMemRows(10000),
		assemble.WithSpillDir(tempDir),
	)
	reg := asm.Registry()
	reg.RegisterTable("app", "big_table", []string{"id"}, []string{"id", "val"})

	// Khởi tạo Txn
	_, err := asm.Push(reader.Event{
		Type: reader.EventGTID,
		Pos:  model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:301"},
	})
	if err != nil {
		t.Fatalf("Push GTID failed: %v", err)
	}

	const totalRows = 50_000_000
	const batchSize = 100_000

	// Chuẩn bị batch mẫu
	batchRows := make([][]any, batchSize)
	for j := 0; j < batchSize; j++ {
		batchRows[j] = []any{j + 1, j * 10}
	}

	t.Logf("Bắt đầu nạp %d rows vào Txn Assembler...", totalRows)

	for i := 0; i < totalRows; i += batchSize {
		ev := reader.Event{
			Type:   reader.EventRows,
			Schema: "app",
			Table:  "big_table",
			Op:     model.OpInsert,
			Rows:   batchRows,
		}
		if _, err := asm.Push(ev); err != nil {
			t.Fatalf("Push batch %d failed: %v", i, err)
		}

		// Kiểm tra định kỳ mỗi 10 triệu row: HeapAlloc phải được giữ ở mức O(1) an toàn
		if (i+batchSize)%10_000_000 == 0 {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			heapMB := m.HeapAlloc / (1024 * 1024)
			t.Logf("Đã nạp %d/%d rows. HeapAlloc: %d MB", i+batchSize, totalRows, heapMB)

			// HeapAlloc không được vượt quá 150 MB (chứng minh 50 triệu row không bị giữ trong RAM)
			if heapMB > 150 {
				t.Fatalf("AC3 FAIL: HeapAlloc = %d MB vượt ngưỡng an toàn (dữ liệu bị giữ trong RAM)", heapMB)
			}
		}
	}

	// Kết thúc transaction
	assembledTxn, err := asm.Push(reader.Event{
		Type: reader.EventXID,
		XID:  77777,
	})
	if err != nil {
		t.Fatalf("Push XID failed: %v", err)
	}

	if assembledTxn == nil {
		t.Fatalf("AC3 FAIL: Txn không được hoàn tất")
	}

	// Đảm bảo Txn đánh dấu IsSpilled() = true
	if !assembledTxn.IsSpilled() {
		t.Fatalf("AC3 FAIL: Txn 50 triệu row nhưng không được spill xuống đĩa")
	}

	// Đảm bảo mảng Rows trong RAM đã được giải phóng
	if len(assembledTxn.Rows) != 0 {
		t.Fatalf("AC3 FAIL: assembledTxn.Rows vẫn chứa %d phần tử trong RAM", len(assembledTxn.Rows))
	}

	// Đảm bảo TotalRows ghi nhận đúng 50.000.000 dòng
	if assembledTxn.TotalRows() != totalRows {
		t.Fatalf("expected total rows %d, got %d", totalRows, assembledTxn.TotalRows())
	}

	// Kiểm tra file spill trên đĩa tồn tại và có dung lượng hợp lệ
	fi, err := os.Stat(assembledTxn.SpillPath)
	if err != nil {
		t.Fatalf("stat spill file failed: %v", err)
	}
	t.Logf("Spill file path: %s (size: %.2f MB)", assembledTxn.SpillPath, float64(fi.Size())/(1024*1024))
	if fi.Size() == 0 {
		t.Fatalf("AC3 FAIL: file tràn rỗng")
	}

	// Kiểm tra RAM cuối cùng
	var finalMem runtime.MemStats
	runtime.ReadMemStats(&finalMem)
	finalHeapMB := finalMem.HeapAlloc / (1024 * 1024)
	t.Logf("AC3 PASS: 50 triệu row được lưu thành công trên đĩa, RAM HeapAlloc = %d MB (< 150 MB)", finalHeapMB)
}

func TestProcessChannel(t *testing.T) {
	asm := assemble.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan reader.Event, 10)
	txns, errs := asm.Process(ctx, in)

	in <- reader.Event{
		Type: reader.EventGTID,
		Pos:  model.Pos{Engine: model.EngineMySQL, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:501"},
	}
	in <- reader.Event{
		Type:  reader.EventQuery,
		Query: "BEGIN",
	}
	in <- reader.Event{
		Type:   reader.EventRows,
		Schema: "app",
		Table:  "test",
		Op:     model.OpInsert,
		Rows:   [][]any{{1, "val1"}},
	}
	in <- reader.Event{
		Type: reader.EventXID,
		XID:  123,
	}
	close(in)

	var received []*model.Txn
	for txn := range txns {
		received = append(received, txn)
	}

	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if len(received) != 1 {
		t.Fatalf("expected 1 txn, got %d", len(received))
	}
}

func TestRowIterator(t *testing.T) {
	// Test in-mem iterator
	inMemRows := []model.RowChange{
		{Table: model.TableID{Schema: "s", Table: "t1"}, Op: model.OpInsert, Key: []model.Value{1}},
		{Table: model.TableID{Schema: "s", Table: "t1"}, Op: model.OpUpdate, Key: []model.Value{2}, OldKey: []model.Value{1}},
	}
	itMem := assemble.NewSliceRowIterator(inMemRows)
	r1, err := itMem.Next()
	if err != nil || r1.Key[0] != 1 {
		t.Fatalf("unexpected r1: %v, %v", r1, err)
	}
	r2, err := itMem.Next()
	if err != nil || r2.Key[0] != 2 || r2.OldKey[0] != 1 {
		t.Fatalf("unexpected r2: %v, %v", r2, err)
	}
	_, err = itMem.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = itMem.Close()

	// Test disk iterator
	spill := assemble.NewSpillBuffer(2, t.TempDir())
	_ = spill.Append(inMemRows[0])
	_ = spill.Append(inMemRows[1])
	_ = spill.Append(model.RowChange{Table: model.TableID{Schema: "s", Table: "t2"}, Op: model.OpDelete, Key: []model.Value{3}})
	var txn model.Txn
	_ = spill.Finish(&txn)
	if !txn.IsSpilled() {
		t.Fatalf("expected txn to be spilled")
	}
	itDisk, err := spill.OpenIterator(&txn)
	if err != nil {
		t.Fatalf("open iterator failed: %v", err)
	}
	defer itDisk.Close()

	dr1, err := itDisk.Next()
	if err != nil || dr1.Key[0] != int64(1) || dr1.Table.Table != "t1" {
		t.Fatalf("unexpected dr1: %v, %v", dr1, err)
	}
	dr2, err := itDisk.Next()
	if err != nil || dr2.Key[0] != int64(2) || dr2.OldKey[0] != int64(1) {
		t.Fatalf("unexpected dr2: %v, %v", dr2, err)
	}
	dr3, err := itDisk.Next()
	if err != nil || dr3.Key[0] != int64(3) || dr3.Op != model.OpDelete || dr3.Table.Table != "t2" {
		t.Fatalf("unexpected dr3: %v, %v", dr3, err)
	}
	_, err = itDisk.Next()
	if err != io.EOF {
		t.Fatalf("expected EOF from disk iterator, got %v", err)
	}
}
