package snapshot_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// TestRS_UpdatePK_AcrossDifferentRangeStates kiểm tra AC1 (P1-11):
// PK cũ và PK mới ở hai range khác trạng thái: đích không còn PK cũ, 0 lệch.
// spec: 02 §5, §8, design §6.1
func TestRS_UpdatePK_AcrossDifferentRangeStates(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tableName := "rs_update_pk_test"
	table := model.TableID{Table: tableName}
	pkCol := "id"
	columnNames := []string{"id", "title", "val"}

	// 1. Tạo bảng trên source và target
	createSQL := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s;
		CREATE TABLE %s (
			id INT PRIMARY KEY,
			title VARCHAR(100) NOT NULL,
			val INT NOT NULL
		) ENGINE=InnoDB;
	`, table.String(), table.String())

	if _, err := srcDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create src table failed: %v", err)
	}
	if _, err := dstDB.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create dst table failed: %v", err)
	}

	// 2. Chèn dữ liệu ban đầu vào source: row id=25 (thuộc Range 1)
	if _, err := srcDB.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, title, val) VALUES (25, 'init-25', 100)", table.String())); err != nil {
		t.Fatalf("insert init row failed: %v", err)
	}

	// Thiết lập 2 range:
	// Range 1: [1, 50]
	// Range 2: [51, 100]
	manager := snapshot.NewRangeStateManager()
	chunk1 := &snapshot.Chunk{
		ID:              1,
		Table:           table,
		PKCol:           pkCol,
		StartPK:         1,
		EndPK:           50,
		State:           snapshot.StatePending,
		BufferedChanges: make(map[int64]*model.RowChange),
	}
	chunk2 := &snapshot.Chunk{
		ID:              2,
		Table:           table,
		PKCol:           pkCol,
		StartPK:         51,
		EndPK:           100,
		State:           snapshot.StatePending,
		BufferedChanges: make(map[int64]*model.RowChange),
	}
	manager.RegisterTable(table, pkCol, []*snapshot.Chunk{chunk1, chunk2}, 100)

	// Range 1 bắt đầu đọc (StateReading) với low watermark
	lowGTIDStr, err := snapshot.GetGlobalGTIDExecuted(srcDB)
	if err != nil {
		t.Fatalf("failed to get low gtid: %v", err)
	}
	lowSet := snapshot.ParseMySQLGTIDSet(lowGTIDStr)
	manager.StartReading(chunk1, lowSet)

	// Giả lập câu SELECT cho chunk 1: trả về row 25
	snapRowsChunk1 := []*model.RowChange{
		{
			Table: table,
			Op:    model.OpInsert,
			Key:   []model.Value{int64(25)},
			Cols: []model.ColValue{
				{Name: "id", Value: 25},
				{Name: "title", Value: "init-25"},
				{Name: "val", Value: 100},
			},
		},
	}

	// Bây giờ trên source: UPDATE đổi PK từ 25 (thuộc Range 1 đang đọc) sang 75 (thuộc Range 2 đang Pending)
	updateSQL := fmt.Sprintf("UPDATE %s SET id = 75, title = 'updated-to-75', val = 200 WHERE id = 25", table.String())
	if _, err := srcDB.ExecContext(ctx, updateSQL); err != nil {
		t.Fatalf("update change PK failed: %v", err)
	}

	postUpdateGTIDStr, err := snapshot.GetGlobalGTIDExecuted(srcDB)
	if err != nil {
		t.Fatalf("failed to get post update gtid: %v", err)
	}
	eventPos := snapshot.LatestPos(postUpdateGTIDStr)

	// Tạo event UPDATE đổi PK: OldKey = 25, Key = 75
	changePKEvent := &model.RowChange{
		Table:  table,
		Op:     model.OpUpdate,
		OldKey: []model.Value{int64(25)},
		Key:    []model.Value{int64(75)},
		Cols: []model.ColValue{
			{Name: "id", Value: 75},
			{Name: "title", Value: "updated-to-75"},
			{Name: "val", Value: 200},
		},
	}

	// Xử lý stream event qua RangeStateManager với cơ chế tách đổi PK
	results := manager.HandleStreamEventWithPKSplit(changePKEvent, eventPos)
	if len(results) != 2 {
		t.Fatalf("expected 2 split results, got %d", len(results))
	}

	// Kết quả 1: Tombstone cho OldKey 25
	// Range 1 đang StateReading -> Phải được BUFFER (để loại bỏ khỏi row snapshot lúc merge)
	res1 := results[0]
	if res1.Event.Op != model.OpDelete || res1.Event.Key[0] != int64(25) {
		t.Errorf("expected tombstone for PK 25, got %v", res1.Event)
	}
	if res1.Action != snapshot.ActionBuffer || res1.Chunk != chunk1 {
		t.Errorf("expected ActionBuffer on chunk1, got %v, chunk=%v", res1.Action, res1.Chunk)
	}

	// Kết quả 2: Upsert cho Key mới 75
	// Range 2 đang StatePending -> Phải được DISCARD (vì câu SELECT Range 2 chạy sau sẽ tự đọc được row 75)
	res2 := results[1]
	if res2.Event.Op != model.OpInsert || res2.Event.Key[0] != int64(75) {
		t.Errorf("expected upsert for PK 75, got %v", res2.Event)
	}
	if res2.Action != snapshot.ActionDiscard || res2.Chunk != chunk2 {
		t.Errorf("expected ActionDiscard on chunk2, got %v, chunk=%v", res2.Action, res2.Chunk)
	}

	// Hoàn thành đọc Range 1: FinishReading -> MergeAndFlush -> Flush sang target
	chunk1HighStr, _ := snapshot.GetGlobalGTIDExecuted(srcDB)
	manager.FinishReading(chunk1, snapshot.ParseMySQLGTIDSet(chunk1HighStr), snapRowsChunk1)

	mergedChunk1 := manager.MergeAndFlush(chunk1)
	// Do có tombstone cho 25, mergedChunk1 không được chứa row 25!
	if len(mergedChunk1) != 0 {
		t.Fatalf("expected 0 rows in mergedChunk1 (row 25 removed by tombstone), got %d rows", len(mergedChunk1))
	}
	manager.MarkDone(chunk1)

	// Bắt đầu đọc Range 2 (StateReading)
	manager.StartReading(chunk2, snapshot.ParseMySQLGTIDSet(chunk1HighStr))
	// Query thực tế trên source trong khoảng [51, 100]
	rows2, err := srcDB.QueryContext(ctx, fmt.Sprintf("SELECT id, title, val FROM %s WHERE id >= 51 AND id <= 100", table.String()))
	if err != nil {
		t.Fatalf("query src chunk2 failed: %v", err)
	}
	defer rows2.Close()

	var snapRowsChunk2 []*model.RowChange
	for rows2.Next() {
		var id, val int
		var title string
		if err := rows2.Scan(&id, &title, &val); err != nil {
			t.Fatalf("scan chunk2 row failed: %v", err)
		}
		snapRowsChunk2 = append(snapRowsChunk2, &model.RowChange{
			Table: table,
			Op:    model.OpInsert,
			Key:   []model.Value{int64(id)},
			Cols: []model.ColValue{
				{Name: "id", Value: id},
				{Name: "title", Value: title},
				{Name: "val", Value: val},
			},
		})
	}

	chunk2HighStr, _ := snapshot.GetGlobalGTIDExecuted(srcDB)
	manager.FinishReading(chunk2, snapshot.ParseMySQLGTIDSet(chunk2HighStr), snapRowsChunk2)

	mergedChunk2 := manager.MergeAndFlush(chunk2)
	// mergedChunk2 phải có row 75
	if len(mergedChunk2) != 1 || mergedChunk2[0].Key[0] != int64(75) {
		t.Fatalf("expected 1 row (pk=75) in mergedChunk2, got %v", mergedChunk2)
	}

	// Flush mergedChunk2 sang target
	for _, row := range mergedChunk2 {
		if err := applyEventToTarget(ctx, dstDB, row, columnNames, pkCol); err != nil {
			t.Fatalf("apply chunk2 row to dst failed: %v", err)
		}
	}
	manager.MarkDone(chunk2)

	// 3. Kiểm tra đích: Đích KHÔNG CÒN PK CŨ (25), và có PK mới (75) với giá trị chính xác
	var countOld int
	if err := dstDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = 25", table.String())).Scan(&countOld); err != nil {
		t.Fatalf("check old pk failed: %v", err)
	}
	if countOld != 0 {
		t.Fatalf("FAILED AC1: Old PK 25 still exists in target (count=%d)", countOld)
	}

	var newTitle string
	var newVal int
	err = dstDB.QueryRowContext(ctx, fmt.Sprintf("SELECT title, val FROM %s WHERE id = 75", table.String())).Scan(&newTitle, &newVal)
	if err != nil {
		t.Fatalf("query new pk 75 failed: %v", err)
	}
	if newTitle != "updated-to-75" || newVal != 200 {
		t.Fatalf("FAILED AC1: New PK 75 has unexpected values: title=%s, val=%d", newTitle, newVal)
	}

	// Đối chiếu 0 lệch giữa source và target
	var srcCount, dstCount int
	_ = srcDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table.String())).Scan(&srcCount)
	_ = dstDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table.String())).Scan(&dstCount)
	if srcCount != dstCount || srcCount != 1 {
		t.Fatalf("FAILED AC1: Mismatch row count: src=%d, dst=%d", srcCount, dstCount)
	}

	t.Logf("PASS AC1: UPDATE change PK across different range states: target has NO old PK (25), and 0 mismatch with source")
}

// TestRS_UpdatePK_SequentialChain kiểm tra AC2 (P1-11):
// Đổi liên tiếp 1 -> 2 -> 3 đúng.
// Khóa 2 nhận upsert rồi tombstone nên bị triệt tiêu, đích cuối cùng không còn khóa 1 và khóa 2, chỉ giữ khóa 3.
func TestRS_UpdatePK_SequentialChain(t *testing.T) {
	table := model.TableID{Schema: "app", Table: "orders_chain"}
	pkCol := "id"

	// 3 ranges:
	// Range 1: [1, 10]
	// Range 2: [11, 20]
	// Range 3: [21, 30]
	manager := snapshot.NewRangeStateManager()
	chunk1 := &snapshot.Chunk{
		ID:              1,
		Table:           table,
		PKCol:           pkCol,
		StartPK:         1,
		EndPK:           10,
		State:           snapshot.StateReading,
		BufferedChanges: make(map[int64]*model.RowChange),
	}
	chunk2 := &snapshot.Chunk{
		ID:              2,
		Table:           table,
		PKCol:           pkCol,
		StartPK:         11,
		EndPK:           20,
		State:           snapshot.StateReading,
		BufferedChanges: make(map[int64]*model.RowChange),
	}
	chunk3 := &snapshot.Chunk{
		ID:              3,
		Table:           table,
		PKCol:           pkCol,
		StartPK:         21,
		EndPK:           30,
		State:           snapshot.StateReading,
		BufferedChanges: make(map[int64]*model.RowChange),
	}
	manager.RegisterTable(table, pkCol, []*snapshot.Chunk{chunk1, chunk2, chunk3}, 30)

	dummyPos := model.Pos{Engine: model.EngineMySQL, GTID: "uuid:100"}

	// Bước 1: UPDATE đổi PK 1 -> 15 (thuộc Range 1 -> Range 2)
	ev1 := &model.RowChange{
		Table:  table,
		Op:     model.OpUpdate,
		OldKey: []model.Value{int64(1)},
		Key:    []model.Value{int64(15)},
		Cols: []model.ColValue{
			{Name: "id", Value: 15},
			{Name: "val", Value: "state-2"},
		},
	}
	res1 := manager.HandleStreamEventWithPKSplit(ev1, dummyPos)
	if len(res1) != 2 {
		t.Fatalf("expected 2 results for step 1, got %d", len(res1))
	}

	// Kiểm tra: chunk1 có tombstone cho 1, chunk2 có upsert cho 15
	if chunk1.BufferedChanges[1] == nil || chunk1.BufferedChanges[1].Op != model.OpDelete {
		t.Fatalf("expected tombstone for PK 1 in chunk1 buffer")
	}
	if chunk2.BufferedChanges[15] == nil || chunk2.BufferedChanges[15].Op != model.OpInsert {
		t.Fatalf("expected upsert for PK 15 in chunk2 buffer")
	}

	// Bước 2: UPDATE đổi tiếp 15 -> 25 (thuộc Range 2 -> Range 3)
	ev2 := &model.RowChange{
		Table:  table,
		Op:     model.OpUpdate,
		OldKey: []model.Value{int64(15)},
		Key:    []model.Value{int64(25)},
		Cols: []model.ColValue{
			{Name: "id", Value: 25},
			{Name: "val", Value: "state-3"},
		},
	}
	dummyPos2 := model.Pos{Engine: model.EngineMySQL, GTID: "uuid:101"}
	res2 := manager.HandleStreamEventWithPKSplit(ev2, dummyPos2)
	if len(res2) != 2 {
		t.Fatalf("expected 2 results for step 2, got %d", len(res2))
	}

	// Kiểm tra:
	// Khóa 15 trong chunk2: ban đầu là Upsert, giờ bị ghi đè thành Tombstone (OpDelete)!
	if chunk2.BufferedChanges[15] == nil || chunk2.BufferedChanges[15].Op != model.OpDelete {
		t.Fatalf("expected PK 15 in chunk2 buffer to be overwritten with tombstone (OpDelete), got %v", chunk2.BufferedChanges[15])
	}
	// Khóa 25 trong chunk3: có upsert
	if chunk3.BufferedChanges[25] == nil || chunk3.BufferedChanges[25].Op != model.OpInsert {
		t.Fatalf("expected upsert for PK 25 in chunk3 buffer")
	}

	// Flush cả 3 chunk:
	// Giả lập snapshot ban đầu có row 1 trong chunk1
	chunk1.SnapshotRows = []*model.RowChange{
		{
			Table: table,
			Op:    model.OpInsert,
			Key:   []model.Value{int64(1)},
			Cols: []model.ColValue{
				{Name: "id", Value: 1},
				{Name: "val", Value: "initial-1"},
			},
		},
	}

	merged1 := manager.MergeAndFlush(chunk1)
	if len(merged1) != 0 {
		t.Fatalf("expected merged1 to have 0 rows (row 1 removed by tombstone), got %d rows", len(merged1))
	}

	merged2 := manager.MergeAndFlush(chunk2)
	if len(merged2) != 0 {
		t.Fatalf("expected merged2 to have 0 rows (row 15 was deleted by tombstone), got %d rows", len(merged2))
	}

	merged3 := manager.MergeAndFlush(chunk3)
	if len(merged3) != 1 || merged3[0].Key[0] != int64(25) {
		t.Fatalf("expected merged3 to have 1 row (pk=25), got %v", merged3)
	}

	colMap := make(map[string]any)
	for _, c := range merged3[0].Cols {
		colMap[c.Name] = c.Value
	}
	if colMap["val"] != "state-3" {
		t.Fatalf("expected val='state-3' for PK 25, got %v", colMap["val"])
	}

	t.Logf("PASS AC2: Sequential PK change 1 -> 2 -> 3 handled correctly: intermediate PKs eliminated, final PK holds latest data")
}
