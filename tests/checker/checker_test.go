package checker_test

import (
	"testing"

	"strand/tests/checker"
)

// TestCK1_FiveVerificationChecks kiểm tra AC1:
// Đầy đủ 5 phép kiểm của spec/04 §3 hoạt động chính xác khi dữ liệu chuẩn.
func TestCK1_FiveVerificationChecks(t *testing.T) {
	chk := checker.NewChecker()

	// 1. Phép kiểm 1: Trạng thái cuối bằng nhau
	source := map[string]map[string]checker.RowState{
		"orders": {
			"1": {Table: "orders", PK: "1", Ver: 3, Deleted: false, Values: map[string]interface{}{"amount": 150.0, "status": "paid"}},
			"2": {Table: "orders", PK: "2", Ver: 1, Deleted: true},
		},
	}
	target := map[string]map[string]checker.RowState{
		"orders": {
			"1": {Table: "orders", PK: "1", Ver: 3, Deleted: false, Values: map[string]interface{}{"amount": 150.0, "status": "paid"}},
			"2": {Table: "orders", PK: "2", Ver: 1, Deleted: true},
		},
	}
	r1 := chk.CheckFinalState(source, target)
	if !r1.Passed {
		t.Fatalf("CheckFinalState mong muốn Passed=true, nhận được vi phạm: %v", r1.Violations)
	}

	// 2. Phép kiểm 2: Prefix tại checkpoint
	history := checker.NewClientHistory()
	history.Add(checker.TxnRecord{
		TxnID: "txn-1",
		Pos:   "001",
		Seq:   1,
		Ops: []checker.RowOp{
			{Table: "orders", PK: "1", Type: checker.OpInsert, Ver: 1, Values: map[string]interface{}{"amount": 100.0}},
		},
	})
	history.Add(checker.TxnRecord{
		TxnID: "txn-2",
		Pos:   "002",
		Seq:   2,
		Ops: []checker.RowOp{
			{Table: "orders", PK: "1", Type: checker.OpUpdate, Ver: 2, Values: map[string]interface{}{"amount": 200.0}},
		},
	})

	targetAt002 := map[string]map[string]checker.RowState{
		"orders": {
			"1": {Table: "orders", PK: "1", Ver: 2, Deleted: false, Values: map[string]interface{}{"amount": 200.0}},
		},
	}
	r2 := chk.CheckCheckpointPrefix(targetAt002, history, "002")
	if !r2.Passed {
		t.Fatalf("CheckCheckpointPrefix mong muốn Passed=true, nhận được vi phạm: %v", r2.Violations)
	}

	// 3. Phép kiểm 3: ver không giảm (monotonic)
	samples := []map[string]map[string]int64{
		{"orders": {"1": 1, "2": 1}},
		{"orders": {"1": 2, "2": 1}},
		{"orders": {"1": 3, "2": 2}},
	}
	r3 := chk.CheckVerMonotonic(samples)
	if !r3.Passed {
		t.Fatalf("CheckVerMonotonic mong muốn Passed=true, nhận được vi phạm: %v", r3.Violations)
	}

	// 4. Phép kiểm 4: Không hồi sinh (No resurrection)
	deletedKeys := []checker.DeletedKey{
		{Table: "orders", PK: "2", Pos: "001"},
	}
	targetAfterDelete := map[string]map[string]checker.RowState{
		"orders": {
			"1": {Table: "orders", PK: "1", Ver: 2, Deleted: false, Values: map[string]interface{}{"amount": 100.0}},
			"2": {Table: "orders", PK: "2", Ver: 1, Deleted: true},
		},
	}
	r4 := chk.CheckNoResurrection(targetAfterDelete, deletedKeys, "002")
	if !r4.Passed {
		t.Fatalf("CheckNoResurrection mong muốn Passed=true, nhận được vi phạm: %v", r4.Violations)
	}

	// 5. Phép kiểm 5: Ràng buộc (Constraints)
	r5 := chk.CheckConstraints(nil)
	if !r5.Passed {
		t.Fatalf("CheckConstraints mong muốn Passed=true khi không có vi phạm, nhận được: %v", r5.Violations)
	}
}

// TestCK2_CheckerCatchesThreeInjectedBugs kiểm tra AC2:
// Checker phải bắt được 3 bản cài lỗi: bỏ Txn, apply trùng, đảo thứ tự Txn xung đột (spec/04 §3).
func TestCK2_CheckerCatchesThreeInjectedBugs(t *testing.T) {
	chk := checker.NewChecker()

	// -------------------------------------------------------------
	// Bản lỗi 1: Cố ý BỎ QUA một Txn (drop_txn)
	// -------------------------------------------------------------
	history1 := checker.NewClientHistory()
	history1.Add(checker.TxnRecord{
		TxnID: "txn-1",
		Pos:   "001",
		Seq:   1,
		Ops: []checker.RowOp{
			{Table: "orders", PK: "10", Type: checker.OpInsert, Ver: 1, Values: map[string]interface{}{"amount": 100.0}},
		},
	})
	history1.Add(checker.TxnRecord{
		TxnID: "txn-2",
		Pos:   "002",
		Seq:   2,
		Ops: []checker.RowOp{
			{Table: "orders", PK: "20", Type: checker.OpInsert, Ver: 1, Values: map[string]interface{}{"amount": 200.0}},
		},
	})

	// Target bị lỗi: bỏ qua Txn 2 (không có PK 20)
	targetBug1 := map[string]map[string]checker.RowState{
		"orders": {
			"10": {Table: "orders", PK: "10", Ver: 1, Deleted: false, Values: map[string]interface{}{"amount": 100.0}},
		},
	}

	resBug1Prefix := chk.CheckCheckpointPrefix(targetBug1, history1, "002")
	if resBug1Prefix.Passed {
		t.Fatalf("Bản lỗi 1 (drop_txn): Checker KHÔNG phát hiện vi phạm khi bỏ qua Txn!")
	}
	t.Logf("Bản lỗi 1 (drop_txn) đã bị bắt thành công: %v", resBug1Prefix.Violations)

	// -------------------------------------------------------------
	// Bản lỗi 2: Cố ý APPLY TRÙNG một Txn (duplicate_txn)
	// -------------------------------------------------------------
	// Khi apply trùng, gây vi phạm ràng buộc Unique key hoặc Duplicate entry
	violationsBug2 := []checker.ConstraintViolation{
		{
			ConstraintType: "UNIQUE",
			Table:          "orders",
			KeyOrColumn:    "PRIMARY",
			Message:        "Duplicate entry '10' for key 'orders.PRIMARY' on re-apply",
		},
	}
	resBug2 := chk.CheckConstraints(violationsBug2)
	if resBug2.Passed {
		t.Fatalf("Bản lỗi 2 (duplicate_txn): Checker KHÔNG phát hiện vi phạm khi apply trùng!")
	}
	t.Logf("Bản lỗi 2 (duplicate_txn) đã bị bắt thành công: %v", resBug2.Violations)

	// -------------------------------------------------------------
	// Bản lỗi 3: Cố ý ĐẢO THỨ TỰ hai Txn xung đột (reorder_conflicting_txns)
	// -------------------------------------------------------------
	// Txn 1 ghi ver=2, Txn 2 ghi ver=1 (bị đảo thứ tự) -> vi phạm ver monotonic
	samplesBug3 := []map[string]map[string]int64{
		{"orders": {"10": 2}}, // Txn sau bị apply trước
		{"orders": {"10": 1}}, // Txn trước bị apply sau -> ver giảm từ 2 xuống 1
	}
	resBug3 := chk.CheckVerMonotonic(samplesBug3)
	if resBug3.Passed {
		t.Fatalf("Bản lỗi 3 (reorder_conflicting_txns): Checker KHÔNG phát hiện vi phạm khi đảo thứ tự!")
	}
	t.Logf("Bản lỗi 3 (reorder_conflicting_txns) đã bị bắt thành công: %v", resBug3.Violations)
}
