package checker

import (
	"fmt"
	"sort"
	"strings"

	"strand/internal/checkpoint"
)

// Checker thực hiện 5 phép kiểm tính đúng độc lập theo spec/04 §3.
type Checker struct{}

// NewChecker khởi tạo Checker.
func NewChecker() *Checker {
	return &Checker{}
}

// 1. CheckFinalState kiểm tra trạng thái cuối bằng nhau:
// Dừng tải, chờ lag về 0, so hash từng row theo thứ tự PK giữa source và target (CP-1, CP-2).
func (c *Checker) CheckFinalState(source, target map[string]map[string]RowState) CheckResult {
	res := CheckResult{
		CheckName:  "FinalStateEquality",
		Passed:     true,
		Violations: make([]string, 0),
	}

	// Tập hợp tất cả các bảng ở cả 2 phía
	tableMap := make(map[string]bool)
	for t := range source {
		tableMap[t] = true
	}
	for t := range target {
		tableMap[t] = true
	}

	for table := range tableMap {
		sRows := source[table]
		tRows := target[table]

		// Lấy danh sách PK
		pkMap := make(map[string]bool)
		for pk := range sRows {
			pkMap[pk] = true
		}
		for pk := range tRows {
			pkMap[pk] = true
		}

		pkList := make([]string, 0, len(pkMap))
		for pk := range pkMap {
			pkList = append(pkList, pk)
		}
		sort.Strings(pkList)

		for _, pk := range pkList {
			sRow, sExists := sRows[pk]
			tRow, tExists := tRows[pk]

			if sExists && !tExists {
				if !sRow.Deleted {
					res.Passed = false
					res.Violations = append(res.Violations,
						fmt.Sprintf("bảng %s pk=%s tồn tại ở source nhưng thiếu ở target", table, pk))
				}
				continue
			}

			if !sExists && tExists {
				if !tRow.Deleted {
					res.Passed = false
					res.Violations = append(res.Violations,
						fmt.Sprintf("bảng %s pk=%s tồn tại ở target nhưng không có ở source", table, pk))
				}
				continue
			}

			// Cả 2 cùng tồn tại
			if sRow.Deleted != tRow.Deleted {
				res.Passed = false
				res.Violations = append(res.Violations,
					fmt.Sprintf("bảng %s pk=%s trạng thái xóa không khớp (source=%v, target=%v)", table, pk, sRow.Deleted, tRow.Deleted))
				continue
			}

			if !sRow.Deleted {
				sHash := sRow.Hash()
				tHash := tRow.Hash()
				if sHash != tHash {
					res.Passed = false
					res.Violations = append(res.Violations,
						fmt.Sprintf("bảng %s pk=%s lệch hash (source=%s ver=%d, target=%s ver=%d)", table, pk, sHash, sRow.Ver, tHash, tRow.Ver))
				}
			}
		}
	}

	return res
}

// 2. CheckCheckpointPrefix kiểm tra Prefix tại checkpoint (strict):
// Dựng lại trạng thái kỳ vọng từ lịch sử đến checkpoint X, so sánh với dữ liệu target (CP-5).
// Target phải bằng đúng prefix: thiếu row/version của TXN <= X là vi phạm, và có row/version
// của TXN sau X (row không có trong prefix, hoặc ver mới hơn) cũng là vi phạm.
func (c *Checker) CheckCheckpointPrefix(target map[string]map[string]RowState, history *ClientHistory, checkpointPos string) CheckResult {
	res := CheckResult{
		CheckName:  "CheckpointPrefix",
		Passed:     true,
		Violations: make([]string, 0),
	}

	// Checkpoint không có trong lịch sử thì không dựng được prefix; không được lặng lẽ so với
	// toàn bộ lịch sử.
	found := false
	for _, txn := range history.Txns {
		if txn.Pos == checkpointPos {
			found = true
			break
		}
	}
	if !found {
		res.Passed = false
		res.Violations = append(res.Violations,
			fmt.Sprintf("checkpoint %q không có trong lịch sử client", checkpointPos))
		return res
	}

	expected := history.ReconstructState(checkpointPos)

	// So sánh target với expected
	for table, expRows := range expected {
		tgtRows := target[table]
		for pk, expRow := range expRows {
			tgtRow, ok := tgtRows[pk]
			if !ok {
				if !expRow.Deleted {
					res.Passed = false
					res.Violations = append(res.Violations,
						fmt.Sprintf("tại checkpoint %s: bảng %s thiếu row pk=%s", checkpointPos, table, pk))
				}
				continue
			}

			if expRow.Deleted != tgtRow.Deleted {
				res.Passed = false
				res.Violations = append(res.Violations,
					fmt.Sprintf("tại checkpoint %s: bảng %s pk=%s trạng thái xóa không khớp (expected=%v, target=%v)",
						checkpointPos, table, pk, expRow.Deleted, tgtRow.Deleted))
				continue
			}

			if !expRow.Deleted && expRow.Hash() != tgtRow.Hash() {
				res.Passed = false
				res.Violations = append(res.Violations,
					fmt.Sprintf("tại checkpoint %s: bảng %s pk=%s lệch dữ liệu so với prefix (exp_ver=%d, tgt_ver=%d)",
						checkpointPos, table, pk, expRow.Ver, tgtRow.Ver))
			}
		}
	}

	// Chiều ngược lại: row có ở target nhưng không có trong prefix là dữ liệu của TXN sau
	// checkpoint (hoặc dữ liệu lạ).
	tables := make([]string, 0, len(target))
	for table := range target {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	for _, table := range tables {
		pks := make([]string, 0, len(target[table]))
		for pk := range target[table] {
			pks = append(pks, pk)
		}
		sort.Strings(pks)
		for _, pk := range pks {
			tgtRow := target[table][pk]
			if tgtRow.Deleted {
				continue
			}
			if _, ok := expected[table][pk]; !ok {
				res.Passed = false
				res.Violations = append(res.Violations,
					fmt.Sprintf("tại checkpoint %s: bảng %s pk=%s (ver=%d) có ở target nhưng không thuộc prefix (dữ liệu của TXN sau checkpoint)",
						checkpointPos, table, pk, tgtRow.Ver))
			}
		}
	}

	return res
}

// 3. CheckVerMonotonic kiểm tra ver không giảm:
// Lấy mẫu target liên tục; ver của mỗi khóa chỉ được tăng, không bao giờ giảm (RS-1).
func (c *Checker) CheckVerMonotonic(samples []map[string]map[string]int64) CheckResult {
	res := CheckResult{
		CheckName:  "VerMonotonic",
		Passed:     true,
		Violations: make([]string, 0),
	}

	if len(samples) < 2 {
		return res
	}

	// Theo dõi ver lớn nhất từng thấy cho mỗi key (table, pk)
	highestVer := make(map[string]map[string]int64)

	for sampleIdx, sample := range samples {
		for table, rows := range sample {
			if _, ok := highestVer[table]; !ok {
				highestVer[table] = make(map[string]int64)
			}

			for pk, ver := range rows {
				prevVer, exists := highestVer[table][pk]
				if exists && ver < prevVer {
					res.Passed = false
					res.Violations = append(res.Violations,
						fmt.Sprintf("mẫu %d: bảng %s pk=%s vi phạm ver monotonic (ver giảm từ %d xuống %d)",
							sampleIdx, table, pk, prevVer, ver))
				} else if ver > prevVer {
					highestVer[table][pk] = ver
				}
			}
		}
	}

	return res
}

// 4. CheckNoResurrection kiểm tra không hồi sinh:
// Khóa đã DELETE ở vị trí thuộc checkpoint thì không được xuất hiện lại ở target (RS-1, đổi PK).
// del.Pos và checkpointPos là GTID set của MySQL; DELETE thuộc checkpoint khi GTID set của nó
// là tập con của GTID set checkpoint.
func (c *Checker) CheckNoResurrection(target map[string]map[string]RowState, deletedKeys []DeletedKey, checkpointPos string) CheckResult {
	res := CheckResult{
		CheckName:  "NoResurrection",
		Passed:     true,
		Violations: make([]string, 0),
	}

	// Vị trí là GTID set của MySQL; so theo quan hệ tập hợp, không so chuỗi
	// ("uuid:9" <= "uuid:10" là sai khi so chuỗi).
	cpSet, err := checkpoint.ParseMySQLGTIDSet(checkpointPos)
	if err != nil || strings.TrimSpace(checkpointPos) == "" {
		res.Passed = false
		res.Violations = append(res.Violations,
			fmt.Sprintf("checkpoint %q không phải GTID set hợp lệ: %v", checkpointPos, err))
		return res
	}

	for _, del := range deletedKeys {
		delSet, err := checkpoint.ParseMySQLGTIDSet(del.Pos)
		if err != nil || strings.TrimSpace(del.Pos) == "" {
			res.Passed = false
			res.Violations = append(res.Violations,
				fmt.Sprintf("bảng %s pk=%s: vị trí DELETE %q không phải GTID set hợp lệ: %v", del.Table, del.PK, del.Pos, err))
			continue
		}
		// Chỉ kiểm tra các key có DELETE nằm trong checkpoint (GTID set của DELETE ⊆ checkpoint).
		if !delSet.IsSubsetOf(cpSet) {
			continue
		}
		if tableRows, ok := target[del.Table]; ok {
			if row, exists := tableRows[del.PK]; exists && !row.Deleted {
				res.Passed = false
				res.Violations = append(res.Violations,
					fmt.Sprintf("khóa hồi sinh trái phép: bảng %s pk=%s đã DELETE ở vị trí %s (thuộc checkpoint %s) nhưng vẫn xuất hiện ở target với ver=%d",
						del.Table, del.PK, del.Pos, checkpointPos, row.Ver))
			}
		}
	}

	return res
}

// ConstraintViolation lỗi ràng buộc.
type ConstraintViolation struct {
	ConstraintType string `json:"constraint_type"` // UNIQUE hoặc FOREIGN_KEY
	Table          string `json:"table"`
	KeyOrColumn    string `json:"key_or_column"`
	Message        string `json:"message"`
}

// 5. CheckConstraints kiểm tra ràng buộc unique và FK:
// Target có đủ unique và FK như source; mọi lỗi ràng buộc khi apply đều là vi phạm (CO-1).
func (c *Checker) CheckConstraints(violations []ConstraintViolation) CheckResult {
	res := CheckResult{
		CheckName:  "Constraints",
		Passed:     len(violations) == 0,
		Violations: make([]string, 0, len(violations)),
	}

	for _, v := range violations {
		res.Violations = append(res.Violations,
			fmt.Sprintf("vi phạm ràng buộc %s trên bảng %s (%s): %s",
				v.ConstraintType, v.Table, v.KeyOrColumn, v.Message))
	}

	return res
}
