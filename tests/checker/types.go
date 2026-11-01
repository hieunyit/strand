package checker

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// OpType loại thao tác dữ liệu.
type OpType string

const (
	OpInsert OpType = "INSERT"
	OpUpdate OpType = "UPDATE"
	OpDelete OpType = "DELETE"
)

// RowOp mô tả thao tác trên một row trong transaction.
type RowOp struct {
	Table  string                 `json:"table"`
	PK     string                 `json:"pk"`
	Type   OpType                 `json:"type"`
	Ver    int64                  `json:"ver"`
	Values map[string]interface{} `json:"values"`
}

// TxnRecord ghi lại transaction ở phía client kèm vị trí theo spec/04 §3.
type TxnRecord struct {
	TxnID string  `json:"txn_id"`
	Pos   string  `json:"pos"` // GTID hoặc LSN
	Seq   int64   `json:"seq"` // Thứ tự commit phía client
	Ops   []RowOp `json:"ops"`
}

// RowState đại diện trạng thái của một row tại một thời điểm.
type RowState struct {
	Table   string                 `json:"table"`
	PK      string                 `json:"pk"`
	Ver     int64                  `json:"ver"`
	Deleted bool                   `json:"deleted"`
	Values  map[string]interface{} `json:"values"`
}

// Hash tính toán mã băm sha256 cho row để so sánh trạng thái cuối.
func (r *RowState) Hash() string {
	if r.Deleted {
		return "DELETED"
	}
	// Sắp xếp keys để hash deterministic
	keys := make([]string, 0, len(r.Values))
	for k := range r.Values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s:%s:v%d:", r.Table, r.PK, r.Ver))
	for _, k := range keys {
		sb.WriteString(fmt.Sprintf("%s=%v;", k, r.Values[k]))
	}
	h := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(h[:])
}

// ClientHistory lưu toàn bộ lịch sử commit của client.
type ClientHistory struct {
	Txns []TxnRecord
}

// NewClientHistory khởi tạo ClientHistory.
func NewClientHistory() *ClientHistory {
	return &ClientHistory{Txns: make([]TxnRecord, 0)}
}

// Add ghi nhận một transaction đã commit.
func (h *ClientHistory) Add(txn TxnRecord) {
	h.Txns = append(h.Txns, txn)
}

// ReconstructState dựng lại trạng thái kỳ vọng của tất cả các bảng đến vị trí upToPos (hoặc hết lịch sử nếu rỗng).
func (h *ClientHistory) ReconstructState(upToPos string) map[string]map[string]RowState {
	state := make(map[string]map[string]RowState)

	for _, txn := range h.Txns {
		for _, op := range txn.Ops {
			if _, ok := state[op.Table]; !ok {
				state[op.Table] = make(map[string]RowState)
			}

			if op.Type == OpDelete {
				state[op.Table][op.PK] = RowState{
					Table:   op.Table,
					PK:      op.PK,
					Ver:     op.Ver,
					Deleted: true,
				}
			} else {
				vals := make(map[string]interface{})
				for k, v := range op.Values {
					vals[k] = v
				}
				state[op.Table][op.PK] = RowState{
					Table:   op.Table,
					PK:      op.PK,
					Ver:     op.Ver,
					Deleted: false,
					Values:  vals,
				}
			}
		}

		if upToPos != "" && txn.Pos == upToPos {
			break
		}
	}
	return state
}

// DeletedKey ghi nhận khóa đã bị xóa ở vị trí nào.
type DeletedKey struct {
	Table string `json:"table"`
	PK    string `json:"pk"`
	Pos   string `json:"pos"`
}

// CheckResult kết quả của từng phép kiểm tính đúng.
type CheckResult struct {
	CheckName  string   `json:"check_name"`
	Passed     bool     `json:"passed"`
	Violations []string `json:"violations"`
}
