package model

import (
	"fmt"
)

// spec: 02 §3

// Engine định danh hệ quản trị cơ sở dữ liệu nguồn/đích.
type Engine string

const (
	EngineMySQL Engine = "mysql"
	EnginePG    Engine = "pg"
)

// Pos là thứ duy nhất về vị trí được ghi xuống (CP-3).
type Pos struct {
	Engine Engine `json:"engine"`
	GTID   string `json:"gtid,omitempty"` // MySQL: "uuid[:tag]:n"
	LSN    uint64 `json:"lsn,omitempty"`  // PG: commit LSN
}

func (p Pos) String() string {
	if p.Engine == EngineMySQL {
		return fmt.Sprintf("mysql:%s", p.GTID)
	}
	return fmt.Sprintf("pg:%X/%X", uint32(p.LSN>>32), uint32(p.LSN))
}

// TableID định danh một bảng theo schema và table name.
type TableID struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

func (t TableID) String() string {
	if t.Schema == "" {
		return t.Table
	}
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// Op là loại thao tác trên một hàng (Insert, Update, Delete).
type Op string

const (
	OpInsert Op = "insert"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

// Value đại diện cho giá trị của một cột trong khóa chính hoặc hàng.
type Value any

// ColValue biểu diễn giá trị của một cột trong hàng dữ liệu.
type ColValue struct {
	Name      string `json:"name"`
	Value     Value  `json:"value"`
	Unchanged bool   `json:"unchanged,omitempty"` // spec: 02 §3 ColValue.Unchanged = true với cột TOAST vắng mặt
}

// RowChange biểu diễn một thay đổi trên một hàng.
type RowChange struct {
	Table  TableID    `json:"table"`
	Op     Op         `json:"op"`
	Key    []Value    `json:"key"`
	OldKey []Value    `json:"old_key,omitempty"` // khác nil khi UPDATE đổi PK
	Cols   []ColValue `json:"cols"`
}

// Txn biểu diễn một transaction đã lắp ráp đủ từ nguồn.
// invariant: CP-3
// seq chỉ nằm trong RAM phục vụ scheduler sắp việc, KHÔNG BAO GIỜ được serialize hay ghi xuống.
type Txn struct {
	seq       uint64      `json:"-"` // chỉ trong RAM, không serialize (CP-3)
	SourcePos Pos         `json:"source_pos"`
	SchemaVer uint32      `json:"schema_ver"`
	Rows      []RowChange `json:"rows"`
	Bytes     int64       `json:"bytes"`

	// Hỗ trợ transaction lớn tràn đĩa theo spec: design §6.4
	SpillPath string `json:"spill_path,omitempty"`
	RowCount  int64  `json:"row_count,omitempty"`
}

// IsSpilled kiểm tra transaction có bị tràn xuống đĩa không.
func (t *Txn) IsSpilled() bool {
	return t != nil && t.SpillPath != ""
}

// TotalRows trả về tổng số dòng thay đổi trong transaction.
func (t *Txn) TotalRows() int64 {
	if t == nil {
		return 0
	}
	if t.RowCount > 0 {
		return t.RowCount
	}
	return int64(len(t.Rows))
}

// Seq trả về số thứ tự nội bộ trong RAM.
func (t *Txn) Seq() uint64 {
	if t == nil {
		return 0
	}
	return t.seq
}

// SetSeq gán số thứ tự nội bộ trong RAM.
func (t *Txn) SetSeq(s uint64) {
	if t != nil {
		t.seq = s
	}
}

// Checkpoint lưu trạng thái đã commit tại target.
// invariant: CP-3
// Không có trường seq.
type Checkpoint struct {
	PipelineID string `json:"pipeline_id"`
	Applied    Pos    `json:"applied"`
	CreatedAt  int64  `json:"created_at"`
}
