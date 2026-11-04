package model

import (
	"fmt"

	"github.com/zeebo/xxh3"
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

// UniqueKey định danh một unique key trên một hàng theo spec: 02 §6.
type UniqueKey struct {
	Name   string  `json:"name,omitempty"`
	Key    []Value `json:"key"`
	OldKey []Value `json:"old_key,omitempty"`
}

// ForeignKey định danh khóa ngoại tham chiếu tới bảng cha theo spec: 02 §6.
type ForeignKey struct {
	ParentTable TableID `json:"parent_table"`
	ParentKey   []Value `json:"parent_key"`
}

// RowChange biểu diễn một thay đổi trên một hàng.
type RowChange struct {
	Table       TableID      `json:"table"`
	Op          Op           `json:"op"`
	Key         []Value      `json:"key"`
	OldKey      []Value      `json:"old_key,omitempty"` // khác nil khi UPDATE đổi PK
	UniqueKeys  []UniqueKey  `json:"unique_keys,omitempty"`
	ForeignKeys []ForeignKey `json:"foreign_keys,omitempty"`
	Cols        []ColValue   `json:"cols"`
}

// AddUniqueKey thêm một unique index key vào row change.
func (r *RowChange) AddUniqueKey(name string, key []Value, oldKey ...[]Value) {
	uk := UniqueKey{Name: name, Key: key}
	if len(oldKey) > 0 {
		uk.OldKey = oldKey[0]
	}
	r.UniqueKeys = append(r.UniqueKeys, uk)
}

// AddForeignKey thêm tham chiếu khóa ngoại tới bảng cha vào row change.
func (r *RowChange) AddForeignKey(parentTable TableID, parentKey []Value) {
	r.ForeignKeys = append(r.ForeignKeys, ForeignKey{
		ParentTable: parentTable,
		ParentKey:   parentKey,
	})
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

	// SpilledTables liệt kê mọi bảng (kể cả bảng cha qua FK) mà Txn đã tràn đĩa chạm tới.
	// Txn tràn đĩa không có Rows trong RAM, nên scheduler dùng danh sách này để
	// dựng phụ thuộc cấp bảng (CO-1).
	SpilledTables []TableID `json:"spilled_tables,omitempty"`

	// DDL chứa câu lệnh DDL khi Txn là một transaction DDL (không có row).
	// Txn DDL vẫn có GTID riêng và phải được ghi nhận vào tập đã apply (CP-1, CP-2).
	DDL string `json:"ddl,omitempty"`
	// DDLSchema là schema mặc định của phiên chạy DDL (QUERY event), để định danh tên bảng không kèm schema.
	DDLSchema string `json:"ddl_schema,omitempty"`
	// BinlogFile/BinlogPos: vị trí event GTID của Txn trong binlog source (MySQL, chỉ khi reader biết;
	// không đi qua spool). Rỗng thì bên dùng phải tự tìm theo GTID.
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  uint64 `json:"binlog_pos,omitempty"`

	// SourceTS: thời điểm commit ở source (giây Unix, từ header binlog), dùng tính lag.
	SourceTS int64 `json:"source_ts,omitempty"`

	// XID: xid top-level của Txn ở source PG (từ BEGIN của pgoutput; 0 = không có / MySQL). Snapshot
	// logic từ source PG dùng nó để biết Txn có visible với snapshot đọc chunk hay không (02 §5).
	XID uint64 `json:"xid,omitempty"`
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

// WritesetByTable tính toán các hash khóa xxh3 theo từng bảng cho Txn theo spec: 02 §6.
// Writeset của Txn T là tập hash xxh3 của:
// - (table, PK) cho mọi row T chạm tới. Với UPDATE đổi PK, lấy cả PK cũ lẫn PK mới.
// - (table, unique_key) cho mọi unique index.
// - (parent_table, parent_key) cho FK.
func (t *Txn) WritesetByTable() map[string][]uint64 {
	ws := make(map[string][]uint64)
	if t == nil {
		return ws
	}
	for _, row := range t.Rows {
		tbl := row.Table.String()
		// 1. (table, PK) và OldKey nếu đổi PK
		if len(row.Key) > 0 {
			keyStr := fmt.Sprintf("%v", row.Key)
			h := xxh3.HashString(tbl + "|" + keyStr)
			ws[tbl] = append(ws[tbl], h)
		}
		if len(row.OldKey) > 0 {
			oldStr := fmt.Sprintf("%v", row.OldKey)
			hOld := xxh3.HashString(tbl + "|" + oldStr)
			ws[tbl] = append(ws[tbl], hOld)
		}

		// 2. (table, unique_key) cho mọi unique index
		for _, uk := range row.UniqueKeys {
			if len(uk.Key) > 0 {
				ukStr := fmt.Sprintf("%v", uk.Key)
				var hUK uint64
				if uk.Name != "" {
					hUK = xxh3.HashString(tbl + "|uk|" + uk.Name + "|" + ukStr)
				} else {
					hUK = xxh3.HashString(tbl + "|uk|" + ukStr)
				}
				ws[tbl] = append(ws[tbl], hUK)
			}
			if len(uk.OldKey) > 0 {
				oldUkStr := fmt.Sprintf("%v", uk.OldKey)
				var hOldUK uint64
				if uk.Name != "" {
					hOldUK = xxh3.HashString(tbl + "|uk|" + uk.Name + "|" + oldUkStr)
				} else {
					hOldUK = xxh3.HashString(tbl + "|uk|" + oldUkStr)
				}
				ws[tbl] = append(ws[tbl], hOldUK)
			}
		}

		// 3. (parent_table, parent_key) cho FK
		for _, fk := range row.ForeignKeys {
			if len(fk.ParentKey) > 0 {
				parentTbl := fk.ParentTable.String()
				fkStr := fmt.Sprintf("%v", fk.ParentKey)
				hFK := xxh3.HashString(parentTbl + "|" + fkStr)
				ws[parentTbl] = append(ws[parentTbl], hFK)
			}
		}
	}
	return ws
}

// Checkpoint lưu trạng thái đã commit tại target.
// invariant: CP-3
// Không có trường seq.
type Checkpoint struct {
	PipelineID string `json:"pipeline_id"`
	Applied    Pos    `json:"applied"`
	CreatedAt  int64  `json:"created_at"`
}
