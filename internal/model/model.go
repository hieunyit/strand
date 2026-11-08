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
	// ParentIndex: tên unique index ở bảng cha mà FK tham chiếu; rỗng = PK của bảng cha.
	// ParentKey theo đúng thứ tự cột của index đó (review R11: FK tới UNIQUE ngoài PK).
	ParentIndex string  `json:"parent_index,omitempty"`
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

// AddForeignKey thêm tham chiếu khóa ngoại tới PK của bảng cha vào row change.
func (r *RowChange) AddForeignKey(parentTable TableID, parentKey []Value) {
	r.AddForeignKeyIndex(parentTable, "", parentKey)
}

// AddForeignKeyIndex thêm tham chiếu khóa ngoại tới unique index parentIndex của bảng cha
// (rỗng = PK).
func (r *RowChange) AddForeignKeyIndex(parentTable TableID, parentIndex string, parentKey []Value) {
	r.ForeignKeys = append(r.ForeignKeys, ForeignKey{
		ParentTable: parentTable,
		ParentIndex: parentIndex,
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
	// SpillOwned chỉ đánh dấu file tạm do process này tạo; spool decode luôn coi là file bền mượn.
	SpillOwned bool  `json:"-"`
	RowCount   int64 `json:"row_count,omitempty"`

	// SpilledTables liệt kê mọi bảng (kể cả bảng cha qua FK) mà Txn đã tràn đĩa chạm tới.
	// Txn tràn đĩa không có Rows trong RAM, nên scheduler dùng danh sách này để
	// dựng phụ thuộc cấp bảng (CO-1).
	SpilledTables []TableID `json:"spilled_tables,omitempty"`
	// SpillWriteset: writeset (hash khóa theo bảng, như WritesetByTable) của Txn tràn đĩa, gom khi ghi
	// file tràn (WritesetCollector). Bảng trong SpilledTables mà vắng mặt ở đây (vượt
	// table_level_threshold / writeset_budget, hoặc không ai gom) thì scheduler dựng phụ thuộc cấp bảng.
	// Chỉ trong RAM: Txn đọc lại từ spool không có nó, nên rơi về cấp bảng (bảo thủ, CO-1).
	SpillWriteset map[string][]uint64 `json:"-"`

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

	// Lane: Txn đi làn tuần tự của scheduler (relaxed, source PG, chạm bảng có ràng buộc duy nhất ngoài
	// PK mà writeset không thấy giá trị cũ): apply đúng thứ tự Admit, gom nhiều Txn liên tiếp vào một
	// commit (sched.NextLaneBatch). Chỉ trong RAM.
	Lane bool `json:"-"`

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
// Txn tràn đĩa không có Rows trong RAM: trả bản sao (map) của SpillWriteset — bảng vắng mặt trong đó
// được scheduler nâng lên cấp bảng qua SpilledTables.
func (t *Txn) WritesetByTable() map[string][]uint64 {
	ws := make(map[string][]uint64)
	if t == nil {
		return ws
	}
	if t.IsSpilled() {
		for tbl, keys := range t.SpillWriteset {
			ws[tbl] = keys
		}
		return ws
	}
	for i := range t.Rows {
		t.Rows[i].AppendWriteset(ws)
	}
	return ws
}

// SpillWritesetKeys: tổng số khóa trong SpillWriteset (0 khi Txn không tràn đĩa).
func (t *Txn) SpillWritesetKeys() int {
	if t == nil {
		return 0
	}
	n := 0
	for _, keys := range t.SpillWriteset {
		n += len(keys)
	}
	return n
}

// AppendWriteset thêm hash khóa writeset của row (spec: 02 §6) vào ws theo bảng. Dùng chung cho Txn
// trong RAM (WritesetByTable) và Txn tràn đĩa (WritesetCollector), nên hai đường cho cùng một tập khóa.
func (row *RowChange) AppendWriteset(ws map[string][]uint64) {
	// Khóa được chuẩn hóa bảo thủ theo collation (keycanon.go, invariant: CO-1): hai giá trị DB coi
	// là bằng nhau luôn cho cùng hash.
	pkHash := func(tbl string, key []Value) uint64 { return xxh3.HashString(tbl + "|" + canonKey(key)) }
	ukHash := func(tbl, name string, key []Value) uint64 {
		if name != "" {
			return xxh3.HashString(tbl + "|uk|" + name + "|" + canonKey(key))
		}
		return xxh3.HashString(tbl + "|uk|" + canonKey(key))
	}
	tbl := row.Table.String()
	// 1. (table, PK) và OldKey nếu đổi PK
	if len(row.Key) > 0 {
		ws[tbl] = append(ws[tbl], pkHash(tbl, row.Key))
	}
	if len(row.OldKey) > 0 {
		ws[tbl] = append(ws[tbl], pkHash(tbl, row.OldKey))
	}

	// 2. (table, unique_key) cho mọi unique index (kể cả khóa hằng của index có cột prefix)
	for _, uk := range row.UniqueKeys {
		if len(uk.Key) > 0 {
			ws[tbl] = append(ws[tbl], ukHash(tbl, uk.Name, uk.Key))
		}
		if len(uk.OldKey) > 0 {
			ws[tbl] = append(ws[tbl], ukHash(tbl, uk.Name, uk.OldKey))
		}
	}

	// 3. (parent_table, parent_key) cho FK — cùng namespace với khóa được tham chiếu ở bảng cha:
	// PK, hoặc unique index ParentIndex.
	for _, fk := range row.ForeignKeys {
		if len(fk.ParentKey) > 0 {
			parentTbl := fk.ParentTable.String()
			if fk.ParentIndex != "" {
				ws[parentTbl] = append(ws[parentTbl], ukHash(parentTbl, fk.ParentIndex, fk.ParentKey))
			} else {
				ws[parentTbl] = append(ws[parentTbl], pkHash(parentTbl, fk.ParentKey))
			}
		}
	}
}

// WritesetCollector gom writeset (chỉ hash khóa) của một Txn đang được ghi ra file tràn, từng row một,
// để Txn tràn đĩa vẫn giữ phụ thuộc cấp row (spec 02 §6: chỉ nâng cấp bảng khi > table_level_threshold
// khóa/bảng/Txn). Tràn đĩa là chuyện RAM của payload, không phải độ hạt xung đột.
//
// Bộ nhớ có trần: bảng nào vượt perTable khóa thì bỏ khóa của bảng đó (scheduler nâng nó lên cấp bảng
// — đúng như Txn trong RAM có len(keys) > threshold); tổng vượt total (writeset_budget) thì bỏ hết
// (mọi bảng của Txn lên cấp bảng, bảo thủ: chỉ thêm cạnh).
type WritesetCollector struct {
	perTable, total int
	ws              map[string][]uint64
	over            map[string]bool
	n               int
	dropped         bool
	touched         []string
}

// NewWritesetCollector tạo collector với trần khóa mỗi bảng (perTable) và trần tổng (total); ≤ 0 = không trần.
func NewWritesetCollector(perTable, total int) *WritesetCollector {
	return &WritesetCollector{
		perTable: perTable,
		total:    total,
		ws:       make(map[string][]uint64),
		over:     make(map[string]bool),
	}
}

// Add thêm khóa writeset của row.
func (c *WritesetCollector) Add(row *RowChange) {
	if c == nil || c.dropped {
		return
	}
	// Các bảng row chạm (bảng của row + bảng cha qua FK), không trùng.
	c.touched = append(c.touched[:0], row.Table.String())
	for _, fk := range row.ForeignKeys {
		name := fk.ParentTable.String()
		dup := false
		for _, x := range c.touched {
			if x == name {
				dup = true
				break
			}
		}
		if !dup {
			c.touched = append(c.touched, name)
		}
	}
	for _, tbl := range c.touched {
		c.n -= len(c.ws[tbl])
	}
	row.AppendWriteset(c.ws)
	for _, tbl := range c.touched {
		if c.over[tbl] || (c.perTable > 0 && len(c.ws[tbl]) > c.perTable) {
			c.over[tbl] = true
			delete(c.ws, tbl)
			continue
		}
		c.n += len(c.ws[tbl])
	}
	if c.total > 0 && c.n > c.total {
		c.dropped = true
		c.ws, c.over, c.n = nil, nil, 0
	}
}

// Writeset trả writeset đã gom (bảng vượt trần vắng mặt); nil khi đã bỏ hết vì vượt trần tổng.
func (c *WritesetCollector) Writeset() map[string][]uint64 {
	if c == nil || c.dropped || len(c.ws) == 0 {
		return nil
	}
	return c.ws
}

// Checkpoint lưu trạng thái đã commit tại target.
// invariant: CP-3
// Không có trường seq.
type Checkpoint struct {
	PipelineID string `json:"pipeline_id"`
	Applied    Pos    `json:"applied"`
	CreatedAt  int64  `json:"created_at"`
}
