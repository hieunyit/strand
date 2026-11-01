package assemble

import (
	"fmt"
	"reflect"
	"sync"

	"strand/internal/model"
)

// TableSchema lưu trữ metadata schema của bảng phục vụ xác định PK.
type TableSchema struct {
	TableID   model.TableID
	PKColumns []string
	Columns   []string
	PKIndices []int
}

// SchemaRegistry quản lý cache schema các bảng.
type SchemaRegistry struct {
	mu     sync.RWMutex
	tables map[string]*TableSchema // "schema.table" -> *TableSchema
}

// NewSchemaRegistry tạo SchemaRegistry mới.
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		tables: make(map[string]*TableSchema),
	}
}

// RegisterTable đăng ký schema cho một bảng.
func (r *SchemaRegistry) RegisterTable(schema, table string, pkColumns []string, columns []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := fmt.Sprintf("%s.%s", schema, table)
	pkIndices := make([]int, 0, len(pkColumns))

	colIndexMap := make(map[string]int, len(columns))
	for i, col := range columns {
		colIndexMap[col] = i
	}

	for _, pk := range pkColumns {
		if idx, ok := colIndexMap[pk]; ok {
			pkIndices = append(pkIndices, idx)
		}
	}

	// Nếu không tìm thấy chỉ số cột theo tên hoặc không có pkColumns, mặc định cột 0 là PK
	if len(pkIndices) == 0 {
		pkIndices = []int{0}
	}

	r.tables[key] = &TableSchema{
		TableID:   model.TableID{Schema: schema, Table: table},
		PKColumns: pkColumns,
		Columns:   columns,
		PKIndices: pkIndices,
	}
}

// GetTable lấy metadata schema của bảng.
func (r *SchemaRegistry) GetTable(schema, table string) *TableSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := fmt.Sprintf("%s.%s", schema, table)
	return r.tables[key]
}

// ExtractPK trích xuất giá trị khóa chính từ hàng dữ liệu.
func (r *SchemaRegistry) ExtractPK(schema, table string, row []any) []model.Value {
	t := r.GetTable(schema, table)
	if t == nil || len(t.PKIndices) == 0 {
		if len(row) > 0 {
			return []model.Value{row[0]}
		}
		return nil
	}

	pk := make([]model.Value, len(t.PKIndices))
	for i, idx := range t.PKIndices {
		if idx < len(row) {
			pk[i] = row[idx]
		}
	}
	return pk
}

// ExtractCols trích xuất danh sách ColValue từ hàng dữ liệu.
func (r *SchemaRegistry) ExtractCols(schema, table string, row []any) []model.ColValue {
	t := r.GetTable(schema, table)
	cols := make([]model.ColValue, len(row))
	for i, v := range row {
		name := fmt.Sprintf("col_%d", i)
		if t != nil && i < len(t.Columns) {
			name = t.Columns[i]
		}
		cols[i] = model.ColValue{
			Name:  name,
			Value: v,
		}
	}
	return cols
}

// KeysEqual so sánh hai khóa chính xem có bằng nhau không.
func KeysEqual(a, b []model.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !reflect.DeepEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}
