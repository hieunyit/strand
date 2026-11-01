package assemble

import (
	"fmt"
	"reflect"
	"sync"

	"strand/internal/model"
)

// TableSchema lưu trữ metadata schema của bảng phục vụ xác định PK.
type TableSchema struct {
	TableID       model.TableID
	PKColumns     []string
	Columns       []string
	PKIndices     []int
	SinglePKIndex int // >= 0 nếu bảng có đúng 1 cột PK
}

// ExtractPK trích xuất giá trị khóa chính từ hàng dữ liệu.
func (t *TableSchema) ExtractPK(row []any) []model.Value {
	if t == nil || len(t.PKIndices) == 0 {
		if len(row) > 0 {
			return []model.Value{row[0]}
		}
		return nil
	}

	if t.SinglePKIndex >= 0 && t.SinglePKIndex < len(row) {
		return []model.Value{row[t.SinglePKIndex]}
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
func (t *TableSchema) ExtractCols(row []any) []model.ColValue {
	cols := make([]model.ColValue, len(row))
	for i, v := range row {
		name := ""
		if t != nil && i < len(t.Columns) {
			name = t.Columns[i]
		} else {
			name = fmt.Sprintf("col_%d", i)
		}
		cols[i] = model.ColValue{
			Name:  name,
			Value: v,
		}
	}
	return cols
}

// SchemaRegistry quản lý cache schema các bảng.
type SchemaRegistry struct {
	mu     sync.RWMutex
	tables map[model.TableID]*TableSchema
}

// NewSchemaRegistry tạo SchemaRegistry mới.
func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{
		tables: make(map[model.TableID]*TableSchema),
	}
}

// RegisterTable đăng ký schema cho một bảng.
func (r *SchemaRegistry) RegisterTable(schema, table string, pkColumns []string, columns []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	tid := model.TableID{Schema: schema, Table: table}
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

	singlePK := -1
	if len(pkIndices) == 1 {
		singlePK = pkIndices[0]
	}

	r.tables[tid] = &TableSchema{
		TableID:       tid,
		PKColumns:     pkColumns,
		Columns:       columns,
		PKIndices:     pkIndices,
		SinglePKIndex: singlePK,
	}
}

// GetTable lấy metadata schema của bảng.
func (r *SchemaRegistry) GetTable(schema, table string) *TableSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tables[model.TableID{Schema: schema, Table: table}]
}

// ExtractPK trích xuất giá trị khóa chính từ hàng dữ liệu.
func (r *SchemaRegistry) ExtractPK(schema, table string, row []any) []model.Value {
	t := r.GetTable(schema, table)
	return t.ExtractPK(row)
}

// ExtractCols trích xuất danh sách ColValue từ hàng dữ liệu.
func (r *SchemaRegistry) ExtractCols(schema, table string, row []any) []model.ColValue {
	t := r.GetTable(schema, table)
	return t.ExtractCols(row)
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
