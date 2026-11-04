package pipeline

import (
	"context"
	"fmt"

	"strand/internal/model"
)

// Cột generated ở TARGET (MySQL `AS (...) STORED|VIRTUAL`, PG `GENERATED ALWAYS AS (...) STORED`)
// không nhận giá trị tường minh: MySQL trả Error 3105, PG trả 428C9. Target tự tính chúng từ các cột
// thường, nên Strand bỏ chúng khỏi mọi câu ghi (snapshot và stream).
//
// Xét theo target chứ không theo source: MySQL→PG có thể ánh xạ cột generated của source thành cột
// thường ở target (tạo tay) — khi đó giá trị vẫn phải được ghi.
//
// tableMeta.Columns giữ NGUYÊN danh sách cột source theo vị trí: registry của assembler ánh xạ cột
// của row event binlog theo vị trí (assemble.SchemaRegistry.RegisterTable), bỏ cột ở đó sẽ lệch tên.
// Chỉ lọc ở chỗ ghi: writableColumns (snapshot) và dropTargetGenerated (stream, router).
//
// invariant: CP-1..5, CO-1/2 — không đổi ranh giới Txn, khóa writeset hay thứ tự; chỉ bớt cột mà
// target tự tính, nên trạng thái cuối của target vẫn bằng source.

// targetGeneratedColumns đọc tên các cột generated của bảng đích (sau rename).
func (p *Pipeline) targetGeneratedColumns(ctx context.Context, id model.TableID) (map[string]bool, error) {
	dst := p.filter.Target(id)
	var q string
	if p.dstEngine == model.EnginePG {
		q = `SELECT a.attname FROM pg_attribute a
		       JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		      WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''`
	} else {
		// GENERATION_EXPRESSION rỗng với cột thường; KHÔNG dùng EXTRA LIKE '%GENERATED%' vì cột có
		// DEFAULT biểu thức (vd. CURRENT_TIMESTAMP) có EXTRA = 'DEFAULT_GENERATED' mà vẫn ghi được.
		q = `SELECT COLUMN_NAME FROM information_schema.COLUMNS
		      WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND GENERATION_EXPRESSION <> ''`
	}
	rows, err := p.dst.QueryContext(ctx, q, dst.Schema, dst.Table)
	if err != nil {
		return nil, fmt.Errorf("read generated columns of target %s: %w", dst, err)
	}
	defer rows.Close()
	var out map[string]bool
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		if out == nil {
			out = map[string]bool{}
		}
		out[n] = true
	}
	return out, rows.Err()
}

// loadTargetGenerated gán TargetGenerated cho từng metadata (gọi sau khi bảng đích chắc chắn tồn tại).
func (p *Pipeline) loadTargetGenerated(ctx context.Context, metas map[model.TableID]*tableMeta) error {
	for id, m := range metas {
		if m == nil {
			continue
		}
		g, err := p.targetGeneratedColumns(ctx, id)
		if err != nil {
			return err
		}
		m.TargetGenerated = g
	}
	return nil
}

// writableColumns là các cột source được ghi sang target (bỏ cột generated ở target), theo thứ tự.
func writableColumns(m *tableMeta) []string {
	if len(m.TargetGenerated) == 0 {
		return m.Columns
	}
	out := make([]string, 0, len(m.Columns))
	for _, c := range m.Columns {
		if !m.TargetGenerated[c] {
			out = append(out, c)
		}
	}
	return out
}

// dropTargetGenerated trả bản sao cols không gồm cột generated ở target. Luôn cấp slice mới khi có
// cột bị bỏ: RowChange gốc có thể vẫn nằm trong buffer của range-state.
func dropTargetGenerated(cols []model.ColValue, skip map[string]bool) []model.ColValue {
	if len(skip) == 0 || len(cols) == 0 {
		return cols
	}
	n := 0
	for _, c := range cols {
		if skip[c.Name] {
			n++
		}
	}
	if n == 0 {
		return cols
	}
	out := make([]model.ColValue, 0, len(cols)-n)
	for _, c := range cols {
		if !skip[c.Name] {
			out = append(out, c)
		}
	}
	return out
}
