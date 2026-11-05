package pipeline

import (
	"context"
	"fmt"
	"strand/internal/snapshot"
	"strand/internal/writer"
	"strings"

	"strand/internal/exitcode"
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

// checkSnapshotKey: bảng có thể snapshot logic với PK của nó không (việc C). PK một cột số nguyên: đường
// int64. PK khác cần codec (kiểu/collation có thứ tự chứng minh được) và PK ở bảng đích có CÙNG thứ tự:
// flush xóa range khóa ở target bằng cận đọc từ source.
//   - cùng engine: cùng cột, cùng kiểu mã hóa, cùng kiểu SQL và collation (PKCodec.SameOrder);
//   - MySQL→PG: chỉ kiểu mà thứ tự hai engine trùng nhau — số (INT/UNSIGNED/DECIMAL ↔ int/numeric), thời
//     gian (DATE/DATETIME/TIMESTAMP ↔ date/timestamp/timestamptz), VARBINARY ↔ bytea. Chuỗi bị từ chối
//     (PAD SPACE / collation MySQL khác "C" của PG).
func (p *Pipeline) checkSnapshotKey(ctx context.Context, id model.TableID, m *tableMeta) error {
	if m.PKNumeric {
		return nil
	}
	reject := func(why string) error {
		return exitcode.New(exitcode.ErrTypeUnmappable, exitcode.Rejected,
			fmt.Sprintf("table %s needs a logical snapshot but its primary key is not supported: %s; seed it from a backup instead", id, why), nil)
	}
	if m.PKCodec == nil {
		why := "unknown key type"
		if m.PKCodecErr != nil {
			why = m.PKCodecErr.Error()
		}
		return reject(why)
	}
	tgt := p.filter.Target(id)
	var dc *snapshot.PKCodec
	var err error
	if p.dstEngine == model.EnginePG {
		dc, err = pgPKCodec(ctx, p.dst, tgt, m.PK)
	} else {
		dc, err = mysqlPKCodec(ctx, p.dst, tgt, m.PK)
	}
	if err != nil {
		return reject(fmt.Sprintf("target table %s: %v", tgt, err))
	}
	if p.srcEngine == p.dstEngine {
		err = m.PKCodec.SameOrder(dc)
	} else {
		err = crossEngineSameOrder(m.PKCodec, dc)
	}
	if err != nil {
		return reject(fmt.Sprintf("target table %s orders its primary key differently from the source: %v", tgt, err))
	}
	m.TargetPKCodec = dc
	return nil
}

// crossEngineSameOrder: MySQL→PG — từng cột PK cùng lớp thứ tự (xem checkSnapshotKey).
func crossEngineSameOrder(src, dst *snapshot.PKCodec) error {
	class := func(k snapshot.PKKind) string {
		switch k {
		case snapshot.PKInt, snapshot.PKUint, snapshot.PKDecimal:
			return "number"
		case snapshot.PKDateTime:
			return "datetime"
		case snapshot.PKBytes:
			return "bytes"
		}
		return ""
	}
	if len(src.Cols) != len(dst.Cols) {
		return fmt.Errorf("primary key has %d columns on the source and %d on the target", len(src.Cols), len(dst.Cols))
	}
	for i := range src.Cols {
		a, b := src.Cols[i], dst.Cols[i]
		ca := class(a.Kind)
		if a.Kind == snapshot.PKBytes && a.Collation != "" {
			ca = "" // chuỗi có collation (kể cả _0900_bin): không đối chiếu với text "C" của PG
		}
		if a.Name != b.Name || ca == "" || ca != class(b.Kind) {
			return fmt.Errorf("primary key column %s (%s → %s %s) has no engine-independent order", a.Name, a.Kind, b.Kind, b.SQLType)
		}
	}
	return nil
}

// pgFlushRange (FlushRange của snapshotter, target PG, việc C): thay range khóa [lo, hi] của bảng đích bằng
// rows — chỉ DELETE khóa của range VẮNG trong rows (không cascade row con của khóa còn lại), upsert phần
// còn lại, trong một transaction.
func (p *Pipeline) pgFlushRange(ctx context.Context, table model.TableID, columnNames []string, rows []*model.RowChange, lo, hi snapshot.Bound) error {
	return p.pgFlushGeneric(ctx, table, columnNames, rows, func(q snapshot.PGKeySQL) (string, []any, int, error) {
		return q.RangeCond(lo, hi, 1)
	})
}

// pgFlushKeySet (FlushKeySet, chunk re-read): như pgFlushRange cho đúng tập khóa keys.
func (p *Pipeline) pgFlushKeySet(ctx context.Context, table model.TableID, columnNames []string, rows []*model.RowChange, keys []snapshot.Bound) error {
	return p.pgFlushGeneric(ctx, table, columnNames, rows, func(q snapshot.PGKeySQL) (string, []any, int, error) {
		return q.InList(keys, 1)
	})
}

func (p *Pipeline) pgFlushGeneric(ctx context.Context, table model.TableID, columnNames []string, rows []*model.RowChange,
	scope func(snapshot.PGKeySQL) (string, []any, int, error)) error {
	src, ok := p.sourceOf(table)
	m := p.table(src)
	if !ok || m == nil || m.PKCodec == nil || m.TargetPKCodec == nil {
		return fmt.Errorf("flush %s: no primary key codec", table)
	}
	q := snapshot.PGKeySQL{Table: table, Codec: m.TargetPKCodec}
	cond, args, next, err := scope(q)
	if err != nil {
		return err
	}
	keep := make([]snapshot.Bound, 0, len(rows))
	for _, r := range rows {
		b, err := m.PKCodec.BoundOf(r.Key)
		if err != nil {
			return fmt.Errorf("flush %s: %w", table, err)
		}
		keep = append(keep, b)
	}
	if len(keep) > 0 {
		in, ka, _, err := q.InList(keep, next)
		if err != nil {
			return err
		}
		cond += " AND NOT (" + in + ")"
		args = append(args, ka...)
	}
	out, err := p.pgFlushImages(table, rows)
	if err != nil {
		return err
	}
	pkCols := make([]string, len(m.TargetPKCodec.Cols))
	for i, c := range m.TargetPKCodec.Cols {
		pkCols[i] = c.Name
	}
	return writer.PGReplaceWhere(ctx, p.dst, table, pkCols, columnNames, out, "DELETE FROM "+pgQuoteTable(table)+" WHERE "+cond, args...)
}

// pgClearUnreadRange (ClearUnreadRange): xóa ở bảng đích PG range [lo, hi] (khi hasRange) và các khóa keys.
func (p *Pipeline) pgClearUnreadRange(ctx context.Context, table model.TableID, lo, hi snapshot.Bound, hasRange bool, keys []snapshot.Bound) error {
	src, ok := p.sourceOf(table)
	m := p.table(src)
	if !ok || m == nil || m.TargetPKCodec == nil {
		return fmt.Errorf("clear unread %s: no primary key codec", table)
	}
	q := snapshot.PGKeySQL{Table: table, Codec: m.TargetPKCodec}
	var stmts []string
	var args [][]any
	if hasRange {
		cond, a, _, err := q.RangeCond(lo, hi, 1)
		if err != nil {
			return err
		}
		stmts, args = append(stmts, "DELETE FROM "+pgQuoteTable(table)+" WHERE "+cond), append(args, a)
	}
	if len(keys) > 0 {
		cond, a, _, err := q.InList(keys, 1)
		if err != nil {
			return err
		}
		stmts, args = append(stmts, "DELETE FROM "+pgQuoteTable(table)+" WHERE "+cond), append(args, a)
	}
	return writer.PGExecFenced(ctx, p.dst, stmts, args)
}

func pgQuoteTable(t model.TableID) string {
	q := func(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
	return q(t.Schema) + "." + q(t.Table)
}
