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
//   - MySQL→PG: kiểu mà thứ tự hai engine trùng nhau — số (INT/UNSIGNED/DECIMAL ↔ int/numeric), thời
//     gian (DATE/DATETIME/TIMESTAMP ↔ date/timestamp/timestamptz), VARBINARY ↔ bytea — đi đường range.
//     Chuỗi (PAD SPACE / collation MySQL khác "C" của PG) không cùng thứ tự: nhận khi
//     snapshot.CrossEngineKeySet thỏa (Gate 2), với ExactKeys — flush theo tập khóa chính xác và dọn phần
//     chưa đọc theo khóa source (snapshot/cross_order.go); ngược lại từ chối.
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
	exact := false
	if p.srcEngine == p.dstEngine {
		err = m.PKCodec.SameOrder(dc)
	} else {
		err = snapshot.CrossEngineSameOrder(m.PKCodec, dc)
		if err != nil && p.srcEngine == model.EngineMySQL && p.dstEngine == model.EnginePG {
			// Gate 2: thứ tự khác nhau (vd. _ci → "C") vẫn snapshot được nếu mọi thao tác ở target theo khóa
			// chính xác (snapshot/cross_order.go). Điều kiện: snapshot.CrossEngineKeySet.
			if kerr := snapshot.CrossEngineKeySet(m.PKCodec, dc); kerr == nil {
				err, exact = nil, true
			} else {
				err = fmt.Errorf("%v; exact-key snapshot is not possible either: %v", err, kerr)
			}
		}
	}
	if err != nil {
		return reject(fmt.Sprintf("target table %s orders its primary key differently from the source: %v", tgt, err))
	}
	p.tablesMu.Lock()
	m.TargetPKCodec, m.ExactKeys = dc, exact
	p.tablesMu.Unlock()
	if exact {
		p.log.Info("target orders the primary key differently from the source: snapshot flushes by exact key set",
			"table", id.String(), "target", tgt.String())
	}
	return nil
}

// exactKeys (ExactKeys của snapshotter): bảng nguồn id có PK mà target sắp khác thứ tự (checkSnapshotKey).
func (p *Pipeline) exactKeys(id model.TableID) bool {
	p.tablesMu.RLock()
	defer p.tablesMu.RUnlock()
	m := p.tables[id]
	return m != nil && m.ExactKeys
}

// exactKeyBatch: số khóa mỗi câu DELETE theo khóa chính xác / mỗi trang quét khóa target.
const exactKeyBatch = 1000

// pgFlushExact (FlushExact, Gate 2, snapshot/cross_order.go): DELETE đúng các khóa deletes (tombstone, theo
// giá trị chính xác, từng lô) rồi upsert rows, trong một transaction. Không DELETE theo range: target sắp
// khóa khác thứ tự source nên range source không biểu diễn được ở target; bất biến T (cross_order.go)
// bảo đảm target chưa có row nào của chunk.
func (p *Pipeline) pgFlushExact(ctx context.Context, table model.TableID, columnNames []string, rows []*model.RowChange, deletes []snapshot.Bound) error {
	src, ok := p.sourceOf(table)
	m := p.table(src)
	if !ok || m == nil || m.TargetPKCodec == nil || !m.ExactKeys {
		return fmt.Errorf("flush %s: no exact-key primary key codec", table)
	}
	q := snapshot.PGKeySQL{Table: table, Codec: m.TargetPKCodec}
	var dels []string
	var args [][]any
	for i := 0; i < len(deletes); i += exactKeyBatch {
		cond, a, _, err := q.InList(deletes[i:min(i+exactKeyBatch, len(deletes))], 1)
		if err != nil {
			return err
		}
		dels, args = append(dels, "DELETE FROM "+pgQuoteTable(table)+" WHERE "+cond), append(args, a)
	}
	out, err := p.pgFlushImages(table, rows)
	if err != nil {
		return err
	}
	pkCols := make([]string, len(m.TargetPKCodec.Cols))
	for i, c := range m.TargetPKCodec.Cols {
		pkCols[i] = c.Name
	}
	return writer.PGReplaceExact(ctx, p.dst, table, pkCols, columnNames, out, dels, args)
}

// pgClearUnreadExact (ClearUnreadRange của bảng ExactKeys): xóa ở target mọi row có khóa SOURCE thuộc phần
// chưa đọc của plan dở — range [lo, hi] theo thứ tự source (khi hasRange) và các khóa keys. Target không
// tính được range source, nên quét toàn bộ khóa target theo trang (keyset theo thứ tự target, exactKeyBatch
// khóa mỗi trang), tính khóa source trong Go (EncodeMany: WEIGHT_STRING của MySQL source theo lô) rồi xóa
// đúng các row thuộc phần chưa đọc (snapshot.InUnread) theo giá trị chính xác. Mỗi row target có đúng một
// khóa source (CrossEngineKeySet điều kiện 1) nên phép thuộc không mơ hồ.
//
// Mỗi trang xóa trong một transaction có fence riêng (bộ nhớ chặn theo trang). Đúng vì thao tác lũy
// đẳng và chạy trong barrier (router dừng, không event nào của bảng được apply): crash giữa chừng để
// plan nguyên như cũ, lần chạy tiếp dọn lại từ đầu. Xóa row ≤ con trỏ không làm trang sau bỏ sót row.
// Chi phí: một lượt quét cả bảng đích, chỉ khi chạy tiếp một plan dở.
func (p *Pipeline) pgClearUnreadExact(ctx context.Context, table model.TableID, m *tableMeta, lo, hi snapshot.Bound, hasRange bool, keys []snapshot.Bound) error {
	reread := make(map[snapshot.Key]bool, len(keys))
	for _, k := range keys {
		reread[k.K] = true
	}
	q := snapshot.PGKeySQL{Table: table, Codec: m.TargetPKCodec}
	return snapshot.ScanPGKeys(ctx, p.dst, q, nil, exactKeyBatch, func(tuples [][]model.Value, _ [][]string) error {
		srcKeys, err := m.PKCodec.EncodeMany(tuples)
		if err != nil {
			return fmt.Errorf("source key of a target row of %s: %w", table, err)
		}
		var del []snapshot.Bound
		for i, k := range srcKeys {
			if snapshot.InUnread(k, lo, hi, hasRange, reread) {
				b, err := m.TargetPKCodec.BoundOf(tuples[i])
				if err != nil {
					return err
				}
				del = append(del, b)
			}
		}
		if len(del) == 0 {
			return nil
		}
		cond, a, _, err := q.InList(del, 1)
		if err != nil {
			return err
		}
		return writer.PGExecFenced(ctx, p.dst, []string{"DELETE FROM " + pgQuoteTable(table) + " WHERE " + cond}, [][]any{a})
	})
}

// pgFlushRange (FlushRange của snapshotter, target PG, việc C): thay range khóa [lo, hi] của bảng đích bằng
// rows — chỉ DELETE khóa của range VẮNG trong rows (không cascade row con của khóa còn lại), upsert phần
// còn lại, trong một transaction.
func (p *Pipeline) pgFlushRange(ctx context.Context, table model.TableID, columnNames []string, rows []*model.RowChange, lo, hi snapshot.Bound) error {
	if src, ok := p.sourceOf(table); ok && p.exactKeys(src) {
		// Phòng thủ: range [lo, hi] theo thứ tự source không có nghĩa ở target của bảng này (pgFlushExact).
		return fmt.Errorf("flush %s: range flush on a target that orders the primary key differently", table)
	}
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
	if m.ExactKeys {
		// Target sắp khóa khác thứ tự source: không xóa theo range ở target (cross_order.go).
		return p.pgClearUnreadExact(ctx, table, m, lo, hi, hasRange, keys)
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
