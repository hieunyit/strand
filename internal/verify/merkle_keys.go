package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// Full verify cho bảng có PK không phải một cột số nguyên (việc C). Cùng thuật toán với VerifyTableFull:
// chia khóa nguồn thành chunk bằng keyset walk, hash từng chunk ở hai phía (count + tổng MD5 row chính
// tắc, không phụ thuộc thứ tự), Merkle, rồi so từng row ở chunk lệch. Khác: chunk là điều kiện TUPLE PK
// (snapshot.MySQLKeySQL / PGKeySQL) dựng bằng CÙNG giá trị biên ở hai phía, với tham số ép đúng kiểu cột
// từng phía — đúng khi hai phía có cùng thứ tự khóa, điều kiện mà caller kiểm trước (PKCodec.SameOrder,
// hoặc lớp thứ tự độc lập engine cho MySQL→PG). Hai chunk mở "< min" và "> max" bắt row thừa ở target.
// Chưa có: --resume (tiến độ lưu biên BIGINT) và so bằng LiveVerifier.

// keyChunk là một lá của Merkle theo khóa: below (< Lo), above (> Hi), all (mọi row), hoặc range [Lo, Hi].
type keyChunk struct {
	id           int
	lo, hi       snapshot.Bound
	below, above bool
	all          bool
}

// keySide là một phía (source/target) của full verify theo khóa.
type keySide struct {
	engine model.Engine
	db     *sql.DB
	table  model.TableID
	codec  *snapshot.PKCodec
}

func (s keySide) cond(c keyChunk) (string, []any, error) {
	if c.all {
		return "1 = 1", nil, nil
	}
	if s.engine == model.EnginePG {
		q := snapshot.PGKeySQL{Table: s.table, Codec: s.codec}
		switch {
		case c.below:
			w, a, _, err := q.Cmp("<", c.lo, 1)
			return w, a, err
		case c.above:
			w, a, _, err := q.Cmp(">", c.hi, 1)
			return w, a, err
		}
		w, a, _, err := q.RangeCond(c.lo, c.hi, 1)
		return w, a, err
	}
	q := snapshot.MySQLKeySQL{Table: s.table, Codec: s.codec}
	switch {
	case c.below:
		return q.Cmp("<", c.lo)
	case c.above:
		return q.Cmp(">", c.hi)
	}
	return q.RangeCond(c.lo, c.hi)
}

func (s keySide) hash(ctx context.Context, c keyChunk, cols []ColumnInfo) (string, error) {
	w, args, err := s.cond(c)
	if err != nil {
		return "", err
	}
	row := rowExpression(s.engine, cols)
	var q string
	if s.engine == model.EnginePG {
		q = fmt.Sprintf("SELECT count(*)::text, COALESCE(sum(('x' || substr(md5(%s), 1, 15))::bit(60)::bigint), 0)::text FROM %s WHERE %s",
			row, QuoteTable(s.engine, s.table), w)
	} else {
		q = fmt.Sprintf("SELECT CAST(COUNT(*) AS CHAR), CAST(COALESCE(SUM(CAST(CONV(SUBSTRING(MD5(%s), 1, 15), 16, 10) AS UNSIGNED)), 0) AS CHAR) FROM %s WHERE %s",
			row, QuoteTable(s.engine, s.table), w)
	}
	var cnt, sum string
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&cnt, &sum); err != nil {
		return "", fmt.Errorf("hash chunk %d of %s: %w", c.id, s.table, err)
	}
	return cnt + ":" + sum, nil
}

// rows: khóa (các cột PK chính tắc) -> row chính tắc của chunk; bounds: cùng khóa -> cận khóa từ giá trị
// gốc (chỉ phía MySQL; để repair).
func (s keySide) rows(ctx context.Context, c keyChunk, cols []ColumnInfo, pkCols []ColumnInfo) (map[string]string, error) {
	out, _, err := s.rowsBounds(ctx, c, cols, pkCols)
	return out, err
}

func (s keySide) rowsBounds(ctx context.Context, c keyChunk, cols []ColumnInfo, pkCols []ColumnInfo) (map[string]string, map[string]snapshot.Bound, error) {
	w, args, err := s.cond(c)
	if err != nil {
		return nil, nil, err
	}
	raw := s.engine == model.EngineMySQL
	sel := make([]string, 0, 2*len(pkCols))
	for _, pc := range pkCols {
		sel = append(sel, canonicalValueExpr(s.engine, pc))
	}
	if raw {
		for _, pc := range pkCols {
			sel = append(sel, QuoteIdent(s.engine, pc.Name))
		}
	}
	q := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s", strings.Join(sel, ", "), rowExpression(s.engine, cols), QuoteTable(s.engine, s.table), w)
	rs, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("read rows of %s: %w", s.table, err)
	}
	defer rs.Close()
	out := map[string]string{}
	bounds := map[string]snapshot.Bound{}
	for rs.Next() {
		parts := make([]sql.RawBytes, len(sel)+1)
		ptrs := make([]any, len(parts))
		for i := range parts {
			ptrs[i] = &parts[i]
		}
		if err := rs.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		// Khóa map phân định từng phần (review F6): nối thẳng bằng "," làm ("x,y","z") trùng ("x","y,z"),
		// một row bị đè và có thể không được so. Dạng "len:value" không trùng giữa hai tuple khác nhau.
		keys := make([]string, len(pkCols))
		for i := range pkCols {
			keys[i] = fmt.Sprintf("%d:%s", len(parts[i]), parts[i])
		}
		k := strings.Join(keys, ",")
		out[k] = string(parts[len(parts)-1])
		if raw {
			vals := make([]model.Value, len(pkCols))
			for i := range pkCols {
				vals[i] = append([]byte{}, parts[len(pkCols)+i]...)
			}
			if b, err := s.codec.BoundOf(vals); err == nil {
				bounds[k] = b
			}
		}
	}
	return out, bounds, rs.Err()
}

// VerifyTableFullKeys: full verify của bảng t có PK tổng quát. srcCodec/dstCodec: codec PK hai phía (cùng
// cột, cùng thứ tự — caller đã kiểm).
func (mv *MerkleVerifier) VerifyTableFullKeys(ctx context.Context, t model.TableID, srcCodec, dstCodec *snapshot.PKCodec) (*FullVerifyReport, error) {
	if mv.cfg.Resume {
		return nil, fmt.Errorf("table %s: --resume is not supported for a primary key that is not a single integer column", t)
	}
	srcDB := mv.srcDB()
	src := keySide{engine: mv.cfg.SourceEngine, db: srcDB, table: t, codec: srcCodec}
	dst := keySide{engine: mv.cfg.TargetEngine, db: mv.cfg.Target, table: mv.targetTable(t), codec: dstCodec}
	pkNames := make([]string, len(srcCodec.Cols))
	for i, c := range srcCodec.Cols {
		pkNames[i] = c.Name
	}
	srcCols, err := GetTableColumns(ctx, srcDB, src.engine, t, pkNames[0])
	if err != nil {
		return nil, err
	}
	allDst, err := GetTableColumns(ctx, dst.db, dst.engine, dst.table, pkNames[0])
	if err != nil {
		return nil, err
	}
	dstCols, err := matchTargetColumns(srcCols, allDst, dst.table)
	if err != nil {
		return nil, err
	}
	pick := func(cols []ColumnInfo) []ColumnInfo {
		out := make([]ColumnInfo, 0, len(pkNames))
		for _, n := range pkNames {
			for _, c := range cols {
				if c.Name == n {
					out = append(out, c)
				}
			}
		}
		return out
	}
	srcPK, dstPK := pick(srcCols), pick(dstCols)
	if len(srcPK) != len(pkNames) || len(dstPK) != len(pkNames) {
		return nil, fmt.Errorf("table %s: primary key columns not found on both sides", t)
	}

	chunks, err := mv.keyChunks(ctx, src)
	if err != nil {
		return nil, err
	}
	rep := &FullVerifyReport{Table: t.String(), TotalChunks: len(chunks)}
	var tick <-chan time.Time
	if mv.cfg.Rate > 0 {
		tk := time.NewTicker(time.Duration(float64(time.Second) / mv.cfg.Rate))
		defer tk.Stop()
		tick = tk.C
	}
	var bad []keyChunk
	for _, c := range chunks {
		if mv.cfg.StopAfter > 0 && rep.HashedChunks >= mv.cfg.StopAfter {
			return rep, fmt.Errorf("full verify of %s stopped after %d chunks: %w", t, rep.HashedChunks, ErrVerifyInterrupted)
		}
		if tick != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-tick:
			}
		}
		sh, err := src.hash(ctx, c, srcCols)
		if err != nil {
			return nil, fmt.Errorf("source: %w", err)
		}
		th, err := dst.hash(ctx, c, dstCols)
		if err != nil {
			return nil, fmt.Errorf("target: %w", err)
		}
		rep.HashedChunks++
		if sh != th {
			bad = append(bad, c)
		}
	}
	rep.MismatchedChunks = len(bad)
	for _, c := range bad {
		s, sb, err := src.rowsBounds(ctx, c, srcCols, srcPK)
		if err != nil {
			return nil, fmt.Errorf("source: %w", err)
		}
		d, db, err := dst.rowsBounds(ctx, c, dstCols, dstPK)
		if err != nil {
			return nil, fmt.Errorf("target: %w", err)
		}
		rep.RangesChecked++
		rep.RowsChecked += len(s)
		for k, sv := range s {
			if dv, ok := d[k]; !ok || dv != sv {
				rep.Mismatches = append(rep.Mismatches, k)
				if b, ok := sb[k]; ok {
					rep.MismatchKeys = append(rep.MismatchKeys, b)
				}
			}
		}
		for k := range d {
			if _, ok := s[k]; !ok {
				rep.Mismatches = append(rep.Mismatches, k)
				if b, ok := db[k]; ok {
					rep.MismatchKeys = append(rep.MismatchKeys, b)
				}
			}
		}
	}
	return rep, nil
}

// keyChunks: "< min", các range keyset của nguồn, "> max" (bảng rỗng: một chunk mọi row).
func (mv *MerkleVerifier) keyChunks(ctx context.Context, src keySide) ([]keyChunk, error) {
	size := int64(mv.cfg.ChunkRows)
	if size <= 0 {
		size = DefaultChunkRows
	}
	var bounds func() (snapshot.Bound, snapshot.Bound, bool, error)
	var end func(lo, max snapshot.Bound) (snapshot.Bound, error)
	if src.engine == model.EnginePG {
		cs := &snapshot.PGChunkSource{DB: src.db}
		bounds = func() (snapshot.Bound, snapshot.Bound, bool, error) { return cs.KeyBounds(ctx, src.table, src.codec) }
		end = func(lo, max snapshot.Bound) (snapshot.Bound, error) {
			return cs.KeyKeysetEnd(ctx, src.table, src.codec, lo, max, size)
		}
	} else {
		q := snapshot.MySQLKeySQL{Table: src.table, Codec: src.codec}
		bounds = func() (snapshot.Bound, snapshot.Bound, bool, error) { return q.Bounds(ctx, src.db) }
		end = func(lo, max snapshot.Bound) (snapshot.Bound, error) { return q.KeysetEnd(ctx, src.db, lo, max, size) }
	}
	mn, mx, ok, err := bounds()
	if err != nil {
		return nil, fmt.Errorf("bounds of %s: %w", src.table, err)
	}
	if !ok {
		return []keyChunk{{id: 0, all: true}}, nil
	}
	chunks := []keyChunk{{id: 0, below: true, lo: mn}}
	lo := mn
	for {
		e, err := end(lo, mx)
		if err != nil {
			return nil, fmt.Errorf("keyset walk on %s: %w", src.table, err)
		}
		chunks = append(chunks, keyChunk{id: len(chunks), lo: lo, hi: e})
		if e.K >= mx.K {
			break
		}
		next, ok := snapshot.After(src.codec, e)
		if !ok {
			break
		}
		lo = next
	}
	return append(chunks, keyChunk{id: len(chunks), above: true, hi: mx}), nil
}
