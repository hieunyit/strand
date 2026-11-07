package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"strand/internal/model"
	pgreader "strand/internal/reader/pg"
)

// Điểm bắt đầu ngẫu nhiên cho verify --mode sample theo tuple PK (verify.SampleKeyRangesEngine): đọc
// một phần bảng tỉ lệ với --rate thay vì keyset walk cả index PK.

// KeySample (PG): tối đa n khóa ngẫu nhiên lấy bằng TABLESAMPLE SYSTEM(pct) — PG chỉ đọc khoảng pct%
// số trang của bảng (không quét toàn bảng như BERNOULLI hay ORDER BY random() trên cả bảng). Trang lấy
// mẫu có thể không chứa row nào (bảng nhỏ, trang rỗng): trả ít hơn n khóa, có thể rỗng.
func (c *PGChunkSource) KeySample(ctx context.Context, table model.TableID, codec *PKCodec, pct float64, n int) ([]Bound, error) {
	if n <= 0 {
		return nil, nil
	}
	types, err := c.columnTypes(ctx, table)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf("SELECT %s FROM %s AS t TABLESAMPLE SYSTEM (%s) ORDER BY random() LIMIT %d",
		c.keyCols(codec), quotePGTable(table), strconv.FormatFloat(pct, 'f', -1, 64), n)
	rows, err := c.DB.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("sample keys of %s: %w", table, err)
	}
	defer rows.Close()
	var out []Bound
	for rows.Next() {
		txt := make([]sql.NullString, len(codec.Cols))
		ptrs := make([]any, len(txt))
		for i := range txt {
			ptrs[i] = &txt[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		vals := make([]model.Value, len(txt))
		for i, col := range codec.Cols {
			if !txt[i].Valid {
				return nil, fmt.Errorf("%s: NULL primary key", table)
			}
			if vals[i], err = pgreader.ParseTextValue(types[col.Name], txt[i].String); err != nil {
				return nil, err
			}
		}
		b, err := codec.BoundOf(vals)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// LeadSeek (MySQL): khóa nhỏ nhất có cột PK đầu tiên ≥ v (v đã bind theo kiểu cột đó) — một lần seek
// trên index PK, không đọc range. ok=false nếu không có khóa nào như vậy.
func (q MySQLKeySQL) LeadSeek(ctx context.Context, db *sql.DB, v any) (Bound, bool, error) {
	k := q.k()
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? ORDER BY %s LIMIT 1",
		k.cols(), quoteTable(q.Table), quoteIdent(k.codec.Cols[0].Name), k.orderBy(false)), v)
	if err != nil {
		return Bound{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return Bound{}, false, rows.Err()
	}
	b, err := k.scanBound(rows)
	return b, err == nil, err
}
