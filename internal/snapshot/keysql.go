package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"strand/internal/model"
)

// keySQL dựng phần SQL theo tuple PK cho keyset walk, SELECT chunk và xóa range (việc C). Mọi so sánh
// dùng row constructor "(a, b) op (?, ?)" — thứ tự từ điển theo cột như ORDER BY a, b — và tham số
// được bind theo đúng kiểu cột (bindValue): so cột số/DECIMAL với tham số chuỗi bị MySQL ép về double
// (mất chính xác > 2^53 / với DECIMAL dài), so VARCHAR PAD SPACE với tham số nhị phân thì bỏ qua
// collation — cả hai làm lệch thứ tự Go ↔ DB.
type keySQL struct {
	table model.TableID
	codec *PKCodec
}

func newKeySQL(table model.TableID, codec *PKCodec) keySQL {
	return keySQL{table: table, codec: codecOrInt(codec)}
}

func (q keySQL) cols() string {
	out := make([]string, len(q.codec.Cols))
	for i, c := range q.codec.Cols {
		out[i] = quoteIdent(c.Name)
	}
	return strings.Join(out, ", ")
}

func (q keySQL) tuple(s string) string {
	if len(q.codec.Cols) == 1 {
		return s
	}
	return "(" + s + ")"
}

// orderBy: danh sách cột PK cho ORDER BY (tăng dần, hoặc giảm dần nếu desc).
func (q keySQL) orderBy(desc bool) string {
	out := make([]string, len(q.codec.Cols))
	for i, c := range q.codec.Cols {
		out[i] = quoteIdent(c.Name)
		if desc {
			out[i] += " DESC"
		}
	}
	return strings.Join(out, ", ")
}

// cmp: "(a, b) op (?, ?)" với tham số đã bind theo kiểu.
func (q keySQL) cmp(op string, b Bound) (string, []any, error) {
	if len(b.V) != len(q.codec.Cols) {
		return "", nil, fmt.Errorf("key bound of %s has %d values, want %d", q.table, len(b.V), len(q.codec.Cols))
	}
	phs := make([]string, len(b.V))
	args := make([]any, len(b.V))
	for i, col := range q.codec.Cols {
		v, err := bindValue(col, b.V[i])
		if err != nil {
			return "", nil, fmt.Errorf("key column %s: %w", col.Name, err)
		}
		args[i] = v
		phs[i] = "?"
		if col.SQLType != "" {
			phs[i] = "CAST(? AS " + col.SQLType + ")"
		}
	}
	return fmt.Sprintf("%s %s %s", q.tuple(q.cols()), op, q.tuple(strings.Join(phs, ", "))), args, nil
}

// rangeCond: điều kiện của range [lo, hi] (lo có thể loại trừ).
func (q keySQL) rangeCond(lo, hi Bound) (string, []any, error) {
	op := ">="
	if lo.Excl {
		op = ">"
	}
	l, la, err := q.cmp(op, lo)
	if err != nil {
		return "", nil, err
	}
	h, ha, err := q.cmp("<=", hi)
	if err != nil {
		return "", nil, err
	}
	return l + " AND " + h, append(la, ha...), nil
}

// bindValue chuyển giá trị khóa (dạng bất kỳ đã biết) về dạng Go bind đúng kiểu cột MySQL.
func bindValue(col PKColumn, v model.Value) (any, error) {
	switch col.Kind {
	case PKInt:
		return toInt64(v)
	case PKUint:
		return toUint64(v)
	case PKBytes:
		// VARBINARY/BINARY và chuỗi NO PAD nhị phân: so byte — bind []byte cho VARBINARY, string cho chuỗi
		// (cùng byte; string giữ collation của cột).
		if b, ok := v.([]byte); ok {
			if b == nil {
				b = []byte{} // nil bind thành NULL
			}
			return b, nil
		}
		return toText(v)
	case PKPadBytes, PKDecimal, PKTime, PKTrimBytes, PKUUID, PKWeight:
		return toText(v)
	case PKDateTime:
		if t, ok := v.(time.Time); ok {
			return t.UTC().Format("2006-01-02 15:04:05.999999"), nil
		}
		return toText(v)
	}
	return nil, fmt.Errorf("unknown key kind %v", col.Kind)
}

// scanBound đọc các cột PK (đã SELECT theo thứ tự codec) của row hiện tại thành Bound.
func (q keySQL) scanBound(rows *sql.Rows) (Bound, error) {
	raw := make([]sql.RawBytes, len(q.codec.Cols))
	ptrs := make([]any, len(raw))
	for i := range raw {
		ptrs[i] = &raw[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return Bound{}, err
	}
	vals := make([]model.Value, len(raw))
	for i, b := range raw {
		if b == nil {
			return Bound{}, fmt.Errorf("NULL in primary key of %s", q.table)
		}
		vals[i] = append([]byte{}, b...) // không nil: []byte(nil) bind thành NULL (khóa rỗng '' bị mất)
	}
	return q.bound(vals)
}

// bound mã hóa vals thành Bound; giá trị giữ dạng bind được (bindValue).
func (q keySQL) bound(vals []model.Value) (Bound, error) {
	k, err := q.codec.Encode(vals)
	if err != nil {
		return Bound{}, fmt.Errorf("%s: %w", q.table, err)
	}
	if q.codec.IsSingleInt() {
		v, _ := toInt64(vals[0])
		return IntBound(v), nil
	}
	norm := make([]model.Value, len(vals))
	for i, col := range q.codec.Cols {
		if norm[i], err = bindValue(col, vals[i]); err != nil {
			return Bound{}, err
		}
	}
	return Bound{K: k, V: norm}, nil
}

// mysqlKeyBounds trả khóa nhỏ nhất và lớn nhất của bảng (ok=false nếu bảng rỗng). ORDER BY … LIMIT 1
// đọc đầu/cuối index PK, O(1) như MIN/MAX.
func (q keySQL) mysqlKeyBounds(ctx context.Context, db *sql.DB) (lo, hi Bound, ok bool, err error) {
	one := func(desc bool) (Bound, bool, error) {
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT 1", q.cols(), quoteTable(q.table), q.orderBy(desc)))
		if err != nil {
			return Bound{}, false, fmt.Errorf("failed to query table bounds: %w", err)
		}
		defer rows.Close()
		if !rows.Next() {
			return Bound{}, false, rows.Err()
		}
		b, err := q.scanBound(rows)
		return b, err == nil, err
	}
	if lo, ok, err = one(false); err != nil || !ok {
		return
	}
	hi, ok, err = one(true)
	return
}

// mysqlKeysetEnd trả khóa cuối của chunk bắt đầu ở lo: khóa thứ `rows` trong [lo, max], hoặc max nếu
// không còn đủ khóa.
func (q keySQL) mysqlKeysetEnd(ctx context.Context, db *sql.DB, lo, max Bound, n int64) (Bound, error) {
	if n <= 0 {
		n = 5000
	}
	cond, args, err := q.rangeCond(lo, max)
	if err != nil {
		return Bound{}, err
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1 OFFSET %d",
		q.cols(), quoteTable(q.table), cond, q.orderBy(false), n-1), args...)
	if err != nil {
		return Bound{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return max, rows.Err()
	}
	return q.scanBound(rows)
}

// MySQLKeySQL: phần SQL theo tuple PK cho MySQL dùng ngoài package (verify full, việc C).
type MySQLKeySQL struct {
	Table model.TableID
	Codec *PKCodec
}

func (q MySQLKeySQL) k() keySQL { return newKeySQL(q.Table, q.Codec) }

// Cmp: "(cột PK) op (tham số của b)".
func (q MySQLKeySQL) Cmp(op string, b Bound) (string, []any, error) { return q.k().cmp(op, b) }

// RangeCond: điều kiện range [lo, hi].
func (q MySQLKeySQL) RangeCond(lo, hi Bound) (string, []any, error) { return q.k().rangeCond(lo, hi) }

// Bounds: khóa nhỏ nhất / lớn nhất của bảng.
func (q MySQLKeySQL) Bounds(ctx context.Context, db *sql.DB) (Bound, Bound, bool, error) {
	return q.k().mysqlKeyBounds(ctx, db)
}

// KeysetEnd: khóa thứ n trong [lo, max].
func (q MySQLKeySQL) KeysetEnd(ctx context.Context, db *sql.DB, lo, max Bound, n int64) (Bound, error) {
	return q.k().mysqlKeysetEnd(ctx, db, lo, max, n)
}

// After trả cận dưới loại trừ ngay sau b (ok=false nếu không còn khóa nào sau b).
func After(codec *PKCodec, b Bound) (Bound, bool) { return after(codecOrInt(codec), b) }
