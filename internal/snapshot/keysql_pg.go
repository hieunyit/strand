package snapshot

import (
	"fmt"
	"strings"

	"strand/internal/model"
)

// PGKeySQL dựng điều kiện theo tuple PK cho PostgreSQL (việc C): row constructor "(t.a, t.b) op ($1::kiểu,
// $2::kiểu)" — so từ điển theo cột như ORDER BY t.a, t.b — với tham số ép về đúng kiểu cột (SQLType =
// format_type của cột) để PG không suy kiểu khác (vd. so text với numeric). Collation của cột "C" thắng
// collation mặc định của tham số. Alias (nếu có) đặt trước tên cột: SELECT `col::text … ORDER BY col` trỏ
// vào cột kết quả kiểu text — phải sắp theo cột của BẢNG (xem 9f614ab).
type PGKeySQL struct {
	Table model.TableID
	Codec *PKCodec
	Alias string
}

func (q PGKeySQL) col(c PKColumn) string {
	if q.Alias != "" {
		return q.Alias + "." + quotePGIdent(c.Name)
	}
	return quotePGIdent(c.Name)
}

// Cols: danh sách cột PK (có alias).
func (q PGKeySQL) Cols() []string {
	out := make([]string, len(q.Codec.Cols))
	for i, c := range q.Codec.Cols {
		out[i] = q.col(c)
	}
	return out
}

// OrderBy: ORDER BY theo các cột PK (của bảng).
func (q PGKeySQL) OrderBy(desc bool) string {
	cols := q.Cols()
	if desc {
		for i := range cols {
			cols[i] += " DESC"
		}
	}
	return strings.Join(cols, ", ")
}

func (q PGKeySQL) tuple(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// params: "$n::kiểu" cho từng giá trị của b, bắt đầu từ $arg; trả tham số đã bind và chỉ số kế tiếp.
func (q PGKeySQL) params(b Bound, arg int) (string, []any, int, error) {
	if len(b.V) != len(q.Codec.Cols) {
		return "", nil, arg, fmt.Errorf("key bound of %s has %d values, want %d", q.Table, len(b.V), len(q.Codec.Cols))
	}
	phs := make([]string, len(b.V))
	args := make([]any, len(b.V))
	for i, c := range q.Codec.Cols {
		v, err := bindValue(c, b.V[i])
		if err != nil {
			return "", nil, arg, fmt.Errorf("key column %s: %w", c.Name, err)
		}
		args[i] = v
		phs[i] = fmt.Sprintf("$%d", arg)
		if c.SQLType != "" {
			phs[i] += "::" + c.SQLType
		}
		arg++
	}
	return q.tuple(phs), args, arg, nil
}

// Cmp: "(cột PK) op (tham số của b)".
func (q PGKeySQL) Cmp(op string, b Bound, arg int) (string, []any, int, error) {
	p, args, next, err := q.params(b, arg)
	if err != nil {
		return "", nil, arg, err
	}
	return q.tuple(q.Cols()) + " " + op + " " + p, args, next, nil
}

// RangeCond: điều kiện range [lo, hi] (lo có thể loại trừ).
func (q PGKeySQL) RangeCond(lo, hi Bound, arg int) (string, []any, int, error) {
	op := ">="
	if lo.Excl {
		op = ">"
	}
	l, la, next, err := q.Cmp(op, lo, arg)
	if err != nil {
		return "", nil, arg, err
	}
	h, ha, next, err := q.Cmp("<=", hi, next)
	if err != nil {
		return "", nil, arg, err
	}
	return l + " AND " + h, append(la, ha...), next, nil
}

// InList: "(cột PK) IN ((…), (…))" cho tập khóa keys (rỗng: FALSE).
func (q PGKeySQL) InList(keys []Bound, arg int) (string, []any, int, error) {
	if len(keys) == 0 {
		return "FALSE", nil, arg, nil
	}
	var parts []string
	var args []any
	for _, k := range keys {
		p, a, next, err := q.params(k, arg)
		if err != nil {
			return "", nil, arg, err
		}
		if len(q.Codec.Cols) == 1 {
			p = "(" + p + ")"
		}
		parts, args, arg = append(parts, p), append(args, a...), next
	}
	return q.tuple(q.Cols()) + " IN (" + strings.Join(parts, ", ") + ")", args, arg, nil
}

// BoundOf mã hóa giá trị khóa vals thành Bound (giá trị giữ dạng bind được).
func (c *PKCodec) BoundOf(vals []model.Value) (Bound, error) {
	return newKeySQL(model.TableID{}, c).bound(vals)
}
