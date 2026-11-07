package verify

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// Verify trực tiếp (sample / delta / sau seed, MySQL→MySQL) cho PK không phải một cột số nguyên (việc C):
// như VerifyRanges nhưng range là cận tuple PK (snapshot.MySQLKeySQL) và row được định danh bằng khóa mã
// hóa; kiểm lại một row nghi lệch bằng điều kiện "(PK) = (giá trị)".

// KeyRange là range [Lo, Hi] theo tuple PK. All: mọi row của bảng (Lo/Hi bỏ qua) — mẫu của bảng source
// rỗng, để bắt row thừa ở target.
type KeyRange struct {
	Lo, Hi snapshot.Bound
	All    bool
}

type keyedRow struct {
	img   string
	bound snapshot.Bound
	label string
}

// readKeyedRows: khóa mã hóa -> ảnh row (cùng mã hóa không mơ hồ như readRows) của câu SELECT q.
func readKeyedRows(ctx context.Context, db *sql.DB, codec *snapshot.PKCodec, pkIdx []int, q string, args ...any) (map[snapshot.Key]keyedRow, error) {
	img, _, err := readRowsAll(ctx, db, q, args...)
	if err != nil {
		return nil, err
	}
	out := make(map[snapshot.Key]keyedRow, len(img))
	for _, r := range img {
		vals := make([]model.Value, len(pkIdx))
		labels := make([]string, len(pkIdx))
		for i, idx := range pkIdx {
			vals[i] = append([]byte{}, r.raw[idx]...)
			labels[i] = string(r.raw[idx])
		}
		b, err := codec.BoundOf(vals)
		if err != nil {
			return nil, err
		}
		out[b.K] = keyedRow{img: r.img, bound: b, label: strings.Join(labels, ",")}
	}
	return out, nil
}

// VerifyKeyRanges so các range tuple của bảng t (PK theo codec) giữa source và target.
func (v *LiveVerifier) VerifyKeyRanges(ctx context.Context, t model.TableID, codec *snapshot.PKCodec, ranges []KeyRange) (*LiveReport, error) {
	rep := &LiveReport{Table: t.String()}
	cols, err := columnList(ctx, v.cfg.Source, t, codec.Cols[0].Name)
	if err != nil {
		return nil, err
	}
	pkIdx := make([]int, len(codec.Cols))
	for j, c := range codec.Cols {
		pkIdx[j] = -1
		for i, n := range cols {
			if n == c.Name {
				pkIdx[j] = i
			}
		}
		if pkIdx[j] < 0 {
			return nil, fmt.Errorf("verify %s: primary key column %s not found", t, c.Name)
		}
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = qi(c)
	}
	list := strings.Join(sel, ", ")
	sq := snapshot.MySQLKeySQL{Table: t, Codec: codec}
	dq := snapshot.MySQLKeySQL{Table: v.target(t), Codec: codec}
	for _, r := range ranges {
		rep.RangesChecked++
		cond := func(q snapshot.MySQLKeySQL) (string, []any, error) {
			if r.All {
				return "1 = 1", nil, nil
			}
			return q.RangeCond(r.Lo, r.Hi)
		}
		w, a, err := cond(sq)
		if err != nil {
			return nil, err
		}
		s, err := readKeyedRows(ctx, v.cfg.Source, codec, pkIdx, fmt.Sprintf("SELECT %s FROM %s WHERE %s", list, qt(t), w), a...)
		if err != nil {
			return nil, err
		}
		w, a, err = cond(dq)
		if err != nil {
			return nil, err
		}
		d, err := readKeyedRows(ctx, v.cfg.Target, codec, pkIdx, fmt.Sprintf("SELECT %s FROM %s WHERE %s", list, qt(v.target(t)), w), a...)
		if err != nil {
			return nil, err
		}
		rep.RowsChecked += len(s)
		suspects := map[snapshot.Key]keyedRow{}
		for k, sv := range s {
			if dv, ok := d[k]; !ok || dv.img != sv.img {
				suspects[k] = sv
			}
		}
		for k, dv := range d {
			if _, ok := s[k]; !ok {
				suspects[k] = dv
			}
		}
		for k, row := range suspects {
			real, hot, err := v.recheckKey(ctx, t, codec, pkIdx, list, k, row.bound)
			if err != nil {
				return nil, err
			}
			switch {
			case real:
				rep.Mismatches = append(rep.Mismatches, row.label)
				rep.MismatchKeys = append(rep.MismatchKeys, row.bound)
			case hot:
				rep.UnverifiableHot = append(rep.UnverifiableHot, row.label)
			}
		}
	}
	return rep, nil
}

func (v *LiveVerifier) recheckKey(ctx context.Context, t model.TableID, codec *snapshot.PKCodec, pkIdx []int, list string, k snapshot.Key, b snapshot.Bound) (real, hot bool, err error) {
	sw, sa, err := snapshot.MySQLKeySQL{Table: t, Codec: codec}.Cmp("=", b)
	if err != nil {
		return false, false, err
	}
	dw, da, err := snapshot.MySQLKeySQL{Table: v.target(t), Codec: codec}.Cmp("=", b)
	if err != nil {
		return false, false, err
	}
	srcQ := fmt.Sprintf("SELECT %s FROM %s WHERE %s", list, qt(t), sw)
	dstQ := fmt.Sprintf("SELECT %s FROM %s WHERE %s", list, qt(v.target(t)), dw)
	w := v.waiter()
	for attempt := 0; attempt < v.cfg.Retries; attempt++ {
		s1, err := readKeyedRows(ctx, v.cfg.Source, codec, pkIdx, srcQ, sa...)
		if err != nil {
			return false, false, err
		}
		if err := waitMark(ctx, w); err != nil {
			return false, false, err
		}
		d, err := readKeyedRows(ctx, v.cfg.Target, codec, pkIdx, dstQ, da...)
		if err != nil {
			return false, false, err
		}
		s2, err := readKeyedRows(ctx, v.cfg.Source, codec, pkIdx, srcQ, sa...)
		if err != nil {
			return false, false, err
		}
		switch classifyObs(keyedObs(s1, d, s2, k)) {
		case verdictUnstable:
			continue // row đổi trong cửa sổ: thử lại
		case verdictMismatch:
			return true, false, nil
		}
		return false, false, nil
	}
	return false, true, nil
}

func keyedObs(s1, d, s2 map[snapshot.Key]keyedRow, k snapshot.Key) rowObs {
	var o rowObs
	var r keyedRow
	r, o.s1ok = s1[k]
	o.s1 = r.img
	r, o.dok = d[k]
	o.d = r.img
	r, o.s2ok = s2[k]
	o.s2 = r.img
	return o
}

// SampleKeyRanges: keyset walk trên index PK theo khối `rows` khóa, giữ mỗi khối với xác suất rate (ít
// nhất một khối). Khóa tổng quát không chọn ngẫu nhiên được trong không gian giá trị như khóa số, nên mẫu
// tốn một lượt đọc index PK (không đọc row) của bảng.
func SampleKeyRanges(ctx context.Context, db *sql.DB, t model.TableID, codec *snapshot.PKCodec, rate float64, rows int64, rng *rand.Rand) ([]KeyRange, error) {
	if rows <= 0 {
		rows = 1000
	}
	q := snapshot.MySQLKeySQL{Table: t, Codec: codec}
	mn, mx, ok, err := q.Bounds(ctx, db)
	if err != nil || !ok {
		return nil, err
	}
	var all, out []KeyRange
	lo := mn
	for {
		e, err := q.KeysetEnd(ctx, db, lo, mx, rows)
		if err != nil {
			return nil, err
		}
		r := KeyRange{Lo: lo, Hi: e}
		all = append(all, r)
		if rng.Float64() < rate {
			out = append(out, r)
		}
		if e.K >= mx.K {
			break
		}
		next, ok := snapshot.After(codec, e)
		if !ok {
			break
		}
		lo = next
	}
	if len(out) == 0 && len(all) > 0 {
		out = append(out, all[rng.Intn(len(all))])
	}
	return out, nil
}

// PointRanges: mỗi khóa thành một range [k, k].
func PointRanges(keys []snapshot.Bound) []KeyRange {
	out := make([]KeyRange, len(keys))
	for i, k := range keys {
		out[i] = KeyRange{Lo: k, Hi: k}
	}
	return out
}
