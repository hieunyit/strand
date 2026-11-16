package rowkey

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// PGIdxRow là một dòng (index unique, cột khóa) của pg_index.
type PGIdxRow struct {
	Index     string
	Primary   bool
	Replident bool // indisreplident
	Immediate bool // indimmediate (false = DEFERRABLE)
	Valid     bool
	Partial   bool
	HasExpr   bool
	Column    sql.NullString // NULL: cột biểu thức
	NotNull   bool
	// RelReplIdent: relreplident của bảng: d (default), n (nothing), f (full), i (index).
	RelReplIdent string
}

// pgKeyRowsSQL: mọi unique index (gồm PRIMARY), chỉ cột khóa (không INCLUDE), theo thứ tự cột trong index.
const pgKeyRowsSQL = `
	SELECT ic.relname, i.indisprimary, i.indisreplident, i.indimmediate, i.indisvalid,
	       i.indpred IS NOT NULL, i.indexprs IS NOT NULL, a.attname, COALESCE(a.attnotnull, false), c.relreplident::text
	  FROM pg_class c
	  JOIN pg_index i ON i.indrelid = c.oid
	  JOIN pg_class ic ON ic.oid = i.indexrelid
	  JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) ON true
	  LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum AND k.attnum > 0
	 WHERE c.oid = $1 AND i.indisunique AND k.ord <= i.indnkeyatts
	 ORDER BY ic.relname, k.ord`

// PGInfo: PRIMARY KEY, REPLICA IDENTITY và unique index của một bảng PostgreSQL.
type PGInfo struct {
	PK           []string
	RelReplIdent string // d, n, f, i ("" nếu bảng không có unique index nào: xem LoadPG)
	ReplIndex    string // tên index của REPLICA IDENTITY USING INDEX
	Candidates   []Candidate
}

// BuildPG gom các dòng (đã sắp theo tên index, thứ tự cột) thành PGInfo.
func BuildPG(rows []PGIdxRow) *PGInfo {
	info := &PGInfo{}
	var order []string
	byName := map[string]*Candidate{}
	for _, r := range rows {
		info.RelReplIdent = r.RelReplIdent
		if r.Primary {
			info.PK = append(info.PK, r.Column.String)
			continue
		}
		if r.Replident {
			info.ReplIndex = r.Index
		}
		c := byName[r.Index]
		if c == nil {
			c = &Candidate{Name: r.Index}
			byName[r.Index] = c
			order = append(order, r.Index)
		}
		col := r.Column.String
		if !r.Column.Valid {
			col = "<expression>"
		}
		c.Columns = append(c.Columns, col)
		why := ""
		switch {
		case !r.Valid:
			why = "index is not valid"
		case r.Partial:
			why = "partial index"
		case r.HasExpr || !r.Column.Valid:
			why = "expression index"
		case !r.Immediate:
			why = "deferrable constraint"
		case !r.NotNull:
			why = fmt.Sprintf("column %s is nullable", col)
		}
		if why != "" && c.Reason == "" {
			c.Reason = why
		}
	}
	for _, n := range order {
		info.Candidates = append(info.Candidates, *byName[n])
	}
	return info
}

// LoadPG đọc catalog của bảng có OID oid. relreplident luôn được điền (kể cả khi bảng không có unique index).
func LoadPG(ctx context.Context, db *sql.DB, oid int64) (*PGInfo, error) {
	rs, err := db.QueryContext(ctx, pgKeyRowsSQL, oid)
	if err != nil {
		return nil, fmt.Errorf("read unique keys: %w", err)
	}
	var rows []PGIdxRow
	for rs.Next() {
		var r PGIdxRow
		if err := rs.Scan(&r.Index, &r.Primary, &r.Replident, &r.Immediate, &r.Valid, &r.Partial, &r.HasExpr, &r.Column, &r.NotNull, &r.RelReplIdent); err != nil {
			rs.Close()
			return nil, err
		}
		rows = append(rows, r)
	}
	rs.Close()
	if err := rs.Err(); err != nil {
		return nil, err
	}
	info := BuildPG(rows)
	if info.RelReplIdent == "" {
		if err := db.QueryRowContext(ctx, `SELECT relreplident::text FROM pg_class WHERE oid = $1`, oid).Scan(&info.RelReplIdent); err != nil {
			return nil, fmt.Errorf("read replica identity: %w", err)
		}
	}
	return info, nil
}

// PGResult: kết quả chọn khóa của bảng PG.
type PGResult struct {
	Info *PGInfo
	// Key nil: không có khóa dùng được. Có PK → PRIMARY; không PK thì chỉ index của REPLICA IDENTITY USING INDEX
	// (pgoutput chỉ gửi các cột đó cho UPDATE/DELETE).
	Key *Key
	// Suggested: unique index đủ điều kiện nên đặt làm REPLICA IDENTITY (khi Key nil); rỗng nếu không có.
	Suggested string
	Rejected  []Candidate
}

// Resolve chọn khóa; typeOK (có thể nil) chỉ dùng để chọn Suggested.
func (i *PGInfo) Resolve(typeOK func(cols []string) error) *PGResult {
	res := &PGResult{Info: i}
	if len(i.PK) > 0 {
		res.Key = &Key{Name: PrimaryName, Columns: append([]string(nil), i.PK...)}
		return res
	}
	if i.RelReplIdent == "i" && i.ReplIndex != "" {
		for _, c := range i.Candidates {
			if c.Name == i.ReplIndex && c.Eligible() {
				res.Key = &Key{Name: c.Name, Columns: append([]string(nil), c.Columns...)}
				return res
			}
		}
	}
	best, rej := Choose(i.Candidates, typeOK)
	res.Rejected = rej
	if best != nil {
		res.Suggested = best.Name
	}
	return res
}

// ResolvePG = LoadPG + Resolve với bộ mã hóa khóa của snapshot làm typeOK.
func ResolvePG(ctx context.Context, db *sql.DB, id model.TableID, oid int64) (*PGResult, error) {
	info, err := LoadPG(ctx, db, oid)
	if err != nil {
		return nil, err
	}
	return info.Resolve(func(cols []string) error {
		_, err := snapshot.PGPKCodec(ctx, db, id, cols)
		return err
	}), nil
}

// PGReplIdentAdvice: mô tả replica identity hiện tại cho thông báo lỗi.
func PGReplIdentAdvice(ri string) string {
	switch ri {
	case "d":
		return "DEFAULT (primary key only)"
	case "n":
		return "NOTHING"
	case "f":
		return "FULL"
	case "i":
		return "USING INDEX"
	}
	return strings.ToUpper(ri)
}
