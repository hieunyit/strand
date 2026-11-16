package rowkey

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/model"
	"strand/internal/snapshot"
)

// MySQLIdxRow là một dòng information_schema.STATISTICS (unique index) kèm thuộc tính cột từ COLUMNS.
type MySQLIdxRow struct {
	Index    string
	Column   sql.NullString // NULL: index biểu thức (functional index, MySQL 8.0.13+)
	SubPart  sql.NullInt64  // khác NULL: cột prefix, ví dụ code(10)
	Nullable string         // COLUMNS.IS_NULLABLE: "YES"/"NO"; rỗng nếu không thấy cột (vd. cột ẩn của index biểu thức)
	Extra    string         // COLUMNS.EXTRA
}

// mysqlKeyRowsSQL: mọi unique index (gồm PRIMARY) theo thứ tự cột trong index. Cột INVISIBLE (MySQL 8.0.23+,
// MariaDB 10.3+) vẫn nằm trong COLUMNS/STATISTICS nên được tính như cột thường.
const mysqlKeyRowsSQL = `
	SELECT s.INDEX_NAME, s.COLUMN_NAME, s.SUB_PART, COALESCE(c.IS_NULLABLE, ''), COALESCE(c.EXTRA, '')
	  FROM information_schema.STATISTICS s
	  LEFT JOIN information_schema.COLUMNS c
	    ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME
	 WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.NON_UNIQUE = 0
	 ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX`

// MySQLInfo: PRIMARY KEY và các unique index của một bảng MySQL/MariaDB.
type MySQLInfo struct {
	PK         []string
	PKPrefix   bool        // PRIMARY KEY có cột prefix
	Candidates []Candidate // unique index khác PRIMARY (kèm lý do nếu không đủ điều kiện)
}

// generatedExtra: COLUMNS.EXTRA của cột sinh (MySQL "VIRTUAL GENERATED"/"STORED GENERATED"; MariaDB còn
// "PERSISTENT"). "DEFAULT_GENERATED" (default biểu thức, MySQL 8) KHÔNG phải cột sinh.
func generatedExtra(extra string) bool {
	e := strings.ToUpper(extra)
	return strings.Contains(e, "VIRTUAL") || strings.Contains(e, "STORED GENERATED") || strings.Contains(e, "PERSISTENT")
}

// BuildMySQL gom các dòng (đã sắp theo INDEX_NAME, SEQ_IN_INDEX) thành PK và danh sách candidate.
func BuildMySQL(rows []MySQLIdxRow) *MySQLInfo {
	info := &MySQLInfo{}
	var order []string
	byName := map[string]*Candidate{}
	for _, r := range rows {
		if r.Index == PrimaryName {
			info.PK = append(info.PK, r.Column.String)
			if r.SubPart.Valid {
				info.PKPrefix = true
			}
			continue
		}
		c := byName[r.Index]
		if c == nil {
			c = &Candidate{Name: r.Index}
			byName[r.Index] = c
			order = append(order, r.Index)
		}
		why := ""
		switch {
		case !r.Column.Valid:
			why = "expression (functional) index"
			c.Columns = append(c.Columns, "<expression>")
		case r.SubPart.Valid:
			why = fmt.Sprintf("prefix part on column %s (uniqueness is on the column prefix only)", r.Column.String)
			c.Columns = append(c.Columns, r.Column.String)
		case !strings.EqualFold(r.Nullable, "NO"):
			why = fmt.Sprintf("column %s is nullable (NULLs are not unique and cannot identify a row)", r.Column.String)
			c.Columns = append(c.Columns, r.Column.String)
		case generatedExtra(r.Extra):
			why = fmt.Sprintf("column %s is a generated column", r.Column.String)
			c.Columns = append(c.Columns, r.Column.String)
		default:
			c.Columns = append(c.Columns, r.Column.String)
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

// LoadMySQL đọc PK và unique index của bảng.
func LoadMySQL(ctx context.Context, db *sql.DB, schema, table string) (*MySQLInfo, error) {
	rs, err := db.QueryContext(ctx, mysqlKeyRowsSQL, schema, table)
	if err != nil {
		return nil, fmt.Errorf("read unique keys of %s.%s: %w", schema, table, err)
	}
	defer rs.Close()
	var rows []MySQLIdxRow
	for rs.Next() {
		var r MySQLIdxRow
		if err := rs.Scan(&r.Index, &r.Column, &r.SubPart, &r.Nullable, &r.Extra); err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
	if err := rs.Err(); err != nil {
		return nil, err
	}
	return BuildMySQL(rows), nil
}

// MySQLResult: kết quả chọn khóa của một bảng MySQL/MariaDB.
type MySQLResult struct {
	Info *MySQLInfo
	// Key nil: bảng không có PK và không có unique index dùng được.
	Key *Key
	// Rejected: unique index bị loại (khi Key là UNIQUE hoặc nil).
	Rejected []Candidate
}

// Resolve chọn khóa từ info: PRIMARY KEY nếu có (kể cả PK có prefix — nơi gọi tự từ chối), nếu không thì
// unique index đủ điều kiện tốt nhất (Choose) với typeOK kiểm kiểu cột theo luật snapshot §5.2.
func (i *MySQLInfo) Resolve(typeOK func(cols []string) error) *MySQLResult {
	res := &MySQLResult{Info: i}
	if len(i.PK) > 0 {
		res.Key = &Key{Name: PrimaryName, Columns: append([]string(nil), i.PK...)}
		return res
	}
	res.Key, res.Rejected = Choose(i.Candidates, typeOK)
	return res
}

// ResolveMySQL = LoadMySQL + Resolve với typeOK là bộ mã hóa khóa của snapshot (snapshot.MySQLPKCodec).
func ResolveMySQL(ctx context.Context, db *sql.DB, schema, table string) (*MySQLResult, error) {
	info, err := LoadMySQL(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}
	id := model.TableID{Schema: schema, Table: table}
	return info.Resolve(func(cols []string) error {
		_, err := snapshot.MySQLPKCodec(ctx, db, id, cols)
		return err
	}), nil
}
