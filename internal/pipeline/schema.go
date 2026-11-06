package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"sort"
	"strings"

	"strand/internal/assemble"
	"strand/internal/exitcode"
	"strand/internal/model"
	mysqlreader "strand/internal/reader/mysql"
	"strand/internal/snapshot"
)

// tableFilter áp dụng tables.include / exclude / rename (spec 02 §10).
// Mẫu dùng cú pháp glob của path.Match, ví dụ "app.*".
type tableFilter struct {
	include []string
	exclude []string
	rename  map[string]model.TableID
	// foldCase: so tên bảng đích không phân biệt hoa thường khi kiểm va chạm (review G3) — target MySQL
	// với lower_case_table_names=1/2 (mặc định trên Windows/macOS) coi a.T và a.t là một bảng. Thận trọng:
	// luôn bật cho target MySQL; target PG so chính xác vì strand luôn quote định danh.
	foldCase bool
}

func newTableFilter(include, exclude []string, rename map[string]string) (*tableFilter, error) {
	return newTargetTableFilter(include, exclude, rename, false)
}

// newTargetTableFilter như newTableFilter, foldCase = target so tên không phân biệt hoa thường (review G3).
func newTargetTableFilter(include, exclude []string, rename map[string]string, foldCase bool) (*tableFilter, error) {
	f := &tableFilter{include: include, exclude: exclude, rename: map[string]model.TableID{}, foldCase: foldCase}
	for _, p := range append(append([]string{}, include...), exclude...) {
		if _, err := path.Match(p, "x.y"); err != nil {
			return nil, fmt.Errorf("invalid table pattern %q: %w", p, err)
		}
	}
	// review R10: rename phải là đơn ánh — hai bảng nguồn về cùng một bảng đích sẽ ghi chung không gian PK.
	froms := make([]string, 0, len(rename))
	for from := range rename {
		froms = append(froms, from)
	}
	sort.Strings(froms)
	byTarget := map[model.TableID]string{}
	for _, from := range froms {
		to := rename[from]
		s, t, ok := strings.Cut(to, ".")
		if !ok {
			return nil, fmt.Errorf("rename target %q must be schema.table", to)
		}
		id := model.TableID{Schema: s, Table: t}
		if prev, dup := byTarget[f.targetKey(id)]; dup {
			return nil, fmt.Errorf("tables.rename maps both %q and %q to %q: each target table must come from exactly one source table", prev, from, to)
		}
		byTarget[f.targetKey(id)] = from
		f.rename[from] = id
	}
	return f, nil
}

// checkTargetCollisions kiểm tên đích (sau rename) của các bảng nguồn đã chọn là duy nhất (review R10).
// newTableFilter chỉ thấy map rename; va chạm giữa một rename và một bảng nguồn KHÔNG rename (ví dụ
// a.t1→a.t2 trong khi a.t2 cũng được include) chỉ phát hiện được khi đã có danh sách bảng thật.
// Bảng trong existing (đã thuộc pipeline) được tính vào; id có mặt cả hai phía chỉ tính một lần.
func checkTargetCollisions(f *tableFilter, tables, existing map[model.TableID]*tableMeta) error {
	ids := make([]model.TableID, 0, len(tables)+len(existing))
	for id := range existing {
		ids = append(ids, id)
	}
	for id := range tables {
		if _, ok := existing[id]; !ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	byTarget := make(map[model.TableID]model.TableID, len(ids))
	for _, id := range ids {
		t := f.Target(id)
		if prev, dup := byTarget[f.targetKey(t)]; dup {
			return exitcode.New("invalid_rename", exitcode.InvalidSyntax,
				fmt.Sprintf("source tables %s and %s both map to target table %s (check tables.rename / tables.include): "+
					"each target table must come from exactly one source table", prev, id, t), nil)
		}
		byTarget[f.targetKey(t)] = id
	}
	return nil
}

// targetKey là khóa so trùng tên bảng đích: chữ thường khi foldCase (review G3), nguyên văn nếu không.
func (f *tableFilter) targetKey(t model.TableID) model.TableID {
	if f.foldCase {
		return model.TableID{Schema: strings.ToLower(t.Schema), Table: strings.ToLower(t.Table)}
	}
	return t
}

func (f *tableFilter) Included(t model.TableID) bool {
	name := t.String()
	in := false
	for _, p := range f.include {
		if ok, _ := path.Match(p, name); ok {
			in = true
			break
		}
	}
	if !in {
		return false
	}
	for _, p := range f.exclude {
		if ok, _ := path.Match(p, name); ok {
			return false
		}
	}
	return true
}

// Target trả về tên bảng ở đích (sau rename).
func (f *tableFilter) Target(t model.TableID) model.TableID {
	if r, ok := f.rename[t.String()]; ok {
		return r
	}
	return t
}

// tableMeta là metadata một bảng nguồn mà pipeline cần.
type tableMeta struct {
	ID        model.TableID
	Columns   []string
	PK        []string
	PKNumeric bool // PK một cột kiểu số nguyên (đường snapshot int64)
	// PKCodec: mã hóa PK giữ thứ tự cho snapshot logic với PK tổng quát (việc C); nil nếu kiểu/collation
	// của PK chưa được hỗ trợ — PKCodecErr nói vì sao.
	PKCodec    *snapshot.PKCodec
	PKCodecErr error
	// TargetPKCodec: codec PK của bảng đích (đã kiểm cùng thứ tự với source; checkSnapshotKey).
	TargetPKCodec *snapshot.PKCodec
	AvgRow        int64 // AVG_ROW_LENGTH (ước lượng chi phí chunk cho governor)
	Keys          assemble.KeyMeta
	// TargetGenerated: cột generated ở bảng ĐÍCH — không ghi giá trị (generated.go).
	TargetGenerated map[string]bool
	// LaneTable (source PG): bảng có ràng buộc duy nhất ngoài PK (UNIQUE, kể cả partial/biểu thức, hoặc
	// exclusion) và không REPLICA IDENTITY FULL — pgoutput không gửi giá trị cũ nên writeset không thấy
	// xung đột; ở relaxed mọi Txn chạm bảng đi làn tuần tự (model.Txn.Lane).
	LaneTable bool
}

// checkPrimaryKeyPrefix từ chối bảng MySQL có PRIMARY KEY chứa cột prefix, ví dụ PRIMARY KEY (code(10))
// (review G1, cùng gốc với N01). Tính duy nhất chỉ nằm trên prefix, trong khi writeset, keyset/codec
// snapshot, verify và upsert/delete của writer đều coi PK là định danh theo TOÀN BỘ giá trị cột: hai row
// khác giá trị nhưng trùng prefix là cùng một row ở nguồn — không có cách ánh xạ an toàn, nên fail closed.
// prefix: tên index → có cột SUB_PART khác NULL (như loadSourceTables đọc).
func checkPrimaryKeyPrefix(id model.TableID, prefix map[string]bool) error {
	if !prefix["PRIMARY"] {
		return nil
	}
	return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, fmt.Sprintf(
		"table %s: primary key with prefix part is not supported (uniqueness is on the column prefix, "+
			"strand identifies rows by the full primary key value); use a full-column primary key", id), nil)
}

// loadSourceTables đọc từ information_schema của source mọi bảng thỏa bộ lọc,
// kèm cột, PK, unique index và FK.
func loadSourceTables(ctx context.Context, db *sql.DB, f *tableFilter) (map[model.TableID]*tableMeta, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES
		 WHERE TABLE_TYPE = 'BASE TABLE'
		   AND TABLE_SCHEMA NOT IN ('mysql','sys','information_schema','performance_schema')`)
	if err != nil {
		return nil, fmt.Errorf("list source tables: %w", err)
	}
	var ids []model.TableID
	for rows.Next() {
		var t model.TableID
		if err := rows.Scan(&t.Schema, &t.Table); err != nil {
			rows.Close()
			return nil, err
		}
		if f.Included(t) {
			ids = append(ids, t)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make(map[model.TableID]*tableMeta, len(ids))
	for _, id := range ids {
		m := &tableMeta{ID: id}
		_ = db.QueryRowContext(ctx, "SELECT COALESCE(AVG_ROW_LENGTH, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
			id.Schema, id.Table).Scan(&m.AvgRow)
		cr, err := db.QueryContext(ctx, `
			SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, id.Schema, id.Table)
		if err != nil {
			return nil, err
		}
		types := map[string]string{}
		for cr.Next() {
			var c, dt string
			if err := cr.Scan(&c, &dt); err != nil {
				cr.Close()
				return nil, err
			}
			m.Columns = append(m.Columns, c)
			types[c] = strings.ToLower(dt)
		}
		cr.Close()

		// Mọi index unique (gồm PRIMARY), theo thứ tự cột trong index. SUB_PART khác NULL = cột prefix
		// (review N01): unique theo prefix nên writeset không được hash cả giá trị cột.
		ir, err := db.QueryContext(ctx, `
			SELECT INDEX_NAME, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
			 ORDER BY INDEX_NAME, SEQ_IN_INDEX`, id.Schema, id.Table)
		if err != nil {
			return nil, err
		}
		idx := map[string][]string{}
		prefix := map[string]bool{}
		var names []string
		for ir.Next() {
			var n, c string
			var sub sql.NullInt64
			if err := ir.Scan(&n, &c, &sub); err != nil {
				ir.Close()
				return nil, err
			}
			if _, ok := idx[n]; !ok {
				names = append(names, n)
			}
			idx[n] = append(idx[n], c)
			if sub.Valid {
				prefix[n] = true
			}
		}
		ir.Close()
		if err := checkPrimaryKeyPrefix(id, prefix); err != nil {
			return nil, err
		}
		sort.Strings(names)
		for _, n := range names {
			if n == "PRIMARY" {
				m.PK = idx[n]
				continue
			}
			m.Keys.Unique = append(m.Keys.Unique, assemble.UniqueIndex{Name: n, Columns: idx[n], Prefix: prefix[n]})
		}
		if len(m.PK) == 1 {
			switch types[m.PK[0]] {
			case "tinyint", "smallint", "mediumint", "int", "bigint":
				m.PKNumeric = true
			}
		}
		if len(m.PK) > 0 {
			m.PKCodec, m.PKCodecErr = mysqlPKCodec(ctx, db, id, m.PK)
			if m.PKNumeric && m.PKCodec != nil && !m.PKCodec.IsSingleInt() {
				// BIGINT UNSIGNED…: giá trị > MaxInt64 không biểu diễn được bằng int64 (biên bảng lỗi, event bị
				// từ chối) — đi đường khóa tổng quát (mã hóa uint64).
				m.PKNumeric = false
			}
		}

		fr, err := db.QueryContext(ctx, `
			SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
			  FROM information_schema.KEY_COLUMN_USAGE
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
			 ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`, id.Schema, id.Table)
		if err != nil {
			return nil, err
		}
		fks := map[string]*assemble.ForeignKeyMeta{}
		var fkNames []string
		for fr.Next() {
			var cn, col, ps, pt, rcol string
			if err := fr.Scan(&cn, &col, &ps, &pt, &rcol); err != nil {
				fr.Close()
				return nil, err
			}
			fk, ok := fks[cn]
			if !ok {
				fk = &assemble.ForeignKeyMeta{ParentTable: model.TableID{Schema: ps, Table: pt}}
				fks[cn] = fk
				fkNames = append(fkNames, cn)
			}
			fk.Columns = append(fk.Columns, col)
			fk.RefColumns = append(fk.RefColumns, rcol)
		}
		fr.Close()
		for _, n := range fkNames {
			m.Keys.ForeignKeys = append(m.Keys.ForeignKeys, *fks[n])
		}
		out[id] = m
	}
	return out, nil
}

// mysqlPKCodec: xem snapshot.MySQLPKCodec.
func mysqlPKCodec(ctx context.Context, db *sql.DB, id model.TableID, pk []string) (*snapshot.PKCodec, error) {
	return snapshot.MySQLPKCodec(ctx, db, id, pk)
}

// mysqlCollationCharsets: xem mysqlreader.LoadCollationCharsets.
func mysqlCollationCharsets(ctx context.Context, db *sql.DB) (map[uint64]string, error) {
	return mysqlreader.LoadCollationCharsets(ctx, db)
}
