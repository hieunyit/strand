package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"sort"
	"strings"

	"strand/internal/assemble"
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
}

func newTableFilter(include, exclude []string, rename map[string]string) (*tableFilter, error) {
	f := &tableFilter{include: include, exclude: exclude, rename: map[string]model.TableID{}}
	for _, p := range append(append([]string{}, include...), exclude...) {
		if _, err := path.Match(p, "x.y"); err != nil {
			return nil, fmt.Errorf("invalid table pattern %q: %w", p, err)
		}
	}
	for from, to := range rename {
		s, t, ok := strings.Cut(to, ".")
		if !ok {
			return nil, fmt.Errorf("rename target %q must be schema.table", to)
		}
		f.rename[from] = model.TableID{Schema: s, Table: t}
	}
	return f, nil
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

		// Mọi index unique (gồm PRIMARY), theo thứ tự cột trong index.
		ir, err := db.QueryContext(ctx, `
			SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS
			 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
			 ORDER BY INDEX_NAME, SEQ_IN_INDEX`, id.Schema, id.Table)
		if err != nil {
			return nil, err
		}
		idx := map[string][]string{}
		var names []string
		for ir.Next() {
			var n, c string
			if err := ir.Scan(&n, &c); err != nil {
				ir.Close()
				return nil, err
			}
			if _, ok := idx[n]; !ok {
				names = append(names, n)
			}
			idx[n] = append(idx[n], c)
		}
		ir.Close()
		sort.Strings(names)
		for _, n := range names {
			if n == "PRIMARY" {
				m.PK = idx[n]
				continue
			}
			m.Keys.Unique = append(m.Keys.Unique, assemble.UniqueIndex{Name: n, Columns: idx[n]})
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
