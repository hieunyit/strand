package pipeline

import (
	"context"
	"fmt"
	"strings"

	"strand/internal/exitcode"
	"strand/internal/model"
	"strand/internal/mysqlver"
	"strand/internal/rowkey"
)

// suggestInput dựng đầu vào gợi ý thêm khóa cho bảng id (flavor/phiên bản của source).
func (p *Pipeline) suggestInput(ctx context.Context, id model.TableID, m *tableMeta) rowkey.SuggestInput {
	in := rowkey.SuggestInput{Table: id, EstRows: m.EstRows, PGIndex: m.KeySuggestIdx, Notes: m.KeyNotes, KeylessWhy: m.KeylessWhy}
	if p.srcEngine == model.EnginePG {
		in.Flavor = "postgres"
		return in
	}
	in.Flavor = "mysql"
	if ver, err := mysqlver.Cached(ctx, p.src); err == nil {
		if ver.IsMariaDB() {
			in.Flavor = "mariadb"
		}
		in.Major, in.Minor, in.Patch = ver.Major, ver.Minor, ver.Patch
	}
	return in
}

// keyLabel: "PRIMARY (id)" / "UNIQUE uk (a, b)" của khóa đang dùng; "" nếu chưa có.
func keyLabel(m *tableMeta) string {
	if m == nil || len(m.PK) == 0 {
		return ""
	}
	if m.Keyless {
		return rowkey.KeylessLabel
	}
	return rowkey.Key{Name: m.KeyName, Columns: m.PK}.Label()
}

// missingKeyError: lỗi missing_pk (exit 3) kèm câu lệnh gợi ý thêm khóa cho bảng không có khóa dùng được.
func (p *Pipeline) missingKeyError(ctx context.Context, id model.TableID, m *tableMeta) error {
	return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, rowkey.Suggest(p.suggestInput(ctx, id, m)), nil)
}

// markKeyless bật chế độ keyless (tables.keyless=full_row) cho bảng không có khóa dùng được và đủ điều kiện:
// kiểu cột so bằng được (rowkey.KeylessProblem); nguồn PostgreSQL cần REPLICA IDENTITY FULL (MySQL/MariaDB đã
// bắt buộc binlog_row_image=FULL). Khi bật, "khóa" = mọi cột theo thứ tự cột: assembler đưa ảnh đầy đủ vào
// RowChange.Key (INSERT/ảnh sau) và OldKey (ảnh trước của UPDATE), writer khớp row theo ảnh đó.
func (p *Pipeline) markKeyless(metas map[model.TableID]*tableMeta) {
	if !p.cfg.Tables.KeylessOn() {
		return
	}
	pg := p.srcEngine == model.EnginePG
	for id, m := range metas {
		if len(m.PK) > 0 {
			continue
		}
		if why := rowkey.KeylessProblem(m.Columns, m.ColTypes, pg, p.cfg.Tables.KeylessAllowFloat); why != "" {
			m.KeylessWhy = why
			continue
		}
		if pg && m.ReplIdent != "f" {
			m.KeylessWhy = fmt.Sprintf("the source table needs REPLICA IDENTITY FULL: ALTER TABLE %q.%q REPLICA IDENTITY FULL;", id.Schema, id.Table)
			continue
		}
		m.Keyless = true
		m.PK = append([]string(nil), m.Columns...)
		m.KeyName = ""
		m.PKNumeric, m.PKCodec, m.PKCodecErr = false, nil, nil
	}
}

// keylessPendingError: bảng keyless còn phải snapshot logic — không có khóa nên không chia range/đối soát được.
func (p *Pipeline) keylessPendingError(ctx context.Context, id model.TableID, m *tableMeta) error {
	in := p.suggestInput(ctx, id, m)
	return exitcode.New(exitcode.ErrMissingPK, exitcode.Rejected, fmt.Sprintf(
		"table %s has no primary key or NOT NULL unique key: tables.keyless=full_row only allows it when its data comes from a seed "+
			"(bootstrap.default=seed / --start-at), not from a logical snapshot (snapshot needs a key to split and reconcile ranges).\n%s",
		id, rowkey.Suggest(in)), nil)
}

// setWriterKey khai báo cột khóa (và chế độ keyless) của bảng đích cho writer.
func setWriterKey(w batchWriter, target model.TableID, m *tableMeta) {
	w.SetPrimaryKey(target, m.PK)
	w.SetKeyless(target, m.Keyless)
	if sw, ok := w.(interface {
		SetKeylessStringCols(model.TableID, []string)
	}); ok && m.Keyless {
		var cols []string
		for _, c := range m.Columns {
			switch m.ColTypes[c] {
			case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set":
				cols = append(cols, c)
			}
		}
		sw.SetKeylessStringCols(target, cols)
	}
}

// checkKeylessTargetCollations: UPDATE/DELETE keyless ở target PG khớp bằng IS NOT DISTINCT FROM theo collation của cột;
// collation không tất định (ICU, không phân biệt hoa/thường/dấu) coi giá trị khác nhau là bằng nhau và có thể trúng
// nhầm hàng — từ chối. Bảng đích chưa tồn tại thì bỏ qua (schema init tạo cột với collation mặc định, tất định).
func (p *Pipeline) checkKeylessTargetCollations(ctx context.Context, id model.TableID, m *tableMeta) error {
	if p.dstEngine != model.EnginePG || !m.Keyless {
		return nil
	}
	tgt := p.filter.Target(id)
	rows, err := p.dst.QueryContext(ctx, `SELECT a.attname, co.collname FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_collation co ON co.oid = a.attcollation
		WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped AND NOT co.collisdeterministic`, tgt.Schema, tgt.Table)
	if err != nil {
		return fmt.Errorf("check collations of target table %s: %w", tgt, err)
	}
	defer rows.Close()
	var bad []string
	for rows.Next() {
		var col, coll string
		if err := rows.Scan(&col, &coll); err != nil {
			return err
		}
		bad = append(bad, col+" ("+coll+")")
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(bad) > 0 {
		return exitcode.New(exitcode.ErrCollationLooser, exitcode.Rejected, fmt.Sprintf(
			"keyless table %s: target column(s) %s use a non-deterministic collation; rows are matched on all columns, so values that differ only by case/accent would be treated as equal and the wrong row could be updated or deleted; use a deterministic collation",
			tgt, strings.Join(bad, ", ")), nil)
	}
	return nil
}
