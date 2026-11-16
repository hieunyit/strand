package pipeline

import (
	"context"
	"fmt"

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
}
