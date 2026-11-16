package writer

import (
	"context"
	"fmt"
	"strings"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// Bảng keyless (tables.keyless=full_row, C-lite): bảng không có khóa, dữ liệu đầu đến từ seed. Pipeline đăng ký
// "khóa" = mọi cột; RowChange.Key là ảnh đầy đủ (INSERT: ảnh sau; DELETE: ảnh trước; UPDATE: ảnh sau, còn
// RowChange.OldKey là ảnh trước khi khác ảnh sau). Writer:
//   - INSERT  → INSERT thường (không upsert: hai hàng giống hệt nhau là hai hàng).
//   - UPDATE  → sửa ĐÚNG MỘT hàng khớp ảnh trước (so bằng NULL-safe theo mọi cột, LIMIT 1).
//   - DELETE  → xóa ĐÚNG MỘT hàng khớp ảnh trước.
//
// UPDATE/DELETE không khớp hàng nào = target lệch source: dừng với keyless_row_missing, không bỏ qua im lặng.
// Dữ liệu và checkpoint nằm chung một transaction target nên replay một Txn đã commit không xảy ra (CP-1).

// keylessMissing: lỗi khi UPDATE/DELETE không khớp hàng nào ở target.
func keylessMissing(t model.TableID, op model.Op) error {
	return exitcode.New(exitcode.ErrKeylessRowMissing, exitcode.DataError, fmt.Sprintf(
		"keyless table %s: %s matched no row on the target (the target diverged from the source: a row that should exist is missing or differs); "+
			"re-seed the table, or run `strand verify` to see the difference", t, op), nil)
}

// keylessMatchCols: cột khớp = các cột đã đăng ký; số giá trị ảnh phải bằng số cột.
func keylessMatchCols(r *pkRegistry, row *model.RowChange, img []model.Value) ([]string, error) {
	cols := r.registered(row.Table)
	if len(cols) == 0 || len(cols) != len(img) {
		return nil, fmt.Errorf("keyless table %s: row image has %d values but %d columns are registered", row.Table, len(img), len(cols))
	}
	return cols, nil
}

// keylessBefore: ảnh trước của UPDATE; nil nếu ảnh trước = ảnh sau (không đổi gì → không cần ghi).
func keylessBefore(row *model.RowChange) []model.Value {
	if len(row.OldKey) > 0 && keysDiffer(row.OldKey, row.Key) {
		return row.OldKey
	}
	return nil
}

// --- MySQL ---

// mysqlMatchCond: điều kiện khớp ảnh trước. Cột chuỗi (str) khớp theo collation của cột (để dùng được index) VÀ
// theo byte: collation _ci/_ai và PAD SPACE coi 'abc' = 'ABC' = 'abc ', nên chỉ `<=>` có thể trúng nhầm hàng.
// CONVERT(... USING utf8mb4) làm hai phía cùng bộ ký tự (cột latin1 vs tham số utf8mb4) trước khi so BINARY.
func mysqlMatchCond(cols []string, str map[string]bool) string {
	conds := make([]string, 0, len(cols))
	for _, c := range cols {
		q := quoteMySQL(c)
		if str[c] {
			conds = append(conds, q+" <=> ? AND BINARY CONVERT("+q+" USING utf8mb4) <=> BINARY ?")
			continue
		}
		conds = append(conds, q+" <=> ?")
	}
	return strings.Join(conds, " AND ")
}

// keylessMatchArgs: tham số của mysqlMatchCond (cột chuỗi dùng giá trị hai lần).
func keylessMatchArgs(cols []string, str map[string]bool, img []model.Value) []any {
	out := make([]any, 0, len(img)+len(str))
	for i, v := range img {
		out = append(out, v)
		if i < len(cols) && str[cols[i]] {
			out = append(out, v)
		}
	}
	return out
}

// buildKeylessInsertMySQL: INSERT thường một hàng.
func buildKeylessInsertMySQL(t model.TableID, cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = quoteMySQL(c)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteMySQLTable(t), strings.Join(q, ", "),
		strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", "))
}

// buildKeylessUpdateMySQL: UPDATE đúng một hàng khớp ảnh trước (NULL-safe `<=>`, LIMIT 1).
func buildKeylessUpdateMySQL(t model.TableID, setCols, matchCols []string, str map[string]bool) string {
	sets := make([]string, len(setCols))
	for i, c := range setCols {
		sets[i] = quoteMySQL(c) + " = ?"
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s LIMIT 1", quoteMySQLTable(t), strings.Join(sets, ", "), mysqlMatchCond(matchCols, str))
}

// buildKeylessDeleteMySQL: DELETE đúng một hàng khớp ảnh trước.
func buildKeylessDeleteMySQL(t model.TableID, matchCols []string, str map[string]bool) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT 1", quoteMySQLTable(t), mysqlMatchCond(matchCols, str))
}

// execAffected chạy một câu ngay (sau khi gửi hết câu đang xếp lô, để giữ thứ tự) và trả số hàng bị ảnh hưởng;
// -1 khi không biết (hook kiểm thử q.send).
func (q *stmtQueue) execAffected(ctx context.Context, query string, args ...any) (int64, error) {
	if err := q.flush(ctx); err != nil {
		return 0, err
	}
	if q.send != nil {
		return -1, q.send(ctx, query, args...)
	}
	res, err := q.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (w *MySQLWriter) applyKeylessMySQL(ctx context.Context, q *stmtQueue, row *model.RowChange) error {
	switch row.Op {
	case model.OpInsert:
		if len(row.Cols) == 0 {
			return fmt.Errorf("table %s: insert without column values", row.Table)
		}
		names, vals := colNamesVals(row.Cols, false)
		return q.execRows(ctx, 1, buildKeylessInsertMySQL(row.Table, names), vals...)
	case model.OpUpdate:
		before := keylessBefore(row)
		if before == nil {
			q.written++ // ảnh trước = ảnh sau: không có gì để ghi
			return nil
		}
		if hasUnchanged(row) {
			return fmt.Errorf("table %s: partial row image is not supported on a MySQL target", row.Table)
		}
		match, err := keylessMatchCols(&w.pk, row, before)
		if err != nil {
			return err
		}
		names, vals := colNamesVals(row.Cols, false)
		str := w.pk.stringCols(row.Table)
		n, err := q.execAffected(ctx, buildKeylessUpdateMySQL(row.Table, names, match, str), append(vals, keylessMatchArgs(match, str, before)...)...)
		if err != nil {
			return fmt.Errorf("keyless update on %s: %w", row.Table, err)
		}
		if n == 0 {
			return keylessMissing(row.Table, row.Op)
		}
		q.written++
		return nil
	case model.OpDelete:
		match, err := keylessMatchCols(&w.pk, row, row.Key)
		if err != nil {
			return err
		}
		str := w.pk.stringCols(row.Table)
		n, err := q.execAffected(ctx, buildKeylessDeleteMySQL(row.Table, match, str), keylessMatchArgs(match, str, row.Key)...)
		if err != nil {
			return fmt.Errorf("keyless delete on %s: %w", row.Table, err)
		}
		if n == 0 {
			return keylessMissing(row.Table, row.Op)
		}
		q.written++
		return nil
	}
	return fmt.Errorf("table %s: unknown op %q", row.Table, row.Op)
}

// colNamesVals tách tên cột và giá trị; skipUnchanged bỏ cột Unchanged (TOAST, PG).
func colNamesVals(cols []model.ColValue, skipUnchanged bool) ([]string, []any) {
	names := make([]string, 0, len(cols))
	vals := make([]any, 0, len(cols))
	for _, c := range cols {
		if skipUnchanged && c.Unchanged {
			continue
		}
		names = append(names, c.Name)
		vals = append(vals, c.Value)
	}
	return names, vals
}

// --- PostgreSQL ---

// pgMatchSubquery: `ctid = (SELECT ctid FROM t WHERE c IS NOT DISTINCT FROM $n ... LIMIT 1)`; tham số đánh số từ start.
func pgMatchSubquery(t model.TableID, cols []string, start int) string {
	conds := make([]string, len(cols))
	for i, c := range cols {
		conds[i] = fmt.Sprintf("%s IS NOT DISTINCT FROM $%d", quotePG(c), start+i)
	}
	tbl := quotePGTable(t)
	return fmt.Sprintf("ctid = (SELECT ctid FROM %s WHERE %s LIMIT 1)", tbl, strings.Join(conds, " AND "))
}

// buildKeylessInsertPG: INSERT thường một hàng.
func buildKeylessInsertPG(t model.TableID, cols []string) string {
	q := make([]string, len(cols))
	ph := make([]string, len(cols))
	for i, c := range cols {
		q[i] = quotePG(c)
		ph[i] = fmt.Sprintf("$%d", i+1)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) OVERRIDING SYSTEM VALUE VALUES (%s)", quotePGTable(t), strings.Join(q, ", "), strings.Join(ph, ", "))
}

// buildKeylessUpdatePG: UPDATE đúng một hàng (chọn bằng ctid của hàng đầu tiên khớp ảnh trước).
func buildKeylessUpdatePG(t model.TableID, setCols, matchCols []string) string {
	sets := make([]string, len(setCols))
	for i, c := range setCols {
		sets[i] = fmt.Sprintf("%s = $%d", quotePG(c), i+1)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", quotePGTable(t), strings.Join(sets, ", "), pgMatchSubquery(t, matchCols, len(setCols)+1))
}

// buildKeylessDeletePG: DELETE đúng một hàng.
func buildKeylessDeletePG(t model.TableID, matchCols []string) string {
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quotePGTable(t), pgMatchSubquery(t, matchCols, 1))
}

func (a *pgApplier) applyKeyless(row *model.RowChange) error {
	if err := a.flushAll(); err != nil {
		return err
	}
	switch row.Op {
	case model.OpInsert:
		if len(row.Cols) == 0 {
			return fmt.Errorf("table %s: insert without column values", row.Table)
		}
		if hasUnchanged(row) {
			return fmt.Errorf("table %s: insert with unchanged (toast) column", row.Table)
		}
		names, vals := colNamesVals(row.Cols, false)
		if _, err := a.tx.Exec(a.ctx, buildKeylessInsertPG(row.Table, names), vals...); err != nil {
			return fmt.Errorf("keyless insert on %s: %w", row.Table, err)
		}
	case model.OpUpdate:
		before := keylessBefore(row)
		if before == nil {
			a.written++
			return nil
		}
		match, err := keylessMatchCols(&a.w.pk, row, before)
		if err != nil {
			return err
		}
		names, vals := colNamesVals(row.Cols, true) // cột TOAST không đổi không nằm trong SET
		tag, err := a.tx.Exec(a.ctx, buildKeylessUpdatePG(row.Table, names, match), append(vals, toArgs(before)...)...)
		if err != nil {
			return fmt.Errorf("keyless update on %s: %w", row.Table, err)
		}
		if tag.RowsAffected() == 0 {
			return keylessMissing(row.Table, row.Op)
		}
	case model.OpDelete:
		match, err := keylessMatchCols(&a.w.pk, row, row.Key)
		if err != nil {
			return err
		}
		tag, err := a.tx.Exec(a.ctx, buildKeylessDeletePG(row.Table, match), toArgs(row.Key)...)
		if err != nil {
			return fmt.Errorf("keyless delete on %s: %w", row.Table, err)
		}
		if tag.RowsAffected() == 0 {
			return keylessMissing(row.Table, row.Op)
		}
	default:
		return fmt.Errorf("table %s: unknown op %q", row.Table, row.Op)
	}
	a.written++
	return nil
}
