package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"strand/internal/checkpoint"
	"strand/internal/model"
)

// spec: 02 §5 (flush chunk snapshot: range ở target đúng bằng ảnh đã merge, trong một transaction)

// IsPGRetryable: lỗi tạm thời của PostgreSQL mà cả transaction đã rollback, nên chạy lại an toàn
// (40001 serialization_failure, 40P01 deadlock_detected, 55P03 lock_not_available).
func IsPGRetryable(err error) bool {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "40001", "40P01", "55P03":
			return true
		}
	}
	return false
}

// IsPGUniqueViolation: vi phạm UNIQUE (23505) hoặc ràng buộc loại trừ (23P01) ở target PG.
func IsPGUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (pe.Code == "23505" || pe.Code == "23P01")
}

// PGReplaceRange thay nguyên range [startPK, endPK] (PK một cột số nguyên) của bảng đích PG bằng
// rows trong MỘT transaction: DELETE các khóa của range KHÔNG có trong rows, rồi INSERT … ON CONFLICT
// (pk) DO UPDATE theo khối. Không DELETE khóa còn trong rows: bảng đích có thể có FK ON DELETE
// CASCADE / SET NULL trỏ tới bảng này (schema-init tạo FK như source) — DELETE rồi INSERT lại cùng
// khóa sẽ cascade xoá/đổi row con đang đúng. Khóa vắng mặt thì đã bị xoá ở source tại high watermark,
// nên cascade ở target khớp với source. Trạng thái cuối của range vẫn đúng bằng rows.
// Target không bao giờ hiển thị trạng thái lùi của range (RS-1). Lỗi tạm thời được retry.
// db mở bằng driver pgx stdlib. Giá trị trong rows phải đã ở dạng PG (typemap đã chuyển).
func PGReplaceRange(ctx context.Context, db *sql.DB, table model.TableID, pkCol string, columnNames []string,
	rows []*model.RowChange, startPK, endPK int64) error {
	keep, err := rowPKs(rows)
	if err != nil {
		return err
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE %s >= $1 AND %s <= $2 AND %s <> ALL($3::bigint[])",
		quotePGTable(table), quotePG(pkCol), quotePG(pkCol), quotePG(pkCol))
	return pgReplaceRetry(ctx, db, table, pkCol, columnNames, rows, q, startPK, endPK, keep)
}

// PGReplaceKeys như PGReplaceRange nhưng thay đúng TẬP khóa keys (chunk re-read, 02 §8): DELETE các khóa
// đó rồi upsert rows (rows chỉ chứa khóa trong keys), trong MỘT transaction.
func PGReplaceKeys(ctx context.Context, db *sql.DB, table model.TableID, pkCol string, columnNames []string,
	rows []*model.RowChange, keys []int64) error {
	keep, err := rowPKs(rows)
	if err != nil {
		return err
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE %s = ANY($1::bigint[]) AND %s <> ALL($2::bigint[])",
		quotePGTable(table), quotePG(pkCol), quotePG(pkCol))
	return pgReplaceRetry(ctx, db, table, pkCol, columnNames, rows, q, keys, keep)
}

// rowPKs lấy PK (một cột số nguyên) của rows để loại khỏi DELETE.
func rowPKs(rows []*model.RowChange) ([]int64, error) {
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		if len(r.Key) != 1 {
			return nil, fmt.Errorf("replace range: row of %s has a %d-column key, want 1", r.Table, len(r.Key))
		}
		switch v := r.Key[0].(type) {
		case int64:
			out = append(out, v)
		case int32:
			out = append(out, int64(v))
		case int:
			out = append(out, int64(v))
		case int16:
			out = append(out, int64(v))
		case int8:
			out = append(out, int64(v))
		case uint32:
			out = append(out, int64(v))
		case uint16:
			out = append(out, int64(v))
		case uint8:
			out = append(out, int64(v))
		case uint64:
			if v > math.MaxInt64 {
				return nil, fmt.Errorf("replace range: key %d of %s exceeds bigint", v, r.Table)
			}
			out = append(out, int64(v))
		default:
			return nil, fmt.Errorf("replace range: unsupported key type %T in %s", v, r.Table)
		}
	}
	return out, nil
}

func pgReplaceRetry(ctx context.Context, db *sql.DB, table model.TableID, pkCol string, columnNames []string,
	rows []*model.RowChange, del string, delArgs ...any) error {
	var err error
	for attempt := 1; attempt <= 20; attempt++ {
		err = pgReplaceOnce(ctx, db, table, pkCol, columnNames, rows, del, delArgs...)
		if err == nil || !IsPGRetryable(err) || ctx.Err() != nil {
			return err
		}
		time.Sleep(time.Duration(attempt*attempt) * 5 * time.Millisecond)
	}
	return err
}

func pgReplaceOnce(ctx context.Context, db *sql.DB, table model.TableID, pkCol string, columnNames []string,
	rows []*model.RowChange, del string, delArgs ...any) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := checkpoint.CheckFence(ctx, tx, true); err != nil { // SG-1, review R07
		return err
	}
	if _, err := tx.ExecContext(ctx, del, delArgs...); err != nil {
		return fmt.Errorf("clear chunk range on target: %w", err)
	}
	if len(rows) > 0 && len(columnNames) > 0 {
		per := pgMaxParams / len(columnNames)
		if per > 1000 {
			per = 1000
		}
		for i := 0; i < len(rows); i += per {
			end := i + per
			if end > len(rows) {
				end = len(rows)
			}
			vals := make([][]any, 0, end-i)
			for _, r := range rows[i:end] {
				m := make(map[string]any, len(r.Cols))
				for _, c := range r.Cols {
					m[c.Name] = c.Value
				}
				v := make([]any, len(columnNames))
				for j, c := range columnNames {
					v[j] = m[c]
				}
				vals = append(vals, v)
			}
			uq, args := buildUpsert(table, columnNames, []string{pkCol}, vals)
			if _, err := tx.ExecContext(ctx, uq, args...); err != nil {
				return fmt.Errorf("upsert flush batch into %s: %w", table, err)
			}
		}
	}
	return tx.Commit()
}

// PGClearTable xóa mọi row của bảng đích PG (bắt đầu snapshot mới của bảng, 02 §5).
func PGClearTable(ctx context.Context, db *sql.DB, table model.TableID) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := checkpoint.CheckFence(ctx, tx, true); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+quotePGTable(table)); err != nil {
		return err
	}
	return tx.Commit()
}

// PGDeleteUnread xóa ở bảng đích PG các row PK trong [lo, hi] (khi hasRange) và các khóa keys, trong một
// transaction: phần "Chưa đọc" của snapshot dở khi chạy tiếp (MySQLSnapshotter.ClearUnread). Bảng con
// đích có FK ON DELETE trỏ tới bảng này thì PG cascade như với mọi DELETE (giới hạn đã ghi ở handoff).
func PGDeleteUnread(ctx context.Context, db *sql.DB, table model.TableID, pkCol string, lo, hi int64, hasRange bool, keys []int64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := checkpoint.CheckFence(ctx, tx, true); err != nil {
		return err
	}
	if hasRange {
		q := fmt.Sprintf("DELETE FROM %s WHERE %s >= $1 AND %s <= $2", quotePGTable(table), quotePG(pkCol), quotePG(pkCol))
		if _, err := tx.ExecContext(ctx, q, lo, hi); err != nil {
			return err
		}
	}
	if len(keys) > 0 {
		q := fmt.Sprintf("DELETE FROM %s WHERE %s = ANY($1::bigint[])", quotePGTable(table), quotePG(pkCol))
		if _, err := tx.ExecContext(ctx, q, keys); err != nil {
			return err
		}
	}
	return tx.Commit()
}
