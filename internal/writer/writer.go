package writer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/exitcode"
	"strand/internal/model"
)

// spec: 02 §3, §4 quy tắc 3, 6; invariant: CP-1, SG-1

// DeltaRow đại diện cho dữ liệu delta của một worker tại thời điểm commit batch.
// spec: 02 §3
type DeltaRow struct {
	PipelineID string
	WorkerID   int
	Epoch      int64
	BatchNo    int64
	Applied    string
}

// Writer là interface áp dụng các batch transaction vào target database.
// invariant: CP-1, SG-1
type Writer interface {
	// ApplyBatch áp dụng batch transaction cùng row delta trong cùng một transaction ở target (CP-1).
	ApplyBatch(ctx context.Context, batch []*model.Txn, delta DeltaRow) error
}

// -----------------------------------------------------------------------------
// MySQLWriter
// -----------------------------------------------------------------------------

// MySQLWriter cài đặt Writer cho MySQL target.
type MySQLWriter struct {
	db         *sql.DB
	pipelineID string
	workerID   int
	epoch      int64
}

// NewMySQLWriter khởi tạo MySQLWriter mới.
func NewMySQLWriter(db *sql.DB, pipelineID string, workerID int, epoch int64) *MySQLWriter {
	return &MySQLWriter{
		db:         db,
		pipelineID: pipelineID,
		workerID:   workerID,
		epoch:      epoch,
	}
}

// EnsureWorkerRow đảm bảo row cho worker_id đã tồn tại trong _strand_worker_delta.
func (w *MySQLWriter) EnsureWorkerRow(ctx context.Context) error {
	query := `INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied)
	VALUES (?, ?, ?, 1, '')
	ON DUPLICATE KEY UPDATE epoch = VALUES(epoch)`
	_, err := w.db.ExecContext(ctx, query, w.pipelineID, w.workerID, w.epoch)
	if err != nil {
		return fmt.Errorf("failed to ensure mysql worker row: %w", err)
	}
	return nil
}

// ApplyBatch ghi dữ liệu của batch và cập nhật row delta trong cùng transaction.
// spec: 02 §4 quy tắc 3: nếu số row bị ảnh hưởng khác 1 thì rollback và trả về lỗi fenced.
func (w *MySQLWriter) ApplyBatch(ctx context.Context, batch []*model.Txn, delta DeltaRow) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Ghi dữ liệu vào các bảng ứng dụng
	for _, txn := range batch {
		if err := applyRowChangesMySQL(ctx, tx, txn.Rows); err != nil {
			return fmt.Errorf("failed to apply row changes: %w", err)
		}
	}

	// 2. Ghi row delta có điều kiện epoch trong CÙNG transaction (CP-1)
	deltaPipelineID := delta.PipelineID
	if deltaPipelineID == "" {
		deltaPipelineID = w.pipelineID
	}
	deltaWorkerID := delta.WorkerID
	if deltaWorkerID == 0 {
		deltaWorkerID = w.workerID
	}
	deltaEpoch := delta.Epoch
	if deltaEpoch == 0 {
		deltaEpoch = w.epoch
	}

	query := "UPDATE _strand_worker_delta SET applied = ?, batch_no = batch_no + 1 WHERE pipeline_id = ? AND worker_id = ? AND epoch = ?"
	res, err := tx.ExecContext(ctx, query, delta.Applied, deltaPipelineID, deltaWorkerID, deltaEpoch)
	if err != nil {
		return fmt.Errorf("failed to update worker delta: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if affected != 1 {
		// spec: 02 §4 quy tắc 3: số row != 1 -> rollback và dừng với mã fenced
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("writer fenced: expected 1 row affected, got %d", affected), nil)
	}

	// 3. Commit cả dữ liệu và delta
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	return nil
}

func applyRowChangesMySQL(ctx context.Context, tx *sql.Tx, rows []model.RowChange) error {
	for _, row := range rows {
		tbl := row.Table.String()
		switch row.Op {
		case model.OpInsert:
			if len(row.Cols) > 0 {
				var colNames []string
				var placeholders []string
				var vals []interface{}
				var updateClauses []string
				for _, c := range row.Cols {
					colNames = append(colNames, fmt.Sprintf("`%s`", c.Name))
					placeholders = append(placeholders, "?")
					vals = append(vals, c.Value)
					updateClauses = append(updateClauses, fmt.Sprintf("`%s`=VALUES(`%s`)", c.Name, c.Name))
				}
				q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
					tbl, strings.Join(colNames, ", "), strings.Join(placeholders, ", "), strings.Join(updateClauses, ", "))
				if _, err := tx.ExecContext(ctx, q, vals...); err != nil {
					return err
				}
			} else if len(row.Key) > 0 {
				q := fmt.Sprintf("INSERT INTO %s (id) VALUES (?) ON DUPLICATE KEY UPDATE id=VALUES(id)", tbl)
				if _, err := tx.ExecContext(ctx, q, row.Key[0]); err != nil {
					return err
				}
			}
		case model.OpUpdate:
			if len(row.Cols) > 0 && len(row.Key) > 0 {
				var setClauses []string
				var vals []interface{}
				for _, c := range row.Cols {
					setClauses = append(setClauses, fmt.Sprintf("`%s`=?", c.Name))
					vals = append(vals, c.Value)
				}
				pk := row.Key[0]
				if len(row.OldKey) > 0 {
					pk = row.OldKey[0]
				}
				vals = append(vals, pk)
				q := fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", tbl, strings.Join(setClauses, ", "))
				if _, err := tx.ExecContext(ctx, q, vals...); err != nil {
					return err
				}
			}
		case model.OpDelete:
			if len(row.Key) > 0 {
				q := fmt.Sprintf("DELETE FROM %s WHERE id = ?", tbl)
				if _, err := tx.ExecContext(ctx, q, row.Key[0]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// PGWriter
// -----------------------------------------------------------------------------

// PGWriter cài đặt Writer cho PostgreSQL target.
type PGWriter struct {
	db         *sql.DB
	pipelineID string
	workerID   int
	epoch      int64
}

// NewPGWriter khởi tạo PGWriter mới.
func NewPGWriter(db *sql.DB, pipelineID string, workerID int, epoch int64) *PGWriter {
	return &PGWriter{
		db:         db,
		pipelineID: pipelineID,
		workerID:   workerID,
		epoch:      epoch,
	}
}

// EnsureWorkerRow đảm bảo row cho worker_id đã tồn tại trong _strand_worker_delta trên Postgres.
func (w *PGWriter) EnsureWorkerRow(ctx context.Context) error {
	query := `INSERT INTO _strand_worker_delta (pipeline_id, worker_id, epoch, batch_no, applied)
	VALUES ($1, $2, $3, 1, '')
	ON CONFLICT (pipeline_id, worker_id) DO UPDATE SET epoch = EXCLUDED.epoch`
	_, err := w.db.ExecContext(ctx, query, w.pipelineID, w.workerID, w.epoch)
	if err != nil {
		return fmt.Errorf("failed to ensure pg worker row: %w", err)
	}
	return nil
}

// ApplyBatch ghi dữ liệu của batch và cập nhật row delta trong cùng transaction trên Postgres.
func (w *PGWriter) ApplyBatch(ctx context.Context, batch []*model.Txn, delta DeltaRow) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Ghi dữ liệu vào các bảng ứng dụng
	for _, txn := range batch {
		if err := applyRowChangesPG(ctx, tx, txn.Rows); err != nil {
			return fmt.Errorf("failed to apply row changes: %w", err)
		}
	}

	// 2. Ghi row delta có điều kiện epoch trong CÙNG transaction
	deltaPipelineID := delta.PipelineID
	if deltaPipelineID == "" {
		deltaPipelineID = w.pipelineID
	}
	deltaWorkerID := delta.WorkerID
	if deltaWorkerID == 0 {
		deltaWorkerID = w.workerID
	}
	deltaEpoch := delta.Epoch
	if deltaEpoch == 0 {
		deltaEpoch = w.epoch
	}

	query := "UPDATE _strand_worker_delta SET applied = $1, batch_no = batch_no + 1 WHERE pipeline_id = $2 AND worker_id = $3 AND epoch = $4"
	res, err := tx.ExecContext(ctx, query, delta.Applied, deltaPipelineID, deltaWorkerID, deltaEpoch)
	if err != nil {
		return fmt.Errorf("failed to update pg worker delta: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if affected != 1 {
		return exitcode.New(exitcode.ErrFenced, exitcode.Fenced, fmt.Sprintf("writer fenced: expected 1 row affected, got %d", affected), nil)
	}

	// 3. Commit cả dữ liệu và delta
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	return nil
}

func applyRowChangesPG(ctx context.Context, tx *sql.Tx, rows []model.RowChange) error {
	for _, row := range rows {
		tbl := row.Table.String()
		switch row.Op {
		case model.OpInsert:
			if len(row.Cols) > 0 {
				var colNames []string
				var placeholders []string
				var vals []interface{}
				var updateClauses []string
				for i, c := range row.Cols {
					colNames = append(colNames, fmt.Sprintf("%q", c.Name))
					placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
					vals = append(vals, c.Value)
					updateClauses = append(updateClauses, fmt.Sprintf("%q = EXCLUDED.%q", c.Name, c.Name))
				}
				q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (id) DO UPDATE SET %s",
					tbl, strings.Join(colNames, ", "), strings.Join(placeholders, ", "), strings.Join(updateClauses, ", "))
				if _, err := tx.ExecContext(ctx, q, vals...); err != nil {
					return err
				}
			} else if len(row.Key) > 0 {
				q := fmt.Sprintf("INSERT INTO %s (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", tbl)
				if _, err := tx.ExecContext(ctx, q, row.Key[0]); err != nil {
					return err
				}
			}
		case model.OpUpdate:
			if len(row.Cols) > 0 && len(row.Key) > 0 {
				var setClauses []string
				var vals []interface{}
				idx := 1
				for _, c := range row.Cols {
					setClauses = append(setClauses, fmt.Sprintf("%q = $%d", c.Name, idx))
					vals = append(vals, c.Value)
					idx++
				}
				pk := row.Key[0]
				if len(row.OldKey) > 0 {
					pk = row.OldKey[0]
				}
				vals = append(vals, pk)
				q := fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", tbl, strings.Join(setClauses, ", "), idx)
				if _, err := tx.ExecContext(ctx, q, vals...); err != nil {
					return err
				}
			}
		case model.OpDelete:
			if len(row.Key) > 0 {
				q := fmt.Sprintf("DELETE FROM %s WHERE id = $1", tbl)
				if _, err := tx.ExecContext(ctx, q, row.Key[0]); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
