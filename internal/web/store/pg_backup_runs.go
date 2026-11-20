package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"strand/internal/backup"
)

func (p *PG) CreateBackupRun(ctx context.Context, r backup.Run) error {
	if r.ID == "" || r.JobID == "" {
		return errors.New("backup run id and job_id are required")
	}
	if r.StartedAt.IsZero() {
		r.StartedAt = time.Now().UTC()
	}
	if r.Attempt <= 0 {
		r.Attempt = 1
	}
	if r.Status == "" {
		r.Status = string(backup.RunStatusRunning)
	}
	query := `INSERT INTO backup_runs (id, job_id, trigger, started_at, finished_at, status,
		bytes_read, bytes_written, duration, backup_id, error_code, error_message, process_id, attempt)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`
	_, err := p.db.ExecContext(ctx, query,
		r.ID, r.JobID, r.Trigger, r.StartedAt, r.FinishedAt, r.Status,
		r.BytesRead, r.BytesWritten, int64(r.Duration), r.BackupID,
		r.ErrorCode, r.ErrorMessage, r.ProcessID, r.Attempt,
	)
	return err
}

func (p *PG) UpdateBackupRun(ctx context.Context, r backup.Run) (bool, error) {
	query := `UPDATE backup_runs SET
		finished_at = $1, status = $2, bytes_read = $3, bytes_written = $4,
		duration = $5, backup_id = $6, error_code = $7, error_message = $8,
		process_id = $9, attempt = $10
		WHERE id = $11`
	res, err := p.db.ExecContext(ctx, query,
		r.FinishedAt, r.Status, r.BytesRead, r.BytesWritten,
		int64(r.Duration), r.BackupID, r.ErrorCode, r.ErrorMessage,
		r.ProcessID, r.Attempt, r.ID,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (p *PG) FinishBackupRun(ctx context.Context, id string, finishedAt time.Time, status string, bytesRead, bytesWritten int64, duration time.Duration, backupID, errorCode, errorMessage string) (backup.Run, bool, error) {
	query := `UPDATE backup_runs SET
		finished_at = $1, status = $2, bytes_read = $3, bytes_written = $4,
		duration = $5, backup_id = $6, error_code = $7, error_message = $8
		WHERE id = $9
		RETURNING id, job_id, trigger, started_at, finished_at, status,
			bytes_read, bytes_written, duration, backup_id, error_code, error_message, process_id, attempt`
	var r backup.Run
	var dur int64
	err := p.db.QueryRowContext(ctx, query,
		finishedAt, status, bytesRead, bytesWritten,
		int64(duration), backupID, errorCode, errorMessage, id,
	).Scan(
		&r.ID, &r.JobID, &r.Trigger, &r.StartedAt, &r.FinishedAt, &r.Status,
		&r.BytesRead, &r.BytesWritten, &dur, &r.BackupID, &r.ErrorCode, &r.ErrorMessage, &r.ProcessID, &r.Attempt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return backup.Run{}, false, nil
	}
	if err != nil {
		return backup.Run{}, false, err
	}
	r.Duration = time.Duration(dur)
	return r, true, nil
}

func (p *PG) GetBackupRun(ctx context.Context, id string) (backup.Run, bool, error) {
	query := `SELECT id, job_id, trigger, started_at, finished_at, status,
		bytes_read, bytes_written, duration, backup_id, error_code, error_message, process_id, attempt
		FROM backup_runs WHERE id = $1`
	var r backup.Run
	var dur int64
	err := p.db.QueryRowContext(ctx, query, id).Scan(
		&r.ID, &r.JobID, &r.Trigger, &r.StartedAt, &r.FinishedAt, &r.Status,
		&r.BytesRead, &r.BytesWritten, &dur, &r.BackupID, &r.ErrorCode, &r.ErrorMessage, &r.ProcessID, &r.Attempt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return backup.Run{}, false, nil
	}
	if err != nil {
		return backup.Run{}, false, err
	}
	r.Duration = time.Duration(dur)
	return r, true, nil
}

func (p *PG) ListBackupRuns(ctx context.Context, f BackupRunFilter) ([]backup.Run, error) {
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var (
		query string
		args  []any
	)
	if f.JobID != "" {
		if f.After != nil {
			query = `SELECT id, job_id, trigger, started_at, finished_at, status,
				bytes_read, bytes_written, duration, backup_id, error_code, error_message, process_id, attempt
				FROM backup_runs
				WHERE job_id = $1 AND (started_at < $2 OR (started_at = $2 AND id < $3))
				ORDER BY started_at DESC, id DESC
				LIMIT $4`
			args = []any{f.JobID, f.After.CreatedAt, f.After.ID, limit}
		} else {
			query = `SELECT id, job_id, trigger, started_at, finished_at, status,
				bytes_read, bytes_written, duration, backup_id, error_code, error_message, process_id, attempt
				FROM backup_runs
				WHERE job_id = $1
				ORDER BY started_at DESC, id DESC
				LIMIT $2`
			args = []any{f.JobID, limit}
		}
	} else {
		if f.After != nil {
			query = `SELECT id, job_id, trigger, started_at, finished_at, status,
				bytes_read, bytes_written, duration, backup_id, error_code, error_message, process_id, attempt
				FROM backup_runs
				WHERE (started_at < $1 OR (started_at = $1 AND id < $2))
				ORDER BY started_at DESC, id DESC
				LIMIT $3`
			args = []any{f.After.CreatedAt, f.After.ID, limit}
		} else {
			query = `SELECT id, job_id, trigger, started_at, finished_at, status,
				bytes_read, bytes_written, duration, backup_id, error_code, error_message, process_id, attempt
				FROM backup_runs
				ORDER BY started_at DESC, id DESC
				LIMIT $1`
			args = []any{limit}
		}
	}
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backup.Run
	for rows.Next() {
		var r backup.Run
		var dur int64
		if err := rows.Scan(
			&r.ID, &r.JobID, &r.Trigger, &r.StartedAt, &r.FinishedAt, &r.Status,
			&r.BytesRead, &r.BytesWritten, &dur, &r.BackupID, &r.ErrorCode, &r.ErrorMessage, &r.ProcessID, &r.Attempt,
		); err != nil {
			return nil, err
		}
		r.Duration = time.Duration(dur)
		out = append(out, r)
	}
	return out, rows.Err()
}
