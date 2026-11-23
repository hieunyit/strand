package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"strand/internal/backup"
)

// BackupProStore: managed backups (metadata updates, retention expiry, legal hold, verification results,
// repositories, notification bookkeeping). Implemented by PG and Mem.
type BackupProStore interface {
	UpdateBackupMeta(ctx context.Context, id string, m backup.Meta) (backup.Record, bool, error)
	ClaimBackupDeletion(ctx context.Context, id string, at time.Time) (backup.Record, bool, error)
	ExpireBackup(ctx context.Context, id string, at time.Time) (backup.Record, bool, error)
	SetBackupLegalHold(ctx context.Context, id string, hold bool) (backup.Record, bool, error)
	// DeleteBackup removes a record that is not complete (see backup.Repository.DeleteBackup).
	DeleteBackup(ctx context.Context, id string, now time.Time) (backup.Record, bool, error)
	AddBackupCheck(ctx context.Context, c backup.CheckResult) (int64, error)
	ListBackupChecks(ctx context.Context, backupID string, limit int) ([]backup.CheckResult, error)
	BackupSummaries(ctx context.Context) ([]backup.SourceSummary, error)

	CreateBackupRepo(ctx context.Context, r backup.Repo) error
	GetBackupRepo(ctx context.Context, id string) (backup.Repo, bool, error)
	ListBackupRepos(ctx context.Context) ([]backup.Repo, error)
	UpdateBackupRepo(ctx context.Context, r backup.Repo) (bool, error)
	DeleteBackupRepo(ctx context.Context, id string) (bool, error)
	// SetBackupRepoTest records the last connectivity test (status ok|warning|failed, JSON checklist).
	SetBackupRepoTest(ctx context.Context, id string, at time.Time, status, detail string, steps []byte) (bool, error)

	// MarkBackupJobNotified claims the notification of the run that finished at runFinishedAt (true once).
	MarkBackupJobNotified(ctx context.Context, id string, runFinishedAt time.Time) (bool, error)
	// MarkBackupJobMissed claims the missed-schedule notification for the due time dueAt (true once).
	MarkBackupJobMissed(ctx context.Context, id string, dueAt time.Time) (bool, error)
	// ClaimBackupJobVerify claims a verification run when the previous one is older than every.
	ClaimBackupJobVerify(ctx context.Context, id string, now time.Time, every time.Duration) (bool, error)
	// ClaimBackupJobPrune claims a retention run when the previous one is older than every.
	ClaimBackupJobPrune(ctx context.Context, id string, now time.Time, every time.Duration) (bool, error)
}

const backupProCols = `, job_id, repository, method, kind, parent_id, copy_of, format, compression, key_id,
	plain_bytes, plain_sha256, oplog_ts, end_position, start_time, end_time, version_id, lock_mode, retain_until,
	checked_at, check_status, check_detail, expired_at, details`

const backupProColCount = 23

func backupProPlaceholders(first int) string {
	parts := make([]string, backupProColCount)
	for i := range parts {
		parts[i] = fmt.Sprintf("$%d", first+i)
	}
	return strings.Join(parts, ",")
}

func kindOrFull(k string) string {
	if k == "" {
		return backup.KindFull
	}
	return k
}

func formatOrRaw(f string) string {
	if f == "" {
		return backup.FormatRaw
	}
	return f
}

func backupProArgs(r backup.Record) ([]any, error) {
	endPos, err := json.Marshal(r.EndPosition)
	if err != nil {
		return nil, err
	}
	details := []byte("{}")
	if len(r.Details) > 0 {
		if details, err = json.Marshal(r.Details); err != nil {
			return nil, err
		}
	}
	return []any{r.JobID, r.Repository, r.Method, kindOrFull(r.Kind), r.ParentID, r.CopyOf, formatOrRaw(r.Format),
		r.Compression, r.KeyID, r.PlainBytes, strings.ToLower(r.PlainSHA256), r.Position.OplogTS, endPos, r.StartTime,
		r.EndTime, r.VersionID, r.LockMode, r.RetainUntil, r.CheckedAt, r.CheckStatus, r.CheckDetail, r.ExpiredAt,
		details}, nil
}

type backupProScan struct {
	r                                    *backup.Record
	endPos, details                      []byte
	start, end, retain, checked, expired sql.NullTime
}

func newBackupProScan(r *backup.Record) *backupProScan { return &backupProScan{r: r} }

func (s *backupProScan) dest() []any {
	r := s.r
	return []any{&r.JobID, &r.Repository, &r.Method, &r.Kind, &r.ParentID, &r.CopyOf, &r.Format, &r.Compression,
		&r.KeyID, &r.PlainBytes, &r.PlainSHA256, &r.Position.OplogTS, &s.endPos, &s.start, &s.end, &r.VersionID,
		&r.LockMode, &s.retain, &s.checked, &r.CheckStatus, &r.CheckDetail, &s.expired, &s.details}
}

func (s *backupProScan) finish() error {
	r := s.r
	opt := func(t sql.NullTime) *time.Time {
		if !t.Valid {
			return nil
		}
		v := t.Time
		return &v
	}
	r.StartTime, r.EndTime, r.RetainUntil, r.CheckedAt, r.ExpiredAt = opt(s.start), opt(s.end), opt(s.retain), opt(s.checked), opt(s.expired)
	if len(s.endPos) > 0 {
		if err := json.Unmarshal(s.endPos, &r.EndPosition); err != nil {
			return err
		}
	}
	if len(s.details) > 2 {
		if err := json.Unmarshal(s.details, &r.Details); err != nil {
			return err
		}
	}
	return nil
}

func encodeJobOptions(o backup.JobOptions) ([]byte, error) { return json.Marshal(o) }

func decodeJobOptions(b []byte, o *backup.JobOptions) error {
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, o)
}

func (p *PG) UpdateBackupMeta(ctx context.Context, id string, m backup.Meta) (backup.Record, bool, error) {
	cur, ok, err := p.GetBackup(ctx, id)
	if err != nil || !ok {
		return cur, ok, err
	}
	if err := backup.ValidateMeta(cur.Engine, m); err != nil {
		return backup.Record{}, false, err
	}
	next := cur
	next.ApplyMeta(m)
	args, err := backupProArgs(next)
	if err != nil {
		return backup.Record{}, false, err
	}
	res, err := p.db.ExecContext(ctx, `UPDATE backups SET gtid_set=$2, lsn=$3, binlog_file=$4, binlog_pos=$5,
		format=$6, compression=$7, key_id=$8, plain_bytes=$9, plain_sha256=$10, oplog_ts=$11, end_position=$12,
		start_time=$13, end_time=$14, version_id=$15, lock_mode=$16, retain_until=$17, details=$18
		WHERE id=$1 AND status IN ('uploading','verifying')`, id, next.Position.GTIDSet, next.Position.LSN,
		next.Position.BinlogFile, next.Position.BinlogPos, args[6], args[7], args[8], args[9], args[10], args[11],
		args[12], args[13], args[14], args[15], args[16], args[17], args[22])
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return backup.Record{}, false, fmt.Errorf("%w: metadata can change only while uploading or verifying", backup.ErrTransition)
	}
	return p.GetBackup(ctx, id)
}

func (p *PG) ClaimBackupDeletion(ctx context.Context, id string, at time.Time) (backup.Record, bool, error) {
	_, err := p.db.ExecContext(ctx, `UPDATE backups SET status='deleting'
		WHERE id=$1 AND status='complete' AND NOT legal_hold AND (retain_until IS NULL OR retain_until <= $2)`, id, at)
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	cur, ok, err := p.GetBackup(ctx, id)
	if err != nil || !ok {
		return cur, false, err
	}
	if cur.Status == backup.StatusDeleting {
		return cur, true, nil
	}
	if cur.Status == backup.StatusExpired {
		return cur, false, nil
	}
	return cur, false, fmt.Errorf("%w: backup is not eligible for deletion (status, legal hold or retention lock)", backup.ErrInvalid)
}

func (p *PG) ExpireBackup(ctx context.Context, id string, at time.Time) (backup.Record, bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backups SET status='expired', expired_at=$2
		WHERE id=$1 AND status IN ('complete', 'deleting') AND NOT legal_hold AND (retain_until IS NULL OR retain_until <= $2)`, id, at)
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	cur, ok, err := p.GetBackup(ctx, id)
	if err != nil || !ok || n == 1 {
		return cur, n == 1, err
	}
	switch {
	case cur.Status == backup.StatusExpired:
		return cur, false, nil
	case cur.LegalHold:
		return backup.Record{}, false, fmt.Errorf("%w: legal-hold backup cannot be expired", backup.ErrInvalid)
	case cur.RetainUntil != nil && cur.RetainUntil.After(at):
		return backup.Record{}, false, fmt.Errorf("%w: backup is object-locked until %s", backup.ErrInvalid, cur.RetainUntil.UTC().Format(time.RFC3339))
	}
	return backup.Record{}, false, backup.ErrTransition
}

func (p *PG) SetBackupLegalHold(ctx context.Context, id string, hold bool) (backup.Record, bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backups SET legal_hold=$2 WHERE id=$1 AND status NOT IN ('expired', 'deleting') AND legal_hold <> $2`, id, hold)
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	cur, ok, err := p.GetBackup(ctx, id)
	if err != nil || !ok {
		return cur, ok, err
	}
	if n == 0 && (cur.Status == backup.StatusExpired || cur.Status == backup.StatusDeleting) {
		return backup.Record{}, false, fmt.Errorf("%w: a backup pending deletion or expired cannot have its hold changed", backup.ErrInvalid)
	}
	return cur, n == 1, nil
}

func (p *PG) DeleteBackup(ctx context.Context, id string, now time.Time) (backup.Record, bool, error) {
	cur, ok, err := p.GetBackup(ctx, id)
	if err != nil || !ok {
		return backup.Record{}, false, err
	}
	if err := backup.DeletableErr(cur, now); err != nil {
		return backup.Record{}, false, err
	}
	// The same guards in the statement: a concurrent finalize, hold or lock wins (checks cascade).
	res, err := p.db.ExecContext(ctx, `DELETE FROM backups WHERE id=$1 AND status NOT IN ('complete', 'deleting') AND NOT legal_hold
		AND (retain_until IS NULL OR retain_until <= $2)`, id, now)
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		again, ok, err := p.GetBackup(ctx, id)
		if err != nil || !ok {
			return backup.Record{}, false, err
		}
		if err := backup.DeletableErr(again, now); err != nil {
			return backup.Record{}, false, err
		}
		return backup.Record{}, false, fmt.Errorf("%w: restore point %s changed while it was being deleted", backup.ErrConflict, id)
	}
	return cur, true, nil
}

func (p *PG) AddBackupCheck(ctx context.Context, c backup.CheckResult) (int64, error) {
	metrics, err := json.Marshal(c.Metrics)
	if err != nil {
		return 0, err
	}
	if c.Metrics == nil {
		metrics = []byte("{}")
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, p.wrap(err)
	}
	defer tx.Rollback()
	var id int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO backup_checks (backup_id, kind, status, started_at, finished_at, detail, metrics)
		VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`, c.BackupID, c.Kind, c.Status, c.StartedAt, c.FinishedAt, c.Detail, metrics).Scan(&id); err != nil {
		return 0, p.wrap(err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backups SET checked_at=$2, check_status=$3, check_detail=$4 WHERE id=$1`,
		c.BackupID, c.FinishedAt, c.Status, truncate(c.Detail, 2000)); err != nil {
		return 0, p.wrap(err)
	}
	return id, p.wrap(tx.Commit())
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func (p *PG) ListBackupChecks(ctx context.Context, backupID string, limit int) ([]backup.CheckResult, error) {
	q := `SELECT id, backup_id, kind, status, started_at, finished_at, detail, metrics FROM backup_checks`
	args := []any{limit}
	if backupID != "" {
		q += ` WHERE backup_id = $2`
		args = append(args, backupID)
	}
	rows, err := p.db.QueryContext(ctx, q+` ORDER BY id DESC LIMIT $1`, args...)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := []backup.CheckResult{}
	for rows.Next() {
		var c backup.CheckResult
		var metrics []byte
		if err := rows.Scan(&c.ID, &c.BackupID, &c.Kind, &c.Status, &c.StartedAt, &c.FinishedAt, &c.Detail, &metrics); err != nil {
			return nil, p.wrap(err)
		}
		if len(metrics) > 2 {
			_ = json.Unmarshal(metrics, &c.Metrics)
		}
		out = append(out, c)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) BackupSummaries(ctx context.Context) ([]backup.SourceSummary, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT source, min(engine), max(job_id),
		count(*) FILTER (WHERE status='complete' AND kind<>'log'),
		COALESCE(sum(bytes) FILTER (WHERE status='complete'), 0),
		max(created_at) FILTER (WHERE status='failed'),
		min(COALESCE(start_time, created_at)) FILTER (WHERE status='complete' AND kind='log'),
		max(COALESCE(end_time, created_at)) FILTER (WHERE status='complete' AND kind='log'),
		count(*) FILTER (WHERE status='complete' AND kind='log')
		FROM backups GROUP BY source ORDER BY source`)
	if err != nil {
		return nil, p.wrap(err)
	}
	var out []backup.SourceSummary
	idx := map[string]int{}
	for rows.Next() {
		var s backup.SourceSummary
		var failed, ls, le sql.NullTime
		if err := rows.Scan(&s.Source, &s.Engine, &s.JobID, &s.Complete, &s.TotalBytes, &failed, &ls, &le, &s.LogSegments); err != nil {
			rows.Close()
			return nil, p.wrap(err)
		}
		if failed.Valid {
			t := failed.Time
			s.LastFailedAt = &t
		}
		if ls.Valid {
			t := ls.Time
			s.LogStart = &t
		}
		if le.Valid {
			t := le.Time
			s.LogEnd = &t
		}
		idx[s.Source] = len(out)
		out = append(out, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, p.wrap(err)
	}
	// Latest restore points per source (last success + size trend).
	rows, err = p.db.QueryContext(ctx, `SELECT `+backupCols+` FROM (
		SELECT b.*, row_number() OVER (PARTITION BY source ORDER BY created_at DESC, id DESC) AS rn
		FROM backups b WHERE status='complete' AND kind <> 'log') x WHERE rn <= 10 ORDER BY source, created_at, id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	for rows.Next() {
		r, err := scanBackup(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		i, ok := idx[r.Source]
		if !ok {
			continue
		}
		rc := r
		out[i].LastSuccess = &rc
		out[i].RecentSizes = append(out[i].RecentSizes, r.Bytes)
	}
	return out, p.wrap(rows.Err())
}

const repoCols = `id, name, kind, secret_mode, env_prefix, endpoint, region, allow_insecure_local, access_key_sealed, secret_key_sealed,
	bucket, prefix, lock_mode, lock_days, retention, created_at, updated_at, last_test_at, last_test_status, last_test_detail, last_test_steps`

func scanRepo(sc interface{ Scan(...any) error }) (backup.Repo, error) {
	var r backup.Repo
	var ret []byte
	var testAt sql.NullTime
	err := sc.Scan(&r.ID, &r.Name, &r.Kind, &r.SecretMode, &r.EnvPrefix, &r.Endpoint, &r.Region, &r.AllowInsecureLocal,
		&r.AccessKeySealed, &r.SecretKeySealed, &r.Bucket, &r.Prefix, &r.LockMode, &r.LockDays, &ret, &r.CreatedAt, &r.UpdatedAt,
		&testAt, &r.LastTestStatus, &r.LastTestDetail, &r.LastTestSteps)
	if err == nil && len(ret) > 0 {
		err = json.Unmarshal(ret, &r.Retention)
	}
	if testAt.Valid {
		t := testAt.Time
		r.LastTestAt = &t
	}
	r.AccessKeySet, r.SecretKeySet = len(r.AccessKeySealed) > 0, len(r.SecretKeySealed) > 0
	if len(r.AccessKeySealed) == 0 {
		r.AccessKeySealed = nil
	}
	if len(r.SecretKeySealed) == 0 {
		r.SecretKeySealed = nil
	}
	return r, err
}

func (p *PG) CreateBackupRepo(ctx context.Context, r backup.Repo) error {
	if err := backup.ValidateRepo(r); err != nil {
		return err
	}
	ret, _ := json.Marshal(r.Retention)
	_, err := p.db.ExecContext(ctx, `INSERT INTO backup_repositories (id, name, kind, secret_mode, env_prefix, endpoint, region,
		allow_insecure_local, access_key_sealed, secret_key_sealed, bucket, prefix, lock_mode, lock_days, retention)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, r.ID, r.Name, r.KindOrS3(), secretModeOrEnv(r.SecretMode),
		r.EnvPrefix, r.Endpoint, r.Region, r.AllowInsecureLocal, r.AccessKeySealed, r.SecretKeySealed, r.Bucket, r.Prefix,
		r.LockMode, r.LockDays, ret)
	if err != nil && strings.Contains(err.Error(), "duplicate key") {
		return backup.ErrConflict
	}
	return p.wrap(err)
}

func secretModeOrEnv(m string) string {
	if m == "" {
		return backup.RepoSecretEnv
	}
	return m
}

func (p *PG) GetBackupRepo(ctx context.Context, id string) (backup.Repo, bool, error) {
	r, err := scanRepo(p.db.QueryRowContext(ctx, `SELECT `+repoCols+` FROM backup_repositories WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return backup.Repo{}, false, nil
	}
	return r, err == nil, p.wrap(err)
}

func (p *PG) ListBackupRepos(ctx context.Context) ([]backup.Repo, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT `+repoCols+` FROM backup_repositories ORDER BY name, id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := []backup.Repo{}
	for rows.Next() {
		r, err := scanRepo(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, r)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) UpdateBackupRepo(ctx context.Context, r backup.Repo) (bool, error) {
	if err := backup.ValidateRepo(r); err != nil {
		return false, err
	}
	ret, _ := json.Marshal(r.Retention)
	res, err := p.db.ExecContext(ctx, `UPDATE backup_repositories SET name=$2, kind=$3, secret_mode=$4, env_prefix=$5, endpoint=$6,
		region=$7, allow_insecure_local=$8, access_key_sealed=$9, secret_key_sealed=$10, bucket=$11, prefix=$12,
		lock_mode=$13, lock_days=$14, retention=$15, updated_at=now() WHERE id=$1`, r.ID, r.Name, r.KindOrS3(),
		secretModeOrEnv(r.SecretMode), r.EnvPrefix, r.Endpoint, r.Region, r.AllowInsecureLocal, r.AccessKeySealed,
		r.SecretKeySealed, r.Bucket, r.Prefix, r.LockMode, r.LockDays, ret)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return false, backup.ErrConflict
		}
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) SetBackupRepoTest(ctx context.Context, id string, at time.Time, status, detail string, steps []byte) (bool, error) {
	if len(steps) == 0 {
		steps = []byte("[]")
	}
	res, err := p.db.ExecContext(ctx, `UPDATE backup_repositories SET last_test_at=$2, last_test_status=$3, last_test_detail=$4,
		last_test_steps=$5::jsonb WHERE id=$1`, id, at, status, detail, string(steps))
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) DeleteBackupRepo(ctx context.Context, id string) (bool, error) {
	var used int
	if err := p.db.QueryRowContext(ctx, `SELECT count(*) FROM backups WHERE repository=$1 AND status <> 'expired'`, id).Scan(&used); err != nil {
		return false, p.wrap(err)
	}
	if used > 0 {
		return false, fmt.Errorf("%w: repository still holds %d backups", backup.ErrInvalid, used)
	}
	res, err := p.db.ExecContext(ctx, `DELETE FROM backup_repositories WHERE id=$1`, id)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) MarkBackupJobNotified(ctx context.Context, id string, runFinishedAt time.Time) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET notified_run_at=$2 WHERE id=$1 AND run_finished_at=$2
		AND notified_run_at IS DISTINCT FROM $2`, id, runFinishedAt)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) MarkBackupJobMissed(ctx context.Context, id string, dueAt time.Time) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET missed_notified_at=$2 WHERE id=$1 AND next_run_at=$2
		AND missed_notified_at IS DISTINCT FROM $2`, id, dueAt)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) ClaimBackupJobVerify(ctx context.Context, id string, now time.Time, every time.Duration) (bool, error) {
	return p.claimJobColumn(ctx, "last_verify_at", id, now, every)
}

func (p *PG) ClaimBackupJobPrune(ctx context.Context, id string, now time.Time, every time.Duration) (bool, error) {
	return p.claimJobColumn(ctx, "last_prune_at", id, now, every)
}

func (p *PG) claimJobColumn(ctx context.Context, col, id string, now time.Time, every time.Duration) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET `+col+`=$2 WHERE id=$1 AND (`+col+` IS NULL OR `+col+` <= $3)`,
		id, now, now.Add(-every))
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}
