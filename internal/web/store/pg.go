package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // driver "pgx"

	"strand/internal/backup"
)

// PG is the PostgreSQL implementation of Store.
type PG struct {
	db  *sql.DB
	dsn string
}

// OpenOptions tune Open.
type OpenOptions struct {
	Logger *slog.Logger
	// MaxWait bounds the connect retries (default 60 s; compose starts web and db together).
	MaxWait time.Duration
	// NoMigrate skips applying migrations (tests of Migrate itself).
	NoMigrate bool
}

// Open connects to PostgreSQL (retrying with exponential backoff 0.5 s .. 5 s until MaxWait) and applies
// pending migrations under an advisory lock. Errors never contain the DSN password.
func Open(ctx context.Context, dsn string, o OpenOptions) (*PG, error) {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.MaxWait <= 0 {
		o.MaxWait = 60 * time.Second
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, RedactError(err, dsn)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)
	deadline := time.Now().Add(o.MaxWait)
	delay := 500 * time.Millisecond
	for {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = db.PingContext(pctx)
		cancel()
		if err == nil {
			break
		}
		if ctx.Err() != nil || time.Now().Add(delay).After(deadline) {
			_ = db.Close()
			return nil, fmt.Errorf("connect to metadata database: %w", RedactError(err, dsn))
		}
		o.Logger.Warn("metadata database not ready, retrying", "err", RedactError(err, dsn).Error(), "retry_in", delay.String())
		select {
		case <-ctx.Done():
			_ = db.Close()
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		if delay < 5*time.Second {
			delay *= 2
		}
	}
	p := &PG{db: db, dsn: dsn}
	if !o.NoMigrate {
		applied, err := Migrate(ctx, db)
		if err != nil {
			_ = db.Close()
			return nil, RedactError(err, dsn)
		}
		if len(applied) > 0 {
			o.Logger.Info("metadata database migrated", "applied", applied)
		}
	}
	return p, nil
}

// DB exposes the pool (tests).
func (p *PG) DB() *sql.DB { return p.db }

func (p *PG) Ping(ctx context.Context) error {
	return RedactError(p.db.PingContext(ctx), p.dsn)
}
func (p *PG) Close() error { return p.db.Close() }

func (p *PG) wrap(err error) error { return RedactError(err, p.dsn) }

func lockKey(name string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte("strand-web:" + name))
	return int64(h.Sum64())
}

// withAdvisoryLock holds pg_advisory_lock(key) on a dedicated connection while fn runs.
func withAdvisoryLock(ctx context.Context, db *sql.DB, name string, fn func(ctx context.Context) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	k := lockKey(name)
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, k); err != nil {
		return err
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, _ = conn.ExecContext(uctx, `SELECT pg_advisory_unlock($1)`, k)
	}()
	// Session advisory locks are released automatically when PostgreSQL drops
	// their connection. Monitor the owning session and cancel callback work on
	// disconnect, or another Web replica could take the lock while a backup keeps
	// running without coordination.
	lockCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn(lockCtx) }()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			cancel()
			return <-done
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			err := conn.PingContext(pingCtx)
			pingCancel()
			if err != nil {
				cancel()
				callbackErr := <-done
				return errors.Join(fmt.Errorf("advisory lock connection lost: %w", err), callbackErr)
			}
		}
	}
}

func (p *PG) Locked(ctx context.Context, key string, fn func(ctx context.Context) error) error {
	return p.wrap(withAdvisoryLock(ctx, p.db, key, fn))
}

func (p *PG) Admin(ctx context.Context) (Admin, bool, error) {
	var a Admin
	err := p.db.QueryRowContext(ctx, `SELECT password_hash, api_token_hash FROM admin WHERE id = 1`).Scan(&a.PasswordHash, &a.APITokenHash)
	if errors.Is(err, sql.ErrNoRows) {
		return Admin{}, false, nil
	}
	return a, err == nil, p.wrap(err)
}

func (p *PG) SetPasswordHash(ctx context.Context, h string) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO admin (id, password_hash) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET password_hash = EXCLUDED.password_hash, updated_at = now()`, h)
	return p.wrap(err)
}

func (p *PG) SetAPITokenHash(ctx context.Context, h string) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO admin (id, api_token_hash) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET api_token_hash = EXCLUDED.api_token_hash, updated_at = now()`, h)
	return p.wrap(err)
}

func (p *PG) PutSession(ctx context.Context, s Session) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO sessions (id_hash, csrf, ip, created_at, last_seen) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (id_hash) DO UPDATE SET csrf = EXCLUDED.csrf, ip = EXCLUDED.ip, created_at = EXCLUDED.created_at, last_seen = EXCLUDED.last_seen`,
		s.IDHash, s.CSRF, s.IP, s.Created, s.LastSeen)
	return p.wrap(err)
}

func (p *PG) GetSession(ctx context.Context, id string) (Session, bool, error) {
	s := Session{IDHash: id}
	err := p.db.QueryRowContext(ctx, `SELECT csrf, ip, created_at, last_seen FROM sessions WHERE id_hash = $1`, id).Scan(&s.CSRF, &s.IP, &s.Created, &s.LastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, false, nil
	}
	return s, err == nil, p.wrap(err)
}

func (p *PG) TouchSession(ctx context.Context, id string, t time.Time) error {
	_, err := p.db.ExecContext(ctx, `UPDATE sessions SET last_seen = $2 WHERE id_hash = $1`, id, t)
	return p.wrap(err)
}

func (p *PG) DeleteSession(ctx context.Context, id string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM sessions WHERE id_hash = $1`, id)
	return p.wrap(err)
}

func (p *PG) PurgeSessions(ctx context.Context, idleBefore, createdBefore time.Time) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM sessions WHERE last_seen < $1 OR created_at < $2`, idleBefore, createdBefore)
	return p.wrap(err)
}

func (p *PG) CountSessions(ctx context.Context) (int, error) {
	var n int
	err := p.db.QueryRowContext(ctx, `SELECT count(*) FROM sessions`).Scan(&n)
	return n, p.wrap(err)
}

const pipelineCols = `name, config_path, env_path, systemd_unit, log_file, pid_file, config_yaml, env_enc, created_at, updated_at`

func scanPipeline(sc interface{ Scan(...any) error }) (Pipeline, error) {
	var p Pipeline
	var y sql.NullString
	err := sc.Scan(&p.Name, &p.ConfigPath, &p.EnvPath, &p.SystemdUnit, &p.LogFile, &p.PidFile, &y, &p.EnvEnc, &p.Created, &p.Updated)
	p.ConfigYAML, p.HasConfig = y.String, y.Valid
	return p, err
}

func (p *PG) UpsertPipeline(ctx context.Context, r Pipeline) error {
	var y any
	if r.HasConfig {
		y = r.ConfigYAML
	}
	_, err := p.db.ExecContext(ctx, `INSERT INTO pipelines (`+pipelineCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8, now(), now())
		ON CONFLICT (name) DO UPDATE SET config_path = EXCLUDED.config_path, env_path = EXCLUDED.env_path,
		  systemd_unit = EXCLUDED.systemd_unit, log_file = EXCLUDED.log_file, pid_file = EXCLUDED.pid_file,
		  config_yaml = EXCLUDED.config_yaml, env_enc = EXCLUDED.env_enc, updated_at = now()`,
		r.Name, r.ConfigPath, r.EnvPath, r.SystemdUnit, r.LogFile, r.PidFile, y, r.EnvEnc)
	return p.wrap(err)
}

func (p *PG) GetPipeline(ctx context.Context, name string) (Pipeline, bool, error) {
	r, err := scanPipeline(p.db.QueryRowContext(ctx, `SELECT `+pipelineCols+` FROM pipelines WHERE name = $1`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return Pipeline{}, false, nil
	}
	return r, err == nil, p.wrap(err)
}

func (p *PG) ListPipelines(ctx context.Context) ([]Pipeline, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT `+pipelineCols+` FROM pipelines ORDER BY name`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []Pipeline
	for rows.Next() {
		r, err := scanPipeline(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, r)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) DeletePipeline(ctx context.Context, name string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM pipelines WHERE name = $1`, name)
	if err != nil {
		return p.wrap(err)
	}
	_, _ = p.db.ExecContext(ctx, `DELETE FROM pipeline_desired WHERE name = $1`, name)
	_, _ = p.db.ExecContext(ctx, `DELETE FROM wizards WHERE pipeline = $1`, name)
	return nil
}

func (p *PG) SetDesired(ctx context.Context, name, desired string) error {
	if desired != DesiredRunning && desired != DesiredStopped {
		return fmt.Errorf("invalid desired state %q", desired)
	}
	_, err := p.db.ExecContext(ctx, `INSERT INTO pipeline_desired (name, desired) VALUES ($1,$2)
		ON CONFLICT (name) DO UPDATE SET desired = EXCLUDED.desired, updated_at = now()`, name, desired)
	return p.wrap(err)
}

func (p *PG) Desired(ctx context.Context) (map[string]string, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT name, desired FROM pipeline_desired`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	m := map[string]string{}
	for rows.Next() {
		var n, d string
		if err := rows.Scan(&n, &d); err != nil {
			return nil, p.wrap(err)
		}
		m[n] = d
	}
	return m, p.wrap(rows.Err())
}

func (p *PG) UpsertJob(ctx context.Context, j Job) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO jobs (id, pipeline, started_at, running, data) VALUES ($1,$2,$3,$4,$5::jsonb)
		ON CONFLICT (id) DO UPDATE SET running = EXCLUDED.running, data = EXCLUDED.data`,
		j.ID, j.Pipeline, j.Started, j.Running, string(j.Data))
	return p.wrap(err)
}

func (p *PG) ListJobs(ctx context.Context) ([]Job, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, pipeline, started_at, running, data::text FROM jobs ORDER BY started_at DESC`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		var d string
		if err := rows.Scan(&j.ID, &j.Pipeline, &j.Started, &j.Running, &d); err != nil {
			return nil, p.wrap(err)
		}
		j.Data = []byte(d)
		out = append(out, j)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) DeleteJob(ctx context.Context, id string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = $1`, id)
	return p.wrap(err)
}

func (p *PG) GetWizard(ctx context.Context, kind, pipeline string) ([]byte, bool, error) {
	var d string
	err := p.db.QueryRowContext(ctx, `SELECT data::text FROM wizards WHERE kind = $1 AND pipeline = $2`, kind, pipeline).Scan(&d)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	return []byte(d), err == nil, p.wrap(err)
}

func (p *PG) PutWizard(ctx context.Context, kind, pipeline string, data []byte) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO wizards (kind, pipeline, data) VALUES ($1,$2,$3::jsonb)
		ON CONFLICT (kind, pipeline) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`, kind, pipeline, string(data))
	return p.wrap(err)
}

func (p *PG) AppendAudit(ctx context.Context, r AuditRow) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO audit_log (ts, ip, action, pipeline, result, data) VALUES ($1,$2,$3,$4,$5,$6::jsonb)`,
		r.Time, r.IP, r.Action, r.Pipeline, r.Result, string(r.Data))
	return p.wrap(err)
}

func (p *PG) AppendAuditBatch(ctx context.Context, rows []AuditRow) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return p.wrap(err)
	}
	defer tx.Rollback() //nolint:errcheck
	st, err := tx.PrepareContext(ctx, `INSERT INTO audit_log (ts, ip, action, pipeline, result, data) VALUES ($1,$2,$3,$4,$5,$6::jsonb)`)
	if err != nil {
		return p.wrap(err)
	}
	defer st.Close()
	for _, r := range rows {
		if _, err := st.ExecContext(ctx, r.Time, r.IP, r.Action, r.Pipeline, r.Result, string(r.Data)); err != nil {
			return p.wrap(err)
		}
	}
	return p.wrap(tx.Commit())
}

func (p *PG) ListAudit(ctx context.Context, n int) ([]AuditRow, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT id, ts, ip, action, pipeline, result, data::text FROM audit_log ORDER BY id DESC LIMIT $1`, n)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []AuditRow
	for rows.Next() {
		var r AuditRow
		var d string
		if err := rows.Scan(&r.ID, &r.Time, &r.IP, &r.Action, &r.Pipeline, &r.Result, &d); err != nil {
			return nil, p.wrap(err)
		}
		r.Data = []byte(d)
		out = append(out, r)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) MetaGet(ctx context.Context, key string) (string, bool, error) {
	var v string
	err := p.db.QueryRowContext(ctx, `SELECT value FROM web_meta WHERE key = $1`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v, err == nil, p.wrap(err)
}

func (p *PG) MetaSet(ctx context.Context, key, value string) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO web_meta (key, value) VALUES ($1,$2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, key, value)
	return p.wrap(err)
}

const backupCols = `id, source, engine, tool, tool_version, bucket, object_key, bytes, sha256,
	gtid_set, lsn, binlog_file, binlog_pos, manifest_uri, status, created_at, verified_at, expires_at, legal_hold`

func scanBackup(sc interface{ Scan(...any) error }) (backup.Record, error) {
	var r backup.Record
	var verified, expires sql.NullTime
	err := sc.Scan(&r.ID, &r.Source, &r.Engine, &r.Tool, &r.ToolVersion, &r.Bucket, &r.ObjectKey, &r.Bytes,
		&r.SHA256, &r.Position.GTIDSet, &r.Position.LSN, &r.Position.BinlogFile, &r.Position.BinlogPos,
		&r.ManifestURI, &r.Status, &r.CreatedAt, &verified, &expires, &r.LegalHold)
	if verified.Valid {
		t := verified.Time
		r.VerifiedAt = &t
	}
	if expires.Valid {
		t := expires.Time
		r.ExpiresAt = &t
	}
	return r, err
}

func (p *PG) CreateBackup(ctx context.Context, r backup.Record) (backup.Record, bool, error) {
	r.SHA256 = strings.ToLower(r.SHA256)
	if err := backup.ValidateCreate(r); err != nil {
		return backup.Record{}, false, err
	}
	res, err := p.db.ExecContext(ctx, `INSERT INTO backups (`+backupCols+`) VALUES
		($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
		ON CONFLICT DO NOTHING`, r.ID, r.Source, r.Engine, r.Tool, r.ToolVersion, r.Bucket, r.ObjectKey,
		r.Bytes, strings.ToLower(r.SHA256), r.Position.GTIDSet, r.Position.LSN, r.Position.BinlogFile,
		r.Position.BinlogPos, r.ManifestURI, r.Status, r.CreatedAt, r.VerifiedAt, r.ExpiresAt, r.LegalHold)
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	if n == 1 {
		return r, true, nil
	}
	old, ok, err := p.GetBackup(ctx, r.ID)
	if err != nil {
		return backup.Record{}, false, err
	}
	if !ok || !backup.SameIdentity(old, r) {
		return backup.Record{}, false, backup.ErrConflict
	}
	return old, false, nil
}

func (p *PG) GetBackup(ctx context.Context, id string) (backup.Record, bool, error) {
	r, err := scanBackup(p.db.QueryRowContext(ctx, `SELECT `+backupCols+` FROM backups WHERE id = $1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return backup.Record{}, false, nil
	}
	return r, err == nil, p.wrap(err)
}

func (p *PG) ListBackups(ctx context.Context, f backup.Filter) ([]backup.Record, error) {
	if f.Limit <= 0 || f.Limit > backup.MaxListLimit {
		f.Limit = backup.MaxListLimit
	}
	query := `SELECT ` + backupCols + ` FROM backups WHERE true`
	args := make([]any, 0, 5)
	add := func(clause string, value any) {
		args = append(args, value)
		query += fmt.Sprintf(" AND %s $%d", clause, len(args))
	}
	if f.Source != "" {
		add("source =", f.Source)
	}
	if f.Engine != "" {
		add("engine =", f.Engine)
	}
	if f.Status != "" {
		add("status =", f.Status)
	}
	if f.CreatedAfter != nil {
		add("created_at >=", *f.CreatedAfter)
	}
	if f.CreatedBefore != nil {
		add("created_at <=", *f.CreatedBefore)
	}
	args = append(args, f.Limit)
	query += fmt.Sprintf(" ORDER BY created_at DESC, id LIMIT $%d", len(args))
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := make([]backup.Record, 0)
	for rows.Next() {
		r, err := scanBackup(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, r)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) UpdateBackupStatus(ctx context.Context, id string, expected, next backup.Status, verifiedAt *time.Time) (backup.Record, bool, error) {
	if next == backup.StatusComplete {
		return backup.Record{}, false, fmt.Errorf("%w: use finalization to complete a backup", backup.ErrInvalid)
	}
	if !backup.CanTransition(expected, next) {
		return backup.Record{}, false, backup.ErrTransition
	}
	if verifiedAt != nil {
		return backup.Record{}, false, fmt.Errorf("%w: verified_at is set by finalization", backup.ErrInvalid)
	}
	res, err := p.db.ExecContext(ctx, `UPDATE backups SET status = $3,
		verified_at = CASE WHEN $3 = 'uploading' THEN NULL ELSE verified_at END
		WHERE id = $1 AND status = $2
		AND (NOT legal_hold OR $3 <> 'expired')`, id, expected, next)
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	if n == 0 {
		cur, ok, err := p.GetBackup(ctx, id)
		if err != nil || !ok {
			return cur, ok, err
		}
		if cur.Status == expected && cur.LegalHold && next == backup.StatusExpired {
			return backup.Record{}, false, fmt.Errorf("%w: legal-hold backup cannot be expired", backup.ErrInvalid)
		}
		return backup.Record{}, false, backup.ErrTransition
	}
	return p.GetBackup(ctx, id)
}

func (p *PG) FinalizeBackup(ctx context.Context, id string, expected backup.Status, bytes int64, sha256, manifestURI string, verifiedAt time.Time) (backup.Record, bool, error) {
	if expected != backup.StatusVerifying {
		return backup.Record{}, false, fmt.Errorf("%w: invalid finalization metadata", backup.ErrInvalid)
	}
	if err := backup.ValidateFinalization(bytes, sha256, manifestURI, verifiedAt); err != nil {
		return backup.Record{}, false, err
	}
	sha256 = strings.ToLower(sha256)
	res, err := p.db.ExecContext(ctx, `UPDATE backups SET bytes = $3, sha256 = $4, manifest_uri = $5,
		status = 'complete', verified_at = $6
		WHERE id = $1 AND status = $2 AND $6 >= created_at`, id, expected, bytes, sha256, manifestURI, verifiedAt)
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return backup.Record{}, false, p.wrap(err)
	}
	if n == 0 {
		cur, ok, err := p.GetBackup(ctx, id)
		if err != nil || !ok {
			return cur, ok, err
		}
		if cur.Status == backup.StatusComplete && cur.Bytes == bytes && cur.SHA256 == sha256 && cur.ManifestURI == manifestURI {
			return cur, false, nil
		}
		if cur.Status == expected && verifiedAt.Before(cur.CreatedAt) {
			return backup.Record{}, false, fmt.Errorf("%w: verified_at precedes created_at", backup.ErrInvalid)
		}
		return backup.Record{}, false, backup.ErrTransition
	}
	return p.GetBackup(ctx, id)
}

const backupJobCols = `id, name, command, every_seconds, artifact_template, manifest_template, bucket, prefix,
	enabled, tunnel_ssh, tunnel_user, tunnel_identity_file, tunnel_known_hosts, tunnel_remote, cleanup_local, next_run_at, updated_at,
	run_started_at, run_finished_at, last_run_status`

func scanBackupJob(sc interface{ Scan(...any) error }) (backup.Job, error) {
	var j backup.Job
	var nextRun, runStarted, runFinished sql.NullTime
	err := sc.Scan(&j.ID, &j.Name, &j.Command, &j.EverySeconds, &j.ArtifactTemplate, &j.ManifestTemplate,
		&j.Bucket, &j.Prefix, &j.Enabled, &j.TunnelSSH, &j.TunnelUser, &j.TunnelIdentity, &j.TunnelKnownHosts,
		&j.TunnelRemote, &j.CleanupLocal, &nextRun, &j.UpdatedAt, &runStarted, &runFinished, &j.LastRunStatus)
	if nextRun.Valid {
		t := nextRun.Time
		j.NextRunAt = &t
	}
	if runStarted.Valid {
		t := runStarted.Time
		j.RunStartedAt = &t
	}
	if runFinished.Valid {
		t := runFinished.Time
		j.RunFinishedAt = &t
	}
	return j, err
}

func (p *PG) CreateBackupJob(ctx context.Context, j backup.Job) error {
	if err := backup.ValidateJob(j); err != nil {
		return err
	}
	_, err := p.db.ExecContext(ctx, `INSERT INTO backup_jobs
		(id, name, command, every_seconds, artifact_template, manifest_template, bucket, prefix, enabled,
		tunnel_ssh, tunnel_user, tunnel_identity_file, tunnel_known_hosts, tunnel_remote, cleanup_local, next_run_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, j.ID, j.Name, j.Command, j.EverySeconds,
		j.ArtifactTemplate, j.ManifestTemplate, j.Bucket, j.Prefix, j.Enabled, j.TunnelSSH, j.TunnelUser,
		j.TunnelIdentity, j.TunnelKnownHosts, j.TunnelRemote, j.CleanupLocal, j.NextRunAt)
	return p.wrap(err)
}

func (p *PG) GetBackupJob(ctx context.Context, id string) (backup.Job, bool, error) {
	j, err := scanBackupJob(p.db.QueryRowContext(ctx, `SELECT `+backupJobCols+` FROM backup_jobs WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return backup.Job{}, false, nil
	}
	return j, err == nil, p.wrap(err)
}

func (p *PG) ListBackupJobs(ctx context.Context) ([]backup.Job, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT `+backupJobCols+` FROM backup_jobs ORDER BY name, id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := make([]backup.Job, 0)
	for rows.Next() {
		j, err := scanBackupJob(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, j)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) UpdateBackupJob(ctx context.Context, j backup.Job) (bool, error) {
	if err := backup.ValidateJob(j); err != nil {
		return false, err
	}
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET name=$2, command=$3, every_seconds=$4,
		artifact_template=$5, manifest_template=$6, bucket=$7, prefix=$8, enabled=$9, tunnel_ssh=$10,
		tunnel_user=$11, tunnel_identity_file=$12, tunnel_known_hosts=$13, tunnel_remote=$14, cleanup_local=$15,
		next_run_at=$16, updated_at=GREATEST(clock_timestamp(), updated_at + interval '1 microsecond')
		WHERE id=$1`, j.ID, j.Name, j.Command, j.EverySeconds, j.ArtifactTemplate, j.ManifestTemplate,
		j.Bucket, j.Prefix, j.Enabled, j.TunnelSSH, j.TunnelUser, j.TunnelIdentity, j.TunnelKnownHosts,
		j.TunnelRemote, j.CleanupLocal, j.NextRunAt)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}

func (p *PG) SetBackupJobNextRun(ctx context.Context, id string, expectedUpdatedAt, nextRunAt time.Time) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET next_run_at=$3 WHERE id=$1 AND updated_at=$2`, id, expectedUpdatedAt, nextRunAt)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}

func (p *PG) BeginBackupJobRun(ctx context.Context, id string, expectedUpdatedAt time.Time) (time.Time, bool, error) {
	var startedAt time.Time
	err := p.db.QueryRowContext(ctx, `UPDATE backup_jobs SET run_started_at=clock_timestamp(), run_finished_at=NULL,
		last_run_status='running',
		next_run_at=CASE WHEN enabled THEN clock_timestamp() + make_interval(secs => every_seconds) ELSE next_run_at END
		WHERE id=$1 AND updated_at=$2 RETURNING run_started_at`, id, expectedUpdatedAt).Scan(&startedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	return startedAt, err == nil, p.wrap(err)
}

func (p *PG) FinishBackupJobRun(ctx context.Context, id string, startedAt time.Time, status backup.RunStatus) (bool, error) {
	if status != backup.RunStatusSucceeded && status != backup.RunStatusFailed && status != backup.RunStatusCancelled {
		return false, fmt.Errorf("%w: invalid backup job run status", backup.ErrInvalid)
	}
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET run_finished_at=clock_timestamp(), last_run_status=$3,
		next_run_at=CASE WHEN enabled THEN clock_timestamp() + make_interval(secs => every_seconds) ELSE NULL END
		WHERE id=$1 AND run_started_at=$2 AND run_finished_at IS NULL AND last_run_status='running'`, id, startedAt, status)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}

func (p *PG) RecoverBackupJobRun(ctx context.Context, id string, expectedUpdatedAt time.Time) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET run_finished_at=clock_timestamp(), last_run_status='interrupted',
		next_run_at=CASE WHEN enabled THEN clock_timestamp() + make_interval(secs => every_seconds) ELSE NULL END
		WHERE id=$1 AND updated_at=$2 AND run_started_at IS NOT NULL AND run_finished_at IS NULL AND last_run_status='running'`, id, expectedUpdatedAt)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}

func (p *PG) DeleteBackupJob(ctx context.Context, id string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `DELETE FROM backup_jobs WHERE id=$1`, id)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}
