package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	_, err := advisoryLocked(ctx, db, name, false, fn)
	return err
}

// advisoryLocked is withAdvisoryLock; with try it does not wait (pg_try_advisory_lock) and reports false,
// without running fn, when another session holds the lock.
func advisoryLocked(ctx context.Context, db *sql.DB, name string, try bool, fn func(ctx context.Context) error) (bool, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	k := lockKey(name)
	if try {
		var got bool
		if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock($1)`, k).Scan(&got); err != nil || !got {
			return false, err
		}
	} else if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, k); err != nil {
		return false, err
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		var released bool
		if err := conn.QueryRowContext(uctx, `SELECT pg_advisory_unlock($1)`, k).Scan(&released); err != nil || !released {
			// Không chắc đã nhả khóa (lỗi mạng, timeout): hủy kết nối thay vì trả về pool, nếu không khóa phiên
			// này bị giữ mãi trên một kết nối rảnh và mọi instance khác chờ vô hạn ở lần migrate/import sau.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
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
			return true, err
		case <-ctx.Done():
			cancel()
			return true, <-done
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
			err := conn.PingContext(pingCtx)
			pingCancel()
			if err != nil {
				cancel()
				callbackErr := <-done
				return true, errors.Join(fmt.Errorf("advisory lock connection lost: %w", err), callbackErr)
			}
		}
	}
}

func (p *PG) Locked(ctx context.Context, key string, fn func(ctx context.Context) error) error {
	return p.wrap(withAdvisoryLock(ctx, p.db, key, fn))
}

func (p *PG) TryLocked(ctx context.Context, key string, fn func(ctx context.Context) error) (bool, error) {
	got, err := advisoryLocked(ctx, p.db, key, true, fn)
	return got, p.wrap(err)
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
	if err != nil {
		return p.wrap(err)
	}
	_, _ = p.db.ExecContext(ctx, `INSERT INTO users (username, display_name, password_hash, role, must_change_password)
		VALUES ('admin', 'Administrator', $1, 'admin', false)
		ON CONFLICT ((lower(username))) DO UPDATE SET password_hash = EXCLUDED.password_hash, updated_at = now()`, h)
	return nil
}

func (p *PG) SetAPITokenHash(ctx context.Context, h string) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO admin (id, api_token_hash) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET api_token_hash = EXCLUDED.api_token_hash, updated_at = now()`, h)
	return p.wrap(err)
}

func (p *PG) PutSession(ctx context.Context, s Session) error {
	var uid any
	if s.UserID > 0 {
		uid = s.UserID
	}
	_, err := p.db.ExecContext(ctx, `INSERT INTO sessions (id_hash, csrf, ip, created_at, last_seen, user_id, username) VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (id_hash) DO UPDATE SET csrf = EXCLUDED.csrf, ip = EXCLUDED.ip, created_at = EXCLUDED.created_at, last_seen = EXCLUDED.last_seen,
		user_id = EXCLUDED.user_id, username = EXCLUDED.username`,
		s.IDHash, s.CSRF, s.IP, s.Created, s.LastSeen, uid, s.Username)
	return p.wrap(err)
}

func (p *PG) GetSession(ctx context.Context, id string) (Session, bool, error) {
	s := Session{IDHash: id}
	var uid sql.NullInt64
	err := p.db.QueryRowContext(ctx, `SELECT csrf, ip, created_at, last_seen, user_id, username FROM sessions WHERE id_hash = $1`, id).
		Scan(&s.CSRF, &s.IP, &s.Created, &s.LastSeen, &uid, &s.Username)
	if errors.Is(err, sql.ErrNoRows) {
		return Session{}, false, nil
	}
	if uid.Valid {
		s.UserID = uid.Int64
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

const pipelineCols = `name, config_path, env_path, systemd_unit, log_file, pid_file, config_yaml, env_enc, origin, missing_since, conflicts::text, created_at, updated_at, tags`

func scanPipeline(sc interface{ Scan(...any) error }) (Pipeline, error) {
	var p Pipeline
	var y sql.NullString
	var miss sql.NullTime
	err := sc.Scan(&p.Name, &p.ConfigPath, &p.EnvPath, &p.SystemdUnit, &p.LogFile, &p.PidFile, &y, &p.EnvEnc, &p.Origin, &miss, &p.Conflicts, &p.Created, &p.Updated, &p.Tags)
	p.ConfigYAML, p.HasConfig = y.String, y.Valid
	if miss.Valid {
		p.MissingSince = miss.Time
	}
	return p, err
}

func pipelineArgs(r Pipeline) []any {
	r.normalize()
	var y, miss any
	if r.HasConfig {
		y = r.ConfigYAML
	}
	if !r.MissingSince.IsZero() {
		miss = r.MissingSince
	}
	return []any{r.Name, r.ConfigPath, r.EnvPath, r.SystemdUnit, r.LogFile, r.PidFile, y, r.EnvEnc, r.Origin, miss, r.Conflicts, r.Tags}
}

const pipelineInsert = `INSERT INTO pipelines (name, config_path, env_path, systemd_unit, log_file, pid_file, config_yaml, env_enc, origin, missing_since, conflicts, created_at, updated_at, tags)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb, now(), now(), $12)`

func (p *PG) UpsertPipeline(ctx context.Context, r Pipeline) error {
	_, err := p.db.ExecContext(ctx, pipelineInsert+`
		ON CONFLICT (name) DO UPDATE SET config_path = EXCLUDED.config_path, env_path = EXCLUDED.env_path,
		  systemd_unit = EXCLUDED.systemd_unit, log_file = EXCLUDED.log_file, pid_file = EXCLUDED.pid_file,
		  config_yaml = EXCLUDED.config_yaml, env_enc = EXCLUDED.env_enc, origin = EXCLUDED.origin,
		  missing_since = EXCLUDED.missing_since, conflicts = EXCLUDED.conflicts, updated_at = now()`, pipelineArgs(r)...)
	return p.wrap(err)
}

func (p *PG) CreatePipeline(ctx context.Context, r Pipeline) (bool, error) {
	res, err := p.db.ExecContext(ctx, pipelineInsert+` ON CONFLICT (name) DO NOTHING`, pipelineArgs(r)...)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (p *PG) UpdatePipelineMeta(ctx context.Context, r Pipeline) error {
	r.normalize()
	var miss any
	if !r.MissingSince.IsZero() {
		miss = r.MissingSince
	}
	_, err := p.db.ExecContext(ctx, `UPDATE pipelines SET config_path = $2, env_path = $3, systemd_unit = $4, log_file = $5, pid_file = $6,
		origin = $7, missing_since = $8, updated_at = now() WHERE name = $1`,
		r.Name, r.ConfigPath, r.EnvPath, r.SystemdUnit, r.LogFile, r.PidFile, r.Origin, miss)
	return p.wrap(err)
}

func (p *PG) UpdatePipelineConfigCompareAndSwap(ctx context.Context, name string, expectedHasConfig bool, expected, config string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE pipelines SET config_yaml = $4, updated_at = now()
		WHERE name = $1 AND ((config_yaml IS NULL AND NOT $2) OR (config_yaml IS NOT NULL AND $2 AND config_yaml = $3))`,
		name, expectedHasConfig, expected, config)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, p.wrap(err)
	}
	return n == 1, nil
}

func (p *PG) UpdatePipelineEnv(ctx context.Context, name, envPath string, envEnc []byte) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE pipelines SET env_path = $2, env_enc = $3, updated_at = now() WHERE name = $1`,
		name, envPath, envEnc)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, p.wrap(err)
	}
	return n == 1, nil
}

func (p *PG) SetPipelineTags(ctx context.Context, name, tags string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE pipelines SET tags = $2, updated_at = now() WHERE name = $1`, name, tags)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n == 1, p.wrap(err)
}

func (p *PG) SetPipelineConflicts(ctx context.Context, name, conflicts string) error {
	if conflicts == "" {
		conflicts = "[]"
	}
	_, err := p.db.ExecContext(ctx, `UPDATE pipelines SET conflicts = $2::jsonb, updated_at = now() WHERE name = $1`, name, conflicts)
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
	// Một transaction: không để lại trạng thái mong muốn "running" mồ côi (tự chạy lại nếu tên được dùng lại).
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return p.wrap(err)
	}
	defer tx.Rollback() //nolint:errcheck
	for _, q := range []string{`DELETE FROM pipelines WHERE name = $1`, `DELETE FROM pipeline_desired WHERE name = $1`, `DELETE FROM wizards WHERE pipeline = $1`} {
		if _, err := tx.ExecContext(ctx, q, name); err != nil {
			return p.wrap(err)
		}
	}
	return p.wrap(tx.Commit())
}

func (p *PG) SetDesired(ctx context.Context, name, desired string) error {
	if desired != DesiredRunning && desired != DesiredStopped {
		return fmt.Errorf("invalid desired state %q", desired)
	}
	_, err := p.db.ExecContext(ctx, `INSERT INTO pipeline_desired (name, desired, reason, updated_at) VALUES ($1,$2,'',$3)
		ON CONFLICT (name) DO UPDATE SET desired = EXCLUDED.desired, reason = '', updated_at = EXCLUDED.updated_at`, name, desired, time.Now().UTC())
	return p.wrap(err)
}

func (p *PG) ObserveStop(ctx context.Context, name string, stoppedAt time.Time) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE pipeline_desired SET desired = 'stopped', reason = $2, updated_at = now()
		WHERE name = $1 AND desired = 'running' AND updated_at <= $3`, name, ReasonObservedStop, stoppedAt)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (p *PG) DesiredStates(ctx context.Context) (map[string]DesiredState, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT name, desired, reason, updated_at FROM pipeline_desired`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	m := map[string]DesiredState{}
	for rows.Next() {
		var n string
		var d DesiredState
		if err := rows.Scan(&n, &d.Desired, &d.Reason, &d.Updated); err != nil {
			return nil, p.wrap(err)
		}
		m[n] = d
	}
	return m, p.wrap(rows.Err())
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

func (p *PG) AppendJobOutput(ctx context.Context, id string, off int64, data []byte) error {
	_, err := p.db.ExecContext(ctx, `INSERT INTO job_output (job_id, off, data) VALUES ($1,$2,$3) ON CONFLICT (job_id, off) DO NOTHING`, id, off, data)
	return p.wrap(err)
}

func (p *PG) ReadJobOutput(ctx context.Context, id string, off int64, max int) ([]byte, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT off, data FROM job_output WHERE job_id = $1 AND off + length(data) > $2 AND off < $3 ORDER BY off`,
		id, off, off+int64(max))
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []byte
	for rows.Next() {
		var o int64
		var d []byte
		if err := rows.Scan(&o, &d); err != nil {
			return nil, p.wrap(err)
		}
		start := off + int64(len(out))
		if o > start {
			break
		}
		out = append(out, d[start-o:]...)
		if len(out) >= max {
			out = out[:max]
			break
		}
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) JobOutputSize(ctx context.Context, id string) (int64, error) {
	// Các chunk được ghi nối tiếp nhau nên offset cuối + độ dài là kích thước (giả định không có lỗ).
	var n sql.NullInt64
	err := p.db.QueryRowContext(ctx, `SELECT max(off + length(data)) FROM job_output WHERE job_id = $1`, id).Scan(&n)
	return n.Int64, p.wrap(err)
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

func (p *PG) AppendAuditBatch(ctx context.Context, rows []AuditRow, progress *MetaKV) error {
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
	if progress != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO web_meta (key, value) VALUES ($1,$2)
			ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, progress.Key, progress.Value); err != nil {
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
	gtid_set, lsn, binlog_file, binlog_pos, manifest_uri, status, created_at, verified_at, expires_at, legal_hold` + backupProCols

func scanBackup(sc interface{ Scan(...any) error }) (backup.Record, error) {
	var r backup.Record
	var verified, expires sql.NullTime
	ext := newBackupProScan(&r)
	err := sc.Scan(append([]any{&r.ID, &r.Source, &r.Engine, &r.Tool, &r.ToolVersion, &r.Bucket, &r.ObjectKey, &r.Bytes,
		&r.SHA256, &r.Position.GTIDSet, &r.Position.LSN, &r.Position.BinlogFile, &r.Position.BinlogPos,
		&r.ManifestURI, &r.Status, &r.CreatedAt, &verified, &expires, &r.LegalHold}, ext.dest()...)...)
	if verified.Valid {
		t := verified.Time
		r.VerifiedAt = &t
	}
	if expires.Valid {
		t := expires.Time
		r.ExpiresAt = &t
	}
	if err == nil {
		err = ext.finish()
	}
	return r, err
}

func (p *PG) CreateBackup(ctx context.Context, r backup.Record) (backup.Record, bool, error) {
	r.SHA256 = strings.ToLower(r.SHA256)
	if err := backup.ValidateCreate(r); err != nil {
		return backup.Record{}, false, err
	}
	args, err := backupProArgs(r)
	if err != nil {
		return backup.Record{}, false, err
	}
	res, err := p.db.ExecContext(ctx, `INSERT INTO backups (`+backupCols+`) VALUES
		($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,`+backupProPlaceholders(20)+`)
		ON CONFLICT DO NOTHING`, append([]any{r.ID, r.Source, r.Engine, r.Tool, r.ToolVersion, r.Bucket, r.ObjectKey,
		r.Bytes, strings.ToLower(r.SHA256), r.Position.GTIDSet, r.Position.LSN, r.Position.BinlogFile,
		r.Position.BinlogPos, r.ManifestURI, r.Status, r.CreatedAt, r.VerifiedAt, r.ExpiresAt, r.LegalHold}, args...)...)
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
	if f.JobID != "" {
		add("job_id =", f.JobID)
	}
	if f.Kind != "" {
		add("kind =", f.Kind)
	}
	if f.NotKind != "" {
		add("kind <>", f.NotKind)
	}
	if f.Repository != "" {
		add("repository =", f.Repository)
	}
	if f.LegalHold != nil {
		add("legal_hold =", *f.LegalHold)
	}
	if f.CreatedAfter != nil {
		add("created_at >=", *f.CreatedAfter)
	}
	if f.CreatedBefore != nil {
		add("created_at <=", *f.CreatedBefore)
	}
	if f.After != nil {
		args = append(args, f.After.CreatedAt, f.After.ID)
		query += fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, f.Limit)
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))
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
	run_started_at, run_finished_at, last_run_status, options, retry_max, retry_backoff_seconds, consecutive_failures`

func scanBackupJob(sc interface{ Scan(...any) error }) (backup.Job, error) {
	var j backup.Job
	var nextRun, runStarted, runFinished sql.NullTime
	var options []byte
	err := sc.Scan(&j.ID, &j.Name, &j.Command, &j.EverySeconds, &j.ArtifactTemplate, &j.ManifestTemplate,
		&j.Bucket, &j.Prefix, &j.Enabled, &j.TunnelSSH, &j.TunnelUser, &j.TunnelIdentity, &j.TunnelKnownHosts,
		&j.TunnelRemote, &j.CleanupLocal, &nextRun, &j.UpdatedAt, &runStarted, &runFinished, &j.LastRunStatus,
		&options, &j.RetryMax, &j.RetryBackoffSeconds, &j.ConsecutiveFailures)
	if err == nil {
		err = decodeJobOptions(options, &j.JobOptions)
	}
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
	options, err := encodeJobOptions(j.JobOptions)
	if err != nil {
		return err
	}
	_, err = p.db.ExecContext(ctx, `INSERT INTO backup_jobs
		(id, name, command, every_seconds, artifact_template, manifest_template, bucket, prefix, enabled,
		tunnel_ssh, tunnel_user, tunnel_identity_file, tunnel_known_hosts, tunnel_remote, cleanup_local, next_run_at,
		options, retry_max, retry_backoff_seconds)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`, j.ID, j.Name, j.Command, j.EverySeconds,
		j.ArtifactTemplate, j.ManifestTemplate, j.Bucket, j.Prefix, j.Enabled, j.TunnelSSH, j.TunnelUser,
		j.TunnelIdentity, j.TunnelKnownHosts, j.TunnelRemote, j.CleanupLocal, j.NextRunAt, options, j.RetryMax,
		j.RetryBackoffSeconds)
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
	options, err := encodeJobOptions(j.JobOptions)
	if err != nil {
		return false, err
	}
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET name=$2, command=$3, every_seconds=$4,
		artifact_template=$5, manifest_template=$6, bucket=$7, prefix=$8, enabled=$9, tunnel_ssh=$10,
		tunnel_user=$11, tunnel_identity_file=$12, tunnel_known_hosts=$13, tunnel_remote=$14, cleanup_local=$15,
		next_run_at=$16, options=$17, retry_max=$18, retry_backoff_seconds=$19, consecutive_failures=0,
		updated_at=GREATEST(clock_timestamp(), updated_at + interval '1 microsecond')
		WHERE id=$1`, j.ID, j.Name, j.Command, j.EverySeconds, j.ArtifactTemplate, j.ManifestTemplate,
		j.Bucket, j.Prefix, j.Enabled, j.TunnelSSH, j.TunnelUser, j.TunnelIdentity, j.TunnelKnownHosts,
		j.TunnelRemote, j.CleanupLocal, j.NextRunAt, options, j.RetryMax, j.RetryBackoffSeconds)
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

// BeginBackupJobRun, FinishBackupJobRun and RecoverBackupJobRun compute next_run_at in Go (backup.Job
// NextRunAfter / NextRunAfterFinish) from the locked row, so cron/daily/weekly/monthly jobs keep their
// schedule: SQL only knows every_seconds. The database clock stamps the run, as before.

func (p *PG) BeginBackupJobRun(ctx context.Context, id string, expectedUpdatedAt time.Time) (time.Time, bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return time.Time{}, false, p.wrap(err)
	}
	defer func() { _ = tx.Rollback() }()
	j, err := scanBackupJob(tx.QueryRowContext(ctx, `UPDATE backup_jobs SET run_started_at=clock_timestamp(),
		run_finished_at=NULL, last_run_status='running'
		WHERE id=$1 AND updated_at=$2 RETURNING `+backupJobCols, id, expectedUpdatedAt))
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, p.wrap(err)
	}
	startedAt := *j.RunStartedAt
	if j.Enabled {
		if _, err := tx.ExecContext(ctx, `UPDATE backup_jobs SET next_run_at=$2 WHERE id=$1`, id, j.NextRunAfter(startedAt)); err != nil {
			return time.Time{}, false, p.wrap(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return time.Time{}, false, p.wrap(err)
	}
	return startedAt, true, nil
}

// lockRunningBackupJob locks the row of a job whose run is still marked running (cond adds the caller's
// conditions on $1 = id, $2) and reads the database clock.
func lockRunningBackupJob(ctx context.Context, tx *sql.Tx, cond string, id string, arg any) (backup.Job, time.Time, error) {
	j, err := scanBackupJob(tx.QueryRowContext(ctx, `SELECT `+backupJobCols+` FROM backup_jobs WHERE id=$1 AND `+cond+`
		AND run_started_at IS NOT NULL AND run_finished_at IS NULL AND last_run_status='running' FOR UPDATE`, id, arg))
	if err != nil {
		return j, time.Time{}, err
	}
	var now time.Time
	err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return j, now, err
}

func (p *PG) FinishBackupJobRun(ctx context.Context, id string, startedAt time.Time, status backup.RunStatus) (bool, error) {
	if status != backup.RunStatusSucceeded && status != backup.RunStatusFailed && status != backup.RunStatusCancelled {
		return false, fmt.Errorf("%w: invalid backup job run status", backup.ErrInvalid)
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, p.wrap(err)
	}
	defer func() { _ = tx.Rollback() }()
	j, finished, err := lockRunningBackupJob(ctx, tx, "run_started_at=$2", id, startedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, p.wrap(err)
	}
	// Retry policy: see backup.Job.NextRunAfterFinish.
	next := j.NextRunAfterFinish(status, finished)
	failures := j.ConsecutiveFailures
	switch status {
	case backup.RunStatusFailed:
		failures++
	case backup.RunStatusSucceeded:
		failures = 0
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backup_jobs SET run_finished_at=$2, last_run_status=$3,
		consecutive_failures=$4, next_run_at=$5 WHERE id=$1`, id, finished, status, failures, next); err != nil {
		return false, p.wrap(err)
	}
	if err := tx.Commit(); err != nil {
		return false, p.wrap(err)
	}
	return true, nil
}

func (p *PG) RecoverBackupJobRun(ctx context.Context, id string, expectedUpdatedAt time.Time) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, p.wrap(err)
	}
	defer func() { _ = tx.Rollback() }()
	j, now, err := lockRunningBackupJob(ctx, tx, "updated_at=$2", id, expectedUpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, p.wrap(err)
	}
	var next *time.Time
	if j.Enabled {
		n := j.NextRunAfter(now)
		next = &n
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backup_jobs SET run_finished_at=$2, last_run_status='interrupted',
		next_run_at=$3 WHERE id=$1`, id, now, next); err != nil {
		return false, p.wrap(err)
	}
	if err := tx.Commit(); err != nil {
		return false, p.wrap(err)
	}
	return true, nil
}

func (p *PG) DeleteBackupJob(ctx context.Context, id string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `DELETE FROM backup_jobs WHERE id=$1`, id)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}
