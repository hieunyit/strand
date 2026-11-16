package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // driver "pgx"
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
		var released bool
		if err := conn.QueryRowContext(uctx, `SELECT pg_advisory_unlock($1)`, k).Scan(&released); err != nil || !released {
			// Không chắc đã nhả khóa (lỗi mạng, timeout): hủy kết nối thay vì trả về pool, nếu không khóa phiên
			// này bị giữ mãi trên một kết nối rảnh và mọi instance khác chờ vô hạn ở lần migrate/import sau.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	return fn(ctx)
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

const pipelineCols = `name, config_path, env_path, systemd_unit, log_file, pid_file, config_yaml, env_enc, origin, missing_since, conflicts::text, created_at, updated_at`

func scanPipeline(sc interface{ Scan(...any) error }) (Pipeline, error) {
	var p Pipeline
	var y sql.NullString
	var miss sql.NullTime
	err := sc.Scan(&p.Name, &p.ConfigPath, &p.EnvPath, &p.SystemdUnit, &p.LogFile, &p.PidFile, &y, &p.EnvEnc, &p.Origin, &miss, &p.Conflicts, &p.Created, &p.Updated)
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
	return []any{r.Name, r.ConfigPath, r.EnvPath, r.SystemdUnit, r.LogFile, r.PidFile, y, r.EnvEnc, r.Origin, miss, r.Conflicts}
}

const pipelineInsert = `INSERT INTO pipelines (name, config_path, env_path, systemd_unit, log_file, pid_file, config_yaml, env_enc, origin, missing_since, conflicts, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb, now(), now())`

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

func (p *PG) SetPipelineConflicts(ctx context.Context, name, conflicts string) error {
	if conflicts == "" {
		conflicts = "[]"
	}
	_, err := p.db.ExecContext(ctx, `UPDATE pipelines SET conflicts = $2::jsonb, updated_at = now() WHERE name = $1`, name, conflicts)
	return p.wrap(err)
}

func (p *PG) DeletePipeline(ctx context.Context, name string) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return p.wrap(err)
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipelines WHERE name = $1`, name); err != nil {
		return p.wrap(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pipeline_desired WHERE name = $1`, name); err != nil {
		return p.wrap(err)
	}
	return p.wrap(tx.Commit())
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
