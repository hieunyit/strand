package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"strand/internal/backup"
)

// SlotRequest asks for a run slot (STRAND_BACKUP_MAX_CONCURRENT / max_per_host): a slot is held while a
// scheduler runs a backup and expires when not renewed for TTL.
type SlotRequest struct {
	JobID      string
	Host       string // source host identity ("" = no per-host limit)
	MaxTotal   int
	MaxPerHost int
	TTL        time.Duration
}

// BackupConfigStore: reusable connections and policies, scheduler run slots and the missed-run record.
// Implemented by PG and Mem.
type BackupConfigStore interface {
	// CreateBackupConnection stores c (including its sealed secrets) and returns it with timestamps.
	CreateBackupConnection(ctx context.Context, c backup.Connection) (backup.Connection, error)
	GetBackupConnection(ctx context.Context, id string) (backup.Connection, bool, error)
	ListBackupConnections(ctx context.Context) ([]backup.Connection, error)
	// UpdateBackupConnection replaces the definition and sealed secrets (the caller merges kept secrets).
	UpdateBackupConnection(ctx context.Context, c backup.Connection) (bool, error)
	// DeleteBackupConnection refuses with *backup.InUseError while jobs reference the connection.
	DeleteBackupConnection(ctx context.Context, id string) (bool, error)
	SetBackupConnectionTest(ctx context.Context, id string, at time.Time, status, detail string) (bool, error)

	CreateBackupPolicy(ctx context.Context, p backup.Policy) (backup.Policy, error)
	GetBackupPolicy(ctx context.Context, id string) (backup.Policy, bool, error)
	ListBackupPolicies(ctx context.Context) ([]backup.Policy, error)
	UpdateBackupPolicy(ctx context.Context, p backup.Policy) (bool, error)
	// DeleteBackupPolicy refuses with *backup.InUseError while jobs reference the policy.
	DeleteBackupPolicy(ctx context.Context, id string) (bool, error)

	// AcquireBackupSlot takes a run slot if the global and per-host limits allow (idempotent per job).
	AcquireBackupSlot(ctx context.Context, r SlotRequest, now time.Time) (bool, error)
	RenewBackupSlot(ctx context.Context, jobID string, now time.Time) error
	ReleaseBackupSlot(ctx context.Context, jobID string) error

	// RecordBackupJobMissed records that the run due at dueAt was not started inside its window: status
	// "missed", and the next run is nextRunAt. Only for the job revision expectedUpdatedAt.
	RecordBackupJobMissed(ctx context.Context, id string, expectedUpdatedAt, nextRunAt time.Time) (bool, error)
}

// jobsUsing returns the ids of jobs whose connection_id (or policy_id) is id.
func jobsUsing(jobs []backup.Job, kind, id string) []string {
	var out []string
	for _, j := range jobs {
		if (kind == "connection" && (j.ConnectionID == id || j.ScratchConnectionID == id)) || (kind == "policy" && j.PolicyID == id) {
			out = append(out, j.ID)
		}
	}
	sort.Strings(out)
	return out
}

// ---- PG ----

const connCols = `id, name, engine, host, port, user_name, secret_mode, password_env, password_sealed, uri_env, uri_sealed,
	tls_mode, ca_cert_pem, ssh_tunnel, labels, created_by, created_at, updated_at, last_test_at, last_test_status, last_test_detail`

func scanConn(sc interface{ Scan(...any) error }) (backup.Connection, error) {
	var c backup.Connection
	var tunnel, labels []byte
	var testAt sql.NullTime
	err := sc.Scan(&c.ID, &c.Name, &c.Engine, &c.Host, &c.Port, &c.User, &c.SecretMode, &c.PasswordEnv, &c.PasswordSealed,
		&c.URIEnv, &c.URISealed, &c.TLSMode, &c.CACertPEM, &tunnel, &labels, &c.CreatedBy, &c.CreatedAt, &c.UpdatedAt,
		&testAt, &c.LastTestStatus, &c.LastTestDetail)
	if err != nil {
		return c, err
	}
	c.PasswordSet, c.URISet = len(c.PasswordSealed) > 0, len(c.URISealed) > 0
	if testAt.Valid {
		t := testAt.Time
		c.LastTestAt = &t
	}
	var t backup.SSHTunnel
	if json.Unmarshal(tunnel, &t) == nil && t.Host != "" {
		c.SSHTunnel = &t
	}
	if json.Unmarshal(labels, &c.Labels) == nil && len(c.Labels) == 0 {
		c.Labels = nil
	}
	return c, nil
}

func connJSON(c backup.Connection) (tunnel, labels []byte) {
	tunnel, labels = []byte("{}"), []byte("{}")
	if c.SSHTunnel != nil {
		tunnel, _ = json.Marshal(c.SSHTunnel)
	}
	if len(c.Labels) > 0 {
		labels, _ = json.Marshal(c.Labels)
	}
	return
}

func (p *PG) CreateBackupConnection(ctx context.Context, c backup.Connection) (backup.Connection, error) {
	c.PasswordSet, c.URISet = len(c.PasswordSealed) > 0, len(c.URISealed) > 0
	if err := c.Validate(); err != nil {
		return backup.Connection{}, err
	}
	tunnel, labels := connJSON(c)
	_, err := p.db.ExecContext(ctx, `INSERT INTO backup_connections (id, name, engine, host, port, user_name, secret_mode,
		password_env, password_sealed, uri_env, uri_sealed, tls_mode, ca_cert_pem, ssh_tunnel, labels, created_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, c.ID, c.Name, c.Engine, c.Host, c.Port, c.User,
		c.SecretMode, c.PasswordEnv, c.PasswordSealed, c.URIEnv, c.URISealed, c.TLSMode, c.CACertPEM, tunnel, labels, c.CreatedBy)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return backup.Connection{}, backup.ErrConflict
		}
		return backup.Connection{}, p.wrap(err)
	}
	got, _, err := p.GetBackupConnection(ctx, c.ID)
	return got, err
}

func (p *PG) GetBackupConnection(ctx context.Context, id string) (backup.Connection, bool, error) {
	c, err := scanConn(p.db.QueryRowContext(ctx, `SELECT `+connCols+` FROM backup_connections WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return backup.Connection{}, false, nil
	}
	return c, err == nil, p.wrap(err)
}

func (p *PG) ListBackupConnections(ctx context.Context) ([]backup.Connection, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT `+connCols+` FROM backup_connections ORDER BY name, id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := []backup.Connection{}
	for rows.Next() {
		c, err := scanConn(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, c)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) UpdateBackupConnection(ctx context.Context, c backup.Connection) (bool, error) {
	c.PasswordSet, c.URISet = len(c.PasswordSealed) > 0, len(c.URISealed) > 0
	if err := c.Validate(); err != nil {
		return false, err
	}
	tunnel, labels := connJSON(c)
	res, err := p.db.ExecContext(ctx, `UPDATE backup_connections SET name=$2, engine=$3, host=$4, port=$5, user_name=$6,
		secret_mode=$7, password_env=$8, password_sealed=$9, uri_env=$10, uri_sealed=$11, tls_mode=$12, ca_cert_pem=$13,
		ssh_tunnel=$14, labels=$15, updated_at=now() WHERE id=$1`, c.ID, c.Name, c.Engine, c.Host, c.Port, c.User,
		c.SecretMode, c.PasswordEnv, c.PasswordSealed, c.URIEnv, c.URISealed, c.TLSMode, c.CACertPEM, tunnel, labels)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return false, backup.ErrConflict
		}
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}

func (p *PG) DeleteBackupConnection(ctx context.Context, id string) (bool, error) {
	return p.deleteReferenced(ctx, "backup_connections", "connection_id", "connection", id)
}

// deleteReferenced deletes the row unless a job's options reference it; one statement, so a job created
// concurrently either sees the row or blocks the delete.
func (p *PG) deleteReferenced(ctx context.Context, table, optionKey, kind, id string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `DELETE FROM `+table+` WHERE id=$1 AND NOT EXISTS (
		SELECT 1 FROM backup_jobs WHERE options->>'`+optionKey+`' = $1)`, id)
	if err != nil {
		return false, p.wrap(err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return true, nil
	}
	jobs, err := p.ListBackupJobs(ctx)
	if err != nil {
		return false, err
	}
	if used := jobsUsing(jobs, kind, id); len(used) > 0 {
		return false, &backup.InUseError{Jobs: used}
	}
	return false, nil
}

func (p *PG) SetBackupConnectionTest(ctx context.Context, id string, at time.Time, status, detail string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_connections SET last_test_at=$2, last_test_status=$3, last_test_detail=$4 WHERE id=$1`,
		id, at, status, detail)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}

func scanPolicy(sc interface{ Scan(...any) error }) (backup.Policy, error) {
	var pol backup.Policy
	var spec []byte
	err := sc.Scan(&pol.ID, &pol.Name, &pol.Description, &pol.Builtin, &spec, &pol.CreatedBy, &pol.CreatedAt, &pol.UpdatedAt)
	if err != nil {
		return pol, err
	}
	id, name, desc, builtin, by, cr, up := pol.ID, pol.Name, pol.Description, pol.Builtin, pol.CreatedBy, pol.CreatedAt, pol.UpdatedAt
	pol = backup.Policy{}
	if err := json.Unmarshal(spec, &pol); err != nil {
		return pol, err
	}
	pol.ID, pol.Name, pol.Description, pol.Builtin, pol.CreatedBy, pol.CreatedAt, pol.UpdatedAt = id, name, desc, builtin, by, cr, up
	return pol, nil
}

const policyCols = `id, name, description, builtin, spec, created_by, created_at, updated_at`

func (p *PG) CreateBackupPolicy(ctx context.Context, pol backup.Policy) (backup.Policy, error) {
	if err := pol.Validate(); err != nil {
		return backup.Policy{}, err
	}
	spec, _ := json.Marshal(pol)
	_, err := p.db.ExecContext(ctx, `INSERT INTO backup_policies (id, name, description, builtin, spec, created_by)
		VALUES ($1,$2,$3,$4,$5,$6)`, pol.ID, pol.Name, pol.Description, pol.Builtin, spec, pol.CreatedBy)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return backup.Policy{}, backup.ErrConflict
		}
		return backup.Policy{}, p.wrap(err)
	}
	got, _, err := p.GetBackupPolicy(ctx, pol.ID)
	return got, err
}

func (p *PG) GetBackupPolicy(ctx context.Context, id string) (backup.Policy, bool, error) {
	pol, err := scanPolicy(p.db.QueryRowContext(ctx, `SELECT `+policyCols+` FROM backup_policies WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return backup.Policy{}, false, nil
	}
	return pol, err == nil, p.wrap(err)
}

func (p *PG) ListBackupPolicies(ctx context.Context) ([]backup.Policy, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT `+policyCols+` FROM backup_policies ORDER BY builtin DESC, name, id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := []backup.Policy{}
	for rows.Next() {
		pol, err := scanPolicy(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, pol)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) UpdateBackupPolicy(ctx context.Context, pol backup.Policy) (bool, error) {
	if err := pol.Validate(); err != nil {
		return false, err
	}
	spec, _ := json.Marshal(pol)
	res, err := p.db.ExecContext(ctx, `UPDATE backup_policies SET name=$2, description=$3, spec=$4, updated_at=now() WHERE id=$1`,
		pol.ID, pol.Name, pol.Description, spec)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return false, backup.ErrConflict
		}
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}

func (p *PG) DeleteBackupPolicy(ctx context.Context, id string) (bool, error) {
	return p.deleteReferenced(ctx, "backup_policies", "policy_id", "policy", id)
}

func (p *PG) AcquireBackupSlot(ctx context.Context, r SlotRequest, now time.Time) (bool, error) {
	got := false
	err := withAdvisoryLock(ctx, p.db, "backup-slots", func(ctx context.Context) error {
		cutoff := now.Add(-r.TTL)
		if _, err := p.db.ExecContext(ctx, `DELETE FROM backup_run_slots WHERE renewed_at < $1`, cutoff); err != nil {
			return err
		}
		var mine int
		if err := p.db.QueryRowContext(ctx, `SELECT count(*) FROM backup_run_slots WHERE job_id=$1`, r.JobID).Scan(&mine); err != nil {
			return err
		}
		if mine > 0 {
			_, err := p.db.ExecContext(ctx, `UPDATE backup_run_slots SET renewed_at=$2 WHERE job_id=$1`, r.JobID, now)
			got = err == nil
			return err
		}
		var total, host int
		if err := p.db.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE $1 <> '' AND host = $1) FROM backup_run_slots`, r.Host).Scan(&total, &host); err != nil {
			return err
		}
		if (r.MaxTotal > 0 && total >= r.MaxTotal) || (r.MaxPerHost > 0 && r.Host != "" && host >= r.MaxPerHost) {
			return nil
		}
		_, err := p.db.ExecContext(ctx, `INSERT INTO backup_run_slots (job_id, host, acquired_at, renewed_at) VALUES ($1,$2,$3,$3)`, r.JobID, r.Host, now)
		got = err == nil
		return err
	})
	return got, p.wrap(err)
}

func (p *PG) RenewBackupSlot(ctx context.Context, jobID string, now time.Time) error {
	_, err := p.db.ExecContext(ctx, `UPDATE backup_run_slots SET renewed_at=$2 WHERE job_id=$1`, jobID, now)
	return p.wrap(err)
}

func (p *PG) ReleaseBackupSlot(ctx context.Context, jobID string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM backup_run_slots WHERE job_id=$1`, jobID)
	return p.wrap(err)
}

func (p *PG) RecordBackupJobMissed(ctx context.Context, id string, expectedUpdatedAt, nextRunAt time.Time) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_jobs SET run_started_at=now(), run_finished_at=now(), last_run_status='missed', next_run_at=$3
		WHERE id=$1 AND updated_at=$2 AND last_run_status <> 'running'`, id, expectedUpdatedAt, nextRunAt)
	if err != nil {
		return false, p.wrap(err)
	}
	n, err := res.RowsAffected()
	return n > 0, p.wrap(err)
}
