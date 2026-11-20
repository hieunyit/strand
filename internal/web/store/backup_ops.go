package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"strand/internal/backup"
)

// RepoUsage is the catalog footprint of one source in one repository (Repository "" = the default
// environment repository). Bytes count every non-expired record, log segments included.
type RepoUsage struct {
	Repository string `json:"repository"`
	Source     string `json:"source"`
	Count      int    `json:"count"`
	Bytes      int64  `json:"bytes"`
	Copies     int    `json:"copies"` // records that are replicas (copy_of set)
}

// KeyUsage counts the non-expired backups written with one encryption key id ("" = unencrypted).
type KeyUsage struct {
	KeyID     string   `json:"key_id"`
	Count     int      `json:"count"`
	Bytes     int64    `json:"bytes"`
	SampleIDs []string `json:"sample_ids,omitempty"` // newest backups (at most 5)
}

// RunCount is the number of runs of a job with one status in a time window.
type RunCount struct {
	JobID  string
	Status string
	N      int
}

// RPOBreach is an open RPO breach episode of a source.
type RPOBreach struct {
	Source     string
	Since      time.Time
	NotifiedAt time.Time
}

// AuditFilter selects audit rows for the activity feed: action prefix, rows older than BeforeID (cursor),
// newest first.
type AuditFilter struct {
	ActionPrefix string
	BeforeID     int64
	Limit        int
	Since, Until *time.Time
}

// BackupOpsStore: notification channels and rules, RPO breach state, repository/key usage, run counts and
// the filtered audit feed. Implemented by PG and Mem.
type BackupOpsStore interface {
	CreateNotifyChannel(ctx context.Context, c backup.NotifyChannel) (backup.NotifyChannel, error)
	GetNotifyChannel(ctx context.Context, id string) (backup.NotifyChannel, bool, error)
	ListNotifyChannels(ctx context.Context) ([]backup.NotifyChannel, error)
	// UpdateNotifyChannel replaces the definition and sealed secret (the caller merges a kept secret).
	UpdateNotifyChannel(ctx context.Context, c backup.NotifyChannel) (bool, error)
	// DeleteNotifyChannel also deletes the rules that target it.
	DeleteNotifyChannel(ctx context.Context, id string) (bool, error)
	SetNotifyChannelTest(ctx context.Context, id string, at time.Time, status, detail string) (bool, error)

	// CreateNotifyRule refuses with ErrInvalid when the channel does not exist and ErrConflict for a duplicate.
	CreateNotifyRule(ctx context.Context, r backup.NotifyRule) (backup.NotifyRule, error)
	GetNotifyRule(ctx context.Context, id string) (backup.NotifyRule, bool, error)
	ListNotifyRules(ctx context.Context) ([]backup.NotifyRule, error)
	SetNotifyRuleEnabled(ctx context.Context, id string, enabled bool) (bool, error)
	DeleteNotifyRule(ctx context.Context, id string) (bool, error)

	// ClaimBackupRPOBreach reports true when a breach of source should be notified now: it is new, or
	// the last notification is older than renotify. ClearBackupRPOBreach ends the episode.
	ClaimBackupRPOBreach(ctx context.Context, source string, now time.Time, renotify time.Duration) (bool, error)
	ClearBackupRPOBreach(ctx context.Context, source string) error
	ListBackupRPOBreaches(ctx context.Context) ([]RPOBreach, error)

	BackupRepoUsage(ctx context.Context) ([]RepoUsage, error)
	BackupKeyUsage(ctx context.Context) ([]KeyUsage, error)
	// BackupRunCounts counts runs started in [since, until) per job and status.
	BackupRunCounts(ctx context.Context, since, until time.Time) ([]RunCount, error)
	ListAuditFiltered(ctx context.Context, f AuditFilter) ([]AuditRow, error)
}

// ---- PG ----

const channelCols = `id, name, kind, enabled, smtp_addr, mail_from, mail_to, smtp_user, secret_sealed, created_by, created_at,
	updated_at, last_test_at, last_test_status, last_test_detail`

func scanChannel(sc interface{ Scan(...any) error }) (backup.NotifyChannel, error) {
	var c backup.NotifyChannel
	var to []byte
	var at *time.Time
	err := sc.Scan(&c.ID, &c.Name, &c.Kind, &c.Enabled, &c.SMTPAddr, &c.From, &to, &c.User, &c.SecretSealed, &c.CreatedBy,
		&c.CreatedAt, &c.UpdatedAt, &at, &c.LastTestStatus, &c.LastTestDetail)
	if err != nil {
		return c, err
	}
	c.LastTestAt = at
	_ = json.Unmarshal(to, &c.To)
	c.SecretSet = len(c.SecretSealed) > 0
	if !c.SecretSet {
		c.SecretSealed = nil
	}
	c.Source = "stored"
	return c, nil
}

func (p *PG) CreateNotifyChannel(ctx context.Context, c backup.NotifyChannel) (backup.NotifyChannel, error) {
	if err := c.Validate(); err != nil {
		return backup.NotifyChannel{}, err
	}
	to, _ := json.Marshal(nonNil(c.To))
	_, err := p.db.ExecContext(ctx, `INSERT INTO backup_notify_channels (id, name, kind, enabled, smtp_addr, mail_from, mail_to,
		smtp_user, secret_sealed, created_by) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)`, c.ID, c.Name, c.Kind, c.Enabled,
		c.SMTPAddr, c.From, string(to), c.User, c.SecretSealed, c.CreatedBy)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return backup.NotifyChannel{}, backup.ErrConflict
		}
		return backup.NotifyChannel{}, p.wrap(err)
	}
	got, _, err := p.GetNotifyChannel(ctx, c.ID)
	return got, err
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func (p *PG) GetNotifyChannel(ctx context.Context, id string) (backup.NotifyChannel, bool, error) {
	c, err := scanChannel(p.db.QueryRowContext(ctx, `SELECT `+channelCols+` FROM backup_notify_channels WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return backup.NotifyChannel{}, false, nil
	}
	return c, err == nil, p.wrap(err)
}

func (p *PG) ListNotifyChannels(ctx context.Context) ([]backup.NotifyChannel, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT `+channelCols+` FROM backup_notify_channels ORDER BY name, id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := []backup.NotifyChannel{}
	for rows.Next() {
		c, err := scanChannel(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, c)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) UpdateNotifyChannel(ctx context.Context, c backup.NotifyChannel) (bool, error) {
	if err := c.Validate(); err != nil {
		return false, err
	}
	to, _ := json.Marshal(nonNil(c.To))
	res, err := p.db.ExecContext(ctx, `UPDATE backup_notify_channels SET name=$2, kind=$3, enabled=$4, smtp_addr=$5, mail_from=$6,
		mail_to=$7::jsonb, smtp_user=$8, secret_sealed=$9, updated_at=now() WHERE id=$1`, c.ID, c.Name, c.Kind, c.Enabled,
		c.SMTPAddr, c.From, string(to), c.User, c.SecretSealed)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return false, backup.ErrConflict
		}
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) DeleteNotifyChannel(ctx context.Context, id string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `DELETE FROM backup_notify_channels WHERE id=$1`, id)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) SetNotifyChannelTest(ctx context.Context, id string, at time.Time, status, detail string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_notify_channels SET last_test_at=$2, last_test_status=$3, last_test_detail=$4 WHERE id=$1`,
		id, at, status, detail)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) CreateNotifyRule(ctx context.Context, r backup.NotifyRule) (backup.NotifyRule, error) {
	if err := r.Validate(); err != nil {
		return backup.NotifyRule{}, err
	}
	_, err := p.db.ExecContext(ctx, `INSERT INTO backup_notify_rules (id, event, scope_kind, scope_id, channel_id, enabled)
		VALUES ($1,$2,$3,$4,$5,$6)`, r.ID, r.Event, r.ScopeKind, r.ScopeID, r.ChannelID, r.Enabled)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "duplicate key"):
			return backup.NotifyRule{}, backup.ErrConflict
		case strings.Contains(err.Error(), "foreign key"):
			return backup.NotifyRule{}, backup.ErrInvalid
		}
		return backup.NotifyRule{}, p.wrap(err)
	}
	got, _, err := p.GetNotifyRule(ctx, r.ID)
	return got, err
}

const ruleCols = `id, event, scope_kind, scope_id, channel_id, enabled, created_at`

func scanRule(sc interface{ Scan(...any) error }) (backup.NotifyRule, error) {
	var r backup.NotifyRule
	err := sc.Scan(&r.ID, &r.Event, &r.ScopeKind, &r.ScopeID, &r.ChannelID, &r.Enabled, &r.CreatedAt)
	return r, err
}

func (p *PG) GetNotifyRule(ctx context.Context, id string) (backup.NotifyRule, bool, error) {
	r, err := scanRule(p.db.QueryRowContext(ctx, `SELECT `+ruleCols+` FROM backup_notify_rules WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return backup.NotifyRule{}, false, nil
	}
	return r, err == nil, p.wrap(err)
}

func (p *PG) ListNotifyRules(ctx context.Context) ([]backup.NotifyRule, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT `+ruleCols+` FROM backup_notify_rules ORDER BY event, scope_kind, scope_id, channel_id, id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	out := []backup.NotifyRule{}
	for rows.Next() {
		r, err := scanRule(rows)
		if err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, r)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) SetNotifyRuleEnabled(ctx context.Context, id string, enabled bool) (bool, error) {
	res, err := p.db.ExecContext(ctx, `UPDATE backup_notify_rules SET enabled=$2 WHERE id=$1`, id, enabled)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) DeleteNotifyRule(ctx context.Context, id string) (bool, error) {
	res, err := p.db.ExecContext(ctx, `DELETE FROM backup_notify_rules WHERE id=$1`, id)
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) ClaimBackupRPOBreach(ctx context.Context, source string, now time.Time, renotify time.Duration) (bool, error) {
	res, err := p.db.ExecContext(ctx, `INSERT INTO backup_rpo_breaches (source, since, notified_at) VALUES ($1,$2,$2)
		ON CONFLICT (source) DO UPDATE SET notified_at = EXCLUDED.notified_at WHERE backup_rpo_breaches.notified_at <= $3`,
		source, now, now.Add(-renotify))
	if err != nil {
		return false, p.wrap(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (p *PG) ClearBackupRPOBreach(ctx context.Context, source string) error {
	_, err := p.db.ExecContext(ctx, `DELETE FROM backup_rpo_breaches WHERE source=$1`, source)
	return p.wrap(err)
}

func (p *PG) ListBackupRPOBreaches(ctx context.Context) ([]RPOBreach, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT source, since, notified_at FROM backup_rpo_breaches ORDER BY source`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []RPOBreach
	for rows.Next() {
		var b RPOBreach
		if err := rows.Scan(&b.Source, &b.Since, &b.NotifiedAt); err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, b)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) BackupRepoUsage(ctx context.Context) ([]RepoUsage, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT repository, source, count(*), COALESCE(sum(bytes),0), count(*) FILTER (WHERE copy_of <> '')
		FROM backups WHERE status <> 'expired' GROUP BY repository, source ORDER BY repository, source`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []RepoUsage
	for rows.Next() {
		var u RepoUsage
		if err := rows.Scan(&u.Repository, &u.Source, &u.Count, &u.Bytes, &u.Copies); err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, u)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) BackupKeyUsage(ctx context.Context) ([]KeyUsage, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT key_id, count(*), COALESCE(sum(bytes),0),
		array_to_string((array_agg(id ORDER BY created_at DESC, id DESC))[1:5], ',') FROM backups WHERE status <> 'expired' GROUP BY key_id ORDER BY key_id`)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []KeyUsage
	for rows.Next() {
		var u KeyUsage
		var ids string
		if err := rows.Scan(&u.KeyID, &u.Count, &u.Bytes, &ids); err != nil {
			return nil, p.wrap(err)
		}
		if ids != "" {
			u.SampleIDs = strings.Split(ids, ",")
		}
		out = append(out, u)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) BackupRunCounts(ctx context.Context, since, until time.Time) ([]RunCount, error) {
	rows, err := p.db.QueryContext(ctx, `SELECT job_id, status, count(*) FROM backup_runs WHERE started_at >= $1 AND started_at < $2
		GROUP BY job_id, status ORDER BY job_id, status`, since, until)
	if err != nil {
		return nil, p.wrap(err)
	}
	defer rows.Close()
	var out []RunCount
	for rows.Next() {
		var c RunCount
		if err := rows.Scan(&c.JobID, &c.Status, &c.N); err != nil {
			return nil, p.wrap(err)
		}
		out = append(out, c)
	}
	return out, p.wrap(rows.Err())
}

func (p *PG) ListAuditFiltered(ctx context.Context, f AuditFilter) ([]AuditRow, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	where, args := []string{"true"}, []any{}
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, strings.Replace(cond, "?", "$"+strconv.Itoa(len(args)), 1))
	}
	if f.ActionPrefix != "" {
		add("starts_with(action, ?)", f.ActionPrefix)
	}
	if f.BeforeID > 0 {
		add("id < ?", f.BeforeID)
	}
	if f.Since != nil {
		add("ts >= ?", *f.Since)
	}
	if f.Until != nil {
		add("ts < ?", *f.Until)
	}
	args = append(args, f.Limit)
	rows, err := p.db.QueryContext(ctx, `SELECT id, ts, ip, action, pipeline, result, data::text FROM audit_log WHERE `+
		strings.Join(where, " AND ")+` ORDER BY id DESC LIMIT $`+strconv.Itoa(len(args)), args...)
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
