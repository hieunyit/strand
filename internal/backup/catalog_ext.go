package backup

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// UpdateMeta records metadata learned while a backup is uploading or verifying.
func (c *Catalog) UpdateMeta(ctx context.Context, id, engine string, m Meta) (Record, bool, error) {
	if !validID(id) {
		return Record{}, false, fmt.Errorf("%w: invalid id", ErrInvalid)
	}
	m.PlainSHA256 = strings.ToLower(m.PlainSHA256)
	if err := ValidateMeta(engine, m); err != nil {
		return Record{}, false, err
	}
	return c.repo.UpdateBackupMeta(ctx, id, m)
}

// Expire marks a complete backup expired after its objects were deleted.
func (c *Catalog) Expire(ctx context.Context, id string, at time.Time) (Record, bool, error) {
	if !validID(id) || at.IsZero() {
		return Record{}, false, fmt.Errorf("%w: invalid id or time", ErrInvalid)
	}
	return c.repo.ExpireBackup(ctx, id, at.UTC())
}

// SetLegalHold sets or clears legal hold.
func (c *Catalog) SetLegalHold(ctx context.Context, id string, hold bool) (Record, bool, error) {
	if !validID(id) {
		return Record{}, false, fmt.Errorf("%w: invalid id", ErrInvalid)
	}
	return c.repo.SetBackupLegalHold(ctx, id, hold)
}

// AddCheck stores a verification result.
func (c *Catalog) AddCheck(ctx context.Context, r CheckResult) (int64, error) {
	if !validID(r.BackupID) || (r.Status != CheckOK && r.Status != CheckFailed) || r.StartedAt.IsZero() ||
		r.FinishedAt.Before(r.StartedAt) || !slugRE.MatchString(r.Kind) || len(r.Detail) > 16384 || len(r.Metrics) > 64 {
		return 0, fmt.Errorf("%w: invalid check result", ErrInvalid)
	}
	return c.repo.AddBackupCheck(ctx, r)
}

// Checks lists recent verification results ("" = all backups), newest first.
func (c *Catalog) Checks(ctx context.Context, backupID string, limit int) ([]CheckResult, error) {
	if backupID != "" && !validID(backupID) {
		return nil, fmt.Errorf("%w: invalid id", ErrInvalid)
	}
	if limit <= 0 || limit > MaxListLimit {
		limit = MaxListLimit
	}
	return c.repo.ListBackupChecks(ctx, backupID, limit)
}

// ListAll pages through every record matching f (newest first), up to max records (0 = 1,000,000).
func (c *Catalog) ListAll(ctx context.Context, f Filter, max int) ([]Record, error) {
	if max <= 0 {
		max = 1000000
	}
	f.Limit = MaxListLimit
	var out []Record
	for {
		page, err := c.List(ctx, f)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < f.Limit || len(out) >= max {
			return out, nil
		}
		last := page[len(page)-1]
		f.After = &Cursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
}

// SortOldestFirst orders records by created_at, id ascending.
func SortOldestFirst(rs []Record) {
	sort.SliceStable(rs, func(i, j int) bool {
		if !rs[i].CreatedAt.Equal(rs[j].CreatedAt) {
			return rs[i].CreatedAt.Before(rs[j].CreatedAt)
		}
		return rs[i].ID < rs[j].ID
	})
}
