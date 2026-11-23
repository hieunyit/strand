package backup

import (
	"context"
	"errors"
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

// ClaimDeletion reserves a backup for retention before any storage writes.
func (c *Catalog) ClaimDeletion(ctx context.Context, id string, at time.Time) (Record, bool, error) {
	if !validID(id) || at.IsZero() {
		return Record{}, false, fmt.Errorf("%w: invalid id or time", ErrInvalid)
	}
	return c.repo.ClaimBackupDeletion(ctx, id, at.UTC())
}

// Expire marks a backup expired after its objects were deleted.
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

// ErrNotComplete reports an operation that needs a complete restore point (restore, verify, copy).
var ErrNotComplete = errors.New("backup catalog: restore point is not complete")

// RequireComplete returns nil for a complete record, else an error wrapping ErrNotComplete that names the
// record and its status, e.g. "restore point bkp-1 is failed: only complete restore points can be restored".
func RequireComplete(r Record, verb string) error {
	if r.Status == StatusComplete {
		return nil
	}
	return fmt.Errorf("%w: restore point %s is %s: only complete restore points can be %s", ErrNotComplete, r.ID, r.Status, verb)
}

// InProgress reports a record whose backup (or copy, or log segment upload) has not finished yet.
func InProgress(s Status) bool {
	return s == StatusQueued || s == StatusUploading || s == StatusVerifying
}

// DeletableErr explains why a record cannot be deleted by hand (nil = it can). Complete restore points are
// removed only by retention (prune), which honors keep counts and incremental chains; legal hold and an
// unexpired Object Lock retention always win.
func DeletableErr(r Record, now time.Time) error {
	switch {
	case r.Status == StatusDeleting:
		return fmt.Errorf("%w: restore point %s is pending deletion by retention", ErrInvalid, r.ID)
	case r.Status == StatusComplete:
		return fmt.Errorf("%w: restore point %s is complete: complete restore points are removed by the retention policy (prune), not deleted by hand", ErrInvalid, r.ID)
	case r.LegalHold:
		return fmt.Errorf("%w: restore point %s is under legal hold: release the hold first", ErrInvalid, r.ID)
	case r.RetainUntil != nil && r.RetainUntil.After(now):
		return fmt.Errorf("%w: restore point %s is object-locked until %s", ErrInvalid, r.ID, r.RetainUntil.UTC().Format(time.RFC3339))
	}
	return nil
}

// Delete removes the catalog record of a backup that is not complete (see Repository.DeleteBackup). The
// caller deletes the record's objects first.
func (c *Catalog) Delete(ctx context.Context, id string, now time.Time) (Record, bool, error) {
	if !validID(id) || now.IsZero() {
		return Record{}, false, fmt.Errorf("%w: invalid id or time", ErrInvalid)
	}
	return c.repo.DeleteBackup(ctx, id, now.UTC())
}
