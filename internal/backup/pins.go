package backup

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// ErrPinned: retention refused to delete a backup that a restore, test restore or verification is reading.
var ErrPinned = errors.New("backup catalog: restore point is pinned by a restore or verification in progress")

// Pinner is implemented by catalogs that can pin backups against retention across processes (the CLI and
// the web server can be different processes, so the pins live in the metadata database). While a backup
// has a live pin, ClaimBackupDeletion and ExpireBackup refuse it with ErrPinned. Pins expire by the store's
// clock unless renewed, so a holder that dies cannot block retention for long.
type Pinner interface {
	// PinBackups pins every id for holder until ttl from now, or extends the holder's existing pins. It
	// fails (ErrInvalid) when one of them does not exist or is no longer complete (retention claimed it).
	PinBackups(ctx context.Context, holder string, ids []string, ttl time.Duration) error
	// ReleaseBackupPins drops every pin of holder.
	ReleaseBackupPins(ctx context.Context, holder string) error
}

// Pin pins ids for holder (see Pinner). A catalog without pin support accepts and ignores the call.
func (c *Catalog) Pin(ctx context.Context, holder string, ids []string, ttl time.Duration) error {
	p, ok := c.repo.(Pinner)
	if !ok {
		return nil
	}
	if !validID(holder) || ttl <= 0 {
		return fmt.Errorf("%w: invalid pin holder or ttl", ErrInvalid)
	}
	seen := map[string]bool{}
	uniq := make([]string, 0, len(ids))
	for _, id := range ids {
		if !validID(id) {
			return fmt.Errorf("%w: invalid id", ErrInvalid)
		}
		if !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	if len(uniq) == 0 {
		return nil
	}
	sort.Strings(uniq)
	return p.PinBackups(ctx, holder, uniq, ttl)
}

// Unpin releases the pins of holder.
func (c *Catalog) Unpin(ctx context.Context, holder string) error {
	p, ok := c.repo.(Pinner)
	if !ok {
		return nil
	}
	if !validID(holder) {
		return fmt.Errorf("%w: invalid pin holder", ErrInvalid)
	}
	return p.ReleaseBackupPins(ctx, holder)
}
