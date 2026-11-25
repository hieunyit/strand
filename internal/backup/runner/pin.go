package runner

import (
	"context"
	"sync"
	"time"

	"strand/internal/backup"
)

// PinTTL is how long a restore pin lives without renewal; PinRecords renews it every PinTTL/4 while the
// reader runs, so a reader that dies blocks retention for at most PinTTL.
var PinTTL = 10 * time.Minute

// PinRecords pins backups against retention while a restore, test restore or verification reads them
// (the pins live in the catalog, so a prune in another process respects them). It fails when one of the
// records is no longer complete, e.g. retention already claimed it. The returned release drops the pins
// and must be called when the reader is done; it is safe to call more than once.
func (e *Env) PinRecords(ctx context.Context, ids []string) (release func(), err error) {
	holder := NewID("pin")
	if err := e.Catalog.Pin(ctx, holder, ids, PinTTL); err != nil {
		return func() {}, err
	}
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(PinTTL / 4)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := e.Catalog.Pin(ctx, holder, ids, PinTTL); err != nil {
					e.logf("renew restore pin: %v", err)
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if err := e.Catalog.Unpin(rctx, holder); err != nil {
				e.logf("release restore pin: %v", err)
			}
		})
	}, nil
}

// PinPlan pins the base chain and the log segments of a restore plan.
func (e *Env) PinPlan(ctx context.Context, plan RestorePlan) (release func(), err error) {
	ids := make([]string, 0, len(plan.Chain)+len(plan.Logs))
	for _, r := range plan.Chain {
		ids = append(ids, r.ID)
	}
	for _, r := range plan.Logs {
		ids = append(ids, r.ID)
	}
	return e.PinRecords(ctx, ids)
}

// PinJob pins every complete backup of a job (verification and the test restore read from them).
func (e *Env) PinJob(ctx context.Context, jobID string) (release func(), err error) {
	rs, err := e.Catalog.ListAll(ctx, backup.Filter{JobID: jobID, Status: backup.StatusComplete}, 0)
	if err != nil {
		return func() {}, err
	}
	ids := make([]string, 0, len(rs))
	for _, r := range rs {
		ids = append(ids, r.ID)
	}
	return e.PinRecords(ctx, ids)
}
