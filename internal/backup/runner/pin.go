package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"strand/internal/backup"
)

// PinTTL is how long a restore pin lives without renewal; PinRecords renews it every PinTTL/4 while the
// reader runs, so a reader that dies blocks retention for at most PinTTL.
var PinTTL = 10 * time.Minute

// ErrPinLapsed: the restore pin could not be renewed for most of PinTTL, so retention in another process
// may soon delete what is being read; the reader is cancelled before the pin can expire.
var ErrPinLapsed = errors.New("restore pin could not be renewed; stopped before retention may delete the backups being read")

// PinRecords pins backups against retention while a restore, test restore or verification reads them
// (the pins live in the catalog, so a prune in another process respects them). It fails when one of the
// records is no longer complete, e.g. retention already claimed it. The reader must use the returned
// context: it is cancelled (cause ErrPinLapsed) when renewals keep failing for 3/4 of PinTTL, before the
// pin can expire under it. The returned release drops the pins and must be called when the reader is done;
// it is safe to call more than once.
func (e *Env) PinRecords(ctx context.Context, ids []string) (pinned context.Context, release func(), err error) {
	holder := NewID("pin")
	firstPin := time.Now()
	if err := e.Catalog.Pin(ctx, holder, ids, PinTTL); err != nil {
		return ctx, func() {}, err
	}
	ttl := PinTTL
	pinned, cancel := context.WithCancelCause(ctx)
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(ttl / 4)
		defer t.Stop()
		lastOK := firstPin
		for {
			select {
			case <-stop:
				return
			case <-pinned.Done():
				return
			case <-t.C:
				attempt := time.Now()
				if err := e.Catalog.Pin(pinned, holder, ids, ttl); err != nil {
					e.logf("renew restore pin: %v", err)
					// Pin hết hạn khoảng lastOK + ttl. Quá nửa TTL mà chưa gia hạn được thì tick kế tiếp
					// (sau ttl/4) đã sát hạn: dừng ngay, chậm nhất khoảng 3/4 TTL sau lần gia hạn cuối.
					if time.Since(lastOK) > ttl/2 {
						cancel(fmt.Errorf("%w: %v", ErrPinLapsed, err))
						return
					}
					continue
				}
				// Tính từ lúc gửi yêu cầu: hạn mới không sớm hơn attempt + ttl.
				lastOK = attempt
			}
		}
	}()
	var once sync.Once
	return pinned, func() {
		once.Do(func() {
			close(stop)
			<-done
			cancel(nil)
			rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer rcancel()
			if err := e.Catalog.Unpin(rctx, holder); err != nil {
				e.logf("release restore pin: %v", err)
			}
		})
	}, nil
}

// PinPlan pins the base chain and the log segments of a restore plan.
func (e *Env) PinPlan(ctx context.Context, plan RestorePlan) (pinned context.Context, release func(), err error) {
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
func (e *Env) PinJob(ctx context.Context, jobID string) (pinned context.Context, release func(), err error) {
	rs, err := e.Catalog.ListAll(ctx, backup.Filter{JobID: jobID, Status: backup.StatusComplete}, 0)
	if err != nil {
		return ctx, func() {}, err
	}
	ids := make([]string, 0, len(rs))
	for _, r := range rs {
		ids = append(ids, r.ID)
	}
	return e.PinRecords(ctx, ids)
}
