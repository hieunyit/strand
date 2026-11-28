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
// context: it is cancelled (cause ErrPinLapsed) when no renewal has succeeded for 3/4 of PinTTL, failing or hung, before the
// pin can expire under it. The returned release drops the pins and must be called when the reader is done;
// it is safe to call more than once.
func (e *Env) PinRecords(ctx context.Context, ids []string) (pinned context.Context, release func(), err error) {
	holder := NewID("pin")
	ttl := PinTTL
	firstPin := time.Now()
	if err := e.Catalog.Pin(ctx, holder, ids, ttl); err != nil {
		return ctx, func() {}, err
	}
	pinned, cancel := context.WithCancelCause(ctx)
	// lastOK: lúc GỬI lần gia hạn thành công gần nhất (hạn mới không sớm hơn lúc đó + ttl).
	var mu sync.Mutex
	lastOK, lastErr := firstPin, error(nil)
	stop, done := make(chan struct{}), make(chan struct{})
	// Gia hạn: mỗi lần có hạn chót riêng (ttl/4), nên một lệnh catalog treo (chờ row lock, mất mạng) không
	// chặn các lần sau.
	go func() {
		defer close(done)
		t := time.NewTicker(ttl / 4)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-pinned.Done():
				return
			case <-t.C:
			}
			attempt := time.Now()
			actx, acancel := context.WithTimeout(pinned, ttl/4)
			err := e.Catalog.Pin(actx, holder, ids, ttl)
			acancel()
			mu.Lock()
			if err != nil {
				lastErr = err
			} else if attempt.After(lastOK) {
				lastOK, lastErr = attempt, nil
			}
			mu.Unlock()
			if err != nil {
				e.logf("renew restore pin: %v", err)
			}
		}
	}()
	// Canh hạn độc lập với I/O: quá 3/4 TTL kể từ lần gia hạn thành công cuối mà chưa gia hạn được thì dừng
	// reader, kể cả khi lệnh gia hạn đang treo không trả về.
	go func() {
		limit := ttl * 3 / 4
		for {
			mu.Lock()
			due := lastOK.Add(limit)
			lerr := lastErr
			mu.Unlock()
			wait := time.Until(due)
			if wait <= 0 {
				if lerr == nil {
					lerr = errors.New("renewal did not complete")
				}
				cancel(fmt.Errorf("%w: %v", ErrPinLapsed, lerr))
				return
			}
			timer := time.NewTimer(wait)
			select {
			case <-stop:
				timer.Stop()
				return
			case <-pinned.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	var once sync.Once
	return pinned, func() {
		once.Do(func() {
			close(stop)
			cancel(nil)
			// Lệnh gia hạn đang treo đã bị hủy qua context; không chờ mãi một catalog không trả lời.
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				e.logf("release restore pin: a renewal is still running")
			}
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
