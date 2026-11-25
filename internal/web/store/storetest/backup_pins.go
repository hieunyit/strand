package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// runBackupPins: a live restore pin makes retention refuse the backup (claim and expire report
// ErrPinned); releasing or letting the pin lapse lets retention proceed; a backup that retention already
// claimed cannot be pinned; a pin and a claim racing on the same backup never both succeed.
func runBackupPins(t *testing.T, s store.Store) {
	ctx := context.Background()
	cat, _ := backup.NewCatalog(s)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	mk := func(id string) backup.Record {
		return completeBackup(t, s, backup.Record{ID: id, Source: "pg-prod", Engine: "postgres", Tool: "pg_dump",
			Bucket: "central-backups", ObjectKey: "pg/" + id + "/dump", Status: backup.StatusQueued, CreatedAt: base,
			JobID: "bjob-pin", Method: backup.MethodPGDump}, backup.Meta{})
	}
	b1, b2 := mk("pin-b1"), mk("pin-b2")
	if _, _, err := cat.Create(ctx, backup.Record{ID: "pin-queued", Source: "pg-prod", Engine: "postgres", Tool: "pg_dump",
		Bucket: "central-backups", ObjectKey: "pg/q/dump", Status: backup.StatusQueued, CreatedAt: base, Method: backup.MethodPGDump}); err != nil {
		t.Fatal(err)
	}
	if err := cat.Pin(ctx, "pin-h1", []string{b1.ID, b2.ID, b1.ID}, time.Minute); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := cat.Pin(ctx, "pin-h1", []string{b1.ID, b2.ID}, time.Minute); err != nil {
		t.Fatalf("renew pin: %v", err)
	}
	if err := cat.Pin(ctx, "pin-h2", []string{b2.ID, "pin-queued"}, time.Minute); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("pin of a backup that is not complete: %v", err)
	}
	if err := cat.Pin(ctx, "pin-h2", []string{"pin-missing"}, time.Minute); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("pin of an unknown backup: %v", err)
	}
	if _, ok, err := cat.ClaimDeletion(ctx, b1.ID, time.Now()); !errors.Is(err, backup.ErrPinned) || ok {
		t.Fatalf("claim of a pinned backup = %v %v", ok, err)
	}
	if _, ok, err := cat.Expire(ctx, b2.ID, time.Now()); !errors.Is(err, backup.ErrPinned) || ok {
		t.Fatalf("expire of a pinned backup = %v %v", ok, err)
	}
	for _, id := range []string{b1.ID, b2.ID} {
		if r, _, _ := cat.Get(ctx, id); r.Status != backup.StatusComplete {
			t.Fatalf("pinned backup %s = %s", id, r.Status)
		}
	}
	// Another holder's release leaves the pin alone.
	must(t, cat.Unpin(ctx, "pin-h2"))
	if _, _, err := cat.ClaimDeletion(ctx, b1.ID, time.Now()); !errors.Is(err, backup.ErrPinned) {
		t.Fatalf("claim after another holder released: %v", err)
	}
	must(t, cat.Unpin(ctx, "pin-h1"))
	if r, ok, err := cat.ClaimDeletion(ctx, b1.ID, time.Now()); err != nil || !ok || r.Status != backup.StatusDeleting {
		t.Fatalf("claim after release = %+v %v %v", r, ok, err)
	}
	if err := cat.Pin(ctx, "pin-h3", []string{b1.ID}, time.Minute); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("pin of a backup claimed by retention: %v", err)
	}
	// A pin that is not renewed lapses.
	must(t, cat.Pin(ctx, "pin-h4", []string{b2.ID}, 50*time.Millisecond))
	time.Sleep(300 * time.Millisecond)
	if _, ok, err := cat.ClaimDeletion(ctx, b2.ID, time.Now()); err != nil || !ok {
		t.Fatalf("claim after the pin lapsed = %v %v", ok, err)
	}
	must(t, cat.Unpin(ctx, "pin-h4"))

	// Pin and claim race: never both win.
	for i := 0; i < 20; i++ {
		r := mk(fmt.Sprintf("pin-race-%02d", i))
		var wg sync.WaitGroup
		var pinErr, claimErr error
		var claimed bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			pinErr = cat.Pin(ctx, fmt.Sprintf("pin-race-h%02d", i), []string{r.ID}, time.Minute)
		}()
		go func() { defer wg.Done(); _, claimed, claimErr = cat.ClaimDeletion(ctx, r.ID, time.Now()) }()
		wg.Wait()
		if pinErr == nil && claimed {
			t.Fatalf("race %d: both the pin and the deletion claim succeeded", i)
		}
		if pinErr == nil && !errors.Is(claimErr, backup.ErrPinned) {
			t.Fatalf("race %d: pin won but claim = %v", i, claimErr)
		}
		if claimed && !errors.Is(pinErr, backup.ErrInvalid) {
			t.Fatalf("race %d: claim won but pin = %v", i, pinErr)
		}
	}
}
