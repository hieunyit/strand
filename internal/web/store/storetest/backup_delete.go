package storetest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// runBackupDelete: only records that are not complete can be deleted by hand; legal hold and an unexpired
// Object Lock retention refuse; checks go with the record; unknown ids report not found.
func runBackupDelete(t *testing.T, s store.Store) {
	ctx := context.Background()
	cat, _ := backup.NewCatalog(s)
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	mk := func(id string) backup.Record {
		r := backup.Record{ID: id, Source: "pg-prod", Engine: "postgres", Tool: "pg_dump", Bucket: "central-backups",
			ObjectKey: "pg/" + id + "/dump", Status: backup.StatusQueued, CreatedAt: base, Method: backup.MethodPGDump}
		if _, _, err := cat.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		return r
	}
	fail := func(id string) {
		if _, _, err := cat.UpdateStatus(ctx, id, backup.StatusFailed, nil); err != nil {
			t.Fatal(err)
		}
	}
	complete := completeBackup(t, s, backup.Record{ID: "d-complete", Source: "pg-prod", Engine: "postgres", Tool: "pg_dump",
		Bucket: "central-backups", ObjectKey: "pg/d-complete/dump", Status: backup.StatusQueued, CreatedAt: base, Method: backup.MethodPGDump}, backup.Meta{})
	if _, ok, err := cat.Delete(ctx, complete.ID, time.Now()); !errors.Is(err, backup.ErrInvalid) || ok || !strings.Contains(err.Error(), "retention") {
		t.Fatalf("delete complete = %v %v", ok, err)
	}
	if _, found, _ := cat.Get(ctx, complete.ID); !found {
		t.Fatal("complete record deleted")
	}

	mk("d-failed")
	fail("d-failed")
	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := cat.AddCheck(ctx, backup.CheckResult{BackupID: "d-failed", Kind: "verify", Status: backup.CheckFailed,
		StartedAt: now, FinishedAt: now, Detail: "missing"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SetBackupLegalHold(ctx, "d-failed", true); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := cat.Delete(ctx, "d-failed", now); !errors.Is(err, backup.ErrInvalid) || ok || !strings.Contains(err.Error(), "legal hold") {
		t.Fatalf("delete under hold = %v %v", ok, err)
	}
	_, _, _ = s.SetBackupLegalHold(ctx, "d-failed", false)
	got, ok, err := cat.Delete(ctx, "d-failed", now)
	if err != nil || !ok || got.ID != "d-failed" || got.Status != backup.StatusFailed {
		t.Fatalf("delete failed record = %+v %v %v", got, ok, err)
	}
	if _, found, _ := cat.Get(ctx, "d-failed"); found {
		t.Fatal("failed record still in the catalog")
	}
	if cs, _ := cat.Checks(ctx, "d-failed", 10); len(cs) != 0 {
		t.Fatalf("checks of a deleted record = %v", cs)
	}
	if _, ok, err := cat.Delete(ctx, "d-failed", now); err != nil || ok {
		t.Fatalf("delete twice = %v %v", ok, err)
	}

	// Object Lock recorded on a record that failed after its upload: refused until retain_until.
	mk("d-locked")
	if _, _, err := cat.UpdateStatus(ctx, "d-locked", backup.StatusUploading, nil); err != nil {
		t.Fatal(err)
	}
	until := now.Add(48 * time.Hour)
	if _, _, err := cat.UpdateMeta(ctx, "d-locked", "postgres", backup.Meta{LockMode: "GOVERNANCE", RetainUntil: &until}); err != nil {
		t.Fatal(err)
	}
	fail("d-locked")
	if _, _, err := cat.Delete(ctx, "d-locked", now); err == nil || !strings.Contains(err.Error(), "object-locked") {
		t.Fatalf("delete locked = %v", err)
	}
	if _, ok, err := cat.Delete(ctx, "d-locked", until.Add(time.Second)); err != nil || !ok {
		t.Fatalf("delete after lock = %v %v", ok, err)
	}

	// An abandoned in-progress record (the caller checked that no writer is left) can be deleted.
	mk("d-queued")
	if _, ok, err := cat.Delete(ctx, "d-queued", now); err != nil || !ok {
		t.Fatalf("delete queued = %v %v", ok, err)
	}
}
