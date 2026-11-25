package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// A restore pins what it reads; retention running meanwhile (possibly in another process) skips the
// pinned backups instead of deleting them mid-restore, and expires them once the pin is released.
func TestPruneSkipsPinnedRestorePoints(t *testing.T) {
	vars := map[string]string{"STRAND_BACKUP_S3_ENDPOINT": "http://127.0.0.1:1", "STRAND_BACKUP_S3_ACCESS_KEY": "k",
		"STRAND_BACKUP_S3_SECRET_KEY": "s", "STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true"}
	st := store.NewMem()
	e := testEnv(t, vars, st)
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Second)
	e.Now = func() time.Time { return now }
	mk := func(id string, at time.Time) {
		r := backup.Record{ID: id, Source: "pg-prod", Engine: "postgres", Tool: "pg_dump", Bucket: "central-backups",
			ObjectKey: "prod/" + id, Status: backup.StatusQueued, CreatedAt: at, Method: backup.MethodPGDump, JobID: "bjob-pin"}
		if _, _, err := e.Catalog.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		_, _, _ = e.Catalog.UpdateStatus(ctx, id, backup.StatusUploading, nil)
		_, _, _ = e.Catalog.UpdateStatus(ctx, id, backup.StatusVerifying, nil)
		if _, _, err := e.Catalog.Finalize(ctx, id, backup.StatusVerifying, 10, strings.Repeat("a", 64), "s3://central-backups/prod/"+id, at); err != nil {
			t.Fatal(err)
		}
	}
	mk("bkp-old", now.Add(-48*time.Hour))
	mk("bkp-new", now.Add(-time.Hour))
	old, _, _ := e.Catalog.Get(ctx, "bkp-old")
	release, err := e.PinPlan(ctx, RestorePlan{Chain: []backup.Record{old}})
	if err != nil {
		t.Fatal(err)
	}
	job := backup.Job{ID: "bjob-pin", JobOptions: backup.JobOptions{Retention: backup.RetentionPolicy{KeepLast: 1}}}
	res, err := e.PruneJob(ctx, job, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Expired) != 0 || len(res.Errors) != 0 || len(res.Skipped) != 1 || res.Skipped[0] != "bkp-old" {
		t.Fatalf("prune during a restore = %+v", res)
	}
	if r, _, _ := e.Catalog.Get(ctx, "bkp-old"); r.Status != backup.StatusComplete {
		t.Fatalf("pinned restore point = %s", r.Status)
	}
	release()
	release() // idempotent
	// Released: retention claims it now (the storage delete then fails against the fake endpoint, but the
	// claim proves the pin is gone).
	cctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	res, _ = e.PruneJob(cctx, job, false)
	if len(res.Skipped) != 0 {
		t.Fatalf("prune after release = %+v", res)
	}
	if r, _, _ := e.Catalog.Get(ctx, "bkp-old"); r.Status == backup.StatusComplete {
		t.Fatalf("released restore point still complete: %+v", res)
	}
	// A record retention already claimed cannot be pinned for a restore.
	if _, err := e.PinRecords(ctx, []string{"bkp-old"}); err == nil {
		t.Fatal("pinned a restore point that retention is deleting")
	}
}
