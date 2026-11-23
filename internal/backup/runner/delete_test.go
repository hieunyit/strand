package runner

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/objectstore"
	"strand/internal/testutil"
	"strand/internal/web/store"
)

// PlanRestore never restores a record that is not complete: a failed/expired/unfinished chosen backup is
// refused with the status named, and a chain whose parent failed names the failed parent.
func TestPlanRestoreRefusesIncompleteRestorePoints(t *testing.T) {
	at := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	vt := at.Add(time.Minute)
	full := backup.Record{ID: "bkp-full", Source: "pg", Engine: "postgres", Status: backup.StatusComplete, CreatedAt: at, VerifiedAt: &vt, Method: backup.MethodPGDump}
	for _, st := range []backup.Status{backup.StatusFailed, backup.StatusExpired, backup.StatusUploading, backup.StatusQueued, backup.StatusVerifying} {
		bad := full
		bad.ID, bad.Status = "bkp-bad", st
		_, err := PlanRestore([]backup.Record{full}, &bad, nil)
		if err == nil || !errors.Is(err, backup.ErrNotComplete) || !strings.Contains(err.Error(), "restore point bkp-bad is "+string(st)) {
			t.Fatalf("%s chosen backup: err = %v", st, err)
		}
	}
	if p, err := PlanRestore([]backup.Record{full}, &full, nil); err != nil || p.Base().ID != full.ID {
		t.Fatalf("complete backup: %v %v", p, err)
	}
	parent := full
	parent.ID, parent.Status = "bkp-parent", backup.StatusFailed
	inc := full
	inc.ID, inc.Kind, inc.ParentID = "bkp-inc", backup.KindIncremental, parent.ID
	if _, err := PlanRestore([]backup.Record{parent, inc}, &inc, nil); err == nil || !strings.Contains(err.Error(), "bkp-parent is failed") {
		t.Fatalf("chain with a failed parent: %v", err)
	}
}

func TestPlanRestoreRejectsInvalidParentChain(t *testing.T) {
	head := backup.Record{ID: "head", ParentID: "parent", Kind: backup.KindIncremental, Status: backup.StatusComplete}
	parent := backup.Record{ID: "parent", ParentID: "head", Kind: backup.KindIncremental, Status: backup.StatusComplete}
	if _, err := PlanRestore([]backup.Record{head, parent}, &head, nil); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("circular incremental chain: %v", err)
	}
	parent.Kind, parent.ParentID = backup.KindLog, ""
	if _, err := PlanRestore([]backup.Record{head, parent}, &head, nil); err == nil || !strings.Contains(err.Error(), "log segment as parent") {
		t.Fatalf("log parent: %v", err)
	}
	parent.Kind, parent.Method = backup.KindFull, backup.MethodPGDump
	head.Method = backup.MethodPGDump
	if _, err := PlanRestore([]backup.Record{head, parent}, &head, nil); err == nil || !strings.Contains(err.Error(), "native restore tool") {
		t.Fatalf("legacy incremental requires native tooling: %v", err)
	}
	var env Env
	if err := env.Restore(t.Context(), RestorePlan{}, RestoreTarget{}); err == nil {
		t.Fatal("empty restore plan accepted")
	}
	if err := env.Restore(t.Context(), RestorePlan{Chain: []backup.Record{parent, head}}, RestoreTarget{}); err == nil || !strings.Contains(err.Error(), "native restore tool") {
		t.Fatalf("execution accepted an incremental chain: %v", err)
	}
}

func TestVerifyJobReportsCancellation(t *testing.T) {
	env := testEnv(t, nil, store.NewMem())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	report, err := env.VerifyJob(ctx, backup.Job{ID: "job"}, VerifyOptions{})
	if !errors.Is(err, context.Canceled) || report.Checked != 0 {
		t.Fatalf("canceled verify: checked=%d err=%v", report.Checked, err)
	}
}

func TestArchiveCursorSkipsNewerCopies(t *testing.T) {
	env := testEnv(t, nil, store.NewMem())
	ctx := t.Context()
	at := time.Now().UTC()
	original := backup.Record{ID: "segment", Source: "pg", JobID: "job", Engine: backup.EnginePostgres,
		Tool: backup.MethodWAL, Method: backup.MethodWAL, Bucket: "archive", ObjectKey: "segment",
		Kind: backup.KindLog, Status: backup.StatusQueued, CreatedAt: at}
	replica := original
	replica.ID, replica.ObjectKey, replica.CopyOf, replica.CreatedAt = "replica", "copies/segment", original.ID, at.Add(time.Minute)
	for _, r := range []backup.Record{original, replica} {
		if _, _, err := env.Catalog.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		for _, status := range []backup.Status{backup.StatusUploading, backup.StatusVerifying} {
			if _, _, err := env.Catalog.UpdateStatus(ctx, r.ID, status, nil); err != nil {
				t.Fatal(err)
			}
		}
		uri := "s3://" + r.Bucket + "/" + r.ObjectKey + ".json"
		if _, _, err := env.Catalog.Finalize(ctx, r.ID, backup.StatusVerifying, 1, strings.Repeat("a", 64), uri, r.CreatedAt); err != nil {
			t.Fatal(err)
		}
	}
	last, err := env.latestOriginalLog(ctx, original.JobID)
	if err != nil || last == nil || last.ID != original.ID {
		t.Fatalf("archive cursor selected %+v, err=%v", last, err)
	}
}

// DeleteRecord against a real S3 server (MinIO): a failed restore point loses its payload, its manifest
// and the unfinished multipart upload it left, then its catalog record; complete, held, object-locked and
// in-progress records are refused and keep their objects; an expired record (objects already deleted by
// retention) is removed from the catalog without touching its repository. Every attempt is audited.
func TestDeleteRecordRemovesPartialObjects(t *testing.T) {
	endpoint := testutil.MinIOEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	obj, err := objectstore.New(objectstore.Config{Endpoint: endpoint, Bucket: "central-backups", AccessKey: "minioadmin", SecretKey: "minioadmin", AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.CreateBucket(ctx); err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{"STRAND_BACKUP_S3_ENDPOINT": endpoint, "STRAND_BACKUP_S3_ACCESS_KEY": "minioadmin",
		"STRAND_BACKUP_S3_SECRET_KEY": "minioadmin", "STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL": "true", "STRAND_BACKUP_TMPDIR": t.TempDir()}
	st := store.NewMem()
	e := testEnv(t, vars, st)
	var audits []string
	e.Audit = func(_ context.Context, action, result string, detail map[string]any) error {
		audits = append(audits, action+":"+result+":"+detail["backup_id"].(string))
		return nil
	}
	now := time.Now().UTC().Truncate(time.Second)
	e.Now = func() time.Time { return now }

	put := func(key string) {
		if _, err := obj.Upload(ctx, key, strings.NewReader("partial "+key), -1, ""); err != nil {
			t.Fatal(err)
		}
	}
	exists := func(key string) bool { _, err := obj.Head(ctx, key); return err == nil }
	// mk creates a record, uploads its payload + manifest and moves it to status (failed by default).
	mk := func(id string, status backup.Status, meta *backup.Meta) backup.Record {
		r := backup.Record{ID: id, Source: "pg-prod", Engine: "postgres", Tool: "pg_dump", Bucket: "central-backups",
			ObjectKey: "prod/pg-prod/" + id + "/dump.sql", ManifestURI: "s3://central-backups/prod/pg-prod/" + id + "/manifest.json",
			Status: backup.StatusQueued, CreatedAt: now.Add(-time.Hour), Method: backup.MethodPGDump}
		if _, _, err := e.Catalog.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		put(r.ObjectKey)
		put("prod/pg-prod/" + id + "/manifest.json")
		if status != backup.StatusQueued {
			_, _, _ = e.Catalog.UpdateStatus(ctx, id, backup.StatusUploading, nil)
		}
		if meta != nil {
			if _, _, err := e.Catalog.UpdateMeta(ctx, id, "postgres", *meta); err != nil {
				t.Fatal(err)
			}
		}
		if status == backup.StatusFailed {
			_, _, _ = e.Catalog.UpdateStatus(ctx, id, backup.StatusFailed, nil)
		}
		got, _, _ := e.Catalog.Get(ctx, id)
		return got
	}

	// Failed: payload, manifest and the abandoned multipart upload are removed, then the record.
	failed := mk("bkp-failed", backup.StatusFailed, nil)
	if err := obj.InitiateForTest(ctx, failed.ObjectKey); err != nil {
		t.Fatal(err)
	}
	res, err := e.DeleteRecord(ctx, failed)
	if err != nil {
		t.Fatal(err)
	}
	if exists(failed.ObjectKey) || exists("prod/pg-prod/bkp-failed/manifest.json") || len(res.DeletedObjects) != 2 || res.AbortedUploads != 1 {
		t.Fatalf("objects left after delete: %+v", res)
	}
	if n, _ := obj.AbortStaleUploads(ctx, failed.ObjectKey); n != 0 {
		t.Fatalf("%d multipart uploads left", n)
	}
	if _, found, _ := e.Catalog.Get(ctx, failed.ID); found {
		t.Fatal("failed record still in the catalog")
	}

	// Refusals keep the objects and the record.
	held := mk("bkp-held", backup.StatusFailed, nil)
	if _, _, err := e.Catalog.SetLegalHold(ctx, held.ID, true); err != nil {
		t.Fatal(err)
	}
	held, _, _ = e.Catalog.Get(ctx, held.ID)
	until := now.Add(24 * time.Hour)
	locked := mk("bkp-locked", backup.StatusFailed, &backup.Meta{LockMode: "GOVERNANCE", RetainUntil: &until})
	running := mk("bkp-running", backup.StatusUploading, nil)
	for _, c := range []struct {
		rec  backup.Record
		want string
	}{{held, "legal hold"}, {locked, "object-locked"}, {running, "is uploading"}} {
		if _, err := e.DeleteRecord(ctx, c.rec); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: err = %v, want %q", c.rec.ID, err, c.want)
		}
		if !exists(c.rec.ObjectKey) {
			t.Fatalf("%s: payload deleted although the delete was refused", c.rec.ID)
		}
		if _, found, _ := e.Catalog.Get(ctx, c.rec.ID); !found {
			t.Fatalf("%s: record deleted although the delete was refused", c.rec.ID)
		}
	}
	complete := mk("bkp-complete", backup.StatusUploading, nil)
	_, _, _ = e.Catalog.UpdateStatus(ctx, complete.ID, backup.StatusVerifying, nil)
	complete, _, err = e.Catalog.Finalize(ctx, complete.ID, backup.StatusVerifying, 10, strings.Repeat("a", 64), complete.ManifestURI, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.DeleteRecord(ctx, complete); err == nil || !strings.Contains(err.Error(), "retention") || !exists(complete.ObjectKey) {
		t.Fatalf("complete restore point: err = %v", err)
	}

	// Expired: retention already deleted the objects; the repository is not contacted (it may be gone).
	exp := backup.Record{ID: "bkp-expired", Source: "pg-prod", Engine: "postgres", Tool: "pg_dump", Bucket: "central-backups",
		ObjectKey: "gone/dump.sql", Status: backup.StatusQueued, CreatedAt: now.Add(-48 * time.Hour), Repository: "removed-repo", Method: backup.MethodPGDump}
	if _, _, err := e.Catalog.Create(ctx, exp); err != nil {
		t.Fatal(err)
	}
	_, _, _ = e.Catalog.UpdateStatus(ctx, exp.ID, backup.StatusUploading, nil)
	_, _, _ = e.Catalog.UpdateStatus(ctx, exp.ID, backup.StatusVerifying, nil)
	if _, _, err := e.Catalog.Finalize(ctx, exp.ID, backup.StatusVerifying, 10, strings.Repeat("b", 64), "s3://central-backups/gone/m.json", now.Add(-47*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.Catalog.Expire(ctx, exp.ID, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	exp, _, _ = e.Catalog.Get(ctx, exp.ID)
	if res, err := e.DeleteRecord(ctx, exp); err != nil || len(res.DeletedObjects) != 0 {
		t.Fatalf("expired record: %+v %v", res, err)
	}
	if _, found, _ := e.Catalog.Get(ctx, exp.ID); found {
		t.Fatal("expired record still in the catalog")
	}
	want := []string{"backup.delete:ok:bkp-failed", "backup.delete:error:bkp-held", "backup.delete:error:bkp-locked",
		"backup.delete:error:bkp-running", "backup.delete:error:bkp-complete", "backup.delete:ok:bkp-expired"}
	if strings.Join(audits, ",") != strings.Join(want, ",") {
		t.Fatalf("audit = %v", audits)
	}
}

func TestBinlogBaseCoverageRequiresExactFileAndPosition(t *testing.T) {
	base := backup.Record{Position: backup.Position{BinlogFile: "binlog.000014", BinlogPos: 100}}
	log := backup.Record{Method: backup.MethodBinlog,
		Position:    backup.Position{BinlogFile: "binlog.000014", BinlogPos: 42},
		EndPosition: backup.Position{BinlogFile: "binlog.000014", BinlogPos: 200},
	}
	if !logCoversBase(log, base) {
		t.Fatal("base position within the archived range was refused")
	}
	log.Position.BinlogPos = 101
	if logCoversBase(log, base) {
		t.Fatal("segment starting after the base position was accepted")
	}
	log.Position.BinlogPos = 4
	log.Position.BinlogFile, log.EndPosition.BinlogFile = "binlog.000015", "binlog.000015"
	if logCoversBase(log, base) {
		t.Fatal("later binlog accepted after losing the base segment")
	}
}
