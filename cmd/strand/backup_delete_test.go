package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/exitcode"
	"strand/internal/objectstore"
	"strand/internal/testutil"
	"strand/internal/web"
	"strand/internal/web/store"
)

// A restore point that is not complete cannot be restored, verified or copied from the CLI (exit 3, the
// status named); `strand backup delete` removes a failed one with its partial objects in S3 (MinIO), refuses
// a complete one, and deletes an in-progress record only with --abandoned. Every delete is audited.
func TestBackupDeleteAndIncompleteGuardsCLI(t *testing.T) {
	dsn := testutil.NewPostgresDSN(t)
	endpoint := testutil.MinIOEndpoint(t)
	old := cfgPath
	defer func() { cfgPath = old }()
	cfgPath = ""
	t.Setenv(web.EnvDatabaseDSN, dsn)
	t.Setenv("STRAND_BACKUP_S3_ENDPOINT", endpoint)
	t.Setenv("STRAND_BACKUP_S3_ACCESS_KEY", "minioadmin")
	t.Setenv("STRAND_BACKUP_S3_SECRET_KEY", "minioadmin")
	t.Setenv("STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL", "true")
	t.Setenv("STRAND_BACKUP_TMPDIR", t.TempDir())
	ctx := context.Background()
	obj, err := objectstore.New(objectstore.Config{Endpoint: endpoint, Bucket: "central", AccessKey: "minioadmin", SecretKey: "minioadmin", AllowInsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.CreateBucket(ctx); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, dsn, store.OpenOptions{MaxWait: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cat, _ := backup.NewCatalog(st)
	exists := func(key string) bool { _, err := obj.Head(ctx, key); return err == nil }
	mk := func(id string, upload bool) backup.Record {
		r := backup.Record{ID: id, Source: "pg-prod", Engine: "postgres", Tool: "pg_dump", Bucket: "central",
			ObjectKey: "prod/" + id + "/dump", ManifestURI: "s3://central/prod/" + id + "/manifest.json",
			Status: backup.StatusQueued, CreatedAt: time.Now().UTC().Add(-time.Hour), Method: backup.MethodPGDump}
		if _, _, err := cat.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		if _, _, err := cat.UpdateStatus(ctx, id, backup.StatusUploading, nil); err != nil {
			t.Fatal(err)
		}
		if upload {
			for _, k := range []string{r.ObjectKey, "prod/" + id + "/manifest.json"} {
				if _, err := obj.Upload(ctx, k, strings.NewReader("partial"), -1, ""); err != nil {
					t.Fatal(err)
				}
			}
		}
		return r
	}
	failed := mk("bkp-failed", true)
	if _, _, err := cat.UpdateStatus(ctx, failed.ID, backup.StatusFailed, nil); err != nil {
		t.Fatal(err)
	}

	// restore / verify / copy of a failed restore point: refused before any work, exit 3.
	for _, c := range []struct {
		args []string
		verb string
	}{
		{[]string{"restore", "--id", failed.ID, "--target-database", "newdb", "--target-host", "127.0.0.1", "--yes"}, "restored"},
		{[]string{"verify", "--id", failed.ID}, "verified"},
		{[]string{"copy", "--id", failed.ID, "--to-repository", "offsite"}, "copied"},
	} {
		_, _, err := runBackup(t, c.args...)
		want := "restore point bkp-failed is failed: only complete restore points can be " + c.verb
		if exitOf(err) != exitcode.Rejected || err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: exit %d err %v, want exit 3 and %q", c.args[0], exitOf(err), err, want)
		}
	}
	if cs, _ := cat.Checks(ctx, failed.ID, 10); len(cs) != 0 {
		t.Fatalf("a refused verify recorded checks: %v", cs)
	}

	// delete of the failed restore point removes its objects, then its record.
	out, _, err := runBackup(t, "delete", "--id", failed.ID)
	if err != nil || !strings.Contains(out, "restore point bkp-failed (failed) deleted; 2 object(s) removed") {
		t.Fatalf("delete failed: %v %s", err, out)
	}
	if exists(failed.ObjectKey) || exists("prod/bkp-failed/manifest.json") {
		t.Fatal("partial objects left in the repository")
	}
	if _, found, _ := cat.Get(ctx, failed.ID); found {
		t.Fatal("record left in the catalog")
	}

	// A complete restore point is never deleted by hand.
	done := mk("bkp-done", true)
	_, _, _ = cat.UpdateStatus(ctx, done.ID, backup.StatusVerifying, nil)
	if _, _, err := cat.Finalize(ctx, done.ID, backup.StatusVerifying, 7, strings.Repeat("a", 64), done.ManifestURI, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runBackup(t, "delete", "--id", done.ID); exitOf(err) != exitcode.Rejected || !strings.Contains(err.Error(), "retention") {
		t.Fatalf("delete complete: exit %d %v", exitOf(err), err)
	}
	if !exists(done.ObjectKey) {
		t.Fatal("complete payload deleted")
	}

	// In progress: refused without --abandoned; with it the record is marked failed and deleted.
	stuck := mk("bkp-stuck", true)
	if _, _, err := runBackup(t, "delete", "--id", stuck.ID); exitOf(err) != exitcode.Rejected || !strings.Contains(err.Error(), "still in progress") {
		t.Fatalf("delete in progress: exit %d %v", exitOf(err), err)
	}
	if !exists(stuck.ObjectKey) {
		t.Fatal("in-progress payload deleted")
	}
	if _, _, err := runBackup(t, "delete", "--id", stuck.ID, "--abandoned"); err != nil {
		t.Fatalf("delete abandoned: %v", err)
	}
	if exists(stuck.ObjectKey) {
		t.Fatal("abandoned payload left")
	}
	if _, found, _ := cat.Get(ctx, stuck.ID); found {
		t.Fatal("abandoned record left")
	}

	rows, err := st.ListAudit(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	var acts []string
	for _, r := range rows {
		acts = append(acts, r.Action+":"+r.Result)
		if strings.Contains(string(r.Data), "minioadmin") {
			t.Fatalf("audit row contains credentials: %s", r.Data)
		}
	}
	joined := strings.Join(acts, ",")
	if strings.Count(joined, "backup.delete:ok") != 2 || strings.Count(joined, "backup.delete:error") != 1 {
		t.Fatalf("audit = %s", joined)
	}
}
