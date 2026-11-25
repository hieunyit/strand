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

// `strand backup delete --abandoned` of a restore point whose job is still marked running: refused while the
// run's process holds the job's execution lock, but a run left "running" by a dead process (the job disabled
// since, so no scheduler recovers it) is recovered and the abandoned record deleted.
func TestBackupDeleteAbandonedRecoversDeadRun(t *testing.T) {
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
	obj, err := objectstore.New(objectstore.Config{Endpoint: endpoint, Bucket: "stale", AccessKey: "minioadmin", SecretKey: "minioadmin", AllowInsecureLocal: true})
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

	j := schedJob("bjob-stale", "h1")
	j.Bucket = "stale"
	cur := createDue(t, st, j, time.Now().UTC().Add(time.Hour))
	if _, ok, err := st.BeginBackupJobRun(ctx, j.ID, cur.UpdatedAt); err != nil || !ok {
		t.Fatalf("begin: %v %v", ok, err)
	}
	cur, _, _ = st.GetBackupJob(ctx, j.ID)
	cur.Enabled, cur.NextRunAt = false, nil
	if ok, err := st.UpdateBackupJob(ctx, cur); err != nil || !ok {
		t.Fatalf("disable: %v %v", ok, err)
	}
	cat, _ := backup.NewCatalog(st)
	r := backup.Record{ID: "bkp-stale", JobID: j.ID, Source: "s-bjob-stale", Engine: "mysql", Tool: "mysqldump", Bucket: "stale",
		ObjectKey: "s/bkp-stale/dump", ManifestURI: "s3://stale/s/bkp-stale/manifest.json",
		Status: backup.StatusQueued, CreatedAt: time.Now().UTC().Add(-time.Hour), Method: backup.MethodMySQLDump}
	if _, _, err := cat.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cat.UpdateStatus(ctx, r.ID, backup.StatusUploading, nil); err != nil {
		t.Fatal(err)
	}

	// A live run (its process holds the execution lock) keeps the refusal.
	held, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- st.Locked(ctx, "backup-job-execution:"+j.ID, func(context.Context) error {
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	_, _, err = runBackup(t, "delete", "--id", r.ID, "--abandoned")
	close(release)
	if lerr := <-done; lerr != nil {
		t.Fatal(lerr)
	}
	if exitOf(err) != exitcode.Rejected || err == nil || !strings.Contains(err.Error(), "is running") {
		t.Fatalf("delete during a live run: exit %d %v", exitOf(err), err)
	}

	if _, _, err := runBackup(t, "delete", "--id", r.ID, "--abandoned"); err != nil {
		t.Fatalf("delete abandoned after the run's process died: %v", err)
	}
	if _, found, _ := cat.Get(ctx, r.ID); found {
		t.Fatal("abandoned record left")
	}
	if got, _, _ := st.GetBackupJob(ctx, j.ID); got.LastRunStatus != backup.RunStatusInterrupted {
		t.Fatalf("dead run not recovered: %s", got.LastRunStatus)
	}
}
