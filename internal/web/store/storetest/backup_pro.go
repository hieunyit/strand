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

func completeBackup(t *testing.T, s store.Store, r backup.Record, meta backup.Meta) backup.Record {
	t.Helper()
	ctx := context.Background()
	cat, _ := backup.NewCatalog(s)
	if _, _, err := cat.Create(ctx, r); err != nil {
		t.Fatalf("create %s: %v", r.ID, err)
	}
	if _, _, err := cat.UpdateStatus(ctx, r.ID, backup.StatusUploading, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := cat.UpdateMeta(ctx, r.ID, r.Engine, meta); err != nil {
		t.Fatalf("meta %s: %v", r.ID, err)
	}
	if _, _, err := cat.UpdateStatus(ctx, r.ID, backup.StatusVerifying, nil); err != nil {
		t.Fatal(err)
	}
	got, _, err := cat.Finalize(ctx, r.ID, backup.StatusVerifying, 1234, strings.Repeat("a", 64), "s3://"+r.Bucket+"/"+r.ObjectKey, r.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func runBackupPro(t *testing.T, s store.Store) {
	ctx := context.Background()
	cat, _ := backup.NewCatalog(s)
	base := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Millisecond)
	start := base.Add(-time.Second)
	end := base.Add(time.Hour)
	lock := base.Add(100 * 24 * time.Hour)

	full := completeBackup(t, s, backup.Record{ID: "b-full", Source: "pg-prod", Engine: "postgres", Tool: "pg_basebackup",
		Bucket: "central-backups", ObjectKey: "pg/b-full/base.tar", Status: backup.StatusQueued, CreatedAt: base,
		JobID: "bjob-pg", Repository: "main", Method: backup.MethodPGBaseBackup, Kind: backup.KindFull},
		backup.Meta{Position: backup.Position{LSN: "0/3000028"}, EndPosition: backup.Position{LSN: "0/3000100"},
			StartTime: &start, Format: backup.FormatSBK1, Compression: "zstd", KeyID: "k1", PlainBytes: 99,
			PlainSHA256: strings.Repeat("B", 64), Details: map[string]string{"timeline": "1"}})
	if full.Kind != backup.KindFull || full.Format != backup.FormatSBK1 || full.KeyID != "k1" || full.PlainSHA256 != strings.Repeat("b", 64) ||
		full.Position.LSN != "0/3000028" || full.EndPosition.LSN != "0/3000100" || full.StartTime == nil || !full.StartTime.Equal(start) ||
		full.Details["timeline"] != "1" || full.JobID != "bjob-pg" || full.Repository != "main" {
		t.Fatalf("full round trip = %+v", full)
	}
	// Metadata is frozen once complete.
	if _, _, err := cat.UpdateMeta(ctx, full.ID, "postgres", backup.Meta{}); !errors.Is(err, backup.ErrTransition) {
		t.Fatalf("meta after complete: %v", err)
	}
	// Engine-specific positions are enforced.
	if _, _, err := cat.UpdateMeta(ctx, full.ID, "postgres", backup.Meta{Position: backup.Position{OplogTS: "1.1"}}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("oplog on postgres: %v", err)
	}
	wal := completeBackup(t, s, backup.Record{ID: "b-wal-1", Source: "pg-prod", Engine: "postgres", Tool: "strand",
		Bucket: "central-backups", ObjectKey: "pg/wal/000000010000000000000003", Status: backup.StatusQueued, CreatedAt: base.Add(time.Minute),
		JobID: "bjob-pg", Method: backup.MethodWAL, Kind: backup.KindLog},
		backup.Meta{Position: backup.Position{LSN: "0/3000000"}, EndPosition: backup.Position{LSN: "0/4000000"}, StartTime: &start, EndTime: &end,
			LockMode: "COMPLIANCE", RetainUntil: &lock, VersionID: "v-1"})
	if wal.LockMode != "COMPLIANCE" || wal.RetainUntil == nil || wal.VersionID != "v-1" {
		t.Fatalf("lock round trip = %+v", wal)
	}
	mongo := completeBackup(t, s, backup.Record{ID: "b-mongo", Source: "rs0", Engine: "mongodb", Tool: "mongodump",
		Bucket: "central-backups", ObjectKey: "mongo/b-mongo", Status: backup.StatusQueued, CreatedAt: base.Add(2 * time.Minute)},
		backup.Meta{Position: backup.Position{OplogTS: "1763600000.5"}})
	if mongo.Position.OplogTS != "1763600000.5" {
		t.Fatalf("mongo = %+v", mongo)
	}
	// Filters.
	if rs, _ := cat.List(ctx, backup.Filter{JobID: "bjob-pg", NotKind: backup.KindLog}); len(rs) != 1 || rs[0].ID != "b-full" {
		t.Fatalf("job filter = %v", ids(rs))
	}
	if rs, _ := cat.List(ctx, backup.Filter{Kind: backup.KindLog}); len(rs) != 1 || rs[0].ID != "b-wal-1" {
		t.Fatalf("kind filter = %v", ids(rs))
	}
	if rs, _ := cat.List(ctx, backup.Filter{Repository: "main"}); len(rs) != 1 {
		t.Fatalf("repo filter = %v", ids(rs))
	}
	// Expiry respects object lock and legal hold.
	if _, _, err := cat.Expire(ctx, wal.ID, base.Add(24*time.Hour)); err == nil || !strings.Contains(err.Error(), "object-locked") {
		t.Fatalf("expire locked: %v", err)
	}
	if _, ok, err := cat.Expire(ctx, wal.ID, lock.Add(time.Second)); err != nil || !ok {
		t.Fatalf("expire after lock: %v %v", ok, err)
	}
	if r, _, _ := cat.Get(ctx, wal.ID); r.Status != backup.StatusExpired || r.ExpiredAt == nil {
		t.Fatalf("expired = %+v", r)
	}
	if _, ok, err := cat.Expire(ctx, wal.ID, lock.Add(time.Hour)); err != nil || ok {
		t.Fatalf("second expire must be idempotent no-op: %v %v", ok, err)
	}
	if _, ok, err := cat.SetLegalHold(ctx, full.ID, true); err != nil || !ok {
		t.Fatalf("hold: %v %v", ok, err)
	}
	if _, _, err := cat.Expire(ctx, full.ID, time.Now()); err == nil {
		t.Fatal("legal hold must block expiry")
	}
	if _, _, err := cat.SetLegalHold(ctx, wal.ID, true); err == nil {
		t.Fatal("hold on expired backup accepted")
	}
	if _, _, err := cat.ClaimDeletion(ctx, full.ID, time.Now()); err == nil {
		t.Fatal("accepted hold must prevent claiming deletion")
	}
	_, _, _ = cat.SetLegalHold(ctx, full.ID, false)
	// Checks.
	now := time.Now().UTC().Truncate(time.Millisecond)
	if _, err := cat.AddCheck(ctx, backup.CheckResult{BackupID: full.ID, Kind: "verify", Status: backup.CheckFailed,
		StartedAt: now, FinishedAt: now.Add(time.Second), Detail: "sha mismatch", Metrics: map[string]string{"bytes": "1234"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.AddCheck(ctx, backup.CheckResult{BackupID: full.ID, Kind: "test_restore", Status: backup.CheckOK,
		StartedAt: now, FinishedAt: now.Add(2 * time.Second), Metrics: map[string]string{"rows": "42"}}); err != nil {
		t.Fatal(err)
	}
	cs, err := cat.Checks(ctx, full.ID, 10)
	if err != nil || len(cs) != 2 || cs[0].Kind != "test_restore" || cs[1].Metrics["bytes"] != "1234" {
		t.Fatalf("checks = %+v %v", cs, err)
	}
	if r, _, _ := cat.Get(ctx, full.ID); r.CheckStatus != backup.CheckOK || r.CheckedAt == nil {
		t.Fatalf("last check = %+v", r)
	}
	// Summaries.
	sums, err := s.BackupSummaries(ctx)
	if err != nil || len(sums) != 2 {
		t.Fatalf("summaries = %+v %v", sums, err)
	}
	for _, sm := range sums {
		if sm.Source == "pg-prod" && (sm.LastSuccess == nil || sm.LastSuccess.ID != "b-full" || sm.Complete != 1 || len(sm.RecentSizes) != 1) {
			t.Fatalf("pg summary = %+v", sm)
		}
	}
	// Repositories.
	repo := backup.Repo{ID: "offsite", Name: "Offsite", EnvPrefix: "STRAND_BACKUP_REPO_OFFSITE_", Bucket: "offsite-backups",
		LockMode: "GOVERNANCE", LockDays: 30, Retention: backup.RetentionPolicy{KeepDaily: 7}}
	must(t, s.CreateBackupRepo(ctx, repo))
	if err := s.CreateBackupRepo(ctx, repo); err == nil {
		t.Fatal("duplicate repo")
	}
	bad := repo
	bad.ID, bad.Name, bad.EnvPrefix = "x", "x", "STRAND_WEB_"
	if err := s.CreateBackupRepo(ctx, bad); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("env prefix must be restricted: %v", err)
	}
	got, ok, _ := s.GetBackupRepo(ctx, "offsite")
	if !ok || got.Retention.KeepDaily != 7 || got.LockDays != 30 {
		t.Fatalf("repo = %+v", got)
	}
	got.Name = "Offsite DR"
	if ok, err := s.UpdateBackupRepo(ctx, got); !ok || err != nil {
		t.Fatalf("update repo %v %v", ok, err)
	}
	if rs, _ := s.ListBackupRepos(ctx); len(rs) != 1 || rs[0].Name != "Offsite DR" {
		t.Fatalf("repos = %+v", rs)
	}
	if _, err := s.DeleteBackupRepo(ctx, "main"); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("a repository that still holds backups must not be deleted: %v", err)
	}
	if ok, err := s.DeleteBackupRepo(ctx, "offsite"); !ok || err != nil {
		t.Fatalf("delete repo %v %v", ok, err)
	}
	for i := 0; i < 2; i++ {
		claimed, ok, err := cat.ClaimDeletion(ctx, full.ID, now)
		if err != nil || !ok || claimed.Status != backup.StatusDeleting {
			t.Fatalf("claim or resume deletion: %+v, %v", claimed, err)
		}
	}
	if _, _, err := cat.SetLegalHold(ctx, full.ID, true); err == nil {
		t.Fatal("hold accepted after retention reserved deletion")
	}
	if _, ok, err := cat.Expire(ctx, full.ID, now); err != nil || !ok {
		t.Fatalf("finish deletion: %v, %v", ok, err)
	}

}

func runBackupRetry(t *testing.T, s store.Store) {
	ctx := context.Background()
	j := backup.Job{ID: "bjob-r", Name: "retry", EverySeconds: 3600, Bucket: "central-backups", Enabled: true,
		RetryMax: 2, RetryBackoffSeconds: 60,
		JobOptions: backup.JobOptions{Method: backup.MethodMySQLDump, Source: "mysql-prod",
			Conn:      backup.ConnSpec{Host: "db1", Port: 3306, User: "backup", PasswordEnv: "MYSQL_BACKUP_PASSWORD", Databases: []string{"shop"}},
			Retention: backup.RetentionPolicy{KeepLast: 3, KeepDaily: 7}, Compress: true, Encrypt: true}}
	must(t, s.CreateBackupJob(ctx, j))
	got, _, _ := s.GetBackupJob(ctx, j.ID)
	if got.Method != backup.MethodMySQLDump || got.Conn.Databases[0] != "shop" || got.Retention.KeepDaily != 7 || got.RetryMax != 2 || !got.Encrypt {
		t.Fatalf("options round trip = %+v", got)
	}
	run := func(status backup.RunStatus) backup.Job {
		cur, _, _ := s.GetBackupJob(ctx, j.ID)
		started, ok, err := s.BeginBackupJobRun(ctx, j.ID, cur.UpdatedAt)
		if !ok || err != nil {
			t.Fatalf("begin %v %v", ok, err)
		}
		if ok, err := s.FinishBackupJobRun(ctx, j.ID, started, status); !ok || err != nil {
			t.Fatalf("finish %v %v", ok, err)
		}
		after, _, _ := s.GetBackupJob(ctx, j.ID)
		return after
	}
	in := func(jb backup.Job) time.Duration { return jb.NextRunAt.Sub(*jb.RunFinishedAt) }
	near := func(d, want time.Duration) bool { return d > want-5*time.Second && d < want+5*time.Second }
	a := run(backup.RunStatusFailed)
	if a.ConsecutiveFailures != 1 || !near(in(a), time.Minute) {
		t.Fatalf("first retry after %v (failures %d), want 1m", in(a), a.ConsecutiveFailures)
	}
	b := run(backup.RunStatusFailed)
	if b.ConsecutiveFailures != 2 || !near(in(b), 2*time.Minute) {
		t.Fatalf("second retry after %v, want 2m (exponential)", in(b))
	}
	c := run(backup.RunStatusFailed)
	if c.ConsecutiveFailures != 3 || !near(in(c), time.Hour) {
		t.Fatalf("after retry_max the interval applies: %v", in(c))
	}
	d := run(backup.RunStatusSucceeded)
	if d.ConsecutiveFailures != 0 || !near(in(d), time.Hour) {
		t.Fatalf("success resets: %+v", d)
	}
	// Notification and verification claims happen once.
	if ok, _ := s.MarkBackupJobNotified(ctx, j.ID, *d.RunFinishedAt); !ok {
		t.Fatal("first notify claim")
	}
	if ok, _ := s.MarkBackupJobNotified(ctx, j.ID, *d.RunFinishedAt); ok {
		t.Fatal("second notify claim must fail")
	}
	if ok, _ := s.MarkBackupJobMissed(ctx, j.ID, *d.NextRunAt); !ok {
		t.Fatal("first missed claim")
	}
	if ok, _ := s.MarkBackupJobMissed(ctx, j.ID, *d.NextRunAt); ok {
		t.Fatal("second missed claim must fail")
	}
	now := time.Now().UTC()
	if ok, _ := s.ClaimBackupJobVerify(ctx, j.ID, now, time.Hour); !ok {
		t.Fatal("verify claim")
	}
	if ok, _ := s.ClaimBackupJobVerify(ctx, j.ID, now.Add(time.Minute), time.Hour); ok {
		t.Fatal("verify claim within interval must fail")
	}
	if ok, _ := s.ClaimBackupJobVerify(ctx, j.ID, now.Add(2*time.Hour), time.Hour); !ok {
		t.Fatal("verify claim after interval")
	}
	if ok, _ := s.ClaimBackupJobPrune(ctx, j.ID, now, time.Hour); !ok {
		t.Fatal("prune claim")
	}
}
