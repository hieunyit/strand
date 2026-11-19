// Package storetest is the behavioural contract every store.Store implementation must satisfy.
package storetest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// Run runs the contract against a fresh store returned by mk (called once per subtest).
func Run(t *testing.T, mk func(t *testing.T) store.Store) {
	ctx := context.Background()
	t.Run("admin", func(t *testing.T) {
		s := mk(t)
		if _, ok, err := s.Admin(ctx); err != nil || ok {
			t.Fatalf("fresh store has admin: ok=%v err=%v", ok, err)
		}
		must(t, s.SetPasswordHash(ctx, "pw1"))
		must(t, s.SetAPITokenHash(ctx, "tok1"))
		a, ok, _ := s.Admin(ctx)
		if !ok || a.PasswordHash != "pw1" || a.APITokenHash != "tok1" {
			t.Fatalf("admin = %+v", a)
		}
		must(t, s.SetPasswordHash(ctx, "pw2")) // does not clobber the token
		a, _, _ = s.Admin(ctx)
		if a.PasswordHash != "pw2" || a.APITokenHash != "tok1" {
			t.Fatalf("admin after passwd = %+v", a)
		}
	})
	t.Run("sessions", func(t *testing.T) {
		s := mk(t)
		now := time.Now().UTC().Truncate(time.Millisecond)
		must(t, s.PutSession(ctx, store.Session{IDHash: "a", CSRF: "c", IP: "1.2.3.4", Created: now, LastSeen: now}))
		must(t, s.PutSession(ctx, store.Session{IDHash: "old", CSRF: "c", Created: now.Add(-13 * time.Hour), LastSeen: now}))
		must(t, s.PutSession(ctx, store.Session{IDHash: "idle", CSRF: "c", Created: now, LastSeen: now.Add(-time.Hour)}))
		got, ok, err := s.GetSession(ctx, "a")
		if err != nil || !ok || got.CSRF != "c" || got.IP != "1.2.3.4" || !got.Created.Equal(now) {
			t.Fatalf("get = %+v ok=%v err=%v", got, ok, err)
		}
		must(t, s.TouchSession(ctx, "a", now.Add(time.Minute)))
		got, _, _ = s.GetSession(ctx, "a")
		if !got.LastSeen.Equal(now.Add(time.Minute)) {
			t.Fatalf("touch: %v", got.LastSeen)
		}
		must(t, s.PurgeSessions(ctx, now.Add(-30*time.Minute), now.Add(-12*time.Hour)))
		if n, _ := s.CountSessions(ctx); n != 1 {
			t.Fatalf("after purge %d sessions, want 1", n)
		}
		must(t, s.DeleteSession(ctx, "a"))
		if _, ok, _ := s.GetSession(ctx, "a"); ok {
			t.Fatal("deleted session still there")
		}
	})
	t.Run("pipelines_and_desired", func(t *testing.T) {
		s := mk(t)
		p := store.Pipeline{Name: "orders", ConfigPath: "/etc/strand/orders.yaml", EnvPath: "/etc/strand/orders.env",
			ConfigYAML: "version: 1\n", HasConfig: true, EnvEnc: []byte{1, 2, 3}}
		must(t, s.UpsertPipeline(ctx, p))
		must(t, s.UpsertPipeline(ctx, store.Pipeline{Name: "bare", ConfigPath: "/x.yaml"}))
		got, ok, _ := s.GetPipeline(ctx, "orders")
		if !ok || got.ConfigYAML != "version: 1\n" || !got.HasConfig || string(got.EnvEnc) != "\x01\x02\x03" || got.EnvPath != p.EnvPath {
			t.Fatalf("pipeline = %+v", got)
		}
		bare, _, _ := s.GetPipeline(ctx, "bare")
		if bare.HasConfig || bare.EnvEnc != nil && len(bare.EnvEnc) != 0 {
			t.Fatalf("bare pipeline = %+v", bare)
		}
		p.ConfigYAML = "version: 2\n"
		must(t, s.UpsertPipeline(ctx, p))
		l, _ := s.ListPipelines(ctx)
		if len(l) != 2 || l[0].Name != "bare" || l[1].ConfigYAML != "version: 2\n" {
			t.Fatalf("list = %+v", l)
		}
		if got.Origin != store.OriginCreated || got.Conflicts != "[]" || !got.MissingSince.IsZero() {
			t.Fatalf("defaults: origin %q conflicts %q missing %v", got.Origin, got.Conflicts, got.MissingSince)
		}
		// Discovery: CreatePipeline never overwrites.
		ok2, err := s.CreatePipeline(ctx, store.Pipeline{Name: "orders", ConfigPath: "/other.yaml", Origin: store.OriginScan})
		must(t, err)
		if ok2 {
			t.Fatal("CreatePipeline overwrote an existing row")
		}
		miss := time.Now().UTC().Truncate(time.Second)
		ok2, err = s.CreatePipeline(ctx, store.Pipeline{Name: "scanned", ConfigPath: "/s.yaml", Origin: store.OriginScan, MissingSince: miss,
			Conflicts: `[{"file": "config", "path": "/s.yaml.conflict-1", "at": "2026-01-01T00:00:00Z"}]`})
		must(t, err)
		sc, _, _ := s.GetPipeline(ctx, "scanned")
		if !ok2 || sc.Origin != store.OriginScan || !sc.MissingSince.Equal(miss) || !strings.Contains(sc.Conflicts, "conflict-1") {
			t.Fatalf("scanned = %+v", sc)
		}
		// Cập nhật metadata không đụng config/env/conflicts.
		sc.ConfigPath, sc.MissingSince, sc.ConfigYAML = "/moved.yaml", time.Time{}, "IGNORED"
		must(t, s.UpdatePipelineMeta(ctx, sc))
		must(t, s.SetPipelineConflicts(ctx, "scanned", `[]`))
		sc2, _, _ := s.GetPipeline(ctx, "scanned")
		if sc2.ConfigPath != "/moved.yaml" || !sc2.MissingSince.IsZero() || sc2.ConfigYAML == "IGNORED" || sc2.Conflicts != "[]" {
			t.Fatalf("meta update = %+v", sc2)
		}
		must(t, s.SetDesired(ctx, "scanned", store.DesiredRunning))
		must(t, s.DeletePipeline(ctx, "scanned"))
		if _, ok, _ := s.GetPipeline(ctx, "scanned"); ok {
			t.Fatal("pipeline not deleted")
		}
		if d, _ := s.Desired(ctx); d["scanned"] != "" {
			t.Fatal("desired state of a deleted pipeline kept")
		}
		if err := s.SetDesired(ctx, "orders", "bogus"); err == nil {
			t.Fatal("bogus desired state accepted")
		}
		must(t, s.SetDesired(ctx, "orders", store.DesiredRunning))
		must(t, s.SetDesired(ctx, "bare", store.DesiredRunning))
		must(t, s.SetDesired(ctx, "bare", store.DesiredStopped))
		d, _ := s.Desired(ctx)
		if d["orders"] != "running" || d["bare"] != "stopped" || len(d) != 2 {
			t.Fatalf("desired = %v", d)
		}
		// ObserveStop: running -> stopped (reason observed_stop) once; never touches a stopped/unknown row.
		if ch, err := s.ObserveStop(ctx, "orders"); err != nil || !ch {
			t.Fatalf("observe stop of a running pipeline: changed=%v err=%v", ch, err)
		}
		if ch, _ := s.ObserveStop(ctx, "orders"); ch {
			t.Fatal("observe stop changed an already stopped pipeline")
		}
		if ch, _ := s.ObserveStop(ctx, "nobody"); ch {
			t.Fatal("observe stop created a row")
		}
		ds, err := s.DesiredStates(ctx)
		must(t, err)
		if ds["orders"].Desired != "stopped" || ds["orders"].Reason != store.ReasonObservedStop || ds["orders"].Updated.IsZero() || ds["bare"].Reason != "" {
			t.Fatalf("desired states = %+v", ds)
		}
		must(t, s.SetDesired(ctx, "orders", store.DesiredRunning)) // a web Start clears the reason
		if ds, _ = s.DesiredStates(ctx); ds["orders"].Desired != "running" || ds["orders"].Reason != "" {
			t.Fatalf("desired after web start = %+v", ds["orders"])
		}
		must(t, s.DeletePipeline(ctx, "orders"))
		if _, ok, _ := s.GetPipeline(ctx, "orders"); ok {
			t.Fatal("deleted pipeline still there")
		}
	})
	t.Run("jobs_wizards", func(t *testing.T) {
		s := mk(t)
		t0 := time.Now().UTC().Truncate(time.Millisecond)
		must(t, s.UpsertJob(ctx, store.Job{ID: "j1", Pipeline: "p", Started: t0, Running: true, Data: []byte(`{"id":"j1"}`)}))
		must(t, s.UpsertJob(ctx, store.Job{ID: "j2", Pipeline: "p", Started: t0.Add(time.Second), Data: []byte(`{"id":"j2"}`)}))
		must(t, s.UpsertJob(ctx, store.Job{ID: "j1", Pipeline: "p", Started: t0, Running: false, Data: []byte(`{"id":"j1","done":true}`)}))
		jobs, _ := s.ListJobs(ctx)
		if len(jobs) != 2 || jobs[0].ID != "j2" || jobs[1].Running || !strings.Contains(string(jobs[1].Data), "done") {
			t.Fatalf("jobs = %+v", jobs)
		}
		// Output: chunk theo offset, idempotent, đọc từ giữa chunk, xóa theo job.
		must(t, s.AppendJobOutput(ctx, "j2", 0, []byte("hello ")))
		must(t, s.AppendJobOutput(ctx, "j2", 6, []byte("world\n")))
		must(t, s.AppendJobOutput(ctx, "j2", 6, []byte("XXXXX\n"))) // re-ship: giữ bản đầu
		if n, _ := s.JobOutputSize(ctx, "j2"); n != 12 {
			t.Fatalf("output size = %d", n)
		}
		if b, _ := s.ReadJobOutput(ctx, "j2", 3, 100); string(b) != "lo world\n" {
			t.Fatalf("output from 3 = %q", b)
		}
		if b, _ := s.ReadJobOutput(ctx, "j2", 0, 8); string(b) != "hello wo" {
			t.Fatalf("output max 8 = %q", b)
		}
		if b, _ := s.ReadJobOutput(ctx, "j2", 12, 8); len(b) != 0 {
			t.Fatalf("output past end = %q", b)
		}
		if err := s.AppendJobOutput(ctx, "nojob", 0, []byte("x")); err == nil {
			t.Fatal("output for an unknown job accepted")
		}
		must(t, s.DeleteJob(ctx, "j2"))
		if b, _ := s.ReadJobOutput(ctx, "j2", 0, 100); len(b) != 0 {
			t.Fatalf("output survived job delete: %q", b)
		}
		if jobs, _ = s.ListJobs(ctx); len(jobs) != 1 {
			t.Fatalf("jobs after delete = %d", len(jobs))
		}
		if _, ok, _ := s.GetWizard(ctx, "repair", "p"); ok {
			t.Fatal("phantom wizard")
		}
		must(t, s.PutWizard(ctx, "repair", "p", []byte(`{"step":"a"}`)))
		must(t, s.PutWizard(ctx, "repair", "p", []byte(`{"step":"b"}`)))
		w, ok, _ := s.GetWizard(ctx, "repair", "p")
		if !ok || !strings.Contains(string(w), `"b"`) {
			t.Fatalf("wizard = %s", w)
		}
	})
	t.Run("audit_and_meta", func(t *testing.T) {
		s := mk(t)
		for i, a := range []string{"login", "start", "stop"} {
			must(t, s.AppendAudit(ctx, store.AuditRow{Time: time.Now().Add(time.Duration(i) * time.Second), Action: a, Pipeline: "p", Result: "ok", Data: []byte(`{"action":"` + a + `"}`)}))
		}
		rows, _ := s.ListAudit(ctx, 2)
		if len(rows) != 2 || rows[0].Action != "stop" || rows[1].Action != "start" {
			t.Fatalf("audit = %+v", rows)
		}
		if _, ok, _ := s.MetaGet(ctx, "k"); ok {
			t.Fatal("phantom meta")
		}
		must(t, s.AppendAuditBatch(ctx, []store.AuditRow{{Time: time.Now(), Action: "b1", Data: []byte(`{}`)}, {Time: time.Now(), Action: "b2", Data: []byte(`{}`)}},
			&store.MetaKV{Key: "prog", Value: "2"}))
		if rows, _ := s.ListAudit(ctx, 1); len(rows) != 1 || rows[0].Action != "b2" {
			t.Fatalf("batch audit = %+v", rows)
		}
		if v, ok, _ := s.MetaGet(ctx, "prog"); !ok || v != "2" {
			t.Fatalf("batch progress = %q %v", v, ok)
		}
		must(t, s.MetaSet(ctx, "k", "v1"))
		must(t, s.MetaSet(ctx, "k", "v2"))
		if v, ok, _ := s.MetaGet(ctx, "k"); !ok || v != "v2" {
			t.Fatalf("meta = %q", v)
		}
	})
	t.Run("audit_archive", func(t *testing.T) {
		s := mk(t)
		old := time.Now().Add(-400 * 24 * time.Hour)
		for i := 0; i < 5; i++ {
			must(t, s.AppendAudit(ctx, store.AuditRow{Time: old.Add(time.Duration(i) * time.Hour), Action: "start", Pipeline: "p", Result: "ok", Data: []byte(`{"i":` + string(rune('0'+i)) + `}`)}))
		}
		must(t, s.AppendAudit(ctx, store.AuditRow{Time: time.Now(), Action: "login", Result: "ok", Data: []byte(`{}`)}))
		before := time.Now().Add(-365 * 24 * time.Hour)
		pl, err := s.PlanAuditArchive(ctx, before)
		must(t, err)
		if pl.Count != 5 || pl.MaxID <= pl.MinID || !pl.MinTS.Before(pl.MaxTS) {
			t.Fatalf("plan = %+v", pl)
		}
		var ids []int64
		must(t, s.ScanAudit(ctx, before, pl.MaxID, func(r store.AuditRow) error { ids = append(ids, r.ID); return nil }))
		if len(ids) != 5 || ids[0] != pl.MinID || ids[4] != pl.MaxID {
			t.Fatalf("scan ids = %v", ids)
		}
		// Count mismatch (rows changed since the export): nothing deleted.
		if _, err := s.ArchiveAudit(ctx, store.AuditArchive{Before: before, MaxID: pl.MaxID, Count: 4, File: "a.jsonl.gz", SHA256: strings.Repeat("a", 64)}); err == nil {
			t.Fatal("archive with a wrong count accepted")
		}
		// Cutoff younger than a day: refused.
		if _, err := s.ArchiveAudit(ctx, store.AuditArchive{Before: time.Now(), MaxID: 1 << 40, Count: 6, File: "x", SHA256: strings.Repeat("a", 64)}); err == nil {
			t.Fatal("archive of recent rows accepted")
		}
		n, err := s.ArchiveAudit(ctx, store.AuditArchive{Before: before, MaxID: pl.MaxID, Count: 5, File: "a.jsonl.gz", SHA256: strings.Repeat("b", 64),
			Detail: map[string]any{"by": "test"}})
		if err != nil || n != 5 {
			t.Fatalf("archive: n=%d err=%v", n, err)
		}
		rows, _ := s.ListAudit(ctx, 10)
		if len(rows) != 2 || rows[0].Action != store.AuditArchiveAction || rows[1].Action != "login" {
			t.Fatalf("audit after archive = %+v", rows)
		}
		if !strings.Contains(string(rows[0].Data), strings.Repeat("b", 64)) || !strings.Contains(string(rows[0].Data), `"rows"`) {
			t.Fatalf("archive entry = %s", rows[0].Data)
		}
		ar, err := s.ListAuditArchives(ctx, 5)
		must(t, err)
		if len(ar) != 1 || ar[0].Count != 5 || ar[0].MaxID != pl.MaxID || ar[0].SHA256 != strings.Repeat("b", 64) {
			t.Fatalf("archives = %+v", ar)
		}
		// New rows get new ids; the archive entry itself is never archivable.
		must(t, s.AppendAudit(ctx, store.AuditRow{Time: old, Action: "late-import", Data: []byte(`{}`)}))
		rows, _ = s.ListAudit(ctx, 1)
		if rows[0].ID <= ar[0].MaxID+1 {
			t.Fatalf("audit id reused: %d", rows[0].ID)
		}
		if pl, _ = s.PlanAuditArchive(ctx, time.Now().Add(-25*time.Hour)); pl.Count != 1 {
			t.Fatalf("plan after archive = %+v (audit.archive must not be archivable)", pl)
		}
	})
	t.Run("lease", func(t *testing.T) {
		s := mk(t)
		a, err := s.TryLease(ctx, "A", `{"host":"a"}`, 400*time.Millisecond)
		must(t, err)
		if !a.Held || a.Holder != "A" || a.Epoch != 1 || a.Remaining <= 0 {
			t.Fatalf("first acquire = %+v", a)
		}
		b, err := s.TryLease(ctx, "B", `{}`, time.Second)
		must(t, err)
		if b.Held || b.Holder != "A" || !strings.Contains(b.Info, `"a"`) {
			t.Fatalf("B took a live lease: %+v", b)
		}
		if a, _ = s.TryLease(ctx, "A", `{}`, 400*time.Millisecond); !a.Held || a.Epoch != 1 {
			t.Fatalf("renew = %+v", a)
		}
		time.Sleep(500 * time.Millisecond) // A stops renewing: the lease expires
		b, _ = s.TryLease(ctx, "B", `{}`, time.Second)
		if !b.Held || b.Holder != "B" || b.Epoch != 2 {
			t.Fatalf("takeover after expiry = %+v", b)
		}
		if a, _ = s.TryLease(ctx, "A", `{}`, time.Second); a.Held {
			t.Fatalf("old holder got the lease back while B holds it: %+v", a)
		}
		must(t, s.ReleaseLease(ctx, "A")) // not the holder: no effect
		if b, _ = s.TryLease(ctx, "B", `{}`, time.Second); !b.Held || b.Epoch != 2 {
			t.Fatalf("release by a non-holder changed the lease: %+v", b)
		}
		must(t, s.ReleaseLease(ctx, "B"))
		if a, _ = s.TryLease(ctx, "A", `{}`, time.Second); !a.Held || a.Epoch != 3 {
			t.Fatalf("acquire after release = %+v", a)
		}
		must(t, s.HeartbeatInstance(ctx, "A", `{"host":"a"}`, "active"))
		must(t, s.HeartbeatInstance(ctx, "B", `{}`, "standby"))
		must(t, s.HeartbeatInstance(ctx, "B", `{}`, "active"))
		in, err := s.ListInstances(ctx)
		must(t, err)
		if len(in) != 2 || in[1].ID != "B" || in[1].Role != "active" || in[0].LastSeen.IsZero() {
			t.Fatalf("instances = %+v", in)
		}
		must(t, s.RemoveInstance(ctx, "A"))
		if in, _ = s.ListInstances(ctx); len(in) != 1 {
			t.Fatalf("instances after remove = %+v", in)
		}
	})
	t.Run("settings", func(t *testing.T) {
		s := mk(t)
		must(t, s.PutSetting(ctx, "scan", `["/a/*.yaml"]`))
		must(t, s.PutSetting(ctx, "scan", `["/b/*.yaml"]`))
		must(t, s.PutSetting(ctx, "secure_cookies", `true`))
		m, err := s.Settings(ctx)
		must(t, err)
		if len(m) != 2 || !strings.Contains(m["scan"], "/b/") || m["secure_cookies"] != "true" {
			t.Fatalf("settings = %v", m)
		}
		must(t, s.DeleteSetting(ctx, "scan"))
		if m, _ = s.Settings(ctx); len(m) != 1 {
			t.Fatalf("settings after delete = %v", m)
		}
	})
	t.Run("metrics_history", func(t *testing.T) {
		s := mk(t)
		base := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
		var rows []store.MetricSample
		for i := 0; i < 36; i++ { // 3 phút, mỗi 5 s
			rows = append(rows, store.MetricSample{Pipeline: "p", T: base.Add(time.Duration(i) * 5 * time.Second), State: "synced", Lag: float64(i), Throughput: 10})
		}
		must(t, s.AppendMetrics(ctx, rows))
		must(t, s.AppendMetrics(ctx, rows[:3])) // trùng: bỏ qua
		got, err := s.QueryMetrics(ctx, "p", base, base.Add(time.Hour), time.Minute, false)
		must(t, err)
		if len(got) != 3 || got[0].Lag != 5.5 || got[0].LagMax != 11 || got[2].Throughput != 10 || got[0].State != "synced" {
			t.Fatalf("raw minute buckets = %+v", got)
		}
		// Rollup các phút trước base+2m, xóa raw trước base+1m.
		must(t, s.MaintainMetrics(ctx, base.Add(2*time.Minute), base.Add(time.Minute), base.Add(-time.Hour)))
		raw, _ := s.QueryMetrics(ctx, "p", base, base.Add(time.Hour), 5*time.Second, false)
		if len(raw) != 24 {
			t.Fatalf("raw after maintain = %d, want 24", len(raw))
		}
		ru, _ := s.QueryMetrics(ctx, "p", base, base.Add(time.Hour), time.Minute, true)
		if len(ru) != 2 || ru[0].Lag != 5.5 || ru[0].LagMax != 11 || ru[1].LagMax != 23 {
			t.Fatalf("rollup = %+v", ru)
		}
		// Chạy lại: idempotent; rồi hết hạn rollup.
		must(t, s.MaintainMetrics(ctx, base.Add(2*time.Minute), base.Add(time.Minute), base.Add(-time.Hour)))
		if ru2, _ := s.QueryMetrics(ctx, "p", base, base.Add(time.Hour), time.Minute, true); len(ru2) != 2 || ru2[1].Lag != ru[1].Lag {
			t.Fatalf("rollup not idempotent: %+v", ru2)
		}
		must(t, s.MaintainMetrics(ctx, base.Add(2*time.Minute), base.Add(time.Minute), base.Add(90*time.Second)))
		if ru3, _ := s.QueryMetrics(ctx, "p", base, base.Add(time.Hour), time.Minute, true); len(ru3) != 0 {
			t.Fatalf("rollup retention: %+v", ru3)
		}
	})
	t.Run("locked_is_exclusive", func(t *testing.T) {
		s := mk(t)
		var inside, maxInside int32
		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := s.Locked(ctx, "x", func(context.Context) error {
					n := atomic.AddInt32(&inside, 1)
					for {
						m := atomic.LoadInt32(&maxInside)
						if n <= m || atomic.CompareAndSwapInt32(&maxInside, m, n) {
							break
						}
					}
					time.Sleep(20 * time.Millisecond)
					atomic.AddInt32(&inside, -1)
					return nil
				})
				if err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if maxInside != 1 {
			t.Fatalf("lock not exclusive: %d holders at once", maxInside)
		}
	})
	t.Run("backups", func(t *testing.T) { runBackups(t, mk(t)) })
	t.Run("backup_jobs", func(t *testing.T) { runBackupJobs(t, mk(t)) })
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func runBackups(t *testing.T, s store.Store) {
	ctx := context.Background()
	cat, err := backup.NewCatalog(s)
	must(t, err)
	base := time.Now().UTC().Truncate(time.Millisecond).Add(-time.Hour)
	rec := func(id, source string, at time.Time) backup.Record {
		return backup.Record{ID: id, Source: source, Engine: "mysql", Tool: "mysqldump", ToolVersion: "8.4",
			Bucket: "central-backups", ObjectKey: "prod/" + id + "/artifact", ManifestURI: "s3://central-backups/prod/" + id + "/manifest.json",
			Position: backup.Position{GTIDSet: "uuid:1-10"}, Status: backup.StatusQueued, CreatedAt: at}
	}
	r1 := rec("b1", "db1", base)
	got, inserted, err := cat.Create(ctx, r1)
	if err != nil || !inserted || got.ID != "b1" {
		t.Fatalf("create: %+v %v %v", got, inserted, err)
	}
	// Idempotent retry with the same identity; conflicting identity or object key is rejected.
	if _, inserted, err := cat.Create(ctx, r1); err != nil || inserted {
		t.Fatalf("retry create: inserted=%v err=%v", inserted, err)
	}
	other := r1
	other.Source = "db2"
	if _, _, err := cat.Create(ctx, other); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("conflicting identity err = %v", err)
	}
	sameKey := rec("b9", "db1", base)
	sameKey.ObjectKey = r1.ObjectKey
	if _, _, err := cat.Create(ctx, sameKey); !errors.Is(err, backup.ErrConflict) {
		t.Fatalf("duplicate object key err = %v", err)
	}
	// Records cannot be created already complete.
	done := rec("b8", "db1", base)
	done.Status = backup.StatusComplete
	if _, _, err := cat.Create(ctx, done); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("create complete err = %v", err)
	}

	// Lifecycle: queued -> uploading -> verifying -> complete (finalize) -> expired.
	if _, _, err := cat.UpdateStatus(ctx, "b1", backup.StatusComplete, nil); err == nil {
		t.Fatal("queued -> complete must be rejected")
	}
	if _, changed, err := cat.UpdateStatus(ctx, "b1", backup.StatusUploading, nil); err != nil || !changed {
		t.Fatalf("to uploading: %v %v", changed, err)
	}
	if _, _, err := cat.Finalize(ctx, "b1", backup.StatusVerifying, 10, strings.Repeat("a", 64), r1.ManifestURI, time.Now()); err == nil {
		t.Fatal("finalize from uploading must fail")
	}
	if _, _, err := cat.UpdateStatus(ctx, "b1", backup.StatusVerifying, nil); err != nil {
		t.Fatal(err)
	}
	sum := strings.Repeat("AB", 32)
	fin, changed, err := cat.Finalize(ctx, "b1", backup.StatusVerifying, 1234, sum, r1.ManifestURI, time.Now())
	if err != nil || !changed || fin.Status != backup.StatusComplete || fin.SHA256 != strings.ToLower(sum) || fin.Bytes != 1234 || fin.VerifiedAt == nil {
		t.Fatalf("finalize: %+v %v %v", fin, changed, err)
	}
	if _, changed, err := cat.Finalize(ctx, "b1", backup.StatusVerifying, 1234, sum, r1.ManifestURI, time.Now()); err != nil || changed {
		t.Fatalf("finalize retry must be an idempotent no-op: %v %v", changed, err)
	}
	if _, _, err := cat.Finalize(ctx, "b1", backup.StatusVerifying, 99, sum, r1.ManifestURI, time.Now()); !errors.Is(err, backup.ErrTransition) {
		t.Fatalf("finalize with different metadata err = %v", err)
	}
	if _, _, err := cat.UpdateStatus(ctx, "b1", backup.StatusUploading, nil); !errors.Is(err, backup.ErrTransition) {
		t.Fatalf("complete -> uploading err = %v", err)
	}

	// Legal hold blocks expiry.
	held := rec("b2", "db1", base.Add(time.Minute))
	held.LegalHold = true
	_, _, err = cat.Create(ctx, held)
	must(t, err)
	for _, st := range []backup.Status{backup.StatusUploading, backup.StatusVerifying} {
		_, _, err = cat.UpdateStatus(ctx, "b2", st, nil)
		must(t, err)
	}
	_, _, err = cat.Finalize(ctx, "b2", backup.StatusVerifying, 1, sum, held.ManifestURI, time.Now())
	must(t, err)
	if _, _, err := cat.UpdateStatus(ctx, "b2", backup.StatusExpired, nil); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("expire legal hold err = %v", err)
	}
	if _, _, err := s.UpdateBackupStatus(ctx, "b2", backup.StatusComplete, backup.StatusExpired, nil); err == nil {
		t.Fatal("store must refuse to expire a legal-hold backup even when called directly")
	}
	if _, _, err := cat.UpdateStatus(ctx, "b1", backup.StatusExpired, nil); err != nil {
		t.Fatal(err)
	}

	// Failed uploads can be retried.
	f := rec("b3", "db2", base.Add(2*time.Minute))
	f.Engine, f.Position = "postgres", backup.Position{LSN: "0/16B3748"}
	_, _, err = cat.Create(ctx, f)
	must(t, err)
	_, _, err = cat.UpdateStatus(ctx, "b3", backup.StatusFailed, nil)
	must(t, err)
	if _, changed, err := cat.UpdateStatus(ctx, "b3", backup.StatusUploading, nil); err != nil || !changed {
		t.Fatalf("retry failed: %v %v", changed, err)
	}
	// Stale expected status (another writer moved it) is a transition error, not a silent overwrite.
	if _, _, err := s.UpdateBackupStatus(ctx, "b3", backup.StatusQueued, backup.StatusFailed, nil); !errors.Is(err, backup.ErrTransition) {
		t.Fatalf("CAS with stale expected status err = %v", err)
	}

	// Listing: newest first, filters, limit.
	all, err := cat.List(ctx, backup.Filter{})
	if err != nil || len(all) != 3 || all[0].ID != "b3" || all[2].ID != "b1" {
		t.Fatalf("list all = %v err %v", ids(all), err)
	}
	if l, _ := cat.List(ctx, backup.Filter{Source: "db1"}); len(l) != 2 {
		t.Fatalf("source filter = %v", ids(l))
	}
	if l, _ := cat.List(ctx, backup.Filter{Engine: "postgres"}); len(l) != 1 || l[0].Position.LSN != "0/16B3748" {
		t.Fatalf("engine filter = %v", ids(l))
	}
	if l, _ := cat.List(ctx, backup.Filter{Status: backup.StatusExpired}); len(l) != 1 || l[0].ID != "b1" {
		t.Fatalf("status filter = %v", ids(l))
	}
	after := base.Add(30 * time.Second)
	if l, _ := cat.List(ctx, backup.Filter{CreatedAfter: &after, Limit: 1}); len(l) != 1 || l[0].ID != "b3" {
		t.Fatalf("created_after+limit = %v", ids(l))
	}
	// Keyset pagination walks every record exactly once, including records sharing created_at.
	for _, id := range []string{"t1", "t2", "t3"} {
		r := rec(id, "db3", base.Add(3*time.Minute))
		_, _, err := cat.Create(ctx, r)
		must(t, err)
	}
	var seen []string
	var cur *backup.Cursor
	for page := 0; page < 10; page++ {
		l, err := cat.List(ctx, backup.Filter{Limit: 2, After: cur})
		must(t, err)
		seen = append(seen, ids(l)...)
		next := backup.NextCursor(l, 2)
		if next == "" {
			break
		}
		c, err := backup.ParseCursor(next)
		must(t, err)
		cur = &c
	}
	if strings.Join(seen, ",") != "t3,t2,t1,b3,b2,b1" {
		t.Fatalf("paged listing = %v", seen)
	}
	if _, err := backup.ParseCursor("not-a-cursor!"); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("bad cursor err = %v", err)
	}
	if _, err := cat.List(ctx, backup.Filter{Status: "bogus"}); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("bad status filter err = %v", err)
	}
	if r, ok, err := cat.Get(ctx, "nope"); err != nil || ok {
		t.Fatalf("get missing = %+v %v %v", r, ok, err)
	}
	if _, _, err := cat.Get(ctx, "../etc"); !errors.Is(err, backup.ErrInvalid) {
		t.Fatalf("get invalid id err = %v", err)
	}
}

func ids(rs []backup.Record) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.ID
	}
	return out
}

func runBackupJobs(t *testing.T, s store.Store) {
	ctx := context.Background()
	j := backup.Job{ID: "bjob-1", Name: "nightly", Command: "/opt/strand-backup.d/mysql", EverySeconds: 3600,
		ArtifactTemplate: "/var/backups/{id}.sql", ManifestTemplate: "/var/backups/{id}.json", Bucket: "central-backups", Enabled: true}
	must(t, s.CreateBackupJob(ctx, j))
	if err := s.CreateBackupJob(ctx, j); err == nil {
		t.Fatal("duplicate job id accepted")
	}
	got, ok, err := s.GetBackupJob(ctx, "bjob-1")
	if err != nil || !ok || got.Name != "nightly" {
		t.Fatalf("get = %+v %v %v", got, ok, err)
	}
	// next_run_at CAS on updated_at.
	next := time.Now().UTC().Add(time.Hour).Truncate(time.Millisecond)
	if ok, err := s.SetBackupJobNextRun(ctx, j.ID, got.UpdatedAt, next); err != nil || !ok {
		t.Fatalf("set next run: %v %v", ok, err)
	}
	got.Name = "nightly-2"
	if ok, err := s.UpdateBackupJob(ctx, got); err != nil || !ok {
		t.Fatalf("update: %v %v", ok, err)
	}
	upd, _, _ := s.GetBackupJob(ctx, j.ID)
	if !upd.UpdatedAt.After(got.UpdatedAt) {
		t.Fatal("update must advance updated_at (other schedulers detect the new revision)")
	}
	if ok, _ := s.SetBackupJobNextRun(ctx, j.ID, got.UpdatedAt, next); ok {
		t.Fatal("stale revision must not move next_run_at")
	}
	if _, ok, _ := s.BeginBackupJobRun(ctx, j.ID, got.UpdatedAt); ok {
		t.Fatal("stale revision must not start a run")
	}
	started, ok, err := s.BeginBackupJobRun(ctx, j.ID, upd.UpdatedAt)
	if err != nil || !ok {
		t.Fatalf("begin: %v %v", ok, err)
	}
	running, _, _ := s.GetBackupJob(ctx, j.ID)
	if running.LastRunStatus != backup.RunStatusRunning || running.NextRunAt == nil || running.NextRunAt.Before(started.Add(59*time.Minute)) {
		t.Fatalf("running job = %+v (next run must be one interval after start: no catch-up burst)", running)
	}
	// A crashed run is recovered as interrupted exactly once.
	if ok, err := s.RecoverBackupJobRun(ctx, j.ID, running.UpdatedAt); err != nil || !ok {
		t.Fatalf("recover: %v %v", ok, err)
	}
	if ok, _ := s.FinishBackupJobRun(ctx, j.ID, started, backup.RunStatusSucceeded); ok {
		t.Fatal("finishing an already-recovered run must be a no-op")
	}
	rec, _, _ := s.GetBackupJob(ctx, j.ID)
	if rec.LastRunStatus != backup.RunStatusInterrupted || rec.RunFinishedAt == nil {
		t.Fatalf("recovered = %+v", rec)
	}
	started, _, _ = s.BeginBackupJobRun(ctx, j.ID, rec.UpdatedAt)
	if ok, err := s.FinishBackupJobRun(ctx, j.ID, started, backup.RunStatusSucceeded); err != nil || !ok {
		t.Fatalf("finish: %v %v", ok, err)
	}
	if l, _ := s.ListBackupJobs(ctx); len(l) != 1 {
		t.Fatalf("list = %d", len(l))
	}
	if ok, err := s.DeleteBackupJob(ctx, j.ID); err != nil || !ok {
		t.Fatalf("delete: %v %v", ok, err)
	}
	if _, ok, _ := s.GetBackupJob(ctx, j.ID); ok {
		t.Fatal("deleted job still there")
	}
}
