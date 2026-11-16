// Package storetest is the behavioural contract every store.Store implementation must satisfy.
package storetest

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
