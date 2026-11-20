package main

import (
	"bytes"
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

func schedJob(id, host string) backup.Job {
	return backup.Job{ID: id, Name: id, EverySeconds: 86400, Enabled: true, Bucket: "central-backups", JobOptions: backup.JobOptions{
		Method: backup.MethodMySQLDump, Source: "s-" + id, Conn: backup.ConnSpec{Host: host, Databases: []string{"app"}}}}
}

func createDue(t *testing.T, st store.Store, j backup.Job, due time.Time) backup.Job {
	t.Helper()
	j.NextRunAt = &due
	if err := st.CreateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	got, _, _ := st.GetBackupJob(context.Background(), j.ID)
	return got
}

func fastSlots(t *testing.T) {
	old1, old2 := slotPoll, slotTTL
	slotPoll, slotTTL = 15*time.Millisecond, 3*time.Second
	t.Cleanup(func() { slotPoll, slotTTL = old1, old2 })
}

// trackedRun is the production run shape: tracked run + schedule advance.
func trackedRun(st store.Store, body func(j backup.Job)) func(context.Context, backup.Job) error {
	return func(ctx context.Context, j backup.Job) error {
		_, err := runTrackedBackupJob(ctx, st, j, func(context.Context) error { body(j); return nil })
		if aerr := advanceSchedule(ctx, st, j.ID, time.Now()); aerr != nil {
			return aerr
		}
		return err
	}
}

func runLoop(t *testing.T, st store.Store, id string, o schedulerOptions) (stop func() string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var out, errb bytes.Buffer
	var mu sync.Mutex
	done := make(chan error, 1)
	go func() {
		err := runScheduleLoop(ctx, &lockedWriter{&errb, &mu}, &lockedWriter{&out, &mu}, st, id, o)
		done <- err
	}()
	return func() string {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("scheduler: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
		mu.Lock()
		defer mu.Unlock()
		return errb.String() + out.String()
	}
}

type lockedWriter struct {
	b  *bytes.Buffer
	mu *sync.Mutex
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConcurrencyFromEnv(t *testing.T) {
	env := func(m map[string]string) func(string) string { return func(k string) string { return m[k] } }
	if l := concurrencyFromEnv(env(nil)); l.maxTotal != 2 || l.maxPerHost != 1 {
		t.Fatalf("defaults = %+v", l)
	}
	if l := concurrencyFromEnv(env(map[string]string{EnvBackupMaxConcurrent: "5", EnvBackupMaxPerHost: "0"})); l.maxTotal != 5 || l.maxPerHost != 0 {
		t.Fatalf("explicit = %+v", l)
	}
	if l := concurrencyFromEnv(env(map[string]string{EnvBackupMaxConcurrent: "abc", EnvBackupMaxPerHost: "-1"})); l.maxTotal != 2 || l.maxPerHost != 1 {
		t.Fatalf("invalid = %+v", l)
	}
}

func TestNextAfterRun(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	j := schedJob("a", "h")
	if _, ok := nextAfterRun(j, now); ok {
		t.Fatal("legacy interval job must keep the store's bookkeeping")
	}
	j.Schedule = &backup.Schedule{Kind: backup.ScheduleDaily, At: "01:00", Timezone: "Asia/Ho_Chi_Minh"}
	n, ok := nextAfterRun(j, now)
	if !ok || !n.Equal(time.Date(2026, 3, 10, 18, 0, 0, 0, time.UTC)) {
		t.Fatalf("daily next = %s %v", n, ok)
	}
	// failed run with retries pending keeps the retry back-off
	j.RetryMax, j.RetryBackoffSeconds, j.ConsecutiveFailures, j.LastRunStatus = 3, 60, 1, backup.RunStatusFailed
	if _, ok := nextAfterRun(j, now); ok {
		t.Fatal("pending retry overridden")
	}
	j.ConsecutiveFailures = 4 // retries exhausted: back to the schedule
	if _, ok := nextAfterRun(j, now); !ok {
		t.Fatal("exhausted retries did not return to the schedule")
	}
}

func TestSourceHost(t *testing.T) {
	a, b := schedJob("a", "DB1.example"), schedJob("b", "db1.example")
	if sourceHost(a) != sourceHost(b) || sourceHost(a) != "db1.example:3306" {
		t.Fatalf("hosts = %q %q", sourceHost(a), sourceHost(b))
	}
	b.Conn.Port = 3307
	if sourceHost(a) == sourceHost(b) {
		t.Fatal("different ports share a host identity")
	}
	pg := schedJob("p", "db1.example")
	pg.Method = backup.MethodPGDump
	if sourceHost(pg) != "db1.example:5432" {
		t.Fatalf("pg host = %q", sourceHost(pg))
	}
}

// A run that was not started within window_minutes of its due time is recorded as missed (never run late),
// and the next run is the schedule's next occurrence.
func TestSchedulerRecordsMissedOutsideWindow(t *testing.T) {
	fastSlots(t)
	st := store.NewMem()
	j := schedJob("late", "h1")
	j.Schedule = &backup.Schedule{Kind: backup.ScheduleDaily, At: "01:00", Timezone: "UTC"}
	j.WindowMinutes = 5
	created := createDue(t, st, j, time.Now().UTC().Add(-3*time.Hour))
	var runs atomic.Int32
	stop := runLoop(t, st, j.ID, schedulerOptions{limits: concurrencyLimits{2, 1}, run: trackedRun(st, func(backup.Job) { runs.Add(1) })})
	waitFor(t, "missed status", func() bool {
		g, _, _ := st.GetBackupJob(context.Background(), j.ID)
		return g.LastRunStatus == backup.RunStatusMissed
	})
	log := stop()
	g, _, _ := st.GetBackupJob(context.Background(), j.ID)
	if runs.Load() != 0 || g.RunFinishedAt == nil || g.NextRunAt == nil || !g.NextRunAt.After(time.Now()) || g.ConsecutiveFailures != 0 {
		t.Fatalf("runs=%d job=%+v", runs.Load(), g)
	}
	want, _ := j.Schedule.Next(time.Now(), time.Time{})
	if !g.NextRunAt.Equal(want) || !strings.Contains(log, "missed") || g.UpdatedAt != created.UpdatedAt {
		t.Fatalf("next=%s want %s log=%q", g.NextRunAt, want, log)
	}
}

func TestSchedulerRunsInsideWindowAndAdvances(t *testing.T) {
	fastSlots(t)
	st := store.NewMem()
	j := schedJob("ontime", "h1")
	j.Schedule = &backup.Schedule{Kind: backup.ScheduleWeekly, At: "02:00", Days: []int{0}, Timezone: "UTC"}
	j.WindowMinutes = 5
	createDue(t, st, j, time.Now().UTC().Add(-time.Minute))
	var runs atomic.Int32
	stop := runLoop(t, st, j.ID, schedulerOptions{limits: concurrencyLimits{2, 1}, run: trackedRun(st, func(backup.Job) { runs.Add(1) })})
	waitFor(t, "successful run", func() bool {
		g, _, _ := st.GetBackupJob(context.Background(), j.ID)
		return g.LastRunStatus == backup.RunStatusSucceeded && g.NextRunAt != nil && g.NextRunAt.After(time.Now().Add(time.Hour))
	})
	stop()
	g, _, _ := st.GetBackupJob(context.Background(), j.ID)
	want, _ := j.Schedule.Next(time.Now(), time.Time{})
	if runs.Load() != 1 || !g.NextRunAt.Equal(want) || g.NextRunAt.Weekday() != time.Sunday {
		t.Fatalf("runs=%d next=%s want %s", runs.Load(), g.NextRunAt, want)
	}
}

// A run that cannot get a slot until its window has passed is missed, not started late.
func TestSchedulerMissedWhenNoSlotInWindow(t *testing.T) {
	fastSlots(t)
	st := store.NewMem()
	ctx := context.Background()
	if ok, _ := st.AcquireBackupSlot(ctx, store.SlotRequest{JobID: "other", Host: "h1:3306", MaxTotal: 2, MaxPerHost: 1, TTL: time.Hour}, time.Now()); !ok {
		t.Fatal("setup")
	}
	j := schedJob("blocked", "h1") // same host as the running "other" job
	j.Schedule = &backup.Schedule{Kind: backup.ScheduleDaily, At: "01:00"}
	j.WindowMinutes = 5
	createDue(t, st, j, time.Now().UTC().Add(-5*time.Minute+700*time.Millisecond))
	var runs atomic.Int32
	stop := runLoop(t, st, j.ID, schedulerOptions{limits: concurrencyLimits{2, 1}, run: trackedRun(st, func(backup.Job) { runs.Add(1) })})
	waitFor(t, "missed", func() bool {
		g, _, _ := st.GetBackupJob(ctx, j.ID)
		return g.LastRunStatus == backup.RunStatusMissed
	})
	stop()
	if runs.Load() != 0 {
		t.Fatal("ran despite the per-host limit")
	}
}

// Global and per-host limits hold across scheduler loops sharing one store (the cross-process case).
func TestSchedulerEnforcesGlobalAndPerHostLimits(t *testing.T) {
	fastSlots(t)
	type scenario struct {
		name   string
		limits concurrencyLimits
		hosts  []string
		maxAt  int32
	}
	for _, sc := range []scenario{
		{"global max 2 of 4 hosts", concurrencyLimits{2, 1}, []string{"a", "b", "c", "d"}, 2},
		{"per host 1 on one host", concurrencyLimits{5, 1}, []string{"a", "a", "a"}, 1},
		{"global 1", concurrencyLimits{1, 0}, []string{"a", "b", "c"}, 1},
		{"unlimited", concurrencyLimits{0, 0}, []string{"a", "a", "a"}, 3},
	} {
		t.Run(sc.name, func(t *testing.T) {
			st := store.NewMem()
			var cur, peak, done atomic.Int32
			body := func(backup.Job) {
				n := cur.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(120 * time.Millisecond)
				cur.Add(-1)
				done.Add(1)
			}
			var stops []func() string
			for i, h := range sc.hosts {
				j := schedJob("j"+string(rune('a'+i)), h)
				j.Schedule = &backup.Schedule{Kind: backup.ScheduleDaily, At: "01:00"}
				createDue(t, st, j, time.Now().UTC().Add(-time.Second))
				stops = append(stops, runLoop(t, st, j.ID, schedulerOptions{limits: sc.limits, run: trackedRun(st, body)}))
			}
			waitFor(t, "all runs", func() bool { return int(done.Load()) == len(sc.hosts) })
			for _, s := range stops {
				s()
			}
			if peak.Load() != sc.maxAt {
				t.Fatalf("peak concurrency = %d, want %d", peak.Load(), sc.maxAt)
			}
			// every slot is released after the runs
			ok, _ := st.AcquireBackupSlot(context.Background(), store.SlotRequest{JobID: "probe", Host: "a:3306", MaxTotal: 1, MaxPerHost: 1, TTL: time.Hour}, time.Now())
			if !ok {
				t.Fatal("slots leaked after the runs")
			}
		})
	}
}

func TestSchedulerJitterDelaysStartWithinWindow(t *testing.T) {
	fastSlots(t)
	st := store.NewMem()
	j := schedJob("jit", "h1")
	j.Schedule = &backup.Schedule{Kind: backup.ScheduleDaily, At: "01:00"}
	j.JitterSeconds = 1
	j.WindowMinutes = 5
	due := time.Now().UTC().Add(-100 * time.Millisecond)
	createDue(t, st, j, due)
	var started atomic.Int64
	stop := runLoop(t, st, j.ID, schedulerOptions{limits: concurrencyLimits{2, 1}, run: trackedRun(st, func(backup.Job) { started.Store(time.Now().UnixNano()) })})
	waitFor(t, "jittered run", func() bool { return started.Load() != 0 })
	stop()
	delay := time.Duration(started.Load() - due.UnixNano())
	if delay < 0 || delay > 1500*time.Millisecond {
		t.Fatalf("start delay = %s, want within [0, 1s+]", delay)
	}
}

// Legacy interval jobs (no schedule) keep the store's every_seconds bookkeeping: next = start + every.
func TestSchedulerLegacyIntervalUnchanged(t *testing.T) {
	fastSlots(t)
	st := store.NewMem()
	j := schedJob("legacy", "h1")
	j.EverySeconds = 3600
	createDue(t, st, j, time.Now().UTC().Add(-time.Second))
	var runs atomic.Int32
	stop := runLoop(t, st, j.ID, schedulerOptions{limits: concurrencyLimits{2, 1}, run: trackedRun(st, func(backup.Job) { runs.Add(1) })})
	waitFor(t, "run", func() bool {
		g, _, _ := st.GetBackupJob(context.Background(), j.ID)
		return g.LastRunStatus == backup.RunStatusSucceeded
	})
	stop()
	g, _, _ := st.GetBackupJob(context.Background(), j.ID)
	if runs.Load() != 1 || g.NextRunAt.Before(time.Now().Add(59*time.Minute)) || g.NextRunAt.After(time.Now().Add(61*time.Minute)) {
		t.Fatalf("runs=%d next=%s", runs.Load(), g.NextRunAt)
	}
}

func TestResolveJobConnection(t *testing.T) {
	st := store.NewMem()
	ctx := context.Background()
	box, _ := store.NewBox("0123456789abcdef0123456789abcdef")
	sealed, _ := box.Seal([]byte("pw"), "conn-1")
	if _, err := st.CreateBackupConnection(ctx, backup.Connection{ID: "conn-1", Name: "prod", Engine: backup.ConnMySQL, Host: "db9", Port: 3307,
		User: "bk", SecretMode: backup.SecretStored, PasswordSealed: sealed}); err != nil {
		t.Fatal(err)
	}
	j := schedJob("x", "stale-host")
	j.ConnectionID = "conn-1"
	envWith := func(v string) func(string) string {
		return func(k string) string {
			if k == backup.EnvConnPassword {
				return v
			}
			return ""
		}
	}
	got, err := resolveJobConnection(ctx, st, j, envWith("pw"))
	if err != nil || got.Conn.Host != "db9" || got.Conn.Port != 3307 || got.Conn.User != "bk" || got.Conn.PasswordEnv != backup.EnvConnPassword || got.Conn.Databases[0] != "app" {
		t.Fatalf("resolved = %+v %v", got.Conn, err)
	}
	if _, err := resolveJobConnection(ctx, st, j, envWith("")); err == nil || !strings.Contains(err.Error(), "Web console") {
		t.Fatalf("missing handed-over secret: %v", err)
	}
	j.ConnectionID = "gone"
	if _, err := resolveJobConnection(ctx, st, j, envWith("pw")); err == nil || !strings.Contains(err.Error(), "no longer exists") {
		t.Fatalf("deleted connection: %v", err)
	}
	j.ConnectionID = ""
	if got, err := resolveJobConnection(ctx, st, j, envWith("")); err != nil || got.Conn.Host != "stale-host" {
		t.Fatalf("legacy inline job changed: %+v %v", got.Conn, err)
	}
}
