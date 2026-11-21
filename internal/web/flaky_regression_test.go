package web

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"strand/internal/backup"
	"strand/internal/web/store"
)

// TestCleanWebShutdownLeavesBackupProcessesAlive proves that a clean web server shutdown
// does NOT kill detached backup processes (e.g. backup schedulers), per CENTRAL-BACKUPS.md:137.
// On 0dbd847, Serve() -> Close() -> stopBackupSchedulers() killed the process, so this failed.
func TestCleanWebShutdownLeavesBackupProcessesAlive(t *testing.T) {
	_, script := backupCommandDir(t)
	st := store.NewMem()
	j := backup.Job{
		ID:               "bjob-shutdown-detach",
		Name:             "nightly",
		Command:          script,
		EverySeconds:     3600,
		ArtifactTemplate: "/var/backups/{id}.sql",
		ManifestTemplate: "/var/backups/{id}.json",
		Bucket:           "central-backups",
		Enabled:          true,
	}
	if err := st.CreateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	e := dbEnv(t, st, func(e *testEnv, _ *Config) { e.hold(true) }, nil)
	t.Cleanup(func() { e.hold(false) })

	waitFor(t, "backup scheduler running", func() bool {
		return len(runningSchedulers(e, j.ID)) == 1
	})
	pid := runningSchedulers(e, j.ID)[0].PID
	if pid <= 0 {
		t.Fatal("expected positive PID")
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancelServe := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		errc <- e.srv.Serve(serveCtx, ln)
	}()

	time.Sleep(50 * time.Millisecond)
	cancelServe()
	if err := <-errc; err != nil {
		t.Fatalf("Serve returned error: %v", err)
	}

	// The backup process must still be alive!
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("expected backup process pid %d to survive clean shutdown, got error: %v", pid, err)
	}

	// Clean up process in test
	e.hold(false)
	_ = syscall.Kill(pid, syscall.SIGTERM)
}

type failStartRunner struct {
	JobRunner
	startCalls atomic.Int32
}

func (f *failStartRunner) Start(ctx context.Context, spec JobSpec) (string, error) {
	f.startCalls.Add(1)
	return "", errors.New("simulated start failure")
}

// TestReconcileStopsOnClose proves that background reconcile loops terminate when Server.Close()
// is called. On 3f28167, Server had no Close() method and reconcileBackupSchedule ran in an
// infinite loop sleeping with time.Sleep.
func TestReconcileStopsOnClose(t *testing.T) {
	origDelay := reconcileDelay
	defer func() { reconcileDelay = origDelay }()
	reconcileDelay = struct{ first, max time.Duration }{10 * time.Millisecond, 20 * time.Millisecond}

	_, script := backupCommandDir(t)
	st := store.NewMem()
	e := dbEnv(t, st, nil, nil)

	runner := &failStartRunner{JobRunner: e.srv.jobs}
	e.srv.SetJobRunner(runner)

	j := backup.Job{
		ID:               "bjob-reconcile-close",
		Name:             "nightly",
		Command:          script,
		EverySeconds:     3600,
		ArtifactTemplate: "/var/backups/{id}.sql",
		ManifestTemplate: "/var/backups/{id}.json",
		Bucket:           "central-backups",
		Enabled:          true,
	}
	if err := st.CreateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}

	pipeline := backupJobPipeline(j.ID)
	done := make(chan struct{})
	go func() {
		e.srv.reconcileBackupSchedule(pipeline)
		close(done)
	}()

	waitFor(t, "reconcile has attempted to start", func() bool {
		return runner.startCalls.Load() >= 2
	})

	// Close the server
	_ = e.srv.Close()

	// Assert the reconcile loop exits promptly
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("reconcileBackupSchedule did not exit after Server.Close()")
	}

	countAtClose := runner.startCalls.Load()
	time.Sleep(50 * time.Millisecond)
	if countAfter := runner.startCalls.Load(); countAfter != countAtClose {
		t.Fatalf("start calls increased after Close: was %d, now %d", countAtClose, countAfter)
	}
}

// TestCancelAlreadyExitedProcessReturnsNilAndHTTP200 tests that cancelling an already-exited
// process returns nil (swallowing ESRCH) and the pause API returns HTTP 200 rather than 409.
func TestCancelAlreadyExitedProcessReturnsNilAndHTTP200(t *testing.T) {
	// Part 1: Directly on FileJobRunner
	dir := t.TempDir()
	bin := newFakeStrand(t, dir)
	r, err := NewFileJobRunner(filepath.Join(dir, "jobs"), bin, nil)
	if err != nil {
		t.Fatal(err)
	}

	holdFile := filepath.Join(dir, "hold")
	if err := os.WriteFile(holdFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(holdFile)

	id, err := r.Start(context.Background(), JobSpec{Pipeline: "p1", Kind: "backup-schedule"})
	if err != nil {
		t.Fatal(err)
	}
	st, ok := r.Get(id)
	if !ok || st.PID <= 0 {
		t.Fatalf("job %s not running or no PID: %+v", id, st)
	}

	// Kill the process directly with SIGKILL
	if err := syscall.Kill(st.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill failed: %v", err)
	}
	waitFor(t, "process gone from OS", func() bool {
		return syscall.Kill(st.PID, 0) != nil
	})

	// Simulate state where Running is true in runner while process is gone in OS:
	// Cancel must return nil because ESRCH is swallowed.
	r.mu.Lock()
	r.jobs[id].Running = true
	r.mu.Unlock()

	if err := r.Cancel(id); err != nil {
		t.Fatalf("Cancel on already-exited process returned error: %v, want nil", err)
	}

	// Subsequent Cancel when job is marked stopped returns errJobNotRunning
	r.mu.Lock()
	r.jobs[id].Running = false
	r.mu.Unlock()
	if err := r.Cancel(id); !errors.Is(err, errJobNotRunning) {
		t.Fatalf("second Cancel returned %v, want %v", err, errJobNotRunning)
	}

	// Part 2: stopBackupJob and pause API returns 200
	_, script := backupCommandDir(t)
	storeMem := store.NewMem()
	jobID := "bjob-pause-esrch"
	bj := backup.Job{
		ID:               jobID,
		Name:             "nightly",
		Command:          script,
		EverySeconds:     3600,
		ArtifactTemplate: "/var/backups/{id}.sql",
		ManifestTemplate: "/var/backups/{id}.json",
		Bucket:           "central-backups",
		Enabled:          true,
	}
	if err := storeMem.CreateBackupJob(context.Background(), bj); err != nil {
		t.Fatal(err)
	}

	e := dbEnv(t, storeMem, func(e *testEnv, _ *Config) { e.hold(true) }, nil)
	t.Cleanup(func() { e.hold(false) })

	waitFor(t, "scheduler running", func() bool {
		return len(runningSchedulers(e, jobID)) == 1
	})
	proc := runningSchedulers(e, jobID)[0]
	if proc.PID <= 0 {
		t.Fatalf("no pid for scheduler: %+v", proc)
	}

	// Kill the child process directly
	_ = syscall.Kill(proc.PID, syscall.SIGKILL)
	waitFor(t, "scheduler proc gone", func() bool {
		return syscall.Kill(proc.PID, 0) != nil
	})

	// Pause via API: POST /api/v1/backup-jobs/{id}/pause
	c, csrf := e.loginOK()
	resp, _ := e.do(c, "POST", "/api/v1/backup-jobs/"+jobID+"/pause", "", map[string]string{"X-CSRF-Token": csrf})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pause API returned %d (expected 200)", resp.StatusCode)
	}

	updated, found, err := storeMem.GetBackupJob(context.Background(), jobID)
	if err != nil || !found {
		t.Fatalf("GetBackupJob failed: found=%v err=%v", found, err)
	}
	if updated.Enabled {
		t.Fatal("expected job to be disabled after pause")
	}
}

type blockingStartRunner struct {
	JobRunner
	entered       atomic.Int32
	maxConcurrent atomic.Int32
	block         chan struct{}
}

func (b *blockingStartRunner) Start(ctx context.Context, spec JobSpec) (string, error) {
	curr := b.entered.Add(1)
	defer b.entered.Add(-1)
	for {
		m := b.maxConcurrent.Load()
		if curr <= m || b.maxConcurrent.CompareAndSwap(m, curr) {
			break
		}
	}
	<-b.block
	return "job-dup-1", nil
}

// TestDuplicateReconcileOnlyRunsOneLoop proves that concurrent reconcileBackupSchedule calls
// for the same pipeline run only one loop at a time.
func TestDuplicateReconcileOnlyRunsOneLoop(t *testing.T) {
	_, script := backupCommandDir(t)
	st := store.NewMem()
	j := backup.Job{
		ID:               "bjob-dup-reconcile",
		Name:             "nightly",
		Command:          script,
		EverySeconds:     3600,
		ArtifactTemplate: "/var/backups/{id}.sql",
		ManifestTemplate: "/var/backups/{id}.json",
		Bucket:           "central-backups",
		Enabled:          true,
	}
	if err := st.CreateBackupJob(context.Background(), j); err != nil {
		t.Fatal(err)
	}

	e := dbEnv(t, st, nil, nil)
	runner := &blockingStartRunner{
		JobRunner: e.srv.jobs,
		block:     make(chan struct{}),
	}
	e.srv.SetJobRunner(runner)

	pipeline := backupJobPipeline(j.ID)

	go e.srv.reconcileBackupSchedule(pipeline)

	waitFor(t, "reconcile loop 1 entered Start", func() bool {
		return runner.entered.Load() == 1
	})

	done2 := make(chan struct{})
	go func() {
		e.srv.reconcileBackupSchedule(pipeline)
		close(done2)
	}()

	select {
	case <-done2:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second reconcileBackupSchedule blocked instead of returning immediately")
	}

	if max := runner.maxConcurrent.Load(); max != 1 {
		t.Fatalf("expected at most 1 concurrent reconcile loop, got %d", max)
	}

	close(runner.block)
}

// TestTemplateCacheAndConcurrentRender verifies that template parsing results are cached
// and that concurrent page rendering across multiple servers under -race does not race.
func TestTemplateCacheAndConcurrentRender(t *testing.T) {
	// 1. parsePagesForLang returns the same map pointer
	m1, err1 := parsePagesForLang("en")
	if err1 != nil {
		t.Fatalf("parsePagesForLang(en) #1 failed: %v", err1)
	}
	m2, err2 := parsePagesForLang("en")
	if err2 != nil {
		t.Fatalf("parsePagesForLang(en) #2 failed: %v", err2)
	}
	if fmt.Sprintf("%p", m1) != fmt.Sprintf("%p", m2) {
		t.Fatalf("expected identical cached template map pointer, got %p vs %p", m1, m2)
	}

	// 2. Two servers render pages concurrently under -race
	st1 := store.NewMem()
	st2 := store.NewMem()
	e1 := dbEnv(t, st1, nil, nil)
	e2 := dbEnv(t, st2, nil, nil)

	c1, _ := e1.loginOK()
	c2, _ := e2.loginOK()

	urls1 := []string{e1.ts.URL + "/backups", e1.ts.URL + "/settings", e1.ts.URL + "/pipelines"}
	urls2 := []string{e2.ts.URL + "/backups", e2.ts.URL + "/settings", e2.ts.URL + "/pipelines"}

	var wg sync.WaitGroup
	errCh := make(chan error, 50)

	for worker := 0; worker < 10; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for iter := 0; iter < 10; iter++ {
				client := c1
				urls := urls1
				if (w+iter)%2 == 1 {
					client = c2
					urls = urls2
				}
				for _, u := range urls {
					resp, err := client.Get(u)
					if err != nil {
						errCh <- fmt.Errorf("GET %s error: %w", u, err)
						return
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						errCh <- fmt.Errorf("GET %s status: %d", u, resp.StatusCode)
						return
					}
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}
