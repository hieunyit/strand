package e2e

import (
	"math/rand"
	"testing"
	"time"
)

// Gate 1 (01 §6, 04 §4): kill -9 ngẫu nhiên N lần (mặc định 1.000) trong khi source chịu
// tải liên tục — kể cả trong lúc bootstrap snapshot — rồi 0 lệch.
// STRAND_E2E_KILL9_ITERATIONS điều chỉnh số lần (giá trị Gate 1 là 1000).
func TestG1_E2E_Kill9RandomCrashes(t *testing.T) {
	seed := seedFromEnv(t)
	iterations := envInt("STRAND_E2E_KILL9_ITERATIONS", 1000)
	rng := rand.New(rand.NewSource(seed))

	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	createSchema(t, src, dst)
	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 20000)
	wl.Start(6, 20000)

	cfg := strandCfg{Name: "e2e-kill9", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: testDir(t)}
	cfgPath := cfg.write(t)
	vs := startVerSampler(dst, appTables...)

	start := time.Now()
	killedDuringRun := 0
	for i := 1; i <= iterations; i++ {
		p := startStrand(t, cfgPath)
		// Thời điểm kill ngẫu nhiên: 50 ms – 2 s sau khi khởi động.
		wait := time.Duration(50+rng.Intn(1950)) * time.Millisecond
		select {
		case <-p.done:
			t.Fatalf("iteration %d: strand exited by itself with code %d before kill\n%s", i, p.exitCode(), p.tail())
		case <-time.After(wait):
		}
		p.kill9()
		killedDuringRun++
		if i%100 == 0 {
			t.Logf("kill -9 iteration %d/%d (elapsed %s, workload ops %d)", i, iterations, time.Since(start).Round(time.Second), wl.ops.Load())
		}
	}
	wl.Stop()
	t.Logf("workload: %d ops, %d client-side errors", wl.ops.Load(), wl.errs.Load())

	p := startStrand(t, cfgPath)
	waitCaughtUp(t, src, dst, &cfg, p, 10*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("final run exit code %d\n%s", code, p.tail())
	}
	vs.check(t)
	compareFinal(t, src, dst, appTables...)
	checkNoOrphans(t, dst)
	t.Logf("PASS: %d kill -9, 0 deviation, total %s", killedDuringRun, time.Since(start).Round(time.Second))
}
