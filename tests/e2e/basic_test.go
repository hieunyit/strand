package e2e

import (
	"testing"
	"time"
)

// Pipeline đầy đủ: bootstrap chế độ A (snapshot logic) trong khi source đang chịu tải,
// rồi stream; trạng thái cuối source == target, ver không giảm, bảng bị loại không sang.
func TestE2E_BootstrapAndStreamUnderLoad(t *testing.T) {
	seed := seedFromEnv(t)
	src := startMySQL(t, mysqlImage("source"), "src", 1, nil)
	dst := startMySQL(t, mysqlImage("target"), "dst", 2, nil)
	createSchema(t, src, dst)

	wl := newWorkload(src.DB, seed)
	wl.Preload(t, 3000)
	wl.Start(8, 3000)

	dst.exec(t, "CREATE DATABASE IF NOT EXISTS strand")
	cfg := strandCfg{Name: "e2e-basic", SourceDSN: src.RootDSN, TargetDSN: dst.RootDSN + "strand",
		Include: []string{"app.*"}, Exclude: []string{"app.ignored"}, Dir: t.TempDir()}
	p := startStrand(t, cfg.write(t))
	vs := startVerSampler(dst, appTables...)

	time.Sleep(time.Duration(envInt("STRAND_E2E_LOAD_SECONDS", 20)) * time.Second)
	wl.Stop()
	t.Logf("workload: %d ops, %d client-side errors (deadlocks/dup keys are expected)", wl.ops.Load(), wl.errs.Load())

	waitCaughtUp(t, src, dst, cfg.Name, p, 3*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d on SIGTERM\n%s", code, p.tail())
	}
	vs.check(t)
	compareFinal(t, src, dst, appTables...)

	checkNoOrphans(t, dst)
	var n int
	_ = dst.DB.QueryRow("SELECT COUNT(*) FROM app.ignored").Scan(&n)
	if n != 0 {
		t.Fatalf("excluded table app.ignored has %d rows on target", n)
	}
}
