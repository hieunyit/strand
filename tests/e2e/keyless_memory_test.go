package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestPGKeylessSlowTargetMemory holds a real PostgreSQL table lock while a
// mixed-size backlog builds, then checks peak memory, recovery and replay.
func TestPGKeylessSlowTargetMemory(t *testing.T) {
	if os.Getenv("STRAND_KEYLESS_RAM_BENCH") != "1" {
		t.Skip("set STRAND_KEYLESS_RAM_BENCH=1 to run the slow-target memory benchmark")
	}
	t.Setenv("STRAND_TXN_TRACE", "")
	const table = "app.work"
	dir := testDir(t)
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	// Add the on-demand heap observer only to this build of the normal CLI.
	probeFiles := map[string]string{
		filepath.Join(root, "cmd/strand/bench_heap_probe.go"): filepath.Join(root, "tests/e2e/testdata/heap_probe.go"),
	}
	if baseline := os.Getenv("STRAND_KEYLESS_RAM_REFERENCE_SCHEDULER"); baseline != "" {
		probeFiles[filepath.Join(root, "internal/sched/scheduler.go")] = baseline
	}
	overlay, err := json.Marshal(map[string]any{"Replace": probeFiles})
	if err != nil {
		t.Fatal(err)
	}
	overlayPath := filepath.Join(dir, "overlay.json")
	if err := os.WriteFile(overlayPath, overlay, 0600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "strand")
	if out, err := exec.Command("go", "build", "-overlay", overlayPath, "-o", bin, "../../cmd/strand").CombinedOutput(); err != nil {
		t.Fatalf("build memory probe: %v\n%s", err, out)
	}
	heap := filepath.Join(dir, "blocked.pprof")
	t.Setenv("STRAND_RAM_PROFILE", heap)
	src := startPG(t, "ram-src", "-c", "fsync=on", "-c", "synchronous_commit=on")
	dst := startPG(t, "ram-dst", "-c", "fsync=on", "-c", "synchronous_commit=on")
	for _, db := range []*pgNode{src, dst} {
		db.exec(t, "CREATE SCHEMA app", "CREATE TABLE app.work(id bigint, payload text)",
			"CREATE TABLE app.marker(id bigint PRIMARY KEY, tag text)")
	}
	src.exec(t, "ALTER TABLE app.work REPLICA IDENTITY FULL", "CREATE PUBLICATION ram FOR TABLE app.work, app.marker")
	var lsn, version string
	if err := src.DB.QueryRow("SELECT lsn::text, version() FROM pg_create_logical_replication_slot('ram','pgoutput',false,true)").Scan(&lsn, &version); err != nil {
		t.Fatal(err)
	}
	cfg := strandCfg{Name: "rambench", SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN,
		TargetDSN: dst.DSN, SourceExtra: ", slot: ram, publication: ram", Include: []string{table, "app.marker"},
		TablesExtra: klKeylessCfg, Bootstrap: "start_at", PostSeedWindow: "1s", CommitOrder: "relaxed",
		Dir: dir, MetricsAddr: freeAddr(t)}
	path := cfg.write(t)
	proc := startStrandBinary(t, bin, path, "--start-at", lsn)
	defer func() { proc.kill9() }()
	waitSynced(t, &cfg, proc, time.Minute)

	block, err := dst.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer block.Rollback()
	var blockerPID int
	if err := block.QueryRow("SELECT pg_backend_pid()").Scan(&blockerPID); err != nil {
		t.Fatal(err)
	}
	if _, err := block.Exec("LOCK TABLE app.work IN SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	// The first INSERT waits on the lock. Two transactions exceed the
	// assembler's 10,000-row threshold. The tail charges non-spilled bytes.
	insert := func(n int) {
		src.exec(t, fmt.Sprintf(`INSERT INTO app.work
			SELECT g%%32, CASE WHEN g%%8=0 THEN NULL ELSE repeat(md5((g%%32)::text),8) END
			FROM generate_series(1,%d) g`, n))
	}
	stateDir := filepath.Join(dir, "state")
	waitMetrics := time.Now().Add(5 * time.Second)
	for {
		_, err := readRAMSample(stateDir, cfg.MetricsAddr, cfg.Name)
		if err == nil {
			break
		}
		if time.Now().After(waitMetrics) {
			t.Fatalf("metrics unavailable before workload: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	samples := make(chan struct{})
	done := make(chan ramSample, 1)
	go func() {
		var peak ramSample
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-samples:
				done <- peak
				return
			case <-tick.C:
				now, err := readRAMSample(stateDir, cfg.MetricsAddr, cfg.Name)
				if err != nil {
					peak.err = err
					continue
				}
				peak.spool = max(peak.spool, now.spool)
				peak.spills = max(peak.spills, now.spills)
				peak.pending = max(peak.pending, now.pending)
				peak.writeset = max(peak.writeset, now.writeset)
			}
		}
	}()
	defer close(samples)
	insert(1)
	insert(12000)
	insert(12000)
	for range 16 {
		insert(256)
	}
	// The independent table can commit while app.work remains blocked. Its
	// appearance proves the reader and router have admitted the whole backlog.
	src.exec(t, "INSERT INTO app.marker VALUES (1, 'queue admitted')")
	deadline := time.Now().Add(time.Minute)
	var marker bool
	for {
		now, err := readRAMSample(stateDir, cfg.MetricsAddr, cfg.Name)
		if err := dst.DB.QueryRow("SELECT EXISTS(SELECT 1 FROM app.marker WHERE id=1)").Scan(&marker); err != nil {
			t.Fatal(err)
		}
		if err == nil && marker && now.spills >= 2 && now.pending > 1<<20 {
			break
		}
		if proc.exited() || time.Now().After(deadline) {
			t.Fatalf("blocked queue did not reach sampling point: %+v %v\n%s", now, err, proc.tail())
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Pending and writeset gauges are sampled once per second.
	time.Sleep(1100 * time.Millisecond)
	var blocked bool
	if err := dst.DB.QueryRow("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))", blockerPID).Scan(&blocked); err != nil || !blocked {
		t.Fatalf("writer waiting on fixture lock = %t: %v", blocked, err)
	}
	if err := proc.cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	waitStrandLog(t, proc, "profile_error=<nil>", time.Second*15)
	for _, l := range strings.Split(proc.output.String(), "\n") {
		if strings.HasPrefix(l, "heap_sample ") {
			t.Log(l)
		}
	}
	top, err := exec.Command("go", "tool", "pprof", "-top", "-sample_index=inuse_space", "-nodecount=15", bin, heap).CombinedOutput()
	if err != nil {
		t.Fatalf("read retained heap: %v\n%s", err, top)
	}
	t.Logf("retained heap during target lock:\n%s", top)
	start := time.Now()
	if err := block.Rollback(); err != nil {
		t.Fatal(err)
	}
	waitPGCaughtUp(t, src, dst, &cfg, proc, 60*time.Second)
	elapsed := time.Since(start)
	checkKeylessBaselineRows(t, src.DB, dst.DB, table)
	hwm, _, err := procMemKB(proc.cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	// Keep this fixture's local budgets independent of deployment SLOs.
	if hwm > 256*1024 || elapsed > 60*time.Second {
		t.Errorf("lock release recovery took %s, peak RSS %d KiB; fixture limits 60 s and 256 MiB", elapsed, hwm)
	}
	samples <- struct{}{}
	peak := <-done
	if peak.err != nil || peak.spool < 1<<20 || peak.spool > 256<<20 || peak.spills < 2 || peak.pending <= 0 {
		t.Errorf("insufficient sampling or spool outside fixture budget: %+v", peak)
	}
	proc.kill9()
	insert(12000)
	restart := time.Now()
	proc = startStrandBinary(t, bin, path)
	waitPGCaughtUp(t, src, dst, &cfg, proc, time.Minute)
	recovery := time.Since(restart)
	checkKeylessBaselineRows(t, src.DB, dst.DB, table)
	after, err := readRAMSample(stateDir, cfg.MetricsAddr, cfg.Name)
	if err != nil || after.spills != 0 {
		t.Errorf("spills after recovery = %d: %v", after.spills, err)
	}
	t.Logf("db=%s rows=%d rss_peak_kib=%d writeset_peak=%.0f pending_peak_bytes=%.0f spool_peak_bytes=%d spill_files_peak=%d catchup=%s restart=%s recovered_rows=%d",
		version, 1+2*12000+16*256, hwm, peak.writeset, peak.pending, peak.spool, peak.spills, elapsed, recovery, 12000)
}

type ramSample struct {
	spool             int64
	spills            int
	pending, writeset float64
	err               error
}

func readRAMSample(dir, addr, pipeline string) (ramSample, error) {
	var sample ramSample
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // concurrent apply removes temporary spill files
		}
		if err != nil || !d.Type().IsRegular() {
			return err
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		sample.spool += info.Size()
		if strings.HasSuffix(d.Name(), ".spill") {
			sample.spills++
		}
		return nil
	})
	if err != nil {
		return sample, err
	}
	text, err := scrapeMetrics(addr)
	if err != nil {
		return sample, err
	}
	labels := map[string]string{"pipeline": pipeline}
	var ok bool
	sample.pending, ok = metricSum(text, "strand_scheduler_pending_bytes", labels)
	if !ok {
		return sample, fmt.Errorf("pending-bytes metric missing")
	}
	sample.writeset, ok = metricSum(text, "strand_writeset_entries", labels)
	if !ok {
		return sample, fmt.Errorf("writeset metric missing")
	}
	return sample, nil
}
