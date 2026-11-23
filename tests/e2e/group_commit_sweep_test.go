package e2e

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"strand/internal/config"
	"strand/internal/txntrace"
)

// Run on a dedicated host. Rotate order to avoid favoring the last configuration.
// The production writer, scheduler, target checkpoint and CLI are used in every run.
func TestPGKeylessGroupCommitSweep(t *testing.T) {
	if os.Getenv("STRAND_BATCH_SWEEP") != "1" {
		t.Skip("set STRAND_BATCH_SWEEP=1")
	}
	flags := []string{"-c", "fsync=on", "-c", "synchronous_commit=on", "-c", "autovacuum=off"}
	src := startPG(t, "batch-source", flags...)
	dst := startPG(t, "batch-target", append(flags, "-c", "track_commit_timestamp=on")...)
	cases := []struct {
		name  string
		batch config.BatchConfig
	}{
		{"default", config.BatchConfig{MaxRows: 2000, MaxBytes: "4MiB", MaxWait: "50ms"}},
		{"short_wait", config.BatchConfig{MaxRows: 2000, MaxBytes: "4MiB", MaxWait: "2ms"}},
		{"row_cap", config.BatchConfig{MaxRows: 16, MaxBytes: "4MiB", MaxWait: "2ms"}},
		{"byte_cap", config.BatchConfig{MaxRows: 2000, MaxBytes: "1KiB", MaxWait: "2ms"}},
	}
	for repeat := 0; repeat < 3; repeat++ {
		for j := range cases {
			tc := cases[(j+repeat)%len(cases)]
			t.Run(fmt.Sprintf("r%d_%s", repeat+1, tc.name), func(t *testing.T) {
				runPGGroupCommitSweep(t, src, dst, tc.batch)
			})
		}
	}
}

func runPGGroupCommitSweep(t *testing.T, src, dst *pgNode, batch config.BatchConfig) {
	const rows = 8192
	const table, logTable = "batchbench.work", "batchbench.traffic"
	name := filepath.Base(t.Name())
	for _, node := range []*pgNode{src, dst} {
		node.exec(t, "CREATE SCHEMA batchbench",
			"CREATE TABLE "+table+" (id bigint NOT NULL, payload text)",
			"INSERT INTO "+table+" SELECT i, md5(i::text) FROM generate_series(1,8192) i",
			"CREATE INDEX ON "+table+"(id)", "ANALYZE "+table,
			"CREATE TABLE "+logTable+" (id bigint PRIMARY KEY, event_time timestamptz NOT NULL)")
	}
	src.exec(t, "ALTER TABLE "+table+" REPLICA IDENTITY FULL", "CREATE PUBLICATION batchbench FOR TABLE "+table+", "+logTable)
	t.Cleanup(func() {
		src.exec(t, "SELECT pg_drop_replication_slot('batchbench')", "DROP PUBLICATION batchbench", "DROP SCHEMA batchbench CASCADE")
		dst.exec(t, "DROP SCHEMA batchbench CASCADE")
	})
	var pos string
	if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot('batchbench','pgoutput',false,true)").Scan(&pos); err != nil {
		t.Fatal(err)
	}
	cfg := strandCfg{Name: name, SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: ", slot: batchbench, publication: batchbench", Include: []string{table, logTable}, Workers: 1, CommitOrder: "strict", Batch: &batch,
		Dir: testDir(t), Bootstrap: "start_at", TablesExtra: klKeylessCfg, PostSeedWindow: "1s"}
	trace := filepath.Join(cfg.Dir, "batch.trace")
	t.Setenv("STRAND_TXN_TRACE", trace)
	path := cfg.write(t)
	proc := startStrand(t, path, "--start-at", pos)
	t.Cleanup(proc.kill9)
	waitSynced(t, &cfg, proc, time.Minute)
	var event int64
	for _, interval := range []time.Duration{10 * time.Millisecond, 0} {
		runKeylessBaseline(t, src.DB, dst.DB, proc, filepath.Join(cfg.Dir, "state"), table, logTable, rows, &event, 30, "update-delete", interval, 0)
	}
	type phase struct {
		interval   time.Duration
		start, end int64
	}
	var phases []phase
	for _, interval := range []time.Duration{10 * time.Millisecond, 0} {
		start := time.Now().UnixNano()
		runKeylessBaseline(t, src.DB, dst.DB, proc, filepath.Join(cfg.Dir, "state"), table, logTable, rows, &event, 250, "update-delete", interval, 1)
		phases = append(phases, phase{interval, start, time.Now().UnixNano()})
	}
	if code := proc.stop(t); code != 0 {
		t.Fatalf("strand exited %d: %s", code, proc.tail())
	}
	for _, phase := range phases {
		reportBatchTimings(t, trace, phase.interval, phase.start, phase.end)
	}

	// Write duplicates offline, then restart using only the durable checkpoint.
	src.exec(t, "INSERT INTO "+table+" VALUES (-1,NULL),(-1,NULL)")
	proc = startStrand(t, path)
	t.Cleanup(proc.kill9)
	deadline := time.Now().Add(time.Minute)
	for {
		var n int
		if err := dst.DB.QueryRow("SELECT count(*) FROM " + table + " WHERE id=-1 AND payload IS NULL").Scan(&n); err != nil {
			t.Fatal(err)
		} else if n == 2 {
			break
		}
		if proc.exited() || time.Now().After(deadline) {
			t.Fatalf("resume from checkpoint: %s", proc.tail())
		}
		time.Sleep(20 * time.Millisecond)
	}
	checkKeylessBaselineRows(t, src.DB, dst.DB, table)
	var markers int64
	if err := dst.DB.QueryRow("SELECT count(*) FROM " + logTable).Scan(&markers); err != nil || markers != event {
		t.Fatalf("markers after restart: %d, want %d: %v", markers, event, err)
	}
	if code := proc.stop(t); code != 0 {
		t.Fatalf("strand restart exited %d: %s", code, proc.tail())
	}
}

func reportBatchTimings(t *testing.T, path string, interval time.Duration, start, end int64) {
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type sample struct {
		routed, gather, gathered, commit, committed int64
		count                                       int64
	}
	data := map[uint64]*sample{}
	scan := bufio.NewScanner(f)
	valid := false
	for scan.Scan() {
		line := scan.Text()
		if strings.HasPrefix(line, "#") {
			var allocated, stored, dropped, unpublished, observed int
			_, err := fmt.Sscanf(line, "# allocated=%d stored=%d dropped=%d unpublished=%d observed_after_dump=%d",
				&allocated, &stored, &dropped, &unpublished, &observed)
			valid = err == nil && allocated == stored && allocated == observed && dropped == 0 && unpublished == 0
			continue
		}
		var ts, a, b int64
		var k int
		var seq, lsn, xid uint64
		if _, err := fmt.Sscanf(line, "%d k=%d lsn=%X xid=%d seq=%d a=%d b=%d", &ts, &k, &lsn, &xid, &seq, &a, &b); err != nil {
			t.Fatal(err)
		}
		if ts < start || ts > end {
			continue
		}
		if data[seq] == nil {
			data[seq] = &sample{}
		}
		v := data[seq]
		switch k {
		case txntrace.Routed:
			v.routed = ts
		case txntrace.GatherStart:
			v.gather = ts
		case txntrace.GatherEnd:
			v.gathered = ts
			v.count = a
		case txntrace.CommitStart:
			v.commit = ts
		case txntrace.CommitEnd:
			v.committed = ts
		}
	}
	if scan.Err() != nil || !valid {
		t.Fatalf("incomplete trace: %v", scan.Err())
	}
	var group, commit, queue []time.Duration
	var total int64
	for _, v := range data {
		if v.gather == 0 || v.gathered == 0 || v.commit == 0 || v.committed == 0 {
			continue
		}
		group = append(group, time.Duration(v.gathered-v.gather))
		commit = append(commit, time.Duration(v.committed-v.commit))
		// Admit publishes to the worker just before the router stamps Routed.
		// Clamp that sub-transition race to zero. Only the first txn in each batch is sampled.
		if v.routed != 0 {
			queue = append(queue, time.Duration(max(int64(0), v.gather-v.routed)))
		}
		total += v.count
	}
	if len(group) == 0 || len(queue) == 0 {
		t.Fatal("no complete batch trace")
	}
	pct := func(v []time.Duration, p int) time.Duration { slices.Sort(v); return v[(p*len(v)+99)/100-1] }
	t.Logf("offered=%s batches=%d tx/batch=%.2f first_dispatch_p95=%s gather_p50=%s gather_p95=%s commit_p50=%s commit_p95=%s",
		interval, len(group), float64(total)/float64(len(group)), pct(queue, 95), pct(group, 50), pct(group, 95), pct(commit, 50), pct(commit, 95))
}
