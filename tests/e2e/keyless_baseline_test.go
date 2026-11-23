package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPGKeylessPerformanceBaseline(t *testing.T) {
	if os.Getenv("STRAND_KEYLESS_BENCH") != "1" {
		t.Skip("set STRAND_KEYLESS_BENCH=1 to run the PostgreSQL CDC benchmark")
	}
	src := startPG(t, "keys-perf-src", "-c", "fsync=on", "-c", "synchronous_commit=on", "-c", "work_mem=4MB")
	dst := startPG(t, "keys-perf-dst", "-c", "fsync=on", "-c", "synchronous_commit=on", "-c", "work_mem=4MB", "-c", "synchronize_seqscans=off", "-c", "track_commit_timestamp=on")
	if reference := os.Getenv("STRAND_KEYLESS_REFERENCE_BIN"); reference != "" {
		// The keyed control crosses the two binaries on the same warmed
		// database and checkpoint, so reseeding cannot skew its comparison.
		t.Run("keyed_control", func(t *testing.T) {
			forEachRun := func(measure func(*testing.T, string, []string, int, int)) {
				runKeylessPairs(t, reference, envInt("STRAND_KEYLESS_CONTROL_PAIRS", 6), func(t *testing.T, binary string) {
					measure(t, binary, []string{"update-delete"}, 400, 1)
				})
			}
			withPGKeylessFixture(t, src, dst, "keyed_control", 1_000_000, false, true, forEachRun)
		})
		// Reseed each keyless run to compare exactly the same rows and index.
		t.Run("indexed_keyless", func(t *testing.T) {
			runKeylessPairs(t, reference, 4, func(t *testing.T, binary string) {
				name := filepath.Base(t.Name())
				runPGKeylessFixture(t, src, dst, name, binary, 1_000_000, true, true,
					[]string{"update-delete"}, 300, 1)
			})
		})
		return
	}
	for _, rows := range []int{100_000, 1_000_000} {
		for _, keyless := range []bool{false, true} {
			name := fmt.Sprintf("kb_%d_%t", rows, keyless)
			t.Run(name, func(t *testing.T) {
				runPGKeylessFixture(t, src, dst, name, strandBin, rows, keyless, os.Getenv("STRAND_KEYLESS_TARGET_INDEX") == "1",
					[]string{"insert", "mixed", "update-delete"}, 60, 3)
			})
		}
	}
}

func runKeylessPairs(t *testing.T, reference string, pairs int, measure func(*testing.T, string)) {
	t.Helper()
	candidate := os.Getenv("STRAND_KEYLESS_CANDIDATE_BIN")
	if candidate == "" {
		candidate = strandBin
	}
	for pair := 1; pair <= pairs; pair++ {
		variants := []struct{ name, path string }{{"reference", reference}, {"candidate", candidate}}
		if pair%2 == 0 {
			variants[0], variants[1] = variants[1], variants[0]
		}
		for _, variant := range variants {
			t.Run(fmt.Sprintf("pair_%d_%s", pair, variant.name), func(t *testing.T) {
				measure(t, variant.path)
			})
		}
	}
}

func runPGKeylessFixture(t *testing.T, src, dst *pgNode, name, binary string, rows int, keyless, targetIndex bool,
	mixes []string, transactions, repeats int) {
	t.Helper()
	withPGKeylessFixture(t, src, dst, name, rows, keyless, targetIndex, func(measure func(*testing.T, string, []string, int, int)) {
		measure(t, binary, mixes, transactions, repeats)
	})
}

func withPGKeylessFixture(t *testing.T, src, dst *pgNode, name string, rows int, keyless, targetIndex bool,
	run func(func(*testing.T, string, []string, int, int))) {
	t.Helper()
	table := name + ".work"
	logTable := name + ".traffic"
	key := " PRIMARY KEY"
	extra := ""
	if keyless {
		key, extra = "", klKeylessCfg
	}
	for _, db := range []*pgNode{src, dst} {
		db.exec(t, "CREATE SCHEMA "+name,
			"CREATE TABLE "+table+" (id bigint NOT NULL"+key+", payload text)",
			fmt.Sprintf("INSERT INTO %s SELECT i, md5(i::text) FROM generate_series(1, %d) i", table, rows),
			"CREATE TABLE "+logTable+" (id bigint PRIMARY KEY, event_time timestamptz NOT NULL)",
			"ANALYZE "+table)
	}
	if keyless {
		src.exec(t, "ALTER TABLE "+table+" REPLICA IDENTITY FULL", "CREATE INDEX ON "+table+" (id)")
		if targetIndex {
			dst.exec(t, "CREATE INDEX ON "+table+" (id)", "ANALYZE "+table)
		}
	}
	src.exec(t, "CREATE PUBLICATION "+name+" FOR TABLE "+table+", "+logTable)
	var lsn string
	if err := src.DB.QueryRow("SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', false, true)", name).Scan(&lsn); err != nil {
		t.Fatal(err)
	}
	cfg := strandCfg{Name: name, SourceKind: "postgres", TargetKind: "postgres", SourceDSN: src.DSN, TargetDSN: dst.DSN,
		SourceExtra: ", slot: " + name + ", publication: " + name, Include: []string{table, logTable},
		Dir: testDir(t), Bootstrap: "start_at", TablesExtra: extra, PostSeedWindow: "1s"}
	path := cfg.write(t)
	start := []string{"--start-at", lsn}
	var event int64
	run(func(t *testing.T, binary string, mixes []string, transactions, repeats int) {
		proc := startStrandBinary(t, binary, path, start...)
		start = nil
		defer proc.kill9()
		waitSynced(t, &cfg, proc, 3*time.Minute)
		for _, mix := range mixes {
			for _, offered := range []time.Duration{10 * time.Millisecond, 0} {
				for repeat := 0; repeat <= repeats; repeat++ {
					count := transactions
					if repeat == 0 {
						count = min(50, transactions/2)
					}
					runKeylessBaseline(t, src.DB, dst.DB, proc, filepath.Join(cfg.Dir, "state"),
						table, logTable, rows, &event, count, mix, offered, repeat)
				}
			}
		}
		if code := proc.stop(t); code != 0 {
			t.Fatalf("strand exit %d\n%s", code, proc.tail())
		}
	})
	checkKeylessBaselineRows(t, src.DB, dst.DB, table)
	src.exec(t, "SELECT pg_drop_replication_slot('"+name+"')", "DROP PUBLICATION "+name, "DROP SCHEMA "+name+" CASCADE")
	dst.exec(t, "DROP SCHEMA "+name+" CASCADE")
}

func runKeylessBaseline(t *testing.T, src, dst *sql.DB, proc *strandProc, spool, table, logTable string,
	rows int, event *int64, count int, mix string, interval time.Duration, repeat int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	started, first := time.Now(), *event
	done := make(chan error, 1)
	go func() {
		for n := 1; n <= count; n++ {
			if n > 1 && interval > 0 {
				timer := time.NewTimer(time.Until(started.Add(time.Duration(n-1) * interval)))
				select {
				case <-ctx.Done():
					timer.Stop()
					done <- ctx.Err()
					return
				case <-timer.C:
				}
			}
			id := first + int64(n)
			kind := n % 10
			query := "INSERT INTO " + table + " VALUES($1, md5(($1::bigint)::text))"
			arg := int64(rows) + id
			if (mix == "mixed" && kind >= 4) || (mix == "update-delete" && kind >= 1) {
				arg = 1 + (id*104729)%int64(rows)
				query = "UPDATE " + table + " SET payload=payload||'X' WHERE id=$1"
				if kind >= 7 {
					query = "DELETE FROM " + table + " WHERE id=$1"
				}
			}
			tx, err := src.BeginTx(ctx, nil)
			if err == nil {
				_, err = tx.ExecContext(ctx, query, arg)
				if err == nil {
					_, err = tx.ExecContext(ctx, "INSERT INTO "+logTable+" VALUES($1, clock_timestamp())", id)
				}
				if err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	latencies := make([]time.Duration, 0, count)
	var applyLag, observeLag []time.Duration
	seen := make(map[int64]bool, count)
	var peakSpool int64
	for len(seen) < count {
		if proc.exited() {
			t.Fatalf("pipeline exited during load\n%s", proc.tail())
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			done = nil
		case <-ctx.Done():
			t.Fatalf("target caught %d of %d transactions: %v", len(seen), count, ctx.Err())
		default:
		}
		result, err := dst.QueryContext(ctx, "SELECT id,event_time,pg_xact_commit_timestamp(xmin) FROM "+logTable+" WHERE id>$1 AND id<=$2", first, first+int64(count))
		if err != nil {
			t.Fatal(err)
		}
		seenAt := time.Now()
		for result.Next() {
			var id int64
			var eventTime, appliedAt time.Time
			if err := result.Scan(&id, &eventTime, &appliedAt); err != nil {
				result.Close()
				t.Fatal(err)
			}
			if !seen[id] {
				seen[id] = true
				latencies = append(latencies, seenAt.Sub(eventTime))
				applyLag = append(applyLag, appliedAt.Sub(eventTime))
				observeLag = append(observeLag, seenAt.Sub(appliedAt))
			}
		}
		result.Close()
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		var bytes int64
		_ = filepath.WalkDir(spool, func(_ string, entry fs.DirEntry, err error) error {
			if err == nil && entry.Type().IsRegular() {
				if info, err := entry.Info(); err == nil {
					bytes += info.Size()
				}
			}
			return nil
		})
		peakSpool = max(peakSpool, bytes)
		if len(seen) < count {
			time.Sleep(5 * time.Millisecond)
		}
	}
	*event += int64(count)
	for _, durations := range [][]time.Duration{latencies, applyLag, observeLag} {
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	}
	percentile := func(p int) time.Duration { return latencies[(p*len(latencies)+99)/100-1] }
	status, _ := os.ReadFile("/proc/" + strconv.Itoa(proc.cmd.Process.Pid) + "/status")
	var peakRSS string
	for _, line := range strings.Split(string(status), "\n") {
		if value, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			peakRSS = strings.TrimSpace(value)
		}
	}
	t.Logf("mix=%s offered_interval=%s repeat=%d tx/s=%.1f lag_p50=%s lag_p95=%s lag_p99=%s peak_spool_bytes=%d pipeline_peak_rss=%q apply_p99=%s observe_p99=%s",
		mix, interval, repeat, float64(count)/time.Since(started).Seconds(), percentile(50), percentile(95), percentile(99), peakSpool, peakRSS, applyLag[(99*count+99)/100-1], observeLag[(99*count+99)/100-1])
}

func checkKeylessBaselineRows(t *testing.T, src, dst *sql.DB, table string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	query := `SELECT id,payload FROM ` + table + ` ORDER BY id,payload COLLATE "C"`
	a, err := src.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := dst.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for row := 1; ; row++ {
		srcOK, dstOK := a.Next(), b.Next()
		if srcOK != dstOK {
			t.Fatalf("different row counts at sorted row %d", row)
		}
		if !srcOK {
			if a.Err() != nil || b.Err() != nil {
				t.Fatalf("source read: %v, target read: %v", a.Err(), b.Err())
			}
			return
		}
		var srcID, dstID int64
		var srcPayload, dstPayload sql.NullString
		if a.Scan(&srcID, &srcPayload) != nil || b.Scan(&dstID, &dstPayload) != nil || srcID != dstID || srcPayload != dstPayload {
			t.Fatalf("different multisets at sorted row %d", row)
		}
	}
}
