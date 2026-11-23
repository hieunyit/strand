package e2e

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"strand/internal/config"
)

// Measure observation lag through both CDC binaries against the same two
// MySQL servers. Run after building the experimental binary with the overlay.
func TestMySQLKeylessRoundTripPipeline(t *testing.T) {
	reference := os.Getenv("STRAND_MYSQL_KEYLESS_REFERENCE")
	if reference == "" || os.Getenv("STRAND_KEYLESS_CANDIDATE_BIN") == "" {
		t.Skip("set STRAND_MYSQL_KEYLESS_REFERENCE and STRAND_KEYLESS_CANDIDATE_BIN")
	}
	src, dst := newPair(t)
	for _, keyed := range []bool{false, true} {
		mode, ddl, extra := "keyless", "INDEX(id)", klKeylessCfg
		if keyed {
			mode, ddl, extra = "keyed", "PRIMARY KEY(id)", ""
		}
		t.Run(mode, func(t *testing.T) {
			for _, node := range []*mysqlNode{src, dst} {
				node.exec(t, "CREATE TABLE app.work (id INT NOT NULL, payload VARCHAR(24), "+ddl+")",
					"CREATE TABLE app.traffic (id BIGINT PRIMARY KEY, event_ns BIGINT NOT NULL)",
					"INSERT INTO app.work VALUES (1,'old'),(2,'OLD'),(3,NULL),(4,'old ')")
				if !keyed {
					node.exec(t, "INSERT INTO app.work VALUES (1,'old')")
				}
			}
			defer src.exec(t, "DROP TABLE app.traffic, app.work")
			defer dst.exec(t, "DROP TABLE app.traffic, app.work")
			runKeylessPairs(t, reference, 3, func(t *testing.T, bin string) {
				src.exec(t, "TRUNCATE TABLE app.traffic")
				dst.exec(t, "TRUNCATE TABLE app.traffic")
				start := strings.ReplaceAll(src.gtidExecuted(t), "\n", "")
				cfg := strandCfg{Name: "my_p2_" + mode + "_" + filepath.Base(t.Name()), SourceDSN: src.RootDSN,
					TargetDSN: dst.RootDSN + "strand", Include: []string{"app.work", "app.traffic"},
					Dir: testDir(t), Bootstrap: "start_at", TablesExtra: extra, PostSeedWindow: "1s",
					CommitOrder: "strict", Batch: &config.BatchConfig{MaxRows: 2000, MaxBytes: "4MiB", MaxWait: "2ms"}}
				proc := startStrandBinary(t, bin, cfg.write(t), "--start-at", start)
				defer proc.kill9()
				waitSynced(t, &cfg, proc, time.Minute)
				var eventID int64
				for _, rate := range []time.Duration{20 * time.Millisecond, 0} {
					runMySQLRoundTripLoad(t, src, dst, proc, &eventID, 30, rate, false)
					runMySQLRoundTripLoad(t, src, dst, proc, &eventID, 300, rate, true)
				}
				before, err := klMulti(src.DB, "SELECT id,COALESCE(HEX(payload),'null') FROM app.work")
				if err != nil {
					t.Fatal(err)
				}
				after, err := klMulti(dst.DB, "SELECT id,COALESCE(HEX(payload),'null') FROM app.work")
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("work multiset: source=%v target=%v err=%v", before, after, err)
				}
				if rc := proc.stop(t); rc != 0 {
					t.Fatalf("strand exit %d\n%s", rc, proc.tail())
				}
			})
		})
	}
}

func runMySQLRoundTripLoad(t *testing.T, src, dst *mysqlNode, proc *strandProc, eventID *int64,
	count int, interval time.Duration, report bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	first, start := *eventID, time.Now()
	done := make(chan error, 1)
	go func() {
		for n := 1; n <= count; n++ {
			if n > 1 && interval > 0 {
				timer := time.NewTimer(time.Until(start.Add(time.Duration(n-1) * interval)))
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					done <- ctx.Err()
					return
				}
			}
			id := first + int64(n)
			err := func() error {
				tx, err := src.DB.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				for k := range 4 {
					temp := -(id*4 + int64(k))
					for _, query := range []string{
						"INSERT INTO app.work VALUES (?,'before')",
						"UPDATE app.work SET payload='after' WHERE id=?",
						"DELETE FROM app.work WHERE id=?",
					} {
						if _, err := tx.ExecContext(ctx, query, temp); err != nil {
							return err
						}
					}
				}
				if _, err := tx.ExecContext(ctx, "INSERT INTO app.traffic VALUES (?,?)", id, time.Now().UnixNano()); err != nil {
					return err
				}
				return tx.Commit()
			}()
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	seen := make(map[int64]bool, count)
	latencies := make([]time.Duration, 0, count)
	for len(seen) < count {
		if proc.exited() {
			t.Fatalf("pipeline exited under load\n%s", proc.tail())
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			done = nil
		case <-ctx.Done():
			t.Fatalf("target observed %d of %d: %v", len(seen), count, ctx.Err())
		default:
		}
		rs, err := dst.DB.QueryContext(ctx, "SELECT id,event_ns FROM app.traffic WHERE id>? AND id<=?", first, first+int64(count))
		if err != nil {
			t.Fatal(err)
		}
		observed := time.Now().UnixNano()
		for rs.Next() {
			var id, ns int64
			if err := rs.Scan(&id, &ns); err != nil {
				rs.Close()
				t.Fatal(err)
			}
			if !seen[id] {
				seen[id] = true
				latencies = append(latencies, time.Duration(observed-ns))
			}
		}
		rs.Close()
		if err := rs.Err(); err != nil {
			t.Fatal(err)
		}
		if len(seen) < count {
			time.Sleep(5 * time.Millisecond)
		}
	}
	*eventID += int64(count)
	slices.Sort(latencies)
	if report {
		t.Logf("offered_interval=%s tx/s=%.1f lag_p95=%s lag_p99=%s",
			interval, float64(count)/time.Since(start).Seconds(),
			latencies[(95*count+99)/100-1], latencies[(99*count+99)/100-1])
	}
}
