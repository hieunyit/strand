package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"strand/internal/checkpoint"
	"strand/internal/model"
	"strand/internal/snapshot"
)

// TestE2E_PGToastCompletionCrashResume executes a bounded crash and resume scenario
// on a PostgreSQL logical snapshot under concurrent TOAST PK moves.
// It verifies:
// 1. Initial snapshot start with chunk planning and bounds.
// 2. Mid-flight SIGKILL interruption (simulated crash) during snapshot execution.
// 3. Saved checkpoint/plan preservation in the state directory.
// 4. Resume retaining the saved bounds snapshot and completed range boundary.
// 5. Final completion (bootstrap finished: pipeline synced) and exact data parity.
func TestE2E_PGToastCompletionCrashResume(t *testing.T) {
	seed := seedFromEnv(t)
	const span = 4000 // Bounded size for quick yet comprehensive multi-chunk execution

	suffix := fmt.Sprintf("%d", time.Now().UnixNano()%1000000)
	slot := fmt.Sprintf("strand_crash_slot_%s", suffix)
	pub := fmt.Sprintf("strand_crash_pub_%s", suffix)

	src, dst := setupPGLogical(t, pub, span, span/2, span)

	wl := newPGWorkload(src.DB, seed)
	wl.Start(2, span)

	var stop atomic.Bool
	var wg sync.WaitGroup
	var toastMoves, pkMoves atomic.Int64

	// Start concurrent movers: TOAST PK moves and standard PK moves
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go pgToastPKMover(src.DB, seed+int64(i), span, &stop, &wg, &toastMoves)
	}
	wg.Add(1)
	go pgPKMover(src.DB, seed+99, span/2, &stop, &wg, &pkMoves)

	var stopOnce sync.Once
	stopWorkload := func() {
		stopOnce.Do(func() {
			stop.Store(true)
			wg.Wait()
			wl.Stop()
		})
	}
	defer stopWorkload()

	// Configure Strand with IOPS throttle to ensure chunk pacing
	stateDir := testDir(t)
	cfg := strandCfg{
		Name:        "e2e-toast-completion-crash",
		SourceKind:  "postgres",
		TargetKind:  "postgres",
		SourceDSN:   src.DSN,
		TargetDSN:   dst.DSN,
		SourceExtra: fmt.Sprintf(", slot: %s, publication: %s, create_slot: true", slot, pub),
		Include:     []string{"app.*"},
		Dir:         stateDir,
		Bootstrap:   "logical",
		SourceIOPS:  30,    // Paced chunk reading
		ChunkRows:   "100", // Require several completed and pending ranges in this bounded fixture.
		LagSLO:      "10m",
	}

	cfgPath := cfg.write(t)
	vs := startPGVerSampler(dst, "app.items", "app.events", "app.accounts")

	// Phase 1: Start Strand
	p := startStrand(t, cfgPath)

	// Wait for the exact durable app.items plan, including the original bounds
	// visibility snapshot, completed progress and unfinished range/reread work.
	// A SQL query or a generic "snapshot" log is not a resume milestone.
	var before snapshot.SnapshotPlan
	deadline := time.Now().Add(90 * time.Second)
	for {
		pl, done, err := readCrashItemsPlan(stateDir, cfg.Name)
		cp, cpErr := checkpoint.LoadAppliedSnapshotPG(context.Background(), dst.DB, cfg.Name, model.EnginePG)
		if err == nil && !done && resumableCrashPlan(pl) && cpErr == nil && cp.Lwd().LSN > 0 {
			before = pl
			break
		}
		if p.exited() || time.Now().After(deadline) {
			t.Fatalf("never reached durable app.items bounds plan with saved progress and pending work: plan=%+v done=%v err=%v checkpoint=%v\n%s", pl, done, err, cpErr, p.tail())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("SIGKILL with durable app.items plan: next=%d max=%d reread=%d bounds=%+v", before.NextPK, before.MaxPK, len(before.Reread), before.BoundSnapshot)
	p.kill9()
	afterKill, done, err := readCrashItemsPlan(stateDir, cfg.Name)
	if err != nil || done || !resumableCrashPlan(afterKill) {
		t.Fatalf("crash missed pending saved plan: plan=%+v done=%v err=%v", afterKill, done, err)
	}
	if !reflect.DeepEqual(before.BoundSnapshot, afterKill.BoundSnapshot) || before.MinPK != afterKill.MinPK || before.MaxPK != afterKill.MaxPK || afterKill.NextPK < before.NextPK {
		t.Fatalf("saved app.items plan lost bounds/progress across SIGKILL: before=%+v after=%+v", before, afterKill)
	}

	// Phase 3: Resume Strand from the same stateDir
	t.Log("Resuming Strand from persisted checkpoint state...")
	p = startStrand(t, cfgPath)
	// Read the actual new-format plan during resume and require the table-specific
	// resume log. A clean bootstrap can converge too, so final parity alone is insufficient.
	resumeDeadline := time.Now().Add(30 * time.Second)
	for {
		resumed, done, err := readCrashItemsPlan(stateDir, cfg.Name)
		resumeLogged := false
		for _, line := range strings.Split(p.output.String(), "\n") {
			if strings.Contains(line, "snapshot resumes from saved plan") && strings.Contains(line, "app.items") {
				resumeLogged = true
				break
			}
		}
		if err == nil && !done && resumed.BoundSnapshot != nil && resumeLogged {
			if !reflect.DeepEqual(afterKill.BoundSnapshot, resumed.BoundSnapshot) || resumed.MinPK != afterKill.MinPK || resumed.MaxPK != afterKill.MaxPK || resumed.NextPK < afterKill.NextPK {
				t.Fatalf("resume replaced saved app.items bounds/progress: saved=%+v resumed=%+v", afterKill, resumed)
			}
			break
		}
		if p.exited() || time.Now().After(resumeDeadline) || done {
			t.Fatalf("did not observe app.items resume from original bound_snapshot plan: done=%v err=%v\n%s", done, err, p.tail())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Stop workload after bounded window to allow convergence
	loadWindow := time.Duration(envInt("STRAND_E2E_PG_TOAST_PK_LOAD_SECONDS", 15)) * time.Second
	time.AfterFunc(loadWindow, stopWorkload)

	// Phase 4: Wait for logical bootstrap completion
	waitSynced(t, &cfg, p, 10*time.Minute)

	syncedDeadline := time.Now().Add(10 * time.Minute)
	for !strings.Contains(p.output.String(), "bootstrap finished: pipeline synced") {
		if p.exited() || time.Now().After(syncedDeadline) {
			t.Fatalf("logical bootstrap did not finish after resume\n%s", p.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}

	stopWorkload()
	t.Logf("Workload summary: %d ops, %d errs, %d TOAST PK moves, %d standard PK moves",
		wl.ops.Load(), wl.errs.Load(), toastMoves.Load(), pkMoves.Load())

	// Phase 5: Catch up and clean shutdown
	waitPGCaughtUp(t, src, dst, &cfg, p, 5*time.Minute)
	if code := p.stop(t); code != 0 {
		t.Fatalf("strand exit code %d upon stop\n%s", code, p.tail())
	}

	// Phase 6: Invariant verification
	out := p.output.String()
	if strings.Contains(out, "not supported yet") {
		t.Fatalf("PK change with unchanged TOAST was rejected\n%s", p.tail())
	}

	vs.check(t)
	comparePG(t, src, dst, pgAppTables...)
	checkPGInvariantsSnapshotKilled(t, dst)
	t.Log("PASS: Crash and resume scenario successfully completed with verified data parity and checkpoint invariants.")
}

// readCrashItemsPlan deliberately decodes the product's new plan format, including
// BoundSnapshot; unrelated JSON or a Pebble directory cannot satisfy the gate.
func readCrashItemsPlan(dir, pipeline string) (snapshot.SnapshotPlan, bool, error) {
	var state struct {
		Pipeline string                           `json:"pipeline_id"`
		Done     map[string]bool                  `json:"snapshot_done"`
		Plans    map[string]snapshot.SnapshotPlan `json:"snapshot_plans"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "state", "pipeline-state.json"))
	if err != nil {
		return snapshot.SnapshotPlan{}, false, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return snapshot.SnapshotPlan{}, false, err
	}
	if state.Pipeline != pipeline {
		return snapshot.SnapshotPlan{}, false, fmt.Errorf("wrong pipeline identity %q", state.Pipeline)
	}
	pl, ok := state.Plans["app.items"]
	if !ok {
		return pl, state.Done["app.items"], fmt.Errorf("missing app.items snapshot plan")
	}
	return pl, state.Done["app.items"], nil
}
func resumableCrashPlan(pl snapshot.SnapshotPlan) bool {
	return pl.BoundSnapshot != nil && pl.BoundSnapshot.Xmax > 0 && pl.BoundSnapshot.BoundsLSN > 0 &&
		pl.NextPK > pl.MinPK && ((!pl.Finished && pl.NextPK <= pl.MaxPK) || len(pl.Reread) > 0)
}

func TestCrashItemsPlanRequiresBoundsAndPendingProgress(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "state"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "state", "pipeline-state.json")
	cases := []struct {
		name, raw string
		valid     bool
	}{
		{"unrelated JSON", `{"checkpoint":"present"}`, false},
		{"legacy plan without bounds", `{"pipeline_id":"test","snapshot_plans":{"app.items":{"min_pk":1,"max_pk":100,"next_pk":20}}}`, false},
		{"wrong table", `{"pipeline_id":"test","snapshot_plans":{"app.other":{"min_pk":1,"max_pk":100,"next_pk":20,"bound_snapshot":{"Xmin":5,"Xmax":10,"bounds_lsn":42}}}}`, false},
		{"wrong pipeline", `{"pipeline_id":"other","snapshot_plans":{"app.items":{"min_pk":1,"max_pk":100,"next_pk":20,"bound_snapshot":{"Xmin":5,"Xmax":10,"bounds_lsn":42}}}}`, false},
		{"unstarted", `{"pipeline_id":"test","snapshot_plans":{"app.items":{"min_pk":1,"max_pk":100,"next_pk":1,"bound_snapshot":{"Xmin":5,"Xmax":10,"bounds_lsn":42}}}}`, false},
		{"finished", `{"pipeline_id":"test","snapshot_plans":{"app.items":{"min_pk":1,"max_pk":100,"next_pk":101,"finished":true,"bound_snapshot":{"Xmin":5,"Xmax":10,"bounds_lsn":42}}}}`, false},
		{"pending ranges", `{"pipeline_id":"test","snapshot_plans":{"app.items":{"min_pk":1,"max_pk":100,"next_pk":20,"bound_snapshot":{"Xmin":5,"Xmax":10,"bounds_lsn":42}}}}`, true},
		{"pending reread", `{"pipeline_id":"test","snapshot_plans":{"app.items":{"min_pk":1,"max_pk":100,"next_pk":101,"finished":true,"reread_keys":[22],"bound_snapshot":{"Xmin":5,"Xmax":10,"bounds_lsn":42}}}}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.raw), 0o600); err != nil {
				t.Fatal(err)
			}
			pl, done, err := readCrashItemsPlan(dir, "test")
			got := err == nil && !done && resumableCrashPlan(pl)
			if got != tc.valid {
				t.Fatalf("gate=%v want=%v plan=%+v err=%v", got, tc.valid, pl, err)
			}
		})
	}
}
