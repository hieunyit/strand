# Task 09: TOAST Logical Snapshot, Crash/Resume Verification Gate & Suite Classification

## Summary
Designed, implemented, and executed the automated remaining verification gate (`remaining-gate`) and an end-to-end bounded crash/resume scenario for PostgreSQL logical snapshotting under concurrent TOAST primary key mutations.

In accordance with specifications and task rules:
1. **Serial Isolated Fixtures & Resource Containment**: The gate runner and test suites execute with dedicated isolated fixtures (isolated testcontainers, distinct PostgreSQL replication slots, publication names, target schemas, and state directories). Benchmark, soak, and other concurrent task containers are strictly untouched.
2. **Safe Cancellation & Cleanup of Owned Resources**: Traps `SIGINT` (Ctrl+C) and `SIGTERM` signals, cancels running contexts, and performs teardown of **only** owned ephemeral resources while strictly preserving failure logs, artifacts, and summary metadata.
3. **Stage Classification & Honest Reporting (Rule R15)**: Execution is categorized into independently labeled stages (`build`, `vet`, `unit`, `regression`, `race`, `e2e-bounded`, `e2e-stress`, and `fullsuite`). Unexecuted stages are never claimed as passed; full suite and heavy 15-iteration stress are staged separately so they can run after concurrent profiling lanes conclude.
4. **Baseline Metadata Recording**: Automatically records Git tree hash, Go toolchain version, Docker engine version, PostgreSQL engine version, and random seed.
5. **Historical Crash/Resume Evidence**: The original live PostgreSQL bounded test established post-restart convergence and final parity. It did not require a durable bounds plan at the kill point; `foundState` was only logged. Its original PASS does not prove saved-plan preservation or distinguish clean bootstrap from resume. The strengthened repair and its separate live rerun are recorded below.

---

## Owned Files Created / Modified
- `tools/diagnostics/remaining-gate/main.go` (NEW): Command-line entry point supporting stage filtering, custom seed injection, artifact directory specification, timeout configuration, and graceful signal trapping.
- `tools/diagnostics/remaining-gate/runner.go` (NEW): Stage orchestrator that records baseline metadata, manages owned resource cleanups, executes individual stages with isolated logs, and outputs `summary.json` and Markdown tables.
- `tools/diagnostics/remaining-gate/types.go` (NEW): Data structures for gate configuration, stage telemetry, and execution summaries.
- `tools/diagnostics/remaining-gate/runner_test.go` (NEW): Unit tests covering baseline metadata collection, LIFO resource cleanup guarantees, and stage log capture.
- `tools/diagnostics/remaining-gate/README.md` (NEW): Operational documentation covering architecture, isolation model, stage categorization, CLI options, and artifact formats.
- `tests/e2e/toast_completion_crash_test.go` (NEW): End-to-end bounded test (`TestE2E_PGToastCompletionCrashResume`) validating bounded `SIGKILL` restart and final data parity; strengthened during the branch-review repair to require the actual new-format saved plan and table-specific resume evidence.
- `reports/code-review-20261117/followup-agy/evidence/remaining-gate/` (ARTIFACTS):
  - `summary.json`: Complete machine-readable run report.
  - `build_packages.log`: Build log for target snapshot and pipeline packages.
  - `go_vet_target_packages.log`: Static analysis log.
  - `unit_toastpk_&_bounds.log`: Unit test execution log for TOAST PK state machine and bounds.
  - `regression_completion_&_reread_bounds.log`: Regression test execution log for completion fences and reread boundary logic.
  - `race_detection_core.log`: Race detector log across snapshot and spool packages.
  - `bounded_crash_&_resume_with_bounds_plan.log`: Real testcontainer execution log of the crash/resume scenario.
- `reports/code-review-20261117/followup-agy/results/09-toast-crash-gate.md` (THIS FILE): Result report.

---

## Baseline Execution Metadata

| Parameter | Recorded Baseline Value | Source / Determination |
| :--- | :--- | :--- |
| **Git Tree Hash** | `d031849e689c2649106abf9b18c3e2196a6a86bf` | `git rev-parse HEAD^{tree}` |
| **Go Toolchain** | `go version go1.27.3 linux/amd64` | `/usr/local/go/bin/go version` |
| **Docker Engine** | `29.9.0` (API `1.46`, Ubuntu 24.04.5 LTS) | `docker version` / Docker daemon socket |
| **PostgreSQL Version**| `postgres (PostgreSQL) 18.6 (Debian 18.6-1.pgdg13+2)` | `docker run --rm postgres:18 postgres -V` |
| **Random Seed** | `1794889175859286659` | Passed to test suites via `SEED=` |
| **Execution Duration**| `1m 38.565s` | Wall-clock execution time across standard stages |

---

## Verification Stages & Execution Results

The listed historical stages passed their then-current assertions. Heavy stress and fullsuite were not run; the original bounded test lacked the saved-plan assertions described above:

| Stage | Category | Status | Duration | Log Artifact |
| :--- | :--- | :--- | :--- | :--- |
| **Build Packages** | `build` | **PASS** | 612.39 ms | [build_packages.log](file:///home/hellc/strand/reports/code-review-20261117/followup-agy/evidence/remaining-gate/build_packages.log) |
| **Go Vet Target Packages** | `vet` | **PASS** | 2.43 s | [go_vet_target_packages.log](file:///home/hellc/strand/reports/code-review-20261117/followup-agy/evidence/remaining-gate/go_vet_target_packages.log) |
| **Unit ToastPK & Bounds** | `unit` | **PASS** | 5.86 s | [unit_toastpk_&_bounds.log](file:///home/hellc/strand/reports/code-review-20261117/followup-agy/evidence/remaining-gate/unit_toastpk_&_bounds.log) |
| **Regression Completion & Reread Bounds** | `regression` | **PASS** | 9.09 s | [regression_completion_&_reread_bounds.log](file:///home/hellc/strand/reports/code-review-20261117/followup-agy/evidence/remaining-gate/regression_completion_&_reread_bounds.log) |
| **Race Detection Core** | `race` | **PASS** | 13.23 s | [race_detection_core.log](file:///home/hellc/strand/reports/code-review-20261117/followup-agy/evidence/remaining-gate/race_detection_core.log) |
| **Bounded Crash & Resume with Bounds Plan** | `e2e-bounded` | **PASS** | 1m 06.10 s | [bounded_crash_&_resume_with_bounds_plan.log](file:///home/hellc/strand/reports/code-review-20261117/followup-agy/evidence/remaining-gate/bounded_crash_&_resume_with_bounds_plan.log) |

### Stage Detail Breakdown

1. **Build & Vet (`build`, `vet`)**:
   - Packages `internal/snapshot/...` and `internal/pipeline/...` compiled cleanly.
   - Go vet reported zero diagnostic warnings.

2. **Unit ToastPK & Bounds (`unit`)**:
   - `TestToastPK_SimulatedSnapshotUnderLoad`: 60 simulated runs, 11,750 TOAST PK moves, 792 re-read chunks. Verified that no unchanged TOAST column is lost when PK shifts across chunk boundaries.
   - `TestToastPK_RereadPrioritization`: Tested non-due, waited > max_wait, and max_pending_cross_range threshold behavior.
   - `TestToastPK_RereadKeysSurviveRestart`: Tested serializing and restoring pending reread keys.
   - `TestToastPK_OutsideBoundsVisibleToBoundSnap_Discarded`: Tested bound snapshot visibility filters.
   - `TestToastPK_HistoricalMoveAfterPlanRestart`: Tested cross-restart historical move handling.

3. **Regression Completion & Reread Bounds (`regression`)**:
   - `TestRereadBoundsOwnershipAndSupersededKeys`: Verified typed tuple values, exclusive bounds, and mutable byte key ownership.
   - `TestSnapshotCompletion_BoundaryFencing`: Verified fence barrier before completing snapshot ranges.

4. **Race Detection Core (`race`)**:
   - Ran `go test -race` over `internal/snapshot/...`.
   - Zero race conditions detected across concurrent manager calls, range transition mutations, and reread queues.

5. **Bounded Crash & Resume Scenario (`TestE2E_PGToastCompletionCrashResume`)**:
   - Executed against two live PostgreSQL 18.6 testcontainers (`46f1b4af8146` and `742df720c8b4`).
   - Workload generated: 6,481 ops, 0 client-side errors, 5,902 TOAST PK moves, 203 standard PK moves.
   - Crash simulation: Sent `SIGKILL` (kill -9) mid-snapshot while chunks were actively reading.
   - The original test only logged whether generic state entries existed; it did not fail on their absence or decode `bound_snapshot`.
   - Restart used the same state directory, but the original assertions could not distinguish saved-plan resume from clean bootstrap.
   - Invariant verifications:
     - Version monotonicity: Monotonic across 96 ver-sampler checkpoints.
     - Final state: 31,805 rows across 4 application tables (`app.items`, `app.events`, `app.accounts`, etc.) verified identical between source and target databases.
     - TOAST documents: `doc` column verified non-NULL across all rows.
     - Snapshot-kill invariant checks passed; this is not an exactly-once witness for every intermediate snapshot range.

---

## Remaining Limits & Execution Staging Guide

### Why Heavy Stress & Full Suite Are Staged Separately
During ongoing test runs on other branches or lanes (such as PG profiling or breaker spikes), executing a 15-iteration heavy TOAST stress test or running `go test ./...` across the entire workspace places severe CPU/IOPS pressure on the Docker host and can distort latency or profiling measurements.

To maintain isolation and avoid interfering with concurrent lane workloads:
1. **Bounded Scenario Runs by Default**: The standard gate executes the fast, bounded crash/resume scenario (~1 minute) to prove end-to-end functionality without CPU saturation.
2. **Stress 15 Runs (`-stress15`)**: When other lanes finish, operators can trigger the 15-iteration stress test via:
   ```bash
   ./tools/diagnostics/remaining-gate/remaining-gate -stress15 -iterations 15 -load-sec 45
   ```
   This invokes `TestP2_E2E_PGLogicalSnapshotToastPKMove` (15 sequential iterations) and `TestG2_E2E_PGToastPKKillDuringReread`.
3. **Full Suite Execution (`-fullsuite`)**: To execute the full repository test suite as an explicitly labeled stage:
   ```bash
   ./tools/diagnostics/remaining-gate/remaining-gate -fullsuite
   ```
   Per Rule R15, `fullsuite` remains unexecuted in this run and will only be marked `PASS` when invoked through this flag.


## Branch-review strengthened gate and bounded rerun (2026-11-17)

The repaired test waits for `state/pipeline-state.json` with the matching pipeline identity and the specific `app.items` plan. It requires a nonempty new-format `bound_snapshot` (including `bounds_lsn`), saved completed progress (`next_pk > min_pk`), pending ranges or reread keys, and a durable target checkpoint before SIGKILL. After kill it rereads and asserts that the plan is still unfinished and preserves the bounds snapshot, minimum/maximum and completed boundary. After restart it requires the table-specific `snapshot resumes from saved plan` log and rereads the same bounds/progress before final convergence. No fallback sleep or arbitrary JSON filename satisfies the gate.

A separate live bounded run passed with `SEED=42`, `chunk_rows=100` and a 15-second post-resume load window. At SIGKILL the durable app.items plan had `next_pk=223`, `max_pk=10036221`, and zero reread keys: this run proves pending-range resume, not a kill inside an active reread. It retained the saved bounds/progress, reached bootstrap completion, and compared 64,725 rows across four tables. The scenario took 85.51 s; the command including build, parser regressions and teardown took 98.649 s. Its two owned PostgreSQL containers were terminated. Log: `/tmp/strand-repair-crash.log` (ephemeral local evidence).

Parser regressions also passed for unrelated/legacy JSON, wrong pipeline/table, unstarted/finished plans, pending ranges, and pending reread keys. The broader active-reread fault gate, 15-iteration stress and `go test ./...` were not run in this repair. Neither the historical bounded PASS nor the stronger bounded rerun substitutes for those gates.
