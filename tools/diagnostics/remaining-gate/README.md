# Strand CDC Remaining Review Verification Gate

The `remaining-gate` tool automates the verification gate for remaining CDC review issues, focusing on PostgreSQL logical snapshotting, TOAST PK movements across chunk ranges, mid-flight crash/resume scenarios with the new bounds plan, race condition detection, and explicit full-suite test staging.

---

## Key Principles & Guardrails

1. **Serial Isolated Fixtures**:
   - Every stage and E2E test runs with isolated, uniquely named fixtures (unique replication slots, publication names, target schemas, and state directories).
   - Never touches, reboots, or disrupts running benchmark or soak test containers.
2. **Safe Cancellation & Cleanup of Owned Resources**:
   - Traps `SIGINT` (Ctrl+C) and `SIGTERM` to initiate orderly cancellation.
   - Cleans up **only** resources created by this tool (tracked temporary directories, isolated fixtures).
   - Preserves all failure artifacts, stage execution logs, and `summary.json` even when cancelled or failed.
3. **Explicit Stage Labeling (Honest Reporting)**:
   - Divides execution into clear, independently reported stages:
     - `build` / `vet`
     - `unit`
     - `regression`
     - `race`
     - `e2e-bounded`
     - `e2e-stress` (optional 15-iteration stress)
     - `fullsuite` (complete repository suite)
   - Adheres strictly to Rule R15: never marks or reports a stage as `PASS` unless it has actually been executed and verified.
4. **Baseline Metadata Recording**:
   - Records the baseline Git tree hash (`git rev-parse HEAD^{tree}`).
   - Records Go toolchain version (`go version`).
   - Records PostgreSQL engine versions (`postgres:18` / `postgres:16`).
   - Records deterministic seeds (`SEED`) for reproducible test executions.

---

## Verification Stages

| Stage | Category | Command / Scope | Purpose |
| :--- | :--- | :--- | :--- |
| **Build Packages** | `build` | `go build ./internal/snapshot/... ./internal/pipeline/...` | Verifies compiler correctness. |
| **Go Vet** | `vet` | `go vet ./internal/snapshot/... ./internal/pipeline/...` | Static analysis for subtle issues. |
| **Unit ToastPK & Bounds** | `unit` | `go test -v -run '^TestToastPK' ./internal/snapshot/...` | Verifies TOAST PK state machine transitions, reread queueing, cap limits, and bounds visibility. |
| **Regression Completion & Bounds** | `regression` | `go test -v -run '^Test(SnapshotCompletion\|ToastPK\|RereadBounds)' ...` | Ensures snapshot completion fences and reread boundary logic do not regress. |
| **Race Detection Core** | `race` | `go test -race -v -run '^Test(ToastPK\|Completion)' ./internal/snapshot/...` | Race detector verification across multi-threaded snapshot/manager routines. |
| **Bounded Crash/Resume** | `e2e-bounded` | `go test -v -run '^TestE2E_PGToastCompletionCrashResume$' ./tests/e2e/...` | Runs a bounded PostgreSQL logical snapshot under concurrent TOAST PK moves, issues SIGKILL mid-flight, validates checkpoint preservation, resumes, and verifies complete data parity. |
| **Stress ToastPK 15 Runs** | `e2e-stress` | `go test -v -run '^TestP2_E2E_PGLogicalSnapshotToastPKMove$' ./tests/e2e/...` | Stress tests 15 consecutive runs of TOAST PK moves across range transitions (opt-in via `-stress15`). |
| **Kill During Reread** | `e2e-stress` | `go test -v -run '^TestG2_E2E_PGToastPKKillDuringReread$' ./tests/e2e/...` | Interleaves SIGKILL during re-read SELECT queries and validates persistence across restarts (opt-in via `-stress15`). |
| **Full Test Suite** | `fullsuite` | `go test ./...` | Complete repository test suite (opt-in via `-fullsuite`). |

---

## CLI Usage

```bash
# Build the gate runner
/usr/local/go/bin/go build -o remaining-gate ./tools/diagnostics/remaining-gate

# Run standard gate (build, vet, unit, regression, race, and bounded crash/resume)
./remaining-gate -workspace /home/hellc/strand

# Run with custom seed and verbose logs
./remaining-gate -seed 1731840000000000000 -v

# Run full gate including 15-iteration stress tests (when concurrent profiling lanes finish)
./remaining-gate -stress15 -iterations 15

# Run full repository test suite as an explicitly labeled stage
./remaining-gate -fullsuite
```

### Flags Reference

- `-go <path>`: Path to Go compiler binary (defaults to `/usr/local/go/bin/go` or `go` on PATH).
- `-workspace <path>`: Root of the Strand repository.
- `-artifacts <path>`: Destination directory for logs and `summary.json` (defaults to `reports/code-review-20261117/followup-agy/evidence/remaining-gate`).
- `-seed <int64>`: Random seed passed to test executions via `SEED=<value>`.
- `-stress15`: Enables heavy 15-iteration TOAST PK moves and re-read kill tests.
- `-iterations <int>`: Number of iterations for the stress stage (default: 15).
- `-load-sec <int>`: Workload duration in seconds for concurrent mover routines.
- `-fullsuite`: Enables full test suite execution (`go test ./...`).
- `-timeout <duration>`: Global execution timeout across all stages (default: 45m).
- `-v`: Live output streaming to stdout.

---

## Artifacts Produced

The gate produces structured output files under `reports/code-review-20261117/followup-agy/evidence/remaining-gate/`:
- `summary.json`: Machine-readable run summary containing timestamp, overall status, total duration, Git tree hash, toolchain/DB versions, seed, and per-stage status.
- `<stage_name>.log`: Complete stdout/stderr trace of each executed stage.
