# Merge hardening review — 5273009..ab5160d..e3d19bb

Reviewed promote SQL/autoincrement, Merkle resume/keyless verification, worker/checkpoint fencing, governor signal changes, Go dependencies, strict PG diagnostic measurements, and crash/gate claims. Excluded other reviewers’ data, prefetch, web, spool, and profiling implementation scope. Initial pending report edits were preserved.

## Confirmed findings, fixed in working tree

1. **P1: Merkle resume silently ignores failed catch-up** (`internal/verify/merkle.go`, original lines 714–718). With a matching cached chunk, ApplyWaiter.Mark/Wait failure or cancellation was discarded, verification returned a clean report, and deleted progress. New `TestMerkleResumeRequiresSuccessfulCatchUp` reproduces all three failures, plus successful resume. Fix propagates errors with table context and preserves progress on failure.
2. **P2: Promote quoting changes raw catalog identifiers** (`internal/promote/mysql_driver.go`, original lines 198–216). `TrimSpace`/`Trim(...,"`")` change a column such as ` id`/a literal boundary backtick, and change the table/schema selected by MAX/ALTER. New ApplyAutoIncrement and FindColumnMax/Setval regressions assert emitted SQL preserves raw catalog components. Fix escapes each raw identifier without stripping bytes, and splits qualified raw schema.table at the first separator. Production callers use catalog values/SequenceInfo, with no SQL-prequoted contract; helper-only prequoted assumptions were corrected.
3. **P2: PG strict diagnostic loses counters at scientific notation** (`tools/diagnostics/pg-strict/main.go`, original scrapeMetrics). Prometheus expfmt emits floats with format `g`; at one million applied transactions, e.g. `1.234567e+06`, ParseInt returned zero, corrupting throughput/rows-per-transaction results. New HTTP scrape regression reproduces transactions=0/rows=0. Fix accepts scientific notation while rejecting fractional, negative, nonfinite, and out-of-int64 values; exact integer MaxInt64 remains supported.

All fixes were explicitly authorized by root after initial read-only findings. No commit was created.

## Evidence

- Before-fix overlay reproductions (no source rollback): `/tmp/strand-hardening-before.log` (promote + resume failures); `/tmp/strand-hardening-pg-before.log` (scientific counter failure).
- Focused fixed regressions passed for promote, verify, and pg-strict. Final focused race run passed all three packages: `/tmp/strand-hardening-after.log`.
- Earlier focused existing checks passed: quote/autoincrement, stable keyless verify, stale worker creation, idle compactor epoch, and MySQL signal error propagation.
- Changed-scope `git diff --check` passed. No existing containers were modified and no destructive DB fixtures were run.

## Other assessment and limits

- Worker initialization takes pipeline shared lock before worker insertion in one transaction, matching takeover lock order. Idle compactor now checks checkpoint epoch. No additional supported-path fencing regression found. Considered FOR SHARE compatibility, then ruled it out: documented MySQL5.x support is SOURCE-only; fence queries execute against MySQL8.x TARGET. Fence code unchanged.
- go.mod changes mark existing direct imports accurately; no dependency versions changed. go.sum tidy updates are consistent with dependency/test resolution. No dependency blocker identified.
- Strict PG report now correctly calls rows/source-txn a source-transaction metric, avoids an unsupported raw-commit ceiling claim, and limits root-cause conclusions to local hypotheses.
- Strengthened bounded crash scenario requires the exact durable app.items bounds plan, unfinished saved work, checkpoint, and table-specific resume evidence. Recorded bounded run killed with pending ranges and zero reread keys; it does not establish active-reread kill, 15-iteration stress, full-suite, real gh-ost, or user-host performance success. Current repaired STATUS/tools notes disclose these limits. No new heavy e2e was rerun in this lane.

No remaining blocker found in this assigned scope after the three working-tree fixes, subject to root integrated validation.
