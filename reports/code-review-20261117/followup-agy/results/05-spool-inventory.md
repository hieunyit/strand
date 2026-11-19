# Task 05: Spool Inventory & Trusted Roots Import for Orphan Spill Sidecars

## Summary
Addressed legacy orphan spill sidecars residing outside standard `<spool-dir>/spill` that cannot be inferred from previous databases or truncated transactions. Implemented an explicit operator-provided trusted spill roots API and offline diagnostic tools (`cmd/strand spool-inspect` and `tools/diagnostics/spool-inventory`).

Key capabilities:
1. **Never guesses filesystem state**: Does not scan arbitrary paths or pretend undiscoverable files are known; requires explicit operator-provided trusted spill roots.
2. **Security & Boundary Enforcement**: Normalizes paths, enforces strict symlink escape detection (`ErrSymlinkEscape`), deduplicates identical and nested/overlapping roots, validates regular files, and prohibits traversal escaping trusted boundaries.
3. **Read-Only Inventory by Default**: Inspection scans and reports discovered sidecars, orphan bytes, and index status without altering Pebble database state or accounting.
4. **Explicit Idempotent Accounting Import**: Updates durable charge index (`spill/path/<base64>`) via explicit apply action without overwriting active/live refcounts (`refs > 0`).
5. **Durable Retention & Quota Integration**: Retains charges until physical deletion by the external owner; accurately reflects imported bytes in `Usage().LocalBytes`, subjecting subsequent operations to local and total hard limits.
6. **Atomic & Resumable Migration**: Supports atomic batch commits with sync; interrupted or resumed imports proceed idempotently without double-charging or index corruption.
7. **Never Deletes Borrowed Files**: Spool lifecycle, truncation, and restart operations strictly preserve borrowed sidecar files on disk.

## Owned Files Modified / Created
- `internal/spool/inventory.go` (NEW): Implemented `CleanAndValidateTrustedRoots`, `ScanInventory`, `ImportInventory`, `ImportTrustedRoots`, `ScanInventoryOffline`, and `ImportInventoryOffline` with symlink escape policy, regular file checks, overlap deduplication, and batch commit.
- `internal/spool/inventory_test.go` (NEW): Added comprehensive test suite verifying outside-root orphans, duplicate roots, symlink escapes, missing/permission errors, reopen persistence, partial failure/resumability, quota limits, non-deletion of borrowed files, live refcount preservation, and offline tools.
- `cmd/strand/spool_inspect.go` (NEW): Integrated `strand spool-inspect` diagnostic command with Cobra via package-level `init()` without modifying `root.go` or touching `seed/prefetch`.
- `cmd/strand/spool_inspect_test.go` (NEW): Unit tests verifying CLI flags, dry-run default, `--apply`, and `--json` outputs.
- `tools/diagnostics/spool-inventory/main.go` (NEW): Standalone offline diagnostic CLI tool supporting `-spool-dir`, `-trusted-root`, `-apply`, `-dry-run`, and `-json`.
- `tools/diagnostics/spool-inventory/main_test.go` (NEW): Automated integration tests building and executing the offline binary.
- `reports/code-review-20261117/followup-agy/results/05-spool-inventory.md` (NEW): This verification report.

## Root Cause Analysis
Previously:
- In `internal/spool/sidecar.go`, database migration (`migrateSidecars`) reconstructed references solely from active transactions in `txn/` and walked the default directory `filepath.Join(s.dir, "spill")`.
- If an older prefetch or pipeline version stored spill sidecars outside `s.dir/spill` (e.g. in custom scratch mounts or separate volumes) and its transactions were subsequently truncated before accounting was enabled, those sidecars became unindexed orphans.
- Because older database state retained no record of truncated spill paths, automated discovery could not infer their locations without recursively scanning the entire filesystem (which is unsafe, slow, and prohibited).
- Spool accounting requires explicit operator-provided boundaries to safely discover, validate, and incorporate retained orphan storage into local and total quota tracking.

## Fix Details

1. **Explicit Operator-Provided Trusted Roots (`CleanAndValidateTrustedRoots`)**:
   - Requires explicit root paths from the operator; never traverses arbitrary or inferred filesystem directories.
   - Cleans and converts paths to absolute canonical paths via `filepath.Abs(filepath.Clean(root))` and `filepath.EvalSymlinks`.
   - Checks that each root exists and is a directory; returns `ErrMissingPath` or `ErrPermission` on failure.
   - Identifies duplicate root entries and emits warnings.
   - Identifies nested/overlapping roots (where root B is a subdirectory of root A), filters redundant sub-walks, and logs warnings.

2. **Symlink Escape Policy & Regular File Checks (`scanRoots`)**:
   - For every entry with `ModeSymlink`, inspects `os.Readlink` destination and `filepath.EvalSymlinks`.
   - If the canonical destination resolves outside the canonical trusted root boundary, rejects the entry and halts with `ErrSymlinkEscape` (or records error if configured non-fatal).
   - Prohibits traversing into symlinked directories to prevent cyclic loops or unexpected directory jumps.
   - For regular file symlinks pointing within the trusted root, resolves canonical paths to ensure deduplication.
   - Enforces regular-file checks (`info.Mode().IsRegular()`), skipping sockets, FIFOs, and devices with descriptive warnings.

3. **Read-Only Inventory Default**:
   - `ScanInventory` and `ScanInventoryOffline` inspect trusted roots against `s.sidecars` (or Pebble `spill/path/` and `spill/ref/` iterators in read-only mode).
   - Compiles an `InventoryReport` detailing discovered files, sizes, existing vs new files, warnings, and errors.
   - Performs zero writes to Pebble and makes zero modifications to accounting memory (`s.sideBytes`).

4. **Explicit Idempotent Accounting Import (`ImportInventory`)**:
   - Updating accounting requires an explicit command/action (`ImportInventory`, `ImportTrustedRoots`, or `--apply`).
   - For each newly discovered file, writes `sidecarKey(normPath) -> sidecarValue(size)` to Pebble with `pebble.Sync`.
   - Idempotently adds to memory: `s.sidecars[normPath] = &sidecar{size: size, refs: 0}` and increments `s.sideBytes += size`.
   - **Preserves Live Refcounts**: If a sidecar already exists in `s.sidecars` with `refs > 0`, its live refcount is strictly preserved and never reset to 0.

5. **Durable Retention & Quota Inclusion**:
   - Imported files are retained in `s.sidecars` and Pebble even with `refs == 0`, ensuring charges persist across spool restarts (`s.Usage().LocalBytes`).
   - Charges are only released when the external file owner physically unlinks the file (`os.Remove`) and references remain 0.
   - `s.sideBytes` is included in `s.Usage().LocalBytes`, enforcing both `WithLocalHardLimit` and `WithTotalHardLimit` on subsequent `Append` calls (`exitcode.DataError`).

6. **Atomic & Resumable Batch Commit**:
   - Processes imports in configurable atomic batches (`WithInventoryBatchSize`, default 1000).
   - If a batch fails or is interrupted prior to `pebble.Sync`, no changes are committed to disk or memory.
   - If resumed or retried, previously committed entries are recognized as already indexed and skipped without double-counting.

7. **Never Deletes Borrowed Files**:
   - Spool never calls unlinking functions on sidecar files during truncate, restart, or refresh. The external caller maintains file ownership.

8. **Diagnostic CLI & Standalone Tooling**:
   - Registered `strand spool-inspect` in `cmd/strand/spool_inspect.go` via `init()` without editing `root.go`.
   - Built standalone offline tool under `tools/diagnostics/spool-inventory` supporting `-spool-dir`, `-trusted-root`, `-apply`, `-dry-run`, and `-json`.

## Executed Checks and Outcomes

### 1. New Spool Inventory Regression Suite (`internal/spool/inventory_test.go`)
Executed: `/usr/local/go/bin/go test -v ./internal/spool -run TestInventory_`
Outcome: `PASS` (0.65s)
- `TestInventory_OutsideRootOrphan`: `PASS` (discovered 2048-byte orphan, verified read-only default, verified 2048 bytes local charge on import)
- `TestInventory_DuplicateRootsAndPath`: `PASS` (deduplicated duplicate roots and nested child roots, zero double-counting)
- `TestInventory_SymlinkEscape`: `PASS` (detected symlink pointing outside trusted root, failed closed with `ErrSymlinkEscape`, zero charge)
- `TestInventory_SymlinkWithinRoot`: `PASS` (allowed symlink to valid target within root, deduplicated canonical target)
- `TestInventory_MissingAndPermissionError`: `PASS` (verified `ErrMissingPath` on missing root and `ErrPermission` on chmod 0000)
- `TestInventory_Reopen`: `PASS` (closed spool, reopened with `spool.New(dbdir)` without roots, verified charge persistence in `s.Usage().LocalBytes`)
- `TestInventory_PartialFailureAndResumable`: `PASS` (simulated batch commit crash, verified rollback, resumed import successfully, confirmed re-import idempotence)
- `TestInventory_QuotaAfterImport`: `PASS` (verified imported orphan bytes exhaust local limit, subsequent append rejected with `exitcode.DataError`)
- `TestInventory_NeverDeletesBorrowedFiles`: `PASS` (appended, truncated, closed, reopened; verified borrowed file remains on disk; released charge only upon physical deletion)
- `TestInventory_PreservesLiveRefcounts`: `PASS` (verified live `refs == 1` is preserved after inventory scan and import)
- `TestInventory_OfflineScanAndImport`: `PASS` (verified read-only `ScanInventoryOffline` and `ImportInventoryOffline`)

### 2. Spool CLI Command Suite (`cmd/strand/spool_inspect_test.go`)
Executed: `/usr/local/go/bin/go test -v ./cmd/strand -run TestSpoolInspectCmd_`
Outcome: `PASS` (0.47s)
- Verified `--spool-dir`, `--trusted-root`, `--dry-run`, `--apply`, and `--json`.
- Verified database remains uncharged after dry-run and charged after `--apply`.

### 3. Standalone Diagnostic Tool Suite (`tools/diagnostics/spool-inventory/main_test.go`)
Executed: `/usr/local/go/bin/go test -v -count=1 ./tools/diagnostics/spool-inventory`
Outcome: `PASS` (6.23s)
- Successfully compiled `spool-inventory` binary.
- Executed offline binary with `-dry-run` and `-apply`, verifying JSON outputs and database persistence.

### 4. Full Internal Spool Test Suite (`internal/spool/...`)
Executed: `/usr/local/go/bin/go test -v ./internal/spool/...`
Outcome: `PASS` (7.44s)
- All 26 unit and integration tests passed, including codec roundtrips, segment overflow, age flush, MinIO container backend, sidecar quotas, and crash safety.

### 5. Formatting
Executed: `/usr/local/go/bin/gofmt -w internal/spool/inventory.go internal/spool/inventory_test.go cmd/strand/spool_inspect.go cmd/strand/spool_inspect_test.go tools/diagnostics/spool-inventory/main.go tools/diagnostics/spool-inventory/main_test.go`
Outcome: Clean formatting, zero errors.

## Remaining Limits
- Unreferenced files residing outside both standard `<spool-dir>/spill` and operator-specified trusted roots remain undiscoverable by design (preventing unconstrained filesystem scanning).
- Symlinks pointing to directories are deliberately not traversed to prevent cyclic reference loops or infinite directory walks.
- Once imported into the durable charge index, orphan sidecar charges are only released when the external process physically removes the file on disk and references are zero.
