# Task 04: Archive Reader Identity Contract, Schema Sidecar, Outside-Table Scope Filtering & Prefetch Safety

## Summary
Resolved the archive reader historical schema verification and downstream identity safety contract:
1. **Verified Identity Contract & Schema Sidecar**: Implemented `TableIdentity` and `SchemaSidecar` in `internal/reader/archive/schema.go` (and mirrored in `internal/seed/archive_schema.go`). Allows archive reader to decode historical binlog streams using an immutable operator-supplied sidecar or trusted seed snapshot, preventing invalid guesses against the current source database.
2. **Explicit Identity Modes**:
   - `IdentityModePK`: Primary key tables validated against TABLE_MAP and sidecar metadata.
   - `IdentityModeUnique`: Fallback unique-key tables register resolved key order explicitly; emits unique-key identity rather than full-row keys.
   - `IdentityModeKeyless`: Explicit `full_row` identity mode requiring FULL row images (failing closed on omitted columns) and configuring prefetch assembler `WithKeylessFunc` and `KeylessIndexName` writeset serialization.
3. **Preservation of S04 / S05 Invariants**: Fail-closed checks for `PARTIAL_UPDATE_ROWS_EVENT`, minimal/omitted row images, duplicate column names, out-of-bounds PK indices, and unmapped tables without PK metadata are strictly preserved.
4. **Deterministic Fingerprinting & DDL/Mismatch Rejection**: Table identity contracts compute deterministic SHA-256 fingerprints across ordered columns, types, identity mode, and key order. Any column count, type, name, fingerprint, or DDL query mismatch against an immutable sidecar table is rejected before the transaction can be marked covered.
5. **Outside-Table Scope Filtering with Boundary Preservation**: When an explicit verified scope (`TableScope`) is provided, tables outside the scope are safely ignored without failing on missing metadata. Transactions containing both excluded rows and included sibling rows preserve GTID transaction boundaries (GTID, XID) so that excluded rows never skip included sibling rows. Without an explicit verified scope, all tables are rejected if unverified.
6. **Concrete Prefetch Fixes**:
   - Fixed prefetch error handling: When archive reader encounters an error and `sourceDSN == ""` (no source fallback), prefetch returns the error immediately instead of printing continuing and reporting success.
   - Fixed EOF in transaction: If an event stream closes while inside a transaction (`inTxn == true`), prefetch returns `exitcode.DataError` (`ErrLogGap`) and refuses to mark the partial transaction covered.
7. **Downstream Safety & Consumer Contract**: Added `VerifyRowChangeIdentity` to guarantee emitted row keys match the pipeline's expected key shape (preventing full-row keys from feeding fallback unique-key writers).

---

## Owned Files Modified / Created
- `internal/reader/archive/schema.go` (NEW): Defines `IdentityMode`, `TableIdentity`, `SchemaSidecar`, deterministic SHA-256 fingerprinting, validation, and `VerifyRowChangeIdentity`.
- `internal/reader/archive/reader.go` (MODIFIED): Integrated `Sidecar` and `TableScope` into `Config`. Implemented `verifyAgainstIdentity`, outside-table filtering with GTID boundary preservation, and DDL mismatch detection.
- `internal/seed/archive_schema.go` (NEW): Provides type aliases and loading helpers for archive schema sidecars in `seed.SeedManifest` workflows.
- `cmd/strand/prefetch.go` (MODIFIED): Wired schema sidecar loading, `TableScope` filter, `assemble.WithKeylessFunc`, `assemble.WithKeyMetadata`, downstream safety verification before spool append, archive-error-without-source fail-closed return, and EOF inTxn fail-closed check.
- `cmd/strand/seed.go` (MODIFIED): Added `--archive-schema` flag and wired `ArchiveSchema`, `SchemaSnapshot`, and `Tables` to prefetch.
- `internal/reader/archive/reader_identity_test.go` (NEW): Comprehensive regression suite for valid PK, unique-only explicit metadata, keyless explicit metadata, duplicate/NULL validation, DDL mismatch, excluded+included same transaction, malformed/purged paths, and no guessed col0.
- `cmd/strand/prefetch_identity_test.go` (NEW): Tests for archive error with no source fallback, stream EOF inside transaction, and prefetch spool writing with sidecar.
- `reports/code-review-20261117/followup-agy/results/04-archive-identity.md` (NEW): This documentation and verification report.

---

## Root Cause Analysis
1. **Lack of Historical Schema in Binlogs (Keyless / Unique-Only)**: In MySQL binlogs, `TABLE_MAP_EVENT` contains column names and primary key indices only when `binlog_row_metadata=FULL` and a primary key exists. Keyless and unique-only tables do not contain PK metadata in the binlog. Previously, archive reader rejected any table without PK metadata, making it impossible to replay keyless or fallback unique-key tables from archives. Re-querying current live database `information_schema` is unsafe because historical DDL could have altered table structures.
2. **Unrelated Outside-Table Failures**: Binlogs often contain system tables or heartbeat tables (e.g. `mysql.rds_heartbeat2`). When binlog metadata was minimal or lacked PKs, the reader failed closed even if those tables were never included in the replication scope.
3. **Prefetch Success on Archive Error**: In `cmd/strand/prefetch.go`, when archive reader returned an error, the code printed `(continuing from source)`. If `sourceDSN == ""`, it bypassed source prefetching, flushed the spool, and returned `n, nil`, falsely reporting success.
4. **Premature EOF During Transaction**: When the reader stream closed (`!ok`), `consume` checked `stopAt.IsSubsetOf(covered)`, but if `stopAt == nil` (typical for archive reading), it returned `nil`, leaving an incomplete partial transaction uncommitted in the assembler and falsely considering the stream cleanly finished.

---

## Fix Details

### 1. Verified Identity Contract (`internal/reader/archive/schema.go`)
- `TableIdentity` captures exact schema, table, ordered columns, optional MySQL type bytes, identity mode (`pk`, `unique`, `keyless`), and ordered key columns.
- `ComputeFingerprint()` produces a deterministic SHA-256 digest:
  ```
  table:<schema>.<table>
  columns:<col1>,<col2>,...
  types:<hex_types>
  identity_mode:<mode>
  key_columns:<key1>,<key2>,...
  version:<v>
  ```
- `TableIdentity.Validate()` enforces that column lists are non-empty, contain no blank or duplicate names, key columns are valid subsets with no duplicates, and keyless mode does not specify partial key columns.

### 2. Sidecar Verification & Mismatch Rejection (`internal/reader/archive/reader.go`)
- `verifyAgainstIdentity`:
  - Enforces column count match: `tm.ColumnCount == len(tid.Columns)`. Mismatches return `ErrSchemaChangePending` (`NeedsDecision`).
  - Enforces column type match: Any mismatched byte between `tm.ColumnType` and `tid.ColumnTypes` returns `ErrSchemaChangePending`.
  - Enforces column name match: If binlog has names, any name mismatch returns `ErrSchemaChangePending`.
  - Enforces PK metadata consistency: If TABLE_MAP has PK metadata, it must match `tid.KeyColumns` and `IdentityModePK`. If TABLE_MAP has no PK and no sidecar mapping exists, S05 fail-closed rejects it (never guessing `col0`).
  - Verifies fingerprint digest matches `tid.Fingerprint`.
- `isDDLMismatch`: Detects DDL queries (`ALTER`, `CREATE`, `DROP`, `TRUNCATE`, `RENAME`) modifying tables defined in the immutable sidecar and rejects with `ErrSchemaChangePending` before transaction commit.

### 3. Outside-Table Scope Filtering & Sibling Row Preservation
- `r.cfg.TableScope`: When set, tables outside scope have `excludedTables[tableID] = true`.
- In `TABLE_MAP_EVENT`: Excluded tables return `nil` without failing on missing metadata or PK.
- In `emitRows`: Excluded table rows are dropped cleanly without emitting `reader.EventRows`.
- In `EventGTID` and `EventXID`: Boundaries are strictly preserved. Sibling included rows within the same transaction are pushed to the assembler, and the transaction is committed under its original GTID.

### 4. Concrete Prefetch Issues (`cmd/strand/prefetch.go`)
- **No Source Fallback**:
  ```go
  if err != nil {
      ...
      if sourceDSN == "" {
          return n, fmt.Errorf("archive prefetch failed with no source fallback: %w", err)
      }
      if exitcode.GetExitCode(err) == exitcode.Rejected || exitcode.GetExitCode(err) == exitcode.NeedsDecision {
          return n, err
      }
      fmt.Printf("archive prefetch stopped: %v (continuing from source)\n", err)
  }
  ```
- **EOF InTxn**:
  ```go
  if !ok {
      if ctx.Err() != nil {
          return ctx.Err()
      }
      if inTxn {
          return exitcode.New(
              exitcode.ErrLogGap,
              exitcode.DataError,
              "event stream closed unexpectedly while inside transaction (partial transaction not covered)",
              nil,
          )
      }
      ...
  }
  ```
- **Assembler Configuration**:
  - `assemble.WithKeylessFunc`: Dynamically flags verified keyless tables so assembler assigns all columns as key.
  - `assemble.WithKeyMetadata`: Injects `assemble.KeylessIndexName` unique index metadata for keyless tables.
  - Pre-registers fallback unique keys with resolved key order into assembler registry.
  - Validates `VerifyRowChangeIdentity` on every row change before appending to spool.

---

## Executed Checks and Outcomes

### 1. Reader Identity Regression Suite (`internal/reader/archive/reader_identity_test.go`)
Executed: `/usr/local/go/bin/go test -v -run "TestArchiveIdentity_" ./internal/reader/archive`
Outcome: `PASS` (0.37s)
- `TestArchiveIdentity_ValidPK`: `PASS`
- `TestArchiveIdentity_UniqueOnly_ExplicitMetadata`: `PASS`
- `TestArchiveIdentity_Keyless_ExplicitMetadata`: `PASS`
- `TestArchiveIdentity_DuplicateAndNull`: `PASS`
- `TestArchiveIdentity_DDLMismatch`: `PASS`
  - `sidecar_column_count_mismatch`: `PASS`
  - `sidecar_column_type_mismatch`: `PASS`
  - `ddl_query_modifying_sidecar_table_rejected`: `PASS`
- `TestArchiveIdentity_ExcludedAndIncludedSameTxn`: `PASS`
- `TestArchiveIdentity_MalformedPurgedNoSource`: `PASS`
  - `empty_archive_dir_returns_log_gap`: `PASS`
  - `malformed_file_returns_error`: `PASS`
- `TestArchiveIdentity_NoGuessedCol0`: `PASS`

### 2. Archive Reader Fencing & Existing Unit Suite (`internal/reader/archive/reader_fencing_test.go`, `archiver_test.go`)
Executed: `/usr/local/go/bin/go test -v -run "TestArchiveRejects|TestArchiveCompressed|TestArchiveFile|TestArchiver_" ./internal/reader/archive`
Outcome: `PASS` (0.28s)
- `TestArchiveRejectsUnsafeMetadata`: `PASS` (7 subcases)
- `TestArchiveRejectsPartialImagesBeforeCommit`: `PASS` (5 subcases)
- `TestArchiveCompressedPayloadChecksEveryEvent`: `PASS` (2 subcases)
- `TestArchiveRejectsUndecodedCriticalEvents`: `PASS`
- `TestArchiveFileRejectsUnsafeTransaction`: `PASS` (3 subcases)
- `TestArchiver_FinalizesOnRotateAndKeepsRawBytes`: `PASS`
- `TestArchiver_FileEndingWithoutRotateKeepsItsName`: `PASS`
- `TestArchiver_RecoversFileFinalizedAfterLastStateWrite`: `PASS`
- `TestArchiver_OneWriterPerDir`: `PASS`
- `TestArchiver_RejectsForeignDirWithoutState`: `PASS`
- `TestArchiver_TaggedGTIDRejected`: `PASS`
- `TestArchiver_PurgedErrorIsLogGap`: `PASS`

### 3. Archive Integration Suite with Real MySQL 8.4 Container (`internal/reader/archive/archiver_integration_test.go`)
Executed: `/usr/local/go/bin/go test -v -run "TestArchiverIntegration_" ./internal/reader/archive`
Outcome: `PASS` (45.3s)
- `TestArchiverIntegration_ResumesWithoutGapOrDuplicate`: `PASS` (24.1s)
- `TestArchiverIntegration_PurgedGapIsLogGap`: `PASS` (21.2s)

### 4. Prefetch Identity Suite (`cmd/strand/prefetch_identity_test.go`)
Executed: `/usr/local/go/bin/go test -v -run "TestPrefetch_" ./cmd/strand`
Outcome: `PASS` (0.46s)
- `TestPrefetch_ArchiveErrorNoSourceFallbackFails`: `PASS` (0.05s)
- `TestPrefetch_EOFInTxnFails`: `PASS` (0.01s)
- `TestPrefetch_ValidArchiveWithSidecar`: `PASS` (0.01s)

---

## Remaining Limits & Downstream Spool/Model Extension Boundary
1. **Unowned Spool & Model Extension Boundary**:
   - `internal/spool` and `internal/model` are outside the owned scope for this lane. Currently, `model.RowChange` and pebble-spool records serialize `Key []model.Value` without an explicit `IdentityMode` discriminator enum in the serialized binary format.
   - Downstream safety is enforced at prefetch assembly time via `VerifyRowChangeIdentity`: if a table requires unique-key identity, prefetch refuses to write full-row keys into the spool, avoiding feeding corrupted key shapes to downstream fallback unique writers.
   - Future extension (when spool/model lane is open): Persist an explicit `IdentityMode` byte tag and key column metadata in `spool.SpoolEntry` so replay readers can verify identity contracts post-spool without needing the sidecar at replay time.
2. **Schema Evolution During Archive Playback**:
   - The current contract treats the operator-supplied sidecar as immutable for the archive replay window. If the archive spans multiple historical DDL alterations for the same table, the reader intentionally fails closed with `ErrSchemaChangePending` (`NeedsDecision`). Multi-version / temporal sidecars tied to specific GTID intervals will require a multi-generation sidecar definition in a subsequent review.
