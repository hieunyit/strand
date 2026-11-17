# Surface/storage repairs B06–B09

Files owned/changed:
- internal/web/masktext.go
- internal/web/masktext_surface_regression_test.go (new)
- internal/spool/inventory.go
- internal/spool/sidecar.go
- internal/spool/surface_regression_test.go (new)

No edits to mask.go, pebble.go, codec.go, command code, or other owners' files. No commits, pushes, or spawned agents.

## B06: multiline DSN exposure

Secret discovery scans decoded YAML scalar values before attempting source-span location. Secret-bearing folded, literal, multiline quoted, and multiline plain DSNs return an error and empty output when a safe single-line replacement cannot be located. Encoded single-line credentials (including Unicode YAML escapes) are masked using decoded DSN components. Ordinary nonsecret multiline scalars remain unchanged.

## B07: quote conversion changes credentials

Embedded secret components are collected from decoded YAML values, restored into the edited decoded DSN, and encoded for the edited YAML/JSON scalar context. The obsolete scanner for raw YAML-escaped keyword passwords was removed; decoded values use the existing DSN scanner. Result validation compares each restored full scalar's decoded value against its expected value by logical path, in addition to rejecting remaining placeholders. Regressions cover double/single/JSON/plain conversions with backslashes and quoted/escaped keyword passwords, while preserving edited host fields.

## B08: stale inventory overwrites live sidecar size

Import revalidates trusted roots, canonical paths, current file sizes, and the current sidecar index while holding the spool mutex. Report sizes/refcounts no longer authorize writes. A current matching entry is skipped; changed live files are rejected; changed unreferenced files use fresh stat sizes. Entries are deduplicated and path replacements via symlinks are rejected. The original scan(100B) -> grow(200B) -> Append(refs=1) -> import(stale report) sequence keeps 200B/refs=1 and reopens correctly.

## B09: post-migration crash orphans evade quota

The sidecar schema version gates only durable-reference migration. Every startup subsequently reconciles unknown regular files under the assembler's standard <spool>/spill tree, including nested directories. Unknown path charges are committed in one synchronous Pebble batch and then published in memory; existing reference counts and sizes are retained. File/directory symlinks are not followed, and a symlink replacing the spill root fails startup. No borrowed or retained files are deleted. External orphan roots still require explicit inventory.

## Validation

Passed with /usr/local/go/bin/go:
- `go test ./internal/web ./internal/spool -count=1` — web 22.063s; spool 7.926s; includes new package regressions.
- `go test -race ./internal/web ./internal/spool -run 'TestMask|TestUnmask|TestRegression|TestInventory|TestSidecar' -count=1` — web 1.880s; spool 2.548s.
- `go test -overlay=/tmp/strand-branch-surface/web-overlay.json ./internal/web -run '^TestSurface' -count=1 -v` — both original safety repros pass.
- `go test -overlay=/tmp/strand-branch-surface/spool-overlay.json ./internal/spool -run '^TestSurface' -count=1 -v` — both original safety repros pass; observed live indexed size 200/refs 1 and startup local usage 4096.
- Final race rerun of all six new regression test functions after scanner cleanup — web 1.929s; spool 2.012s; passes.
- After removing obsolete raw scanner, `go test ./internal/web -run 'TestMask|TestUnmask|TestRegression' -count=1` — passes.

Limits: multiline secret-bearing scalars deliberately fail closed rather than offering a format-preserving editor roundtrip. Embedded DSN scalar quote style and surrounding document layout are retained where safe, but YAML escapes inside that scalar may be normalized. Inventory mutex protects spool mutations, not arbitrary concurrent external filesystem writers; startup reconciliation likewise assumes the standard spill directory is not concurrently modified before the spool is opened. No live database/browser/full repository suite was run by this agent.
