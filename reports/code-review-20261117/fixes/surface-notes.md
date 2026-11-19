# S06–S10 surface fixes

Implemented scoped fixes in `internal/web/mask.go`, `internal/web/masktext.go`, `internal/config/env.go`, and `internal/governor/signals.go`.

- S06/S08: config text masking now uses YAML node spans to replace secret scalars while preserving source formatting, including compact JSON and flow YAML. Unmask restoration associates secrets by YAML path and rejects any unmatched placeholder that remains in a secret field or embedded DSN.
- S07: keyword DSN scanning handles quoted values, backslash escapes, and spaces around `=`; malformed unterminated quoted values are masked through end of input.
- S09: environment string values retain leading/trailing whitespace; duration, boolean, and numeric values are trimmed before parsing.
- S10: MySQL signal query, scan, iteration, malformed counter, and missing-counter failures propagate. Replica status falls back only for MySQL syntax-not-supported errors; an empty result remains valid on a primary without replicas.

Regression coverage added/extended in `internal/web/masktext_test.go`, `internal/web/web_test.go`, `internal/config/env_internal_test.go`, and new `internal/governor/signals_mysql_errors_test.go`.

Checks passed:

- `/usr/local/go/bin/go test ./internal/config`
- `/usr/local/go/bin/go test ./internal/governor -count=1` (includes its isolated MySQL testcontainer)
- `/usr/local/go/bin/go test ./internal/typemap`
- `/usr/local/go/bin/go test ./internal/web -run 'TestMask(Unmask|Config|Secrets)|TestUnmask' -count=1`
- `gofmt` and `git diff --check` on owned files

Web package-wide tests were not run because the package has PostgreSQL testcontainer paths in `storeKinds`/`dbstore_test.go`; masking tests were selected directly. No real PostgreSQL DSN/database test was added for quoted keyword DSNs. A MySQL syntax error number 1064 is the recognized unsupported-statement signal; other query errors are surfaced.

The requested agy Claude run exhausted quota (429). A follow-up Gemini run stalled without changes and was interrupted; all final fixes and checks above were completed locally.

Follow-up review hardening: secret-key YAML aliases now fail closed because their anchored source scalar could otherwise remain visible elsewhere. Added a regression test proving reordered compact JSON placeholders fail closed (line path aggregation does not restore after same-line key reordering; this is a documented availability limitation, while avoiding secret misassignment). Focused follow-up web tests for alias rejection, reordered compact JSON, and compact masking passed.

Whitespace-only password follow-up: `MaskConfigText` and `MaskConfigYAML` now mask every nonempty secret scalar, including values containing only spaces. The `TestMaskUnmaskWhitespaceOnlyPassword` regression verifies masking and exact round-trip restoration. Its focused web test and `git diff --check` passed.
