# Task 01: Web Config Scalar-Based Masking & Unmasking

## Summary
Replaced line-based secret restoration (`yamlLinePaths` and line-grouped `maskTokRE` substitution) with semantic scalar mapping based on AST logical path (`/%d:key` mapping path + `[%d]` sequence index identity) and exact byte spans.

This resolves failures when:
1. Multiple logical paths share one line (e.g., compact JSON, flow YAML).
2. Fields are reordered on a single line or across lines.
3. One field is edited with a real secret while another field on the same line retains a masked placeholder.
4. Block YAML is converted to flow/JSON or vice versa (line split/join).
5. Secrets containing delimiters (e.g. commas or quotes) are moved into flow-style mappings.
6. Embedded DSNs (PostgreSQL URLs, MySQL DSNs, and quoted/escaped keyword DSNs) are edited or relocated.

## Owned Files Modified / Created
- `internal/web/masktext.go`: Implemented scalar-based secret collection, byte-span masking, and AST path-driven unmasking with semantic validation and context-aware quoting.
- `internal/web/masktext_test.go`: Updated `TestUnmaskReorderedCompactJSON` to verify that reordered compact JSON restores each field to its corresponding secret rather than failing closed on line grouping.
- `internal/web/maskscalar_regression_test.go` (NEW): Added comprehensive regression suite covering all 8 review criteria.

## Root Cause Analysis
Previously, `UnmaskConfigText` called `yamlLinePaths`, which mapped line numbers (`n.Line`) to concatenated paths (`path + "\x00"`). When multiple secrets were on the same line:
- In compact JSON (`{"source":{"password":"..."},"target":{"password":"..."}}`), all keys on line 1 were merged into one key path string.
- If fields were reordered (`target` before `source`), the path string didn't match the original, or text substitution restored secrets in appearance order rather than by field identity.
- If a user edited one secret to a real value while leaving the other as `********`, `len(maskTokRE) != len(secrets)` (1 != 2), causing an ambiguous value rejection.
- Embedded DSNs were scanned line-by-line across arbitrary text, risking interference with comments or non-scalar tokens.

## Fix Details
1. **Scalar Path Identity**:
   - Every YAML scalar is identified by its full logical path in the AST: mapping keys are length-prefixed (`/%d:%s`), and sequences use index brackets (`[%d]`).
   - Line numbers and line ordering are never used for matching secret identity.
2. **Exact Byte-Span Replacements**:
   - `scalarStart` and `scalarEnd` compute exact byte offsets `[off, end)` in the source text.
   - For keyed secrets, the placeholder replacement is applied strictly to `[off, end)` of that scalar.
   - For embedded DSNs, embedded secrets are scanned within the scalar's byte span, recording `ordinal` (0, 1, ...). Placeholders in the edited scalar bind strictly to the matching ordinal in the original scalar.
3. **Syntax-Aware Safe Quoting & Encoding**:
   - When a secret is restored into JSON (`isJSONText`), it is safely double-quoted with standard JSON escaping (`safeDoubleQuote`).
   - When an unquoted plain secret containing commas or special characters is moved into flow YAML, `formatSecretReplacement` safely double-quotes the scalar to prevent syntax errors or key truncation.
   - Quoted secrets in block or flow YAML preserve their original quoting format.
4. **Preservation of Deliberate Edits**:
   - If an edited scalar's value does not match `maskStr` (`"********"`), it is left untouched, preserving the user's deliberately entered plaintext credential.
5. **Fail-Closed Security Invariants**:
   - Unmatched placeholders (paths not present in the original configuration) fail closed.
   - Embedded DSN placeholder counts differing from the original scalar count fail closed (rejecting ambiguous count changes).
   - YAML aliases on secret keys fail closed in both masking and unmasking.
   - Multi-line block scalars on secret keys fail closed.
   - The final restored document is unmarshaled and verified to ensure all secrets match expected semantic values and no unmasked `maskStr` remains in any secret scalar or DSN.
6. **Comments Untouched**:
   - Comments are not YAML scalar nodes and are never scanned for secrets or placeholders. Arbitrary asterisks in comments are preserved without interference.

## Executed Checks and Outcomes

### 1. Regression Test Suite (`internal/web/maskscalar_regression_test.go`)
Executed: `/usr/local/go/bin/go test -v ./internal/web -run TestRegression_`
Outcome: `PASS` (0.41s)
- `TestRegression_CompactReorderSourceTarget`: `PASS`
- `TestRegression_OneRealEditedOnePlaceholderSameLine`: `PASS`
- `TestRegression_LineSplitJoin`: `PASS`
- `TestRegression_SequenceIndexes`: `PASS`
- `TestRegression_EmbeddedDSNs`: `PASS`
- `TestRegression_PlainSecretWithCommaOrQuotesMovedIntoFlow`: `PASS`
- `TestRegression_UnmatchedPlaceholderAndAliasFailClosed`: `PASS`
- `TestRegression_StarsInCommentsPreserved`: `PASS`

### 2. Selected Web Mask Tests (`internal/web/masktext_test.go` + `internal/web/mask_test.go`)
Executed: `/usr/local/go/bin/go test -v ./internal/web -run "TestMask|TestUnmask"`
Outcome: `PASS` (0.34s)
- `TestMaskUnmaskConfigTextRoundTrip`: `PASS`
- `TestMaskConfigTextBlockScalarSecretRejected`: `PASS`
- `TestMaskConfigTextCompactJSONAndFlowYAML`: `PASS`
- `TestUnmaskRejectsCompactUnmatchedMaskedPassword`: `PASS`
- `TestMaskConfigTextSecretAliasRejected`: `PASS`
- `TestMaskUnmaskWhitespaceOnlyPassword`: `PASS`
- `TestUnmaskReorderedCompactJSON`: `PASS`
- `TestMaskConfigYAMLAndSecrets`: `PASS`

### 3. Full Web Package Test Run
Executed: `/usr/local/go/bin/go test ./internal/web`
Outcome: `PASS` (ok strand/internal/web 22.34s)

### 4. Code Formatting
Executed: `/usr/local/go/bin/gofmt -w internal/web/masktext.go internal/web/masktext_test.go internal/web/maskscalar_regression_test.go`
Outcome: Clean formatting, zero errors.

## Remaining Limits
- Multi-line block scalars (`|` and `>`) for secret keys remain explicitly rejected as unsafe for line-span replacement (consistent with existing spec).
- YAML anchors/aliases for secrets remain explicitly rejected to prevent secret disclosure via shared anchor references.
- In multi-secret embedded DSN scalars (e.g. two passwords in one connection string), editing one embedded password while leaving the other as a placeholder is rejected as ambiguous; full scalar restoration requires matching placeholder counts or typing all real credentials.
