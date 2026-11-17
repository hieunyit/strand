# Task Report: 02-target-constraints — Source-vs-Target Constraint Compatibility Verification

## 1. Summary of Changes & Architecture

### Problem Context
The prior `CheckTargetKey` implementation operated under two significant limitations:
1. It received only a single logical key and explicitly bypassed validation whenever the table had a primary key (`if key.IsPrimary() { return nil }`).
2. It lacked access to the source database handle (`*sql.DB`), preventing it from inspecting source unique metadata to prove whether additional target unique constraints were safely mirrored or implied by the source.

Consequently, while strict-subset competing keys for fallback row keys were caught, target-only unique constraints (e.g., target table having an unmirrored `UNIQUE(email)` alongside `PRIMARY(id)`) were skipped, posing silent merge data loss risks on MySQL targets (`ON DUPLICATE KEY UPDATE`) and replication abort errors on PostgreSQL targets (`ON CONFLICT`).

### Implementation Details
We implemented a comprehensive, higher-level constraint compatibility verification system without speculative SQL rewriting or dropping constraints:

1. **`internal/validate/constraint_compat.go` (NEW)**:
   - **`InspectTableConstraints(ctx, db, engine, table)`**: Derives complete constraint metadata:
     - Primary key and candidate unique keys (`PRIMARY KEY`, `UNIQUE KEY`, `UNIQUE INDEX`).
     - Detailed column descriptors: nullability (`Nullable`), collation (`Collation`), data type (`DataType`), and prefix lengths (`SubPart`).
     - Index descriptors: partial indexes (`IsPartial`), functional/expression indexes (`HasExpr`), prefix indexes (`HasPrefix`), and PostgreSQL 15+ `NullsNotDistinct`.
     - Supports MySQL catalog (`information_schema.TABLES`, `information_schema.COLUMNS`, `information_schema.STATISTICS`) and PostgreSQL catalogs (`pg_class`, `pg_index`, `pg_attribute`, `information_schema`).
   - **Rename Contract Mapping**:
     - `ConstraintCheckOptions.TableRename`: Maps source `schema.table` to target `schema.table`.
     - `ConstraintCheckOptions.ColumnRename`: Maps source column names to target column names under rename contracts.
   - **Constraint Implication Theorem ($S \subseteq T$)**:
     - A target unique constraint on columns $T$ is acceptable if there exists at least one source constraint on columns $S$ such that the mapped source columns are a subset of the target unique columns ($S \subseteq T$).
     - If $S = T$ (mirrored constraint, e.g., `PRIMARY(id)` + `UNIQUE(email)`): Accepted when descriptors match.
     - If $S \subsetneq T$ (target superset, e.g., source `PRIMARY(id)` and target `UNIQUE(id, tenant_id)`): Accepted because uniqueness on $S$ guarantees uniqueness on $T$.
     - If $T \subsetneq S$ (target strict subset, e.g., source composite key `(tenant_id, id)` and target `UNIQUE(id)`): Explicitly REJECTED.
   - **Semantics & Descriptors Verification**:
     - **Nullability**: If a source column allows `NULL`, it cannot imply a target superset constraint (since duplicate `NULL`s on source could violate the superset on target). For mirrored constraints, if target requires `NOT NULL` or enforces `NULLS NOT DISTINCT` while source allows `NULL`, it is rejected.
     - **Functional / Expression Descriptors**: If target has a functional index (e.g. `lower(email)`), equivalence is reported as inconclusive/incompatible unless identical mirrored descriptors are present on source, with explicit remediation suggested.
     - **Prefix Descriptors (`SUB_PART`)**: Target prefix unique indexes (e.g. `email(10)`) are rejected because prefix constraints cannot guarantee full value uniqueness. Source prefix indexes likewise cannot imply full target uniqueness.
     - **Collation Verification**: If target column collation is looser than source (case-insensitive vs binary/case-sensitive, accent-insensitive, or PAD SPACE vs NO PAD), it is rejected with code `collation_looser` (`exitcode.ErrCollationLooser`).
   - **Engine-Specific Error Reporting with Actionable Remediation**:
     - **MySQL target**: Identifies silent row merge / overwrite risk (`MySQL ON DUPLICATE KEY UPDATE will match this constraint and silently overwrite distinct source rows`).
     - **PostgreSQL target**: Identifies replication abort risk (`PostgreSQL ON CONFLICT does not arbitrate on this constraint and will abort replication with duplicate key violation error`).
     - Every failure provides explicit remediation guidance (e.g. dropping the conflicting target constraint, adding the constraint to source, or aligning column collations).
   - **Missing Target Table Handling**:
     - If target table does not exist in target database (`tableExists == false`), returns `nil` so preflight checks pass before `strand schema init` creates target tables.

2. **`internal/validate/rowkey.go` (OWNED)**:
   - Wired `CheckConstraintCompatibility` into `CheckKeys` whenever both source and target DB handles are connected (`src != nil && dst != nil`).
   - Retained the existing `CheckTargetKey` execution and fallback exact identity rule for backwards compatibility.
   - Exported `CheckRowKeys` as a direct validation entry point for callers.

3. **`internal/validate/constraint_compat_test.go` (NEW)**:
   - Implemented mock SQL drivers to test all constraint matrix permutations without network dependencies.
   - Implemented an isolated live MySQL 8.4 container test (`TestIsolatedLiveMySQLCheck`) using testcontainers.

---

## 2. Regression Tests & Verification Outcomes

### Regression Test Suite (`internal/validate/constraint_compat_test.go`)
Executed using `/usr/local/go/bin/go test -v ./internal/validate -run TestRegression`:

| Test Name | Scenario Checked | Outcome |
| :--- | :--- | :--- |
| `TestRegression_PrimaryIDAndMirroredEmailAccepted` | Source `PRIMARY(id)` + `UNIQUE(email)` mirrored on target MySQL & PG | **PASS** |
| `TestRegression_TargetOnlyEmailRejected/Target_MySQL_Silent_Merge` | Target-only `UNIQUE(email)` rejected with MySQL ON DUPLICATE silent overwrite hazard & remediation | **PASS** |
| `TestRegression_TargetOnlyEmailRejected/Target_Postgres_OnConflict_Failure` | Target-only `UNIQUE(email)` rejected with PG ON CONFLICT failure & remediation | **PASS** |
| `TestRegression_NullableUnique/Source_Nullable_Target_NotNull_Rejected` | Source nullable column with target `NOT NULL` rejected | **PASS** |
| `TestRegression_NullableUnique/Source_Nullable_Subset_Cannot_Imply_Superset` | Nullable subset `UNIQUE(a)` cannot imply superset `UNIQUE(a, b)` | **PASS** |
| `TestRegression_NullableUnique/Mirrored_Nullable_Accepted` | Mirrored nullable `UNIQUE(email)` with matching nullability accepted | **PASS** |
| `TestRegression_NullableUnique/Target_NullsNotDistinct_Rejected` | Target `NULLS NOT DISTINCT` rejected when source permits multiple nulls | **PASS** |
| `TestRegression_StrictSubsetVsSuperset/Superset_Accepted` | Target superset `UNIQUE(id, tenant_id)` implied by source `PRIMARY(id)` accepted | **PASS** |
| `TestRegression_StrictSubsetVsSuperset/Strict_Subset_Rejected` | Target strict subset `UNIQUE(id)` of composite `PRIMARY(tenant_id, id)` rejected | **PASS** |
| `TestRegression_FunctionalAndPrefixDescriptors/Target_Functional_Index_Rejected` | Target functional expression index rejected with remediation | **PASS** |
| `TestRegression_FunctionalAndPrefixDescriptors/Target_Prefix_Index_Rejected` | Target prefix index (`SUB_PART`) rejected with remediation | **PASS** |
| `TestRegression_FunctionalAndPrefixDescriptors/Source_Prefix_Index_Cannot_Imply_Target` | Source prefix index cannot imply full target unique | **PASS** |
| `TestRegression_FunctionalAndPrefixDescriptors/Mirrored_Functional_Index_Accepted` | Identical mirrored functional descriptors accepted | **PASS** |
| `TestRegression_TargetMissingTable` | Target table not yet created returns `nil` (schema init path) | **PASS** |
| `TestRegression_RenamedTableAndColumns` | Table rename (`app.users` -> `app.app_users`) and column rename (`email` -> `user_email`) accepted | **PASS** |
| `TestRegression_CollationLooser` | Target looser collation (`utf8mb4_general_ci` vs `utf8mb4_bin`) rejected with `collation_looser` | **PASS** |
| `TestRegression_CheckRowKeysIntegration` | End-to-end `CheckRowKeys` integration verifies mirrored passes & target-only fails | **PASS** |

### Existing Target Key Tests
Executed using `/usr/local/go/bin/go test -v ./internal/validate -run TestCheckTargetKey`:
- `TestCheckTargetKeyRejectsConflictingUniqueConstraints`: **PASS** (all 7 subtests: exact, subset_only, exact_and_subset, exact_and_unrelated, exact_and_functional, exact_and_prefix, exact_and_superset)
- `TestCheckTargetKeyMirroredPrimaryAndEmailUnique`: **PASS**

### Isolated Live Database Verification
Executed using `/usr/local/go/bin/go test -v ./internal/validate -run TestIsolatedLiveMySQLCheck`:
- Spun up isolated MySQL 8.4 container (`mysql:8.4`).
- Executed real DDL for `src_db.users` and `dst_db.users` with `PRIMARY KEY (id)` and `UNIQUE KEY uk_email (email)`.
- Verified `CheckTableConstraintCompatibility` passes on live mirrored schemas.
- Added target-only `UNIQUE KEY uk_phone (phone)`: verified live check rejects with `ErrMissingPK` (exit 3) and MySQL silent-merge warning.
- Replaced target constraint with superset `UNIQUE KEY uk_id_email (id, email)`: verified live check passes.
- Test outcome: **PASS** (elapsed 20.44s).

---

## 3. Files Modified and Created

- `internal/validate/constraint_compat.go`: **NEW** (Constraint compatibility verification engine, catalog inspection, descriptor validation, rename mapping).
- `internal/validate/constraint_compat_test.go`: **NEW** (Regression unit tests with mock SQL drivers + isolated live MySQL 8.4 verification).
- `internal/validate/rowkey.go`: **MODIFIED** (Wired constraint check into `CheckKeys` when both DB handles are provided; added `CheckRowKeys`).
- `reports/code-review-20261117/followup-agy/results/02-target-constraints.md`: **NEW** (This report).

Existing ~68 pending changes in the workspace were preserved intact.

---

## 4. Remaining Limitations & Boundaries

1. **Complex Partial Indexes**: Target unique indexes with custom SQL `WHERE` predicates (`indpred IS NOT NULL`) cannot be proven equivalent to arbitrary source constraints automatically; such partial unique constraints are flagged as inconclusive with remediation rather than guessed safe.
2. **Dynamic DDL during CDC**: Preflight constraint compatibility validates table definitions at `strand validate` time. DDL applied out-of-band to the target while the CDC pipeline is actively streaming remains governed by the replication worker error handlers.
