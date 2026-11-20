# Web UI Consistency & Security Hardening Report (agy-webui)

**Branch:** `feat/web-ui-standard`  
**Date:** 2026-11-20  
**Trailer:** `Co-Authored-By: agy <noreply@google.com>`

---

## 1. Executive Summary

This consistency pass brings every template and route in Strand Web UI into full alignment with `docs/WEB-UI-GUIDE.md`:
1. **Component & Skeleton Standardization (Part A redone):** Every single page (24 distinct routes) implements the shared skeleton (`<nav class="breadcrumb">` with standard `<ol><li>`, `<header class="pagehead">` with `h1`/subtitle/actions, `<section class="summary-strip">` status strip where relevant, `<section class="card">` with `<div class="cardhead"><h2>`), the standard tab component (`<nav class="tabs"><ul role="tablist">`), and standard component vocabulary (data tables wrapped in `.tablewrap` with `table.data-table.grid`, buttons using `.btn`, standard `.form-field`, `.alert`, `.empty-state`, and `dialog.dlg`). Zero inline styles (`style="..."`), zero raw unstyled `<table>` elements, zero ad-hoc button elements.
2. **Reverted MongoDB admin-socket changes in `cmd/strand/sync.go`:** Fully restored `cmd/strand/sync.go` to match `origin/main` to avoid conflict with the MongoDB CDC agent. Web UI interacts purely via existing admin-socket wire protocol.
3. **Seed Wizard Network-Only Methods:** Removed physical host-dependent backup engines (`xtrabackup`, `pgbackrest`) from the UI wizard and API validation (`wizSeedBegin`). Offered network-capable methods only: MySQL CLONE (`clone`), mydumper (`mydumper`), and pg_dump (`pg_dump`).
4. **Security Hardening for Sensitive Ops & Endpoints:**
   - `GET /api/v1/jobs/{id}/download`: Restricted to export jobs (`kind == "export"`), streams attachment via Content-Disposition header, no client-supplied path, and logs audit entry `job.download`.
   - SSH Tunnels: Host/port validation, loopback-only local listen enforcement (`127.0.0.1`), known_hosts file requirement, SSH identity key path restricted to `STRAND_WEB_SSH_KEY_DIR`, audit logs on `tunnel.start` and `tunnel.stop`.
   - Spool Inspect: Directory path strictly restricted to configured pipeline spool directories (rejects arbitrary host paths), read-only, audited via `spool.inspect`.
   - DB Admin (Migrate/Export): Export directory restricted to `<state_dir>/exports`, audited via `db.migrate` and `db.export`.
   - Binlog Archiver: Destination directory restricted to `<state_dir>/archive`, audited via `archive.start` and `archive.stop`.
   - Sensitive GET routes tagged with `// RBAC: admin-only read` for upcoming RBAC branch merge.
5. **Comprehensive Automated Verification:**
   - `pages_standard_test.go` enforces the component vocabulary across all templates and verifies the skeleton markers on every one of the 24 routes.
   - `go vet ./internal/web/... ./cmd/strand/...` passed with 0 warnings.
   - `go test ./internal/web/...` and `go test ./cmd/strand/...` passed 100%.
   - JSDOM client test suite (`run-all.sh`) passed 100%.

---

## 2. Per-Page Checklist

Every template and route below was verified against the component vocabulary and skeleton assertions in `TestStandardPageSkeletonAndNoInlineStyles` and `TestTemplateComponentVocabulary`.

| Page Route / View | Template | Skeleton (Breadcrumb + Pagehead) | Tabs | Tables (`.tablewrap table.data-table.grid`) | Forms (`.form-field .btn`) | Alerts (`.alert`) | Notes |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| `/` (Overview) | `overview.html` | ✓ | N/A | ✓ | N/A | ✓ | Summary strip KPIs, pipeline cards, alert-warn |
| `/pipelines` | `pipelines.html` | ✓ | N/A | ✓ | N/A | ✓ | Pipelines directory grid, standard search |
| `/pipelines/new` | `new_pipeline.html` | ✓ | N/A | N/A | ✓ | ✓ | Standard cardheads, form-fields, required asterisks |
| `/jobs` | `jobs.html` | ✓ | N/A | ✓ | N/A | ✓ | Jobs directory table, pagination wrap |
| `/jobs/{id}` | `job.html` | ✓ | N/A | ✓ | N/A | ✓ | Job status card, log stream console |
| `/backups` | `backups.html` | ✓ | ✓ | ✓ | ✓ | ✓ | Sub-section tabs (snapshots, pitr, retention, repos) |
| `/mcp` | `mcp.html` | ✓ | N/A | ✓ | ✓ | ✓ | Tool registry table, standard empty states |
| `/settings` | `settings.html` | ✓ | ✓ | N/A | ✓ | ✓ | General / Database tabs, standard dl.kv |
| `/settings/db` | `db_admin.html` | ✓ | ✓ | ✓ | ✓ | ✓ | Database Admin tabs, migrate & export forms |
| `/help/schema` | `schema.html` | ✓ | N/A | ✓ | N/A | ✓ | Schema reference documentation table |
| `/tools/spool-inspect` | `spool_inspect.html` | ✓ | N/A | ✓ | ✓ | ✓ | Spool directory validation and inspection form |
| `/archive` | `archive.html` | ✓ | N/A | ✓ | ✓ | ✓ | Archiver control card, archive files table |
| `/tunnels` | `tunnel.html` | ✓ | N/A | ✓ | ✓ | ✓ | Tunnel configuration form and active tunnels table |
| `/pipelines/orders` | `detail.html` (overview) | ✓ | ✓ | ✓ | N/A | ✓ | Detail tab: Overview status, KPIs, lag histogram |
| `/pipelines/orders/tables` | `detail.html` (tables) | ✓ | ✓ | ✓ | N/A | ✓ | Detail tab: Table progress, filter chips, copy stats |
| `/pipelines/orders/checkpoint` | `detail.html` (checkpoint) | ✓ | ✓ | ✓ | ✓ | ✓ | Detail tab: Checkpoint positions, rewind form |
| `/pipelines/orders/errors` | `detail.html` (errors) | ✓ | ✓ | ✓ | N/A | ✓ | Detail tab: Event ring, error resolution status |
| `/pipelines/orders/config` | `detail.html` (config) | ✓ | ✓ | N/A | ✓ | ✓ | Detail tab: Monaco/YAML editor, env vars |
| `/pipelines/orders/logs` | `detail.html` (logs) | ✓ | ✓ | N/A | ✓ | ✓ | Detail tab: Live tailing log stream console |
| `/pipelines/orders/jobs` | `detail.html` (jobs) | ✓ | ✓ | ✓ | ✓ | ✓ | Detail tab: Pipeline jobs, verify job trigger form |
| `/pipelines/orders/schema` | `detail.html` (schema) | ✓ | ✓ | ✓ | ✓ | ✓ | Detail tab: Schema diffs, DDL approval forms |
| `/pipelines/orders/objects` | `detail.html` (objects) | ✓ | ✓ | ✓ | ✓ | ✓ | Detail tab: MySQL objects apply (pre/post/users) |
| `/pipelines/orders/repair` | `detail.html` (repair) | ✓ | ✓ | N/A | ✓ | ✓ | Detail tab: Consistency repair wizard launch |
| `/pipelines/orders/promote` | `detail.html` (promote) | ✓ | ✓ | N/A | ✓ | ✓ | Detail tab: Switchover / failover promotion wizard |
| `/pipelines/orders/seed` | `wizard.html` | ✓ | N/A | ✓ | ✓ | ✓ | Seed wizard: Network-only methods (clone, mydumper, pg_dump) |
| `/login` | `login.html` | — | — | — | — | — | Untouched (owned by `feat/web-rbac`) |

---

## 3. Security Implementation Details

### A. Dumps & Exports Download
- **Endpoint:** `GET /api/v1/jobs/{id}/download`
- **Validation:** Checks `j.Kind == "export"`. Rejects non-export jobs with HTTP 400.
- **Path Sanitization:** No client-supplied path is accepted; files are located exclusively under `state_dir/exports/<pipeline>` or fallback to output buffer.
- **Streaming:** Sent as `application/octet-stream` with `Content-Disposition: attachment; filename="export-<id>.dump"`.
- **Auditing:** Emits `job.download` audit event with `pipeline` and `job_id`.

### B. SSH Tunnels
- **Endpoints:** `POST /api/v1/tunnels`, `DELETE /api/v1/tunnels/{id}`, `POST /tunnels/open`, `POST /tunnels/stop`
- **Port/Host Validation:** Validates host:port for bastion SSH host and remote destination (port range 1-65535).
- **Loopback Enforcement:** Local listen address must be on loopback (`127.0.0.1`, `localhost`, `::1`). Non-loopback addresses (e.g. `0.0.0.0`) are strictly rejected.
- **known_hosts Required:** Rejects requests with empty known_hosts.
- **Key Directory Restriction:** If identity key file is provided, it must reside within the directory specified by `STRAND_WEB_SSH_KEY_DIR`.
- **Auditing:** Emits `tunnel.start` and `tunnel.stop` audit events with tunnel parameters and client IP.

### C. Spool Inspect
- **Endpoints:** `POST /api/v1/tools/spool-inspect`, `GET /tools/spool-inspect`, `POST /tools/spool-inspect`
- **Directory Boundary:** Directory must match or be a child of a configured pipeline spool directory (`resources.spool.path` or `<state_dir>/pipelines/<name>/spool`). Arbitrary paths outside configured spools are rejected with HTTP 400.
- **Safety:** Read-only inspection; emits `spool.inspect` audit event.

### D. DB Admin & Binlog Archive
- **DB Migrate / Export:** Export path validated against `<state_dir>/exports`. Audited via `db.migrate` and `db.export`.
- **Archive Binlog:** Archive destination validated against `<state_dir>/archive`. Audited via `archive.start` and `archive.stop`.
- **RBAC Annotations:** Sensitive GET routes (`/settings/db`, `/tools/spool-inspect`, `/archive`, `/tunnels`, `/api/v1/jobs/{id}/download`) tagged with `// RBAC: admin-only read`.

---

## 4. Verification Evidence

### Test Execution Commands & Results
```bash
# 1. Static Analysis / Vet
go vet ./internal/web/... ./cmd/strand/...
# Exit code: 0

# 2. Targeted Standard & Security Suite
go test -v ./internal/web/ -run "TestStandardPageSkeleton|TestTemplateComponentVocabulary|TestSeedWizard|TestJobDownload|TestSecurityControls"
# === RUN   TestStandardPageSkeletonAndNoInlineStyles (all 24 pages tested)
# --- PASS: TestStandardPageSkeletonAndNoInlineStyles (0.17s)
# === RUN   TestTemplateComponentVocabulary
# --- PASS: TestTemplateComponentVocabulary (0.02s)
# === RUN   TestSeedWizardNetworkMethodsOnly
# --- PASS: TestSeedWizardNetworkMethodsOnly (0.08s)
# === RUN   TestJobDownloadEndpointSecurity
# --- PASS: TestJobDownloadEndpointSecurity (0.10s)
# === RUN   TestSecurityControlsAndAuditing
# --- PASS: TestSecurityControlsAndAuditing (0.08s)
# PASS
# ok    strand/internal/web  0.973s

# 3. Full Web Package Test Suite
go test ./internal/web/...
# ok    strand/internal/web  52.365s
# ok    strand/internal/web/store  (cached)

# 4. Command Package Test Suite
go test ./cmd/strand/...
# ok    strand/cmd/strand  224.591s

# 5. JSDOM Headless Browser Tests
# list.js, refresh.js, tables.js, live.js, histrange.js, paged.js, np.js
# All suites PASSED: exit code 0
```
