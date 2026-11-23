# Strand Web UI Architecture & Design Guide

## 1. Executive Summary & Inventory

Strand Web UI is an operational dashboard for high-throughput distributed CDC replication pipelines (MySQL, PostgreSQL, MariaDB, MongoDB). This document establishes standard patterns, shared components, and a single cohesive page skeleton across all views, eliminating one-off markup and visual inconsistencies.

### 1.1 Complete Page & Tab Inventory

| Page Path | Purpose | Key Sub-views / Tabs | Status & Summary Strip | Primary Actions |
| :--- | :--- | :--- | :--- | :--- |
| `/` | System Overview | Cluster health, needs-attention list, aggregate metrics | 5 KPI summary cards (Pipelines, Needs attention, Max lag, Rows/s, Snapshot tables), State filter pills | Link to `/pipelines`, Link to `/pipelines/new` |
| `/pipelines` | Pipelines Directory | Searchable, sortable list of all registered pipelines | Summary count of pipelines and active filters | Search input, State dropdown, "New pipeline" button |
| `/pipelines/new` | Pipeline Creation | Form to provision new replication pipeline | Engine selector, Connection inputs, Tuning section | Validate config, Test DB connection, Create pipeline |
| `/pipelines/{name}` | Pipeline Detail | 12 unified tabs for inspection and operations | `pipeline-head` strip (State badge, lag, throughput, zone, snapshot progress & ETA, uptime, checkpoint, breaker banner) | Start, Stop, Pause, Resume, Restart, Delete |
| ↳ `?tab=graph` | Topology & Metrics | Visual topology DAG, 1h / 6h / 24h / 7d historical line charts | Chart range segment selector | Throttle rate modal / form |
| ↳ `?tab=tables` | Table Snapshot Progress | Per-table snapshot status, chunk progress, ETA | Progress bar, filter chips (All, Running, Pending, Done) | Table search, Sort selector, Snapshot Add form |
| ↳ `?tab=checkpoint` | CDC Checkpoint | Target checkpoint LSN / GTID, rewind tool | Checkpoint timestamp, lag to current head | Rewind checkpoint form |
| ↳ `?tab=errors` | Errors & Diagnostics | Recent WARN/ERROR events, last exit code explanation | Exit code badge and remediation recommendation | Live events filter |
| ↳ `?tab=config` | Pipeline Config & Tuning | Masked YAML editor, .env editor, tuning sliders | Configuration file path and validation status | Validate YAML, Save & Apply, Restart pipeline |
| ↳ `?tab=logs` | Live Console Log Tail | Monospace terminal-style streaming logs | Connection status, follow toggle, log level filter | Jump to bottom, Pause streaming |
| ↳ `?tab=jobs` | Pipeline Job History | Executed verify, schema, and maintenance jobs | Total jobs count, running job indicator | Run Verify modal |
| ↳ `?tab=repair` | Repair Wizard | Guided 4-step target discrepancy reconciliation | Interactive step progress tracker | Begin, Stop, Run repair, Start |
| ↳ `?tab=promote` | Promotion Wizard | Guided cutover and fencing wizard | Interactive step progress tracker | Plan dry-run, Run promote, Abort |
| ↳ `?tab=schema` | Schema Evolution | Pending and approved DDL schema barriers | Pending DDL count, auto-schema status | Preview DDL, Approve & Apply |
| ↳ `?tab=objects` | Database Objects | Non-table objects sync (views, procs, triggers, users) | Object phase selector (pre, users, post) | List objects, Apply phase |
| ↳ `?tab=seed` | Seed Wizard | Mode B backup seeding plan and execution | SD-1..SD-4 validation status | Plan seed, Apply seed (destructive) |
| `/jobs` | Global Jobs Directory | History and status of all background jobs | Total jobs, running count, success/fail counts | Kind filter, Search |
| `/jobs/{id}` | Job Execution Detail | Real-time console output and exit code for a single job | Job status badge, duration, exit code | Cancel job, Download output |
| `/backups` | Backup Dashboard | Protected sources, success rate, needs attention, 14-day activity, recent verification | Backup KPIs, standby/repository/key banners | Open jobs, restore points |
| `/backups/jobs`, `/backups/jobs/{id}` | Backup Jobs | Job list with filters, job detail (overview, runs, restore points, settings, activity) | Last 10 runs, next run | New backup job (wizard), Run, Pause, Resume |
| `/backups/catalog`, `/backups/catalog/{id}` | Restore Points | Paged catalog, timeline by source, backup detail | Recovery window | Restore, Verify, Legal hold, Copy to repository |
| `/backups/restore` | Restore Wizard | 4 steps: restore point, target (saved connection), options, review | Recovery window | Start restore |
| Backup resources | Section (tab strip) | `/backups/connections`, `/backups/repositories`, `/backups/policies`, `/backups/keys` (admin), `/backups/notifications` (admin) | Last test status | New connection / repository / policy, Test connection |
| Reports | Section (tab strip) | `/backups/reports`, `/backups/activity` (admin) | Period summary | Export |
| Tools | Section (tab strip, admin) | `/archive`, `/tunnels`, `/tools/spool-inspect` | Running archivers / tunnels | Start, Stop, Scan |
| `/mcp` | Model Context Protocol | Strand MCP server status, declared tools, call audit | MCP connection status, socket path | Refresh tool definitions |
| Settings | Section (tab strip) | `/settings` (admin), `/settings/db` (admin), `/mcp`, `/help/schema` | | |
| `/settings` | System Settings | Console configuration, theme, security policies | Active session info, token manager | Save settings, Generate token |
| ↳ `?tab=general` | Console Settings | Refresh interval, session timeouts, TLS policies | Setting key-value inputs | Save settings |
| `/settings/db` | Metadata DB Admin | PostgreSQL metadata migration & export tools | Migration plan summary, schema version | Dry-run migration, Apply migration, Export DB |
| `/help/schema` | Config Reference | Complete JSON schema reference for pipeline configs | Interactive schema browser, required fields | Copy JSON schema |
| `/tools/spool-inspect`| Spool Sidecar Inspector| Read-only Pebble spool database offline inventory | Discovered sidecars, orphan files, total bytes | Scan spool inventory |

The SSH Tunnel tool uses the Web service's trusted host keys. Set
`STRAND_WEB_SSH_KNOWN_HOSTS` to an absolute path on the server, or leave it unset
to use the Web service account's `~/.ssh/known_hosts`. The file must contain the
verified bastion host key. In Compose, mount the file read-only and use its
container path for the environment variable.

---

## 2. Identified Design & Ergonomic Inconsistencies

1. **Inconsistent Page Skeleton:**
   - Some pages started with `<section class="card"><h2>` immediately, while others had KPI cards or toolbars.
   - Breadcrumbs were completely missing, leaving deep tabs (e.g. `/pipelines/orders/tables`) disorienting for operators.
   - Action buttons (Start, Stop, Delete, Add) were placed in disparate headers, cards, or floating divs without standard visual hierarchy.

2. **Fragmented Tabs Implementation:**
   - Pipeline detail tabs (`<nav class="tabs"><a ...>`) used anchor tags with server-side routes.
   - Backup tabs (`.b-tabs > button.b-tab`) used client-side tab switching with completely different markup, CSS classes, and active indicators.
   - Inconsistent accessibility markup: missing `role="tablist"`, `role="tab"`, `aria-selected`, and arrow keyboard navigation.

3. **Ad-hoc Status & Metric Displays:**
   - Different pages displayed metrics with conflicting font-weights, tabular numerals, units, and decimal precisions (e.g., `s`, `sec`, `seconds`).
   - Lack of unified badge classes for states (`st-synced`, `st-running`, `st-failed`, `st-paused`).

4. **Form Markup & Validation Variations:**
   - Field labels and inputs had varying margins, error message positions, and required indicators.
   - Error handling from API calls was custom in each page rather than parsing standardized `{error, fields}` payloads.

5. **CLI Features Absent from the Web Console:**
   - Seed plan & apply (`strand seed`), binlog archiving (`strand archive binlog`), SSH tunneling (`strand tunnel open`), export downloads (`strand export`), config JSON schema (`strand config schema`), spool inspection (`strand spool-inspect`), and metadata database migrations (`strand web db`) were previously CLI-only.

---

## 3. The Unified Page Skeleton

Every page in Strand Web UI strictly adheres to the following structural hierarchy inside `#content`:

```html
<!-- 1. Breadcrumb Navigation -->
<nav class="breadcrumb" aria-label="Breadcrumb">
  <ol>
    <li><a href="/">Overview</a></li>
    <li><a href="/pipelines">Pipelines</a></li>
    <li aria-current="page">orders</li>
  </ol>
</nav>

<!-- 2. Page Header: Title, Subtitle, and Primary Actions -->
<header class="pagehead">
  <div class="pagehead-title">
    <h1>orders</h1>
    <p class="pagehead-sub">MySQL 8.0 &rarr; PostgreSQL 16 &middot; background mode &middot; pid 18429</p>
  </div>
  <div class="pagehead-actions" role="toolbar" aria-label="Page actions">
    <!-- Secondary actions (ghost / default) then Primary action -->
    <button type="button" class="btn" data-action="restart">Restart</button>
    <button type="button" class="btn danger" data-action="stop">Stop</button>
    <button type="button" class="btn primary" data-action="start">Start</button>
  </div>
</header>

<!-- 3. Status / Summary Strip (Optional per page) -->
<section class="summary-strip" aria-label="Pipeline status summary">
  <!-- Key metrics, KPI pills, or pipeline-head -->
</section>

<!-- 4. Tab Navigation (Where page has sub-views) -->
<nav class="tabs" role="tablist" aria-label="Pipeline sections">
  <a href="/pipelines/orders" role="tab" class="tab active" aria-selected="true" aria-current="page">
    <svg class="ic"><use href="/static/icons.svg#i-graph"/></svg>Graph
  </a>
  <a href="/pipelines/orders/tables" role="tab" class="tab" aria-selected="false">
    <svg class="ic"><use href="/static/icons.svg#i-table"/></svg>Tables <span class="tab-count">24</span>
  </a>
  <!-- additional tabs -->
</nav>

<!-- 5. Content Cards -->
<div class="page-content">
  <section class="card">
    <div class="cardhead">
      <h2>Section Title</h2>
    </div>
    <!-- Card Body -->
  </section>
</div>
```

---

## 4. Shared Component Catalog

### 4.1 Card (`.card`)
White/dark elevated surface with subtle border, 12px radius, and standard padding.
- Header: `.cardhead` with `h2` and optional actions/controls.
- Footnote/Helper: `.muted.small`.

### 4.2 Data Table (`table.grid` inside `.tablewrap`)
- Responsive scroll container (`.tablewrap`), sortable column headers (`th button.sortbtn[data-sort]`), ARIA sort indications (`th[aria-sort="ascending|descending|none"]`).
- Right-aligned tabular numeric data (`td.r.c-num`).
- Monospace identifier columns (`td.mono`).
- Empty state row: `tr.empty > td > .empty-state`.
- No-match row for client filters: `tr.nomatch[hidden]`.

### 4.3 Key-Value List (`dl.kv`)
Two-column semantic definition list for system metadata, configuration values, and detail inspectors:
```html
<dl class="kv">
  <dt>Pipeline ID</dt>
  <dd class="mono">orders_v2</dd>
  <dt>Source Host</dt>
  <dd class="mono">10.0.1.25:3306</dd>
</dl>
```

### 4.4 Badge & Status Pill (`.badge`)
Consistent status indicator with distinct state themes:
- `.badge.st-synced` (Green / OK)
- `.badge.st-bootstrapping` (Blue / Active sync)
- `.badge.st-paused` (Yellow / Paused)
- `.badge.st-failed` (Red / Critical)
- `.badge.st-stopped` (Neutral / Idle)

### 4.5 Form Controls & Two-Column Layout (`form.two-col`)
- Field container: `.form-field` containing `<label>`, optional `<span class="req">*</span>`, `<input|select|textarea>`, optional `<span class="help">`, and `<span class="field-error" hidden>`.
- Responsive two-column form grid (`form.two-col`) switching to single-column on mobile (< 768px).

### 4.6 Buttons (`.btn`)
- Variants: `.btn` (default secondary), `.btn.primary`, `.btn.danger`, `.btn.ghost`.
- Sizes: `.btn.sm` (tables/lists), default, `.btn.lg` (wizards/call-to-action).
- Extension point: every mutating button carries `data-action="..."` for RBAC enforcement.

### 4.7 Modals & Dialogs (`dialog.dlg#confirm`)
Native `<dialog>` with focus trapping, confirmation prompt, optional confirmation string retyping (`data-retype`), Cancel button, and Confirm button.

### 4.8 Toast Notifications (`#toast`)
Fixed bottom-right notification pill with polite ARIA live region, distinct success and error themes, and click-to-dismiss.

### 4.9 Inline Alerts (`.alert`)
Standard alert banner for contextual notifications:
- `.alert.alert-info`
- `.alert.alert-warn`
- `.alert.alert-error`
- `.alert.alert-success`

### 4.10 Empty States (`.empty-state`)
Centered illustration/icon, bold title, descriptive subtitle, and optional call-to-action button for tables or sections with no records.

### 4.11 Code and Log Viewer (`pre.code.log`)
Monospace dark container with auto-scroll preservation, line wrap toggling, and keyboard navigation.

---

## 5. Standard Formatting Rules

1. **Units:**
   - Durations: `ms` for < 1s, `s` with 2 decimals (e.g. `0.25 s`) for lag, `h m s` for uptimes.
   - Throughput: `rows/s` and `txn/s` (always lowercase with forward slash).
   - Sizes: `B`, `KB`, `MB`, `GB` (binary 1024-based).
2. **Timestamps:**
   - Display: Local formatted date-time with second precision (`2026-11-20 12:45:00`).
   - ISO 8601 attributes: `<time datetime="YYYY-MM-DDTHH:mm:ssZ07:00">`.
   - Relative timestamps: `updated 5s ago`, `2m ago`.
3. **Numbers:**
   - Formatted with thousand separators: `1,250,000`.
   - Tabular numerals enabled via font feature settings (`tabular-nums`).
4. **API Errors:**
   - Handled via `renderFormErrors(form, errorPayload)` mapping `{error: string, fields: Record<string, string>}` to inline alerts and respective `.field-error` spans.
