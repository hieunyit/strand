# Central backup catalog and S3 storage

Strand can register an existing native database backup in the central metadata
catalog, stream its artifact to an S3-compatible bucket, and expose read-only
catalog tools over MCP. Backup creation remains the responsibility of the native
backup tool; `strand backup push` accepts its v1 seed manifest and the resulting
file or directory.

## Requirements

- Configure the Strand Web PostgreSQL metadata database using `--config` or
  `STRAND_WEB_DATABASE_DSN`. The database user must be able to apply the Web
  migrations.
- Create the S3 bucket in advance. Strand uses path-style S3 requests and does
  not create buckets.
- Use an HTTPS endpoint in production. HTTP is accepted only for a loopback
  endpoint when `STRAND_BACKUP_S3_ALLOW_INSECURE_LOCAL=true` is explicitly set.
- The temporary directory used for multipart staging (`STRAND_BACKUP_TMPDIR`,
  default the OS temp dir) needs `--part-size-mib × --upload-concurrency` free
  space per running upload (64 MiB × 4 = 256 MiB by default). Staged parts are
  0600 files and are removed as soon as each part is uploaded.
- Add a bucket lifecycle rule `AbortIncompleteMultipartUpload` (e.g. 7 days) as
  a backstop. `strand backup push` itself aborts its upload on any failure or
  Ctrl-C, and on a retry with the same `--id` aborts uploads left behind by a
  killed earlier attempt.
- Private CAs: set `SSL_CERT_FILE`/`SSL_CERT_DIR`; there is no option to skip
  TLS verification.

Set these environment variables for S3 access:

```sh
STRAND_BACKUP_S3_ENDPOINT=https://s3.example.com
STRAND_BACKUP_S3_REGION=us-east-1
STRAND_BACKUP_S3_ACCESS_KEY=...
STRAND_BACKUP_S3_SECRET_KEY=...
# Optional for temporary credentials:
STRAND_BACKUP_S3_SESSION_TOKEN=...
```

## Upload and catalog

```sh
strand backup push \
  --config web.yaml \
  --manifest /backups/seed.json \
  --artifact /backups/native-backup \
  --bucket central-backups \
  --prefix production/mysql
```

`--artifact` can be a regular file or directory. Directories are streamed as
TAR without holding the entire archive in RAM; the S3 key ends in `.tar` to
preserve that packaging information. Regular files are stored byte-for-byte.
The manifest sidecar retains all supplied JSON fields while changing
`backup_ref` to the managed S3 artifact URI. The command records the native
tool, source identity, engine, recovery position, size, SHA-256, manifest URI,
verification time, expiration, and legal-hold state. Before marking the record
complete, Strand checks the uploaded object's size and reads it back from S3 to
compare its SHA-256 with the source stream; it also verifies the manifest
sidecar. This adds S3 download bandwidth roughly equal to the uploaded size.
Set `STRAND_BACKUP_VERIFY_READBACK=false` only when that cost is unacceptable;
then verification uses size and any checksum metadata returned by S3. A failed
or interrupted attempt can be retried with the same `--id` and identical backup
metadata; a per-backup metadata lock prevents concurrent uploaders from
replacing each other's objects. Retrying an already-complete ID rechecks both
the artifact and manifest sidecar.

For recurring backup jobs, `strand backup schedule` runs a trusted native
backup wrapper in the foreground, then uploads its artifact and seed manifest
after each successful run:

```sh
strand backup schedule --config web.yaml --every 24h \
  --artifact '/var/backups/mysql-{id}.sql' \
  --manifest '/var/backups/mysql-{id}.seed.json' \
  --bucket central-backups --prefix production/mysql -- \
  /usr/local/sbin/create-mysql-backup
```

The wrapper receives `STRAND_BACKUP_ID`, `STRAND_BACKUP_ARTIFACT`, and
`STRAND_BACKUP_MANIFEST` in its environment and must create both files. With
`--tunnel-ssh`, `--tunnel-user`, and `--tunnel-remote`, the command also receives
`STRAND_BACKUP_SOURCE_ADDR` pointing to a loopback SSH forward. Supervise the
foreground scheduler with systemd or another process manager. Its output is the
job log; completed backups appear in the central catalog. Uploads are read back
from S3 and SHA-256 checked by default, including the manifest sidecar. This
uses additional S3 download bandwidth roughly equal to the uploaded data. Set
`STRAND_BACKUP_VERIFY_READBACK=false` only when that cost is unacceptable; then
verification is limited to object size and available S3 checksum metadata.

The Web **Backups** page can create, edit, start, pause, run once, and delete
these recurring job definitions. The Web service must have its S3 credentials,
metadata DSN, wrapper executable, backup output directory, and (for tunnels)
SSH identity and `known_hosts` files mounted and readable in its own container
or host. Job configuration is stored in PostgreSQL and enabled schedules resume
when Web starts. Only the ACTIVE Web instance (the `web_lease` holder) starts
scheduler processes: a standby starts none and answers backup-job writes with
503 like every other write; on takeover the new active instance starts the
enabled schedulers and the old one stops its own when it steps down (a run in
progress there is recorded as interrupted). As a safety net, PostgreSQL advisory
locks still elect one scheduler per job across Web replicas (scheduler processes are
detached: if the old active instance exits abruptly, its scheduler keeps the job's
lock and schedule until it ends or the job is paused/edited, and the new active
instance's scheduler waits for that lock, so runs never double up), and a persisted next-run timestamp prevents a Web restart from
triggering an extra backup. A second replica waits for the scheduler lock and
takes over if the active scheduler exits. Scheduled and **Run now** executions
share a per-job execution lock, so invocations of one job do not overlap.
The catalog records when a job run starts and finishes. If a process dies during
a backup, the next scheduler records the interrupted run and waits a full
interval before retrying. After taking the execution lock, a scheduler checks
the persisted due time again so a queued schedule does not run immediately
after a **Run now** operation.
Each invocation receives unique `{id}`-expanded output paths.
The wrapper runs as the Web service OS account, so install only trusted wrapper
scripts and grant that account the minimum required access. Web-managed jobs
are disabled until `STRAND_WEB_BACKUP_COMMAND_DIR` names a directory of wrapper
scripts (not group/world-writable, ideally root-owned); a job's command must be
an executable directly inside it. Otherwise anyone with the Web admin password
or API token could run any program on the host. Artifact/manifest templates
must be clean absolute paths (no `..`), because local cleanup removes them
recursively. The wrapper's environment has Strand's own secrets removed
(`STRAND_WEB_*`, `STRAND_BACKUP_S3_*`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`); give it only its source-database credentials. Creating,
editing, starting, pausing, running and deleting jobs is audited
(`backup_job.*`) and refused when the audit log cannot be written; scheduled
uploads write `backup.push` audit rows. If a job's scheduler process exits
unexpectedly, Web restarts it with capped backoff (up to 5 minutes) for as long
as the job is enabled. Job output and exit
status are available through the Web Jobs view. **Run now** starts a separate
one-off execution; it waits for an active run to finish before it starts.
The Web form enables local cleanup by default: after a verified successful S3
upload, Strand removes that run's generated artifact and manifest. Failed runs
keep their local files for diagnosis/retry. Uncheck cleanup if a local copy is
required; provision enough disk and separately manage retained files. Managed
scheduler logs are capped at 32 MiB, after which output is discarded with a
marker. The Web job runner still connects child output directly to a file
descriptor so a Web restart does not break a detached backup process.

Uploads are multipart for objects over 8 MiB: parts (`--part-size-mib`,
default 64) are uploaded in parallel (`--upload-concurrency`, default 4).
Transient S3 errors (network errors, 408, 429, 5xx, including the
"200 OK with an error body" completion case) are retried with exponential
backoff. Memory use is bounded (parts are staged on disk); for directory
archives of unknown size the part size doubles every 1,000 parts so archives
of several TiB fit within S3's 10,000-part limit. Backups read only local
files; the source database is not touched, so the replication governor is not
involved (use `ionice`/`nice` if disk read bandwidth on the backup host
matters).

Usage and configuration errors exit 2. Every push and fetch is recorded in the
Web audit log (`backup.push`, `backup.fetch`); if the audit row cannot be
written the command fails (and `fetch` removes the downloaded copy).

The upload path accepts any v1 seed manifest whose `kind` is a lowercase tool
identifier and whose engine/recovery point is valid. Existing Strand kinds
include `xtrabackup`, `mydumper`, `mysqldump`, `clone`, `pgbackrest`, `walg`,
and `strand-export`. Since upload is format agnostic, additional native outputs
such as `mariabackup`, `mysql-shell`, `pg_dump`, `pg_basebackup`, `barman`, and
`pg_probackup` can also be cataloged when accompanied by a v1 manifest; this
does not imply that Strand currently creates or restores those formats.

## List and fetch (restore input)

```sh
strand backup list --config web.yaml [--source S] [--engine mysql|postgres] [--status complete] [--limit 50] [--json] [--cursor TOKEN]
strand backup fetch --config web.yaml --id bkp-... --out /restore/dump.sql [--with-manifest]
```

`fetch` only accepts `complete` backups, streams the object to a temporary
file next to `--out`, verifies its size and SHA-256 against the catalog, and
only then links it to `--out`. It never overwrites an existing path (exit 2)
and a digest mismatch exits 5 (`verify_mismatch`) without leaving a file.
Directory artifacts are fetched as the `.tar` they were uploaded as.

Strand does not restore into a database: restore the verified file with the
native tool, into a target you chose explicitly, e.g. for `mysqldump`:

```sh
mysql -h restore-host -u root -p restored_db < /restore/dump.sql
```

## Web UI and API

The Web UI adds a read-only **Backups** page (newest first, 200 per page with
an "Older backups" link). Authenticated APIs are:

- `GET /api/v1/backups` with optional `source`, `engine`, `status`, `limit`,
  `created_after`, `created_before`, and `cursor` (the `next_cursor` of the
  previous page; present only when the page is full).
- `GET /api/v1/backups/{id}` for a catalog record.
- `GET /api/v1/backups/{id}/status` for lifecycle and verification status.

## MCP

Run `strand mcp --config web.yaml` as an MCP stdio server. Configure the MCP
client to launch this command and pass the metadata database configuration via
the config file or `STRAND_WEB_DATABASE_DSN`. stdout is reserved for MCP JSON-RPC;
startup diagnostics go to stderr.

The server exposes read-only `list_backups`, `get_backup`, and `backup_status`
tools (a fixed allowlist; any other tool name or argument is refused).
Responses omit bucket names, object keys, manifest URIs, credentials, and
signed URLs. The MCP surface cannot upload, download, delete, restore,
or change retention/legal-hold state.

Security model: the transport is stdio only; there is no network listener, so
there is nothing to expose on a port. Whoever can launch `strand mcp` must
already hold the metadata database credentials, which are the authorisation
boundary; give MCP clients a database role limited to what they need. Every
tool call writes an audit row (`mcp.list_backups`, ...) with the sanitised
arguments; if the audit row cannot be written the result is withheld. At most
16 calls run concurrently per session.

## SSH tunnel

For a database reachable only from a bastion, open a loopback-only forward and
configure the native backup command to connect to the printed local address:

```sh
strand tunnel open --ssh bastion.example.net --user backup \
  --identity-file ~/.ssh/backup_ed25519 \
  --remote mysql.internal:3306
```

The SSH host must already be present in `~/.ssh/known_hosts` (or the file passed
with `--known-hosts`). Unknown host keys are rejected. The forward only reaches the
configured `--remote` (no SOCKS/dynamic forwarding) and listens on loopback
only; note that any local user on the backup host can connect to that loopback
port while the tunnel is open. The SSH handshake is bounded (15 s) and SSH
keepalives (every 30 s) close the forward when the bastion stops answering. The tunnel stops when the
process receives SIGINT or SIGTERM. Scheduled backup jobs can manage this tunnel
for each run using the `--tunnel-*` options.

## Managed backups

Besides uploading artifacts produced elsewhere, Strand runs backups itself.
Every feature below works from the CLI and from the Web UI (Backups page); the
Web runs schedules, retention, verification and log archiving on the **active**
instance only (lease), each as a `strand backup ...` process visible on the
Jobs page with its log.

### Built-in methods

| Method | Engine | Kind | Recovery position recorded | Streams to S3 |
|---|---|---|---|---|
| `mysqldump`, `mariadb-dump` | MySQL/MariaDB | full, logical (`--single-transaction`) | binlog file/pos + GTID set (from `--source-data`/`--master-data`) | yes (stdout) |
| `mydumper` | MySQL | full, logical, parallel | `metadata` SHOW MASTER STATUS | tar of the dump dir |
| `pg_dump` | PostgreSQL | full, logical (`-Fc`) | consistent time | yes |
| `pg_basebackup` | PostgreSQL | full, physical (`-Ft -X fetch`) | start/end LSN, timeline | yes (`-D -`) |
| `mongodump` | MongoDB | full, logical (`--archive --oplog`) | oplog timestamp before/after | yes |
| `custom` | any | whatever the wrapper produces | from the v1 seed manifest | file/dir upload |

Strand builds the native tool's argv itself (no shell). Tools are taken from
`STRAND_BACKUP_TOOLS_DIR` when set (only there), else from
`STRAND_WEB_BACKUP_COMMAND_DIR`, else `PATH`. Passwords are never in argv or
in the catalog: a job names an environment variable of the Web service
(`conn.password_env`, MongoDB `conn.uri_env`) and Strand passes the value as
`MYSQL_PWD`/`PGPASSWORD` or in a 0600 option file in a private temp directory.
Names reserved for Strand's own secrets (`STRAND_WEB_*`, `STRAND_BACKUP_S3_*`,
`STRAND_BACKUP_REPO_*`, `STRAND_BACKUP_ENCRYPTION*`, `STRAND_BACKUP_NOTIFY*`,
`AWS_*`) are refused. A failing tool aborts the upload and marks the record
`failed`. Each stored payload is read back (and decrypted/decompressed with its
plaintext SHA-256 checked) before the record becomes `complete`; set
`STRAND_BACKUP_VERIFY_READBACK=false` to skip.

```sh
strand backup schedule --job-id bjob-...        # scheduler of a built-in job (started by the Web)
strand backup schedule --job-id bjob-... --once # one run ("Run now")
```

### Compression and client-side encryption

Payloads can be compressed (zstd, levels 1-4, multi-threaded) and encrypted
(AES-256-GCM) on the Strand host before upload (`compress`/`encrypt` job
options, `strand backup push --compress --encrypt`). Format "SBK1":

```
header = "STRNDBK1" | flags | chunk size | key id | salt(32) | key check(8)
body   = zstd stream, then (if encrypted) 1 MiB frames: len | AES-256-GCM(frame)
```

Each stream gets its own key: HKDF-SHA256(master key, random salt); frame nonces
are a counter plus a last-frame flag (STREAM construction), and the header is the
AAD of every frame, so truncation, reordering, header edits and bit flips are
detected. The catalog records `format`, `compression`, `key_id`, the stored
size/SHA-256 and the plaintext size/SHA-256. `strand backup fetch` and restores
decode transparently (`fetch --raw` keeps the stored bytes).

Keys (32 random bytes; `strand backup keygen`):

```sh
STRAND_BACKUP_ENCRYPTION_KEYS="2026q1:<base64>,2025q4:<base64>"   # first = active
STRAND_BACKUP_ENCRYPTION_KEYS_FILE=/etc/strand/backup-keys          # "id:key" lines, chmod 600
STRAND_BACKUP_ENCRYPTION_KEY=<base64>  (+ STRAND_BACKUP_ENCRYPTION_KEY_ID, default "default")
```

Rotation: put the new key first and keep old keys listed until every backup
encrypted with them has expired; restoring needs the key id recorded in the
catalog, and a wrong key fails with "wrong encryption key" before any data is
decoded. Losing a key loses its backups: keep keys in your secret manager.

### Repositories, Object Lock and copies (3-2-1)

Without repositories, backups go to `STRAND_BACKUP_S3_*` + the job's bucket.
Named repositories (Web: Repositories; API `/api/v1/backup-repositories`) hold
only an id, bucket, prefix, Object Lock settings and a default retention policy;
endpoint and credentials stay in the environment under the repository's prefix:

```sh
STRAND_BACKUP_REPO_OFFSITE_ENDPOINT=https://s3.eu-central-1.amazonaws.com
STRAND_BACKUP_REPO_OFFSITE_REGION=eu-central-1
STRAND_BACKUP_REPO_OFFSITE_ACCESS_KEY=... / _SECRET_KEY=... / _SESSION_TOKEN=...
```

With `lock_mode` GOVERNANCE or COMPLIANCE and `lock_days`, every object is
written with S3 Object Lock retention (the bucket must be created with Object
Lock; Strand sends Content-MD5 as required) and the catalog records the version
id and retain-until date. `replicate_to` copies each new backup to a second
repository (`strand backup copy --id ... --to-repository ...`): bytes are copied
unchanged (still encrypted), verified on download and after upload, and
cataloged as a new record with `copy_of`.

### Retention

GFS policy per job (and a default per repository for backups without a job):
`keep_last`, `keep_hourly`, `keep_daily`, `keep_weekly`, `keep_monthly`,
`keep_yearly`, `keep_within_days`, `max_age_days`, plus `lock_days`. A backup is
kept when any rule selects it; `max_age_days` is a hard cap. Guarantees: the
newest full backup is never expired; a kept incremental keeps its whole chain;
log segments are kept from the oldest kept base onward (PITR stays possible for
every kept base); legal hold and an unexpired Object Lock always block expiry.

```sh
strand backup prune --job-id bjob-... --dry-run   # decision and reason per backup
strand backup prune --job-id bjob-...             # delete objects (versions), then mark expired
```

The Web previews a policy (also an unsaved one) and the active instance applies
each job's policy hourly. Objects are deleted before the record is marked
`expired`, every expiry is audited (`backup.expire`), and a failed deletion is
retried on the next run.

### Point-in-time recovery

Enable `log_archive` on a built-in job; the active Web keeps one
`strand backup log-archive --job-id ...` process per job:

- **MySQL** (GTID mode): Strand's binlog archiver (the one behind
  `strand archive binlog`: GTID auto-position, crash-safe, gap detection)
  writes to a local staging directory; each finalized binlog file is uploaded
  with its first/last event time and GTIDs, then removed locally.
  `log_flush_seconds` issues `FLUSH BINARY LOGS` that often (needs RELOAD) to
  bound the RPO.
- **PostgreSQL**: Strand is a physical replication client on its own slot
  (`strand_<job>`; needs REPLICATION) — no `archive_command`, nothing installed
  on the database host. Each completed WAL segment is uploaded with its start/end
  LSN. `log_flush_seconds` calls `pg_switch_wal()` on quiet databases. Drop the
  slot when you delete the job, or WAL accumulates on the server.
- **MongoDB** (replica set): tailable cursor on `local.oplog.rs`; slices of up to
  `log_flush_seconds` (default 60 s) or 64 MiB, contiguous by timestamp. An
  oplog rollover is reported as a gap.

The dashboard shows the recoverable window per source; `strand backup verify`
checks log continuity. Restore to a time:

```sh
strand backup restore --source mysql-prod --to-time 2026-11-20T10:15:00Z --dry-run   # plan: base + logs
strand backup restore --source mysql-prod --to-time 2026-11-20T10:15:00Z \
  --target-host db-restore --target-user restore --target-password-env RESTORE_PW \
  --target-database shop_20261120 --yes
strand backup restore --source pg-prod --to-time 2026-11-20T10:15:00Z --target-dir /srv/restore/pg --yes
```

Strand chooses the newest base backup consistent before the target and the
contiguous logs up to it. The target is always new — the database must not
exist, the directory must be empty — and the source is never written:

- MySQL: the dump is loaded into the new database (its `GTID_PURGED` statement
  is commented out), then `mysqlbinlog --exclude-gtids=<dump GTIDs>
  --skip-gtids --rewrite-db=<src>-><new> --stop-datetime=<target>` is replayed
  into it. Requires a single-database dump.
- PostgreSQL: the base backup is extracted, the needed WAL is staged in
  `strand_wal/`, and `restore_command`, `recovery_target_time` and
  `recovery_target_action=promote` are set with `recovery.signal`; start
  PostgreSQL on the directory to recover and promote.
- MongoDB: `mongorestore --oplogReplay` of the base into a new deployment, then
  the archived oplog with `--oplogLimit`; single-database dumps restore under a
  new name (`--nsFrom/--nsTo`).

`--id <backup>` restores one backup as is. The Web restore wizard (Backups page)
requires typing the target name to confirm and opens the job log.

### Verification and test restores

```sh
strand backup verify --id bkp-...                  # HEAD + full re-read: stored and plaintext SHA-256
strand backup verify --job-id bjob-... --sample 10 # 10% of payloads fully re-read (newest always), the rest HEAD-checked,
                                                    # incremental chains complete, log continuity
strand backup verify --job-id bjob-... --test-restore
```

`--test-restore` (job option `test_restore` + a `scratch` connection) restores
the newest mysqldump/pg_dump backup into a scratch database
`strand_verify_<time>`, records per-table row counts, and drops it. Results are
stored per backup (`/api/v1/backups/{id}/checks`), shown on the dashboard and
counted in metrics. `verify_every_seconds` schedules it on the active instance.

### Connections, policies and schedules

A job can keep its inline `conn` (unchanged) or reference reusable objects.

**Connections** (`/api/v1/backup-connections`, migration 017) hold the source
definition once: engine (`mysql|mariadb|postgres|mongodb`), host, port, user,
TLS mode and CA, optional SSH tunnel reference and labels. The secret is either
`env` (the name of a variable in the Web environment, as before) or `stored`:
the password/URI typed in the console is sealed with the Web secret key
(`STRAND_WEB_SECRET_KEY`, AES-256-GCM, the connection id as additional data, so
a ciphertext cannot be moved to another row; `STRAND_WEB_SECRET_KEY_PREVIOUS`
re-seals them on a key rotation). The secret is never returned: GET shows only
`password_set` / `uri_set`. The Web process decrypts it only to start a backup
process, passing it in that process's **environment** (`STRAND_BACKUP_CONN_PASSWORD`,
`STRAND_BACKUP_CONN_URI`), never in argv or a file; the scheduler, Run now and the
log archiver of a job on a stored connection are started this way, so a job on a
stored connection must be run from the Web console (a bare CLI run fails with a
clear message). A connection cannot be deleted while jobs use it (409 with the
job list); editing one re-materializes its jobs and restarts their schedulers.

**Test connection** (`POST /api/v1/backup-connections/test` for an unsaved body,
`/{id}/test` for a saved one; role operator) connects and reports the server
version, the privileges a backup lacks (MySQL/MariaDB: `SELECT, RELOAD, LOCK TABLES,
REPLICATION CLIENT, SHOW VIEW, EVENT, TRIGGER`; PostgreSQL: the `REPLICATION`
attribute), `gtid_mode`/`log_bin`, `wal_level`, the MongoDB replica set and the
database list. It also returns `pitr_allowed`, `pitr_reason` and `pitr_methods`,
computed with the same rules as `ValidatePITR` plus the server prerequisites it
measured (MySQL: `gtid_mode=ON` and `log_bin=ON`; MariaDB: never; PostgreSQL:
`wal_level` replica/logical and a REPLICATION role, base `pg_basebackup`;
MongoDB: a replica set). The result of a saved connection is kept in
`last_test_*`. The test makes the Web server open a connection to the given
host, which is why creating connections is admin-only while testing is operator.

**Policies** (`/api/v1/backup-policies`, migration 018) bundle schedule, window,
jitter, retention, PITR/RPO (`log_archive`, `log_flush_seconds`, `rpo_minutes`,
`rto_minutes`), storage and protection (`repository`, `replicate_to`, compression,
encryption), verification (`verify_every_seconds`, `test_restore`) and retries.
Gold, Silver and Bronze are created once on the first start (daily 01:00 with
PITR and RPO 5 min / daily 01:00 / Sunday 02:00, retention 14d+8w+12m / 7d+4w /
4w) and are editable and deletable; a restart never re-creates them. A job
following a policy stores the effective values and the list of fields it
`overrides`: a field the request carried with a value different from the policy
becomes an override (or send `overrides` explicitly), everything else follows the
policy. A policy a job's method cannot honour is applied as far as possible and
reported in `warnings` (PITR on `mariadb-dump`, `pg_dump` or a multi-database
`mysqldump` is switched off; test-restore needs a scratch connection), so a policy
edit never makes a stored job unrunnable. Editing a policy re-materializes every
job that follows it and restarts its scheduler; `PUT` returns `affects_jobs` and
`POST /api/v1/backup-policies/{id}/preview` is the dry run (affected jobs and what
changes for each, nothing written). A policy cannot be deleted while jobs use it.

**Schedules.** `schedule` is `{kind, at, days, cron, timezone, every_seconds}`:
`interval` (`every_seconds`, the legacy `every_seconds` of a job without
`schedule`), `daily` (`at` `HH:MM`), `weekly` (`days` 0-6, 0 = Sunday),
`monthly` (`days` 1-31, a day past the end of a short month runs on its last day),
`cron` (5 fields: lists, ranges, steps, month/weekday names, weekday 7 = Sunday;
day-of-month and day-of-week combine with OR like Vixie cron; no `@macros`).
`timezone` is an IANA name (UTC when empty; zone data is embedded). For
non-interval kinds `every_seconds` is kept as the typical gap between runs, for the
retry back-off cap. `POST /api/v1/backup-schedules/preview` returns the next runs
(default 5, up to 20) in UTC and in the zone.

`window_minutes`: a run not started within this many minutes of its due time
(scheduler down, standby, or no concurrency slot free) is recorded as status
`missed`, not run late, and the next occurrence is scheduled; the `missed`
notification is sent. 0 = run whenever possible (catch-up, as before).
`jitter_seconds` delays each start by a random 0..N seconds (never past the
window). `STRAND_BACKUP_MAX_CONCURRENT` (default 2, 0 = unlimited) and
`STRAND_BACKUP_MAX_PER_HOST` (default 1, 0 = unlimited; host = connection
host:port) are enforced across all scheduler processes of the active instance
through slots in the metadata database (renewed while a run lasts, expiring 90 s
after a crash); Run now is not queued behind them.

### Retries, bandwidth, staged parts

`retry_max` and `retry_backoff_seconds`: after a failed run the next attempt is
due after backoff × 2^(failures-1), capped by the interval, until retry_max
consecutive failures; success resets the counter. `bandwidth_kbps` limits the
upload rate of a job. Multipart staging files are named with the owner PID and
files of dead processes are removed whenever a repository is opened.

### Notifications

Configured in the Web service environment only (destinations are secrets):

```sh
STRAND_BACKUP_NOTIFY_WEBHOOK=https://hooks.example.com/strand      # JSON event
STRAND_BACKUP_NOTIFY_SLACK_WEBHOOK=https://hooks.slack.com/...     # {"text": ...} (Slack/Mattermost)
STRAND_BACKUP_NOTIFY_SMTP_ADDR=smtp.example.com:587                # STARTTLS required except loopback
STRAND_BACKUP_NOTIFY_SMTP_FROM=strand@example.com
STRAND_BACKUP_NOTIFY_SMTP_TO=dba@example.com,ops@example.com
STRAND_BACKUP_NOTIFY_SMTP_USER=... / STRAND_BACKUP_NOTIFY_SMTP_PASSWORD=...
STRAND_BACKUP_NOTIFY_ON=failure,missed,verify_failed               # default; add success or use all
```

Events: run `failure`/`success`, `missed` schedule (due more than max(5 min,
10% of the interval) ago without a run, or a run outside its `window_minutes`), `verify_failed`. Each event is sent
once across instances and audited (`backup.notify`).

### Metrics and alerts

`GET /metrics` (Web session or API token) exposes Prometheus gauges:
`strand_backup_last_success_timestamp_seconds{source,engine}`,
`strand_backup_last_size_bytes`, `strand_backup_last_duration_seconds`,
`strand_backup_stored_bytes`, `strand_backup_recovery_window_end_timestamp_seconds`,
`strand_backup_job_consecutive_failures{job,name}`,
`strand_backup_job_last_run_success`, `strand_backup_recent_check_failures`.

```yaml
groups:
- name: strand-backups
  rules:
  - alert: StrandBackupTooOld        # RPO breach (daily jobs)
    expr: time() - strand_backup_last_success_timestamp_seconds > 26*3600
  - alert: StrandBackupFailing
    expr: strand_backup_job_consecutive_failures >= 2
  - alert: StrandPITRLagging         # log archiving stalled
    expr: time() - strand_backup_recovery_window_end_timestamp_seconds > 900
  - alert: StrandBackupVerifyFailed
    expr: strand_backup_recent_check_failures > 0
```

### Catalog schema (migrations 014 and 016)

`backups` gains: `job_id`, `repository`, `method`, `kind` (full | incremental |
log), `parent_id`, `copy_of`, `format`, `compression`, `key_id`, `plain_bytes`,
`plain_sha256`, `oplog_ts`, `end_position` (JSON), `start_time`, `end_time`,
`version_id`, `lock_mode`, `retain_until`, `checked_at`, `check_status`,
`check_detail`, `expired_at`, `details` (JSON: tool version, timeline, to_lsn,
seq, server_file, databases, ...); engine `mongodb`. New tables
`backup_checks` (verification history) and `backup_repositories`;
`backup_jobs` gains `options` (JSON: method, connection, retention, encryption,
log archiving, verification), retry columns and notification/claim timestamps.
`backup_runs` (migration 016) stores execution history for every job run
(`schedule`, `manual`, `retry`, `api`), with normalized `error_code` and
remediation advice.
Log segments are self-describing (their catalog row is the manifest); base
backups get a `manifest.json` sidecar containing the catalog record so a
repository can be re-indexed without the database.

### Web UI and Route Architecture (P1)

Strand Web splits backup management across dedicated, fast-loading routes under the "Data protection" sidebar group:
- `/backups` (Overview dashboard): Stat tiles (protected sources, 24h success rate, failing jobs, storage used, test-restore status), standby/repo/key banners, needs-attention list, and 14-day stacked activity chart rendered server-side as CSP-safe SVG.
- `/backups/jobs`: Filterbar (query, engine, status, repository) synced to the URL, bulk action bar (Run, Pause, Resume, Delete), last-10-runs indicators, and next run countdowns.
- `/backups/jobs/{id}`: Detailed job view with header actions and tabs: Overview (30-day run sparkline), Runs (cursor-paged run history with live log stream and running execution cancellation), Restore points, Settings (read-only with edit mode), and Activity (audit entries filtered to this job).
- `/backups/catalog` and `/backups/catalog/{id}`: Paged restore point browser and backup detail inspect view with position coordinates and verification history.
- `/backups/restore`: Restore wizard.

### Connections, policies and the job wizard (P2/P3 UI)

- `/backups/connections` and `/backups/connections/{id}`: table with a filter bar (name/host, engine, last test status) and, for admins, a
  `New connection` drawer (`dialog.drawer`) whose fields follow the engine (default port, URI variable for MongoDB, environment variable or
  stored encrypted secret). `Test connection` shows a checklist (connect, version, privileges, log_bin/GTID or wal_level or replica set,
  databases, point-in-time recovery verdict). The detail page has an Overview and a `Used by` tab. Operators can test; only admins create,
  edit or delete. Secrets are never rendered.
- `/backups/policies` and `/backups/policies/{id}`: policy table (schedule in words, retention in words, PITR, verify, job count). The
  editor has a schedule field with a live "next 5 runs" preview in the chosen time zone and, on save of an existing policy, an
  `Affects N jobs` confirmation fed by `POST /api/v1/backup-policies/{id}/preview`.
- `/backups/jobs/new` and `/backups/jobs/{id}/edit` (admin only): 4-step wizard (`ol.stepper`, `aria-current="step"`) with a live Summary
  column. Step 1 needs a successful connection test before continuing, lists the databases from the test, offers only the methods of the
  connection's engine (Recommended badge; the custom script is under Advanced). Step 2 picks a policy (Gold/Silver/Bronze/custom) or a custom
  schedule; the PITR switch is enabled only when the test result allows it for the chosen method and databases and otherwise shows the reason.
  Step 3 chooses the repository (immutable badge), a second copy, encryption (on by default when keys exist, yellow warning when none) and
  compression. Step 4 sets verification, test restore (mysqldump, mariadb-dump, pg_dump only), retries and shows the Review before
  `Create and run first backup` / `Create only`. Fields that differ from the selected policy carry an `overridden` badge and are sent as
  overrides; untouched fields are not sent, so the job keeps following the policy.
- The jobs list and job page show the human schedule, the policy and connection, `missed` runs (filter `status=missed`) and the job
  warnings (for example a policy PITR request that cannot apply to the job). Built-in jobs with an inline connection offer
  `Convert to connection` (`POST /api/v1/backup-jobs/{id}/convert-connection`, admin, audited as `backup_job.convert_connection`): the
  inline host/user/password-variable become an environment-mode connection (reused when an identical one exists) and the job points to it.

### PITR Constraints (P0)

Validation rules enforced by `backup.ValidateJob` and UI:
- **MySQL**: Method `mysqldump` with a GTID-capable connection and exactly one database. MariaDB / `mariadb-dump` is rejected with `"MariaDB does not support binlog archiving"`. Multi-database configurations are rejected with `"PITR requires exactly one database"`.
- **PostgreSQL**: Only `pg_basebackup`. `pg_dump` is rejected with `"PITR needs pg_basebackup as the base backup"`.
- **MongoDB**: `mongodump`.
- **Custom**: Rejected.
- Stored jobs from older versions that fail these constraints are still listed and presented with clear warning badges; the scheduler skips their log archiving with an explicit logged reason instead of failing silently. The web UI disables the PITR checkbox with explanatory help text when unsupported combinations are chosen.

## Current limits

- Strand runs on its own host, never on the database host, so physical backup
  methods that read the datadir locally (`xtrabackup`, `mariabackup`) and
  incremental-chain creation paths are removed. Existing catalog records with
  `kind=incremental` or method `xtrabackup` from older versions still list, are
  retained and expired safely according to retention policies, and can be
  restored using `strand backup fetch` and native tooling (they are just not
  created anymore).
- MySQL PITR needs GTID mode and a single-database `mysqldump` base; MariaDB
  binlog archiving is not supported (MariaDB GTIDs).
- PostgreSQL PITR follows one timeline; tablespaces (symlinks) are refused on
  restore. pg_dump bases are not PITR anchors (use pg_basebackup).
- No deduplication across backups (every run is a full stream; incremental chain
  creation is removed).
- Test restores cover mysqldump/mariadb-dump/pg_dump; physical and MongoDB test
  restores are not automated.
- Job output files are capped at 32 MiB each; output after the cap is discarded
  and a truncation marker is written.
