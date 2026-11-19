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

## Current scope

This schedules trusted native wrappers and centralizes their output, catalog,
verified download (`strand backup fetch`), and MCP discovery. It does not itself
implement every database vendor's native backup procedure, restore into
databases, automatically expire objects, or delete S3 data; use bucket lifecycle
rules for retention until a managed retention job exists. Backups are not
encrypted by Strand: use bucket default encryption (SSE-S3/SSE-KMS) and TLS.

Retention and restore workflows intentionally remain explicit. Mature tools
often separate snapshot expiration from physical pruning, because pruning can
hold repository locks and should be followed by integrity checks ([restic
forget/prune guidance](https://restic.readthedocs.io/en/stable/060_forget.html)).
For database-native incremental chains, a base or intermediate backup may be a
dependency of later artifacts; deleting it without dependency-aware retention
can make the chain unrestorable ([Percona XtraBackup incremental prepare
guidance](https://docs.percona.com/percona-xtrabackup/8.4/prepare-incremental-backup.html)).
Likewise, point-in-time recovery depends on retaining the required WAL alongside
base backups ([pgBackRest user guide](https://pgbackrest.org/user-guide.html)).
Before adding automated expiry or prune, Strand needs format-aware dependency
metadata, a restore/drill state, and a policy that preserves required logs.
Job output files are capped at 32 MiB each; output after the cap is discarded
and a truncation marker is written, while process status and exit code remain
available.
