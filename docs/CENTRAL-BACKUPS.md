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
| `mydumper` | MySQL | full, logical, parallel | `metadata` source position (`[source]` of mydumper 0.16+, `[master]`, or legacy SHOW MASTER STATUS text) | tar of the dump dir |
| `pg_dump` | PostgreSQL | full, logical (`-Fc`) | consistent time | yes |
| `pg_basebackup` | PostgreSQL | full, physical (`-Ft -X fetch`) | start/end LSN, timeline | yes (`-D -`) |
| `mongodump` | MongoDB | full, logical (`--archive --oplog`) | last BSON timestamp embedded in the oplog | yes |
| `custom` | any | whatever the wrapper produces | from the v1 seed manifest | file/dir upload |

Strand builds the native tool's argv itself (no shell). Tools are taken from
`STRAND_BACKUP_TOOLS_DIR` when set (only there), else from
`STRAND_WEB_BACKUP_COMMAND_DIR`, else `PATH`. Passwords are never in argv or
in the catalog: the password (or MongoDB URI) of a connection is entered in the
console and stored sealed in the metadata database; the Web service hands it to
the backup process in its environment and Strand passes it to the tool as
`MYSQL_PWD`/`PGPASSWORD` or in a 0600 option file in a private temp directory.
(Older jobs and connections that name a variable of the Web service in
`conn.password_env` / `conn.uri_env` keep working; the console no longer creates
them and shows them as legacy.)
Names reserved for Strand's own secrets (`STRAND_WEB_*`, `STRAND_BACKUP_S3_*`,
`STRAND_BACKUP_REPO_*`, `STRAND_BACKUP_ENCRYPTION*`, `STRAND_BACKUP_NOTIFY*`,
`AWS_*`) are refused. A failing tool aborts the upload and marks the record
`failed`. Each stored payload is read back (and decrypted/decompressed with its
plaintext SHA-256 checked) before the record becomes `complete`; set
`STRAND_BACKUP_VERIFY_READBACK=false` to skip.

**Tool preflight.** A job whose native tool is missing never runs: the Web
refuses to create/save an *enabled* job, to enable/resume it and to Run it now
(HTTP 409, code `tool_missing`, naming the tool), does not start its scheduler at
boot or in the reconcile loop, and refuses a restore whose restore tool
(`mysql`, plus `mysqlbinlog` for binlog replay, `pg_restore`, `mongorestore`) is
missing; a job with test restore also needs the restore tool. A disabled job can
still be saved (with a warning) to prepare it before installing the tools.
`strand backup schedule --job-id` checks the tool before it records a run or a
catalog record, so a missing tool leaves no failed restore point. The job list
and job page show a "tool missing" badge and disable Run/Resume; the wizard
shows found/version or missing for the chosen method. Admins see every tool
(found/missing, path, version, methods) on **Backup resources > Backup tools**
(`/backups/tools`, `GET /api/v1/backup-tools`). Fix: install the tool in the
Strand Web host/image or set `STRAND_BACKUP_TOOLS_DIR`. The log methods
(binlog, WAL, oplog) and `pg_basebackup` restores need no tool.

```sh
strand backup schedule --job-id bjob-...        # scheduler of a built-in job (started by the Web)
strand backup schedule --job-id bjob-... --once # one run ("Run now")
```

### Native tools

Built-in methods and restores run the database vendors' own tools. A job whose
tool cannot be found fails with `tool_missing: native tool <name> not found`.

**Container image.** The Web image (`Dockerfile`) ships every tool, from the
vendors' official distributions, pinned by `ARG` (apt repositories verified
with pinned signing-key fingerprints, downloads with pinned SHA-256; the MySQL
tarballs also with Oracle's GPG signature):

| Method / operation | Tools | In the image | Servers |
|---|---|---|---|
| `mysqldump` backup | `mysqldump` | Oracle MySQL 8.4.13 (LTS) | MySQL 5.7, 8.2+ incl. 8.4 (`--source-data=2`) |
| `mysqldump` backup, MySQL 8.0/8.1 server | `mysqldump-8.0` | Oracle MySQL 8.0.46 (`mysqldump` only) | MySQL 8.0/8.1 (picked automatically, see below) |
| `mariadb-dump` backup | `mariadb-dump` | MariaDB 12.3.4 (LTS) | MariaDB 10.6, 10.11, 11.4 tested (`--master-data=2 --gtid`) |
| restore of `mysqldump`/`mariadb-dump`, test restores | `mysql` | Oracle MySQL 8.4.13 | MySQL 5.7-8.4, MariaDB (MariaDB dumps load with it, tested on 10.6-11.4) |
| MySQL point-in-time restore | `mysqlbinlog` | Oracle MySQL 8.4.13 | MySQL 5.7/8.0/8.4 binlogs |
| `mydumper` backup (restore: `myloader` by hand) | `mydumper`, `myloader` | mydumper 1.0.19-1 (built against MySQL 8.4 client) | MySQL 5.7/8.0/8.4, MariaDB |
| `pg_dump` backup, restore, test restores | `pg_dump`, `pg_restore`, `psql` | PostgreSQL 19.0 (PGDG) | servers 17+ (dump and restore target) |
| `pg_dump` backup / restore, PostgreSQL <= 16 | `pg_dump-16`, `pg_restore-16` | PostgreSQL 16.16 (PGDG) | servers 13-16 tested (picked automatically, see below) |
| `pg_basebackup` backup | `pg_basebackup` | PostgreSQL 19.0 (PGDG) | 13-18 tested (`-Ft -X fetch`, needs a `replication` pg_hba entry) |
| `mongodump` backup, restore | `mongodump`, `mongorestore` | MongoDB Database Tools 100.20.1 | MongoDB 4.4-8.x (7.0 and 8.0 tested) |

Both architectures (amd64, arm64) get the same versions. On arm64 Oracle
publishes no `.deb` and no "minimal" tarball: the build downloads the full
generic aarch64 tarballs (about 850 MB each, only the client binaries are
kept), and MongoDB tools come from the `ubuntu2204-arm64` package (static Go
binaries) because there is no Debian 12 arm64 build.

Why two MySQL clients: every Oracle `mysqldump` from 8.2 on (8.4, 9.x) asks an
8.x server for its binlog position with `SHOW BINARY LOG STATUS`, which exists
only from MySQL 8.2, so `--source-data` fails on MySQL 8.0/8.1 (error 1064);
the 8.0 client cannot dump 8.4 (it sends `SHOW MASTER STATUS`, removed in 8.4).
For a `mysqldump` job Strand asks the server for `VERSION()` and uses
`mysqldump-8.0` when the server is MySQL 8.0/8.1 and the default `mysqldump` is
8.2 or newer (`mysqldump: MySQL 8.0.x server, using mysqldump-8.0` in the job
log); without a `mysqldump-8.0` such a job fails with an error saying so.
MySQL 5.7 is dumped by the 8.4 client (it sends `SHOW MASTER STATUS` there and
skips column statistics). Oracle's and MariaDB's clients cannot be installed
together as packages (`mariadb-client` conflicts with `mysql-community-client`),
so the image installs MariaDB's packages (without `mariadb-client-compat`: no
MariaDB binaries named `mysql*`) and puts Oracle's binaries in `/opt/mysql`
and `/opt/mysql-8.0`, linked from `/usr/local/bin`; PostgreSQL's binaries are
linked from `/usr/lib/postgresql/19/bin` (bypassing `pg_wrapper`), the 16
client's as `pg_dump-16`/`pg_restore-16`.

Why two PostgreSQL clients: `pg_restore` 17 or newer always sends
`SET transaction_timeout = 0`, which PostgreSQL 16 and older reject, and
restores run with `--exit-on-error`; `pg_restore` 16 cannot read archives
written by `pg_dump` 17+ (archive format 1.16). So for a PostgreSQL <= 16
source Strand dumps with `pg_dump-16` (job log: `pg_dump: PostgreSQL 16.x
server, using pg_dump-16`), and for a restore or test restore into a
PostgreSQL <= 16 server it reads the target's `server_version` and uses
`pg_restore-16` (`restore: PostgreSQL 15.x target, using pg_restore-16`).
`pg_restore-<N>` / `pg_dump-<N>` for N from the server's major to 16 are
accepted too (exact major first). Without `pg_dump-16` the backup still runs,
with a warning that it will only restore into PostgreSQL 17+; restoring such
an archive (or any `pg_dump` 17+ archive) into PostgreSQL <= 16 fails with an
error saying so, and without `pg_restore-16` with an error naming the tool.
`runner.OptionalTools()` lists these versioned tools for status pages.

Known limits:

- Backups taken before this change from PostgreSQL <= 16 servers by
  `pg_dump` 17+ restore only into PostgreSQL 17 or newer (or by hand:
  `strand backup fetch`, then `pg_restore` 19 without `--exit-on-error`).
- The MariaDB 11.4+ client requires TLS when it cannot rule out an insecure
  login. Jobs always pass the port, which works against servers without TLS;
  `mariadb` by hand against a MariaDB server without TLS needs `--skip-ssl`.

Check an image with `make image-tools-test IMAGE=<image>` (every tool found
on `PATH`, root-owned, not group/world-writable, `--version` runs as user
`strand`) and `make image-tools-smoke IMAGE=<image>` (dumps and restores against
throw-away MySQL 8.4/8.0/5.7, MariaDB, PostgreSQL 15-18, MongoDB containers, then
`TestPGDumpRestoreOlderServers`: Strand's own backup and restore of PostgreSQL
15/16 into 15/16/17 with the image's tools).

**Packages (`.deb`/`.rpm`).** The packages do not depend on the tools (CDC
pipelines need none); they recommend the distribution's PostgreSQL client and
suggest the others. Install the vendors' clients (repo.mysql.com,
apt.postgresql.org / yum.postgresql.org, MariaDB, MongoDB, mydumper releases):
the Debian `default-mysql-client` is MariaDB's and is not suitable for the
`mysqldump` method on MySQL 8, and `pg_dump` must be at least the server's
major version. To pin exactly which binaries the Web service runs, put them
(or symlinks) in one directory and set `STRAND_BACKUP_TOOLS_DIR`; Strand then
looks only there (no `PATH` fallback), and each file must be executable and not
group/world-writable:

```sh
sudo install -d -m 0755 -o root -g root /opt/strand-tools
sudo ln -s /usr/bin/mysqldump /usr/bin/mysql /usr/bin/mysqlbinlog /opt/strand-tools/
sudo ln -s /usr/lib/postgresql/17/bin/pg_dump /usr/lib/postgresql/17/bin/pg_restore \
  /usr/lib/postgresql/17/bin/pg_basebackup /usr/lib/postgresql/17/bin/psql /opt/strand-tools/
# MySQL 8.0 servers with a mysqldump >= 8.2: also provide MySQL 8.0's mysqldump as mysqldump-8.0;
# PostgreSQL <= 16 servers with a client >= 17: also /usr/lib/postgresql/16/bin/pg_dump as pg_dump-16
# and .../pg_restore as pg_restore-16
echo 'STRAND_BACKUP_TOOLS_DIR=/opt/strand-tools' | sudo tee -a /etc/strand/web.env
sudo systemctl restart strand-web
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

Repositories are defined in the console (Web: **Backup resources → Repositories**,
`/backups/repositories`; API `/api/v1/backup-repositories`). The form has:

* **General**: name, storage type (S3-compatible), provider (Amazon S3, MinIO,
  Ceph RGW, Wasabi, other; it only fills the endpoint format).
* **Connection**: endpoint URL (https, without the bucket), region, and
  "allow plain HTTP to a local endpoint" (loopback only, for testing).
* **Credentials**: access key ID and secret access key.
* **Storage location**: bucket and path (prefix); the page previews
  `s3://bucket/path/<source>/...`.
* **Retention (automatic deletion)**: delete backups older than N days, always
  keep the newest N, keep every backup of the last N days, daily / weekly /
  monthly / yearly backups to keep. It applies to backups in the repository that
  belong to no job (manual and imported ones); scheduled jobs use the retention of
  their policy.
* **Immutability**: Object Lock mode (GOVERNANCE / COMPLIANCE) and lock period.
* **Test connection** runs the checklist (list, put, get and compare, delete one
  small object, Object Lock status) on the settings of the form before they are
  saved (`POST /api/v1/backup-repositories/test`, admin; nothing is stored).

The keys are sealed with the Web secret key (`STRAND_WEB_SECRET_KEY`, AES-256-GCM,
the repository id as additional data) and are write-only: no response, page, audit
entry, log or metric carries them (only `access_key_set`/`secret_key_set`). The Web
service opens them only to put them in the **environment** of the
`strand backup ...` processes it starts (schedulers, log archivers, verify, prune,
copy, restore), under a prefix derived from the repository id; they never appear in
argv. Editing a repository's credentials restarts the schedulers of the enabled jobs
that use it (their environment is fixed when they start): a run in progress of
those jobs is cancelled, and their next due time is kept.

Repositories read from environment variables of the Web service
(`STRAND_BACKUP_REPO_<NAME>_ENDPOINT`, `_REGION`, `_ACCESS_KEY`, `_SECRET_KEY`) are a
legacy mode: existing ones keep working and are labelled legacy; editing one in the
console and entering the endpoint and keys converts it. New ones cannot be created.
Jobs created before repositories existed keep using `STRAND_BACKUP_S3_*` and their
own bucket; the job wizard always picks a repository.

`kind` is `s3` (AWS, MinIO, Ceph); `azure` and `gcs` are reserved in the schema
and rejected until implemented. A repository with `lock_mode: COMPLIANCE` is shown
with an **immutable** badge. The list shows, per repository, the bytes and number
of backups in the catalog (non-expired records, log segments included) and the
copies it holds; the detail page shows the space per source and the jobs and
policies that use it. Deleting a repository is refused (409) while it holds
non-expired backups or while a job or policy names it.

**Test** (`POST /api/v1/backup-repositories/{id}/test`, operator): a checklist of
`list` the test prefix, `put` a 60-byte object under `<prefix>/.strand-repo-test/`,
`get` it back and compare, `delete` it, and `object_lock` (does the bucket have
Object Lock? failed when the repository requires GOVERNANCE/COMPLIANCE but the
bucket has none: Object Lock can only be enabled when a bucket is created), each
with its latency. A step that fails makes the later dependent ones `skipped`. The
outcome is stored on the repository (`last_test_*`). The test object is written
without retention; a bucket default retention would block its deletion and is
reported on the `delete` step.

With `lock_mode` GOVERNANCE or COMPLIANCE and `lock_days`, every object is
written with S3 Object Lock retention (the bucket must be created with Object
Lock; Strand sends Content-MD5 as required) and the catalog records the version
id and retain-until date as soon as the payload upload succeeds. A record that
fails after that point (a later step of the run) keeps them, so a delete is
refused while the lock holds and afterwards removes that exact version, not just
a delete marker. Failed records written by older versions have no version id and
are not repaired. Lock checks compare `retain_until` with the database clock, not
the clock of the Web host.

When a large upload fails, the object store's own error (for example 403
`AccessDenied`, a missing bucket) is reported and classified `repo_unreachable`,
not hidden behind a pipe error. When finalizing a backup fails after its
`manifest.json` was uploaded as complete, that manifest version is deleted; if
Object Lock refuses the delete, a manifest marked `failed` is written over it, so
a repository re-index never trusts it. `replicate_to` copies each new backup to a second
repository (`strand backup copy --id ... --to-repository ...`): bytes are copied
unchanged (still encrypted), verified on download and after upload, and
cataloged as a new record with `copy_of`.

### Retention

GFS policy per job (and a default per repository for backups without a job):
`keep_last`, `keep_hourly`, `keep_daily`, `keep_weekly`, `keep_monthly`,
`keep_yearly`, `keep_within_days`, `max_age_days`, plus `lock_days`. A backup is
kept when any rule selects it; `max_age_days` is a hard cap. Guarantees: the
newest full backup is never expired; a kept incremental keeps its whole chain;
log segments are kept from the oldest kept base onward.

Retention is planned **per lineage**: originals among originals, and copies made
by `replicate_to` (records with `copy_of`) among the copies in the same target
repository and bucket. A copy is cataloged after its original, so planning them
together would let a fresh copy take a keep slot and expire the original it was
made from. Every guarantee above holds per lineage.

A restore point whose last check failed (`check_status = failed`) never fills a
keep slot (`keep_last`, hourly/daily/..., `keep_within_days`) and is not the
"newest full" the safety net protects. It is kept only while it is newer than the
oldest kept good restore point, then expires. When a lineage has no good restore
point at all, failed-check ones are planned as ordinary restore points rather
than all deleted.

**Pins.** A restore, a test restore and a verification pin the records they read
(base, incremental chain and log segments) in `backup_pins` (migration 024). A
prune, in any process or instance, skips a pinned record (`skipped` in the prune
result, no `backup.expire` audit) and retries it on the next run. A pin is renewed
every 2.5 minutes and lapses 10 minutes after its holder stops renewing it
(database clock), so a crashed restore cannot block retention for long. When
renewals keep failing (catalog unreachable) for over half that time, the
restore, verification or test restore stops with "restore pin could not be
renewed" before the pin can lapse under it. A restore
that starts after retention claimed one of its records is refused ("restore point
is no longer available"). Retention claims each
object in the catalog before touching storage. A legal hold or an unexpired
Object Lock blocks the claim. A record in `deleting` can no longer be restored
or put on hold, and a later prune retries interrupted storage deletion.

```sh
strand backup prune --job-id bjob-... --dry-run   # decision and reason per backup
strand backup prune --job-id bjob-...             # delete objects (versions), then mark expired
```

The Web previews a policy (also an unsaved one) and the active instance applies
each job's policy hourly. Object Lock retention is checked against the database
clock. Objects are deleted before the record is marked
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

- MySQL: the dump is loaded into the new database (its `GTID_PURGED` and
  `SQL_LOG_BIN` statements are commented out, so the whole restore is written to
  the target's binary log and replicas of the target stay in step; the loaded
  data gets new GTIDs there, and the restore user needs no
  `SYSTEM_VARIABLES_ADMIN`), then `mysqlbinlog --exclude-gtids=<dump GTIDs>
  --skip-gtids --rewrite-db=<src>-><new> --stop-datetime=<target>` is replayed
  into it. Requires a single-database dump and a **whole-second** target time:
  `--stop-datetime` has one-second resolution, so a fractional target is refused
  with the two whole seconds around it suggested.

  Before it creates the target database Strand scans the binlogs, and during the
  replay a filter between `mysqlbinlog` and `mysql` passes only statements it has
  checked. The restore stops, naming the binlog position, when a statement:
  - names the source database or another database explicitly
    (`UPDATE shop.orders ...`): replayed into the new database it would change
    that database itself. Restore to a separate server under the same database
    name instead;
  - is `CREATE`/`ALTER`/`DROP DATABASE` on the source database;
  - changes the source database while the session's default database was another
    one (`--database` would skip it and the restore would silently diverge);
  - was seen by the scan but not replayed.

  Pick a target before that statement, restore without a target time, or replay
  the binlogs by hand. The check can refuse more than needed, for example when a
  table alias equals a database name (`UPDATE t AS shop SET shop.col = 1`).
- PostgreSQL: the base backup is extracted, the needed WAL is staged in
  `strand_wal/`, and `restore_command`, `recovery_target_time` and
  `recovery_target_action=promote` are set with `recovery.signal`; start
  PostgreSQL on the directory to recover and promote. Needs PostgreSQL 12 or
  newer: a base backup of 11 or older with a target time is refused (restore it
  without one and write `recovery.conf` by hand). PostgreSQL 12 ends recovery and
  promotes without an error when WAL runs out before the target, so Strand reads
  the staged WAL and refuses a target it does not reach; recovering to the newest
  point on 12 needs a later transaction already archived.
- MongoDB: restore into a new deployment with one `mongorestore --oplogReplay`.
  Strand appends verified archive entries strictly after the embedded base
  timestamp and before the chosen UTC second. Single-database dumps restore under a
  new name (`--nsFrom/--nsTo`).

`--id <backup>` restores one backup as is. The Web restore wizard (Backups page)
requires typing the target name to confirm and opens the job log.

A failed or cancelled restore removes what it wrote: MySQL and PostgreSQL drop
the database the restore created; MongoDB drops the target database (verified
absent before the restore) or, for a whole-deployment restore, every non-system
database that was not in the verified-empty deployment and that the restored
archive contains (its namespace blocks and every `ns` in its oplog, including
`applyOps` and `renameCollection`). A database another client created on the
target during the restore is not in the archive: it is left in place and named
in the error. Admin users the restore created are not removed. A cleanup failure is part of the returned error. On PostgreSQL
12 and older, which lack `DROP DATABASE ... WITH (FORCE)`, Strand refuses new
connections to the database and terminates its other sessions first; the user
needs the right to terminate them (database owner or `pg_signal_backend`). A
partial MySQL restore (`tables`) matches `db.table`, so a table of the same name
in another database is not picked up.

### Verification and test restores

```sh
strand backup verify --id bkp-...                  # HEAD + full re-read: stored and plaintext SHA-256
strand backup verify --job-id bjob-... --sample 10 # 10% of payloads fully re-read (newest always), the rest HEAD-checked,
                                                    # incremental chains complete, log continuity
strand backup verify --job-id bjob-... --test-restore
```

`--test-restore` (job option `test_restore` + a `scratch` connection) restores
the newest mysqldump/pg_dump backup into a scratch database
`strand_verify_<unix time>_<12 random hex>`, records per-table row counts, and
drops it. Only a database this run created is dropped: two runs at once, or an
existing database of the same name, are never touched. Sampled verification
always fully re-reads the newest **base** backup (not the newest log segment), and
the records a verification or test restore reads are pinned against retention. Results are
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
job list); editing one re-materializes its jobs. A run in progress is not
cancelled: it finishes with the settings it started with, and the scheduler
restarts with the new ones after it (at once when idle).

The native tools (mysqldump, pg_dump, mongodump, mysql, pg_restore, ...) run with
every `STRAND_*` variable removed from their environment (repository and
encryption keys, the source, scratch and restore-target secrets) and without the
variables a legacy connection names in `password_env`/`uri_env`; each tool gets
its own secret only, through `MYSQL_PWD`/`PGPASSWORD` or a temporary option file.
Logged tool output masks the real secrets (passwords, tokens, the password of a
credentialed URI), also when one is split across two writes, and no longer masks
ordinary values such as `PGSSLMODE` or paths.

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
job that follows it; like a connection edit, a run in progress finishes with its
old settings and the scheduler picks up the new ones after it; `PUT` returns `affects_jobs` and
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
retry back-off cap.

The next due time always follows the schedule: when a run starts, finishes or is
recovered after a crash, the next run is the schedule's next occurrence (a daily
02:00 job runs at 02:00, not drifting with the finish time). A cancelled run keeps
the due time it already had. A failed run with retries left is retried after its
back-off, but never later than the next regular run. Editing a job keeps its due
time unless the schedule itself changed; then the new schedule decides, counted
from the last run for interval jobs (due at once when that is already past) and
from now for the other kinds. Enabling a disabled job starts it like a new one.
Editing the job itself still cancels its run in progress. A run recorded as
`running` whose process is gone (no holder of the job's execution lock on any
instance) is marked interrupted when the Web starts or takes over and on every
maintenance tick, also for a job disabled after the crash. `POST /api/v1/backup-schedules/preview` returns the next runs
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

Two sources, both delivered by the Web service (active instance):

1. **From environment** (read-only channel `env`): the destinations are secrets,
   so they can be set in the environment of the Web service:

```sh
STRAND_BACKUP_NOTIFY_WEBHOOK=https://hooks.example.com/strand      # JSON event
STRAND_BACKUP_NOTIFY_SLACK_WEBHOOK=https://hooks.slack.com/...     # {"text": ...} (Slack/Mattermost)
STRAND_BACKUP_NOTIFY_TEAMS_WEBHOOK=https://...                     # Microsoft Teams (MessageCard)
STRAND_BACKUP_NOTIFY_SMTP_ADDR=smtp.example.com:587                # STARTTLS required except loopback
STRAND_BACKUP_NOTIFY_SMTP_FROM=strand@example.com
STRAND_BACKUP_NOTIFY_SMTP_TO=dba@example.com,ops@example.com
STRAND_BACKUP_NOTIFY_SMTP_USER=... / STRAND_BACKUP_NOTIFY_SMTP_PASSWORD=...
STRAND_BACKUP_NOTIFY_ON=failure,missed,verify_failed,rpo_breach,repo_unreachable   # default; add success or use all
```

   It appears on the Notifications page as "From environment" with the kinds of
   destination and the events it follows, never the destinations; it cannot be
   edited, deleted or used as a rule target.

2. **Stored channels** (migration 019; Web: **Notifications**,
   `/backups/notifications`, admin; API `/api/v1/backup-notify-channels`):
   `webhook`, `slack`, `teams` or `email` (SMTP server, from, recipients, optional
   user/password). The destination URL or SMTP password is sealed like repository
   keys and write-only. A stored channel receives nothing until a **rule**
   (`/api/v1/backup-notify-rules`) routes an event to it: *event* (`failure`,
   `missed`, `verify_failed`, `success`, `rpo_breach`, `repo_unreachable`) x *scope*
   (all jobs, one policy, one job) -> channel. **Send test**
   (`POST .../{id}/test`, admin; also for `env`) delivers a `test` event and records
   the result on the channel; a failure reports the HTTP status without the URL.
   Destinations are contacted from the Web service, so an admin can point a channel
   at any URL the service can reach: keep channel administration to trusted admins.

Events: run `failure`/`success`, `missed` schedule (due more than max(5 min,
10% of the interval) ago without a run, or a run outside its `window_minutes`),
`verify_failed`, `rpo_breach` (see below) and `repo_unreachable` (a run that failed
with the normalized error code `repo_unreachable`; it replaces `failure` for that
run). Each event is sent once across instances and every delivery is audited
(`backup.notify`, channel id, no destination). Email subjects are one line: a
multi-line error (for example of `verify_failed`) goes to the body only.

### Metrics and alerts

`GET /metrics` (Web session or API token) exposes Prometheus gauges:
`strand_backup_last_success_timestamp_seconds{source,engine}`,
`strand_backup_last_size_bytes`, `strand_backup_last_duration_seconds`,
`strand_backup_stored_bytes`, `strand_backup_recovery_window_end_timestamp_seconds`,
`strand_backup_job_consecutive_failures{job,name}`,
`strand_backup_job_last_run_success`, `strand_backup_recent_check_failures`,
`strand_backup_rpo_seconds{source}`, `strand_backup_rpo_target_seconds{source}`,
`strand_backup_rpo_breach{source}`.

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

### Encryption keys page (admin)

`/backups/keys` and `GET /api/v1/backup-keys` list the key ids from
`STRAND_BACKUP_ENCRYPTION_KEYS` (or the file/single-key variants) with a short
non-reversible fingerprint, which key is the active **write** key (the first one
listed), and how many backups (and bytes) were written with each. A catalog backup
whose `key_id` is no longer configured is reported as a warning (with sample
backup ids): it cannot be restored until the key is added back. Unencrypted
backups are counted separately. Key material is never returned or rendered, and a
malformed key variable is reported without echoing it. With no key configured a
notice says new backups cannot be encrypted.

### RPO/SLA and compliance

Each policy has `rpo_minutes` (the target) and `rto_minutes` (informational). For
every source the **current RPO** is `now - latest restore point`: when the job
archives logs (PITR), the end of the newest unbroken run of log segments that
reaches back over a complete base backup of the same lineage (a newer base backup
wins); otherwise the point of the last complete backup (its consistent point, i.e.
`start_time`, else creation time). Segments after a gap, or segments without any
base backup, restore nothing and earn no RPO credit. A PITR source with no such
run falls back to its last backup; a source with no restore point has no RPO (`-1`) and is never
flagged, so a new job does not alarm before its first run. A source over its target
is a **breach**:

* `GET /api/v1/backup-rpo` and the metrics `strand_backup_rpo_seconds{source}`,
  `strand_backup_rpo_target_seconds{source}`, `strand_backup_rpo_breach{source}`;
* an `rpo_breach` notification (rules or the environment channel), once per breach
  episode and again every 6 h while it lasts (state in `backup_rpo_breaches`,
  migration 020, so instances do not repeat it); a paused (disabled) job is reported
  but does not page. The episode ends when the source is back under its target.

**Compliance report**: `GET /api/v1/backup-reports/compliance?from=&to=` and the
CSV `GET /api/v1/backup-reports/compliance.csv?from=&to=` (default: last 30 days,
at most 366). Per source: policy, RPO target/RTO, current RPO and breach, runs /
succeeded / failed in the window (failed = failed, partial and missed; cancelled
and running runs are not counted), success rate in the window and over the 7 and 30
days before `to`, the last restore point and the last test-restore (time and
status); the summary adds the overall 7/30-day success rate, the number of sources
over their RPO and the newest test-restore. The CSV starts with a UTF-8 BOM and uses
CRLF so Excel opens it correctly; text cells starting with `=`, `+`, `-` or `@`
(or a tab/CR) are prefixed with `'` so a hostile job or source name cannot become a
spreadsheet formula. Each export is audited (`backup.report_export`).

### Audit and the Activity feed

Every backup write is recorded in the audit log **before** it takes effect (the
request is refused when the audit cannot be written), with the acting user:
`backup_job.*` (create, update, delete, start, stop, run, prune, verify),
`backup_policy.*`, `backup_connection.*` (create, update, delete, test),
`backup_repository.*` (create, update, delete, test), `backup_notify_channel.*`,
`backup_notify_rule.*`, `backup_run.cancel`, `backup.restore`, `backup.legal_hold`,
`backup.verify`, `backup.copy`, `backup.report_export`, and `backup.notify` for
deliveries. Updates carry `detail.changes`, `{field: {from, to}}`; secret fields are
never recorded, only `set`, `cleared` or `changed`, and detail maps pass through a
mask for secret-named keys as a second line of defence.

`/backups/activity` and `GET /api/v1/backup-activity` (admin) show the audit
entries whose action starts with `backup`, newest first, cursor-paged
(`next_cursor` = id of the last row), filterable by `user`, `action` prefix and
time.

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
Migration 019 adds the stored-credential columns of `backup_repositories`
(`kind`, `secret_mode`, `endpoint`, `region`, `allow_insecure_local`, sealed
`access_key_sealed`/`secret_key_sealed`, last test and its checklist) and the tables
`backup_notify_channels` and `backup_notify_rules`; migration 020 adds
`backup_rpo_breaches`. Migration 024 adds `backup_pins` (`backup_id`, `holder`,
`expires_at` on the database clock; deleted with the backup): the restore pins of
the Retention section.
Log segments are self-describing (their catalog row is the manifest); base
backups get a `manifest.json` sidecar containing the catalog record so a
repository can be re-indexed without the database.

### Web UI and Route Architecture (P1)

Strand Web splits backup management across dedicated, fast-loading routes under the "Data protection" sidebar group
(Backup dashboard, Backup jobs, Restore points, Restore, Backup resources, Reports). Backup resources groups Connections,
Repositories, Policies, Encryption keys and Notifications; Reports groups Reports and Activity; each page of a group shows the
group's pages as a tab strip (admin-only pages are listed to admins only).
- `/backups` (Backup dashboard): Stat tiles (protected sources, 24h success rate, failing jobs, storage used, test-restore status), standby/repo/key banners, needs-attention list, and 14-day stacked activity chart rendered server-side as CSP-safe SVG.
- `/backups/jobs`: Filterbar (query, engine, status, repository) synced to the URL, bulk action bar (Run, Pause, Resume, Delete), last-10-runs indicators, and next run countdowns.
- `/backups/jobs/{id}`: Detailed job view with header actions and tabs: Overview (30-day run sparkline), Runs (cursor-paged run history with live log stream and running execution cancellation), Restore points, Settings (read-only with edit mode), and Activity (audit entries filtered to this job).
- `/backups/catalog` and `/backups/catalog/{id}`: Paged restore point browser and backup detail inspect view with position coordinates and verification history.
- `/backups/restore`: Restore wizard. A restore into a database goes to a saved connection (its password stored encrypted,
  its TLS and SSH tunnel settings applied); a physical restore goes to a new directory.

### Connections, policies and the job wizard (P2/P3 UI)

- `/backups/connections` and `/backups/connections/{id}`: table with a filter bar (name/host, engine, last test status) and, for admins, a
  `New connection` drawer (`dialog.drawer`) whose fields follow the engine (default port, connection URI for MongoDB); the password is
  entered there and stored encrypted (there is no environment-variable option). `Test connection` shows a checklist (connect, version, privileges, log_bin/GTID or wal_level or replica set,
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
  compression. Step 4 sets verification, test restore (mysqldump, mariadb-dump, pg_dump only; the scratch server is a saved connection of the
  same engine, `scratch_connection_id`, whose stored password reaches only the verify process in `STRAND_BACKUP_SCRATCH_PASSWORD`), retries and shows the Review before
  `Create and run first backup` / `Create only`. Fields that differ from the selected policy carry an `overridden` badge and are sent as
  overrides; untouched fields are not sent, so the job keeps following the policy.
- The jobs list and job page show the human schedule, the policy and connection, `missed` runs (filter `status=missed`) and the job
  warnings (for example a policy PITR request that cannot apply to the job). Built-in jobs with an inline connection offer
  `Convert to connection` (`POST /api/v1/backup-jobs/{id}/convert-connection`, admin, audited as `backup_job.convert_connection`): the
  inline host/user/password-variable become an environment-mode connection (reused when an identical one exists) and the job points to it.

### Restore point statuses and actions

A catalog record moves `queued` -> `uploading` -> `verifying` -> `complete`; any step can end in `failed`, and
retention turns `complete` into `expired` after deleting its objects. Only a `complete` record is a restore point:

| Status | Catalog list / detail actions | Restore, verify, copy, legal hold | Delete |
| --- | --- | --- | --- |
| `complete` | Restore, Verify, Copy to..., Set/Release legal hold | allowed | no: retention (prune) removes it, honoring keep counts, incremental chains, legal hold and Object Lock |
| `failed` | View error (the failed run's error code and message, links to the job's runs and the process logs), Delete | 409 (API) / exit 3 (CLI) | yes (operator+) |
| `expired` | Delete (removes the record; its objects are already gone) | 409 / exit 3 | yes (operator+) |
| `queued`, `uploading`, `verifying` | none (in-progress badge, Detail only) | 409 / exit 3 | only when its writer is gone (see below) |

`DELETE /api/v1/backups/{id}` (operator and admin; audited `backup.delete`) runs `strand backup delete --id <id>` as a job:
the process gets the repository credentials in its environment (like verify, copy and prune; the Web holds no S3 client),
aborts an unfinished multipart upload of the payload, deletes the payload and the `manifest.json` sidecar, then the catalog
record and its checks. A crash after deleting objects leaves a record without objects, and deleting it
again finishes the job. Refused (409, nothing deleted): a complete record, legal hold (release it first; a hold can be released
on any record but set only on a complete one), an Object Lock retention (`retain_until`) still in the future, and a record whose
writer may still run: its job has a run in progress, its job's log archiver or its copy process is running, or (no job and no
copy to check, e.g. `strand backup push`) it started less than 24 hours ago. An abandoned in-progress record is marked
`failed` by the process before it is deleted (`--abandoned`). From a shell: `strand backup delete --id <id> [--abandoned] [--json]`.

The restore wizard lists complete restore points only; `/backups/restore?backup=<id>` with a record that is not complete shows
the reason and does not preselect it. Timeline dots of records that are not complete open the record, not the wizard. The
status filter of `/backups/catalog` covers every status (`status=failed` lists the records to clean up).

### PITR Constraints (P0)

Validation rules enforced by `backup.ValidateJob` and UI:
- **MySQL**: Method `mysqldump` with a GTID-capable connection and exactly one database. MariaDB / `mariadb-dump` is rejected with `"MariaDB does not support binlog archiving"`. Multi-database configurations are rejected with `"PITR requires exactly one database"`.
- **PostgreSQL**: Only `pg_basebackup`. `pg_dump` is rejected with `"PITR needs pg_basebackup as the base backup"`.
- **MongoDB**: a whole-deployment `mongodump --oplog` with archiving enabled
  records its embedded, majority-committed oplog boundary. The archive
  account needs `replSetGetConfig` and `replSetGetStatus` privileges. PITR
  uses one replica set and a whole-second UTC cutoff. Take a new base and
  start a new archive job if legacy records lack replica-set provenance.
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
- A MySQL PITR target must be at least the first whole second after the base dump completed. An earlier target can use an older base with continuous binlog coverage. Dumps without a recorded completion time remain available for standalone restore.
- New MySQL and PostgreSQL archives record the server identity. A PITR base
  and its logs must come from the same job and server. Legacy records without
  that evidence remain usable as standalone backups. Start a new archive job
  and take a new base when upgrading an unverified archive.
- PostgreSQL PITR follows one timeline. The CLI prepares a directory from
  contiguous WAL and marks the target as pending replay verification. Start
  PostgreSQL and verify it reaches the requested time. The console does not
  advertise a time window from WAL ingest timestamps.
- MongoDB restores require a new database (`--target-database`) or a dedicated
  empty deployment (`--target-deployment`). Confirm the endpoint and
  namespace with `--confirm-target mongodb://host:27017/new_db` or
  `--confirm-target 'mongodb://host:27017/*'`.
- Native tools that cannot enforce a saved TLS policy fail before using it.
  For MySQL or MongoDB hostname verification, run the native tool over a
  connection that preserves the server hostname. PostgreSQL supports hostname
  verification through the local SSH forward.
- No deduplication across backups (every run is a full stream; incremental chain
  creation is removed).
- Test restores cover mysqldump/mariadb-dump/pg_dump; physical and MongoDB test
  restores are not automated.
- Job output files are capped at 32 MiB each; output after the cap is discarded
  and a truncation marker is written.
- MySQL PITR replays only whole seconds and stops at binlog statements that would
  write outside the new database (see Point-in-time recovery); there is no opt-in
  to replay them anyway.
- PostgreSQL PITR needs PostgreSQL 12 or newer.
- `strand backup fetch` and `copy` do not pin the records they read; only
  restore, test restore and verification do.
- The RPO shown on a job page is computed from its 50 newest records; the RPO API,
  metrics and report use the whole catalog.
