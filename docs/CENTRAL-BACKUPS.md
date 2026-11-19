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
verification time, expiration, and legal-hold state. It checks the uploaded
object size with S3 HEAD and registers a record as complete only after the
artifact and manifest sidecar have uploaded. SHA-256 is computed while reading
the source stream and persisted in the catalog; every part is sent with its
SigV4 payload hash, so S3 rejects a part corrupted in transit. The uploaded
object is not downloaded a second time; `strand backup fetch` verifies it
against the catalog digest when it is used. The backup ID is printed before
the upload starts: a failed or interrupted attempt can be retried with the
same `--id` and identical backup metadata; a per-backup lock in the metadata
database prevents concurrent uploaders from replacing each other's objects.

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

## Current scope

This provides centralized upload, catalog, verified download, and MCP discovery
for existing backup artifacts. It does not schedule native backup jobs, restore
into databases, enforce `expires_at` (nothing moves records to `expired` or
deletes objects yet), or delete S3 data; use bucket lifecycle rules for
retention until a managed retention job exists. Backups are not encrypted by
Strand: use bucket default encryption (SSE-S3/SSE-KMS) and TLS. Failed
attempts leave their record in `failed` and may leave a partial object under
the backup's key until retried.
