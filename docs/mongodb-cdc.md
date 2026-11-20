# MongoDB → MongoDB CDC: production guide

Strand replicates selected collections of one MongoDB database into a database of
another MongoDB deployment: an initial snapshot, then the source change stream, applied
continuously with the checkpoint committed atomically with the data. It is a normal
Strand pipeline: `strand sync -c <config>` (also `--background`, `stop`, `status`,
`validate`, `plan`, `top`, `pause`, `resume`, `checkpoint show`, `verify`,
`snapshot add`, the systemd unit `strand@<name>`, the Web UI and Prometheus metrics).

## Requirements

| | |
|---|---|
| Versions | MongoDB 4.4, 5.0, 6.0, 7.0, 8.0 (source and target, any combination; tested matrix below) |
| Topology | Replica sets (1 or more members) on both sides. Sharded clusters (mongos) and standalone servers are refused. |
| Driver | MongoDB Go Driver v2.10 (minimum server 4.4). |
| Target | Database `_strand_cdc` is reserved for checkpoints, progress and the lease. One running pipeline per target database (lease). |
| Collections | Ordinary collections. Views and time-series collections must be excluded (validate refuses them). |

## Configuration

```yaml
version: 1
name: orders-replica
source:
  kind: mongodb
  uri: "${SRC_URI}"            # or dsn: mongodb://..., or uri_env: NAME_OF_ENV_VAR
  # user: cdc_src              # optional: credentials outside the URI
  # password_env: SRC_PASSWORD
  database: app
target:
  kind: mongodb
  uri: "${DST_URI}"
  database: app_replica         # default: source.database
tables:
  include: ["app.*"]            # "database.collection" globs
  exclude: ["app.tmp_*"]
bootstrap:
  logical:
    max_parallel: 4             # collections copied concurrently (1..32, default 1)
checkpoint:
  flush_every: 1s               # idle checkpoint interval
mongodb:
  snapshot_batch: 1000          # documents per snapshot transaction (default 1000)
  stream_batch: 2000            # change events per apply transaction (default 2000)
  lease_ttl: 45s                # single-writer lease TTL, target clock (>= 3s)
  overwrite_target: false       # true: the first snapshot may drop existing target collections
  updates: auto                 # auto | delta | lookup (see "Updates")
  indexes: off                  # off | replicate (see "Indexes and collection options")
observability:
  metrics: ":9464"
  admin_socket: /run/strand/orders-replica.sock
resources:
  spool: { path: /var/lib/strand/orders-replica }   # log/pid of --background
```

`bootstrap.logical.chunk_rows` and `target.batch.max_rows` are accepted as fallbacks for
`snapshot_batch` / `stream_batch`. Keys of SQL pipelines that have no meaning here
(`tables.rename`, `tables.mask`, `bootstrap.start_at`, seed modes) are refused; others
(governor, IOPS limits, spool limits, write_mode) are ignored with a warning.
URIs and passwords are never logged (errors are redacted). `strand mongodb sync -c
<config>` is an alias of `strand sync -c <config>`; the flag form
(`strand mongodb sync --pipeline ... --include ...`) remains for scripts.

Commands:

```sh
strand validate -c p.yaml          # topology, version, privileges, oplog window vs snapshot ETA, target state
strand plan -c p.yaml              # per-collection sizes, snapshot ETA, oplog window (exit 3 if ETA > 50% of the window; --force)
strand sync -c p.yaml --background # or systemctl enable --now strand@orders-replica
strand top -c p.yaml               # lag, events/s, snapshot progress
strand pause|resume -c p.yaml      # pause --snapshot: stop copying only
strand checkpoint show -c p.yaml   # resume token + cluster time
strand verify -c p.yaml --mode full [--table app.orders] [--repair fix]
strand verify -c p.yaml --mode sample --rate 5000     # random sample of 5000 documents per collection
strand snapshot add -c p.yaml --table app.orders      # re-copy one collection (= strand mongodb repair --collection orders)
strand stop -c p.yaml
```

## Consistency model

- **Handoff.** Before the first document is read, a change stream is opened on the
  source database and its resume token is saved. The snapshot reads with
  `readConcern: majority`, paging each collection by `_id` (index bound `min()`, so
  `_id`s of mixed BSON types are paged correctly). Every change made during the scan
  is replayed afterwards (snapshot overlap); replay converges to the source.
- **Atomic checkpoints.** Each snapshot batch commits in one target transaction
  together with the collection's progress document; each stream batch commits together
  with its resume token. After a crash (even `kill -9`) the snapshot resumes from the
  last committed `_id` and the stream from the last committed token: nothing is lost,
  nothing is applied twice, documents cannot be duplicated (unique `_id`).
- **Source transactions** commit atomically on the target: the batch size cut-off
  never splits a source transaction (unless it exceeds 20x the batch size).
- **Writes** use `w: majority, j: true`; reads `readConcern: majority`.
  The read preference comes from the URI (default `primary`; `readPreference=secondaryPreferred`
  moves snapshot and lookup reads off the source primary; the change stream follows the
  same preference). Do not lower the target write concern: checkpoints rely on it.
- **Fencing.** One pipeline owns a target database through a lease in
  `_strand_cdc.mongo_sync_locks` (expiry on the target clock). Every target transaction
  also updates one of the owner's fence documents (one per parallel worker); a new
  owner rewrites all fence documents first, so a stale process (GC pause, SIGSTOP, VM
  freeze, network partition) cannot commit anything once taken over and exits with
  "lease lost" (exit 4).
- **Lost history.** If the saved resume point is no longer in the source oplog the
  pipeline fails closed (exit 5, `log_gap`): "source change history is no longer
  available; a fresh snapshot (new pipeline name) is required". It never silently
  starts at the present. An idle pipeline keeps its checkpoint fresh with post-batch
  resume tokens.

## Updates

`mongodb.updates`:

- `delta` — update events carry only their update description
  (`updatedFields`, `removedFields`, `truncatedArrays`); Strand applies it to the
  target's current image of the document (read in one `$in` query per batch, or the
  image already in the batch) and writes the result. Semantics match the server
  exactly, including dotted paths, array indexes, padding with `null`, `$unset`,
  `$pop/$push/$pull/$slice` (reported as truncations or whole arrays), pipeline-style
  updates and field order; this was verified byte for byte against the real
  post-image for 3000 random updates of 24 kinds on every supported version.
  Whenever the description cannot be applied exactly (a field name containing `.` on
  servers without `disambiguatedPaths`, a pre-image that does not match, an `_id`
  type that cannot be canonicalized), Strand reads the current document from the
  source instead (lookup), per document.
- `lookup` — `fullDocument: updateLookup` (the previous behaviour): every update
  event carries the current majority-committed document.
- `auto` (default) — `delta` on 4.4 and 6.1+, `lookup` on 5.0–6.0: from 5.0 a field
  name containing `.` can be created with `$setField`, and before 6.1 the update
  description cannot tell it apart from a nested path inside an existing sub-document.
  Choose `delta` explicitly on 5.0–6.0 only if no field name contains `.`.

Throughput gain of `delta` (backlog apply, 50 000 events on ~1 KiB documents, local
containers): see "Performance".

## Snapshot

Up to `bootstrap.logical.max_parallel` collections are copied at once (largest first),
each in `_id` order, each batch fenced by its own lease slot. A single collection is
copied by one worker (ranges within a collection are not split): plan the window with
the size of the largest collection. Pausing (`strand pause --snapshot`) stops copying
after the current batch.

**Sizing.** Measured copy rates (one worker, ~1 KiB documents, local containers):
~12 000 docs/s single node, ~11 000 docs/s 3-node replica sets (see "Performance").
`validate`/`plan` estimate the snapshot with a conservative 5 000 docs/s per worker.

**Oplog window.** The source oplog must keep every change made from the start of the
snapshot until the stream reads it, plus margin for outages. `validate` fails when the
estimated snapshot exceeds 50% of the source oplog window (projected to the configured
oplog size while it is still filling up). Raise the window with
`replSetResizeOplog` (size) or `storage.oplogMinRetentionHours` (4.4+) before the
first sync of large collections, and keep `strand_retention_headroom_seconds` alerting.

## DDL, indexes and collection options

- `drop`, `dropDatabase`, `rename` are applied. A rename after the snapshot overlap
  renames the target collection in place (any size, instant). A rename that happens
  during the snapshot overlap, or into the selection, re-copies the destination
  collection; that re-copy is checkpointed per batch and resumes after a crash.
  `dropDatabase` invalidates the stream, which is restarted after the invalidate.
- `mongodb.indexes: off` (default): indexes, validators and other collection options
  are not replicated; create the indexes you need on the target yourself. Do **not**
  create unique indexes on the target in this mode (intermediate states during replay
  may violate them).
- `mongodb.indexes: replicate`: each target collection is created with the source's
  options (validator, validationLevel/Action, collation, capped/size/max,
  clusteredIndex, expireAfterSeconds); indexes are created after the copy and kept in
  sync (created, changed, dropped when dropped on the source) every minute and right
  after DDL events (6.0+). Unique indexes are built only once the target is past the
  snapshot overlap. Strand writes with `bypassDocumentValidation`. Only indexes Strand
  created are ever dropped. TTL indexes run on the target too: documents may expire
  there slightly before the source deletes them (the source delete then is a no-op).
  Needs the extra privileges listed below.

## Failover and restarts

- Source or target elections (step-down, primary crash) are handled automatically:
  the driver retries reads/writes and resumes the change stream; anything it cannot
  absorb (transaction aborted by the election, killed cursor, unknown commit result)
  restarts the work inside the process from the last committed checkpoint, with
  backoff (1 s doubling to 30 s), keeping the lease. Verified on 3-member replica sets
  with step-downs and `kill -9` of the primary on both sides during snapshot and
  streaming (no loss, no duplicates).
- Use a URI listing several members (or `mongodb+srv://`) with `replicaSet=`; a
  single seed works but cannot reconnect if that host is down.
- The lease survives an election as long as one refresh succeeds within
  `lease_ttl` (refresh every TTL/3). Keep `lease_ttl` above the longest expected
  election (default 45 s).
- Process crash: a restarted process on the same host takes over at once when the
  previous owner is provably gone (same hostname, kernel boot id and PID namespace,
  PID gone or reused). Otherwise (e.g. the old owner ran on another host) it waits
  for the lease to expire (up to 2 × `lease_ttl`) and then starts; it exits with
  "lease held" (exit 4, not restarted by `strand@.service`) only when another live
  instance keeps refreshing the lease.
- Exit codes: 0 stopped, 2 configuration, 4 lease held/lost, 5 source history lost
  (resnapshot under a new name), 1 other (permanent errors: authentication,
  unsupported events).

## Security

- TLS: put `tls=true&tlsCAFile=/etc/strand/ca.pem` in the URI (tested with a
  self-signed CA, `requireTLS`). x.509 client authentication:
  `authMechanism=MONGODB-X509&tlsCertificateKeyFile=/etc/strand/client.pem`.
  SCRAM-SHA-256: `authMechanism=SCRAM-SHA-256&authSource=admin`.
- Keep URIs in the environment (`uri: "${SRC_URI}"` with an `.env`/systemd
  `EnvironmentFile=`, or `uri_env`), or credentials in `password_env`.
- Least-privilege roles (checked by `strand validate`):

```js
// source (database app)
db.getSiblingDB("admin").createRole({ role: "strandSource", roles: [], privileges: [
  { resource: { db: "app", collection: "" }, actions: ["find", "changeStream", "listCollections", "listIndexes", "collStats"] },
  { resource: { db: "local", collection: "oplog.rs" }, actions: ["find", "collStats"] } ] })   // oplog window (optional)
// target (database app_replica)
db.getSiblingDB("admin").createRole({ role: "strandTarget", roles: [], privileges: [
  { resource: { db: "app_replica", collection: "" }, actions: ["find", "insert", "update", "remove",
      "createCollection", "dropCollection", "renameCollectionSameDB", "listCollections", "createIndex",
      // only with mongodb.indexes: replicate
      "dropIndex", "listIndexes", "collMod", "bypassDocumentValidation"] },
  { resource: { db: "_strand_cdc", collection: "" }, actions: ["find", "insert", "update", "remove",
      "createCollection", "listCollections", "createIndex"] } ] })
```

## Monitoring

Admin socket (`strand top`, Web UI): state (`bootstrapping`, `synced`, `paused`,
`retrying`, `failed`), lag, events/s, per-collection snapshot progress (`tables`),
recent warnings (`events`), checkpoint.

Prometheus (`observability.metrics`):

| Metric | Meaning |
|---|---|
| `strand_lag_seconds{phase}` | now − cluster time of the last applied event (0 when caught up); phase bootstrap/synced |
| `strand_apply_txn_total`, `strand_apply_rows_total` | target transactions / document writes (rate = docs/s) |
| `strand_mongodb_events_total{op}` | change events applied by type (rate = events/s) |
| `strand_mongodb_snapshot_docs_total` | documents copied (rate = snapshot docs/s) |
| `strand_seed_progress_ratio{table}`, `strand_snapshot_ranges{table,state}` | snapshot progress per collection |
| `strand_retention_headroom_seconds` | source oplog window − lag |
| `strand_mongodb_lookups_total` | update events resolved by a source read |
| `strand_mongodb_retries_total` | automatic restarts after transient errors |
| `strand_fenced_total`, `strand_mongodb_up` | lease lost; 1 while copying/streaming |

Suggested alerts:

```yaml
- alert: StrandMongoLag
  expr: strand_lag_seconds{phase="synced"} > 60
  for: 5m
- alert: StrandMongoOplogHeadroom
  expr: strand_retention_headroom_seconds < 3600
- alert: StrandMongoDown
  expr: strand_mongodb_up == 0 or absent(strand_mongodb_up)
  for: 2m
- alert: StrandMongoRetrying
  expr: increase(strand_mongodb_retries_total[15m]) > 5
- alert: StrandMongoFenced
  expr: increase(strand_fenced_total[5m]) > 0
```

`/healthz` turns unhealthy when the pipeline failed.

## Runbook

- **First sync of a large database**: `strand validate` → enlarge the oplog if the
  window check fails → `strand plan` → `strand sync --background` (or systemd) →
  watch `strand top` until `synced` → `strand verify --mode full`.
- **Lag grows**: check `strand_mongodb_lookups_total` (many lookups: consider
  `updates: delta` on 6.1+), target write latency, `stream_batch`. During a repair or
  rename re-copy the stream waits for the copy.
- **"source change history is no longer available"**: the pipeline was down longer
  than the oplog window. Drop the target collections (or use `overwrite_target`) and
  start under a new pipeline name; then enlarge the oplog.
- **verify reports differences**: rerun (it rechecks differing documents to absorb
  lag); if they persist, `strand verify --table db.coll --repair fix` (or
  `strand snapshot add --table db.coll`) re-copies that collection while the pipeline
  runs (the change stream pauses during the copy and resumes from where it stopped).
- **Exit 4 "lease held"** after a crash on another host: wait `lease_ttl` (systemd
  restarts it). **"lease lost"**: another instance took over — make sure only one
  instance of the pipeline runs.
- **Changing the include/exclude filter or the databases** requires a new pipeline
  name (the checkpoint is bound to them).
- **Cut-over**: stop writes on the source, wait for `strand top` lag 0 /
  `strand verify --mode full` OK, `strand stop`, switch the application.

## Tested matrix and performance

TEST_MATRIX_PLACEHOLDER

### Performance

Local Docker containers on one 16-core host (shared with other workloads; numbers vary
±20%), ~1 KiB documents, `w: majority, j: true`. Stream rates are the time to apply a
backlog of 50 000 events produced while the pipeline was stopped
(`TestContainer_Throughput*`, `STRAND_MONGO_PERF_UPDATES`, `STRAND_MONGO_PERF_PARALLEL`).

| | before (experimental) single / 3-node | `updates: delta` single / 3-node | `updates: lookup` single / 3-node |
|---|---|---|---|
| snapshot, 1 worker (docs/s) | 12 234 / 11 360 | 11 759 / 11 257 | 10 347 / 12 166 |
| snapshot, `max_parallel: 4`, 5 collections (docs/s) | sequential only | 33 628 / 36 227 | |
| insert (events/s) | 7 551 / 7 465 | 7 977 / 9 704 | 8 974 / 7 292 |
| update, `$set`+`$inc` on ~1 KiB docs (events/s) | 3 669 / 3 426 | 4 759 / 4 684 | 3 667 / 3 563 |
| delete (events/s) | 9 010 / 9 461 | 8 931 / 9 027 | 11 543 / 10 166 |
| mixed 30% upsert-replace / 50% update / 20% delete (events/s) | 5 723 / 4 626 | 6 282–6 645 / 3 575–4 712 | 5 133 / 4 751 |

`delta` applies updates ~30% faster than `updateLookup` and replaces the per-event
document read on the source (inside the change stream cursor) by one batched read on
the target; the parallel snapshot scales ~3x with 4 workers.

## Limitations

- Sharded clusters are refused (replica sets only).
- One source database into one target database per pipeline; no transforms, no
  collection renaming, no filtering of documents.
- A single collection is copied by one worker (no range splitting).
- Views, time-series collections and (with `indexes: replicate`) client-side field
  level encryption options are not replicated.
- In `delta` mode on 5.0–6.0 sources, field names containing `.` require
  `updates: lookup` (the default `auto` does this).
- Field order and BSON types are preserved exactly; the snapshot overlap may show a
  document newer than the event being replayed on the target (the final state
  converges to the source).
