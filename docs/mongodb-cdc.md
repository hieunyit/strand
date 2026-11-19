# MongoDB to MongoDB CDC (experimental)

The initial implementation is a separate long-running command. It copies selected
collections and then consumes a MongoDB change stream. The source must be a replica
set or sharded cluster. The target stores resume state and a process lease in
`_strand_cdc`; reserve that database for Strand.
The command allows one active MongoDB CDC pipeline per target database to prevent
overlapping pipelines from racing on the same target collections.

The module uses MongoDB Go Driver v2.10.0, which requires Go 1.25 or later and is
compatible with MongoDB Server 4.4 and later. Change Streams require a WiredTiger
replica set or sharded cluster using replica set protocol v1; this command does not
request MongoDB 6.0-only expanded events.

```sh
export MONGO_SOURCE_URI='mongodb://user:password@source1:27017/?replicaSet=rs0'
export MONGO_TARGET_URI='mongodb://user:password@target1:27017/'
strand mongodb sync \
  --pipeline app-prod \
  --source-database app --target-database app_replica \
  --include 'app.orders' --include 'app.customers'
```

The command reads URIs from `MONGO_SOURCE_URI` and `MONGO_TARGET_URI` by default;
`--source-uri-env` and `--target-uri-env` select different environment variable names.

The first snapshot refuses to overwrite an existing matching target collection.
`--overwrite-target` explicitly permits dropping/recreating those collections. Use a
new pipeline name when changing source identity or source/target database mapping.
Resume checkpoints are saved after up to 100 events or 1 second, so a crash can replay
the current checkpoint batch. Document writes are idempotent by `_id`.
Target writes and checkpoint updates request `w: majority, j: true`; this protects the
checkpoint from moving ahead of data that is not durably acknowledged, at the cost of
additional target latency. The saved checkpoint is bound to the source, target, and
include/exclude filters; change those settings under a new pipeline name.

## Current scope and limitations

- This command is experimental and is not integrated with `strand sync`, Web UI,
  admin socket, metrics, validation, or the SQL pipeline lifecycle.
- Snapshot does not preserve collection options or indexes. Collection-level DDL
  and index changes are not mirrored. Views and time-series collections are rejected
  during bootstrap.
- Updates use `fullDocument: updateLookup`; this converges to the latest document
  image and does not promise the exact post-image at the event's original time.
- The initial scan is sequential by collection. Snapshot writes use unordered bulk
  inserts in batches of up to 500 documents or 8 MiB; sustained CDC writes and
  checkpoint writes are still serialized, so high-latency target links limit rate.
- Change events are applied one at a time. Multi-document transactions are not
  atomically committed on the target.
- The source oplog must retain the bootstrap start time until the initial snapshot
  completes. If it expires, the command stops with an error; create a fresh pipeline
  and reseed rather than silently starting at the present.
- This first version targets one source database into one target database. It does
  not map database names, transforms, or BSON schemas.

Treat the feature as a development preview until integration, failure-recovery,
data-integrity, and load testing are completed.
