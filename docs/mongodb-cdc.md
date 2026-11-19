# MongoDB to MongoDB CDC (experimental)

The implementation is a separate long-running command. It copies selected
collections and then consumes a MongoDB change stream. Source and target must be
replica sets (a single-node replica set is fine); sharded clusters (mongos) and
standalone servers are refused, as are servers older than 4.4. The target stores
resume state and a process lease in `_strand_cdc`; reserve that database for Strand.
The command allows one active MongoDB CDC pipeline per target database.

The module uses MongoDB Go Driver v2.10.0 (requires Go 1.25+). It does not request
MongoDB 6.0-only expanded events.

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
new pipeline name when changing source identity or source/target database mapping;
the saved checkpoint is bound to the source, target, and include/exclude filters.

## Consistency model

- **Handoff.** Before the first document is read, a change stream is opened on the
  source database and its resume token is saved. The snapshot then reads with
  `readConcern: majority`, paging each collection by `_id` (index bound `min()`, so
  `_id`s of mixed BSON types are paged correctly). Every change made during the scan
  is replayed afterwards; replay is idempotent (insert/replace/update = upsert of the
  full document by `_id`, delete = delete-if-exists), so overlap is harmless.
- **Atomic checkpoints.** Each snapshot batch commits in one target transaction
  together with the per-collection snapshot progress; each stream batch commits
  together with its resume token. After a crash (even `kill -9`) the snapshot resumes
  from the last committed `_id` and the stream from the last committed token; nothing
  is lost and documents cannot be duplicated (unique `_id`).
- **Source transactions** are not split by the size cut-off: all events of one source
  transaction commit in one target transaction (unless it exceeds 20x the batch size),
  so readers of the target never observe a partially applied transaction.
- **Updates** use `fullDocument: updateLookup`; the update description is projected
  away and not applied. Intermediate target states may therefore show a newer image
  of a document than the event's own time; the final state converges to the source.
- **Fencing.** The lease (`_strand_cdc.mongo_sync_locks`) expires on the target clock
  (`$$NOW`), so client clock skew does not matter. Every target transaction also
  updates the lease document under `owner == self`; a process paused past its TTL
  (GC, SIGSTOP, VM freeze) whose lease was taken over cannot commit anything and stops
  with "lease lost".
- **Lost history.** If the saved resume point is no longer in the source oplog the
  command fails with "source change history is no longer available; a fresh snapshot
  (new pipeline name) is required". It never silently starts at the present. An idle
  pipeline keeps advancing its checkpoint with post-batch resume tokens.
- **DDL.** Collection `drop`, `rename` and `dropDatabase` are applied (rename re-copies
  the destination collection from the source; dropDatabase invalidates the stream,
  which is restarted after the invalidate). Indexes, collection options, validators,
  views and time-series collections are not replicated; a selected view/time-series
  collection is refused (exclude it). Unique indexes must not be added to target
  collections: intermediate states during replay may violate them.
- Writes and checkpoints use `w: majority, j: true`; transient transaction errors are
  retried by the driver, other errors stop the process (restart resumes).

## Current scope and limitations

- This command is experimental and is not integrated with `strand sync`, Web UI,
  admin socket, metrics, validation, or the SQL pipeline lifecycle. Progress is logged
  (JSON on stderr) every 10 s: documents copied/s, events/s, lag.
- Snapshot is sequential by collection and single-threaded per collection.
- One source database into one target database; no transforms.
- A crash restarts lease acquisition: the restarted process waits for the dead
  owner's lease TTL (default 45 s) and exits with "holds the lease" until then; run
  it under a supervisor that restarts it.

Treat the feature as a development preview until it has run against production-sized
data and multi-node replica sets (failover during sync has not been tested).
