# PostgreSQL strict diagnostic harness

Run in fresh owned Docker containers (PostgreSQL 18), with allocated loopback ports:

```sh
/usr/local/go/bin/go run ./tools/diagnostics/pg-strict -out-dir=/tmp/pg-strict-run
```

The harness mutates its fixture: it initializes/truncates `bench.accounts`, replaces
`strand_strict_diag_pub`, and updates target data during the raw commit probe.
External DSNs are rejected unless `-auto-docker=false` and the separate
`-allow-destructive-existing-databases` flag are both explicit. That flag permits
these destructive operations in **both** supplied databases. Use isolated disposable
databases. An external DSN by itself never authorizes mutation.

Docker names and labels are unique per create; cleanup uses only returned container
IDs. Source/target ports default to Docker allocation and metrics to OS allocation;
the endpoint is discovered from the child process's successful listener announcement.
Explicit port flags remain available and never evict an existing service.

Output directories must be owned by the current user and private (0700). Fresh
paths are created 0700; existing permissive directories and symlink components
are rejected without changing their permissions. Directories are exclusively
locked while a run is active. Choose a distinct `-out-dir` for concurrent runs; an
existing lock is never removed by a second run. A stale lock after a killed runner
must be checked before manual removal.

Credential configs are 0600 in a separate temporary
private directory, removed on return. The retained config is redacted for sharing and is not a runnable credential config. Child
stdout/stderr are redacted before persistence, and transaction traces are redacted
before export. Review any additional manually collected artifacts before sharing.

`rows_per_source_txn` is applied rows divided by applied source transactions; it is
not target commit batch size. Optional `-txntrace` records target batch membership
for separate analysis. `apply_txns_to_raw_commits_ratio` compares two workloads;
the raw single-connection update/commit probe is not an apply throughput ceiling.
A local run does not establish the cause of throughput on another host. No strict
ordering or durability defaults are changed.
