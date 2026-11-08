# External client evidence, Stage 1

`benchclient` prepares read-only command plans and parses raw `pgbench -l` transaction logs. It does **not** run a benchmark or choose a governor. It is separate from the current MySQL-only `tools/bench` dev runner.

```bash
go run ./tools/benchclient --backend pgbench --binary /owned/bin/pgbench \
  --log-prefix /owned/artifacts/pgbench --clients 16 --jobs 8 \
  --duration 10m --rate 200

go run ./tools/benchclient --backend sysbench --binary /owned/bin/sysbench \
  --log-prefix /owned/artifacts/sysbench --clients 16 --jobs 8 \
  --duration 10m --rate 200
```

The JSON plans say `official_ready: false`. The `pgbench` plan enables unaggregated per-transaction logs. A controller still needs to create an owned PG fixture, provide `PG*` connection environment, restore identical source state, schedule the fixed wave/spikes, and capture Strand metrics and final equality. The `sysbench` plan is a command skeleton: connection options and raw per-event telemetry are not wired. Its interval p99 cannot be combined into exact snapshot-window p99.

After a separately controlled `pgbench` run with raw log files `pgbench.<pid>[.<thread>]`, calculate exact p99 over completed transactions in the snapshot window:

```bash
go run ./tools/benchclient \
  --parse-pgbench-prefix /owned/artifacts/pgbench \
  --snapshot-start 2026-11-08T00:00:00Z \
  --snapshot-end 2026-11-08T00:10:00Z
```

The parser uses all matching files, refuses malformed records or empty windows, counts failed/skipped records, and retains the raw file paths in its JSON result. A failed/skipped count makes the window incomplete. The half-open time window is `[start,end)`, using the `pgbench` completion timestamp and the logged client latency in microseconds. The result remains `official_ready: false` until the whole Gate 2 protocol is executed and reviewed.
