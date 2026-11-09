# External client evidence, preparation

`benchclient` prepares read-only command plans and parses raw `pgbench -l` transaction logs. It does **not** run a benchmark or choose a governor. It is separate from the current MySQL-only `tools/bench` dev runner.

```bash
go run ./tools/benchclient --backend pgbench --binary /owned/bin/pgbench \
  --log-prefix /owned/artifacts/pgbench --clients 16 --jobs 8 \
  --duration 10m --rate 200

go run ./tools/benchclient --backend sysbench --binary /owned/bin/sysbench \
  --log-prefix /owned/artifacts/sysbench --clients 16 --jobs 8 \
  --duration 10m --rate 200
```

The JSON plans say `official_ready: false`. The `pgbench` plan enables unaggregated per-transaction logs. A controller still needs to create an owned PG fixture, provide `PG*` connection environment, restore identical source state, schedule the fixed wave/spikes, and capture Strand metrics and final equality. The `sysbench` plan is a command skeleton: connection options and the owned fixture are not wired. Its interval p99 cannot be combined into exact snapshot-window p99.

After a separately controlled `pgbench` run with raw log files `pgbench.<pid>[.<thread>]`, calculate exact p99 over completed transactions in the snapshot window:

```bash
go run ./tools/benchclient \
  --parse-pgbench-prefix /owned/artifacts/pgbench \
  --snapshot-start 2026-11-08T00:00:00Z \
  --snapshot-end 2026-11-08T00:10:00Z
```

The parser uses all matching files, refuses malformed records or empty windows, counts failed/skipped records, and retains the raw file paths in its JSON result. A failed/skipped count makes the window incomplete. The half-open time window is `[start,end)`, using the `pgbench` completion timestamp and the logged client latency in microseconds. The result remains `official_ready: false` until the whole Gate 2 protocol is executed and reviewed.

## Instrumented sysbench raw latency, Stage 2 preparation

The patch at `patches/sysbench-1.0.20-raw-events.patch` (SHA256 `8cfb1da36ba9b5aa3febd24d9ed4a56da8981b2ac0fb0df04a23b5d09d7b43c4`) targets upstream sysbench tag `1.0.20` (tag object `f3da4313`, commit `ebf1c90da05dea94648165e4f149abc20c979557`). It reads the same nanosecond duration returned by `sb_timer_stop` in `sb_event_stop`, after the measured event ends, and records completion time from `CLOCK_REALTIME` to a buffered file per worker. It leaves bundled `oltp_read_write.lua` and its transaction retry loop unchanged. Set `STRAND_BENCH_RAW_PREFIX=/owned/artifacts/sysbench/raw` in the owned sysbench process; existing files are refused (`O_EXCL`). Each file has contiguous per-thread sequence numbers and a final count/error footer, and stderr has one `STRAND_RAW_EVENTS` count/error/run-status marker only after workers have joined. The patch also makes upstream worker join errors return failure. Any failed workload, missing marker, write, flush, close, clock, footer, or count error invalidates the raw evidence.

After an owned run, parse exact completion-window p99:

```bash
go run ./tools/benchclient \
  --parse-sysbench-prefix /owned/artifacts/sysbench/raw \
  --sysbench-stderr /owned/artifacts/sysbench/stderr.log \
  --sysbench-threads 16 \
  --sysbench-exit-code 0 \
  --snapshot-start 2026-11-08T00:00:00Z \
  --snapshot-end 2026-11-08T00:10:00Z
```

The parser checks raw count against the instrumentation marker and requires the actual sysbench process exit code 0, not sysbench's final measured-phase event counter: a future controller must account for any sysbench warmup reset, compare phase-matched counters, and inspect ignored SQL errors and retry counters. Compilation of the patch, timing-overhead pilot, fixture orchestration, client connection flags, three randomized repetitions per governor, telemetry completeness, plots, and final decision are pending. No official benchmark has run; parser exit 0 only confirms parsing, and `official_ready` remains false.
