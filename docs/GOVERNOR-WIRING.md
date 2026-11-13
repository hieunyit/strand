# Governor → actuator (review N11, backlog #31)

Spec 02 §7 và design §5.2: actuator duy nhất là x = log R; cửa sổ L, cỡ chunk và phần chia của arbiter
đều suy ra từ R. Bảng dưới ghi rõ đầu ra nào của governor đã nối vào actuator thật của pipeline và đầu ra
nào **chưa có actuator** (không được coi là đã thi hành).

## Đầu ra → actuator

| Đầu ra của governor | Actuator trong pipeline | Trạng thái |
| --- | --- | --- |
| R background (`CurrentRate()` phần background; GRACE: sau arbiter) | `pace` → `backgroundRate` → `tokenBucket` chung của mọi reader snapshot (`internal/pipeline/pipeline.go`). Chờ = chi phí chunk (trang 16 KiB) / R | **Đã nối.** Token bucket chung: bootstrap và `snapshot add` chạy song song cùng chia một R (trước đây mỗi reader một R). Mọi governor bị kẹp bởi `resources.source.iops.hard` |
| Cỡ chunk: `NextChunkRows` (rows · clamp(250 ms / t_chunk, 0,5, 2), chi phí ≤ R · 0,25 s) | `MySQLSnapshotter.NextChunkRows` (`internal/snapshot/mysql_snapshot.go`), gọi sau mỗi chunk chính của keyset walk (PK số nguyên và PK tổng quát) với `ChunkStat` (rows đã dùng, rows / byte thực đọc, thời gian đọc) → `Pipeline.nextChunkRows` | **Đã nối** cho `onoff` và `grace-v2` khi `bootstrap.logical.chunk_rows: auto` (mặc định). `static` và `chunk_rows` là số cố định giữ cỡ chunk cố định (mốc benchmark). Chunk đọc lại (re-read khóa TOAST) không đổi cỡ chunk. Cỡ chunk không lưu vào plan: restart bắt đầu lại từ cỡ của plan |
| Cửa sổ L = ⌈R · T_op · 1,25⌉ (`ConcurrencyWindow`) | **Không có actuator**: mỗi bảng có đúng một reader nối tiếp; số reader song song chỉ đổi theo lệnh `snapshot add` của người vận hành (`bootstrap.logical.max_parallel` vẫn là khóa chưa thi hành). `ConcurrencyWindow` không được gọi | L thực tế = số reader đang chạy. GRACE chỉ nhận `SetWindowSaturated(readers > gated)`: Green chỉ tăng R khi có reader snapshot đang chạy và không bị giữ ở cổng của pace (pause, windows, breaker, background dừng) |
| `SetForegroundDemand` / `SetForegroundFloor` (arbiter: foreground trước) | **Không có actuator**: foreground (stream binlog/WAL, apply) không đi qua token bucket của source và chưa có bộ đếm chi phí source của nó | **Chưa nối.** Arbiter chạy nhánh "chưa biết nhu cầu foreground": background nhận cả R. Áp lực của foreground vào governor qua P_lag (`SetLagPressure`): synced + P_lag ≥ 1 → background 0; bootstrap + P_lag ≥ 1 → trần lên hard limit, background ≥ 50% R |
| `SetBackgroundWeights` / `BackgroundShare` | Chỉ có một tác vụ background đi qua token bucket (snapshot). `verify` đọc source **không** qua governor | **Chưa nối** (một tác vụ: share = R background). Verify chưa bị giãn nhịp — khoảng hở còn lại |
| Breaker / `BackgroundRunning` | Cổng của `pace` (`paceGate`) | Đã nối (như trước) |
| P_lag (`SetLagPressure`) | `feedGrace` mỗi tick: max(lag / lag_slo, WAL slot giữ / `max_slot_wal_keep_size`) — số hạng WAL mới, chỉ source PG | Đã nối; monitor dùng cùng công thức để bật `bootstrap_retention_risk` |

## bootstrap_retention_risk trong pace

Trước đây `pace` trả về ngay khi `retentionRisk` (và breaker đóng), **trước** bước giãn nhịp: snapshot
chạy không giới hạn, vượt cả hard limit, trong khi spec ghi "tăng ưu tiên snapshot và prefetch, **vẫn
trong hard limit**". Kết hợp với lỗi thứ hai bên dưới, một source PG rảnh hơn lag_slo giây là đủ để
snapshot PG chạy không giãn nhịp. Nay retention risk chỉ bỏ qua `windows` và việc governor dừng
background (để không tự khóa chết), vẫn giãn theo R:

- `grace-v2`: R background của arbiter (bootstrap + P_lag ≥ 1 đã nâng trần lên hard limit);
- `onoff`: hard limit (không cấu hình hard thì soft);
- `static`: soft limit (R cố định theo định nghĩa).

`pause --snapshot` và breaker vẫn thắng retention risk.

## Tín hiệu source PostgreSQL (`internal/governor/signals_pg.go`)

Cùng bộ tín hiệu với MySQL (`MySQLDBSignalReader`), theo cột PostgreSQL của bảng loại tín hiệu (design §5.2):

| DBSignals | MySQL | PostgreSQL |
| --- | --- | --- |
| `ReplicaLagSeconds` | `Seconds_Behind_Source` | standby: now − `pg_last_xact_replay_timestamp()` khi còn WAL đã nhận chưa replay; primary: `max(replay_lag)` của standby vật lý trong `pg_stat_replication` (walsender logical bị loại; walsender có `replay_lsn` NULL — pg_receivewal, kể cả là standby đồng bộ, pg_basebackup — bị loại vì replay_lag của chúng do walsender ngoại suy và tăng mãi; standby đã replay tới `sent_lsn` tính 0) |
| `QueryP99` / `QueryBaseline` | latency câu poll (baseline cố định 10 ms) | latency TỪNG câu của poll (mỗi câu một mẫu; `pg_settings` đọc cache 60 s, không tính). Baseline **đo được**: p99 của 100 mẫu đầu (sàn 1 ms), p99 hiện tại trên cửa sổ trượt 100 mẫu. Hằng 10 ms cũ làm breaker mở và P_strand_p99 ≈ 6-7 ngay dưới tải pgbench (smoke Gate 2) |
| `HardSignal` | Δ`Innodb_buffer_pool_wait_free`, Δ`Innodb_log_waits`, spool ≥ 95% | `wal_status` của slot = `unreserved` / `lost`, spool ≥ 95% |

Ngoài DBSignals: số hạng WAL của P_lag (`pg_wal_lsn_diff(head, restart_lsn)` / `max_slot_wal_keep_size`)
và "reader đã đuổi kịp source" cho lag bootstrap: walsender của slot cách head ≤ 16 MiB
(`pg_wal_lsn_diff(head, sent_lsn)`). Trước đây `caughtUpWithSource` luôn false với PG, nên lag bootstrap
= thời gian từ commit cuối đã route: source rảnh vài giây là `bootstrap_retention_risk` bật sai.

Lúc dựng governor (`onoff` / `grace-v2`) reader đọc thử một lần: lỗi (thiếu quyền trên view, kết nối) →
cảnh báo `cannot read source governor signals; using static`; role không có `pg_monitor` (và không phải
superuser) → cảnh báo `governor signals degraded` (replay_lag của standby bị ẩn, đọc thành 0) nhưng vẫn
chạy governor đã chọn. Source PG không còn bị ép về `static`.

## Còn thiếu

- Tín hiệu mềm theo tải chưa có ở cả hai engine: EWMA `Threads_running` (MySQL), tỉ lệ wait event IO /
  commit latency (PG). Tín hiệu theo đồng thời (lock wait, deadlock → trần L) chưa có vì L không có actuator.
- Token "quyết toán theo số thực đo" (design §5.1 hàng 2: RLS của Δ`Innodb_data_reads`,
  `pg_stat_statements.shared_blks_read`) chưa có; chi phí chunk vẫn là byte / 16 KiB.
- Foreground demand / floor, background weights, verify qua governor: xem bảng trên.
- Chưa có e2e chạy GRACE trên source PG; benchmark G2-12 trên PG cần chạy lại sau thay đổi này.
