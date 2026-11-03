# Strand CDC — Hướng dẫn chạy và tùy chọn (P1 / Gate 1)

Cập nhật: 2026-11-03

Strand P1 đồng bộ một chiều MySQL → MySQL (8.0/8.4), một process `strand sync` cho mỗi pipeline; tài liệu này ứng với nhánh `review/gate1-fixes` (commit `2332300`).

## 1. Yêu cầu môi trường

Source phải bật GTID và binlog ROW đầy đủ; tất cả đổi được online, riêng `log_bin` và `log_replica_updates` cần restart nếu chưa bật. `strand validate` kiểm các điều kiện này, kể cả từng session đang mở.

### Biến bắt buộc trên source

| Biến | Giá trị | Đổi online | Ghi chú |
| --- | --- | --- | --- |
| `gtid_mode` | `ON` | Có, qua `OFF_PERMISSIVE` → `ON_PERMISSIVE` → `ON` | Trước đó bật `enforce_gtid_consistency`; đợi `Ongoing_anonymous_transaction_count = 0` |
| `binlog_format` | `ROW` | Có | Phạm vi session: kết nối cũ giữ giá trị cũ tới khi kết nối lại |
| `binlog_row_image` | `FULL` | Có | Phạm vi session; binlog to hơn nếu đang dùng `MINIMAL` |
| `binlog_row_value_options` | rỗng | Có | `PARTIAL_JSON` bị từ chối |
| `binlog_row_metadata` | `FULL` (khuyến nghị) | Có | Mặc định `MINIMAL`; validate chỉ cảnh báo |
| `log_bin` | bật | Không, cần restart | 8.0+ bật sẵn |
| `log_replica_updates` | `ON` khi đọc từ replica | Không, cần restart | 8.0+ bật sẵn |
| `server_id` | khác 0 | Có | Đặt cố định trong file cấu hình |

Quy trình đổi an toàn:

1. `SET PERSIST binlog_row_metadata = FULL; SET PERSIST binlog_row_image = FULL; SET PERSIST binlog_format = ROW;`
2. Cho ứng dụng kết nối lại (khởi động lại pool); trên replica chạy `STOP REPLICA; START REPLICA;`.
3. Chạy `strand validate -c pipeline.yaml`; nó từ chối (exit 3) nếu còn session ghi binlog với giá trị cũ và liệt kê thread.
4. Chỉ khởi động Strand hoặc lấy điểm seed sau khi validate sạch.

### Quyền DB tối thiểu (spec 03 §2)

| User | Nơi | Quyền | Dùng cho |
| --- | --- | --- | --- |
| `strand` | Source | `REPLICATION SLAVE, REPLICATION CLIENT ON *.*`; `SELECT ON app.*` | Đọc binlog, snapshot, verify |
| `strand` | Source (khuyến nghị) | `SELECT ON performance_schema.variables_by_thread`, `performance_schema.threads` | Kiểm session trong validate; thiếu thì chỉ cảnh báo |
| `strand` | Target | `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP ON app.*`; `CONNECTION_ADMIN ON *.*` | Apply, bảng `_strand_*`; ghi được khi target `read_only=ON`; ngắt kết nối instance cũ |
| `strand_admin` | Source và target | `SYSTEM_VARIABLES_ADMIN ON *.*` | Chỉ khi `promote`: fence source, mở ghi target |

Strand không ghi gì vào source khi đồng bộ; metadata (`_strand_pipeline`, `_strand_checkpoint`, `_strand_worker_delta`, `_strand_promote`) nằm trong schema của target DSN (ví dụ `strand`). Ngoại lệ duy nhất là `promote` bật `super_read_only` trên source cũ.

## 2. File cấu hình pipeline.yaml

Mỗi pipeline có một file YAML; thứ tự ưu tiên là YAML, rồi biến môi trường `STRAND_*`, rồi cờ dòng lệnh. Khóa lạ bị từ chối (exit 2). Xem schema đầy đủ bằng `strand config schema`.

### Ví dụ tối thiểu

```yaml
version: 1
name: orders-sync
source: { kind: mysql, dsn: "strand:***@tcp(10.0.0.11:3306)/", server_id: 47001 }
target:
  kind: mysql
  dsn: "strand:***@tcp(10.0.0.21:3306)/strand"   # schema chứa bảng _strand_*
  commit_order: relaxed                          # hoặc strict
  batch: { max_rows: 2000, max_bytes: 4MiB, max_wait: 50ms }
tables:
  include: ["app.*"]
  exclude: ["app.audit_log"]
  rename: { "app.orders": "app2.orders" }
bootstrap: { default: logical }                  # logical | seed
checkpoint: { store: target, flush_every: 500ms }
resources:
  spool: { path: /var/lib/strand, soft: 20GiB, hard: 50GiB, overflow: "s3://strand-staging/orders" }
  source: { iops: { soft: 600, hard: 1200 } }
  workers: { max: 8 }
governor:
  algorithm: onoff
  tick: 1s
  lag_slo: 5s
  breaker: { replica_lag: { max: 30s }, strand_query_p99: { max_ratio: 3 } }
verify: { delta: on, post_seed_window: 30m }
observability:
  metrics: ":9464"
  admin_socket: /run/strand/orders-sync.sock
  log: { level: info, format: json }
```

### Các khóa có tác dụng ở P1

| Khóa | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `name` | bắt buộc | ID pipeline (khóa chống chạy trùng, checkpoint) |
| `source.kind` / `target.kind` | bắt buộc | `mysql` (PG ở P2) |
| `source.dsn` | bắt buộc | Source hoặc replica có `log_replica_updates=ON` |
| `source.server_id` | tự sinh | Server ID khi đọc binlog; phải duy nhất trong cụm |
| `target.dsn` | bắt buộc | Schema trong DSN chứa bảng `_strand_*` |
| `target.commit_order` | `relaxed` | `relaxed`: song song theo writeset, nhanh hơn; `strict`: đúng thứ tự source, target luôn là một tiền tố của source |
| `target.batch.max_rows` | 2000 | Đóng batch group commit khi đủ số row |
| `target.batch.max_bytes` | 4MiB | Đóng batch khi đủ dung lượng (ước lượng theo dữ liệu row) |
| `target.batch.max_wait` | 0 (không chờ) | Chờ tối đa để gom thêm Txn; tăng throughput, thêm độ trễ |
| `tables.include` / `exclude` | bắt buộc / rỗng | Mẫu glob `schema.table`; đổi giữa các lần chạy được (bảng mới sẽ được snapshot) |
| `tables.rename` | rỗng | Đổi tên bảng ở đích `"src.tbl": "dst.tbl"` |
| `bootstrap.default` | `logical` | `logical`: Strand tự snapshot (PK một cột số nguyên); `seed`: từ backup (mục 4) |
| `bootstrap.logical.chunk_rows` | auto (5000) | Cỡ chunk snapshot |
| `checkpoint.flush_every` | 500ms | Chu kỳ gộp row delta vào row checkpoint toàn cục |
| `resources.spool.path` | bắt buộc | Thư mục spool và trạng thái (prefetch dùng `<path>/prefetch`) |
| `resources.spool.hard` | không giới hạn | Trần spool local; ≥ 95% là tín hiệu cứng dừng snapshot |
| `resources.spool.overflow` | rỗng | Tầng bucket `s3://bucket/prefix` (endpoint, key qua `STRAND_S3_*`) |
| `resources.source.iops.soft` / `hard` | 600 / không | Tốc độ đọc snapshot (token/giây); đổi nóng bằng `strand throttle set` |
| `resources.workers.max` | 8 | Số luồng apply (strict luôn 1) |
| `governor.algorithm` | `onoff` | `onoff` hoặc `static`; `grace-v2` lùi về `onoff` kèm cảnh báo (P2) |
| `governor.tick` / `lag_slo` | 1s / 5s | Nhịp điều tốc và lag mục tiêu |
| `governor.breaker.replica_lag.max` | tắt | Replica lag vượt ngưỡng thì ngắt snapshot |
| `governor.breaker.strand_query_p99.max_ratio` | tắt | Độ trễ query của Strand vượt n lần nền thì ngắt |
| `verify.delta` | `off` | `on`: verify các khóa CDC vừa chạm |
| `verify.post_seed_window` | 30m | Cửa sổ verify sau seed |
| `observability.metrics` | tắt | Địa chỉ HTTP cho `/metrics` và `/healthz` |
| `observability.admin_socket` | tắt | Unix socket (quyền 0700) cho `top`, `pause`, `throttle`, `snapshot add`, `checkpoint`, `verify --mode delta` |
| `observability.log.level` / `format` | `info` / `json` | Mức và định dạng log |

### Biến môi trường

- `STRAND_<đường dẫn khóa viết hoa, nối bằng _>` ghi đè YAML, ví dụ `STRAND_TARGET_DSN`, `STRAND_GOVERNOR_LAG_SLO=10s`, `STRAND_RESOURCES_WORKERS_MAX=16`. Chỉ áp cho khóa đơn (chuỗi, số, bool, thời lượng), không áp cho map/list.
- `STRAND_S3_ENDPOINT`, `STRAND_S3_ACCESS_KEY`, `STRAND_S3_SECRET_KEY`: credential của tầng bucket.

### Khóa được chấp nhận nhưng chưa có tác dụng ở P1

- Có cảnh báo khi `validate`/`sync`: `source.read_from=standby`, `tables.mask`, `resources.*.bandwidth`, `resources.network`, `resources.schedule`, `resources.workers.min`, các khóa `bootstrap.per_table.*.seed` (`credentials`, `encryption_key`, `load`, `verify_after`).
- Chưa có cảnh báo (đặt cũng không đổi hành vi): `source.read_from` (mọi giá trị; muốn đọc replica thì trỏ `source.dsn` vào replica), `target.write_mode`, `resources.memory`, `resources.spool.soft`, `resources.target.iops`, `schema.additive`/`destructive`, `governor.probe`, `governor.metrics_source`, `verify.full`, `bootstrap.logical.max_parallel`, `bootstrap.catch_up.binlog_archive`.

## 3. Khởi chạy: validate, plan, sync

Trình tự chuẩn là `validate` → `plan` → `sync`; mọi lệnh nhận file cấu hình qua `-c` / `--config`.

1. **Kiểm tra điều kiện:** `strand validate -c pipeline.yaml`
    - Kiểm cấu hình, biến binlog/GTID global, từng session đang mở, bảng tồn tại và có PK, partial JSON.
    - Exit 0 khi đạt; exit 3 kèm lý do khi bị từ chối; cảnh báo (khóa chưa hỗ trợ, `binlog_row_metadata`) in ra stderr.
2. **Ước lượng snapshot:** `strand plan -c pipeline.yaml [--force]`
    - In từng bảng: số row ước lượng, dung lượng, chế độ và ETA theo `resources.source.iops.soft`.
    - Từ chối chế độ logical khi ETA > 50% thời gian giữ binlog; `--force` để chấp nhận rủi ro, nếu không thì chuyển sang seed từ backup.
3. **Chạy pipeline:** `strand sync -c pipeline.yaml [--start-at ...] [--seed-manifest seed.json]`
    - Không có `--start-at`: lần đầu snapshot logic mọi bảng rồi stream; các lần sau tiếp tục từ checkpoint trong target.
    - `--start-at seed`: bắt đầu từ điểm P trong `seed.json` sau khi `seed apply` xong (mục 4).
    - `--start-at <gtid set>`: bỏ qua snapshot, coi target đã có dữ liệu tại GTID đó.
    - Chạy foreground; `SIGTERM`/`Ctrl-C` dừng đọc, chờ writer, ghi checkpoint rồi thoát 0.

Khởi động lại luôn an toàn: checkpoint nằm trong target nên kill -9 giữa chừng không mất hoặc lặp dữ liệu. Hai instance cùng `name` không chạy song song được: instance mới giữ khóa, tăng epoch và ngắt kết nối của instance cũ.

Triển khai thường trực dùng systemd (`strand@<name>.service`) hoặc StatefulSet trên FKE (`deploy/`); không tự restart sau exit 2–5 và 10 (mục 8).

## 4. Seed từ backup

Seed từ backup không đọc dữ liệu bảng trên source (0 lượt đọc, có e2e) và là cách chọn cho bảng lớn hoặc PK không phải một cột số nguyên. Strand không tự restore; nó kiểm điểm P, đọc trước binlog trong lúc restore và bắt kịp từ P.

### Quy trình

1. Tạo backup bằng công cụ gốc (nên chạy trên replica).
2. `strand seed plan --type <loại> --backup <đường dẫn> --source <dsn> [--target <dsn>] --out seed.json`: kiểm SD-1..SD-4 (P hợp lệ, log còn phủ từ P, phiên bản công cụ và target) và ghi `seed.json`.
3. Restore vào target, đồng thời prefetch binlog: `strand seed apply --manifest seed.json --target <dsn> --spool <spool.path>/prefetch --source <dsn> [--restore-cmd "<lệnh restore>"]`. Nó kiểm `gtid_executed` của target = P, gỡ cấu hình replication còn sót.
4. `strand sync -c pipeline.yaml --start-at seed --seed-manifest seed.json`: phát lại spool prefetch, đọc tiếp từ source, chạy post-seed verify trong `verify.post_seed_window`.

### Theo loại backup

| `--type` | Lấy P từ | Yêu cầu riêng | Trạng thái e2e |
| --- | --- | --- | --- |
| `xtrabackup` | `xtrabackup_binlog_info` | Backup đã `--prepare`; XtraBackup 8.4 | Pass 20 GB dưới tải |
| `clone` | `gtid_executed` của target sau CLONE | Plugin clone, người dùng có `BACKUP_ADMIN` | Pass |
| `mydumper` | file `metadata`, section `[source]` | mydumper v1.0.3-1; **bắt buộc** `--dump-log <log stderr>` (Strand từ chối `NO_LOCK`); backup cũ không có log thì `--accept-unverified-lock-mode` và nhận cảnh báo; restore bằng `myloader --drop-table=DROP` | Pass |
| `mysqldump` | dòng `SET @@GLOBAL.GTID_PURGED` | Dump bằng `--single-transaction --source-data=2 --set-gtid-purged=ON`; file `.sql` phải còn dòng `-- Dump completed`; trên target chạy `RESET BINARY LOGS AND GTIDS` trước khi nạp; luôn có cảnh báo không kiểm được `--single-transaction` | Pass |

Tạo schema `strand` trên target sau khi restore bằng `SET SESSION sql_log_bin = 0; CREATE DATABASE strand; SET SESSION sql_log_bin = 1;` để `gtid_executed` vẫn đúng bằng P.

### Cờ của seed plan

| Cờ | Ý nghĩa |
| --- | --- |
| `--type` | `xtrabackup` \| `clone` \| `mydumper` \| `mysqldump` |
| `--backup` | Thư mục backup hoặc file dump |
| `--source` | DSN source: đọc `gtid_purged` để kiểm log còn phủ từ P |
| `--target` | DSN target đã restore: đối chiếu P, phiên bản |
| `--archive-until` | GTID set đã có trong archive binlog (khi source đã xóa log) |
| `--tables` | Danh sách bảng của seed |
| `--allow-prerelease-tool` | Chấp nhận backup tạo bằng bản RC/beta của công cụ |
| `--dump-log`, `--accept-unverified-lock-mode` | Riêng mydumper (bảng trên) |
| `--out` | File manifest, mặc định `seed.json` |
| `--dry-run` | In manifest, không ghi file |

### Cờ của seed apply

| Cờ | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `--manifest` | `seed.json` | Manifest từ `seed plan` |
| `--target` | bắt buộc | DSN target đã restore |
| `--print-only` | false | Chỉ in các bước restore gợi ý, không làm gì |
| `--prefetch` | true | Đọc binlog từ P vào spool trong lúc restore, cứu phần log source sẽ xóa |
| `--spool` | bắt buộc khi prefetch | Phải là `<resources.spool.path>/prefetch` của pipeline |
| `--source` | rỗng | DSN source hoặc replica để prefetch |
| `--archive` | rỗng | Thư mục binlog archive, đọc trước phần source đã xóa |
| `--restore-cmd` | rỗng | Lệnh restore (`sh -c`) chạy song song; prefetch đọc liên tục tới khi lệnh xong |
| `--spool-local-hard` | 0 | Trần byte tầng local; vượt thì tràn sang `--overflow` |
| `--overflow` | rỗng | Tầng bucket `s3://bucket/prefix` (credential qua `STRAND_S3_*`) |
| `--spool-hard` | 0 | Trần tổng mọi tầng; chạm trần thì exit 5 `prefetch_exhausted` |

## 5. Vận hành khi đang chạy

Các lệnh dưới đây nói chuyện với `strand sync` đang chạy qua admin socket; socket lấy từ `--socket`, nếu không thì `observability.admin_socket` trong file `-c`, cuối cùng `/run/strand/default.sock`.

| Lệnh | Tác dụng |
| --- | --- |
| `strand top -c p.yaml` | Trạng thái, lag (giây), tốc độ (row/s), vùng governor, tiến độ và ETA snapshot |
| `strand pause -c p.yaml` | Dừng cả đọc và apply (giữ vị trí) |
| `strand pause --snapshot -c p.yaml` | Chỉ dừng snapshot logic; stream CDC vẫn chạy |
| `strand resume -c p.yaml` | Chạy lại tất cả |
| `strand resume --snapshot-only -c p.yaml` | Chỉ chạy lại phần snapshot |
| `strand throttle set --source-iops N [--soft] -c p.yaml` | Đổi nóng giới hạn đọc snapshot trên source; `--target-iops` được nhận nhưng chưa có tác dụng |
| `strand snapshot add --table app.t -c p.yaml` | Thêm bảng vào pipeline đang chạy: snapshot logic có điều tốc, không dừng stream (PK một cột số nguyên) |
| `strand checkpoint show -c p.yaml` | Pipeline, epoch, tập GTID đã apply, thời điểm cập nhật |
| `strand checkpoint rewind --to <gtid set> -c p.yaml` | Tua vị trí về GTID set cho trước (kiểm `gtid_purged`); epoch tăng |
| `strand verify --mode sample --table app.t [--rate 0.01] [--repair report\|fix] -c p.yaml` | Lấy mẫu theo khoảng PK, so source và target; `fix` sửa row lệch |
| `strand verify --mode delta -c p.yaml` | Verify các khóa CDC vừa chạm (qua socket); lệch thì exit 5 |

`verify --mode full` (cây Merkle) thuộc P2 và hiện trả lỗi. Thay đổi `tables.include`/`exclude` cần khởi động lại: bảng mới được snapshot, bảng bị loại thôi đồng bộ.

## 6. Promote và failover

`strand promote` chuyển ứng dụng sang target theo các bước có trạng thái (bảng `_strand_promote`), chạy lại được từ bước dở. Giữ `strand sync` chạy trong lúc promote; dừng nó sau khi promote xong.

```bash
strand promote --pipeline orders-sync \
  --target "strand_admin:***@tcp(10.0.0.21:3306)/strand" \
  --fence  "strand_admin:***@tcp(10.0.0.11:3306)/" \
  --catchup-timeout 5m
```

1. **Fence source cũ:** `SET GLOBAL super_read_only = ON` qua `--fence`.
2. **Giữ target chỉ đọc:** `read_only=ON`, `super_read_only=OFF`; ứng dụng không ghi được, Strand (có `CONNECTION_ADMIN`) vẫn ghi.
3. **Bắt kịp:** chờ tới khi tập đã apply chứa toàn bộ `gtid_executed` của source, tối đa `--catchup-timeout`.
4. **Sequence:** MySQL tự nâng `AUTO_INCREMENT` khi chèn giá trị tường minh nên không cần làm gì; `--peak-rate`, `--sync-interval`, `--override-sequences` dành cho PG (P2).
5. **Mở ghi target:** `read_only=OFF`.

### Failover khi source đã chết

- Không fence được source (mất kết nối hoặc thiếu `--fence`): promote dừng exit 10 và in tập GTID đã apply ở target cùng thời điểm checkpoint cuối, tức phần dữ liệu sẽ mất nếu tiếp tục.
- Cô lập source bằng cách khác (tắt máy, chặn mạng), rồi chạy lại cùng lệnh thêm `--source-fenced-externally` để xác nhận chấp nhận mất phần đó.
- `--dry-run`: chạy thử, không đổi gì.

`--pipeline` phải trùng `name` trong cấu hình (mặc định `default`).

## 7. Điều chỉnh khi tải tăng

Governor chỉ điều tốc phần đọc snapshot trên source; stream thay đổi luôn được apply nhanh nhất có thể, nên target phải chịu được tải ghi tương đương source.

### Strand tự làm

- **Governor `onoff`** (mỗi `tick`): áp suất P đo bằng lag so với `lag_slo` và nguy cơ mất binlog; P ≥ 1 thì dừng snapshot, P < 0,9 trong 3 tick thì chạy lại ở `iops.soft`.
- **Breaker:** replica lag hoặc độ trễ query của Strand vượt ngưỡng thì cắt snapshot; mở lại sau 60 giây liên tục dưới ngưỡng.
- **Tín hiệu cứng:** `Innodb_buffer_pool_wait_free` hoặc `Innodb_log_waits` tăng trên source, hoặc spool local ≥ 95% `spool.hard`, thì dừng snapshot ngay trong tick đó.
- **Stream:** Txn không đụng nhau chạy song song (relaxed), gom group commit và multi-row upsert để bắt kịp.

### Chỉnh tay

| Mục đích | Thao tác | Hiệu lực |
| --- | --- | --- |
| Giảm đọc trên source khi snapshot | `strand throttle set --source-iops N [--soft]` | Ngay |
| Nhường source trong giờ cao điểm | `strand pause --snapshot`, sau đó `strand resume --snapshot-only` | Ngay |
| Governor nhạy hơn | `governor.lag_slo` thấp hơn, `governor.tick` | Khởi động lại |
| Ngưỡng breaker | `governor.breaker.replica_lag.max`, `strand_query_p99.max_ratio` | Khởi động lại |
| Target bắt kịp nhanh hơn | `commit_order: relaxed`, tăng `resources.workers.max` | Khởi động lại |
| Batch lớn hơn, ít commit hơn | `target.batch.max_rows`, `max_bytes`, `max_wait` (ví dụ 50ms) | Khởi động lại |
| Đệm khi target chậm hoặc source sắp xóa log | `resources.spool.hard`, `resources.spool.overflow` | Khởi động lại |
| Không chạm primary | Trỏ `source.dsn` vào replica (`log_replica_updates=ON`) | Khởi động lại |

Khởi động lại an toàn nhờ checkpoint trong target. Chưa có ở P1: tự co giãn số worker (`workers.min`), điều tốc stream, governor GRACE v2 (P2-07).

## 8. Exit code, xử lý sự cố và giám sát

Exit 1 là lỗi tạm thời nên được tự restart; exit 2–5 và 10 cần người xem (systemd `RestartPreventExitStatus=2 3 4 5 10`).

| Exit | Mã lỗi | Nguyên nhân | Xử lý |
| --- | --- | --- | --- |
| 1 | (không phân loại) | Mất kết nối, lỗi tạm | Tự restart; xem log nếu lặp lại |
| 2 | `invalid_syntax` | Cấu hình sai, khóa lạ, cờ sai | Sửa cấu hình |
| 3 | `binlog_config_invalid`, `missing_pk`, `type_unmappable`, `seed_invariant_failed`, `prerelease_tool` | validate, plan hoặc seed plan từ chối | Sửa đúng điều kiện nêu trong thông báo |
| 4 | `pipeline_locked`, `fenced` | Instance khác cùng `name` đang chạy hoặc đã thay thế instance này | Bảo đảm chỉ một instance |
| 5 | `log_gap` | Source đã xóa binlog Strand chưa apply | Cấp lại từ archive (`--archive`), hoặc seed lại |
| 5 | `prefetch_exhausted` | Spool prefetch đầy (`--spool-hard`) | Tăng spool, thêm tầng bucket |
| 5 | `verify_mismatch` | Verify thấy row lệch | `verify --repair fix`, điều tra nguyên nhân |
| 10 | `schema_change_pending` | DDL trên bảng của pipeline (kể cả `CREATE`/`RENAME` tạo bảng khớp bộ lọc) mà target chưa khớp | Áp cùng DDL ở target rồi chạy lại `sync`; Strand so cột và đi tiếp |
| 10 | `promote_needs_decision` | Promote không fence được source | Cô lập source, chạy lại với `--source-fenced-externally` |

Bảng được `RENAME` từ bảng không thuộc pipeline vào bộ lọc cũng dừng exit 10; chạy lại thì Strand snapshot bảng đó. Thay đổi user và quyền không được đồng bộ; tạo tay ở cả hai bên.

### Giám sát

- `GET /healthz` trên `observability.metrics`: 200 khi vòng chính còn tiến (liveness probe).
- `GET /metrics` (Prometheus): `strand_lag_seconds`, `strand_apply_txn_total`, `strand_apply_rows_total`, `strand_holes`, `strand_writeset_entries`, `strand_spool_bytes` / `strand_spool_capacity_bytes`, `strand_retention_headroom_seconds`, `strand_governor_zone` / `strand_governor_pressure` / `strand_governor_rate_tokens`, `strand_breaker_open`, `strand_snapshot_ranges`, `strand_seed_progress_ratio`, `strand_verify_mismatch_total`, `strand_verify_unverifiable_hot_total`, `strand_fenced_total`, `strand_checkpoint_compaction_lag_seconds`.
- Cảnh báo nên có: lag vượt `lag_slo` kéo dài, `retention_headroom` sắp về 0 (sắp `log_gap`), spool > 80% dung lượng, `verify_mismatch_total` tăng, process thoát với exit 2–5 hoặc 10.

## 9. Giới hạn của P1 và quyết định chờ cập nhật spec

P1 là đồng bộ một chiều MySQL → MySQL; những điểm dưới đây cần biết trước khi đưa vào dùng.

- Bắt buộc `gtid_mode=ON`; không hỗ trợ định vị bằng file + position.
- Không đồng bộ hai chiều (không lọc nguồn gốc thay đổi, không giải quyết xung đột).
- Chỉ chép dữ liệu bảng có PK; không chép user, quyền, view, routine, trigger, event (seed bằng XtraBackup/CLONE thì bản backup mang theo chúng).
- DDL không tự áp: Strand dừng `schema_change_pending` cho tới khi target khớp (`approve` ở P3).
- Snapshot logic chỉ cho PK một cột số nguyên; bảng khác phải seed từ backup.
- Một process cho mỗi pipeline, chưa tự failover (HA ở P4); số worker cố định.
- Hiệu năng đo trên máy test (container): khoảng 2.100 txn/s relaxed, 1.600 txn/s strict; chưa có benchmark đối chứng.

### Quyết định đã chốt trong code, chờ owner cập nhật spec

Chi tiết từng dòng nằm trong `questions/DECISIONS-2026-11-02.md`.

| # | Nội dung | Phần spec |
| --- | --- | --- |
| 1A | `strict`: một worker, row toàn cục ghi trong transaction commit | 02 §4, §6 |
| 2B | Snapshot logic chỉ PK một cột số nguyên ở P1 | 02 §5, 01 §5 |
| 3B | `pause --snapshot`, `resume --snapshot-only` | 02 §10 |
| 4A | Giữ bảng `_strand_promote` | 02 §4 |
| 5 | `foreign_key_checks=0` cho phiên ghi; KILL kết nối instance cũ | 02 §8, 03 §2 |
| 6 | Cờ và khóa cấu hình mới | 02 §10 |
| 7 | Failover: exit 10 hiển thị phần dữ liệu sẽ mất, xác nhận bằng `--source-fenced-externally` | 02 §8, 05 §5 |
| 8 | `--type mysqldump` kèm cảnh báo bắt buộc | 02 §9 |
| 9 | mydumper bắt buộc `--dump-log` | 02 §9, §10 |
| 10 | RS-1: chờ GTID của event đã bỏ trước SELECT chunk | 02 §5 |
| 11 | validate kiểm từng session đang mở | 02 §11, 03 §2 |
| 12 | DDL: đi tiếp khi schema target khớp; xét bảng mới | 02 §12 |
| 13 | Mặc định group commit; `workers.min` chưa có | 02 §6, §10 |
| 14 | Soak trước Gate 1: 12 giờ thay 72 giờ | 04 §1 |
