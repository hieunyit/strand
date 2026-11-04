# Công cụ Soak Test (`tools/soak`)

Công cụ `tools/soak` thực hiện kiểm thử bền bỉ (soak test) cho pipeline Strand CDC theo đặc tả tại `spec/04-testing.md` §2 ("Staging, tải đều + lỗi định kỳ, 72 giờ trước mỗi gate; kiểm rò rỉ bộ nhớ, phình checkpoint, lệch tích lũy").

## 1. Hành vi chính

1. **Sinh tải liên tục trên source**: tự động tạo lược đồ `app` (`app.accounts`, `app.orders`, `app.events`, `app.ignored`) trên cả source và target nếu chưa có, nạp dữ liệu ban đầu và chạy đồng thời `--workload-clients` kết nối ghi liên tục (cập nhật khóa nóng theo phân bố Zipf, chèn cha–con có FK, xóa, đổi PK của `app.events`, đổi chéo unique key `code` trên `app.accounts`, ghi bảng `app.ignored` nằm ngoài bộ lọc, và giao dịch nhiều dòng; cột `ver = UUID_SHORT()`).
2. **Supervisor quản lý `strand sync`**: chạy `<strand> sync -c <config>` như tiến trình con và tự động khởi động lại khi tiến trình thoát với mã `∉ {2, 3, 4, 5, 10}` (`RestartPreventExitStatus` tại `spec/03-deploy.md` §4). Nếu gặp mã thoát thuộc `{2, 3, 4, 5, 10}`, công cụ dừng ngay đợt soak test và đánh dấu `FAIL`.
3. **Tiêm lỗi định kỳ (`--fault-every`)**: mỗi chu kỳ chọn ngẫu nhiên một trong ba lỗi:
   - `kill -9` (`SIGKILL`): buộc tiến trình dừng đột ngột để kiểm tra phục hồi và compaction (CP-1, CP-2).
   - `SIGTERM`: yêu cầu dừng an toàn và kiểm tra mã thoát phải bằng `0`.
   - `SIGSTOP` trong 30–120 giây rồi `SIGCONT`: đóng băng tạm thời tiến trình rồi cho chạy tiếp.
4. **Lấy mẫu mỗi 60 giây**: ghi nhận bộ nhớ thực `VmRSS` từ `/proc/<pid>/status`, độ dài cột `applied` của `_strand_checkpoint` cộng tổng độ dài `_strand_worker_delta.applied`, độ trễ tính bằng số GTID chênh lệch giữa `@@GLOBAL.gtid_executed` trên source và tập đã apply trên target (`checkpoint.LoadAppliedSnapshotMySQL`), cùng tổng số lần restart.
5. **Đối chiếu trạng thái cuối**: khi hết `--duration`, dừng bộ sinh tải, chờ target bắt kịp toàn bộ `gtid_executed` của source (tối đa 30 phút), gửi `SIGTERM` dừng `strand`, sau đó so sánh streaming các bảng `app.accounts`, `app.orders`, `app.events`: đọc song song source và target theo PK tăng dần bằng keyset pagination (`WHERE id > ? ORDER BY id LIMIT --compare-chunk-rows`, mặc định 10000) rồi merge-compare (thiếu ở target, thừa ở target, lệch giá trị theo từng cột), giữ tối đa 20 vi phạm và đếm tổng. Bộ nhớ là O(chunk), không phụ thuộc kích thước bảng (bản cũ dump toàn bộ bảng vào map ≈ 536 B/dòng nên với `app.events` 65M dòng cần >35 GB và driver bị OOM-kill).
6. **Trần RSS của chính driver** (`--driver-rss-limit-mb`, mặc định 2048): RSS của driver được kiểm tra mỗi `--driver-rss-check-every` (5s) suốt cả pha chạy lẫn pha kiểm tra cuối, in ra dòng tiến độ (`driver_rss=`) và lưu trong mẫu/báo cáo. Vượt trần thì đợt soak dừng với `pass=false` và `error_reason` nêu rõ thay vì bị OOM-killer giết mà không có báo cáo.

---

## 2. Cách chạy với `make up` và cấu hình mẫu

### Bước 1: Dựng hạ tầng MySQL và biên dịch binary

```bash
make up
go build -o ./bin/strand ./cmd/strand
go build -o ./bin/soak ./tools/soak
```

Nếu dùng Docker Compose trong `dev/compose.yaml`, hãy kiểm tra cổng host được ánh xạ tới `mysql-src` và `mysql-dst` (hoặc chạy trực tiếp trên VM/mạng container):

```bash
docker compose -p strand-dev -f dev/compose.yaml ps
```

### Bước 2: Tạo tệp cấu hình `pipeline.yaml` mẫu

```yaml
version: 1
name: soak-orders
source:
  kind: mysql
  dsn: "root:dev@tcp(127.0.0.1:3306)/app"
  server_id: 41001
  read_from: primary
target:
  kind: mysql
  dsn: "root:dev@tcp(127.0.0.1:3307)/app"
  write_mode: upsert
  commit_order: relaxed
  batch:
    max_rows: 2000
    max_bytes: 4MiB
    max_wait: 50ms
tables:
  include: ["app.accounts", "app.orders", "app.events"]
  exclude: ["app.ignored"]
bootstrap:
  default: logical
checkpoint:
  store: target
  flush_every: 500ms
resources:
  spool:
    path: /tmp/strand-soak-spool
    soft: 2GiB
    hard: 4GiB
  source:
    iops:
      soft: 1000
      hard: 1500
  workers:
    min: 1
    max: 8
governor:
  algorithm: onoff
  tick: 1s
  lag_slo: 5s
  breaker:
    strand_query_p99:
      max_ratio: 1000
    replica_lag:
      max: 1h
verify:
  delta: off
  post_seed_window: 5s
observability:
  metrics: "127.0.0.1:9464"
  admin_socket: "/tmp/strand-soak-admin.sock"
  log:
    level: info
    format: json
```

### Bước 3: Chạy `soak`

Chạy đủ 72 giờ trước Gate:

```bash
./bin/soak \
  --strand ./bin/strand \
  --config ./pipeline.yaml \
  --source "root:dev@tcp(127.0.0.1:3306)/app" \
  --target "root:dev@tcp(127.0.0.1:3307)/app" \
  --duration 72h \
  --fault-every 10m \
  --workload-clients 8 \
  --span 100000 \
  --report soak-report.json
```

Chạy lại đúng kịch bản ngẫu nhiên từ một `seed` đã in ra log:

```bash
./bin/soak \
  --strand ./bin/strand \
  --config ./pipeline.yaml \
  --source "root:dev@tcp(127.0.0.1:3306)/app" \
  --target "root:dev@tcp(127.0.0.1:3307)/app" \
  --seed 1793627814000000000 \
  --duration 1h \
  --fault-every 5m \
  --report soak-report.json
```

---

## 3. Các cờ dòng lệnh

| Cờ | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `--strand` | `strand` | Đường dẫn tới binary `strand` |
| `--config` | *(bắt buộc)* | Đường dẫn tệp cấu hình `pipeline.yaml` |
| `--source` | Lấy từ `--config` | DSN kết nối tới MySQL source |
| `--target` | Lấy từ `--config` | DSN kết nối tới MySQL target (nơi lưu bảng `_strand_*`) |
| `--duration` | `72h` | Thời lượng chạy soak test |
| `--fault-every` | `10m` | Chu kỳ tiêm lỗi ngẫu nhiên (`kill -9`, `SIGTERM`, `SIGSTOP`/`SIGCONT`) |
| `--workload-clients` | `8` | Số goroutine sinh tải đồng thời trên source |
| `--span` | `100000` | Phạm vi khóa cơ sở của bộ sinh tải |
| `--report` | `soak-report.json` | Đường dẫn xuất tệp báo cáo JSON |
| `--seed` | Thời gian hiện tại | Hạt giống ngẫu nhiên (in ra stdout khi khởi chạy để tái lập) |
| `--checkpoint-max-bytes` | `65536` | Ngưỡng kích thước checkpoint tối đa cho phép (byte, theo CP-4) |
| `--rss-growth-max` | `5.0` | Ngưỡng tốc độ tăng RSS tối đa cho phép trên nửa sau thời gian (MB/giờ) |
| `--driver-rss-limit-mb` | `2048` | Trần RSS (MiB) của chính driver soak; vượt trần thì FAIL với lý do rõ ràng (`0` = tắt) |
| `--driver-rss-check-every` | `5s` | Chu kỳ kiểm tra RSS của driver |
| `--compare-chunk-rows` | `10000` | Số dòng mỗi chunk keyset khi so sánh trạng thái cuối |
| `--compare-only` | `false` | Chỉ chạy so sánh streaming source/target rồi thoát (không sinh tải, không chạy `strand`) |
| `--pprof-addr` | *(tắt)* | Phục vụ `net/http/pprof` của driver (ví dụ `127.0.0.1:6060`) để lấy hồ sơ heap |

---

## 4. Ý nghĩa báo cáo JSON (`soak-report.json`)

- `seed`: Hạt giống ngẫu nhiên đã dùng cho bộ sinh tải và bộ tiêm lỗi.
- `duration`, `duration_seconds`, `configured_duration`: Thời lượng thực tế đã chạy và thời lượng cấu hình.
- `faults_injected`: Số lần tiêm từng loại lỗi (`kill9`, `sigterm`, `sigstop`).
- `restarts`: Tổng số lần supervisor khởi động lại tiến trình `strand`.
- `samples`: Danh sách mẫu thu thập mỗi 60 giây, gồm:
  - `timestamp`, `elapsed_seconds`: Thời điểm lấy mẫu và số giây tính từ lúc bắt đầu.
  - `rss_bytes`, `rss_mb`: Bộ nhớ `VmRSS` của tiến trình `strand`.
  - `checkpoint_applied_bytes`: Độ dài cột `applied` trong bảng `_strand_checkpoint`.
  - `worker_delta_applied_bytes`: Tổng độ dài cột `applied` trong bảng `_strand_worker_delta` của pipeline.
  - `checkpoint_bytes`: Tổng `checkpoint_applied_bytes + worker_delta_applied_bytes`.
  - `lag_gtids`: Số GTID đã thực thi trên source nhưng chưa có trong tập đã apply trên target.
  - `restarts`: Số lần restart tính đến thời điểm lấy mẫu.
  - `driver_rss_bytes`, `driver_rss_mb`: `VmRSS` của chính tiến trình driver soak.
- `driver_rss_max_bytes`, `driver_rss_limit_bytes`: RSS lớn nhất của driver trong cả đợt (gồm pha so sánh cuối) và trần đã cấu hình.
- `violations` (tối đa 20), `violations_total`, `compared_rows_source`, `compared_rows_target`: Kết quả so sánh streaming trạng thái cuối.
- Kết luận (`conclusion` và các trường cùng tên ở cấp cao nhất):
  - `rss_growth`: Hệ số góc hồi quy tuyến tính (MB/giờ) của `rss_mb` theo thời gian trên **nửa sau** khoảng thời gian chạy (loại bỏ giai đoạn cấp phát bộ nhớ/cache ban đầu).
  - `checkpoint_max_bytes`: Giá trị `checkpoint_bytes` lớn nhất ghi nhận được trong suốt đợt chạy.
  - `final_state_equal`: `true` khi target bắt kịp source, dừng an toàn và toàn bộ dữ liệu trên `app.accounts`, `app.orders`, `app.events` khớp hoàn toàn với source (so sánh streaming theo PK, cùng ngữ nghĩa giá trị với `checker.RowState.Hash`).
  - `forbidden_exit`, `forbidden_exit_code`: Ghi nhận nếu `strand` thoát với mã cấm tự động restart (`{2, 3, 4, 5, 10}`) hoặc thoát khác `0` khi nhận `SIGTERM`.
  - `pass`: Kết quả tổng hợp cuối cùng:
    $$\text{pass} = \text{final\_state\_equal} \land \neg\text{forbidden\_exit} \land (\text{checkpoint\_max\_bytes} \le 65536) \land (\text{rss\_growth} \le 5\,\text{MB/giờ})$$
