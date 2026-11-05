# Kế hoạch Web GUI cho Strand (tham khảo Apache Flink Web Dashboard)

Ngày 2026-11-05. Owner yêu cầu: giao diện giống Flink, **một tài khoản admin**, không cầu kỳ, nhưng **đủ mọi thao tác** mà CLI đang có. Spec xếp Web UI vào P4 (sau control plane). Làm sớm là lệch kế hoạch: ghi thành một quyết định mới trong `questions/DECISIONS-2026-11-02.md` khi bắt đầu. Không sửa `spec/`.

## 1. Flink làm gì, Strand lấy gì

| Flink Dashboard | Strand tương ứng |
| --- | --- |
| Overview: số TaskManager/slot, danh sách job đang chạy và đã xong | Overview: số pipeline theo trạng thái (`synced`, `bootstrapping`, `paused`, `failed`, `stopped`), lag lớn nhất, tổng rows/s, danh sách pipeline |
| Running / Completed Jobs | Danh sách pipeline: tên, cặp engine (MySQL→PG…), trạng thái, lag, rows/s, uptime, zone governor, exit code lần dừng gần nhất |
| Job graph (vertex, parallelism, records in/out, backpressure) | Sơ đồ pipeline: Source reader → Assembler → Scheduler → Writer ×N → Target. Mỗi nút hiện số liệu có sẵn: event/s, holes, writeset entries, spool bytes, lag |
| Subtasks của vertex | Tab **Tables**: từng bảng, trạng thái snapshot (pending / running / done), số range, tiến độ, ETA, lệch verify |
| Tab Checkpoints (history, summary, config) | Tab **Checkpoint**: vị trí đã apply (GTID/LSN), epoch, thời điểm cập nhật, compaction lag, nút **Rewind** |
| Tab Exceptions | Tab **Errors**: lỗi/cảnh báo gần nhất, exit code kèm ý nghĩa và cách xử lý (bảng 8.3 của USAGE) |
| Tab Configuration | Tab **Config**: cấu hình hiệu lực (che mật khẩu), các khóa đang bị cảnh báo "không có tác dụng" |
| Logs / stdout của TaskManager | Tab **Logs**: tail journal của `strand@<name>` |
| Nút Cancel Job | Nút **Stop / Start / Restart**, **Pause / Resume** |
| Submit New Job | Trang **New pipeline**: form tạo `/etc/strand/<name>.yaml` + `.env`, chạy `validate` và `plan`, rồi Start (mốc M5) |
| REST API (CLI và UI dùng chung) | REST `/api/v1/…` (JSON), UI gọi chính API này; script dùng được |
| Không có xác thực (dựa vào proxy) | **Có**: một tài khoản admin, session cookie (mục 4) |

## 2. Kiến trúc

Thêm lệnh mới `strand web` trong cùng binary (package mới `internal/web`, cần ADR theo spec 06). Nó chạy như một service riêng (`strand-web.service`) trên **cùng máy** với các pipeline.

```
trình duyệt ──HTTPS──> strand web ──unix socket──> strand@a (sync)   admin socket: top, pause, throttle, …
                           │        ──unix socket──> strand@b (sync)
                           │──systemctl/journalctl──> start / stop / status / log
                           │──exec `strand …`──────> verify, promote, validate, plan, schema init --dry-run
                           └──DB (DSN của pipeline)─> đọc _strand_checkpoint/_strand_promote khi pipeline đang dừng
```

- **Phát hiện pipeline:** quét `/etc/strand/*.yaml` (bố cục của gói `.deb`/`.rpm`). Tên pipeline là tên file. Lấy socket từ `observability.admin_socket`, credential từ `/etc/strand/<name>.env`.
- **Pipeline đang dừng hoặc đã lỗi** (không còn socket): trạng thái lấy từ `systemctl show` (ActiveState, ExecMainStatus = exit code). Vị trí lấy từ bảng checkpoint ở target.
- **Thao tác dài** (verify full, repair, promote): chạy `strand <lệnh>` thành tiến trình con, truyền đối số dạng mảng (**không** qua `sh -c`), stream output lên UI, lưu lịch sử. Không cài lại logic của CLI trong web; mọi điều kiện an toàn của CLI (repair cần pipeline dừng, promote có trạng thái…) giữ nguyên.
- **Biểu đồ:** web lấy mẫu admin socket và `/metrics` mỗi 5 giây, giữ ring buffer 1 giờ trong RAM. Lịch sử dài vẫn dùng Prometheus.
- **Quyền chạy:** `strand-web` cần đọc `/etc/strand/*.env` (root:strand 0600) và gọi `systemctl start/stop strand@*`. Phương án v1: chạy root với hardening của systemd (`ProtectSystem=strict`, `NoNewPrivileges`, chỉ ghi `/var/lib/strand-web`). **Owner cần duyệt điểm này** (mục 7).
- **Frontend:** HTML render phía server (`html/template`), cộng **htmx** cho cập nhật một phần và **uPlot** cho biểu đồ. Cả hai là file JS vendored, nhúng bằng `go:embed`. Một binary, không cần toolchain Node, không thêm Go dependency.
- **v1 chỉ quản lý pipeline trên cùng máy.** Nhiều máy cần admin qua TCP + TLS, đó là control plane của P4, ngoài phạm vi này.

## 3. Thao tác (đủ như CLI)

| Thao tác | Cách thực hiện | Xác nhận |
| --- | --- | --- |
| Start / Stop / Restart pipeline | `systemctl start\|stop\|restart strand@<name>` | Stop / Restart: hộp thoại |
| Enable / Disable (khởi động cùng máy) | `systemctl enable\|disable` | — |
| Pause / Resume, Pause snapshot / Resume snapshot-only | admin socket | — |
| Throttle (soft IOPS) | admin socket | — |
| Snapshot add (chọn bảng từ danh sách của source) | admin socket | hộp thoại |
| Checkpoint show / **Rewind** (MySQL→MySQL) | admin socket | **gõ lại tên pipeline** |
| Verify sample / delta / full (chọn bảng, rate) | exec `strand verify` | — |
| **Repair** (`--repair fix`) | Wizard: (1) chờ lag ≈ 0, (2) Stop pipeline, (3) exec verify full fix, (4) Start | gõ lại tên pipeline |
| **Promote** | Wizard theo 5 bước, hiện trạng thái từ `_strand_promote`. Failover `--source-fenced-externally` có hộp thoại riêng, hiện phần dữ liệu sẽ mất | gõ lại tên pipeline (hai lần với failover) |
| Validate / Plan / Schema init `--dry-run` | exec, hiện kết quả | — |
| Xem / sửa cấu hình | Xem: v1. Sửa: lưu file, chạy validate trước khi lưu, Restart (M5) | hộp thoại diff |
| Tạo pipeline mới | M5 | — |

Mọi thao tác ghi **audit log**: thời điểm, IP, thao tác, pipeline, đối số, kết quả, exit code.

## 4. Xác thực: một tài khoản admin

- `strand web passwd` hỏi mật khẩu, ghi **bcrypt hash** (`golang.org/x/crypto`, đã nằm trong `deps.allow`) vào `/etc/strand/web.yaml` (0600). Không có mật khẩu mặc định: chưa đặt thì `strand web` không khởi động.
- Đăng nhập tạo session ngẫu nhiên 256 bit, lưu trong RAM. Cookie `HttpOnly`, `Secure`, `SameSite=Strict`. Hết hạn sau 30 phút không thao tác, tối đa 12 giờ.
- CSRF token cho mọi POST. Giới hạn đăng nhập sai: 5 lần/phút/IP, sau đó chờ tăng dần.
- TLS bắt buộc (`tls_cert`, `tls_key`), trừ khi chỉ bind `127.0.0.1` (dùng sau reverse proxy).
- Header bảo mật: `Content-Security-Policy: default-src 'self'`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`.
- API cho script: cùng session, hoặc token tĩnh tùy chọn trong `web.yaml` (hash).

## 5. Thay đổi cần ở `strand sync` (nhỏ, chỉ quan sát)

Admin socket hiện có: `top`, `pause`, `resume`, `throttle`, `checkpoint show/rewind`, `snapshot add`, `verify delta`. Thêm, không phá giao thức cũ:

1. `status`: epoch, uptime, vị trí bắt đầu, governor (thuật toán, rate hiện tại, áp suất từng tín hiệu, breaker), số worker, commit_order.
2. `tables`: từng bảng — trạng thái snapshot, số range theo trạng thái, row ước lượng/đã chép, ETA, có/không PK tổng quát.
3. `events`: ring buffer 200 bản ghi WARN/ERROR gần nhất (một slog handler ghi song song, không đổi log hiện có).

Không đụng đường dữ liệu (reader, scheduler, writer, checkpoint). Test: unit cho từng lệnh socket mới.

## 6. Mốc và ước lượng

| Mốc | Nội dung | Ngày |
| --- | --- | --- |
| M0 | ADR `internal/web`, quyết định trong DECISIONS, đặc tả API `/api/v1`, mockup 4 trang chính | 1 |
| M1 | Khung: `strand web`, `web.yaml`, `strand web passwd`, login/session/CSRF/rate limit, TLS, audit log, phát hiện pipeline, unit `strand-web.service` (và đưa vào gói) | 2 |
| M2 | Trang chỉ đọc: Overview, danh sách, chi tiết (sơ đồ, Tables, Checkpoint, Errors, Config, Logs), cập nhật trực tiếp, biểu đồ; 3 lệnh socket mới ở `sync` | 3–4 |
| M3 | Thao tác: start/stop/restart/enable, pause/resume, throttle, snapshot add, rewind; chạy nền verify sample/delta/full với output trực tiếp và lịch sử | 3 |
| M4 | Wizard Repair và Promote (kể cả failover) | 2 |
| M5 | Sửa cấu hình, tạo pipeline mới | 2 |
| M6 | Test và review bảo mật (mục 8) | 2 |

Tổng khoảng 15 ngày làm việc. M1–M4 là phần owner yêu cầu, M5 làm được sau.

## 7. Điểm cần owner quyết trước khi làm

1. `strand-web` chạy **root** (đơn giản; cần để đọc `.env` và `systemctl`), hay chạy user `strand` kèm quy tắc polkit/sudo và đổi quyền file `.env` (root:strand 0640)?
2. v1 chỉ một máy (đề xuất), hay cần nhiều máy ngay (khi đó phải làm admin qua TCP + TLS trước)?
3. Có cần M5 (sửa cấu hình, tạo pipeline qua web) trong bản đầu không?

## 8. Nghiệm thu

- Không đăng nhập: mọi trang và mọi `/api/v1` trả 401/302. POST không có CSRF token: 403. Sai mật khẩu 6 lần/phút: bị chặn.
- Mỗi thao tác ở mục 3 có test HTTP với pipeline thật (MySQL→MySQL và PG→PG, binary thật): kết quả giống hệt khi chạy CLI tương ứng, và có dòng audit log.
- Repair qua wizard trên pipeline đang có tải: 0 lệch sau khi chạy lại (như e2e P1-15).
- Promote qua wizard: cùng kịch bản với e2e promote hiện có; failover hiện đúng phần dữ liệu sẽ mất.
- Pipeline bị `kill -9`: trang chuyển sang `failed`/`stopped`, hiện exit code và log cuối, Start chạy lại được.
- Không có đối số nào từ form đi qua shell; tên pipeline và tên bảng được kiểm theo whitelist ký tự.
- `strand sync` không chậm đi: benchmark hiện có, có và không có web lấy mẫu mỗi 5 giây.
