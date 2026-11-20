# Strand Web API `/api/v1`

Đặc tả REST của `strand web` (kế hoạch: `docs/PLAN-WEB-UI.md`, quyết định: `questions/DECISIONS-WEB-UI-2026-11-15.md`). Giao diện web và script dùng chung các route này. Phiên bản M5: các route đọc, đăng nhập/đăng xuất, **thao tác** (§4), **job** (§4.3), **sửa cấu hình** (§3.1) và **tạo pipeline mới** (§3.2).

## 1. Quy ước

- JSON UTF-8. Lỗi: `{"error": "<mô tả>"}` kèm mã HTTP.
- Mọi route (trừ đăng nhập) đòi xác thực. Không xác thực: **401** (API), **302 → /login** (trang HTML).
- Header bảo mật trên mọi phản hồi: `Content-Security-Policy: default-src 'self'`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `X-Content-Type-Options: nosniff`, `Cache-Control: no-store`.
- Tên pipeline trong đường dẫn phải khớp `^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$` và đã được đăng ký; ngược lại **404**.

## 2. Xác thực và Phân quyền (RBAC)

### 2.1 Phương thức xác thực

1. **Session** (trình duyệt). `GET /login` đặt cookie `strand_login` (giá trị = token CSRF của lần đăng nhập). `POST /api/v1/login` với header `X-CSRF-Token: <giá trị cookie strand_login>` và body `{"username": "...", "password": "..."}`. Thành công: **200** `{"csrf": "<token>", "idle_timeout_s": 1800, "max_age_s": 43200}` và cookie `strand_session` (`HttpOnly`, `SameSite=Strict`, `Secure` khi có TLS; ID phiên được xoay mới sau khi đăng nhập thành công). Mọi request `POST` sau đó mang header `X-CSRF-Token: <csrf>` (hoặc trường form `csrf`); thiếu/sai: **403**.
   - **Đổi mật khẩu lần đầu (First login):** Tài khoản có cờ `must_change_password=true` khi đăng nhập thông thường sẽ nhận phản hồi **400** `{"error": "password change required on first login", "must_change_password": true}`. Client gửi thêm trường `"new_password": "..."` trong payload đăng nhập để cập nhật mật khẩu mới và hoàn tất đăng nhập.
2. **API Token người dùng (Scoped Bearer Token):** Mỗi người dùng có thể tạo và quản lý token riêng trên trang My account (`/account`) hoặc API `/api/v1/account/tokens`. Gửi header `Authorization: Bearer <token>`. Quyền hạn của token tự động kế thừa theo vai trò của người dùng tạo ra nó. Khi người dùng bị vô hiệu hóa, bị xóa, hoặc token bị thu hồi: trả về **401 Unauthorized**.
3. **Legacy Global API Token:** Token API toàn cục cũ (`strand web token`) được hỗ trợ kế thừa với quyền `admin`. Mọi phản hồi khi dùng token này có thêm header `Deprecation: true` và `Warning: 299 - "Legacy API token is deprecated; migrate to per-user API tokens"`.

Giới hạn đăng nhập: Tối đa 5 lần sai/phút/IP hoặc per-user, lần thứ 6 trả **429 Too Many Requests** kèm `Retry-After`; thời gian khóa 15 phút. Phiên hết hạn sau 30 phút không dùng hoặc tối đa 12 giờ. Khi tài khoản bị vô hiệu hóa hoặc đổi mật khẩu, toàn bộ phiên đăng nhập của người dùng đó bị hủy lập tức.

### 2.2 Bảng vai trò và quyền hạn (RBAC)

Hệ thống phân quyền trung tâm gồm 3 vai trò:

| Quyền / Route | `viewer` | `operator` | `admin` |
| --- | :---: | :---: | :---: |
| Xem trạng thái, dashboard, số liệu, tables, checkpoint | ✅ | ✅ | ✅ |
| Xem cấu hình pipeline (`/api/v1/pipelines/{name}/config` — bí mật luôn che) | ✅ | ✅ | ✅ |
| Vận hành pipeline (start, stop, restart, pause, resume, throttle, snapshot add) | ❌ (403) | ✅ | ✅ |
| Chạy các tác vụ verify, repair, backup run (`/api/v1/backup-jobs/{id}/run`) | ❌ (403) | ✅ | ✅ |
| Quản lý tài khoản cá nhân (`/account`, `/api/v1/account/*`) | ✅ | ✅ | ✅ |
| Sửa cấu hình pipeline, lưu YAML (`/api/v1/pipelines/{name}/config`) | ❌ (403) | ❌ (403) | ✅ |
| Xem và sửa biến môi trường pipeline (`/api/v1/pipelines/{name}/env`) | ❌ (403) | ❌ (403) | ✅ |
| Tạo / xóa pipeline mới (`POST /api/v1/pipelines`, `DELETE /api/v1/pipelines/{name}`) | ❌ (403) | ❌ (403) | ✅ |
| Thao tác sao lưu nâng cao (tạo/sửa job, restore, delete backup) | ❌ (403) | ❌ (403) | ✅ |
| Cài đặt hệ thống (`/settings`, `/api/v1/settings`) | ❌ (403) | ❌ (403) | ✅ |
| Quản lý người dùng (`/users`, `/api/v1/users/*`) | ❌ (403) | ❌ (403) | ✅ |
| Xem / xuất Audit log (`/audit`, `/api/v1/audit/*`) | ❌ (403) | ❌ (403) | ✅ |

Khi người dùng không đủ quyền truy cập, API trả về JSON nhất quán:
```json
{
  "error": "forbidden: this action requires <role> role",
  "code": "FORBIDDEN"
}
```
Trên trình duyệt (HTML route), trả về trang 403 Forbidden với thông báo quyền hạn rõ ràng.

### 2.3 Route xác thực & tài khoản

| Route | Phương thức | Quyền | Mô tả |
| --- | --- | --- | --- |
| `/api/v1/login` | POST | Public | Đăng nhập bằng `username` và `password` (kèm `new_password` nếu đổi lần đầu). |
| `/api/v1/logout` | POST | Auth | Đăng xuất, hủy phiên hiện tại. |
| `/healthz` | GET | Public | Healthcheck và trạng thái cluster lease. |
| `/api/v1/session` | GET | Auth | Thông tin phiên hiện tại, username, role, CSRF token. |
| `/api/v1/account/password` | POST | Auth | Đổi mật khẩu tài khoản hiện tại (`current_password`, `new_password`). |
| `/api/v1/account/tokens` | GET | Auth | Liệt kê danh sách API token của tài khoản. |
| `/api/v1/account/tokens` | POST | Auth | Tạo API token mới (`name`). Trả token dạng rõ một lần duy nhất. |
| `/api/v1/account/tokens/{id}` | DELETE | Auth | Thu hồi API token. |
| `/api/v1/account/sessions` | GET | Auth | Danh sách các phiên đăng nhập đang hoạt động của tài khoản. |
| `/api/v1/account/sessions/signout-others` | POST | Auth | Đăng xuất toàn bộ các phiên khác của tài khoản này. |

### 2.4 Route quản lý người dùng (Admin only)

| Route | Phương thức | Quyền | Mô tả |
| --- | --- | --- | --- |
| `/api/v1/users` | GET | Admin | Liệt kê tất cả người dùng, vai trò, trạng thái, last login. |
| `/api/v1/users` | POST | Admin | Tạo người dùng mới (`username`, `role`, `display_name`, `password`, `must_change_password`). |
| `/api/v1/users/{username}/role` | PUT | Admin | Cập nhật vai trò người dùng (`admin`, `operator`, `viewer`). |
| `/api/v1/users/{username}/disable` | POST | Admin | Vô hiệu hóa người dùng (hủy mọi phiên đăng nhập). |
| `/api/v1/users/{username}/enable` | POST | Admin | Kích hoạt lại người dùng. |
| `/api/v1/users/{username}/reset-password` | POST | Admin | Đặt lại mật khẩu (`password`, hoặc sinh ngẫu nhiên). |
| `/api/v1/users/{username}/tokens/revoke` | POST | Admin | Thu hồi toàn bộ API token của người dùng này. |
| `/api/v1/users/{username}` | DELETE | Admin | Xóa người dùng và toàn bộ phiên/token liên quan. |

## 3. Tài nguyên

Trạng thái (`state`) của pipeline: `synced`, `bootstrapping`, `paused`, `starting`, `failed`, `stopped`, `unreachable` (tiến trình sống nhưng admin socket không trả lời), `unknown`. Khi admin socket trả lời thì lấy trạng thái từ `top`; không thì từ systemd (`ActiveState`, `ExecMainStatus`) hoặc file pid.

### `GET /api/v1/overview`

```json
{"counts": {"synced": 1, "bootstrapping": 0, "paused": 0, "starting": 0, "failed": 0, "stopped": 0, "unreachable": 0, "unknown": 0},
 "total": 1, "max_lag_seconds": 0.25, "max_lag_pipeline": "orders", "rows_per_sec": 1234.5,
 "warnings": ["scan: ... skipped ..."],
 "pipelines": [ <PipelineView> ]}
```

### `GET /api/v1/pipelines` → `{"pipelines": [PipelineView], "warnings": []}`

### `POST /api/v1/pipelines` (Tạo pipeline mới)

Body JSON (hoặc Form URL-encoded):
`{"name": "...", "from": "mysql|mariadb|postgres", "to": "mysql|postgres", "source_host", "source_port", "target_host", "target_port", "source_user", "target_user", "source_password", "target_password", "source_password_env", "target_password_env", "database", "databases", "target_database", "target_pg_database", "schema", "include", "exclude", "commit_order", "spool_dir", "workers", "server_id", "iops_soft", "iops_hard", "metrics", ...}`. `include`/`exclude`/`databases` là mảng chuỗi (JSON) hoặc chuỗi phân cách dấu phẩy (form). Số nguyên sai ở form: **400** nêu tên trường. `databases` (hoặc `database`, nhiều tên cách dấu phẩy) sinh `tables.include` `<db>.*` cho nguồn MySQL/MariaDB. `target_database` (target MySQL) là database chứa `_strand_*`, mặc định `strand` (khác database dữ liệu; phải `CREATE DATABASE` trước khi sync). `target_pg_database` (target PG) là database PostgreSQL đích để chép vào, mặc định cùng tên database nguồn; bắt buộc khi nguồn MySQL là `*`. Cặp không hỗ trợ (`postgres`→`mysql`, `mariadb`→`postgres`): **400**. `metrics` mặc định là cổng trống đầu tiên từ `127.0.0.1:9464`; trùng cổng pipeline khác: **409**. Admin socket mặc định `<spool>/admin.sock`, tự chuyển sang đường dẫn ngắn hơn khi vượt giới hạn 107 byte.

Tạo cấu hình YAML và file `.env` (mode `0600`) trong `config_dir` (khai báo trong `web.yaml`, mặc định `/etc/strand` nếu ghi được, nếu không thì `<state_dir>/pipelines`). Mật khẩu chỉ ghi vào `.env`, không bao giờ vào YAML. Chạy `strand validate` và `strand plan`, thêm pipeline vào registry (lưu bền trong `<state_dir>/pipelines.json`, mode `0600`, không sửa `web.yaml`; entry cùng tên trong `web.yaml` thắng), gọi `Registry.Refresh`. File `.env` ghi dạng `KEY=VALUE` (giá trị đặc biệt đặt trong nháy kép, tương thích systemd `EnvironmentFile`); mật khẩu chứa xuống dòng bị từ chối 400.
Trả về **200** (cả khi validate lỗi; xem `ok`):
```json
{
  "ok": true,
  "pipeline": "orders-sync",
  "config_path": "/etc/strand/orders-sync.yaml",
  "env_path": "/etc/strand/orders-sync.env",
  "validate": {"exit_code": 0, "output": "..."},
  "plan": {"exit_code": 0, "output": "..."},
  "message": "Pipeline created successfully"
}
```
`ok` = `strand validate` thoát 0. Nếu validate lỗi: vẫn **200**, `ok: false`, `message` bắt đầu "Pipeline files were created but validation failed (exit N)...", file YAML/`.env` được giữ lại và pipeline đã đăng ký để sửa ở tab Config; phải sửa cho validate qua rồi mới Start. `plan` chỉ để tham khảo.
Lỗi: **400** tên sai whitelist hoặc tham số sai, **409** tên pipeline hoặc file đã tồn tại. Audit: `pipeline.create`.

`PipelineView`:

```json
{"name": "orders", "pair": "mysql → postgres", "mode": "systemd|background", "origin": "scan|web.yaml",
 "config_path": "/etc/strand/orders.yaml", "state": "synced", "admin_reachable": true,
 "runtime": {"mode": "systemd", "running": true, "active_state": "active", "sub_state": "running",
             "exit_code": 5, "pid": 1234, "since": "...", "error": "...", "last_error": "..."},
 "top": {"pipeline_id": "orders", "state": "synced", "lag_seconds": 0.25, "throughput": 1234, "zone": "green",
         "progress": 0, "eta_seconds": 0, "tables_done": 0, "tables_total": 0, "breaker_open": false},
 "last_exit": {"code": 5, "meaning": "...", "action": "...", "auto_restart": false},
 "config_error": "", "sampled_at": "2026-11-15T10:00:00Z"}
```

`runtime.exit_code` và `last_exit` chỉ có khi pipeline đang dừng và biết exit code: từ systemd, hoặc (chế độ nền) suy ra từ đuôi log. Chế độ nền: lần chạy cuối kết thúc bằng lỗi (dòng `Error...:` của CLI hoặc ERROR `pipeline failed` sau mốc khởi động gần nhất) thì `state = "failed"`, `runtime.last_error` là lý do (đã che bí mật), `last_exit.code` theo mã `[code]` hoặc 1 nếu lỗi không gắn mã; dừng êm vẫn là `stopped`.

### `GET /api/v1/pipelines/{name}` → `{"pipeline": PipelineView, "status": StatusView}`

### `GET /api/v1/pipelines/{name}/status`

`{"status": <admin status>, "error": "..."}`. `status`: `pipeline_id`, `state`, `epoch`, `started_at`, `uptime_seconds`, `start_at`, `start_position`, `source_kind`, `target_kind`, `commit_order`, `workers`, `paused`, `snapshot_paused`, `applied_txns`, `applied_rows`, `governor{algorithm, foreground_rate, background_rate, zone, pressure, breaker_open, breaker_reason}`. Pipeline dừng: chỉ có `error`.

### `GET /api/v1/pipelines/{name}/tables`

`{"tables": [{"table", "state": "done|running|pending", "ranges": {"Done": 2, "Pending": 6}, "rows_estimate", "rows_copied", "progress_pct", "eta_seconds", "pk": ["id"], "generic_pk": false}], "done", "running", "pending", "error"}`. `rows_estimate` lấy từ thống kê của source (lệch được); `rows_copied` là xấp xỉ theo tiến độ PK.

**Phân trang** (pipeline nhiều bảng): thêm bất kỳ tham số nào trong `q` (chuỗi con của tên bảng, không phân biệt hoa thường), `state` (`all|running|pending|done`), `sort` (`ord` = đang chạy trước rồi theo tên, `name`, `pct` giảm dần, `rows` giảm dần, `eta` tăng dần, chưa biết ở cuối), `page` (từ 1), `per_page` (mặc định 100, tối đa 500) thì server lọc/sắp xếp và chỉ trả một trang: `tables` = các bảng của trang, kèm `"paged": true, "query": {q, state, sort, page, per_page}, "matching", "pages", "first", "last"`; `done/running/pending/total` và các tổng vẫn là của **toàn bộ** danh sách. Không có tham số: toàn bộ danh sách như trước (tương thích).

### `GET /api/v1/pipelines/{name}/checkpoint`

`{"checkpoint": {"pipeline_id", "epoch", "applied", "updated_at"}, "start_position", "compaction_lag_seconds", "error"}`. Chỉ có khi pipeline đang chạy.

### `GET /api/v1/pipelines/{name}/errors`

`{"live_events": [LogEvent], "log_events": [LogEvent], "live_error", "last_exit": {code, meaning, action, auto_restart}, "exit_from": "systemd|log", "error_code"}`. `live_events` là ring 200 bản ghi WARN/ERROR của tiến trình đang chạy; `log_events` rút từ đuôi log (cho cả pipeline đã dừng). `last_exit` có khi pipeline dừng: từ systemd, hoặc suy ra từ mã lỗi `[code]` trong log (`exit_from = "log"`). `LogEvent`: `{"time", "level", "message"}`; mật khẩu trong message bị che. Với `log_events`, `message` gồm cả thuộc tính của bản ghi (`error`, `reason`, `hint`, `table`, ... trừ `time/level/msg/pipeline/epoch`) theo khuôn `msg  k=v  k2=v2` như `live_events`, và dòng `Error: ...` cuối của CLI (chế độ nền) nếu lý do chưa có trong sự kiện nào. `exit_from = "log"` cũng áp dụng khi chế độ nền suy ra exit code từ log.

### `GET /api/v1/pipelines/{name}/config`

`{"path", "yaml", "unimplemented_keys": [...], "error"}`. `yaml` là văn bản file cấu hình đọc thô (chưa thay `${VAR}`), giữ nguyên định dạng/chú thích/dòng trống (không tuần tự hóa lại), mọi giá trị bí mật bị thay bằng `********` (thay theo dòng; khóa có `password`, `secret`, `token`, `credential`, `private`, `encryption_key`, `access_key`, mật khẩu nhúng trong DSN/URL). Mỗi lần xem được ghi audit `config.view`.

### `POST /api/v1/pipelines/{name}/config/validate`

Ghi chú: các route nhận `yaml` (validate/lưu) khôi phục bí mật theo dòng từ văn bản đã che (`********`) của `GET .../config`; dòng `********` không khôi phục được bị từ chối **400**.

Body JSON: `{"yaml": "..."}`.
Ghi cấu hình ra file tạm cùng thư mục, chạy `strand validate -c <tmp>` với môi trường của pipeline, so sánh và tạo unified diff so với file cấu hình hiện tại, rồi xóa file tạm.
Trả về **200**:
`{"valid": true|false, "exit_code": 0, "output": "...", "diff": "--- ...\n+++ ..."}`.

### `POST /api/v1/pipelines/{name}/config`

Body JSON: `{"yaml": "...", "restart": true|false}`.
Lưu cấu hình:
1. Ghi ra file tạm cùng thư mục.
2. Chạy `strand validate -c <tmp>`. Nếu thất bại: trả **502** (hoặc **400**) kèm output của validate, xóa file tạm, **không ghi đè**.
3. Nếu validate thành công: tạo bản sao lưu `<file>.bak.<timestamp>`.
4. Ghi đè nguyên tử bằng `os.Rename`.
5. Nạp lại cấu hình và gọi `Registry.Refresh`.
6. Nếu `restart=true`: khởi động lại pipeline (systemctl restart hoặc stop rồi sync background).
Trả về **200**: `{"ok": true, "backup": "<path>", "restarted": bool, "message": "..."}`. Audit: `config.edit`.

### `GET /api/v1/pipelines/{name}/tuning`

Giá trị hiện tại của các khóa chỉnh tốc độ/an toàn snapshot, đọc từ file YAML: `{"path", "fields": [{"key", "label", "help", "yaml_key", "value", "set", "default", "options"?}], "error"?}`. `set=false` nghĩa là khóa vắng và Strand dùng `default`. Khóa (đều đã có tác dụng): `iops_soft` (`resources.source.iops.soft`), `iops_hard` (`resources.source.iops.hard`), `chunk_rows` (`bootstrap.logical.chunk_rows`), `max_parallel` (`bootstrap.logical.max_parallel`, 1–32), `windows` (`bootstrap.logical.windows`), `algorithm` (`governor.algorithm`), `lag_slo` (`governor.lag_slo`), `breaker_max_ratio` (`governor.breaker.strand_query_p99.max_ratio`), `replica_lag_max` (`governor.breaker.replica_lag.max`), `workers` (`resources.workers.max`). Khóa chỉ được nhận mà chưa dùng (USAGE.md 6.3) cố ý không có.

### `POST /api/v1/pipelines/{name}/tuning`

Body JSON phẳng, chỉ gồm khóa muốn đổi: `{"iops_soft": 300, "windows": ["22:00-06:00"], "algorithm": "grace-v2", "lag_slo": "", "restart": false, "dry_run": false}`. Khóa vắng = giữ nguyên; giá trị rỗng (`""`/`null`) = xóa khóa khỏi file (về mặc định). `windows` nhận mảng hoặc chuỗi cách nhau dấu phẩy. `dry_run=true`: chỉ validate và trả diff, không ghi.
Sửa node YAML tại chỗ (giữ chú thích và khóa khác), rồi đi đường lưu cấu hình như `POST .../config`: file tạm, `strand validate` với env của pipeline (lỗi thì **502**, không ghi), sao lưu `<file>.bak.<timestamp>`, ghi đè nguyên tử giữ mode/owner, tùy chọn `restart`. Khóa mới có hiệu lực sau restart; muốn đổi tốc độ đọc ngay dùng Throttle (`throttle set`), không ghi vào file.
Kiểm tra: `iops_*` số nguyên 1..10000000 và `iops_hard` >= `iops_soft`; `chunk_rows` `auto` hoặc số nguyên 1..10000000; `max_parallel` 1..32; `windows` dạng `HH:MM-HH:MM`; `algorithm` thuộc `static|onoff|grace-v2`; `lag_slo`, `replica_lag_max` là duration (1ms..24h); `breaker_max_ratio` số lớn hơn 1 và tối đa 1000; `workers` 1..1024. Sai thì **400** `{"error": "...", "fields": {"<khóa>": "<lý do>"}}` và file không đổi; khóa lạ cũng 400.
Trả về **200**: `{"ok", "dry_run"?, "changed": [...], "diff", "valid"?, "output"?, "backup", "restarted", "message"}`. Không có gì đổi thì `changed` rỗng và không ghi. Audit: `config.tuning` (khóa đã đổi) và `config.edit`.

`POST /api/v1/pipelines` (tạo mới) nhận thêm các trường tùy chọn cùng ý nghĩa: `iops_soft`, `iops_hard`, `chunk_rows`, `max_parallel`, `windows` (mảng), `governor_algorithm`, `lag_slo`, `breaker_max_ratio`, `replica_lag_max`; giá trị sai trả 400.

### `GET /api/v1/pipelines/{name}/env`

Trả về các biến môi trường từ file `.env` của pipeline: `{"path": "...", "exists": bool, "entries": {"KEY": "val", "SRC_PW": "********"}}`. Giá trị bí mật luôn được che bằng `********`.

### `POST /api/v1/pipelines/{name}/env`

Body JSON: `{"entries": {"KEY": "val", "SRC_PW": "newpassword"}}`.
Cập nhật file `.env`: nếu giá trị gửi lên là `********` thì giữ nguyên giá trị cũ; nếu là giá trị mới thì cập nhật. Tạo file sao lưu `<env>.bak.<timestamp>`, ghi đè nguyên tử, quyền file luôn là **0600**. Audit: `env.edit`.

### `GET /api/v1/pipelines/{name}/logs?lines=N`

`{"source": "journalctl -u strand@x.service | file /path", "lines": [...], "error"}`; mặc định 200 dòng, tối đa 2000; mật khẩu bị che.

### `GET /api/v1/pipelines/{name}/series`

Tham số `?range=1h|6h|24h|7d` (mặc định `1h`). `{"range", "interval_seconds", "samples": [{"t", "state", "lag_seconds", "throughput", "apply_rows_per_s", "rows_per_s", "snapshot_rows_per_s", "holes", "writeset_entries", "spool_bytes", "stale"}], "error"}`. `rows_per_s`/`snapshot_rows_per_s` là tốc độ đã làm mượt (xem §3.3); `stale: true` khi lần lấy mẫu đó thất bại (giá trị là của lần tốt gần nhất).

- `1h`: tối đa 720 mẫu từ ring buffer trong RAM (cả hai chế độ; chế độ PostgreSQL nạp lại ring từ DB khi web khởi động lại).
- `6h`, `24h`, `7d`: chỉ chế độ PostgreSQL, đọc từ bảng `metric_samples` (raw, giữ 24 giờ) và `metric_samples_1m` (trung bình 1 phút, giữ 7 ngày), gộp theo bucket 30 s / 2 phút / 15 phút (`interval_seconds` = độ rộng bucket, khoảng 700 điểm). Mỗi bucket là trung bình, riêng `lag_seconds` là **giá trị lớn nhất** của bucket. Chế độ file: trả dữ liệu 1 giờ, `"range": "1h"` và `error` giải thích. Tab Graph có nút 6 h / 24 h / 7 d ở chế độ PostgreSQL (biểu đồ vẽ từ API này, điểm mới vẫn nối trực tiếp từ SSE).

Ghi: mẫu được gom và ghi theo lô mỗi 30 giây (một INSERT cho mọi pipeline), bảo trì mỗi giờ (gộp 1 phút, xóa dữ liệu hết hạn). Khối lượng: N pipeline x 12 dòng/phút ở `metric_samples` (khoảng 17 nghìn dòng/ngày/pipeline, xóa sau 24 giờ) + 1 dòng/phút/pipeline ở `metric_samples_1m`.

### 3.3 Số liệu sống: sampler, SSE `GET /api/v1/stream`, `GET /api/v1/live`

**Đường dữ liệu.** Bộ lấy mẫu (sampler) trong `strand web` là thành phần **duy nhất** hỏi admin socket (`top`, `status`, `tables`, `checkpoint show`, `events`) và `/metrics` của pipeline: mỗi pipeline tối đa một lần hỏi mỗi chu kỳ (`sample_interval_sec`, mặc định 5 s), hạn giờ `min(0.8 × chu kỳ, 4 s)` (tối thiểu 1 s), tối đa 8 pipeline song song, pipeline còn đang được hỏi từ chu kỳ trước thì bỏ lượt (không dồn). Trang HTML, các route đọc ở §3 và luồng SSE chỉ đọc bản mẫu mới nhất trong RAM: **không request nào gọi admin socket** (trừ thao tác §4 và `source-tables`, là truy vấn source theo yêu cầu, có cache 30 s). Sau một thao tác thành công, pipeline đó được lấy mẫu lại ngay.

**Ổn định số liệu.**
- Tốc độ (`rows_per_s` = dòng apply CDC/s, `txn_per_s`, `snapshot_rows_per_s`) là trung bình trên cửa sổ trượt **30 s** của bộ đếm (`status.applied_rows`, `status.applied_txns`, `top.snapshot_rows`); bộ đếm giảm (pipeline khởi động lại) thì cửa sổ bắt đầu lại. Chưa có cửa sổ (mẫu đầu) hoặc thiếu `status`: EWMA hằng số thời gian 30 s của tốc độ tức thời trong `top`. Pipeline không có bộ đếm byte nên không có bytes/s.
- Lag hiển thị với đơn vị cố định: `0.25 s` (dưới 10 s), `12.3 s` (dưới 1 phút), `4m 05s`, `2h 03m`. Màu: `warn` từ 30 s, `bad` từ 300 s, có **trễ pha**: chỉ hạ mức khi xuống dưới 80% ngưỡng (24 s / 240 s), trường `lag_level`.
- **Luật trạng thái**: huy hiệu trạng thái chỉ đổi khi **hai mẫu liên tiếp** cùng trạng thái mới; riêng `failed` và `stopped` đổi ngay (và lần lấy mẫu đầu tiên, và mẫu lấy lại ngay sau một thao tác của người dùng). `raw_state` là trạng thái suy từ riêng mẫu mới nhất.
- **Stale**: lần hỏi thất bại (hết giờ, lỗi) khi tiến trình vẫn chạy thì giữ số liệu tốt gần nhất, đặt `stale: true`, `last_ok` (thời điểm mẫu tốt), `poll_error`; giao diện làm mờ số, hiện biểu tượng cảnh báo và "updated N s ago" thay vì để trống. Tiến trình đã dừng thì không giữ số cũ.

**`GET /api/v1/stream?pipeline=<name>|all[&logs=1]`** — Server-Sent Events (`text/event-stream`), xác thực như mọi route (cookie session hoặc `Authorization: Bearer`; GET nên không cần CSRF). `pipeline=<name>`: bản ghi của pipeline đó đầy đủ (bảng, sự kiện lỗi, chuỗi của sơ đồ Graph), pipeline khác ở dạng gọn; `all` (mặc định): mọi bản ghi dạng gọn. `logs=1` (chỉ với một pipeline): thêm sự kiện `log`. Tên không tồn tại: **404**. Quá 128 luồng đồng thời: **503**.

Khung: đầu tiên `retry: 3000`, rồi:

| `event:` | `id:` | `data` |
| --- | --- | --- |
| `snapshot` | có | `LiveSnapshot`: `{"interval_s", "server_time", "last_event_id", "pipeline", "overview": LiveOverview, "pipelines": [LiveRecord]}` — trạng thái đầy đủ: khi kết nối, khi `Last-Event-ID` không nối tiếp được, hoặc khi client quá chậm làm rơi sự kiện |
| `sample` | có | `LiveRecord` của một pipeline, sau mỗi lần lấy mẫu pipeline đó |
| `overview` | có | `LiveOverview`, sau mỗi chu kỳ |
| `structure` | có | `{"pipelines": [tên]}` khi danh sách pipeline đổi (client tải lại trang) |
| `log` | không | `{"pipeline", "lines": [...], "initial", "gap", "error"}` mỗi 2 s khi có dòng mới; `initial: true` là 200 dòng cuối (client bỏ phần trùng với dòng đã hiện), `gap: true` khi có dòng bị bỏ qua; mật khẩu đã che |
| `bye` | không | `{"reason"}` khi session hết hạn/đăng xuất; luồng đóng |

Comment `: hb <unix>` mỗi 15 s (giữ kết nối qua proxy; mỗi nhịp kiểm lại session). **Nối lại**: `EventSource` tự gửi `Last-Event-ID`; còn trong vòng 512 sự kiện gần nhất thì server phát lại đúng các sự kiện bị lỡ, không thì gửi `snapshot`. Proxy phía trước cần tắt buffering (server gửi `X-Accel-Buffering: no`).

`LiveRecord`:

```json
{"name": "orders", "state": "synced", "raw_state": "synced", "reachable": true, "stale": false,
 "sampled_at": "...", "last_ok": "...", "age_s": 0.4,
 "lag_s": 0.25, "lag_level": "", "rows_s": 1234.5, "txn_s": 41.2, "snapshot_rows_s": 0, "zone": "green",
 "sev": 7, "attention": false,
 "tables": {"done": 1, "running": 1, "pending": 1, "total": 3, "pct": 33.3, "rows_copied": 250000, "rows_estimate": 1000000, "rows_pct": 25, "eta_s": 90},
 "uptime_s": 3600, "checkpoint_at": "...",
 "f": {"state": "synced", "lag": "0.25 s", "rows": "1,235", "txn": "41.2", "tables": "1 / 3", "tables_pct": "33.3%", "uptime": "1h 00m 00s", "ckpt": "3 s ago", "reason": "", "...": "..."},
 "pt": {"t": "...", "lag_seconds": 0.25, "rows_per_s": 1234.5, "...": "một điểm như /series"},
 "table_rows": [{"table": "app.orders", "state": "running", "pct": 25, "rows_copied": 250000, "rows_estimate": 1000000, "eta_s": 90, "f": {"pct": "25.0%", "rows": "250,000 / 1,000,000", "eta": "1m 30s"}}],
 "tables_full": false,
 "events_new": [{"time", "level", "message"}]}
```

**Nhiều bảng** (> 200 bảng): luồng sống không bao giờ gửi toàn bộ danh sách. `snapshot` chỉ chứa các bảng đang chạy (`tables_full` vắng); sự kiện `sample` chỉ chứa các bảng đổi; khi danh sách bảng đổi (bảng mới/bị bỏ) hoặc hơn 200 bảng đổi cùng lúc thì gửi `"tables_changed": true` không kèm dòng nào, và trang Tables dựng lại trang đang xem từ server (giữ bộ lọc/sắp xếp/trang).

`f` là chuỗi hiển thị theo khóa `data-bind` của trang, định dạng giống hệt trang render ở server (số không "nhảy" khi trang tải lại). `lag_s` vắng khi không có số đọc. Ở bản đầy đủ: `table_rows` chỉ gồm bảng **đổi** từ sự kiện trước (`tables_full: true` = toàn bộ danh sách, ví dụ trong `snapshot` hoặc khi danh sách bảng đổi), `events_new` chỉ gồm cảnh báo/lỗi mới; `f` có thêm `g_*` (sơ đồ Graph), `governor`, `ckpt_applied`, `epoch`. `LiveOverview`: `{"total", "healthy", "attention": [tên], "max_lag_s", "max_lag_pipeline", "rows_s", "tables_done", "tables_total", "tables_pct", "counts": {trạng thái: n}, "pipelines": [tên], "f": {...}}`.

**`GET /api/v1/live?pipeline=<name>|all`** → `LiveSnapshot` (JSON thường). Trình duyệt dùng khi `EventSource` không có hoặc lỗi liên tục (3 lần): hỏi mỗi chu kỳ, thử lại SSE sau 60 s.

Ví dụ: `curl -N -H "Authorization: Bearer $TOKEN" 'https://web:8443/api/v1/stream?pipeline=orders'`.

### `GET /api/v1/settings`, `POST /api/v1/settings` (trang `/settings`)

`GET` → `{"database_mode": bool, "settings": [{"key", "label", "help", "kind": "bool|int|string|list", "value", "source": "env|database|web.yaml|default", "file_value", "env", "editable", "restart_required", "pending_restart"}]}`.

Khóa: `scan` (danh sách glob tuyệt đối, áp dụng ở lần quét kế tiếp), `config_dir` (đường dẫn tuyệt đối hoặc rỗng), `secure_cookies` (env `STRAND_WEB_SECURE_COOKIES`), `trust_forwarded_for` (env `STRAND_WEB_TRUST_FORWARDED_FOR`), `shutdown_stop_pipelines`, `sample_interval_sec` (1 đến 300, **cần khởi động lại web**), `audit_retention_days` (0..36500, mặc định 365, 0 = giữ mãi), `audit_archive_auto` (mặc định tắt), `audit_archive_dir` (rỗng = `<state_dir>/audit-archive`), `audit_rotate_mb` (0..102400, mặc định 100, 0 = không xoay), `audit_rotate_days` (0..36500, mặc định 0 = tắt) (§5). Khóa `int` có thêm `min`, `max`. Cài đặt khởi động (`listen`, TLS, `allow_plain_http`, `state_dir`, `database`, `strand_binary`, mật khẩu/token) chỉ ở `web.yaml`/env và không sửa được ở đây.

Thứ tự ưu tiên của giá trị hiệu lực: **biến môi trường** (khác rỗng) > **database** (bảng `web_settings`, đã lưu trên trang Settings) > **web.yaml** > mặc định. Khóa do env đặt thì không sửa được (409).

`POST` body `{"khóa": giá_trị, ...}`; `null` = reset (xóa dòng DB, trở về web.yaml/mặc định). Kiểm hợp lệ trước khi ghi (400 kèm lý do), mỗi khóa đổi ghi audit `settings.edit` (`started` trước, rồi `ok`/`error`, `detail.key/old/new/old_source`; không ghi được audit thì không đổi). → `{"ok": true, "changed": [...], "settings": [...]}`. Chế độ file: **409** (sửa `web.yaml` rồi khởi động lại).

### `GET /api/v1/backups`, `GET /api/v1/backups/{id}`, `GET /api/v1/backups/{id}/status` (trang `/backups`)

Catalog backup tập trung (chỉ đọc). `GET /api/v1/backups` nhận
`source`, `engine`, `status`, `limit` (1..500, mặc định 200), `created_after`, `created_before` (RFC3339) và
`cursor`; trả `{"backups": [Record], "limit": N, "next_cursor": "..."}` (mới nhất trước; `next_cursor` chỉ có khi
trang đầy). Xem `docs/CENTRAL-BACKUPS.md`.

### Registry trong DB (chế độ PostgreSQL)

`PipelineView` có thêm `origin` (`created` | `web.yaml` | `scan`), `missing` (lý do, khi entry web.yaml/file scan đã mất) và `disk_conflicts` (`[{"file": "config|env", "path": "<bản cất>", "at"}]`). Action liên quan (§4): `registry-restore`, `registry-forget` (cần `confirm_name`; 409 khi pipeline đang chạy), `conflict-adopt` và `conflict-dismiss` (tham số `path` phải là một bản cất của pipeline đó). Chế độ file: 409.

## 4. Thao tác

`POST /api/v1/pipelines/{name}/actions/{action}` — body JSON `{"khóa": "giá trị chuỗi"}` (có thể rỗng). Với mọi action server: kiểm session + CSRF (hoặc Bearer), kiểm tên pipeline (404), kiểm tên action (404 `unknown action`), kiểm `confirm_name` khi action yêu cầu (400 nếu không bằng đúng tên pipeline), ghi audit `started` (không ghi được thì **không chạy**), chạy, rồi ghi audit kết quả (`ok`/`error`, `exit_code`). Thao tác chạy tách khỏi kết nối của client (client ngắt giữa `stop` không làm hỏng việc đang dở), giới hạn 170 giây.

Phản hồi **200**: `{"message", "job_id", "exit_code", "data"}`. Lỗi: **400** tham số sai (whitelist), **404** không có pipeline/action/job, **409** xung đột (đã có job đang chạy), **502** lệnh/ socket báo lỗi (`error` có nội dung đã che mật khẩu), **500** lỗi nội bộ (không ghi được audit, env_file hỏng).

### 4.1 Vòng đời và điều khiển

| Action | Tham số | Cách chạy | Ghi chú |
| --- | --- | --- | --- |
| `start` | - | systemd: `systemctl start <unit>`. Nền: `<strand_binary> sync -c <cfg> --background [--log-file= --pid-file=]` | môi trường tiến trình con = môi trường của web + `env_file` |
| `stop` | - | systemd: `systemctl stop <unit>`. Nền: `<strand_binary> stop -c <cfg> [--pid-file=]` | UI hỏi xác nhận; chờ ghi checkpoint (tối đa 150 s) |
| `restart` | - | systemd: `systemctl restart <unit>`. Nền: `stop` rồi `sync --background` (dừng nếu `stop` lỗi) | UI hỏi xác nhận |
| `enable`, `disable` | - | `systemctl enable|disable <unit>` | chỉ chế độ systemd; chế độ nền: **400** |
| `pause` | - | admin socket `pause` | |
| `pause-snapshot` | - | admin socket `pause_snapshot` | chỉ dừng snapshot, stream vẫn chạy |
| `resume` | - | admin socket `resume` | |
| `resume-snapshot-only` | - | admin socket `resume` với `snapshot_only` | chỉ snapshot, hoãn stream |
| `throttle` | `source_iops` (1..10000000) | admin socket `throttle_set` với `soft=true` | chỉ soft limit, chỉ tiến trình đang chạy; không ghi vào file cấu hình |
| `snapshot-add` | `table` hoặc `tables` (`schema.table`, nhiều bảng cách nhau bằng dấu phẩy, tối đa 200) | admin socket `snapshot_add` lần lượt từng bảng; dừng ở bảng lỗi (lỗi nêu các bảng đã bắt đầu) | UI hỏi xác nhận |
| `checkpoint-show` | - | admin socket `checkpoint_show`; `data.checkpoint` | chỉ đọc |
| `checkpoint-rewind` | `to` (GTID set hoặc LSN), **`confirm_name`** | admin socket `checkpoint_rewind` | MySQL → MySQL; pipeline tự kiểm `gtid_purged`. Cần pipeline đang chạy |
| `config-validate` | `yaml` | `<strand_binary> validate -c <tmp>` với env của pipeline; tính diff | kiểm tra cú pháp và sinh diff |
| `config-save` | `yaml`, `restart` (tùy chọn) | validate tmp -> sao lưu `.bak.<ts>` -> ghi đè nguyên tử -> refresh -> restart | không ghi đè nếu validate lỗi |
| `env-save` | `entries` (hoặc các cặp key/val) | cập nhật file `.env` (mode 0600) -> sao lưu `.bak.<ts>` -> refresh | giá trị `********` giữ nguyên giá trị cũ |
| `job-cancel` | `job_id` | SIGTERM tới process group của job (SIGKILL sau 10 s) | job phải thuộc pipeline này; đã xong: **409** |

`systemctl start/stop` chạy bằng user của `strand web` (không sudo/polkit): hệ thống phải cho phép user đó (quyết định W4). Thiếu quyền thì **502** kèm thông báo của systemctl.

### 4.2 `verify` (job)

`POST .../actions/verify` với `mode` (`sample`|`full`|`delta`) và tùy chọn `table` (`schema.table`, bắt buộc với `sample`, không dùng với `delta`), `rate` (số; sample: (0,1], full: (0,100000]; không dùng với delta), `resume` (`true`, chỉ `full`). Chạy tiến trình con `<strand_binary> verify -c <cfg> --mode <mode> [--table=<t>] [--rate=<r>] [--resume] [--socket=<sock> khi delta]` (luôn `--repair report`; sửa dữ liệu là việc của wizard Repair, §4.4). Trả **200** `{"message", "job_id"}` ngay; **409** nếu pipeline đã có job đang chạy. Mọi điều kiện an toàn và mã thoát là của CLI (`verify` thoát 5 khi lệch).

### 4.2b Plan, schema evolution, database objects (job)

Cùng cách chạy như `verify` (một job mỗi pipeline, output trực tiếp ở `/jobs/{id}`, lịch sử trong DB; file cấu hình/.env được dựng lại từ DB trước khi chạy). Nút ở tab **Jobs** của pipeline.

| Action | Tham số | Lệnh |
| --- | --- | --- |
| `plan` | | `plan -c <cfg>` |
| `schema-diff` | `all` (`true`: gồm cả thay đổi đã áp) | `schema diff -c <cfg> [--all]`: thay đổi schema chờ duyệt + DDL đề xuất |
| `schema-approve-dry` | `id` (số) | `schema approve <id> -c <cfg> --dry-run` |
| `schema-approve` | `id`, `confirm_name` | `schema approve <id> -c <cfg>` (áp DDL lên target) |
| `schema-init-dry` | | `schema init -c <cfg> --dry-run` (DDL của bảng còn thiếu ở target) |
| `objects-list` | | `objects list -c <cfg>` (nguồn MySQL/MariaDB; khác: **400**) |
| `objects-apply` | `phase` (`pre`\|`users`\|`post`), `confirm_name` | `objects apply -c <cfg> --phase <p>` (cần target MySQL; khác: **400**) |

### Tải về / tải lên cấu hình, MCP

- `GET /pipelines/{name}/config.yaml` (hoặc `GET /api/v1/pipelines/{name}/config/download`): bản cấu hình trong DB, bí mật che `********`, `Content-Disposition: attachment`; audit `config.download`. Tải lên: ô **Upload YAML** của trình sửa (tab Config) nạp file vào ô soạn thảo rồi xem diff; chỉ ghi khi bấm Save (đi qua `POST .../config` như sửa tay; `********` được khôi phục từ bản lưu).
- `GET /mcp` (trang) và `GET /api/v1/mcp` → `{"command", "config_path", "client_json", "claude_code", "tools": [{name, description}], "recent_calls": [{time, tool, result}]}`: cách kết nối `strand mcp` (server MCP stdio, chỉ đọc, do client tự chạy), allowlist tool, các lần gọi gần đây (audit `mcp.<tool>`).

### Pipeline MongoDB

`POST /api/v1/pipelines` với `from` = `to` = `mongodb`: `source_uri`, `target_uri` (bí mật: chỉ vào `.env` mã hóa dưới tên `SRC_URI`/`DST_URI`, YAML ghi `uri: "${SRC_URI}"`), `source_database`, `target_mongo_database`, `include`/`exclude` dạng `db.collection`, tùy chọn `snapshot_batch`, `stream_batch` (1..100000), `lease_ttl` (5s..1h), `overwrite_target`. Tab Config > Tuning của pipeline MongoDB chỉ có `snapshot_batch`, `stream_batch`, `lease_ttl` (mục `mongodb:`); khóa SQL bị từ chối (**400** "not a setting of this pipeline kind") và ngược lại.

### 4.4 Wizard Repair và Promote (M4)

Trạng thái wizard ở `<state_dir>/wizards/<repair|promote>-<pipeline>.json` (0600, ghi nguyên tử, đọc lại từ đĩa mỗi request nên web khởi động lại vẫn tiếp tục). Thứ tự bước được ép ở server: gọi sai bước trả **409** kèm bước hiện tại. Kết quả job (mã thoát) được đồng bộ vào state khi đọc. Mọi bước chạy tiến trình con `strand` (argv mảng, `env_file`), một job mỗi pipeline.

`GET /api/v1/pipelines/{name}/repair` và `.../promote` → `{"kind", "pipeline", "step", "active", "state": {step, job_id, job_done, last_exit, failover, opts, history[]} | null, "steps": [{n, title, status}], "job", "output_tail", "pipeline_running", "lag_seconds", "lag_ok", "lag_threshold", "decision", "promote_state", "can": {action: bool}}`. `step`: repair `wait`→`verify`→`start`→`done` (hoặc `aborted`); promote `plan`→`planning`→`ready`→`running`→`done`, `decision` (exit 10), `failed` (khác 0 và 10), `aborted`. Trang HTML: `/pipelines/{name}/repair` và `/promote`.

| Action | Yêu cầu | Việc làm |
| --- | --- | --- |
| `repair-begin` | `confirm_name`; tùy chọn `lag_threshold` (giây, mặc định 5), `table` | Mở wizard (409 nếu đang có wizard) |
| `repair-stop` | bước `wait`; lag đọc được và ≤ ngưỡng (nếu pipeline đang chạy) | Stop như nút Stop (systemctl hoặc `strand stop -c <cfg>`) |
| `repair-run` | `confirm_name`; bước `verify` | Job `repair`: `verify -c <cfg> --mode full --repair fix [--table=<t>]`; **200** `{job_id}` |
| `repair-start` | bước `start` (job repair thoát 0) | Start như nút Start |
| `repair-abort` | wizard đang mở, không có job chạy | Đóng wizard (không tự start pipeline) |
| `promote-plan` | `target_dsn`, `fence_dsn` (+ tùy chọn dưới) | Job `promote-plan`: `promote -c <cfg> --pipeline=<name> --engine=<mysql\|pg> --target=.. --fence=.. [opts] --dry-run` |
| `promote-run` | `confirm_name`; bước `ready`, `decision` hoặc `failed` | Job `promote`: cùng argv không `--dry-run` |
| `promote-failover` | `confirm_name` **và** `confirm_name2` (cùng bằng tên pipeline); chỉ ở bước `decision`; `fence_dsn` tùy chọn | Job `promote-failover`: như `promote-run` thêm `--source-fenced-externally` |
| `promote-abort` | như `repair-abort` | Đóng wizard (tiến độ promote vẫn nằm trong `_strand_promote`) |

Tùy chọn promote (đều qua whitelist): `catchup_timeout`, `fence_timeout` (`30s`, `5m`, `1h`), `peak_rate`, `sync_interval` (số), `app_role`, `schemas` (chỉ target PG), `source_app_roles`, `objects_post` (`true`, chỉ target MySQL), `override_sequences` (JSON `{"seq": số}`). `--engine` lấy từ `target.kind` của cấu hình, `--pipeline` từ `name` của cấu hình. `target_dsn` và `fence_dsn` không bao giờ được lưu; dòng audit ghi `<hidden>` và `Job.args` ghi `--target=<hidden>`. `decision` là đoạn CLI in sau dòng `=== PROMOTE NEEDS OPERATOR DECISION ===` (nguyên văn, gồm phần dữ liệu sẽ mất).

### 4.3 Job

Job lưu ở `<state_dir>/jobs/<id>.json` (trạng thái) và `<id>.log` (stdout+stderr, ghi thẳng vào file). `id` dạng `20261115T100000Z-1a2b3c4d`. Mỗi pipeline giữ 100 job đã xong gần nhất.

| Route | Mô tả |
| --- | --- |
| `GET /api/v1/jobs?pipeline=` | `{"jobs": [Job]}` mới nhất trước |
| `GET /api/v1/pipelines/{name}/jobs` | như trên cho một pipeline |
| `GET /api/v1/jobs/{id}` | `{"job": Job}` |
| `GET /api/v1/jobs/{id}/output?offset=N` | `{"data", "offset", "running", "exit_code"}`: output từ byte `N`; gọi lại với `offset` trả về cho tới khi `running=false`. Khi còn chạy chỉ trả đến dòng đầy đủ cuối cùng; mật khẩu/DSN bị che; tối đa 256 KiB mỗi lần |
| `GET /api/v1/pipelines/{name}/source-tables` | `{"tables": [{"table", "in_pipeline"}], "error"}` từ admin socket `source_tables` (chỉ đọc); dùng để chọn bảng cho `snapshot-add`/`verify` |

`Job`: `{"id", "pipeline", "kind", "args" (argv của strand, không gồm tên binary), "started", "ended", "running", "exit_code", "pid", "cancelled", "interrupted", "error"}`.

Khi web chết hoặc khởi động lại: tiến trình con có process group riêng và ghi output thẳng vào file nên **tiếp tục chạy**; không có zombie (con mồ côi do init thu dọn). Web mới nạp lại các job còn `running`: pid còn đúng là tiến trình đó thì nhận nuôi (vẫn chặn job thứ hai của pipeline), khi nó thoát thì đóng job nhưng `exit_code` vắng và `error` ghi rõ vì web không còn là cha để đọc mã thoát; pid đã mất thì đánh dấu `interrupted`.

## 5. Audit log

Chế độ file: `<state_dir>/audit.log`. Chế độ PostgreSQL (`database.dsn`): bảng `audit_log` chỉ-thêm (cột `ts, ip, action, pipeline, result, data`); `database.audit: both` (mặc định) vẫn ghi file làm bản sao (USAGE.md 7.15). Mỗi dòng/bản ghi là một JSON: `{"time", "ip", "action", "pipeline", "args", "result": "ok|denied|error|started", "exit_code", "detail"}`. Hành động: `login` (ok/denied, `detail.reason` = `bad_password`/`rate_limited`), `login_csrf_rejected`, `csrf_rejected`, `logout`, `config.view`, `config.edit`, `env.edit`, `pipeline.create`, `autostart` (web tự chạy lại pipeline có trạng thái mong muốn `running` khi khởi động), `desired.observed_stop` (pipeline nền dừng êm **không do web**, ví dụ `strand stop` từ CLI: trạng thái mong muốn chuyển `stopped`, không tự chạy lại; `detail.stopped_at`), `web.lease.acquired` (instance này thành active; `detail.epoch`, `previous_holder`), `audit.archive` (lưu trữ audit cũ: `detail.rows`, `min_id`, `max_id`, `from`, `to`, `before`, `file`, `sha256`, `by`), `audit.rotate` (dòng đầu của `audit.log` mới sau khi xoay: `detail.previous`, `sha256`, `bytes`, `reason`), `state.import` (lần nhập file vào DB đầu tiên), `settings.edit`, và mọi action ở §4 (hai dòng: `started`, rồi `ok`/`error` kèm `exit_code` khi có). Job kết thúc ghi thêm `job.end` (`result` `ok` khi exit 0, `error` nếu khác hoặc không quan sát được; `exit_code`; `detail.job_id`, `cancelled`).

### 5.1 Lưu giữ (retention)

`audit_log` vẫn chỉ-thêm: UPDATE và TRUNCATE luôn bị từ chối; DELETE chỉ đi qua hàm `strand_audit_archive()` (migration 009). Quy trình (`strand web db audit-archive`, hoặc tự động mỗi ngày trên instance active khi `audit_archive_auto` bật): (1) chọn các dòng `ts` < mốc (mặc định `now - audit_retention_days`, ít nhất 1 ngày trước), trừ các dòng `audit.archive`; (2) xuất theo `id` ra `audit-<from>-<to>.jsonl.gz` (gzip, một dòng JSON `{"id","ts","ip","action","pipeline","result","data"}` mỗi bản ghi, 0600, ghi file tạm + fsync + rename); (3) đọc lại để kiểm chứng (số dòng, id đầu/cuối, sha256); (4) ghi `<file>.sha256` (định dạng `sha256sum`) và `<file>.manifest.json`; (5) hàm DB ghi một dòng manifest vào `audit_archives` (cũng chỉ-thêm), xóa **đúng** các dòng đó (số lượng phải khớp số dòng trong file, không thì không xóa gì) và thêm dòng `audit.archive`, trong một transaction. Trigger chỉ cho DELETE đi qua với dòng nằm trong một manifest tạo **trong cùng transaction** (đặt cờ phiên bằng tay không đủ) và không bao giờ cho xóa dòng `audit.archive`: mọi lần xóa audit đều để lại bản ghi vĩnh viễn trỏ tới sha256 của file. File lưu trữ không bao giờ bị Strand xóa. Tăng cứng: để bảng thuộc một role quản trị, cấp cho role của web INSERT/SELECT trên `audit_log` và EXECUTE trên `strand_audit_archive` (hàm là SECURITY DEFINER).

Chế độ file (và bản sao file ở chế độ DB `database.audit: both|file`): `audit.log` được xoay khi vượt `audit_rotate_mb` MiB (kiểm mỗi lần ghi) hoặc dòng đầu cũ hơn `audit_rotate_days` ngày (kiểm hằng ngày) thành `audit-<thời điểm>.log.gz` + `.log.gz.sha256`; `audit.log` mới bắt đầu bằng dòng `audit.rotate` ghi tên và sha256 của file vừa xoay (chuỗi băm). `strand web db migrate` chỉ nhập `audit.log` hiện tại, không nhập các file đã xoay.

## 6. Trang HTML

`/` (Overview), `/pipelines`, `/pipelines/new` (Tạo pipeline mới), `/pipelines/{name}` (Graph), `/pipelines/{name}/{tables|checkpoint|errors|config|logs|jobs|repair|promote}`, `/jobs` (mọi job), `/jobs/{id}` (output trực tiếp), `/settings` (GET; POST form có `csrf`), `/login`, `POST /login`, `POST /logout`, `/static/*` (công khai). Overview, Pipelines và mọi tab của trang pipeline nhận số liệu qua SSE (§3.3) và cập nhật **tại chỗ** (phần tử có `data-bind*`: số, huy hiệu, thanh tiến độ, sparkline, biểu đồ, danh sách bảng đang chép, Errors, đuôi Logs), không tải lại cả trang; trang chỉ tải lại khi cấu trúc đổi (danh sách pipeline, tập "Needs attention", trạng thái của pipeline đang xem, bảng mới) và không bao giờ khi người dùng đang gõ, đang mở hộp thoại hoặc đang bôi đen chữ. Logs: bám đuôi trực tiếp (mặc định bật, `?follow=0` để tắt; cuộn lên thì tạm dừng và hiện số dòng mới). Trang Jobs vẫn tự làm mới 5 giây.

Tab Tables với hơn 200 bảng được **phân trang ở server** (100 dòng/trang): tìm kiếm (`?tq=`), lọc trạng thái (`tstate=running|pending|done`, các chip là liên kết), sắp xếp (`tsort=name|pct|rows|eta`), trang (`tpage=`); phần tổng hợp, bộ đếm và "Copying now" (tối đa 50 bảng đang chép) luôn hiện. Dòng của trang được cập nhật tại chỗ; trang dựng lại khi một dòng đang hiện đổi trạng thái hoặc danh sách bảng đổi. 2.000 bảng: ~120 KiB, 10.000 bảng: ~130 KiB mỗi trang. Tối đa 200 bảng: như cũ (mọi dòng, lọc/sắp xếp ở trình duyệt).

Pipeline nền đã dừng êm ngoài web có `"stopped_outside_web": true` trong `PipelineView` và lý do "stopped outside the web (strand stop or a signal); it is not started automatically".

## 7. Nhiều instance web trên một database (active / standby)

Chế độ PostgreSQL: chỉ **một** instance là **active**, giữ lease trong bảng `web_lease` (migration 008; gia hạn mỗi TTL/3, TTL mặc định 30 s, `STRAND_WEB_LEASE_TTL=<giây>` 3..600; hết hạn tính bằng đồng hồ của DB; `epoch` tăng mỗi lần đổi chủ). Chỉ instance active: tự chạy lại pipeline theo trạng thái mong muốn, nhận ra pipeline dừng ngoài web, ghi và bảo trì lịch sử số liệu, xử lý job còn `running` của lần trước, ghi registry từ `web.yaml`/scan, lưu trữ audit tự động, và nhận **mọi request ghi**. Các instance khác là **standby** (chỉ đọc): trang, API đọc, SSE và đăng nhập vẫn chạy (session nằm trong DB nên đăng nhập dùng được ở mọi instance); request ghi (trừ logout) trả **503** `{"error": "This web instance is a standby (read-only) ... (<host> <listen>) ..."}` với `X-Strand-Role: standby` và `Retry-After`; trang có banner. Standby tiếp quản khi lease hết hạn (active chết hoặc mất DB lâu hơn TTL) hoặc **ngay** khi active dừng êm (nhả lease); khi tiếp quản nó ghi `web.lease.acquired`, xử lý lại job và chạy autostart. Active không gia hạn được thì tự thôi làm active khi tới hạn cục bộ (trước khi standby có thể lấy lease). Bảng `web_instances` giữ nhịp tim từng instance.

