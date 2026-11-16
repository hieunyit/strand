# Strand Web API `/api/v1`

Đặc tả REST của `strand web` (kế hoạch: `docs/PLAN-WEB-UI.md`, quyết định: `questions/DECISIONS-WEB-UI-2026-11-15.md`). Giao diện web và script dùng chung các route này. Phiên bản M5: các route đọc, đăng nhập/đăng xuất, **thao tác** (§4), **job** (§4.3), **sửa cấu hình** (§3.1) và **tạo pipeline mới** (§3.2).

## 1. Quy ước

- JSON UTF-8. Lỗi: `{"error": "<mô tả>"}` kèm mã HTTP.
- Mọi route (trừ đăng nhập) đòi xác thực. Không xác thực: **401** (API), **302 → /login** (trang HTML).
- Header bảo mật trên mọi phản hồi: `Content-Security-Policy: default-src 'self'`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`, `X-Content-Type-Options: nosniff`, `Cache-Control: no-store`.
- Tên pipeline trong đường dẫn phải khớp `^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$` và đã được đăng ký; ngược lại **404**.

## 2. Xác thực

Hai cách:

1. **Session** (trình duyệt). `GET /login` đặt cookie `strand_login` (giá trị = token CSRF của lần đăng nhập). `POST /api/v1/login` với header `X-CSRF-Token: <giá trị cookie strand_login>` và body `{"password": "..."}`. Thành công: **200** `{"csrf": "<token>", "idle_timeout_s": 1800, "max_age_s": 43200}` và cookie `strand_session` (`HttpOnly`, `SameSite=Strict`, `Secure` khi có TLS). Mọi request `POST` sau đó mang header `X-CSRF-Token: <csrf>` (hoặc trường form `csrf`); thiếu/sai: **403**.
2. **Token tĩnh** (script). Tạo bằng `strand web token --config web.yaml` (in một lần; `web.yaml` chỉ giữ sha256). Gửi `Authorization: Bearer <token>`. Không dùng cookie nên không cần CSRF.

Giới hạn đăng nhập: 5 lần sai/phút/IP, lần thứ 6 trả **429** kèm `Retry-After`; khóa tăng gấp đôi mỗi lần tái phạm (tối đa 15 phút). Session hết hạn sau 30 phút không dùng hoặc 12 giờ.

| Route | Mô tả |
| --- | --- |
| `POST /api/v1/login` | như trên. 401 sai mật khẩu, 403 thiếu CSRF, 429 bị chặn |
| `POST /api/v1/logout` | hủy session. Cần CSRF. **200** `{"ok": true}` |
| `GET /healthz` | **Không cần xác thực.** `200 {"status":"ok","version","database":{"mode":"file"` hoặc `"postgres","ok":true}}`; chế độ PostgreSQL mà DB không ping được: **503** `{"status":"degraded","database":{"ok":false,"error":"metadata database unreachable (details in the web log)"}}` (chi tiết lỗi chỉ ghi vào log của web, không lộ host/user cho người chưa đăng nhập). Dùng cho HEALTHCHECK/orchestrator |
| `GET /api/v1/session` | `{"authenticated": true, "version", "csrf", "idle_timeout_s", "max_age_s"}` (`csrf` vắng khi dùng Bearer) |

## 3. Tài nguyên

Trạng thái (`state`) của pipeline: `synced`, `bootstrapping`, `paused`, `starting`, `failed`, `stopped`, `unreachable` (tiến trình sống nhưng admin socket không trả lời), `unknown`. Khi admin socket trả lời thì lấy trạng thái từ `top`; không thì từ systemd (`ActiveState`, `ExecMainStatus`) hoặc file pid.

### `GET /api/v1/overview`

```json
{"counts": {"synced": 1, "bootstrapping": 0, "paused": 0, "starting": 0, "failed": 0, "stopped": 0, "unreachable": 0, "unknown": 0},
 "total": 1, "max_lag_seconds": 0.25, "max_lag_pipeline": "orders", "rows_per_sec": 1234,
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

Giá trị hiện tại của các khóa chỉnh tốc độ/an toàn snapshot, đọc từ file YAML: `{"path", "fields": [{"key", "label", "help", "yaml_key", "value", "set", "default", "options"?}], "error"?}`. `set=false` nghĩa là khóa vắng và Strand dùng `default`. Khóa (đều đã có tác dụng): `iops_soft` (`resources.source.iops.soft`), `iops_hard` (`resources.source.iops.hard`), `chunk_rows` (`bootstrap.logical.chunk_rows`), `windows` (`bootstrap.logical.windows`), `algorithm` (`governor.algorithm`), `lag_slo` (`governor.lag_slo`), `breaker_max_ratio` (`governor.breaker.strand_query_p99.max_ratio`), `replica_lag_max` (`governor.breaker.replica_lag.max`), `workers` (`resources.workers.max`). Khóa chỉ được nhận mà chưa dùng (USAGE.md 6.3, ví dụ `bootstrap.logical.max_parallel`) cố ý không có.

### `POST /api/v1/pipelines/{name}/tuning`

Body JSON phẳng, chỉ gồm khóa muốn đổi: `{"iops_soft": 300, "windows": ["22:00-06:00"], "algorithm": "grace-v2", "lag_slo": "", "restart": false, "dry_run": false}`. Khóa vắng = giữ nguyên; giá trị rỗng (`""`/`null`) = xóa khóa khỏi file (về mặc định). `windows` nhận mảng hoặc chuỗi cách nhau dấu phẩy. `dry_run=true`: chỉ validate và trả diff, không ghi.
Sửa node YAML tại chỗ (giữ chú thích và khóa khác), rồi đi đường lưu cấu hình như `POST .../config`: file tạm, `strand validate` với env của pipeline (lỗi thì **502**, không ghi), sao lưu `<file>.bak.<timestamp>`, ghi đè nguyên tử giữ mode/owner, tùy chọn `restart`. Khóa mới có hiệu lực sau restart; muốn đổi tốc độ đọc ngay dùng Throttle (`throttle set`), không ghi vào file.
Kiểm tra: `iops_*` số nguyên 1..10000000 và `iops_hard` >= `iops_soft`; `chunk_rows` `auto` hoặc số nguyên 1..10000000; `windows` dạng `HH:MM-HH:MM`; `algorithm` thuộc `static|onoff|grace-v2`; `lag_slo`, `replica_lag_max` là duration (1ms..24h); `breaker_max_ratio` số lớn hơn 1 và tối đa 1000; `workers` 1..1024. Sai thì **400** `{"error": "...", "fields": {"<khóa>": "<lý do>"}}` và file không đổi; khóa lạ cũng 400.
Trả về **200**: `{"ok", "dry_run"?, "changed": [...], "diff", "valid"?, "output"?, "backup", "restarted", "message"}`. Không có gì đổi thì `changed` rỗng và không ghi. Audit: `config.tuning` (khóa đã đổi) và `config.edit`.

`POST /api/v1/pipelines` (tạo mới) nhận thêm các trường tùy chọn cùng ý nghĩa: `iops_soft`, `iops_hard`, `chunk_rows`, `windows` (mảng), `governor_algorithm`, `lag_slo`, `breaker_max_ratio`, `replica_lag_max`; giá trị sai trả 400.

### `GET /api/v1/pipelines/{name}/env`

Trả về các biến môi trường từ file `.env` của pipeline: `{"path": "...", "exists": bool, "entries": {"KEY": "val", "SRC_PW": "********"}}`. Giá trị bí mật luôn được che bằng `********`.

### `POST /api/v1/pipelines/{name}/env`

Body JSON: `{"entries": {"KEY": "val", "SRC_PW": "newpassword"}}`.
Cập nhật file `.env`: nếu giá trị gửi lên là `********` thì giữ nguyên giá trị cũ; nếu là giá trị mới thì cập nhật. Tạo file sao lưu `<env>.bak.<timestamp>`, ghi đè nguyên tử, quyền file luôn là **0600**. Audit: `env.edit`.

### `GET /api/v1/pipelines/{name}/logs?lines=N`

`{"source": "journalctl -u strand@x.service | file /path", "lines": [...], "error"}`; mặc định 200 dòng, tối đa 2000; mật khẩu bị che.

### `GET /api/v1/pipelines/{name}/series`

`{"interval_seconds": 5, "samples": [{"t", "state", "lag_seconds", "throughput", "apply_rows_per_s", "holes", "writeset_entries", "spool_bytes"}]}` — tối đa 720 mẫu (1 giờ) từ ring buffer trong RAM.

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

Chế độ file: `<state_dir>/audit.log`. Chế độ PostgreSQL (`database.dsn`): bảng `audit_log` chỉ-thêm (cột `ts, ip, action, pipeline, result, data`); `database.audit: both` (mặc định) vẫn ghi file làm bản sao (USAGE.md 7.15). Mỗi dòng/bản ghi là một JSON: `{"time", "ip", "action", "pipeline", "args", "result": "ok|denied|error|started", "exit_code", "detail"}`. Hành động: `login` (ok/denied, `detail.reason` = `bad_password`/`rate_limited`), `login_csrf_rejected`, `csrf_rejected`, `logout`, `config.view`, `config.edit`, `env.edit`, `pipeline.create`, `autostart` (web tự chạy lại pipeline có trạng thái mong muốn `running` khi khởi động), `state.import` (lần nhập file vào DB đầu tiên), và mọi action ở §4 (hai dòng: `started`, rồi `ok`/`error` kèm `exit_code` khi có). Job kết thúc ghi thêm `job.end` (`result` `ok` khi exit 0, `error` nếu khác hoặc không quan sát được; `exit_code`; `detail.job_id`, `cancelled`).

## 6. Trang HTML

`/` (Overview), `/pipelines`, `/pipelines/new` (Tạo pipeline mới), `/pipelines/{name}` (Graph), `/pipelines/{name}/{tables|checkpoint|errors|config|logs|jobs|repair|promote}`, `/jobs` (mọi job), `/jobs/{id}` (output trực tiếp), `/login`, `POST /login`, `POST /logout`, `/static/*` (công khai). Trang tự làm mới 5 giây bằng `fetch` cùng URL (trừ Config và Logs; Logs có `?follow=1`).
