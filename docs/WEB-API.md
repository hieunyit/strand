# Strand Web API `/api/v1`

Đặc tả REST của `strand web` (kế hoạch: `docs/PLAN-WEB-UI.md`, quyết định: `questions/DECISIONS-WEB-UI-2026-11-15.md`). Giao diện web và script dùng chung các route này. Phiên bản M3: các route đọc, đăng nhập/đăng xuất, **thao tác** (§4) và **job** (§4.3).

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
| `GET /api/v1/session` | `{"authenticated": true, "version", "csrf", "idle_timeout_s", "max_age_s"}` (`csrf` vắng khi dùng Bearer) |

## 3. Tài nguyên

Trạng thái (`state`) của pipeline: `synced`, `bootstrapping`, `paused`, `starting`, `failed`, `stopped`, `unreachable` (tiến trình sống nhưng admin socket không trả lời), `unknown`. Khi admin socket trả lời thì lấy trạng thái từ `top`; không thì từ systemd (`ActiveState`, `ExecMainStatus`) hoặc file pid.

### `GET /api/v1/overview`

```json
{"counts": {"synced": 1, "failed": 0, "stopped": 0, "...": 0},
 "total": 1, "max_lag_seconds": 0.25, "max_lag_pipeline": "orders", "rows_per_sec": 1234,
 "warnings": ["scan: ... skipped ..."],
 "pipelines": [ <PipelineView> ]}
```

### `GET /api/v1/pipelines` → `{"pipelines": [PipelineView], "warnings": []}`

`PipelineView`:

```json
{"name": "orders", "pair": "mysql → postgres", "mode": "systemd|background", "origin": "scan|web.yaml",
 "config_path": "/etc/strand/orders.yaml", "state": "synced", "admin_reachable": true,
 "runtime": {"mode": "systemd", "running": true, "active_state": "active", "sub_state": "running",
             "exit_code": 5, "pid": 1234, "since": "...", "error": "..."},
 "top": {"pipeline_id": "orders", "state": "synced", "lag_seconds": 0.25, "throughput": 1234, "zone": "green",
         "progress": 0, "eta_seconds": 0, "tables_done": 0, "tables_total": 0, "breaker_open": false},
 "last_exit": {"code": 5, "meaning": "...", "action": "...", "auto_restart": false},
 "config_error": "", "sampled_at": "2026-11-15T10:00:00Z"}
```

`runtime.exit_code` và `last_exit` chỉ có khi pipeline đang dừng và systemd biết exit code.

### `GET /api/v1/pipelines/{name}` → `{"pipeline": PipelineView, "status": StatusView}`

### `GET /api/v1/pipelines/{name}/status`

`{"status": <admin status>, "error": "..."}`. `status`: `pipeline_id`, `state`, `epoch`, `started_at`, `uptime_seconds`, `start_at`, `start_position`, `source_kind`, `target_kind`, `commit_order`, `workers`, `paused`, `snapshot_paused`, `applied_txns`, `applied_rows`, `governor{algorithm, foreground_rate, background_rate, zone, pressure, breaker_open, breaker_reason}`. Pipeline dừng: chỉ có `error`.

### `GET /api/v1/pipelines/{name}/tables`

`{"tables": [{"table", "state": "done|running|pending", "ranges": {"Done": 2, "Pending": 6}, "rows_estimate", "rows_copied", "progress_pct", "eta_seconds", "pk": ["id"], "generic_pk": false}], "done", "running", "pending", "error"}`. `rows_estimate` lấy từ thống kê của source (lệch được); `rows_copied` là xấp xỉ theo tiến độ PK.

### `GET /api/v1/pipelines/{name}/checkpoint`

`{"checkpoint": {"pipeline_id", "epoch", "applied", "updated_at"}, "start_position", "compaction_lag_seconds", "error"}`. Chỉ có khi pipeline đang chạy.

### `GET /api/v1/pipelines/{name}/errors`

`{"live_events": [LogEvent], "log_events": [LogEvent], "live_error", "last_exit": {code, meaning, action, auto_restart}, "exit_from": "systemd|log", "error_code"}`. `live_events` là ring 200 bản ghi WARN/ERROR của tiến trình đang chạy; `log_events` rút từ đuôi log (cho cả pipeline đã dừng). `last_exit` có khi pipeline dừng: từ systemd, hoặc suy ra từ mã lỗi `[code]` trong log (`exit_from = "log"`). `LogEvent`: `{"time", "level", "message"}`; mật khẩu trong message bị che.

### `GET /api/v1/pipelines/{name}/config`

`{"path", "yaml", "unimplemented_keys": [...], "error"}`. `yaml` là file cấu hình đọc thô (chưa thay `${VAR}`), mọi giá trị bí mật bị thay bằng `********` (khóa có `password`, `secret`, `token`, `credential`, `private`, `encryption_key`, `access_key`, mật khẩu nhúng trong DSN/URL). Mỗi lần xem được ghi audit `config.view`.

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

`<state_dir>/audit.log`, mỗi dòng một JSON: `{"time", "ip", "action", "pipeline", "args", "result": "ok|denied|error|started", "exit_code", "detail"}`. Hành động: `login` (ok/denied, `detail.reason` = `bad_password`/`rate_limited`), `login_csrf_rejected`, `csrf_rejected`, `logout`, `config.view`, và mọi action ở §4 (hai dòng: `started`, rồi `ok`/`error` kèm `exit_code` khi có). Job kết thúc ghi thêm `job.end` (`result` `ok` khi exit 0, `error` nếu khác hoặc không quan sát được; `exit_code`; `detail.job_id`, `cancelled`).

## 6. Trang HTML

`/` (Overview), `/pipelines`, `/pipelines/{name}` (Graph), `/pipelines/{name}/{tables|checkpoint|errors|config|logs|jobs|repair|promote}`, `/jobs` (mọi job), `/jobs/{id}` (output trực tiếp), `/login`, `POST /login`, `POST /logout`, `/static/*` (công khai). Trang tự làm mới 5 giây bằng `fetch` cùng URL (trừ Config và Logs; Logs có `?follow=1`).
