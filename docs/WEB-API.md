# Strand Web API `/api/v1`

Đặc tả REST của `strand web` (kế hoạch: `docs/PLAN-WEB-UI.md`, quyết định: `questions/DECISIONS-WEB-UI-2026-11-15.md`). Giao diện web và script dùng chung các route này. Phiên bản M2: **chỉ đọc**, cộng đăng nhập/đăng xuất và điểm vào cho thao tác (M3+).

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

## 4. Thao tác (điểm mở rộng cho M3–M5)

`POST /api/v1/pipelines/{name}/actions/{action}` — body JSON `{"khóa": "giá trị chuỗi"}`. M2 chưa đăng ký action nào nên luôn trả **404** `unknown action` (sau khi qua xác thực và CSRF). Với action đã đăng ký, server: kiểm session + CSRF, kiểm tên pipeline, kiểm `confirm_name` khi action yêu cầu (400 nếu sai), ghi dòng audit `started`, chạy, rồi ghi dòng audit kết quả (`ok`/`error`, `exit_code`). Phản hồi **200**: `{"message", "job_id", "exit_code", "data"}`; lỗi **502**.

Thao tác dài chạy qua `JobRunner` (M3): tiến trình con `strand <lệnh>` với đối số dạng mảng, không qua shell.

## 5. Audit log

`<state_dir>/audit.log`, mỗi dòng một JSON: `{"time", "ip", "action", "pipeline", "args", "result": "ok|denied|error|started", "exit_code", "detail"}`. Hành động M2: `login` (ok/denied, `detail.reason` = `bad_password`/`rate_limited`), `login_csrf_rejected`, `csrf_rejected`, `logout`, `config.view`.

## 6. Trang HTML

`/` (Overview), `/pipelines`, `/pipelines/{name}` (Graph), `/pipelines/{name}/{tables|checkpoint|errors|config|logs}`, `/login`, `POST /login`, `POST /logout`, `/static/*` (công khai). Trang tự làm mới 5 giây bằng `fetch` cùng URL (trừ Config và Logs; Logs có `?follow=1`).
