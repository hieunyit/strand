# Báo cáo Milestone M5 — Sửa cấu hình và tạo pipeline mới qua web

Trạng thái: **PASS**  
Nhánh: `feat/web-ui-m5`  
Ngày thực hiện: 2026-11-15  

## 1. Công việc đã thực hiện

### 1.1 Sửa cấu hình pipeline
- **Chế độ sửa YAML trên Web UI & REST API:**
  - Tab Config (`/pipelines/{name}/config`) bổ sung giao diện chuyển đổi giữa xem và sửa cấu hình qua `<textarea>` YAML.
  - Endpoint `POST /api/v1/pipelines/{name}/config/validate` (và action `config-validate`): ghi file tạm cùng thư mục, thực thi `<strand_binary> validate -c <tmp>` với biến môi trường của pipeline, tính toán unified diff so với cấu hình hiện tại và trả về kết quả kiểm tra mà không thay đổi file gốc.
  - Endpoint `POST /api/v1/pipelines/{name}/config` (và action `config-save`):
    - (a) Ghi nội dung mới ra file tạm trong cùng thư mục với file cấu hình gốc.
    - (b) Chạy `strand validate -c <tmp>`. Nếu validate thất bại: xóa file tạm, trả lỗi HTTP 502/400 kèm output, **tuyệt đối không ghi đè** file cấu hình.
    - (c) Nếu validate thành công: tạo bản sao lưu `<file>.bak.<timestamp>`.
    - (d) Ghi đè nguyên tử bằng `os.Rename`.
    - (e) Nạp lại cấu hình và gọi `Registry.Refresh`.
    - (f) Tùy chọn `restart: true`: tự động kích hoạt `ActRestart` để khởi động lại pipeline an toàn.
  - **Bảo mật mật khẩu:**
    - Mật khẩu và thông tin xác thực không bao giờ hiện ra qua Web UI hay REST API (được che bằng `********` qua `MaskConfigYAML`).
    - Nếu người dùng submit cấu hình chứa `********` tại các khóa bí mật cũ, server tự động khôi phục giá trị bí mật từ file gốc (unmask) để tránh ghi đè làm mất mật khẩu.
    - File `.env` được quản lý riêng qua `GET /api/v1/pipelines/{name}/env` và `POST /api/v1/pipelines/{name}/env` (action `env-save`). Giá trị bí mật trong `.env` là write-only (hiển thị `********`; nếu giữ `********` thì giữ nguyên giá trị cũ, nếu nhập chuỗi mới thì cập nhật). File `.env` luôn được lưu với quyền **0600** và giữ bản sao lưu `.bak.<timestamp>`.

### 1.2 Tạo pipeline mới
- **Trang `/pipelines/new` và API `POST /api/v1/pipelines`:**
  - Form thu thập thông tin: tên pipeline (whitelist), engine nguồn/đích (MySQL, MariaDB, PostgreSQL), host/port, user, mật khẩu nguồn/đích, tên biến môi trường (SRC_PW, DST_PW), database, schema, table include/exclude, spool dir, commit order, workers, tuning...
  - **Sinh file YAML + .env:**
    - Tái sử dụng logic cấu hình mẫu từ `cmd/strand/config_init.go`.
    - File `.env` được tạo với mode **0600**, chứa các lệnh `export <VAR>="<password>"`.
    - File YAML chỉ tham chiếu biến môi trường (ví dụ `${SRC_PW}` hoặc `password_env: SRC_PW`), mật khẩu không bao giờ được ghi vào YAML.
    - Thư mục lưu cấu hình: sử dụng `config_dir` khai báo trong `web.yaml`; nếu không khai báo thì mặc định `/etc/strand` nếu có quyền ghi, ngược lại dùng `<state_dir>/pipelines`.
  - **Xác thực và lập kế hoạch tự động:**
    - Chạy `<strand_binary> validate -c <yaml_path>` và `<strand_binary> plan -c <yaml_path>` với biến môi trường từ file `.env` vừa sinh.
    - Hiển thị kết quả validate và plan chi tiết trên giao diện.
    - Nút **Start** ngay trên trang kết quả để khởi động pipeline ở chế độ nền (`strand sync -c <cfg> --background`).
  - **Đăng ký vào hệ thống:**
    - Tự động thêm pipeline vào registry (và ghi vào mục `pipelines` của `web.yaml` nếu không khớp các mẫu `scan`), sau đó gọi `Registry.Refresh`.

### 1.3 An toàn & Bảo mật
- Xác thực và CSRF: Mọi route POST đều đòi hỏi session hợp lệ kèm CSRF token (hoặc Bearer token tĩnh).
- Chặn Path Traversal: Tên pipeline được kiểm tra chặt chẽ theo whitelist `^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`. Kiểm tra đường dẫn file đảm bảo luôn nằm trong `config_dir`.
- Chống trùng lặp: Từ chối với HTTP 409 Conflict nếu tên pipeline hoặc file cấu hình đã tồn tại.
- Audit Log: Mọi thao tác đều được ghi vào `<state_dir>/audit.log` với các action `config.view`, `config.edit`, `env.edit`, `pipeline.create`. Thao tác chỉ được thực hiện khi audit log sẵn sàng ghi.
- Không truyền đối số qua shell: Mọi lệnh con (`validate`, `plan`, `sync`, `stop`) đều truyền đối số dạng mảng `[]string`.

## 2. Quyết định thiết kế
- **Tách biệt code mới để tránh xung đột với M4 (Wizard Repair/Promote):**
  - Toàn bộ logic sửa cấu hình đặt trong `internal/web/configedit.go`.
  - Toàn bộ logic tạo pipeline mới đặt trong `internal/web/newpipeline.go`.
  - Template mới `internal/web/templates/new_pipeline.html`.
  - Chỉ chỉnh sửa tối thiểu các file dùng chung: thêm `ConfigDir` vào `internal/web/config.go`, đăng ký route trong `internal/web/server.go`, cập nhật `internal/web/pages.go`, `internal/web/templates/base.html`, `internal/web/templates/detail.html`, `internal/web/static/app.js`.
- **Thuật toán Unified Diff tự thân (LCS DP):**
  - Không thêm bất kỳ thư viện ngoài nào (tuân thủ tuyệt đối `deps.allow`).
  - Sử dụng thuật toán quy hoạch động LCS để tìm chuỗi thao tác sửa đổi và format theo chuẩn unified diff (`---`, `+++`, `@@ -old,len +new,len @@`).
- **Khôi phục mật khẩu (Unmasking):**
  - Khi người dùng gửi lại cấu hình có chứa `********`, server đối chiếu với file ban đầu để khôi phục giá trị thực sự, tránh trường hợp người dùng vô tình làm hỏng mật khẩu đã cấu hình trước đó.

## 3. Danh sách test và kết quả

Tất cả unit test chạy bằng `go test -v ./internal/web` và đều **PASS** 100%:

| Test | Mục đích kiểm tra | Kết quả |
|---|---|---|
| `TestConfigEditAuthAndCSRF` | Kiểm tra bắt buộc xác thực (401) và CSRF (403) trên các route sửa config/env; kiểm tra đăng ký actions | **PASS** |
| `TestConfigValidateAndDiff` | Kiểm tra `POST .../config/validate` chạy validate trên file tạm và sinh unified diff chính xác | **PASS** |
| `TestConfigSaveValidationFailureDoesNotOverwrite` | Kiểm tra khi validate thất bại thì trả 502/400, không ghi đè file cấu hình, ghi audit error | **PASS** |
| `TestConfigSaveAtomicAndBackup` | Kiểm tra ghi đè nguyên tử, tạo file `<file>.bak.<timestamp>`, restart và ghi audit ok | **PASS** |
| `TestConfigSecretsNeverLeakedAndWriteOnly` | Kiểm tra mật khẩu bị che trong YAML và .env; sửa .env giữ `********` không mất pass; mode 0600 cho .env | **PASS** |
| `TestConfigPathTraversalBlocked` | Kiểm tra chặn các tên pipeline chứa `..%2f`, `%2e%2e`, `orders%00`, `x y` (400/404) | **PASS** |
| `TestNewPipelineAuthAndCSRF` | Bắt buộc đăng nhập (302/401) và CSRF (403) khi tạo pipeline mới | **PASS** |
| `TestNewPipelineCreation` | Tạo pipeline mới: sinh file YAML + .env (mode 0600), chạy validate và plan, nạp vào registry, gọi nút Start | **PASS** |
| `TestNewPipelineDuplicateRejected` | Tên pipeline trùng lặp bị từ chối với 409 Conflict | **PASS** |
| `TestNewPipelinePathTraversalBlocked` | Tên pipeline chứa `../../bad`, `/etc/passwd`, khoảng trắng, ký tự đặc biệt bị từ chối với 400 | **PASS** |
| `TestNewPipelineConfigDirFallback` | Kiểm tra fallback `config_dir` sang `<state_dir>/pipelines` khi không có quyền ghi `/etc/strand` | **PASS** |

Lệnh kiểm tra:
```bash
export PATH=$PATH:/usr/local/go/bin
go test -v -run "TestConfig|TestNewPipeline" ./internal/web
go test ./internal/web
go vet ./internal/web
```

## 4. Tài liệu đã cập nhật
- `docs/WEB-API.md`: Cập nhật đặc tả các endpoint M5 (`POST /api/v1/pipelines`, `POST /api/v1/pipelines/{name}/config/validate`, `POST /api/v1/pipelines/{name}/config`, `GET /api/v1/pipelines/{name}/env`, `POST /api/v1/pipelines/{name}/env`, các actions và audit log).
- `docs/USAGE.md`: Cập nhật mục 7.15 tài liệu hóa tính năng sửa cấu hình và tạo pipeline mới qua web.
- `docs/PLAN-WEB-UI.md`: Cập nhật trạng thái Milestone M5 thành **Xong**.

## 5. Giới hạn & Tồn tại
- Do ràng buộc máy chủ (benchmark chính thức chạy đến 16:30), các test e2e với cơ sở dữ liệu thật và docker containers không được chạy trước 16:30; toàn bộ kiểm tra được thực hiện qua bộ mock/fake strand binary và httptest theo đúng chuẩn M3.
