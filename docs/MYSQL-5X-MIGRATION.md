# Chuyển MySQL 5.5 / 5.6 / 5.7 lên MySQL 8.x hoặc PostgreSQL bằng Strand

Cập nhật: 2026-11-12, ứng với nhánh `main` từ commit `57cd109` trở đi.

Người mới: làm theo **mục 0** (build) rồi **mục A** (chạy thử 15 phút). Mọi lệnh trong hai mục này đã được chạy thật trên MySQL 5.6 → MySQL 8.4 và cho đúng kết quả in ở đây. Các mục 1–11 là chi tiết cho lúc chạy thật.

Phần chung (cấu hình đầy đủ, mọi lệnh, metric, exit code) xem `docs/USAGE.md`.

## Mục lục

0. [Build và cài đặt](#0-build-và-cài-đặt)
A. [Chạy thử từ đầu đến cuối (15 phút)](#a-chạy-thử-từ-đầu-đến-cuối-15-phút)
1. [Hỗ trợ gì](#1-hỗ-trợ-gì)
2. [Chọn cách chạy](#2-chọn-cách-chạy)
3. [Chuẩn bị nguồn 5.x](#3-chuẩn-bị-nguồn-5x)
4. [Chuẩn bị đích](#4-chuẩn-bị-đích)
5. [File cấu hình](#5-file-cấu-hình)
6. [Chạy: kiểm tra trước](#6-chạy-kiểm-tra-trước)
7. [Chạy: nạp dữ liệu ban đầu và stream](#7-chạy-nạp-dữ-liệu-ban-đầu-và-stream)
8. [Theo dõi và kiểm tra dữ liệu](#8-theo-dõi-và-kiểm-tra-dữ-liệu)
9. [Cutover (promote)](#9-cutover-promote)
10. [Giới hạn và xử lý sự cố](#10-giới-hạn-và-xử-lý-sự-cố)
11. [Checklist một trang](#11-checklist-một-trang)
12. [Chạy thường trực bằng systemd](#12-chạy-thường-trực-bằng-systemd)

---

## 0. Build và cài đặt

Strand là **một file chạy duy nhất** (`strand`), không cần cài thêm thư viện. Chạy trên Linux (amd64 hoặc arm64). Máy chạy Strand chỉ cần kết nối mạng tới MySQL nguồn và CSDL đích; không cần cài trên server MySQL.

### 0.1 Build từ mã nguồn

Cần Go **1.25 trở lên** (`go version` để kiểm tra; tải ở https://go.dev/dl/).

```bash
git clone https://github.com/hieunyit/strand.git
cd strand
git checkout main
CGO_ENABLED=0 go build -trimpath \
  -ldflags "-s -w -X main.version=$(git describe --tags --always) -X main.commit=$(git rev-parse --short HEAD)" \
  -o strand ./cmd/strand
./strand version          # ví dụ: strand 57cd109 (57cd109) linux/amd64 go1.27.3
```

`CGO_ENABLED=0` cho ra binary tĩnh, chép sang máy Linux khác cùng kiến trúc là chạy được. Build cho arm64: thêm `GOARCH=arm64` trước `go build`.

Cài vào PATH:

```bash
sudo install -m 0755 strand /usr/local/bin/strand
strand --help             # danh sách lệnh
strand sync --help        # cờ của từng lệnh
```

### 0.2 Hoặc đóng gói `.deb` / `.rpm`

```bash
VERSION=1.0.0 scripts/package.sh          # cần mạng lần đầu (tải nfpm qua go run)
ls dist/                                  # strand_1.0.0_amd64.deb, strand-1.0.0-1.x86_64.rpm, tarball + .sha256
sudo dpkg -i dist/strand_1.0.0_amd64.deb  # hoặc: sudo rpm -i dist/strand-1.0.0-1.x86_64.rpm
```

Gói cài `/usr/bin/strand`, unit systemd `strand@.service`, user hệ thống `strand` và thư mục `/etc/strand`. Chạy bằng systemd: mục 12.

---

## A. Chạy thử từ đầu đến cuối (15 phút)

Ví dụ: MySQL 5.6 nguồn (không GTID) ở `10.0.0.11`, MySQL 8.4 đích ở `10.0.0.21`, database ứng dụng là `app`. Thay địa chỉ, mật khẩu cho đúng môi trường của bạn. Nên thử trên bản sao dữ liệu trước.

**Bước 1 — Nguồn 5.x đã bật binlog dạng ROW.** Kiểm tra:

```sql
SELECT @@log_bin, @@binlog_format, @@server_id;   -- cần: 1, ROW, khác 0
```

Nếu chưa đúng, sửa `my.cnf` theo mục 3.1 rồi restart MySQL nguồn.

**Bước 2 — Tạo user cho Strand trên nguồn** (chạy bằng root của nguồn):

```sql
CREATE USER 'strand'@'%' IDENTIFIED BY 'srcpw';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'strand'@'%';
GRANT SELECT, LOCK TABLES ON app.* TO 'strand'@'%';
```

**Bước 3 — Chuẩn bị đích 8.x** (chạy bằng root của đích):

```sql
CREATE DATABASE strand;                       -- nơi Strand lưu checkpoint (_strand_*)
CREATE USER 'strand'@'%' IDENTIFIED BY 'dstpw';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON app.*    TO 'strand'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON strand.* TO 'strand'@'%';
GRANT CONNECTION_ADMIN ON *.* TO 'strand'@'%';
```

Không cần tạo bảng `app.*` ở đích: Strand tự tạo từ cấu trúc của nguồn.

**Bước 4 — Viết file cấu hình** `legacy-app.yaml`, đặt cạnh binary:

```yaml
version: 1
name: legacy-app
source:
  kind: mysql
  host: 10.0.0.11
  port: 3306
  user: strand
  password_env: SRC_PW          # mật khẩu lấy từ biến môi trường SRC_PW
  server_id: 47001              # số bất kỳ, không trùng server_id nào trong cụm MySQL
target:
  kind: mysql
  host: 10.0.0.21
  port: 3306
  user: strand
  password_env: DST_PW
  database: strand
  commit_order: relaxed
tables:
  include: ["app.*"]
resources:
  spool: { path: ./state }      # thư mục trạng thái cục bộ; Strand tự tạo
observability:
  admin_socket: ./run/admin.sock   # để dùng strand top / verify / checkpoint
  metrics: "127.0.0.1:9464"
```

```bash
mkdir -p state run
export SRC_PW='srcpw' DST_PW='dstpw'
```

**Bước 5 — Kiểm tra trước:**

```bash
strand validate -c legacy-app.yaml
```

Kết quả mong đợi (hai dòng `warning` là bình thường với nguồn 5.6):

```
warning: source is MySQL 5.6: binlog_row_metadata is not available (...); column metadata is read from the source catalog
warning: cannot verify per-session binlog settings (... variables_by_thread ...)
2 table(s) will be created on target
Preflight validation passed for pipeline: legacy-app
```

```bash
strand plan -c legacy-app.yaml       # số row, dung lượng, ETA snapshot so với thời gian giữ binlog
```

**Bước 6 — Chạy đồng bộ** (để chạy trong cửa sổ terminal riêng, hoặc `tmux`):

```bash
strand sync -c legacy-app.yaml
```

Log in dạng JSON. Các dòng chính, theo thứ tự:

```
"source has no GTID (gtid_mode OFF or absent): running in BINLOG POSITION MODE; ..."
"logical bootstrap in binlog position mode: streaming from the current head"
"target schema initialized"
"snapshot start" / "snapshot done"        (mỗi bảng một cặp)
"bootstrap finished: pipeline synced"
```

Ứng dụng vẫn ghi vào nguồn bình thường trong lúc này.

**Bước 7 — Theo dõi** (terminal khác, cùng thư mục, cùng biến môi trường):

```bash
strand top -c legacy-app.yaml
```

```
Pipeline:   legacy-app
Trạng thái: synced
Lag:        0.000 s
```

**Bước 8 — Kiểm dữ liệu từng bảng:**

```bash
strand verify -c legacy-app.yaml --mode full --table app.customers
strand verify -c legacy-app.yaml --mode full --table app.orders
# verify app.orders (full): 3 chunks (...), 0 mismatched chunks, ..., 0 mismatches, 0 unverifiable_hot
```

`0 mismatches` là khớp.

**Bước 9 — Khi nguồn có ALTER TABLE.** Strand **không tự áp DDL**. Khi gặp `ALTER` trên bảng đang đồng bộ, `sync` dừng với exit 10 và in đúng câu lệnh cần chạy:

```
Error [schema_change_pending]: DDL at mysql-bin.000004:2334 changes pipeline table(s) and the target schema
does not match yet: ... Apply the change on the target, then resume.
DDL: "ALTER TABLE app.customers ADD COLUMN email VARCHAR(100) NULL"
```

Chạy đúng câu `ALTER` đó trên đích, rồi chạy lại `strand sync -c legacy-app.yaml`. Strand đi tiếp từ checkpoint, không mất dữ liệu.

**Bước 10 — Dừng / chạy lại.** `Ctrl-C` dừng an toàn (log `pipeline stopped cleanly`). Chạy lại đúng lệnh ở bước 6 thì đi tiếp từ chỗ cũ, không snapshot lại.

**Bước 11 — Cutover** khi đã sẵn sàng chuyển ứng dụng sang đích. Giữ `strand sync` đang chạy, ở terminal khác:

```bash
strand promote -c legacy-app.yaml --pipeline legacy-app \
  --target "root:***@tcp(10.0.0.21:3306)/strand" \
  --fence  "root:***@tcp(10.0.0.11:3306)/" \
  --source-app-roles app_user --fence-timeout 30s --catchup-timeout 5m
```

```
Promote pipeline legacy-app thành công. Target đã mở quyền ghi.
Lưu ý nguồn MySQL cũ (...): khóa FTWRL nhả khi lệnh này thoát, read_only=ON vẫn giữ nhưng KHÔNG chặn user SUPER. ...
```

Sau đó: nguồn ở `read_only=1`, đích mở ghi, bộ đếm `AUTO_INCREMENT` đã chép sang đích. Trỏ ứng dụng sang đích, rồi `Ctrl-C` để dừng `strand sync`. Chi tiết: mục 9.

`--source-app-roles`: tên user MySQL của ứng dụng trên nguồn, Strand ngắt các session của user này. `--target`/`--fence` dùng user có quyền đổi `read_only` (root hoặc user có `SUPER`).

---

## 1. Hỗ trợ gì

| Nguồn | GTID | Chế độ của Strand | Kiểm thử tự động (e2e) |
| --- | --- | --- | --- |
| MySQL 5.5 | không có | Vị trí binlog | Có |
| MySQL 5.6 | `gtid_mode=OFF` | Vị trí binlog | Có |
| MySQL 5.6 | `gtid_mode=ON` | GTID | **Chưa có e2e**. Code cho phép nhưng nên thử kỹ trên bản sao trước |
| MySQL 5.7 | `gtid_mode=OFF` | Vị trí binlog | Có |
| MySQL 5.7 | `gtid_mode=ON` | GTID (như nguồn 8.x) | Có |
| MySQL 5.4 trở xuống | — | Bị từ chối: `MySQL source 5.5 or later is required` | — |
| MariaDB 10.6 | (GTID MariaDB không dùng) | Vị trí binlog, xem [MARIADB-MIGRATION.md](MARIADB-MIGRATION.md) | Có |

MySQL 5.4 chưa từng có bản GA, nên không hỗ trợ.

Đích: MySQL 8.4 (có e2e), MySQL 8.0 (chưa có e2e), PostgreSQL 16/17/18.

**Chế độ vị trí binlog:** Strand định danh mỗi giao dịch bằng `(file binlog, vị trí kết thúc)` thay cho GTID. Strand tự chọn chế độ này khi nguồn không có GTID và in cảnh báo:

```
source has no GTID (gtid_mode OFF or absent): running in BINLOG POSITION MODE; ...
```

Checkpoint vẫn nằm trong target. Dừng hoặc kill -9 rồi chạy lại không làm mất hay lặp dữ liệu, giống chế độ GTID.

Những gì chạy được ở chế độ vị trí binlog:

| Tính năng | Chế độ vị trí binlog | Ghi chú |
| --- | --- | --- |
| Snapshot logic + stream (`sync` không có `--start-at`) | Có | |
| `--start-at <file>:<pos>` | Có | Ví dụ `mysql-bin.000123:4567` |
| Seed từ `mysqldump` | Có | `--single-transaction --master-data=2` |
| Seed từ `mydumper` | Có | Lấy file/pos trong `metadata` |
| Seed từ XtraBackup, CLONE | **Không** | XtraBackup cần GTID; CLONE cần nguồn 8.0.17+ |
| `snapshot add` | Có | |
| ALTER / DROP COLUMN trên nguồn khi đang chạy | Có, **bán tự động** | `sync` dừng (exit 10) và in câu DDL; chạy câu đó trên đích rồi chạy lại `sync` (mục A, bước 9). Strand lưu lịch sử schema nên row cũ trước DDL vẫn giải mã đúng |
| `CREATE TABLE` mới trên nguồn khi đang chạy | **Không tự động** | Phải tạo bảng ở đích bằng tay (mục 10) |
| `verify --mode full` | Có | |
| `promote` | Có | Fence bằng `read_only` + `FLUSH TABLES WITH READ LOCK` (mục 9) |
| `checkpoint rewind` | **Không** | |
| Đọc từ replica / đổi sang server nguồn khác | **Không** | Vị trí binlog khác nhau trên mỗi server |
| Prefetch của `seed apply` | **Không** | Strand bỏ qua và in `note: ... prefetch is skipped` |

---

## 2. Chọn cách chạy

```
Nguồn có bật GTID (5.7 gtid_mode=ON)?
├─ Có  → làm như nguồn 8.x (docs/USAGE.md). Nạp ban đầu: snapshot logic, mysqldump hoặc mydumper
└─ Không (5.5, hoặc 5.6/5.7 gtid_mode=OFF) → chế độ vị trí binlog. Chọn một trong ba:
     A. Snapshot logic    — đơn giản nhất, không cần backup. Hợp với DB vừa và nhỏ
                            (strand plan báo ETA < 50% thời gian giữ binlog)
     B. Seed mysqldump/mydumper — DB lớn, hoặc muốn nạp nhanh bằng công cụ của bạn
     C. --start-at file:pos — bạn đã tự có một bản đích khớp nguồn tại một vị trí binlog đã biết
```

Không cần bật GTID trên nguồn cũ. Trên 5.6, đổi `gtid_mode` cần restart toàn bộ topology, nên chế độ vị trí binlog thường là lựa chọn đúng.

---

## 3. Chuẩn bị nguồn 5.x

### 3.1 Biến server

MySQL 5.5/5.6 không có `SET PERSIST`. Đặt các biến trong `my.cnf` rồi restart. Các biến có thể đổi online thì dùng `SET GLOBAL`, rồi ghi thêm vào `my.cnf`.

```ini
[mysqld]
server-id        = 1              # khác 0, duy nhất trong cụm
log-bin          = mysql-bin      # bắt buộc; KHÔNG đổi tên này khi pipeline đang dùng
binlog_format    = ROW
binlog_row_image = FULL           # 5.6+; 5.5 không có biến này (luôn ghi đầy đủ)
expire_logs_days = 7              # thời gian giữ binlog (5.x); xem 3.2
# Chỉ khi đọc từ replica của nguồn:
# log-slave-updates = 1
```

| Biến | Cần | Ghi chú |
| --- | --- | --- |
| `log_bin` | `ON` | Thiếu thì `validate` báo `binary logging is disabled on the source` |
| `binlog_format` | `ROW` | Biến theo session: kết nối cũ của ứng dụng giữ giá trị cũ. Cho ứng dụng kết nối lại sau khi đổi |
| `binlog_row_image` | `FULL` | 5.6/5.7 |
| `server_id` | khác 0 | |
| `gtid_mode` | `ON` hoặc `OFF` | `ON_PERMISSIVE` / `OFF_PERMISSIVE` bị từ chối |
| `binlog_row_metadata` | — | Không có ở 5.x. Strand đọc tên cột, unsigned, charset và ENUM/SET từ `information_schema` |

### 3.2 Thời gian giữ binlog

5.x dùng `expire_logs_days`. Strand quy đổi thành giây.

Binlog phải còn giữ, tính từ vị trí bắt đầu, trong suốt:

- **Cách A:** toàn bộ thời gian snapshot logic. `strand plan` tính ETA và từ chối khi ETA vượt 50% thời gian giữ.
- **Cách B:** thời gian dump + chép + restore + bắt kịp.

Nếu file binlog tại vị trí bắt đầu đã bị xóa, `sync` dừng với `log_gap` (exit 5).

**Không chạy `RESET MASTER` và không đổi `log-bin` trên nguồn** khi pipeline đang dùng. Strand phát hiện vị trí binlog lùi lại hoặc đổi tên file, rồi dừng.

### 3.3 User trên nguồn

```sql
CREATE USER 'strand'@'%' IDENTIFIED BY '***';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'strand'@'%';
GRANT SELECT ON app.* TO 'strand'@'%';
-- Cách A cần LOCK TABLES để lấy watermark (5.5/5.6 không có WAIT_FOR_EXECUTED_GTID_SET):
GRANT LOCK TABLES ON app.* TO 'strand'@'%';
```

Promote cần user có `SUPER` trên nguồn, vì phải đặt `read_only` và chạy `FLUSH TABLES WITH READ LOCK`.

---

## 4. Chuẩn bị đích

### 4.1 Đích MySQL 8.x

- Tạo database chứa metadata của Strand (bảng `_strand_*`):
  ```sql
  SET SESSION sql_log_bin=0; CREATE DATABASE IF NOT EXISTS strand; SET SESSION sql_log_bin=1;
  ```
- **Schema các bảng:**
  - Cách A và C: để `bootstrap.create_schema: auto` (mặc định). Strand tạo bảng từ `SHOW CREATE TABLE` của nguồn lúc khởi động. Nếu muốn tự kiểm DDL, chạy `strand schema init -c p.yaml --dry-run`.
  - Cách B: bản dump đã chứa DDL.
- **Khác biệt 5.x → 8.x Strand tự xử lý:**
  - Độ rộng hiển thị số nguyên (`int(11)` của nguồn với `int` của 8.0.19+) không bị coi là lệch schema.
  - Ngày `0000-00-00`: session ghi của Strand nới `sql_mode` để giá trị này đi qua được.
  - Kiểu `TIME`/`DATETIME`/`TIMESTAMP` định dạng cũ (trước 5.6.4) được giải mã đúng.
- **Charset:** bảng `utf8` (utf8mb3) ở nguồn vẫn là `utf8mb3` ở đích nếu Strand tạo bảng. Muốn chuyển sang `utf8mb4` thì tạo bảng đích bằng tay trước khi chạy `sync`.
- User `strand` trên đích: quyền như `docs/USAGE.md` mục 4.3.

### 4.2 Đích PostgreSQL

Giống MySQL→PG ở `docs/USAGE.md` mục 3.2 (ánh xạ kiểu). Đích PG không có `sql_log_bin`, chỉ cần database và schema đích.

---

## 5. File cấu hình

### 5.1 MySQL 5.5/5.6 (không GTID) → MySQL 8.4

```yaml
version: 1
name: legacy-app                     # ID pipeline; giữ nguyên suốt vòng đời
source:
  kind: mysql
  host: 10.0.0.11                    # PHẢI là server có binlog sẽ đọc; không đổi server sau này
  port: 3306
  user: strand
  password_env: SRC_PW
  server_id: 47001                   # duy nhất trong cụm (Strand đăng ký như một replica)
target:
  kind: mysql
  host: 10.0.0.21
  user: strand
  password_env: DST_PW
  database: strand                   # nơi chứa bảng _strand_*
  commit_order: relaxed              # nhanh hơn; strict = đúng thứ tự commit, một luồng ghi
tables:
  include: ["app.*"]
  exclude: ["app.tmp_*"]
bootstrap:
  default: logical                   # cách A. Cách B/C: seed hoặc start_at, kèm --start-at
  create_schema: auto
  logical: { chunk_rows: 5000 }
resources:
  spool: { path: /var/lib/strand/legacy-app }
  source: { iops: { soft: 600 } }    # tốc độ đọc snapshot; đổi nóng bằng strand throttle
  workers: { max: 8 }
verify: { post_seed_window: 30m }
observability:
  admin_socket: /run/strand/legacy-app/admin.sock   # systemd: nằm trong RuntimeDirectory (mục 12)
  metrics: ":9464"
  log: { level: info, format: json }
```

### 5.2 MySQL 5.7 (GTID ON) → PostgreSQL

```yaml
version: 1
name: legacy-to-pg
source: { kind: mysql, host: 10.0.0.12, user: strand, password_env: SRC_PW, server_id: 47002 }
target:
  kind: postgres
  dsn: "postgres://strand:${DST_PW}@pg-dst:5432/app?sslmode=require"
tables: { include: ["app.*"] }
resources: { spool: { path: /var/lib/strand/legacy-to-pg } }
observability: { admin_socket: /run/strand/legacy-to-pg/admin.sock, metrics: ":9465" }
```

Mật khẩu: đặt biến môi trường (`export SRC_PW=... DST_PW=...`) trước khi chạy, không ghi thẳng vào file.

---

## 6. Chạy: kiểm tra trước

```bash
strand validate -c legacy-app.yaml   # exit 0 = đạt; exit 3 = bị từ chối, kèm lý do
strand plan     -c legacy-app.yaml   # số row, dung lượng, ETA snapshot, so với thời gian giữ binlog
```

`validate` kiểm:

- Phiên bản nguồn (MySQL ≥ 5.5, hoặc MariaDB 10.6).
- `log_bin`, `binlog_format`, `binlog_row_image`, `gtid_mode`.
- Bảng có PRIMARY KEY và mẫu `include` có khớp bảng.
- Ánh xạ kiểu (nếu đích là PG).

Lỗi thường gặp:

| Thông báo | Sửa |
| --- | --- |
| `MySQL source 5.5 or later is required` | Nguồn quá cũ |
| `binary logging is disabled on the source` | Bật `log-bin` trong `my.cnf`, restart |
| `gtid_mode must be ON (GTID mode) or OFF (binlog position mode)` | Đưa `gtid_mode` về `ON` hoặc `OFF` |
| `binlog_format must be ROW` | `SET GLOBAL binlog_format=ROW`, ghi vào `my.cnf`, cho ứng dụng kết nối lại |
| `missing_pk` | Thêm PRIMARY KEY cho bảng, hoặc `exclude` bảng đó |

---

## 7. Chạy: nạp dữ liệu ban đầu và stream

`sync` chạy foreground. Dùng systemd (`deploy/`) hoặc `tmux`/`screen` khi chạy thật. `Ctrl-C`/`SIGTERM` dừng an toàn (exit 0). Chạy lại cùng lệnh `strand sync -c ...` (không `--start-at`) thì tiếp tục từ checkpoint.

### 7.A Snapshot logic (không cần backup)

```bash
strand sync -c legacy-app.yaml
```

Strand làm theo thứ tự:

1. Ghi nhận vị trí hiện tại của binlog.
2. Tạo bảng ở đích.
3. Đọc từng bảng theo chunk, có điều tốc, song song với stream binlog.
4. Báo `bootstrap finished: pipeline synced`.

### 7.B Seed bằng mysqldump (nguồn không GTID)

```bash
# 1. Dump trên nguồn (5.5/5.6/5.7). --master-data=2 ghi vị trí CHANGE MASTER (dạng comment) vào file.
mysqldump -u root -p --single-transaction --master-data=2 \
  --default-character-set=utf8mb4 --databases app > dump.sql

# 2. Nạp vào đích và tạo database metadata.
mysql -h 10.0.0.21 -u root -p < dump.sql
mysql -h 10.0.0.21 -u root -p -e "SET SESSION sql_log_bin=0; CREATE DATABASE IF NOT EXISTS strand; SET SESSION sql_log_bin=1;"

# 3. Lập manifest: đọc file/pos từ dump, kiểm binlog nguồn còn phủ từ vị trí đó.
strand seed plan --type mysqldump --backup dump.sql \
  --source "root:***@tcp(10.0.0.11:3306)/" --target "root:***@tcp(10.0.0.21:3306)/" --out seed.json

# 4. Kiểm đích (prefetch tự bỏ qua ở chế độ vị trí binlog).
strand seed apply --manifest seed.json --target "root:***@tcp(10.0.0.21:3306)/" \
  --spool /var/lib/strand/legacy-app/prefetch --source "root:***@tcp(10.0.0.11:3306)/"

# 5. Chạy từ vị trí của bản dump. Đặt bootstrap.default: seed trong file cấu hình.
strand sync -c legacy-app.yaml --start-at seed --seed-manifest seed.json
```

- Ứng dụng **có thể ghi tiếp** trên nguồn trong lúc dump và restore. Các giao dịch sau vị trí dump được Strand phát lại.
- Sau khi bắt kịp, Strand chạy verify sau seed trong `verify.post_seed_window` rồi mới báo `synced`.
- **mydumper:** `--type mydumper --backup <thư mục dump> --dump-log <file stderr của mydumper>`. Vị trí lấy từ file `metadata`.
- **Nguồn 5.7 GTID ON:** dùng thêm `--set-gtid-purged=ON` và chạy `RESET MASTER` trên đích trước khi nạp. 5.7 ghi `GTID_PURGED` ở cuối file; Strand tự tính P từ vị trí `CHANGE MASTER`.

### 7.C `--start-at <file>:<pos>`

Dùng khi đích đã khớp nguồn tại một vị trí binlog bạn biết chắc, ví dụ đích từng là replica, đã `STOP SLAVE`, và bạn ghi lại `Relay_Master_Log_File`/`Exec_Master_Log_Pos`.

```bash
strand sync -c legacy-app.yaml --start-at mysql-bin.000123:4567
```

Vị trí phải là ranh giới event, trong file còn tồn tại, và không vượt cuối file. Sai thì `sync` từ chối với `not an event boundary`, `log gap` hoặc `beyond the end`.

---

## 8. Theo dõi và kiểm tra dữ liệu

```bash
strand top -c legacy-app.yaml        # trạng thái (bootstrapping/synced), lag, row/s, tiến độ snapshot
strand checkpoint show -c legacy-app.yaml
curl -s localhost:9464/metrics | grep strand_lag_seconds
```

- Trạng thái mong đợi: `bootstrapping` → `synced`.
- **Kiểm dữ liệu trước cutover:** `--table` không nhận wildcard, nên chạy từng bảng:
  ```bash
  for t in app.users app.orders; do strand verify -c legacy-app.yaml --mode full --table $t; done
  ```
  Kết quả `0 mismatches` là khớp. Row đang bị ghi liên tục có thể được đếm `unverifiable_hot`; đó không phải lệch.
- **Tạm dừng / điều tốc:** `strand pause`, `strand resume`, `strand throttle set --source-iops 300`.

---

## 9. Cutover (promote)

Giữ `strand sync` chạy trong lúc promote.

```bash
strand promote -c legacy-app.yaml --pipeline legacy-app \
  --target "strand_admin:***@tcp(10.0.0.21:3306)/strand" \
  --fence  "root:***@tcp(10.0.0.11:3306)/" \
  --source-app-roles app_user \
  --fence-timeout 30s --catchup-timeout 5m
```

Các bước Strand làm với nguồn 5.5 / 5.6 / 5.7 < 5.7.8 (không có `super_read_only`):

1. Đặt `read_only=ON` và giữ `FLUSH TABLES WITH READ LOCK` trên một kết nối riêng suốt quá trình promote. Lúc này cả user `SUPER` cũng không ghi được.
2. Ngắt session của các user trong `--source-app-roles`.
3. Chờ pipeline áp nốt tới cuối binlog nguồn, tối đa `--catchup-timeout`.
4. Chuyển bộ đếm `AUTO_INCREMENT` của từng bảng từ nguồn sang đích. `-c` cung cấp `tables.rename` nếu có.
5. Mở quyền ghi trên đích.

Sau khi lệnh thoát:

- Khóa FTWRL **được nhả**. `read_only=ON` vẫn giữ, nhưng **không chặn user `SUPER`**.
- Giữ nguồn tắt, hoặc chặn kết nối ứng dụng, cho tới khi trỏ ứng dụng sang đích xong.
- Rồi dừng `strand sync` (`Ctrl-C`).

Promote bị ngắt giữa chừng thì chạy lại cùng lệnh: nó tiếp tục từ bước dở.

---

## 10. Giới hạn và xử lý sự cố

| Tình huống | Hành vi / cách xử lý |
| --- | --- |
| `CREATE TABLE` mới trên nguồn (khớp `include`) khi đang chạy, chế độ vị trí binlog | Pipeline dừng với `cannot prove source schema at CREATE ...` (exit 10). Tạo bảng ở đích bằng tay (cùng cấu trúc), rồi chạy lại `sync` |
| Muốn thêm bảng có sẵn vào pipeline | `strand snapshot add --table app.t -c p.yaml` khi đang chạy, hoặc thêm vào `include` rồi khởi động lại |
| Muốn đọc từ replica, hoặc nguồn failover sang server khác | Không hỗ trợ ở chế độ vị trí binlog. Pipeline phải bắt đầu lại (A hoặc B) với server mới |
| `log_gap` (exit 5) | Binlog tại vị trí cần đọc đã bị xóa. Tăng `expire_logs_days`, nạp lại (A hoặc B) |
| `binlog positions are not monotonic (RESET MASTER or replaced binlog?)` | Có người `RESET MASTER` hoặc thay binlog trên nguồn. Bắt đầu lại pipeline |
| `checkpoint rewind` | Không có ở chế độ vị trí binlog |
| Manifest seed | Không chứa định danh server nguồn ở chế độ vị trí binlog. **Tự bảo đảm** dump lấy từ đúng server trong cấu hình |
| Tốc độ | Không có prefetch. Strict (một luồng ghi) chậm hơn relaxed. Đích PG ở strict khoảng vài trăm giao dịch/giây |
| Exit code | 0 dừng sạch; 2 cấu hình; 3 bị từ chối; 4 bị instance khác thay; 5 dữ liệu/log; 10 cần quyết định của người vận hành (xem `docs/USAGE.md` 8.3) |

---

## 11. Checklist một trang

1. Nguồn: `log-bin`, `binlog_format=ROW`, `server-id`, `expire_logs_days` đủ dài. User `strand` có quyền ở mục 3.3.
2. Đích: MySQL 8.x hoặc PG đã chạy. Đã tạo database `strand` (MySQL). User `strand` có quyền.
3. Viết `legacy-app.yaml` (mục 5). Đặt `SRC_PW`, `DST_PW`.
4. `strand validate -c legacy-app.yaml` ra exit 0. `strand plan` ra ETA hợp lý.
5. Nạp ban đầu: chọn A (`sync`), B (mysqldump/mydumper + `seed plan` + `seed apply` + `sync --start-at seed`) hoặc C (`--start-at file:pos`).
6. Chờ `strand top` báo `synced`. Lag ổn định gần 0.
7. `strand verify --mode full --table ...` từng bảng: 0 mismatches.
8. Hẹn giờ cutover: `strand promote ...` (mục 9). Trỏ ứng dụng sang đích. Dừng `sync`. Tắt/khóa nguồn.

---

## 12. Chạy thường trực bằng systemd

Cài bằng gói (mục 0.2) thì unit `strand@.service` đã có. Cài binary tay (mục 0.1) thì chép unit và tạo user:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin strand
sudo cp deploy/systemd/strand@.service /etc/systemd/system/   # unit gọi /usr/local/bin/strand
sudo install -d -m 0750 -o root -g strand /etc/strand
sudo systemctl daemon-reload
```

Mỗi pipeline một instance `strand@<name>`, đọc `/etc/strand/<name>.yaml` và `/etc/strand/<name>.env`:

```bash
sudo cp legacy-app.yaml /etc/strand/legacy-app.yaml
printf 'SRC_PW=srcpw\nDST_PW=dstpw\n' | sudo tee /etc/strand/legacy-app.env >/dev/null
sudo chown root:strand /etc/strand/legacy-app.*
sudo chmod 0640 /etc/strand/legacy-app.yaml; sudo chmod 0600 /etc/strand/legacy-app.env
sudo systemctl enable --now strand@legacy-app
journalctl -u strand@legacy-app -f        # xem log
strand top -c /etc/strand/legacy-app.yaml
```

Trong file cấu hình khi chạy bằng systemd, **phải** dùng đúng các đường dẫn mà unit cho phép ghi:

```yaml
resources:
  spool: { path: /var/lib/strand/legacy-app }              # StateDirectory
observability:
  admin_socket: /run/strand/legacy-app/admin.sock          # RuntimeDirectory
```

- Unit **không tự restart** khi exit 2, 3, 4, 5 hoặc 10 (lỗi cấu hình, bị từ chối, bị instance khác thay, mất log, cần người quyết định, ví dụ ALTER ở mục A bước 9). Xử lý xong thì `sudo systemctl start strand@legacy-app`.
- Lỗi khác (mất mạng, DB restart): systemd tự chạy lại sau 10 giây; Strand đi tiếp từ checkpoint.
- Dừng: `sudo systemctl stop strand@legacy-app` (gửi SIGTERM, dừng sạch).
