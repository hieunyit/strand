# Chuyển MariaDB 10.6 lên MySQL 8.x bằng Strand

Strand chép toàn bộ dữ liệu từ MariaDB 10.6 sang MySQL 8.x, sau đó đồng bộ liên tục các thay đổi (đọc binlog) cho tới khi bạn cutover (`promote`) ứng dụng sang MySQL. Ứng dụng vẫn ghi vào MariaDB trong suốt quá trình. Thời gian dừng ghi chỉ là lúc promote, thường vài giây.

Các lệnh trong mục A đã được chạy thật trên MariaDB 10.6.3 → MySQL 8.4.

Tài liệu này chỉ ghi phần riêng của MariaDB. Phần dùng chung với MySQL 5.x (build, systemd, theo dõi) nằm trong [MYSQL-5X-MIGRATION.md](MYSQL-5X-MIGRATION.md).

## Mục lục

- [0. Build](#0-build)
- [A. Chạy thử từ đầu đến cuối](#a-chạy-thử-từ-đầu-đến-cuối)
- [1. Hỗ trợ gì, không hỗ trợ gì](#1-hỗ-trợ-gì-không-hỗ-trợ-gì)
- [2. Khác biệt MariaDB → MySQL cần biết](#2-khác-biệt-mariadb--mysql-cần-biết)
- [3. Lỗi thường gặp](#3-lỗi-thường-gặp)
- [4. Checklist ngày chuyển](#4-checklist-ngày-chuyển)
- [5. Lệnh SQL kiểm tra trước trên MariaDB](#5-lệnh-sql-kiểm-tra-trước-trên-mariadb)

---

## 0. Build

Cần Go 1.25 trở lên, trên máy Linux bất kỳ (không cần cài lên máy MariaDB):

```bash
git clone https://github.com/hieunyit/strand.git && cd strand
CGO_ENABLED=0 go build -trimpath -o strand ./cmd/strand
./strand version
sudo install -m 0755 strand /usr/local/bin/strand
```

Kết quả là một file chạy duy nhất. Có thể chép sang máy Linux khác để chạy. Muốn đóng gói `.deb`/`.rpm` hoặc chạy bằng systemd: xem mục 0.2 và 12 của [MYSQL-5X-MIGRATION.md](MYSQL-5X-MIGRATION.md).

Nên chạy Strand trên một máy kết nối được tới cả MariaDB (nguồn) lẫn MySQL (đích), ví dụ chính máy MySQL đích.

---

## A. Chạy thử từ đầu đến cuối

Ví dụ: MariaDB nguồn ở `10.0.0.11:3306`, MySQL 8.4 đích ở `10.0.0.21:3306`, cần chuyển database `app`.

### Bước 1. Bật binlog dạng ROW trên MariaDB

Kiểm tra:

```sql
SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@log_bin_compress;
-- cần: 1, ROW, FULL, 0
```

Nếu chưa đúng, thêm vào `my.cnf` của MariaDB (mục `[mysqld]`) rồi restart:

```ini
[mysqld]
server-id        = 1
log-bin          = mysql-bin
binlog_format    = ROW
binlog_row_image = FULL
log_bin_compress = OFF
binlog_expire_logs_seconds = 864000   # giữ binlog 10 ngày, đủ cho snapshot và các lần dừng
```

### Bước 2. Tạo user cho Strand trên MariaDB

```sql
CREATE USER 'strand'@'%' IDENTIFIED BY 'srcpw';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'strand'@'%';
GRANT SELECT, LOCK TABLES ON app.* TO 'strand'@'%';
```

`REPLICATION CLIENT` trên MariaDB 10.6 là tên cũ của `BINLOG MONITOR`. Strand cần quyền này để đọc vị trí binlog.

### Bước 3. Chuẩn bị MySQL 8.x đích

```sql
CREATE DATABASE strand;   -- nơi Strand lưu checkpoint (_strand_*)
CREATE DATABASE app;
CREATE USER 'strand'@'%' IDENTIFIED BY 'dstpw';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP ON app.*    TO 'strand'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP ON strand.* TO 'strand'@'%';
GRANT CONNECTION_ADMIN ON *.* TO 'strand'@'%';
```

Không cần tạo bảng: Strand tự tạo bảng trên đích theo cấu trúc của nguồn, giữ nguyên charset và collation.

### Bước 4. File cấu hình `mdb-app.yaml`

```yaml
version: 1
name: mdb-app
source:
  kind: mysql            # MariaDB cũng khai báo là mysql; Strand tự nhận ra MariaDB
  host: 10.0.0.11
  port: 3306
  user: strand
  password_env: SRC_PW
  server_id: 47001       # số duy nhất, khác server_id của MariaDB và mọi replica
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
  spool: { path: ./state }          # thư mục làm việc của Strand
observability:
  admin_socket: ./run/admin.sock    # cho lệnh strand top
  metrics: "127.0.0.1:9464"
```

### Bước 5. Kiểm tra trước

```bash
export SRC_PW=srcpw DST_PW=dstpw
strand validate -c mdb-app.yaml
```

Kết quả đạt:

```
warning: cannot verify per-session binlog settings (...)   # bỏ qua được, xem mục 3
3 table(s) will be created on target
Preflight validation passed for pipeline: mdb-app
```

Nếu bị từ chối (exit 3), thông báo ghi rõ bảng và cách sửa. Xem mục 3.

```bash
strand plan -c mdb-app.yaml     # số dòng, dung lượng, thời gian snapshot ước tính
```

### Bước 6. Chạy đồng bộ

```bash
strand sync -c mdb-app.yaml
```

Log cần thấy:

```
"msg":"source has no GTID (gtid_mode OFF or absent): running in BINLOG POSITION MODE; ..."
"msg":"source MySQL version","version":"10.6.3-MariaDB-..."
"msg":"logical bootstrap in binlog position mode: streaming from the current head"
"msg":"snapshot done","table":"app.t" ...
"msg":"bootstrap finished: pipeline synced"
```

Cảnh báo `BINLOG POSITION MODE` là bình thường với MariaDB. Strand không dùng GTID của MariaDB.

Từ đây mọi INSERT/UPDATE/DELETE trên MariaDB được chép sang MySQL sau khoảng dưới một giây. Lệnh chạy liên tục. `Ctrl-C` dừng an toàn, chạy lại cùng lệnh thì tiếp tục từ chỗ đã dừng. Strand bị kill đột ngột hay mất điện cũng không mất hoặc lặp dữ liệu. Muốn chạy nền lâu dài, dùng systemd (mục 12 của tài liệu 5.x).

### Bước 7. Theo dõi

```bash
strand top -c mdb-app.yaml
# Trạng thái: synced   Lag: 0.000 s ...
strand checkpoint show -c mdb-app.yaml
```

### Bước 8. Kiểm tra dữ liệu khớp

```bash
strand verify -c mdb-app.yaml --mode full --table app.t
# verify app.t (full): 3 chunks (...), 0 mismatched chunks, ..., 0 mismatches, 0 unverifiable_hot
```

Mỗi bảng chạy một lần. Nếu lệch, lệnh thoát khác 0 và in khóa của dòng lệch.

### Bước 9. Khi nguồn đổi cấu trúc bảng (ALTER TABLE)

Strand **không tự áp DDL**. Khi có `ALTER TABLE` trên MariaDB, `strand sync` dừng (exit 10) và in câu lệnh:

```
Error [schema_change_pending]: DDL at mysql-bin.000003:8575 changes pipeline table(s) and the target schema does not match yet: ...
DDL: "ALTER TABLE app.t ADD COLUMN extra INT NULL DEFAULT 7"
```

Cách xử lý:

1. Chạy câu ALTER đó trên MySQL đích. Nếu câu dùng cú pháp chỉ MariaDB mới có, sửa lại cho MySQL.
2. Chạy lại `strand sync -c mdb-app.yaml`.

Nguồn vẫn ghi bình thường trong lúc Strand dừng. Khi chạy lại, Strand đọc tiếp binlog từ chỗ đã dừng.

`CREATE TABLE` bảng mới trong lúc đang chạy: tạo bảng đó trên đích bằng tay trước, rồi chạy lại `sync`.

### Bước 10. Cutover (promote)

Chọn giờ ít tải. Ứng dụng kết nối MariaDB bằng user `app_user`:

```bash
strand promote -c mdb-app.yaml --pipeline mdb-app \
  --target "root:***@tcp(10.0.0.21:3306)/strand" \
  --fence  "root:***@tcp(10.0.0.11:3306)/" \
  --source-app-roles app_user --fence-timeout 30s --catchup-timeout 5m
```

Lệnh này làm theo thứ tự:

1. Đặt MariaDB `read_only=1` và giữ `FLUSH TABLES WITH READ LOCK`.
2. Ngắt các phiên của `app_user`.
3. Chờ đích nhận hết thay đổi cuối cùng.
4. Mang `AUTO_INCREMENT` sang đích.
5. Mở quyền ghi trên đích.

Kết quả:

```
Promote pipeline mdb-app thành công. Target đã mở quyền ghi.
```

Sau đó:

1. Trỏ ứng dụng sang MySQL 8.x.
2. Dừng `strand sync`.
3. Giữ MariaDB ở `read_only` hoặc tắt hẳn.

MariaDB 10.6 không có `super_read_only`, nên user có quyền SUPER vẫn ghi được sau khi `promote` thoát. Đừng để ứng dụng dùng user SUPER.

---

## 1. Hỗ trợ gì, không hỗ trợ gì

| Hạng mục | Trạng thái |
| --- | --- |
| Nguồn MariaDB 10.6.x | Hỗ trợ (đã chạy thật trên 10.6.3 và 10.6 mới nhất) |
| MariaDB 10.5 trở xuống, 10.7 trở lên, 11.x | Bị từ chối: `only MariaDB 10.6 is supported` |
| Đích MySQL 8.4 | Hỗ trợ (có e2e). MySQL 8.0: code cho phép, chưa có e2e |
| Đích PostgreSQL từ nguồn MariaDB | Bị từ chối (chưa kiểm) |
| Snapshot ban đầu + stream thay đổi | Có |
| Dừng/chạy lại, kill đột ngột | Có, không mất và không lặp dữ liệu |
| `verify` (so sánh dữ liệu) | Có |
| `promote` (cutover) | Có |
| ALTER TABLE | Bán tự động: Strand dừng và in câu DDL, bạn áp lên đích rồi chạy lại |
| `binlog_row_metadata` | NO_LOG (mặc định) và FULL đã kiểm |
| Bảng InnoDB | Có |
| Bảng MyISAM / Aria / MEMORY | Bị từ chối. Đổi sang InnoDB trên nguồn (`ALTER TABLE ... ENGINE=InnoDB`) hoặc loại ra |
| Bảng không có PRIMARY KEY | Bị từ chối. Thêm PRIMARY KEY hoặc loại ra |
| Kiểu `INET6` | Bị từ chối (MySQL không có kiểu này) |
| Cột `COMPRESSED` | Bị từ chối |
| Bảng `WITH SYSTEM VERSIONING` | Bị từ chối |
| `SEQUENCE` | Bỏ qua, có cảnh báo. Tự tạo lại cơ chế tương đương trên MySQL |
| Collation riêng của MariaDB (vd `utf8mb4_general_nopad_ci`) | Bị từ chối, liệt kê cột và bảng |
| `log_bin_compress=ON` | Bị từ chối |
| View, trigger, procedure, function, event, user | **Không chép**. Tạo lại trên MySQL bằng tay (nhiều cú pháp khác MariaDB) |
| Seed bằng mysqldump hoặc mariabackup | Chưa kiểm với MariaDB. Dùng snapshot logic (mặc định, như mục A) |
| Đọc từ replica MariaDB, đổi máy nguồn giữa chừng | Không hỗ trợ (chế độ vị trí binlog) |

## 2. Khác biệt MariaDB → MySQL cần biết

- **Collation mặc định:** `utf8mb4` của MariaDB 10.6 mặc định là `utf8mb4_general_ci`, còn MySQL 8 là `utf8mb4_0900_ai_ci`. Strand **giữ nguyên `utf8mb4_general_ci`** trên bảng đích để so sánh chuỗi và khóa duy nhất cho kết quả giống hệt nguồn. Muốn đổi sang `0900`, làm sau cutover và kiểm trùng khóa trước.
- **JSON:** trên MariaDB, `JSON` thực chất là `LONGTEXT` kèm `CHECK (json_valid(...))`. Bảng đích cũng là `LONGTEXT` có cùng CHECK, nội dung giữ nguyên từng byte. Muốn dùng kiểu `JSON` thật của MySQL, `ALTER` sau cutover.
- **DEFAULT dạng hàm** (`DEFAULT uuid()`, `rand()`, `curdate()`, `concat(...)`) và **DEFAULT trên TEXT/BLOB/JSON** (`TEXT DEFAULT 'x'`): MySQL không nhận cú pháp này của MariaDB. Khi tạo bảng đích, Strand tự đổi sang dạng biểu thức `DEFAULT (uuid())`, `DEFAULT ('x')`, ý nghĩa không đổi.
- **Cột sinh tự động** (`AS (...) VIRTUAL`, `PERSISTENT`/`STORED`) và **cột `INVISIBLE`**: được giữ nguyên (đã kiểm).
- **`latin1` và các charset khác:** được chuyển đúng ký tự (đã kiểm `latin1` với `VARCHAR` và `TEXT`).
- **GTID MariaDB** (`0-1-123`) không dùng được trên MySQL. Strand định danh giao dịch bằng vị trí binlog `(file, offset)`.
- **MariaDB 10.6.3** là bản ổn định đầu tiên của dòng 10.6 (2021). Nên nâng lên bản 10.6 mới nhất trước khi chuyển. Đây chỉ là nâng bản vá, ít rủi ro, và Strand chạy được với cả hai.
- **performance_schema** trên MariaDB tắt mặc định. Vì vậy `validate` và `sync` in vài cảnh báo `cannot verify per-session binlog settings` hoặc `cannot list stale instance connections`. Không ảnh hưởng đồng bộ. Hãy tự bảo đảm không ứng dụng nào đặt `binlog_format` khác ROW ở mức session.

## 3. Lỗi thường gặp

| Thông báo | Sửa |
| --- | --- |
| `only MariaDB 10.6 is supported` | Nâng hoặc hạ nguồn về MariaDB 10.6 trước |
| `binary logging is disabled on the source` | Bật `log-bin` và `server-id` trong `my.cnf`, restart |
| `binlog_format must be ROW` | `SET GLOBAL binlog_format=ROW`, ghi vào `my.cnf`, cho ứng dụng kết nối lại |
| `log_bin_compress is ON` | `SET GLOBAL log_bin_compress=OFF`, ghi vào `my.cnf` |
| `... has MariaDB-only type inet6` | Đổi cột sang `VARCHAR(39)` hoặc `BINARY(16)` trên nguồn, hoặc loại bảng ra |
| `... is a MariaDB COMPRESSED column` | Bỏ thuộc tính `COMPRESSED` trên nguồn, hoặc loại bảng ra |
| `... system-versioned table` | `ALTER TABLE ... DROP SYSTEM VERSIONING` trên nguồn, hoặc loại bảng ra |
| `source collation(s) not available on the target: ...` | Đổi cột hoặc bảng sang collation MySQL có (vd `utf8mb4_general_ci`, `utf8mb4_bin`) |
| `uses nontransactional/unsupported engine MyISAM` (hoặc Aria) | `ALTER TABLE ... ENGINE=InnoDB` trên nguồn |
| `has no primary key` | Thêm PRIMARY KEY |
| `Unknown database 'strand'` | Tạo database `strand` trên đích (mục A bước 3) |
| `schema_change_pending` (exit 10) | Xem mục A bước 9 |
| `compressed MariaDB binlog event` khi đang chạy | Ai đó đã bật `log_bin_compress`. Tắt đi rồi tạo lại pipeline từ đầu |

Loại bảng ra bằng `tables.exclude: ["app.bang_x"]` trong file cấu hình.

## 4. Checklist ngày chuyển

1. `strand validate` đạt. Mọi bảng cần chuyển đều có trong danh sách sẽ tạo.
2. `strand top` báo `synced` và lag xấp xỉ 0.
3. `strand verify --mode full` cho từng bảng: 0 mismatches.
4. Đã tạo lại trên MySQL: user và quyền của ứng dụng, view, trigger, procedure, event (Strand không chép các thứ này).
5. Ứng dụng đã thử với MySQL 8 trên bản sao: cú pháp SQL riêng của MariaDB, collation, chế độ `sql_mode`.
6. Chạy `strand promote` (mục A bước 10). Trỏ ứng dụng sang MySQL. Dừng `strand sync`. Giữ MariaDB ở `read_only`.

## 5. Lệnh SQL kiểm tra trước trên MariaDB

Chạy bằng `root`, thay `app` bằng database cần chuyển. `strand validate` tự kiểm các mục 1–7 và 10. Chạy trước các lệnh này để biết sẵn cần sửa gì. Mục 8 Strand không kiểm, bạn phải tự xử lý.

```sql
-- 1. Phiên bản: phải là 10.6.x
SELECT VERSION();

-- 2. Binlog. Cần: 1, khác 0, ROW, FULL, 0, đủ lớn (0 = không tự xóa)
SELECT @@log_bin, @@server_id, @@binlog_format, @@binlog_row_image, @@log_bin_compress, @@binlog_expire_logs_seconds;
SHOW MASTER STATUS;   -- phải có 1 dòng
SHOW BINARY LOGS;

-- 3. Bảng không phải InnoDB: bị từ chối
SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES
WHERE TABLE_SCHEMA='app' AND TABLE_TYPE='BASE TABLE' AND ENGINE <> 'InnoDB';

-- 4. Bảng không có PRIMARY KEY: bị từ chối
SELECT t.TABLE_NAME FROM information_schema.TABLES t
LEFT JOIN information_schema.STATISTICS s
  ON s.TABLE_SCHEMA=t.TABLE_SCHEMA AND s.TABLE_NAME=t.TABLE_NAME AND s.INDEX_NAME='PRIMARY'
WHERE t.TABLE_SCHEMA='app' AND t.TABLE_TYPE='BASE TABLE' AND s.INDEX_NAME IS NULL;

-- 5. SEQUENCE (bỏ qua) và bảng system-versioned (bị từ chối)
SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES
WHERE TABLE_SCHEMA='app' AND TABLE_TYPE IN ('SEQUENCE','SYSTEM VERSIONED');

-- 6. Kiểu riêng MariaDB và cột COMPRESSED: bị từ chối
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='app' AND (DATA_TYPE IN ('inet6','inet4','uuid') OR COLUMN_TYPE LIKE '%compressed%');

-- 7. Collation đang dùng: đối chiếu với SHOW COLLATION trên MySQL đích (đáng ngờ: tên có "nopad")
SELECT DISTINCT COLLATION_NAME FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='app' AND COLLATION_NAME IS NOT NULL;

-- 8. Strand KHÔNG chép: tạo lại bằng tay trên MySQL
SELECT TABLE_NAME FROM information_schema.VIEWS WHERE TABLE_SCHEMA='app';
SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='app';
SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='app';
SELECT EVENT_NAME FROM information_schema.EVENTS WHERE EVENT_SCHEMA='app';
SELECT User, Host FROM mysql.user;

-- 9. Dung lượng (để ước thời gian snapshot)
SELECT TABLE_NAME, TABLE_ROWS, ROUND((DATA_LENGTH+INDEX_LENGTH)/1024/1024) AS MB
FROM information_schema.TABLES WHERE TABLE_SCHEMA='app' ORDER BY MB DESC;

-- 10. Quyền của user strand. Cần: REPLICATION SLAVE, BINLOG MONITOR (= REPLICATION CLIENT) ON *.*; SELECT, LOCK TABLES ON app.*
SHOW GRANTS FOR 'strand'@'%';
```
