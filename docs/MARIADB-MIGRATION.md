# Chuyển MariaDB 10.3–11.4 lên MySQL 8.x bằng Strand

Strand chép toàn bộ dữ liệu từ MariaDB (10.3 đến 11.4, xem mục 1) sang MySQL 8.x, sau đó đồng bộ liên tục các thay đổi (đọc binlog) cho tới khi bạn cutover (`promote`) ứng dụng sang MySQL. Ứng dụng vẫn ghi vào MariaDB trong suốt quá trình. Thời gian dừng ghi chỉ là lúc promote, thường vài giây.

Các lệnh trong mục A đã được chạy thật trên MariaDB 10.6.3 → MySQL 8.4. Các bản MariaDB khác: xem bảng phiên bản ở mục 1.

Tài liệu này chỉ ghi phần riêng của MariaDB. Phần dùng chung với MySQL 5.x (build, systemd, theo dõi) nằm trong [MYSQL-5X-MIGRATION.md](MYSQL-5X-MIGRATION.md).

## Mục lục

- [0. Build](#0-build)
- [A. Chạy thử từ đầu đến cuối](#a-chạy-thử-từ-đầu-đến-cuối)
- [B. Chuyển view/trigger/procedure/event/user](#b-chuyển-viewtriggerprocedureeventuser)
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
# MariaDB < 10.6.1 (10.3, 10.4, 10.5) không có biến trên, dùng: expire_logs_days = 10
```

### Bước 2. Tạo user cho Strand trên MariaDB

Một pipeline chuyển được **một, nhiều, hoặc tất cả database** cùng lúc (đọc chung một luồng binlog). Chọn một trong hai cách cấp quyền đọc:

```sql
CREATE USER 'strand'@'%' IDENTIFIED BY 'srcpw';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'strand'@'%';

-- Cách 1: chỉ các database cần chuyển (lặp lại cho từng database)
GRANT SELECT, LOCK TABLES ON app.*  TO 'strand'@'%';
GRANT SELECT, LOCK TABLES ON shop.* TO 'strand'@'%';
GRANT SELECT, LOCK TABLES ON crm.*  TO 'strand'@'%';

-- Cách 2: toàn bộ server
GRANT SELECT, LOCK TABLES ON *.* TO 'strand'@'%';
```

Tùy chọn, để log chẩn đoán khi snapshot đứng liệt kê được giao dịch đang mở lâu trên nguồn: `GRANT PROCESS ON *.* TO 'strand'@'%';`.

MariaDB 10.5 trở lên: cấp thêm `GRANT SLAVE MONITOR ON *.* TO 'strand'@'%';` để Strand đọc được replica lag (thiếu thì `validate` cảnh báo và breaker không dùng được tín hiệu này).

`REPLICATION CLIENT` là quyền Strand cần để đọc vị trí binlog. Từ MariaDB 10.5.2 nó là tên cũ của `BINLOG MONITOR` (cả hai cách ghi đều được); 10.3 và 10.4 chỉ có `REPLICATION CLIENT`.

Liệt kê database đang có để chọn:

```sql
SELECT SCHEMA_NAME FROM information_schema.SCHEMATA
WHERE SCHEMA_NAME NOT IN ('mysql','sys','information_schema','performance_schema');
```

### Bước 3. Chuẩn bị MySQL 8.x đích

Strand tự tạo database và bảng trên đích, giữ nguyên tên, charset và collation như nguồn. Bạn chỉ cần tạo database `strand` (nơi lưu checkpoint) và user:

```sql
CREATE DATABASE strand;   -- nơi Strand lưu checkpoint (_strand_*)
CREATE USER 'strand'@'%' IDENTIFIED BY 'dstpw';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON strand.* TO 'strand'@'%';
GRANT CONNECTION_ADMIN ON *.* TO 'strand'@'%';

-- Cách 1: từng database cần chuyển (cùng danh sách với bước 2)
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON app.*  TO 'strand'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON shop.* TO 'strand'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON crm.*  TO 'strand'@'%';

-- Cách 2: toàn bộ server (đích mới, chưa có dữ liệu khác)
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON *.* TO 'strand'@'%';
```

`REFERENCES` cần cho bảng có FOREIGN KEY (thiếu thì `apply schema DDL: ... REFERENCES command denied`).

Cần `strand objects` (view, trigger, procedure, event) thì cấp thêm trên các database đó: `CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, TRIGGER, EVENT` (xem mục B).

### Bước 4. File cấu hình `mdb-app.yaml`

Sinh file mẫu có chú thích rồi sửa host:

```bash
# một hoặc nhiều database (phân cách bằng dấu phẩy)
strand config init --from mariadb --to mysql --name mdb-app \
  --source-host 10.0.0.11 --target-host 10.0.0.21 --database app,shop,crm -o mdb-app.yaml

# toàn bộ database người dùng của server
strand config init --from mariadb --to mysql --name mdb-all \
  --source-host 10.0.0.11 --target-host 10.0.0.21 --database '*' -o mdb-all.yaml

# chạy bằng systemd thì thêm --systemd (đường dẫn /var/lib/strand/<name>, /run/strand/<name>/admin.sock)
```

Trong file, phần chọn bảng là `tables.include`:

```yaml
tables:
  include: ["app.*", "shop.*", "crm.*"]   # hoặc ["*.*"] = mọi database người dùng
  exclude: ["shop.log_*", "crm.tmp_import"]   # bỏ bớt bảng (tùy chọn)
```

`mysql`, `sys`, `information_schema`, `performance_schema` luôn bị bỏ qua. User và quyền của ứng dụng (nằm trong `mysql`) chuyển bằng `strand objects` (mục B).

Nội dung tối thiểu tương đương:

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

Chạy nền, không giữ terminal:

```bash
strand sync -c mdb-app.yaml --background      # hoặc -d; in PID, đường dẫn log
strand status -c mdb-app.yaml                 # còn chạy không, vài dòng log cuối
strand top -c mdb-app.yaml                    # lag, tiến độ
strand stop -c mdb-app.yaml                   # dừng an toàn (SIGTERM, ghi checkpoint)
```

Log mặc định `<resources.spool.path>/strand.log`, PID `<resources.spool.path>/strand.pid` (đổi bằng `--log-file`, `--pid-file`). Lỗi ngay lúc khởi động (sai mật khẩu, thiếu quyền...) thì lệnh báo luôn kèm log. Chạy nền kiểu này không tự chạy lại khi máy reboot hay lỗi tạm thời: chạy lâu dài nên dùng systemd (mục 12 của tài liệu 5.x).

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

## B. Chuyển view/trigger/procedure/event/user

`sync` chỉ chép bảng. Phần còn lại do `strand objects` (nguồn MariaDB/MySQL → đích MySQL; cặp khác bị từ chối exit 3 `objects_unsupported`). Dùng cùng file `-c` của pipeline, tôn trọng `tables.include/exclude/rename`.

**Quy tắc thời điểm (quan trọng):**

| Đối tượng | Khi nào tạo trên đích | Lý do |
| --- | --- | --- |
| View, procedure, function | Sau khi bảng đã có (sau `sync` seed xong), `--phase pre` | Không chủ động ghi dữ liệu |
| Event | `--phase pre` tạo ở trạng thái `DISABLE`; chỉ bật ở `--phase post` | Event chạy trên đích trước cutover sẽ ghi trùng |
| **Trigger** | **Chỉ ở cutover** (`--phase post`) | Binlog ROW đã chứa dòng do trigger sinh ra ở nguồn. Trigger trên đích trong lúc đồng bộ sẽ áp lần hai |
| User và quyền | Bất kỳ lúc nào, `--phase users` | Không liên quan dữ liệu |

```bash
# 1. Xem có gì (cảnh báo cú pháp MariaDB in ra stderr)
strand objects list -c mdb-app.yaml

# 2. Xuất file để duyệt/chạy bằng mysql client (01-views-routines.sql, 02-events-disabled.sql,
#    03-triggers.sql, 04-events-enable.sql, users.sql, objects.json)
strand objects export -c mdb-app.yaml --out ./objects

# 3. Sau khi sync đã seed bảng xong: view, procedure, function, event (DISABLE). Thử trước bằng --dry-run
strand objects apply -c mdb-app.yaml --phase pre --dry-run
strand objects apply -c mdb-app.yaml --phase pre

# 4. User và quyền (xem lưu ý mật khẩu bên dưới)
strand objects apply -c mdb-app.yaml --phase users --from ./objects

# 5. Ngày cutover: trigger và bật event chạy tự động ngay trước khi mở ghi
strand promote -c mdb-app.yaml --pipeline mdb-app --objects-post \
  --target "mysql://strand_admin:***@10.0.0.21:3306/strand" \
  --fence  "mysql://strand_admin:***@10.0.0.11:3306/" --catchup-timeout 5m
```

- `apply` idempotent: đối tượng đã có và giống hệt thì `identical` (bỏ qua); có nhưng khác thì `failed` kèm lý do, không ghi đè. Mỗi đối tượng in một dòng kết quả; còn lỗi thì exit 1 sau khi đã thử hết.
- `--objects-post` chạy `--phase post` sau bước bắt kịp và **trước** khi mở ghi, đồng thời đặt `event_scheduler=OFF` trên nguồn lúc fence. Cần DSN `--fence` có quyền SUPER/`SET GLOBAL` và đọc được định nghĩa ở nguồn; nguồn đã chết thì thêm `--objects-from ./objects` (dùng thư mục đã export).
- **Nếu bước này lỗi:** promote dừng, đích vẫn `read_only`, chưa mở ghi. Sửa nguyên nhân (vd trigger lạ đã tồn tại với nội dung khác: `DROP TRIGGER`) rồi chạy lại đúng lệnh cũ; promote tiếp từ bước mở ghi.
- **DEFINER:** mặc định bỏ, đối tượng thuộc tài khoản chạy `apply` (`CURRENT_USER`). `--keep-definer` giữ nguyên, user đó phải có sẵn trên đích.
- **Đổi tên schema** (`tables.rename`): tên có qualified trong view/trigger/routine/event được viết lại theo cấu hình. View được tạo theo thứ tự phụ thuộc (view trên view).
- **Cú pháp riêng của MariaDB** được phát hiện và in cảnh báo kèm tên đối tượng: `BEGIN NOT ATOMIC`, `PACKAGE`, `NEXTVAL`/sequence, `RETURNING`, `CREATE OR REPLACE` ngoài view, routine `sql_mode=ORACLE`. Đối tượng đó cần sửa tay; `apply` báo lỗi nếu MySQL từ chối.
- **Trigger trên bảng bị loại** (`tables.exclude`) bị bỏ qua, có ghi chú.

### User và quyền

`users.sql` gồm `CREATE USER` + `GRANT` cho user có quyền trên schema được chọn. Bỏ qua `root`, `mariadb.sys`, user của Strand và user chỉ có quyền replication.

- Hash `mysql_native_password` được giữ (`IDENTIFIED WITH mysql_native_password AS '<hash>'`) nên ứng dụng đăng nhập bằng mật khẩu cũ. **MySQL 8.4 tắt plugin này mặc định:** thêm `mysql_native_password=ON` vào `my.cnf` của đích rồi khởi động lại, hoặc dùng `--auth reset` (mật khẩu ngẫu nhiên mới, `caching_sha2_password`, in ra để đặt lại cho ứng dụng).
- Quyền riêng của MariaDB được đổi sang tương đương MySQL hoặc bị chú thích (`-- ...`) kèm ghi chú: `BINLOG MONITOR`, `SLAVE MONITOR`, `READ_ONLY ADMIN`, `DELETE HISTORY`... Đọc lại `users.sql` trước khi áp.
- Role đơn giản được chuyển; role phức tạp cần xem tay.

---

## 1. Hỗ trợ gì, không hỗ trợ gì

| Hạng mục | Trạng thái |
| --- | --- |
| Nguồn MariaDB 10.3, 10.4, 10.5, 10.6, 10.11, 11.4 | Hỗ trợ, đã chạy e2e thật trên từng bản (bảng bên dưới) |
| MariaDB 10.7–10.10, 11.0–11.3, 11.5 trở lên | Nhận nhưng chưa kiểm e2e |
| MariaDB 10.2 trở xuống | Bị từ chối: `MariaDB 10.3 or later is required` |
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
| Kiểu `INET6` (10.5+), `INET4` (10.10+), `UUID` (10.7+) | Hỗ trợ khi nằm ngoài khóa chính: đích là `VARCHAR(39)` / `VARCHAR(15)` / `CHAR(36)` (ascii_bin), chữ giống hệt `SELECT` của MariaDB |
| `INET6`/`INET4`/`UUID` nằm trong PRIMARY KEY | Bị từ chối (thứ tự khóa khác thứ tự chữ nên không chứng minh được khoảng khóa) |
| Cột `COMPRESSED` | Hỗ trợ: đích là cột thường (không nén), giá trị gốc giữ nguyên |
| Bảng `WITH SYSTEM VERSIONING` (kể cả `PARTITION BY SYSTEM_TIME`) | Hỗ trợ: đích là bảng thường chỉ có hàng **hiện hành**, không có `row_start`/`row_end`. **Lịch sử không được chuyển** (có cảnh báo) |
| System-versioned khai báo cột `ROW START/ROW END` tường minh, `PERIOD FOR` / `WITHOUT OVERLAPS` (application-time, 10.4+/10.5+) | Bị từ chối |
| `SEQUENCE` | Bỏ qua, có cảnh báo. Tự tạo lại cơ chế tương đương trên MySQL |
| Collation riêng của MariaDB (`nopad`, `uca1400`) | Ánh xạ sang collation MySQL, xem mục 2.1. Cột khóa mà chỉ có ánh xạ gần đúng thì bị từ chối |
| `log_bin_compress=ON` | Bị từ chối |
| View, procedure, function, trigger, event, user và quyền | `sync` **không** chép. Dùng `strand objects` (mục B): tự xuất, chuyển DEFINER/tên schema, cảnh báo cú pháp riêng của MariaDB |
| Seed bằng mysqldump hoặc mariabackup | Chưa kiểm với MariaDB. Dùng snapshot logic (mặc định, như mục A) |
| Đọc từ replica MariaDB, đổi máy nguồn giữa chừng | Không hỗ trợ (chế độ vị trí binlog) |

### Bảng phiên bản MariaDB

| Bản | e2e | Khác biệt đã xử lý |
| --- | --- | --- |
| 10.3, 10.4, 10.5 | Có | Retention binlog dùng `expire_logs_days` (không có `binlog_expire_logs_seconds`); charset tên `utf8` (đích là `utf8mb3`) |
| 10.6 | Có | Chuẩn của tài liệu này |
| 10.11 | Có | Như 10.6 |
| 11.4 | Có | Mặc định `utf8mb4` là `utf8mb4_uca1400_ai_ci` (đọc từ nguồn, ánh xạ ở mục 2.1); image không có client `mysql` (Strand không cần) |

## 2. Khác biệt MariaDB → MySQL cần biết

- **Collation mặc định:** `utf8mb4` của MariaDB đến 10.11 mặc định là `utf8mb4_general_ci`, còn MySQL 8 là `utf8mb4_0900_ai_ci`. Strand đọc collation mặc định thật từ nguồn (11.x có thể là `uca1400_ai_ci`, hoặc đặt bằng `character_set_collations`) và ghi tường minh vào bảng đích. Với `general_ci` thì giữ nguyên để so sánh chuỗi và khóa duy nhất giống hệt nguồn. Muốn đổi sang `0900`, làm sau cutover và kiểm trùng khóa trước.
- **Tên charset:** `utf8` ở MariaDB ≤ 10.5 là `utf8mb3` ở 10.6+ và MySQL 8; Strand xử lý cả hai.
- **JSON:** trên MariaDB, `JSON` thực chất là `LONGTEXT` kèm `CHECK (json_valid(...))`. Bảng đích cũng là `LONGTEXT` có cùng CHECK, nội dung giữ nguyên từng byte. Muốn dùng kiểu `JSON` thật của MySQL, `ALTER` sau cutover.
- **DEFAULT dạng hàm** (`DEFAULT uuid()`, `rand()`, `curdate()`, `concat(...)`) và **DEFAULT trên TEXT/BLOB/JSON** (`TEXT DEFAULT 'x'`): MySQL không nhận cú pháp này của MariaDB. Khi tạo bảng đích, Strand tự đổi sang dạng biểu thức `DEFAULT (uuid())`, `DEFAULT ('x')`, ý nghĩa không đổi.
- **Cột sinh tự động** (`AS (...) VIRTUAL`, `PERSISTENT`/`STORED`) và **cột `INVISIBLE`**: được giữ nguyên (đã kiểm).
- **`latin1` và các charset khác:** được chuyển đúng ký tự (đã kiểm `latin1` với `VARCHAR` và `TEXT`).
- **INET6/INET4/UUID:** MySQL không có các kiểu này. Binlog mang byte thô, Strand đổi sang đúng chữ mà `SELECT` trên MariaDB trả (vd `::ffff:1.2.3.4`, `1:0:0:2::3`, UUID chữ thường). Đích là cột chữ, nên `ORDER BY` trên đích theo chữ, không theo thứ tự byte như `INET6`/`UUID` của MariaDB.
- **Cột `COMPRESSED`:** Strand giải nén giá trị từ binlog (cả nén thô lẫn `column_compression_zlib_wrap=ON`, giá trị ngắn không nén, rỗng, NULL). Cột đích không nén, tốn dung lượng hơn nguồn. Header nén lạ hoặc độ dài sau giải nén sai thì Strand dừng, không ghi sai.
- **Bảng system-versioned:** đích chỉ giữ hàng hiện hành. `UPDATE` ở nguồn thành `UPDATE`, `DELETE` thành `DELETE` ở đích; hàng lịch sử, `DELETE HISTORY`, purge theo partition không ảnh hưởng đích. Truy vấn `FOR SYSTEM_TIME` sau cutover sẽ không còn lịch sử; muốn giữ, hãy xuất lịch sử riêng trước khi chuyển. Mọi `ALTER` vẫn đi qua `schema_change_pending` như bảng thường.
- **GTID MariaDB** (`0-1-123`) không dùng được trên MySQL. Strand định danh giao dịch bằng vị trí binlog `(file, offset)`.
- **MariaDB 10.6.3** là bản ổn định đầu tiên của dòng 10.6 (2021). Nên nâng lên bản 10.6 mới nhất trước khi chuyển. Đây chỉ là nâng bản vá, ít rủi ro, và Strand chạy được với cả hai.
- **performance_schema** trên MariaDB tắt mặc định. Vì vậy `validate` và `sync` in vài cảnh báo `cannot verify per-session binlog settings` hoặc `cannot list stale instance connections`. Không ảnh hưởng đồng bộ. Hãy tự bảo đảm không ứng dụng nào đặt `binlog_format` khác ROW ở mức session.

### 2.1. Ánh xạ collation

Collation mà MySQL đích không có sẽ được ánh xạ khi tạo bảng, cột và database (đích dùng collation ở cột "MySQL"). `strand validate` in cảnh báo nêu bảng, cột và ánh xạ.

| MariaDB | MySQL | Loại | Dùng cho cột khóa (PK/UNIQUE) |
| --- | --- | --- | --- |
| `utf8mb4_nopad_bin` | `utf8mb4_0900_bin` | Tương đương hoàn toàn (cùng NO PAD, so theo code point; đã kiểm ORDER BY và so sánh kể cả dấu cách cuối trên cả hai server) | Được từ MariaDB 10.5. Ở 10.3/10.4, `nopad_bin` coi `'a'` và `'a\0'` là bằng nhau (lỗi của MariaDB, sửa ở 10.5) nên chỉ gần đúng: khóa bị từ chối |
| `*_nopad_ci` / `*_nopad_bin` khác (vd `utf8mb4_general_nopad_ci`) | bản PAD SPACE cùng tên (`utf8mb4_general_ci`) | Gần đúng: `'a'` và `'a '` bằng nhau trên đích, khác nhau trên nguồn | Từ chối |
| `utf8mb4_uca1400_ai_ci`, `_as_ci`, `_as_cs` | `utf8mb4_0900_ai_ci`, `_as_ci`, `_as_cs` | Gần đúng: Unicode 14 so với 9, NO PAD so với PAD SPACE | Từ chối |
| `utf8mb4_uca1400_ai_cs` | `utf8mb4_0900_as_cs` | Gần đúng, chặt hơn (MySQL không có ai_cs) | Từ chối |
| `utf8mb3_uca1400_*ci` / `*cs` | `utf8mb3_unicode_ci` / `utf8mb3_bin` | Gần đúng | Từ chối |

Với cột khóa, so sánh khác nhau có thể gộp hai khóa khác nhau ở nguồn thành một ở đích (mất dữ liệu), nên Strand từ chối và nêu tên cột. Cách sửa: đổi collation cột đó trên nguồn (vd `utf8mb4_general_ci`, `utf8mb4_bin`, `utf8mb4_nopad_bin`) hoặc loại bảng ra. Cột không phải khóa vẫn sắp xếp và so sánh khác nguồn một chút (hoa/thường, dấu, dấu cách cuối) trên truy vấn `WHERE`/`ORDER BY` của ứng dụng: xem cảnh báo và thử trước. Collation không có ánh xạ nào thì bị từ chối.

## 3. Lỗi thường gặp

| Thông báo | Sửa |
| --- | --- |
| `MariaDB 10.3 or later is required` | Nâng nguồn lên MariaDB 10.3 trở lên |
| `binary logging is disabled on the source` | Bật `log-bin` và `server-id` trong `my.cnf`, restart |
| `binlog_format must be ROW` | `SET GLOBAL binlog_format=ROW`, ghi vào `my.cnf`, cho ứng dụng kết nối lại |
| `log_bin_compress is ON` | `SET GLOBAL log_bin_compress=OFF`, ghi vào `my.cnf` |
| `primary key column ... has MariaDB type inet6/inet4/uuid ... key ranges cannot be proven equal` | Đổi khóa chính sang kiểu thường (vd khóa thay thế `BIGINT`), hoặc loại bảng ra |
| `declares its system-versioning period columns explicitly` | Dùng `WITH SYSTEM VERSIONING` thường (row_start/row_end ẩn), bỏ versioning, hoặc loại bảng ra |
| `has a PERIOD FOR definition` | Bỏ PERIOD (application-time) trên nguồn, hoặc loại bảng ra |
| `system-versioned table ...: only current rows are migrated` (cảnh báo) | Chủ ý: lịch sử không chuyển |
| `compressed-column header ...` / `inflated N bytes` khi đang chạy | Dữ liệu nén không đọc được; Strand dừng. Báo lỗi kèm bảng/cột trong thông báo |
| `column(s) in a PRIMARY KEY or UNIQUE index use a MariaDB collation without an exact MySQL equivalent` | Đổi collation cột khóa trên nguồn, xem mục 2.1 |
| `source collation(s) not available on the target and without a mapping` | Đổi cột hoặc bảng sang collation MySQL có (vd `utf8mb4_general_ci`, `utf8mb4_bin`) |
| `uses nontransactional/unsupported engine MyISAM` (hoặc Aria) | `ALTER TABLE ... ENGINE=InnoDB` trên nguồn |
| `has no primary key` | Thêm PRIMARY KEY |
| `Unknown database 'strand'` | Tạo database `strand` trên đích (mục A bước 3) |
| `schema_change_pending` (exit 10) | Xem mục A bước 9 |
| `compressed MariaDB binlog event` khi đang chạy | Ai đó đã bật `log_bin_compress`. Tắt đi rồi tạo lại pipeline từ đầu |

Loại bảng ra bằng `tables.exclude: ["app.bang_x"]` trong file cấu hình.

### Snapshot đứng yên hoặc rất chậm

- Log có dòng `snapshot made no progress` (mỗi 2 phút khi snapshot không ghi thêm row nào): nêu bảng, breaker và lý do, tốc độ governor, giao dịch mở lâu nhất trên nguồn (`oldest_source_txns`, cần quyền `PROCESS`) và phiên đang chờ khóa. Giao dịch ứng dụng mở hàng giờ chặn `LOCK TABLES … READ` của snapshot: commit/kill nó thì snapshot chạy tiếp.

- `strand top -c ...`: dòng `Breaker: MỞ ...` nêu lý do. Breaker bảo vệ nguồn: khi mở, snapshot tạm dừng; đóng lại sau 60 giây liên tục dưới ngưỡng. Nếu nguồn không thật sự quá tải, nới `governor.breaker.strand_query_p99.max_ratio` (mặc định 3, so với độ trễ đo lúc khởi động) hoặc `replica_lag.max`.
- Tăng tốc khi nguồn còn dư tải: `resources.source.iops` (mặc định 600 IOPS ≈ 9 MiB/s), `resources.workers.max`, `bootstrap.logical.chunk_rows` (chunk lớn hơn = ít lần `LOCK TABLES` hơn). Phía đích trong lúc chép: `innodb_flush_log_at_trx_commit=2`, `sync_binlog=0` (đặt lại `1` trước `promote`).
- Đổi cấu hình: `strand stop` / Ctrl-C rồi chạy lại `sync`, snapshot đọc tiếp từ chỗ dở.

## 4. Checklist ngày chuyển

1. `strand validate` đạt. Mọi bảng cần chuyển đều có trong danh sách sẽ tạo.
2. `strand top` báo `synced` và lag xấp xỉ 0.
3. `strand verify --mode full` cho từng bảng: 0 mismatches.
4. Đã chuyển view, procedure, function, event (`objects apply --phase pre`), user và quyền (`--phase users`); cutover dùng `promote --objects-post` để tạo trigger và bật event (mục B).
5. Ứng dụng đã thử với MySQL 8 trên bản sao: cú pháp SQL riêng của MariaDB, collation, chế độ `sql_mode`.
6. Chạy `strand promote --objects-post` (mục A bước 10, mục B). Trỏ ứng dụng sang MySQL. Dừng `strand sync`. Giữ MariaDB ở `read_only`.

## 5. Lệnh SQL kiểm tra trước trên MariaDB

Chạy bằng `root`, thay `app` bằng database cần chuyển. `strand validate` tự kiểm các mục 1–7 và 10. Chạy trước các lệnh này để biết sẵn cần sửa gì. Mục 8 Strand không kiểm, bạn phải tự xử lý.

```sql
-- 1. Phiên bản: 10.3 trở lên
SELECT VERSION();

-- 2. Binlog. Cần: 1, khác 0, ROW, FULL, 0, đủ lớn (0 = không tự xóa)
SELECT @@log_bin, @@server_id, @@binlog_format, @@binlog_row_image, @@log_bin_compress, @@binlog_expire_logs_seconds;  -- 10.3–10.5: dùng @@expire_logs_days
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

-- 5. SEQUENCE (bỏ qua) và bảng system-versioned (chuyển hàng hiện hành, bỏ lịch sử)
SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES
WHERE TABLE_SCHEMA='app' AND TABLE_TYPE IN ('SEQUENCE','SYSTEM VERSIONED');

-- 6. Kiểu riêng MariaDB và cột COMPRESSED (đổi sang chữ / giải nén; kiểu này nằm trong PRIMARY KEY thì bị từ chối)
SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='app' AND (DATA_TYPE IN ('inet6','inet4','uuid') OR COLUMN_TYPE LIKE '%compressed%');

-- 7. Collation đang dùng: tên có "nopad" hoặc "uca1400" sẽ được ánh xạ (mục 2.1)
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

-- 10. Quyền của user strand. Cần: REPLICATION SLAVE, REPLICATION CLIENT (= BINLOG MONITOR từ 10.5.2) ON *.*; SELECT, LOCK TABLES ON app.*
SHOW GRANTS FOR 'strand'@'%';
```
