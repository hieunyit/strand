# Strand CDC — Hướng dẫn sử dụng

Cập nhật: 2026-11-05. Tài liệu ứng với code sau khi gộp các nhánh `agent/schema-init-fixes`, `agent/review-deep` và `agent/generic-pk-v2` vào `review/gate1-fixes`. Mọi câu dưới đây đã được đối chiếu với code và `strand <lệnh> --help`. Khóa được nhận mà chưa có tác dụng được ghi rõ ở mục 6.3.

Strand đồng bộ **một chiều** dữ liệu từ một CSDL nguồn (source) sang một CSDL đích (target), bằng CDC: đọc binlog (MySQL) hoặc logical replication (PostgreSQL). Mỗi pipeline là một process `strand sync`. Trạng thái (checkpoint) nằm trong target, nên dừng hoặc kill -9 rồi chạy lại không mất và không lặp dữ liệu.

## Mục lục

1. [Tổng quan và ma trận hỗ trợ](#1-tổng-quan-và-ma-trận-hỗ-trợ)
2. [Chọn phạm vi đồng bộ](#2-chọn-phạm-vi-đồng-bộ)
3. [Chuẩn bị target](#3-chuẩn-bị-target)
4. [Yêu cầu theo engine](#4-yêu-cầu-theo-engine)
5. [Yêu cầu và giới hạn của bảng](#5-yêu-cầu-và-giới-hạn-của-bảng)
6. [Tham chiếu cấu hình](#6-tham-chiếu-cấu-hình)
7. [Tham chiếu lệnh](#7-tham-chiếu-lệnh)
8. [Vận hành, tuning, metric, exit code](#8-vận-hành-tuning-metric-exit-code)
9. [Công thức đầu-cuối](#9-công-thức-đầu-cuối)
10. [FAQ, xử lý sự cố, giới hạn đã biết](#10-faq-xử-lý-sự-cố-giới-hạn-đã-biết)

---

## 1. Tổng quan và ma trận hỗ trợ

### 1.1 Cặp engine

| Source → Target | Hỗ trợ |
| --- | --- |
| MySQL → MySQL | Có |
| MySQL → PostgreSQL | Có |
| PostgreSQL → PostgreSQL | Có |
| PostgreSQL → MySQL | **Không**. `sync` từ chối: `unsupported pipeline postgres → mysql` |

Phiên bản có test tự động: MySQL 8.4, PostgreSQL 16, 17 và 18. Source MySQL 5.5, 5.6 và 5.7 (có hoặc không GTID) cũng có e2e: xem `docs/MYSQL-5X-MIGRATION.md`. Source MariaDB 10.6 (chỉ sang MySQL 8.x) có e2e: xem `docs/MARIADB-MIGRATION.md`. MySQL 8.0 có trong spec nhưng chưa có test tự động. Source PostgreSQL cần ít nhất PG 14, vì watermark snapshot dùng `messages` của pgoutput.

### 1.2 Tính năng theo cặp

| Tính năng | MySQL→MySQL | MySQL→PG | PG→PG |
| --- | --- | --- | --- |
| Snapshot logic (`bootstrap.default: logical`, `snapshot add`) | Có | Có | Có (source phải là primary) |
| Snapshot logic với PK không phải một cột số nguyên | Có (mức 1 + 2, mục 5.2) | Chỉ kiểu số, thời gian, VARBINARY | Có (mức 1, mục 5.2) |
| Tự tạo schema đích (`create_schema: auto`, `schema init`) | Có | Có (theo typemap) | Có |
| Seed từ backup | xtrabackup, clone, mydumper, mysqldump | — | pgBackRest, WAL-G, `strand export` |
| `--start-at` | GTID set | — (dùng vị trí GTID của source) | LSN `X/Y` |
| `verify --mode sample` | Có | Không | Không |
| `verify --mode delta` / verify tự động sau seed | Có | Không (chỉ cảnh báo, xem 10.3) | Không (chỉ cảnh báo) |
| `verify --mode full` | Có | Có | Có |
| `verify --repair fix` | Có | Không (khác engine) | Có, trừ PK tổng quát |
| `checkpoint rewind` | Có | Không | Không |
| Governor `onoff` / `grace-v2` | Có | Có | Không, luôn `static` |
| `promote` | Có | Có | Có |

`--start-at` với MySQL→PG nhận GTID set của source MySQL, giống MySQL→MySQL.

### 1.3 Các khái niệm

- **Chế độ A (snapshot logic):** Strand tự đọc bảng nguồn theo chunk, song song với stream. Không cần backup. Có điều tốc để không đè source.
- **Chế độ B (seed):** bạn restore một bản backup vào target. Strand kiểm vị trí P của backup rồi bắt kịp từ P. Không đọc dữ liệu bảng trên source.
- **`--start-at`:** bạn cam kết target đã khớp source tại một vị trí. Strand chỉ stream từ đó.
- **`commit_order`:**
  - `strict` (mặc định): apply đúng thứ tự commit của source, một luồng ghi. Target luôn là một tiền tố của source.
  - `relaxed`: các Txn không đụng nhau chạy song song. Nhanh hơn, nhưng target có thể tạm thời có "lỗ".
- **Trạng thái pipeline** (`strand top`): `starting` → `bootstrapping` → `synced`. Ngoài ra còn `paused` và `failed`.
  - `bootstrapping`: đang snapshot hoặc đang verify sau seed.
  - `synced`: mọi bảng đã xong.

---

## 2. Chọn phạm vi đồng bộ

Strand chỉ đồng bộ những bảng khớp `tables.include` và không khớp `tables.exclude`. Mỗi mẫu là một **glob** trên tên `schema.table`, theo cú pháp `path.Match` của Go: `*` khớp mọi chuỗi, `?` khớp một ký tự, `[abc]` khớp một lớp ký tự.

| Muốn đồng bộ | `tables.include` |
| --- | --- |
| Một bảng | `["app.orders"]` |
| Vài bảng | `["app.orders", "app.users"]` |
| Cả một database (MySQL) hoặc schema (PG) | `["app.*"]` |
| Mọi bảng có tiền tố | `["app.order_*"]` |
| Cả instance | `["*.*"]` |

Bạn **không bắt buộc** phải liệt kê từng bảng: `app.*` là cả database `app`, `*.*` là mọi schema người dùng. Schema hệ thống luôn bị bỏ qua:

- MySQL: `mysql`, `sys`, `information_schema`, `performance_schema`.
- PostgreSQL: `pg_catalog`, `information_schema` và mọi schema `pg_*`.

Chỉ bảng thường (MySQL `BASE TABLE`; PG bảng thường và bảng partitioned gốc) được đồng bộ. View, bảng tạm và partition lá của PG thì không.

- `tables.exclude`: cùng cú pháp, ví dụ `["app.audit_*", "app.tmp_*"]`.
- `tables.rename`: đổi tên bảng ở target, ánh xạ **từng bảng**, ví dụ `{"app.orders": "sales.orders"}`. Không có rename theo cả schema.
- `strand validate` từ chối (exit 3) khi một mẫu include không khớp bảng nào trên source MySQL. `sync` không kiểm điều này.

**Bảng mới trong lúc chạy:**

- Source MySQL: `CREATE TABLE` hoặc `RENAME TABLE` tạo ra bảng khớp bộ lọc thì được tạo ở target và nhận row (mục 3.4).
- Source PG: DDL không đi qua pgoutput. Dùng `strand snapshot add --table s.t` hoặc khởi động lại pipeline.

**Đổi `include`/`exclude` giữa các lần chạy:** dừng pipeline, sửa file, chạy lại.

- Bảng mới khớp bộ lọc được snapshot logic (với PG source thì phải có trong publication).
- Bảng bị loại thì thôi đồng bộ. Dữ liệu đã có ở target vẫn giữ nguyên.

### Checklist đồng bộ cả instance

1. `tables.include: ["*.*"]`. Loại bảng không cần bằng `exclude`.
2. Mọi bảng phải có PRIMARY KEY (mục 5.1). Bảng không có PK làm `sync`/`validate` dừng với `missing_pk`.
3. Kiểm từng bảng có PK snapshot logic được (mục 5.2). Bảng không hỗ trợ: seed từ backup hoặc đổi kiểu PK.
4. User, quyền, view, routine, trigger và event **không** được sao chép. Tạo tay ở target. Seed bằng XtraBackup/CLONE thì backup mang theo chúng.
5. PG source: publication phải chứa mọi bảng (`FOR ALL TABLES` hoặc liệt kê). Bảng partitioned cần `publish_via_partition_root = true`.
6. Chạy `strand validate`, rồi `strand plan` (ETA, chọn A hay B), rồi `strand sync`.

---

## 3. Chuẩn bị target

### 3.1 Tự tạo schema (mặc định)

`bootstrap.create_schema: auto` là mặc định. `sync` tạo database/schema và các bảng còn thiếu ở target trước khi snapshot logic, theo phạm vi `tables.include`/`exclude`/`rename`. Bảng đã tồn tại không bị sửa cấu trúc.

```bash
strand schema init -c pipeline.yaml --dry-run   # chỉ in DDL
strand schema init -c pipeline.yaml             # tạo bảng còn thiếu (chạy lại an toàn)
strand validate   -c pipeline.yaml             # báo "N table(s) will be created on target"
```

- `schema init` vẫn tạo bảng khi `create_schema: off`. `off` chỉ tắt việc tự tạo của `sync`.
- Với `off`, bảng đích thiếu làm `validate` trả exit 3.
- Biến môi trường tương ứng: `STRAND_BOOTSTRAP_CREATE_SCHEMA=off`.

Theo cặp engine:

- **MySQL→MySQL:**
  - Dùng `SHOW CREATE TABLE`, giữ PK, index, FK, partition và charset/collation.
  - Bỏ bộ đếm `AUTO_INCREMENT=N`.
  - Không sao chép trigger, event, routine. Bỏ qua view.
  - Target phải là InnoDB.
- **MySQL→PG:**
  - Theo bảng ánh xạ ở 3.2. Cột khóa chuỗi dùng `COLLATE "C"`. ENUM thành `text` + CHECK. AUTO_INCREMENT thành identity khi kiểu đích là smallint, integer hoặc bigint.
  - Default chỉ được giữ khi dịch được an toàn. Default bị bỏ thì có cảnh báo.
  - Bị từ chối (exit 3, cần tạo bảng tay): index prefix, index biểu thức, fulltext, cột generated, AUTO_INCREMENT ánh xạ sang numeric.
- **PG→PG:**
  - Dựng từ catalog, không gọi `pg_dump`.
  - Giữ NOT NULL, default, collation, PK/UNIQUE/CHECK/exclusion, index, FK (tạo sau cùng), identity và sequence sở hữu, biểu thức generated.
  - Hỗ trợ partition một cấp.
  - Từ chối subpartition và enum/domain do người dùng định nghĩa.
  - Sequence độc lập không được sao chép.

Về giao dịch và các trường hợp đặc biệt:

- DDL PG của một lần init chạy trong một transaction. MySQL commit từng DDL; bị ngắt thì lần sau tạo nốt phần còn thiếu.
- `sync` chỉ tự tạo bảng **còn chờ snapshot**. Bảng đã được đánh dấu xong qua seed hoặc `--start-at` mà bị thiếu ở target thì không được tạo rỗng (để không che lỗi restore).
- Database metadata trong DSN target (ví dụ `strand`) phải có sẵn và kết nối được.
- Target PG với `auto`: bảng cha được snapshot trước bảng con. FK vòng hoặc tự tham chiếu trong tập bảng chờ snapshot bị từ chối. Xem `questions/OPEN-schema-init-fk.md`.

### 3.2 Ánh xạ kiểu MySQL → PostgreSQL

Mặc định cố định, chưa có khóa cấu hình để đổi chính sách (`internal/typemap`):

| MySQL | PostgreSQL | Ghi chú |
| --- | --- | --- |
| `TINYINT` (có/không dấu) | `smallint` | Kiểm phạm vi |
| `SMALLINT` / `SMALLINT UNSIGNED` | `smallint` / `integer` | |
| `MEDIUMINT` (có/không dấu) | `integer` | |
| `INT` / `INT UNSIGNED` | `integer` / `bigint` | |
| `BIGINT` / `BIGINT UNSIGNED` | `bigint` / `numeric(20,0)` | |
| `DECIMAL(p,s)` | `numeric(p,s)` | |
| `FLOAT` / `DOUBLE` | `real` / `double precision` | |
| `BIT(n)` | `bit varying(n)` | |
| `CHAR(n)` / `VARCHAR(n)` | `char(n)` / `varchar(n)` | Cột khóa: `COLLATE "C"` |
| `TEXT` (mọi cỡ) | `text` | |
| `BINARY`, `VARBINARY`, `BLOB` (mọi cỡ) | `bytea` | |
| `DATE` | `date` | Zero date bị từ chối |
| `DATETIME(n)` | `timestamp(n)` | Giữ nguyên giờ đồng hồ |
| `TIMESTAMP(n)` | `timestamptz(n)` | Đọc theo UTC |
| `TIME(n)` | `time(n)` | |
| `YEAR` | `smallint` | |
| `JSON` | `jsonb` | |
| `ENUM(...)` | `text` + CHECK | |
| `SET(...)` | `text[]` | |
| Kiểu hình học (`GEOMETRY`, `POINT`...) | Không hỗ trợ | `type_unmappable` (exit 3) |

`strand validate` với MySQL→PG kiểm ba điều:

1. Kiểu nào không ánh xạ được (`type_unmappable`).
2. Collation cột khóa ở target không được lỏng hơn source, và không được dùng `citext` (`collation_looser`).
3. Có giá trị thực tế gây lỗi không: zero date, giá trị ENUM ngoài danh sách. Kiểm bằng truy vấn có LIMIT.

### 3.3 Tạo schema bằng tay (khi `create_schema: off` hoặc bị từ chối)

- MySQL: `mysqldump --no-data --skip-triggers --databases app | mysql -h target`. Trigger ở target cũng ghi khi Strand apply, nên đừng tạo trigger trùng với logic của source.
- PG: `pg_dump --schema-only -d app | psql -h target`.
- MySQL→PG: tạo theo bảng 3.2, rồi `strand validate` kiểm lại.

### 3.4 DDL về sau

Strand **không tự áp DDL**.

- **Source MySQL:**
  - `ALTER`, `DROP` hoặc `RENAME` trên bảng của pipeline làm Strand dừng với exit 10 `schema_change_pending`. Áp cùng DDL ở target rồi chạy lại `sync`; Strand so cột và đi tiếp.
  - `CREATE TABLE` khớp bộ lọc thì được tạo ở target tại barrier, trước row đầu tiên. Strand giữ metadata lock đọc và kiểm binlog để không lấy schema đã bị ALTER sau đó. Không kiểm được trong 30 giây thì dừng exit 10.
  - Bảng `RENAME` từ ngoài bộ lọc vào cũng dừng exit 10. Chạy lại thì bảng đó được snapshot.
- **Source PG:** thay đổi cột được phát hiện qua message Relation của pgoutput. Không có DDL tự động.

`schema.additive` và `schema.destructive` chưa có tác dụng (mục 6.3).

---

## 4. Yêu cầu theo engine

### 4.1 Source MySQL

Source cần binlog ROW đầy đủ. Bảng dưới là cho source có GTID (8.x, 5.7 `gtid_mode=ON`). Source không GTID (5.5, 5.6/5.7 `gtid_mode=OFF`) chạy ở chế độ vị trí binlog: xem `docs/MYSQL-5X-MIGRATION.md`. Source MariaDB 10.6: xem `docs/MARIADB-MIGRATION.md`. Mọi biến đổi được online, trừ `log_bin` và `log_replica_updates` (cần restart nếu chưa bật). `strand validate` kiểm cả giá trị global lẫn từng session đang mở.

| Biến | Giá trị | Đổi online | Ghi chú |
| --- | --- | --- | --- |
| `gtid_mode` | `ON` | Có, qua `OFF_PERMISSIVE` → `ON_PERMISSIVE` → `ON` | Trước đó bật `enforce_gtid_consistency` và đợi `Ongoing_anonymous_transaction_count = 0` |
| `binlog_format` | `ROW` | Có | Phạm vi session: kết nối cũ giữ giá trị cũ |
| `binlog_row_image` | `FULL` | Có | Phạm vi session |
| `binlog_row_value_options` | rỗng | Có | `PARTIAL_JSON` bị từ chối |
| `binlog_row_metadata` | `FULL` (khuyến nghị) | Có | `MINIMAL` chỉ bị cảnh báo |
| `log_bin` | bật | Không | 8.0+ bật sẵn |
| `log_replica_updates` | `ON` khi đọc từ replica | Không | |
| `server_id` | khác 0 | Có | |

Đổi an toàn:

1. `SET PERSIST binlog_row_metadata=FULL; SET PERSIST binlog_row_image=FULL; SET PERSIST binlog_format=ROW;`
2. Cho ứng dụng kết nối lại. Trên replica chạy `STOP REPLICA; START REPLICA;`.
3. `strand validate`. Nó trả exit 3 và liệt kê thread nếu còn session ghi binlog với giá trị cũ.

Nên đọc từ replica có `log_replica_updates=ON`: trỏ `source` vào replica. `source.read_from` chưa có tác dụng.

Source có GTID thì định vị bằng GTID. Source không GTID thì định vị bằng file + position (`--start-at mysql-bin.000123:4567`), với các giới hạn ở `docs/MYSQL-5X-MIGRATION.md` mục 10. Retention binlog phải đủ dài để phủ thời gian snapshot logic (`strand plan` tính ETA) hoặc tuổi backup cộng thời gian restore (chế độ B).

### 4.2 Source PostgreSQL

| Yêu cầu | Chi tiết |
| --- | --- |
| `wal_level` | `logical`. Thiếu thì exit 3 |
| `max_replication_slots`, `max_wal_senders` | Mỗi pipeline 1 slot và 1 walsender. Nên đặt `max_slot_wal_keep_size` |
| Slot | Mặc định tên `strand_<name>`. `source.create_slot: true` để Strand tạo; mặc định `false`, slot thiếu thì exit 5 `log_gap`. PG17+: `failover_slot: true` tạo slot `FAILOVER` |
| Publication | **DBA tạo trước**. Mặc định tên `strand_<name>`: `CREATE PUBLICATION strand_x FOR TABLE app.a, app.b WITH (publish = 'insert, update, delete');` hoặc `FOR ALL TABLES`. TRUNCATE chưa hỗ trợ: nếu publication phát TRUNCATE thì Strand dừng |
| Bảng partitioned | `ALTER PUBLICATION … SET (publish_via_partition_root = true)`. Thiếu thì Strand từ chối chạy và in đúng lệnh cần chạy |
| REPLICA IDENTITY | `DEFAULT` (PK) là đủ. UPDATE và DELETE gửi khóa theo replica identity |
| Snapshot logic | Source phải là **primary**: watermark dùng `pg_logical_emit_message`. Cần PG 14+ |
| Decode trên standby | PG 16+ và `hot_standby_feedback=on` (reader kiểm). `read_from: standby` chưa triển khai; snapshot logic trên standby bị từ chối |
| Quyền | `LOGIN REPLICATION`, `SELECT` trên bảng, `SELECT` trên sequence (đồng bộ `last_value` cho promote), dòng `replication` trong `pg_hba.conf` |

`strand validate` **chưa** kiểm các điều kiện PG ở trên. Lỗi chỉ hiện khi `sync` khởi động (handoff dòng 33).

Kết nối PG chỉ khai báo được bằng `dsn` (`postgres://user:pass@host:5432/db?sslmode=require`). Các trường `host`/`user`/`password_env` chỉ dành cho `kind: mysql`.

### 4.3 Quyền

| User | Nơi | Quyền | Dùng cho |
| --- | --- | --- | --- |
| `strand` | Source MySQL | `REPLICATION SLAVE, REPLICATION CLIENT ON *.*`; `SELECT ON app.*` | Đọc binlog, snapshot, verify |
| `strand` | Source MySQL (khuyến nghị) | `SELECT` trên `performance_schema.variables_by_thread`, `performance_schema.threads` | validate kiểm session; thiếu thì chỉ cảnh báo |
| `strand` | Target MySQL | `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP ON app.*` và trên schema metadata; `CONNECTION_ADMIN ON *.*` | Apply, bảng `_strand_*`, ghi khi `read_only=ON`, ngắt instance cũ |
| `strand` | Target PG | `CONNECT, TEMPORARY` trên database; `USAGE, CREATE` trên schema; `SELECT, INSERT, UPDATE, DELETE` trên bảng; `UPDATE` trên sequence | Apply, `_strand_*`, setval khi promote |
| `strand_admin` | Source và target MySQL | `SYSTEM_VARIABLES_ADMIN ON *.*` | Chỉ `promote` |
| (PG promote) | Source và target PG | Quyền `ALTER SYSTEM`, `ALTER ROLE`, `pg_terminate_backend`, GRANT/REVOKE trên bảng của ứng dụng | Chỉ `promote` |

Để tạo schema, target cần quyền CREATE trên database/schema đích (và quyền tạo database nếu chưa có). `validate` không kiểm quyền; thiếu quyền chỉ lộ ra khi chạy.

Strand không ghi gì vào source khi đồng bộ, với hai ngoại lệ:

- `promote` fence source cũ.
- PG snapshot logic gọi `pg_logical_emit_message`. Hàm này chỉ ghi một message vào WAL, không ghi bảng.

Metadata `_strand_pipeline`, `_strand_checkpoint`, `_strand_worker_delta` và `_strand_promote` nằm trong database/schema của target DSN.

---

## 5. Yêu cầu và giới hạn của bảng

### 5.1 Bắt buộc có PRIMARY KEY

Mọi bảng của pipeline phải có PK. Thiếu thì exit 3 `missing_pk`.

Lý do: apply dùng upsert và delete theo khóa, và việc gán thay đổi vào chunk snapshot cũng theo khóa.

### 5.2 Snapshot logic với từng kiểu PK

Snapshot logic chia bảng thành các range khóa. Mỗi thay đổi từ stream phải được gán đúng range bằng phép so trong Go. Vì vậy **thứ tự và phép bằng của khóa trong Go phải giống hệt DB** (`ORDER BY pk`, unique index). Lệch một chút là mất dữ liệu âm thầm. Strand chỉ nhận những kiểu có thứ tự chứng minh được, còn lại từ chối rõ ràng.

| Kiểu cột PK | MySQL | PostgreSQL |
| --- | --- | --- |
| Số nguyên (kể cả `UNSIGNED`), `YEAR` | Có | `int2/int4/int8` |
| `DECIMAL`/`numeric` | Có | Có |
| `DATE`, `DATETIME`, `TIMESTAMP` | Có | `date`, `timestamp`, `timestamptz` |
| `TIME` | Có | Không |
| `BINARY`/`VARBINARY`, charset `binary` | Có | `bytea` |
| `CHAR`/`VARCHAR` collation `_bin` (PAD SPACE và NO PAD) | Có (mức 1) | — |
| `CHAR`/`VARCHAR` collation không nhị phân (`_ci`, `_ai`, `general_ci`…) | Có (mức 2: thứ tự lấy từ `WEIGHT_STRING` của chính MySQL) | — |
| `text`/`varchar`/`char` collation `"C"`, `"POSIX"`, `ucs_basic` (hoặc collation mặc định của DB là C) | — | Có |
| `text` collation ICU hoặc libc khác C (`en_US.UTF-8`…) | — | **Không** (mức 3, đang mở) |
| `uuid` | — | Có |
| Kiểu khác (`FLOAT`, `ENUM`, `JSON`…) | Không | Không |

Ghi chú:

- Charset chuỗi MySQL được nhận: `utf8mb4`, `utf8mb3`, `latin1`, `ascii`, `binary`. Charset khác (`gbk`, `utf16`…) bị từ chối.
- PK nhiều cột được nhận khi mọi cột đều thuộc bảng trên. Các cột được so như tuple theo thứ tự của PK.
- Bảng đích phải có **cùng** PK: cùng cột, cùng kiểu, cùng collation. Bảng do `schema init` tạo luôn thỏa. MySQL→PG xem hai ý ngay dưới.
- MySQL→PG, khóa mà thứ tự hai engine trùng nhau (số, thời gian, `VARBINARY`↔`bytea`): snapshot và `verify --mode full` theo range khóa như PG→PG.
- MySQL→PG, khóa chuỗi (`_ci`, `_ai`, `general_ci`, `_bin`…) sang `text`/`varchar`/`char` `COLLATE "C"` (cột khóa chuỗi do `schema init` tạo thỏa điều kiện bên dưới): thứ tự và phép bằng hai phía khác nhau (`'a' = 'A'` ở source, khác nhau ở target), nhưng bảng **có dữ liệu sẵn** vẫn được snapshot logic. Strand không dùng range ở target cho bảng này:
  - Flush chunk theo **tập khóa chính xác**: ghi đúng các row của chunk, xóa đúng khóa đã bị xóa trong lúc đọc chunk. Không xóa theo range.
  - Chạy tiếp snapshot dở (sau crash, kill -9): quét **toàn bộ khóa** bảng đích, tính khóa source bằng `WEIGHT_STRING` trên source (theo lô 128 giá trị), xóa row thuộc phần chưa đọc. Lượt quét chạy khi router đang dừng, nên stream của mọi bảng đứng chờ trong lúc đó (bảng lớn: vài phút).
  - `verify --mode full`: chunk theo thứ tự source. Target được quét toàn bảng một lượt, mỗi row gom vào chunk theo khóa source. Chunk lệch được so từng row, mỗi lượt quét target lo tối đa 16 chunk lệch. Biến thể khác byte nhưng bằng nhau ở source (`'a'` ở source, `'A'` ở target) bị báo lệch.
  - Log khi khởi động: `target orders the primary key differently from the source: snapshot flushes by exact key set`.
  - Điều kiện: charset `utf8mb4`/`utf8mb3`/`ascii` (latin1 bị từ chối). Target là `text`/`varchar` `COLLATE "C"`, hoặc `char(n) COLLATE "C"` khi cột source là `CHAR(n)` hay collation PAD SPACE. Cột khác trong PK ghép chỉ được là số nguyên hoặc `DECIMAL` (cột thời gian hay nhị phân cạnh cột chuỗi bị từ chối). Chưa có `verify --resume` và `verify --repair fix` cho các bảng này.
- Bị từ chối nghĩa là `sync` hoặc `snapshot add` dừng exit 3 với thông báo `table … needs a logical snapshot but its primary key is not supported: <lý do>; seed it from a backup instead`. Cách xử lý: seed từ backup (mục 7.5), `--start-at`, hoặc đổi kiểu/collation cột khóa.

Với PK tổng quát (không phải một cột số nguyên), các phần sau còn thiếu và đều bị từ chối rõ ràng:

- `verify --resume`.
- `verify --repair fix` với PG→PG.
- Chia đôi chunk khi query snapshot quá thời gian (chỉ có cho khóa số nguyên).

### 5.3 Giới hạn khác

- **UNIQUE ngoài PK:**
  - MySQL: các Txn đụng cùng giá trị unique được xếp tuần tự. Khóa chuỗi collation `_ci` được gộp theo collation. Unique index có cột prefix (ví dụ `UNIQUE(code(3))`) xếp tuần tự mọi Txn chạm index đó trong bảng.
  - PG (relaxed): bảng có UNIQUE ngoài PK, unique index biểu thức/partial hoặc exclusion constraint, mà không đặt `REPLICA IDENTITY FULL`, đi **làn tuần tự**: mọi Txn chạm bảng đó được apply đúng thứ tự commit ở một luồng riêng, gom nhiều Txn liền nhau vào một transaction ở target. Txn khác vẫn chạy song song; Txn phụ thuộc một Txn làn cũng đi vào làn. Lý do: pgoutput không gửi giá trị unique cũ nên không tự phát hiện được xung đột. Các Txn đang chờ trong làn cũng tính vào trần bộ nhớ của scheduler.
  - PG có nhiều Txn chạm bảng như vậy: làn tuần tự làm relaxed gần bằng tốc độ strict. Nếu phần lớn tải đi qua các bảng này, dùng `strict` cho đơn giản; hoặc đặt `ALTER TABLE ... REPLICA IDENTITY FULL` để bảng không phải đi làn (đổi lại WAL lớn hơn).
- **FK:** MySQL: phiên ghi của Strand tắt kiểm FK (`foreign_key_checks=0`). PG: FK vẫn được kiểm; snapshot chép bảng cha trước bảng con. Cả hai engine: Txn chạm cùng row cha/con được xếp theo thứ tự commit, kể cả FK tham chiếu UNIQUE ngoài PK.
- **TOAST (PG):** UPDATE mà cột TOAST không đổi chỉ gửi các cột còn lại.
  - Nếu row không có ở target, Strand dừng với `verify_mismatch` (exit 5). Sửa bằng `verify --mode full --table … --repair fix` rồi chạy lại.
  - Đổi PK kèm cột TOAST không đổi khi bảng đang snapshot: Strand đọc lại khóa mới từ source. Metric `strand_pending_cross_range` và `strand_pending_cross_range_oldest_seconds` theo dõi việc này.
- **TRUNCATE:** chưa hỗ trợ. PG: tạo publication với `publish = 'insert, update, delete'`.
- **Charset (MySQL→MySQL):** cột `latin1` và các charset khác được đổi đúng sang chuỗi trước khi ghi. `latin1` theo biến thể cp1252 của MySQL.
- **Múi giờ:** phiên MySQL của source luôn chạy UTC, nên `TIMESTAMP` không bị lệch theo `time_zone` của server.
- **Không đồng bộ hai chiều:** không lọc nguồn gốc thay đổi, không giải quyết xung đột.
- **Không đồng bộ:** user, quyền, view, routine, trigger, event, sequence độc lập (PG; promote xử lý `last_value`).

---

## 6. Tham chiếu cấu hình

Mỗi pipeline có một file YAML. Thứ tự ưu tiên: YAML, rồi biến môi trường `STRAND_*`, rồi cờ dòng lệnh. Khóa lạ bị từ chối (exit 2). In JSON Schema đầy đủ bằng `strand config schema`.

### 6.1 Kết nối

Mỗi phía chọn **một** trong ba cách:

| Cách | Ví dụ | Ghi chú |
| --- | --- | --- |
| Tách trường (chỉ MySQL) | `host: db1`, `port: 3306`, `user: strand`, `password_env: SRC_PW`, `database: strand` | `port` mặc định 3306. Dùng `password` **hoặc** `password_env` (tên biến môi trường), không dùng cả hai |
| URL | `dsn: "mysql://strand:p%40ss@db1:3306/strand?tls=true"` / `dsn: "postgres://strand:pw@pg1:5432/app?sslmode=require"` | Ký tự đặc biệt trong mật khẩu phải URL-encode (`@` → `%40`) |
| DSN driver Go (MySQL) | `dsn: "strand:pass@tcp(db1:3306)/strand?tls=true"` | |

- Không được đặt `dsn` cùng lúc với `host`.
- Trong YAML có thể dùng `${TÊN_BIẾN}`; giá trị được thay khi đọc file.
- Tham số `tls` của DSN MySQL source cũng áp cho kết nối đọc binlog.
- `target.database` (hoặc database trong DSN target) là nơi chứa bảng `_strand_*`.

### 6.2 Các khóa có tác dụng

| Khóa | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `version` | 1 | Phiên bản file cấu hình |
| `name` | **bắt buộc** | ID pipeline: khóa chống chạy trùng, checkpoint, tên slot/publication mặc định |
| `source.kind` / `target.kind` | **bắt buộc** | `mysql` \| `postgres` |
| `source.*` / `target.*` (kết nối) | **bắt buộc** | Mục 6.1 |
| `source.server_id` | tự sinh | Server ID khi đọc binlog; phải duy nhất trong cụm |
| `source.slot` | `strand_<name>` | PG: slot logical (ký tự lạ thành `_`) |
| `source.publication` | `strand_<name>` | PG: publication |
| `source.create_slot` | `false` | PG: tạo slot khi chưa có |
| `source.failover_slot` | `false` | PG17+: tạo slot `FAILOVER` |
| `source.sequence_sync_interval` | `5s` | PG: chu kỳ ghi `last_value` sequence độc lập vào `_strand_promote` (dùng khi promote không đọc được source). Đặt cùng giá trị cho `promote --sync-interval` |
| `target.commit_order` | **`strict`** | `strict` \| `relaxed` (mục 1.3) |
| `target.batch.max_rows` | 2000 | Đóng batch khi đủ số row |
| `target.batch.max_bytes` | `4MiB` | Đóng batch khi đủ dung lượng (ước lượng) |
| `target.batch.max_wait` | 0 | Chờ tối đa để gom thêm Txn. Relaxed: chuỗi Txn của một khóa nóng được gom cùng batch (không còn giới hạn 1/max_wait Txn/s). Strict: khóa nóng vẫn chỉ đạt khoảng 1/max_wait Txn/s (handoff dòng 26) |
| `tables.include` | **bắt buộc** | Glob `schema.table` (mục 2) |
| `tables.exclude` | rỗng | Glob |
| `tables.rename` | rỗng | `"src.t": "dst.t"` |
| `bootstrap.default` | `logical` | `logical` (chế độ A). `seed` hoặc `start_at` cho pipeline mới bắt buộc `--start-at`, không có thì exit 2 |
| `bootstrap.per_table.<t>.mode` | — | Chỉ `strand plan` đọc (ETA theo bảng); `sync` không dùng |
| `bootstrap.logical.chunk_rows` | `auto` (5000) | Số row mỗi chunk snapshot |
| `bootstrap.logical.windows` | rỗng (mọi lúc) | Khung giờ cho phép snapshot, ví dụ `["22:00-06:00"]` (giờ địa phương của process; qua nửa đêm được). Khi `bootstrap_retention_risk` bật, snapshot chạy cả ngoài khung |
| `bootstrap.create_schema` | `auto` | Mục 3.1 |
| `checkpoint.flush_every` | `500ms` | Chu kỳ gộp row delta vào checkpoint toàn cục (relaxed) |
| `resources.spool.path` | **bắt buộc** | Thư mục trạng thái cục bộ, spill, prefetch (`<path>/prefetch`), file tràn của reader PG (`<path>/pg-reader-<name>`) |
| `resources.spool.hard` | không giới hạn | Trần spool local; từ 95% trở lên là tín hiệu cứng dừng snapshot |
| `resources.spool.overflow` | rỗng | Tầng bucket `s3://bucket/prefix` (credential qua `STRAND_S3_*`) |
| `resources.source.iops.soft` | 600 | Tốc độ đọc snapshot (token/giây). Đổi nóng bằng `throttle set` |
| `resources.source.iops.hard` | không | Trần cho GRACE |
| `resources.workers.max` | 8 | Số luồng apply (relaxed). `strict` luôn 1 |
| `governor.algorithm` | `onoff` | `onoff` \| `grace-v2` \| `static`. Source PG dùng tín hiệu PG (docs/GOVERNOR-WIRING.md); thiếu quyền đọc view thống kê thì lùi về `static` |
| `governor.tick` / `governor.lag_slo` | `1s` / `5s` | Nhịp điều tốc và lag mục tiêu |
| `governor.probe` | `on` | `grace-v2`: `off` tắt dither ±1% |
| `governor.breaker.replica_lag.max` | tắt | Replica lag vượt ngưỡng thì cắt snapshot |
| `governor.breaker.strand_query_p99.max_ratio` | tắt | Độ trễ query của Strand vượt n lần nền thì cắt snapshot |
| `verify.delta` | `off` | `on`: verify các khóa CDC vừa chạm (MySQL→MySQL) |
| `verify.post_seed_window` | `30m` | Cửa sổ verify sau seed / `--start-at` (MySQL→MySQL) |
| `observability.metrics` | tắt | Địa chỉ HTTP cho `/metrics` và `/healthz`, ví dụ `":9464"` |
| `observability.admin_socket` | tắt | Unix socket cho `top`, `pause`, `resume`, `throttle`, `snapshot add`, `checkpoint`, `verify --mode delta`. Lệnh client mặc định dùng `/run/strand/default.sock` |
| `observability.log.level` / `format` | `info` / `json` | `debug` \| `info` \| `warn` \| `error`; `json` \| `text` |

Biến môi trường:

- `STRAND_<ĐƯỜNG_DẪN_KHÓA>` ghi đè khóa đơn (chuỗi, số, bool, thời lượng), không áp cho map và list. Ví dụ `STRAND_TARGET_DSN`, `STRAND_GOVERNOR_LAG_SLO=10s`, `STRAND_RESOURCES_WORKERS_MAX=16`.
- `STRAND_S3_ENDPOINT`, `STRAND_S3_ACCESS_KEY`, `STRAND_S3_SECRET_KEY`: credential cho tầng bucket.

### 6.3 Khóa được nhận nhưng chưa có tác dụng

**Có cảnh báo** khi `validate`/`sync` (`config key … is accepted but not implemented in this version`):

- `source.read_from=standby`, `tables.mask`
- `resources.source.bandwidth`, `resources.target.bandwidth`, `resources.network.backup_read`, `resources.schedule`
- `resources.memory.*` (không có giới hạn RSS), `resources.target.iops` (ghi target không bị điều tốc), `resources.spool.soft`
- `target.write_mode=insert` (writer luôn upsert)
- `resources.workers.min` khi khác `workers.max` (số worker cố định)
- `bootstrap.per_table.<t>.seed.{credentials, encryption_key, load, verify_after}`

- `source.read_from=replica`: muốn đọc replica thì trỏ kết nối vào replica.
- `schema.additive=auto`, `schema.destructive=approve`: DDL không bao giờ được áp tự động; pipeline dừng exit 10.
- `checkpoint.store` khác `target`: checkpoint luôn nằm ở target.
- `governor.metrics_source`
- `verify.full.cron`, `verify.full.rate`: không có verify full theo lịch; chạy `strand verify --mode full` bằng cron ngoài.
- `bootstrap.logical.max_parallel`, `bootstrap.catch_up.binlog_archive` (dùng `seed apply --archive`).

Không cảnh báo khi giá trị đúng là hành vi thật: `read_from: primary`, `schema.additive: pause`, `schema.destructive: pause`, `checkpoint.store: target`.

### 6.4 Ví dụ

Tối thiểu (MySQL→MySQL):

```yaml
version: 1
name: orders-sync
source: { kind: mysql, host: 10.0.0.11, user: strand, password_env: SRC_PW }
target: { kind: mysql, host: 10.0.0.21, user: strand, password_env: DST_PW, database: strand }
tables: { include: ["app.*"] }
resources: { spool: { path: /var/lib/strand/orders-sync } }
observability: { admin_socket: /run/strand/orders-sync.sock, metrics: ":9464" }
```

PG→PG:

```yaml
version: 1
name: billing
source:
  kind: postgres
  dsn: "postgres://strand:${SRC_PW}@pg-src:5432/billing?sslmode=require"
  create_slot: true            # slot strand_billing
  publication: strand_billing  # DBA tạo trước
target:
  kind: postgres
  dsn: "postgres://strand:${DST_PW}@pg-dst:5432/billing?sslmode=require"
tables: { include: ["public.*"] }
resources: { spool: { path: /var/lib/strand/billing } }
observability: { admin_socket: /run/strand/billing.sock }
```

Đầy đủ (tùy chọn nâng cao):

```yaml
version: 1
name: orders-sync
source: { kind: mysql, host: 10.0.0.11, user: strand, password_env: SRC_PW, server_id: 47001 }
target:
  kind: mysql
  host: 10.0.0.21
  user: strand
  password_env: DST_PW
  database: strand
  commit_order: relaxed
  batch: { max_rows: 2000, max_bytes: 4MiB }
tables:
  include: ["app.*"]
  exclude: ["app.audit_log"]
  rename: { "app.orders": "app2.orders" }
bootstrap:
  default: logical
  logical: { chunk_rows: 10000, windows: ["22:00-06:00"] }
resources:
  spool: { path: /var/lib/strand/orders-sync, hard: 50GiB, overflow: "s3://strand-staging/orders" }
  source: { iops: { soft: 600, hard: 1200 } }
  workers: { max: 8 }
governor:
  algorithm: grace-v2
  lag_slo: 5s
  breaker: { replica_lag: { max: 30s }, strand_query_p99: { max_ratio: 3 } }
verify: { delta: on, post_seed_window: 30m }
observability:
  metrics: ":9464"
  admin_socket: /run/strand/orders-sync.sock
  log: { level: info, format: json }
```

---

## 7. Tham chiếu lệnh

Cờ chung:

- `-c, --config <file>`: file cấu hình.
- `--dry-run`: chạy thử. Lệnh có hỗ trợ thì không ghi target. `verify --repair fix --dry-run` chỉ báo cáo.

Lệnh nói chuyện với `sync` đang chạy (`top`, `pause`, `resume`, `throttle`, `snapshot add`, `checkpoint`, `verify --mode delta`) tìm socket theo thứ tự:

1. `--socket`
2. `observability.admin_socket` trong file `-c`
3. `/run/strand/default.sock`

### 7.0 `config init`

`strand config init --from mysql|mariadb|postgres --to mysql|postgres [--name N] [--source-host H] [--target-host H] [--database app] [--systemd] [-o file] [--force]`. Sinh file cấu hình mẫu có chú thích; mật khẩu luôn đọc từ biến môi trường `SRC_PW`/`DST_PW`. Không có `-o` thì in ra màn hình. File đã có thì không ghi đè, trừ khi có `--force`. Cặp nguồn/đích không hỗ trợ (MariaDB → PG, PG → MySQL) bị từ chối.

### 7.1 `validate`

`strand validate -c p.yaml`. Exit 0 khi đạt, exit 3 kèm lý do khi bị từ chối. Cảnh báo in ra stderr.

Nó kiểm:

- **Cấu hình:** kind, `include` không rỗng, `commit_order`, `write_mode`, khóa chưa có tác dụng.
- **Source MySQL:**
  - Biến binlog/GTID global và từng session.
  - Bảng tồn tại và có PK.
  - Mẫu include khớp ít nhất một bảng.
  - Partial JSON.
- **MySQL→PG:** ánh xạ kiểu, collation khóa, giá trị không chuyển được.
- **Schema đích:** số bảng sẽ được tạo. Với `create_schema: off`, bảng thiếu thì exit 3.

Không kiểm: điều kiện PG source (mục 4.2), quyền, PK có snapshot logic được không (lỗi này chỉ hiện ở `sync`).

### 7.2 `plan`

`strand plan -c p.yaml [--force]` in từng bảng: số row ước lượng, dung lượng, chế độ và ETA theo `resources.source.iops.soft`. Nó từ chối chế độ A khi ETA vượt 50% thời gian giữ binlog. `--force` chấp nhận rủi ro; nếu không, dùng chế độ B.

### 7.3 `sync`

```bash
strand sync -c p.yaml                                   # lần đầu: snapshot logic rồi stream; các lần sau: từ checkpoint
strand sync -c p.yaml --start-at seed --seed-manifest seed.json
strand sync -c p.yaml --start-at '3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5000'   # MySQL: GTID set
strand sync -c p.yaml --start-at 0/16B3748                                        # PG: LSN
```

- `--start-at` chỉ dùng cho pipeline mới. Khi đã có checkpoint, dùng `checkpoint rewind`.
- Chạy foreground. `SIGTERM`/`Ctrl-C` dừng đọc, chờ writer, ghi checkpoint rồi thoát 0.
- Hai instance cùng `name`: instance mới giữ khóa, tăng epoch và ngắt instance cũ. Instance cũ dừng với exit 4 `fenced`.
- Triển khai thường trực: systemd `strand@<name>.service` hoặc StatefulSet (`deploy/`), với `RestartPreventExitStatus=2 3 4 5 10`.

### 7.4 `schema init`

Xem mục 3.1.

### 7.5 `seed plan` / `seed apply` (chế độ B)

Quy trình chung:

1. Backup bằng công cụ gốc.
2. `seed plan` kiểm SD-1..SD-4 và ghi `seed.json`: P hợp lệ, log còn phủ từ P, phiên bản công cụ và target.
3. Restore, kèm `seed apply` để kiểm target đúng P và prefetch log.
4. `sync --start-at seed`.

| `--type` | Engine | Lấy P từ | Yêu cầu riêng |
| --- | --- | --- | --- |
| `xtrabackup` | MySQL | `xtrabackup_binlog_info` | Backup đã `--prepare` |
| `clone` | MySQL | `gtid_executed` của target sau CLONE | Plugin clone; `BACKUP_ADMIN` trên donor |
| `mydumper` | MySQL | `metadata`, section `[source]` | **Bắt buộc** `--dump-log <stderr của mydumper>` (từ chối `NO_LOCK`). Backup cũ không có log: `--accept-unverified-lock-mode` |
| `mysqldump` | MySQL | `SET @@GLOBAL.GTID_PURGED` | `--single-transaction --source-data=2 --set-gtid-purged=ON`; file còn dòng `-- Dump completed`; `RESET BINARY LOGS AND GTIDS` trên target trước khi nạp. Luôn có cảnh báo |
| `pgbackrest` | PG | slot (`--slot`) | `--archive-info` (JSON của `pgbackrest repo-ls archive/<stanza>/<archive-id> --recurse --output=json`; `info` chỉ có min/max nên không đủ chứng minh WAL liên tục), `--retention-full`, `--backup-label`, `--eta` |
| `walg` | PG | slot (`--slot`) | `--archive-info` (JSON của `wal-g wal-show --detailed-json`), `--tool-version` |
| — (`strand export`) | PG | slot do export tạo | `export` ghi `seed.json` trực tiếp, không cần `seed plan` |

Cờ `seed plan`:

- `--type`, `--backup`: bắt buộc.
- `--source`: DSN source; đọc `gtid_purged` hoặc slot.
- `--target`: DSN target đã restore; đối chiếu P và phiên bản.
- `--archive-until`: GTID set đã có trong archive binlog.
- `--tables`, `--out` (mặc định `seed.json`), `--allow-prerelease-tool`.
- Riêng PG: `--slot`, `--backup-label`, `--archive-info`, `--tool-version`, `--eta` (mặc định 2h), `--retention-full`, `--full-interval`.
- PG SD-2: WAL từ đầu backup tới P phải có liên tục trên timeline của backup. Lỗ giữa các dải, chỉ có min/max, hoặc archive có timeline mới hơn bắt đầu trước P (source có thể đã failover) đều bị từ chối `log_gap` (exit 5).
- PG `--tables` (và `tables` trong `seed.json`): rỗng hoặc `*` = mọi bảng (mặc định của seed vật lý, vì restore vật lý chứa cả cluster); còn lại là pattern kiểu `pg_dump -t`: `schema.table`, `*`/`?` là wildcard, chữ ngoài ngoặc kép bị hạ về chữ thường, `"Tên"` giữ nguyên hoa/thường; tên không có schema hiểu là `public`. `sync --start-at seed` coi bảng khớp là đã có dữ liệu, bảng còn lại được snapshot logic.

Cờ `seed apply`:

| Cờ | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `--manifest` | `seed.json` | Manifest |
| `--target` | bắt buộc | DSN target đã restore |
| `--print-only` | false | Chỉ in các bước restore gợi ý |
| `--restore-cmd` | rỗng | Lệnh restore (`sh -c`) chạy song song với prefetch |
| `--prefetch` | true | MySQL: đọc binlog từ P vào spool trong lúc restore |
| `--spool` | — | Phải là `<resources.spool.path>/prefetch` |
| `--source` | rỗng | DSN source hoặc replica để prefetch |
| `--archive` | rỗng | Thư mục binlog archive |
| `--spool-local-hard`, `--overflow`, `--spool-hard` | 0, rỗng, 0 | Trần tầng local, tầng bucket, trần tổng (chạm thì exit 5 `prefetch_exhausted`) |

`seed apply` theo engine:

- **MySQL:** kiểm `gtid_executed` của target bằng P và gỡ cấu hình replication còn sót. Tạo schema metadata với `SET SESSION sql_log_bin=0; CREATE DATABASE strand; SET SESSION sql_log_bin=1;` để `gtid_executed` vẫn đúng bằng P.
- **PG (pgBackRest/WAL-G):**
  - Restore target thành standby với `recovery_target_lsn = P`, `recovery_target_inclusive = on`, `recovery_target_action = pause`.
  - `seed apply` kiểm `pg_last_wal_replay_lsn() = P` rồi gọi `pg_promote()`.
  - Chạy lại sau khi đã promote vẫn an toàn.
  - Trước khi mở ghi: tắt `archive_mode` (hoặc dùng stanza khác), xóa publication/subscription thừa kế từ source.
- **`strand export`:** kiểm các bảng của export đã có ở target (sau `pg_restore`).

### 7.6 `export` (PG)

```bash
strand export --source "postgres://strand@pg-src/app" --slot strand_billing \
  --tables public.orders,public.users --out /backup/dump -F d -j 8 --manifest seed.json
```

Lệnh tạo slot cùng exported snapshot, rồi chạy `pg_dump --snapshot` (`--pg-dump` chỉ đường dẫn binary). P là `consistent_point` của slot. Sau đó: `pg_restore -j 8 -d <target> /backup/dump`, rồi `seed apply`, rồi `sync --start-at seed`.

### 7.7 `snapshot add`

`strand snapshot add --table app.t -c p.yaml` thêm bảng vào pipeline đang chạy. Bảng được snapshot logic có điều tốc, không dừng stream. PK theo mục 5.2. PG source: bảng phải có trong publication.

### 7.8 `verify`

| Chế độ | Lệnh | Cặp | Ghi chú |
| --- | --- | --- | --- |
| sample | `verify --mode sample --table app.t [--rate 0.01]` | Mọi cặp (PG→PG, MySQL→PG: chỉ PK một cột số nguyên) | Lấy mẫu `--rate` phần row theo khoảng khóa, so trực tiếp (khác engine: so chuỗi chính tắc như full) |
| delta | `verify --mode delta` | Mọi cặp | Qua socket: verify các khóa CDC vừa chạm. Lệch thì exit 5 |
| full | `verify --mode full [--table app.t] [--rate N]` | Mọi cặp | Cây Merkle theo chunk 10.000 row, rồi so từng row ở chunk lệch. `--rate` ở đây là chunk/giây (không đặt thì 10) |

- Mọi chế độ kiểm lại row lệch: đọc source, chờ target apply tới vị trí log của source lúc đọc (MySQL: `gtid_executed`; PG: WAL insert LSN, kèm một watermark `pg_logical_emit_message` để pipeline dời checkpoint qua đó cả khi source rảnh), đọc target, đọc lại source. Row đổi ở source trong mọi lần thử (mặc định 5) được đếm `unverifiable_hot` (metric `strand_verify_unverifiable_hot_total`), không báo lệch.
- Bước chờ apply chỉ chạy khi pipeline đang chạy (khóa pipeline trên target đang bị giữ). Pipeline đã dừng thì không chờ; pipeline đang `pause` thì chờ hết hạn (2 phút) rồi lỗi. Source PG đọc qua `--replica-dsn` (standby) không ghi được watermark: source rảnh thì chờ có thể hết hạn.
- `--table` của full: không đặt thì dùng `tables.include`, nhưng **không nhận wildcard**. Với `app.*`, phải chạy từng bảng (handoff dòng 51).
- `--resume`: chạy tiếp lượt full bị ngắt. Chỉ dùng lại chunk đã khớp. Lượt chạy hết thì tiến độ bị xóa. Không có cho PK tổng quát.
- `--replica-dsn`: đọc phía source từ replica.
- `--repair report|fix`:
  - `fix` chép lại row lệch từ source: một transaction, tắt kiểm FK, không ghi cột generated.
  - Chỉ cùng engine; PK tổng quát chỉ MySQL→MySQL.
  - **`fix` yêu cầu pipeline đã dừng.** Nó lấy khóa của pipeline; `sync` đang chạy thì exit 4. Lý do: repair song song với stream có thể ghi đè ảnh mới bằng ảnh cũ.
  - Quy trình đúng: (1) để pipeline bắt kịp; (2) dừng `sync`; (3) `verify --mode full --table … --repair fix`; (4) chạy lại `sync`. Thay đổi source trong lúc dừng nằm sau checkpoint nên sẽ được phát lại.
  - Nếu repair gặp trùng UNIQUE vì source vừa đổi, nó báo `the source changed while repairing`; làm lại quy trình.

### 7.9 `pause` / `resume` / `throttle` / `top` / `checkpoint`

| Lệnh | Tác dụng |
| --- | --- |
| `strand top` | Trạng thái, lag (giây), tốc độ (row/s giữa hai mẫu gần nhất), vùng governor, tiến độ và ETA snapshot |
| `strand pause` | Dừng đọc và apply, giữ vị trí |
| `strand pause --snapshot` | Chỉ dừng snapshot logic; stream vẫn chạy |
| `strand resume` | Chạy lại tất cả |
| `strand resume --snapshot-only` | Chỉ chạy lại snapshot, hoãn stream |
| `strand throttle set --source-iops N [--soft]` | Đổi nóng **soft limit** đọc snapshot, áp cho mọi governor kể cả `static`. Có hay không `--soft` đều đổi soft; hard limit chỉ đổi qua cấu hình và khởi động lại. `--target-iops` bị từ chối (chưa thi hành) |
| `strand checkpoint show` | Pipeline, epoch, vị trí đã apply, thời điểm cập nhật |
| `strand checkpoint rewind --to <gtid set>` | MySQL→MySQL: tua vị trí (kiểm `gtid_purged`), tăng epoch, pipeline dừng và phải chạy lại |

### 7.10 `promote`

`strand promote` chuyển ứng dụng sang target theo các bước có trạng thái (bảng `_strand_promote`); chạy lại sẽ tiếp từ bước dở. Giữ `sync` chạy trong lúc promote, dừng nó sau khi promote xong.

```bash
# MySQL
strand promote --pipeline orders-sync \
  --target "mysql://strand_admin:***@10.0.0.21:3306/strand" \
  --fence  "mysql://strand_admin:***@10.0.0.11:3306/" --catchup-timeout 5m
# PG
strand promote --pipeline billing --engine pg \
  --target "postgres://admin@pg-dst/billing" --fence "postgres://admin@pg-src/billing" \
  --app-role app_rw --source-app-roles app_rw --schemas public
```

| Bước | MySQL | PostgreSQL |
| --- | --- | --- |
| 1. Fence source | `SET GLOBAL super_read_only = ON` | `ALTER SYSTEM SET default_transaction_read_only = on` + reload; role trong `--source-app-roles` bị `NOLOGIN`; ngắt session client (trừ user Strand và walsender) |
| 2. Giữ target chỉ đọc | `read_only=ON`, `super_read_only=OFF` | Role `--app-role`: `default_transaction_read_only=on`, REVOKE quyền ghi (lưu lại để cấp lại) |
| 3. Bắt kịp | Chờ tập đã apply chứa `gtid_executed` của source, tối đa `--catchup-timeout` | Như MySQL, theo LSN |
| 4. Sequence | Không cần (AUTO_INCREMENT tự nâng) | `setval` theo `last_value` của source cộng biên `2·cache·inc + 2·r_peak·Δt_sync·inc` (`--peak-rate`, `--sync-interval`, `--override-sequences`) |
| 5. Mở ghi | `read_only=OFF` | Cấp lại quyền ghi cho role ứng dụng |

- `--pipeline` phải trùng `name` (mặc định `default`).
- `--engine auto` nhận diện engine từ DSN.
- Không có `--source-app-roles` thì fence PG chỉ mang tính hợp tác: role ứng dụng có thể tự đặt lại `default_transaction_read_only`.

**Failover khi source đã chết:**

1. Không fence được source thì promote dừng exit 10 `promote_needs_decision`. Nó in vị trí đã apply ở target và thời điểm checkpoint cuối, tức phần dữ liệu sẽ mất nếu tiếp tục.
2. Cô lập source bằng cách khác: tắt máy hoặc chặn mạng.
3. Chạy lại cùng lệnh, thêm `--source-fenced-externally`.

### 7.10b `objects` (MySQL/MariaDB → MySQL)

Chuyển view, procedure, function, trigger, event, user và quyền (`sync` chỉ chép bảng). Chỉ nhận nguồn họ MySQL và đích MySQL, cặp khác exit 3 `objects_unsupported`. Chi tiết và thời điểm chạy: [MARIADB-MIGRATION.md mục B](MARIADB-MIGRATION.md).

```bash
strand objects list   -c p.yaml
strand objects export -c p.yaml --out ./objects [--keep-definer] [--auth native|reset]
strand objects apply  -c p.yaml --phase pre|users|post [--from ./objects] [--dry-run]
```

- `pre`: view (theo thứ tự phụ thuộc), procedure, function, event ở trạng thái DISABLE. `post`: trigger và bật event. `users`: `CREATE USER` + `GRANT`.
- Trigger chỉ tạo ở cutover (`post`), vì binlog ROW đã chứa dòng trigger sinh ra ở nguồn.
- Idempotent: giống hệt thì bỏ qua, khác thì lỗi rõ ràng. `--source-dsn`/`--target-dsn` thay DSN trong cấu hình.
- `strand promote --objects-post [--objects-from ./objects]` chạy `post` sau bắt kịp, trước khi mở ghi; lỗi thì target vẫn read-only, chạy lại cùng lệnh. Tắt `event_scheduler` ở nguồn khi fence.
- `strand validate` in cảnh báo "N views, M triggers... not migrated automatically" khi nguồn có các đối tượng này.

### 7.11 `config schema`

`strand config schema` in JSON Schema của file cấu hình.

### 7.12 `archive binlog` (MySQL)

```bash
strand archive binlog --from "mysql://archiver:***@replica1:3306/" --to /archive/orders-db [--server-id 42001]
```

Lưu binlog liên tục từ một MySQL (nên là replica) vào thư mục, thay cho script `mysqlbinlog --raw --stop-never`. Thư mục này dùng trực tiếp cho `seed apply --archive`.

- Quyền: `REPLICATION SLAVE, REPLICATION CLIENT`. Cờ `tls=` trong DSN được áp cho kết nối binlog.
- File chỉ xuất hiện dưới tên `NNNNNNNN-<tên trên server>` khi server rotate (đủ transaction, đã fsync). File đang ghi tên `.partial-*`, reader bỏ qua. Số thứ tự ở đầu tên giữ đúng thứ tự cả khi replica được dựng lại và đánh số binlog lại từ đầu.
- Chạy lại sau khi dừng hoặc crash: tiếp tục theo GTID (trạng thái ở `.strand-archive.json`), không lặp, không hở. Có thể trỏ `--from` sang replica khác có cùng GTID.
- Lần chạy đầu bắt đầu từ `gtid_purged` của source (mọi binlog còn trên server), hoặc `--start-gtid`.
- Source đã purge phần chưa lưu thì dừng exit 5 `log_gap`. Hai archiver cùng thư mục: exit 4.
- Phần cuối chỉ được lưu sau khi server rotate. Muốn chốt thường xuyên hơn: giảm `max_binlog_size` hoặc chạy `FLUSH BINARY LOGS` định kỳ.
- Chưa có: đẩy thẳng lên S3 (`--to` chỉ nhận thư mục; đồng bộ thư mục lên bucket bằng công cụ riêng), dọn file cũ theo retention, GTID có tag (MySQL 8.4 `gtid tags`: bị từ chối).

### 7.13 `version`

`strand version` (hoặc `strand --version`) in phiên bản, commit, nền tảng.

### 7.14 Cài đặt bằng gói `.deb` / `.rpm`

`scripts/package.sh` dựng vào `dist/`: tarball + SHA-256 và gói `.deb`/`.rpm` cho linux/amd64 và linux/arm64 (binary tĩnh).

| Đường dẫn | Nội dung |
| --- | --- |
| `/usr/bin/strand` | Binary (gói dùng `/usr/bin`; tarball theo 03 §4 dùng `/usr/local/bin`) |
| `/usr/lib/systemd/system/strand@.service` | Unit template, một instance mỗi pipeline |
| `/etc/strand/` | root:strand 0750; đặt `<name>.yaml` (0640) và `<name>.env` (0600) |
| `/usr/share/doc/strand/` | `USAGE.md`, ví dụ `pipeline.yaml`, `pipeline.env` |
| `/var/lib/strand/<name>`, `/run/strand/<name>` | systemd tạo (StateDirectory / RuntimeDirectory) |

```bash
sudo dpkg -i strand_<ver>_amd64.deb      # hoặc: sudo rpm -i strand-<ver>.x86_64.rpm
sudo cp /usr/share/doc/strand/examples/pipeline.yaml /etc/strand/orders-sync.yaml
sudo cp /usr/share/doc/strand/examples/pipeline.env  /etc/strand/orders-sync.env
sudo chown root:strand /etc/strand/orders-sync.*; sudo chmod 0640 /etc/strand/orders-sync.yaml; sudo chmod 0600 /etc/strand/orders-sync.env
sudo systemctl enable --now strand@orders-sync
```

- Cài gói tạo user/group hệ thống `strand`, không tự bật pipeline nào.
- Nâng cấp: pipeline đang chạy giữ binary cũ tới khi `systemctl restart strand@<name>` (an toàn nhờ checkpoint).
- Gỡ gói: dừng và disable mọi `strand@*`. Không xóa `/etc/strand`, `/var/lib/strand` hay user, kể cả khi purge.
- Chưa có: chữ ký gói (03 §4 yêu cầu ký); ký file `.sha256` bằng khóa phát hành của tổ chức.

---

## 8. Vận hành, tuning, metric, exit code

### 8.1 Điều tốc

Governor chỉ điều tốc **phần đọc snapshot** trên source. Stream luôn được apply nhanh nhất có thể, nên target phải chịu được tải ghi tương đương source.

- **`onoff`:** mỗi `tick`, áp suất P được đo bằng lag so với `lag_slo` và nguy cơ mất binlog. P ≥ 1 thì dừng snapshot. P < 0,9 trong 3 tick thì chạy lại ở `iops.soft`.
- **`grace-v2`:** điều tốc liên tục trong khoảng [soft, hard] theo độ nhạy đo được của source. Có dither ±1% (`probe`).
- **`static`:** nhịp cố định `iops.soft`, không breaker. Là chế độ dự phòng cho source PG khi thiếu quyền đọc view thống kê.
- **Breaker:** replica lag hoặc độ trễ query vượt ngưỡng thì cắt snapshot. Mở lại sau 60 giây liên tục dưới ngưỡng.
- **Tín hiệu cứng** (dừng snapshot ngay trong tick): `Innodb_buffer_pool_wait_free` hoặc `Innodb_log_waits` tăng trên source MySQL, hoặc spool local đạt 95% `spool.hard`.
- **Trong bootstrap:** lag vượt `lag_slo` thì log `bootstrap_retention_risk` và **ưu tiên** snapshot thay vì dừng, để không tự khóa chết.

| Mục đích | Thao tác | Hiệu lực |
| --- | --- | --- |
| Giảm đọc trên source khi snapshot | `throttle set --source-iops N` | Ngay |
| Nhường source giờ cao điểm | `pause --snapshot` / `resume --snapshot-only`, hoặc `bootstrap.logical.windows` | Ngay / khởi động lại |
| Governor nhạy hơn | `governor.lag_slo`, `governor.tick` | Khởi động lại |
| Target bắt kịp nhanh hơn | `commit_order: relaxed`, tăng `workers.max` | Khởi động lại |
| Ít commit hơn | `target.batch.max_rows` / `max_bytes` | Khởi động lại |
| Đệm khi target chậm | `spool.hard`, `spool.overflow` | Khởi động lại |
| Không chạm primary (MySQL) | Trỏ source vào replica | Khởi động lại |

Bộ nhớ: không có trần RSS (`resources.memory` chưa thi hành). Các phần lớn được tràn ra đĩa:

- Assembler tràn theo **số row** (handoff dòng 30).
- Reader PG giữ Txn streaming/2PC trong RAM tới tổng 64 MiB, vượt thì tràn ra `<spool.path>/pg-reader-<name>`.
- Chunk snapshot có trần byte; đọc vượt trần thì chunk được tách.

### 8.2 Metric (`GET /metrics` trên `observability.metrics`)

`GET /healthz` trả 200 khi vòng chính còn tiến (liveness).

| Metric | Nhãn | Ý nghĩa |
| --- | --- | --- |
| `strand_lag_seconds` | pipeline, phase (`bootstrap`\|`synced`) | Độ trễ. Xem 10.3: có thể báo 0 khi reader đứng sau source |
| `strand_apply_txn_total`, `strand_apply_rows_total` | pipeline | Số Txn và row đã apply |
| `strand_holes` | pipeline | Số lỗ chưa lấp (relaxed) |
| `strand_checkpoint_compaction_lag_seconds` | pipeline | Tuổi lần gộp checkpoint gần nhất (strict: 0) |
| `strand_writeset_entries` | pipeline, table | Số khóa đang giữ trong writeset |
| `strand_pending_cross_range`, `…_oldest_seconds` | pipeline | Đổi PK xuyên range đang chờ đọc lại |
| `strand_governor_rate_tokens` | pipeline, resource | Tốc độ cấp token |
| `strand_governor_zone` | pipeline | 0 Green, 1 Yellow, 2 Red |
| `strand_governor_pressure` | pipeline, signal | Áp suất theo tín hiệu |
| `strand_sensitivity_unknown` | pipeline | GRACE chưa đo được độ nhạy (0/1) |
| `strand_breaker_open` | pipeline | Breaker đang mở (0/1) |
| `strand_spool_bytes`, `strand_spool_capacity_bytes` | pipeline, tier | Spool đang dùng / trần |
| `strand_snapshot_ranges` | pipeline, table, state | Số range snapshot theo trạng thái |
| `strand_seed_progress_ratio` | pipeline, table | Tiến độ bootstrap từng bảng (0..1) |
| `strand_retention_headroom_seconds` | pipeline | Source MySQL, chỉ trong bootstrap: thời gian còn lại trước khi binlog cần đọc bị xóa |
| `strand_verify_mismatch_total`, `strand_verify_unverifiable_hot_total` | pipeline, table | Row lệch / row quá nóng khi verify |
| `strand_fenced_total` | pipeline | Số lần bị instance khác thay thế |

Cảnh báo nên đặt:

- Lag vượt `lag_slo` kéo dài.
- `retention_headroom` sắp về 0.
- Spool trên 80% trần.
- `verify_mismatch_total` tăng.
- Process thoát với exit 2–5 hoặc 10.

### 8.3 Exit code

Exit 1 là lỗi tạm, nên tự restart. Exit 2–5 và 10 cần người xem.

| Exit | Mã | Nguyên nhân | Xử lý |
| --- | --- | --- | --- |
| 0 | — | Thành công / dừng bằng tín hiệu | — |
| 1 | (không phân loại) | Mất kết nối, lỗi tạm | Tự restart; xem log nếu lặp lại |
| 2 | `invalid_syntax`, `invalid_connection`, … | Cấu hình hoặc cờ sai, khóa lạ | Sửa cấu hình |
| 3 | `binlog_config_invalid` | Biến binlog/GTID sai; PG `wal_level`, publication partition | Sửa theo thông báo (mục 4) |
| 3 | `missing_pk` | Bảng không có PK | Thêm PK hoặc loại bảng |
| 3 | `type_unmappable` | Kiểu không ánh xạ được; PK không snapshot logic được | Mục 3.2, 5.2 |
| 3 | `collation_looser` | Collation khóa ở target lỏng hơn source | Dùng collation chặt hơn (PG: `"C"`) |
| 3 | `seed_invariant_failed`, `prerelease_tool` | `seed plan`/`apply` từ chối | Sửa đúng điều kiện SD nêu ra |
| 3 | `schema_change_pending` | `validate` với `create_schema: off` mà thiếu bảng | `schema init` |
| 4 | `pipeline_locked`, `fenced` | Instance khác cùng `name` đang chạy hoặc đã thay thế; `repair fix` khi `sync` đang chạy | Bảo đảm chỉ một instance |
| 5 | `log_gap` | Source đã xóa log chưa apply; slot PG không tồn tại | Cấp lại từ archive (`--archive`), hoặc seed lại |
| 5 | `prefetch_exhausted` | Spool prefetch đầy | Tăng spool, thêm tầng bucket |
| 5 | `verify_mismatch` | Verify thấy lệch; UPDATE TOAST mà row không có ở target | Quy trình repair (7.8), điều tra nguyên nhân |
| 10 | `schema_change_pending` | DDL trên bảng của pipeline mà target chưa khớp | Áp DDL ở target rồi chạy lại |
| 10 | `promote_needs_decision` | Promote không fence được source | Cô lập source, chạy lại với `--source-fenced-externally` |

---

## 9. Công thức đầu-cuối

### (a) MySQL→MySQL cả instance, bootstrap logic

```bash
export SRC_PW=... DST_PW=...
cat > all.yaml <<'EOF'
version: 1
name: all-sync
source: { kind: mysql, host: replica1, user: strand, password_env: SRC_PW }
target: { kind: mysql, host: newdb1, user: strand, password_env: DST_PW, database: strand }
tables: { include: ["*.*"], exclude: ["tmp.*"] }
resources: { spool: { path: /var/lib/strand/all-sync } }
observability: { admin_socket: /run/strand/all-sync.sock, metrics: ":9464" }
EOF
mysql -h newdb1 -e 'CREATE DATABASE IF NOT EXISTS strand'
strand validate -c all.yaml
strand plan -c all.yaml          # ETA quá 50% retention: chuyển sang (b)
strand sync -c all.yaml          # tự tạo schema, snapshot, stream
strand top -c all.yaml           # đợi state = synced
for t in $(mysql -h replica1 -N -e "SELECT CONCAT(table_schema,'.',table_name) FROM information_schema.tables WHERE table_schema NOT IN ('mysql','sys','information_schema','performance_schema','tmp') AND table_type='BASE TABLE'"); do
  strand verify -c all.yaml --mode full --table "$t"
done
```

### (b) MySQL→MySQL seed bằng XtraBackup

```bash
xtrabackup --backup --target-dir=/bk && xtrabackup --prepare --target-dir=/bk
strand seed plan -c p.yaml --type xtrabackup --backup /bk --source "$SRC_DSN" --out seed.json
strand seed apply -c p.yaml --manifest seed.json --target "$DST_DSN" --source "$SRC_DSN" \
  --spool /var/lib/strand/p/prefetch --restore-cmd "/opt/restore-xtrabackup.sh /bk"
mysql -h newdb1 -e 'SET SESSION sql_log_bin=0; CREATE DATABASE IF NOT EXISTS strand; SET SESSION sql_log_bin=1;'
strand sync -c p.yaml --start-at seed --seed-manifest seed.json   # verify sau seed trong post_seed_window
```

### (c) MySQL→PG cả database

```bash
# p.yaml: source kind mysql; target kind postgres, dsn postgres://...; tables.include ["app.*"]
strand schema init -c p.yaml --dry-run    # xem DDL theo typemap
strand validate -c p.yaml                 # kiểu, collation khóa, giá trị
strand sync -c p.yaml
strand verify -c p.yaml --mode full --table app.orders   # từng bảng
```

Bảng có PK chuỗi `_ci`/`_bin` sang `COLLATE "C"` được snapshot logic theo tập khóa chính xác và verify full được (mục 5.2). Bảng có PK mà mục 5.2 từ chối: (1) để bảng đó rỗng lúc bắt đầu; hoặc (2) nạp dữ liệu tại một GTID đã biết, rồi dùng `--start-at` cho pipeline riêng của bảng đó.

### (d) PG→PG seed bằng `strand export`

```bash
psql "$SRC" -c "CREATE PUBLICATION strand_billing FOR TABLE public.orders, public.users WITH (publish='insert, update, delete')"
strand export --source "$SRC" --slot strand_billing --tables public.orders,public.users \
  --out /backup/billing -F d -j 8 --manifest seed.json
pg_restore -j 8 -d "$DST" /backup/billing
strand seed apply -c billing.yaml --manifest seed.json --target "$DST"
strand sync -c billing.yaml --start-at seed --seed-manifest seed.json
strand verify -c billing.yaml --mode full --table public.orders
```

### (e) Cutover / promote

1. `strand top`: lag ổn định nhỏ, state `synced`.
2. `verify --mode full` từng bảng, khi tải thấp. Lệch thì dùng quy trình repair ở 7.8.
3. Ứng dụng vào chế độ bảo trì, hoặc chấp nhận vài giây chỉ đọc.
4. `strand promote …` (7.10) khi `sync` vẫn chạy.
5. Trỏ ứng dụng sang target, rồi dừng `sync`.
6. Không chạy lại `sync` cũ: source đã bị fence.

---

## 10. FAQ, xử lý sự cố, giới hạn đã biết

### 10.1 FAQ

**Có phải liệt kê từng bảng không?** Không. `app.*` là cả database, `*.*` là cả instance (mục 2).

**Tại sao bảng của tôi bị từ chối snapshot logic?** PK của nó có kiểu hoặc collation mà Strand không chứng minh được thứ tự (mục 5.2). Thông báo lỗi nêu cột và lý do. Cách xử lý: seed từ backup, hoặc đổi collation (PG: `COLLATE "C"`).

**Đổi `commit_order` giữa chừng được không?** Được: dừng, sửa, chạy lại. Checkpoint tương thích.

**Kill -9 có an toàn không?** Có. Checkpoint commit cùng dữ liệu trong target. Snapshot dở được tiếp tục, phần chunk chưa đọc được xóa trước khi stream ghi lại.

**Đọc từ replica MySQL?** Trỏ `source` vào replica có `log_replica_updates=ON`. `promote --fence` vẫn trỏ vào primary cũ.

### 10.2 Xử lý sự cố

| Triệu chứng | Nguyên nhân thường gặp | Xử lý |
| --- | --- | --- |
| validate exit 3 liệt kê thread | Session cũ còn `binlog_format`/`row_image` cũ | Cho ứng dụng kết nối lại; `STOP/START REPLICA` |
| sync exit 5 `log_gap` ngay khi khởi động (PG) | Slot chưa có, `create_slot: false` | Tạo slot hoặc đặt `create_slot: true` |
| sync exit 3 về publication partition | Thiếu `publish_via_partition_root` | Chạy lệnh `ALTER PUBLICATION` trong thông báo |
| sync dừng vì TRUNCATE (PG) | Publication phát truncate | `ALTER PUBLICATION … SET (publish = 'insert, update, delete')` |
| exit 10 `schema_change_pending` | DDL ở source | Áp cùng DDL ở target, chạy lại |
| `verify --repair fix` exit 4 | `sync` đang chạy | Dừng `sync` trước (7.8) |
| repair báo `the source changed while repairing` | Source đổi UNIQUE trong lúc repair | Để pipeline bắt kịp, dừng, repair lại |
| `verify --mode full` báo `must be schema.table (no wildcard)` | `tables.include` có wildcard | Dùng `--table` cho từng bảng |
| Snapshot đứng yên | Governor dừng (zone Red), breaker mở, ngoài `windows`, `pause --snapshot` | `strand top`; xem `strand_governor_*`, `strand_breaker_open` |
| Lag tăng mãi | Target ghi chậm hơn source | `relaxed` + tăng `workers.max`; kiểm IO target |

### 10.3 Giới hạn đã biết

- **Lag:** `strand_lag_seconds` và `top` báo 0 khi scheduler hết việc, kể cả khi reader đang đứng sau source (ví dụ mạng treo). Chưa có heartbeat (handoff dòng 48). Hãy theo dõi thêm `apply_txn_total` có tăng không.
- **Verify sau seed ngoài MySQL→MySQL:** chỉ ghi cảnh báo rồi bỏ qua, pipeline vẫn báo `synced` dù chưa verify (handoff dòng 52). Hãy tự chạy `verify --mode full`.
- **`validate` không kiểm PG source** (dòng 33) và không kiểm quyền.
- **`target.batch.max_wait`** làm khóa nóng chậm lại ở `strict` (dòng 26); `relaxed` gom chuỗi Txn phụ thuộc vào cùng batch.
- **PG identity `ALWAYS`:** PK bị `UPDATE … SET id = DEFAULT` kèm cột TOAST không đổi làm writer lỗi 428C9 (dòng 27, hiếm).
- **Source PG:** governor `onoff`/`grace-v2` dùng tín hiệu PG (replica lag của standby đang replay, WAL slot); thiếu quyền thì lùi về `static`.
- **Không có:** đồng bộ hai chiều, DDL tự động (`schema.*`), verify full theo lịch, HA tự failover của chính Strand, tự co giãn worker, mask cột, đọc từ PG standby.
- **Hiệu năng:** đo trên máy test container, khoảng 2.100 txn/s relaxed và 1.600 txn/s strict (MySQL→MySQL). Chưa có benchmark Gate 2.

### 10.4 Quyết định đã chốt trong code, chờ cập nhật spec

Chi tiết nằm trong `questions/DECISIONS-2026-11-02.md` và `docs/HANDOFF-CODEX.md`.

| # | Nội dung | Phần spec |
| --- | --- | --- |
| 1A | `strict`: một worker; row toàn cục ghi trong transaction commit | 02 §4, §6 |
| 2B | Snapshot logic: ban đầu chỉ PK một cột số nguyên; nay mở rộng theo mục 5.2 (việc C) | 02 §5, 01 §5 |
| 3B | `pause --snapshot`, `resume --snapshot-only` | 02 §10 |
| 4A | Giữ bảng `_strand_promote` | 02 §4 |
| 5 | `foreign_key_checks=0` cho phiên ghi; KILL kết nối instance cũ | 02 §8, 03 §2 |
| 6 | Cờ và khóa cấu hình mới | 02 §10 |
| 7 | Failover: exit 10 hiển thị phần dữ liệu sẽ mất, xác nhận bằng `--source-fenced-externally` | 02 §8, 05 §5 |
| 8 | `--type mysqldump` kèm cảnh báo bắt buộc | 02 §9 |
| 9 | mydumper bắt buộc `--dump-log` | 02 §9, §10 |
| 10 | RS-1: chờ GTID của event đã bỏ trước SELECT chunk | 02 §5 |
| 11 | validate kiểm từng session đang mở | 02 §11, 03 §2 |
| 12 | DDL: đi tiếp khi schema target khớp; xét bảng mới | 02 §12 |
| 13 | Mặc định group commit; `workers.min` chưa có | 02 §6, §10 |
| 14 | Soak trước Gate 1: 12 giờ thay vì 72 giờ | 04 §1 |
| 17 | `bootstrap.create_schema: auto` mặc định | 02 §10 |
| — | `verify --repair fix` yêu cầu pipeline dừng và target đã bắt kịp (review R04); runbook spec cần cập nhật | 05 |
