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
| `verify --mode sample` | Có | Có (PK một cột số nguyên) | Có (PK một cột số nguyên) |
| `verify --mode delta` / verify tự động sau seed | Có | Có | Có |
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
2. Mọi bảng phải có khóa hàng: PRIMARY KEY, hoặc UNIQUE index NOT NULL (mục 5.1). Bảng không có khóa làm `sync`/`validate` dừng với `missing_pk`, kèm lệnh `ALTER TABLE` gợi ý cho từng bảng (mục 5.1.2).
3. Kiểm từng bảng có PK snapshot logic được (mục 5.2). Bảng không hỗ trợ: seed từ backup hoặc đổi kiểu PK.
4. User, quyền, view, routine, trigger và event **không** được sao chép. Tạo tay ở target. Seed bằng XtraBackup/CLONE thì backup mang theo chúng.
5. PG source: publication phải chứa mọi bảng (`FOR ALL TABLES` hoặc liệt kê), phát INSERT/UPDATE/DELETE và không lọc dòng hay bỏ cột dữ liệu. Bảng partitioned cần `publish_via_partition_root = true`. Dừng pipeline trước khi đổi publication. PostgreSQL không gửi các thay đổi đã bị lọc, nên reader không thể phát hiện chúng từ stream.
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

### 3.4 DDL về sau (schema evolution)

Với source MySQL hoặc PostgreSQL, Strand so schema bảng ở source (sau DDL với MySQL hoặc sau message Relation mới với PG) với bảng ở target tại barrier DDL (sau khi drain) và phân loại từng khác biệt:

| Loại | Gồm | Hành vi |
| --- | --- | --- |
| **Cộng thêm** (additive) | `ADD COLUMN` nullable hoặc có `DEFAULT`; nới kiểu: `VARCHAR`/`VARBINARY` dài hơn, số nguyên sang số nguyên rộng hơn **cùng dấu**, `DECIMAL` tăng precision cùng scale, họ `TEXT`/`BLOB` lớn hơn, `NOT NULL` → `NULL`; thêm index thường (không unique) | `schema.additive: auto` (mặc định): Strand sinh DDL cho target (MySQL, hoặc PG theo typemap, có áp `tables.rename`), chạy, nạp lại metadata và đi tiếp. Ghi log `additive schema change applied to target` |
| **Phá hủy** (destructive) | `DROP COLUMN`, đổi tên cột (= drop + add), thu hẹp hoặc đổi họ kiểu (kể cả `INT UNSIGNED` → `BIGINT`), `NULL` → `NOT NULL`, `ADD COLUMN NOT NULL` không default, đổi PK, thêm index unique, bỏ/đổi index, đổi sang charset mà cột đích không chứa nổi (đích `utf8mb4` chứa mọi charset nên không tính; chỉ khác collation cũng không tính, như trước P3), cột generated hoặc `AUTO_INCREMENT`, đổi thứ tự cột | Dừng exit 10 `schema_change_pending` |

Không chắc thì coi là phá hủy. `schema.additive: pause` đưa cả thay đổi cộng thêm vào luồng dừng và duyệt. `schema.destructive: pause|approve` hiện cùng hành vi (luôn dừng và chờ `schema approve`).

Khi dừng, Strand ghi một dòng vào bảng `_strand_schema_change` ở target (id, pipeline, vị trí DDL ở source, DDL gốc, DDL đề xuất cho target, phân loại, trạng thái `pending` / `approved` / `applied`, thời điểm tạo) rồi thoát với thông báo chỉ cách xử lý:

```bash
strand schema diff -c pipeline.yaml              # liệt kê thay đổi đang chờ cùng DDL đề xuất (--all: gồm cả đã áp)
strand schema approve 3 -c pipeline.yaml --dry-run   # chỉ in DDL sẽ chạy
strand schema approve 3 -c pipeline.yaml         # chạy DDL đề xuất ở target theo thứ tự, đánh dấu applied
strand sync -c pipeline.yaml                     # chạy tiếp: schema target đã khớp
```

- `approve` idempotent: id đã `applied` thì không làm gì và báo `already applied`. Chạy lại `sync` khi chưa duyệt vẫn dừng và dùng lại cùng bản ghi (không ghi trùng).
- PG: toàn bộ DDL của một lần `approve` và việc đánh dấu chạy trong một transaction. MySQL: DDL tự commit từng câu; nếu bị ngắt giữa chừng, trạng thái là `approved`, câu đã áp sẽ báo lỗi trùng khi chạy lại — áp nốt phần còn lại bằng tay rồi chạy `sync` (Strand so schema và đi tiếp).
- Dòng đề xuất bắt đầu bằng `-- MANUAL:` là thay đổi Strand không tự áp (đổi PK, đổi thứ tự cột, index prefix/fulltext, cột generated...): `approve` từ chối; áp tay ở target rồi chạy lại `sync`. Source MariaDB: mọi thay đổi đều ở dạng này.
- Vẫn có thể áp DDL tay ở target trước khi chạy lại như trước đây; khi schema khớp, bản ghi chờ tương ứng được đóng.
- **Giới hạn đã biết:** so sánh dùng schema source tại lúc Strand xử lý DDL, không phải đúng tại vị trí DDL trong log. Nếu source đã có thêm DDL sau đó thì diff có thể gồm cả thay đổi sau: thay đổi cộng thêm áp sớm vô hại, thay đổi phá hủy thì dừng chờ duyệt. Với PG target, kiểu rộng hơn typemap (schema do DBA tự tạo) được coi là khớp; index chỉ có ở target không bị coi là `DROP INDEX`; cột mới của source luôn được nối cuối bảng PG và thứ tự cột không được so.
- `CREATE TABLE` khớp bộ lọc thì được tạo ở target tại barrier, trước row đầu tiên. Strand giữ metadata lock đọc và kiểm binlog để không lấy schema đã bị ALTER sau đó. Không kiểm được trong 30 giây thì dừng exit 10.
- Bảng `RENAME` từ ngoài bộ lọc vào, hoặc `DROP`/`RENAME` bảng của pipeline, dừng exit 10 như trước (áp tay ở target rồi chạy lại; bảng rename-vào được snapshot khi chạy lại).
- **Source PG:** DDL không đi qua logical replication; Strand phát hiện thay đổi schema khi nhận message `Relation` mới từ pgoutput khác với metadata hiện có. Strand tạo barrier tại đúng vị trí trong luồng ngay trước row đầu tiên dùng schema mới, drain các transaction trước đó, rồi phân loại qua schemagen (cộng thêm thì tự áp lên target PG, phá hủy thì ghi `_strand_schema_change` và dừng exit 10 chờ `strand schema approve`). Lưu ý: Relation chỉ được server PG gửi trước row đầu tiên sau DDL; nếu ADD COLUMN chưa có row nào sau đó thì chưa phát hiện, chấp nhận.

**Online DDL (gh-ost, pt-osc; P3-02).** Strand nhận diện theo dấu hiệu của tool (bảng gốc `t` phải tồn tại cùng schema):

- Bảng tạm `_t_gho`, `_t_ghc`, `_t_del` (gh-ost; kể cả `_t_<14 số>_del` của `--timestamp-old-table`), `_t_new`, `_t_old` (pt-osc), tên bị cắt khi quá dài, và trigger `pt_osc_*`: mọi DDL chỉ đụng chúng bị bỏ qua (không drain, không tạo ở target, không dừng); event row của chúng không được ghi và không được snapshot; phần row của `t` trong cùng transaction (trigger pt-osc) vẫn được ghi.
- Cut-over `RENAME TABLE t TO _t_del, _t_gho TO t` (hoặc `_t_old` / `_t_new`) được coi là MỘT `ALTER` trên `t`: đi qua đường ở trên (cộng thêm thì áp một lần lên target tại barrier, phá hủy thì dừng chờ `approve`). Log: `online DDL cut-over detected` rồi `online DDL cut-over processed at barrier`.
- Chuỗi không khớp mẫu (bảng `_gho` bỏ dở, gh-ost bị hủy, RENAME lạ) không bao giờ làm mất row của `t`: bảng tạm vẫn bị bỏ qua, phần còn lại theo hành vi cũ.
- Giới hạn: `pt-osc --drop-swap` (DROP `t` rồi RENAME) và tên bảng tạm tùy biến (`--new-table-name`, `--force-table-names`) không nhận diện được trừ khi khai báo `extra_patterns`; ALTER nhiều cột sinh nhiều câu `ALTER` ở target (một câu mỗi thay đổi); bảng thường tên `_x_new`/`_x_old` khi `x` cũng thuộc pipeline bị coi là bảng tạm (tắt bằng `tools: []`).

```yaml
schema:
  online_ddl: { tools: [gh-ost, pt-osc], extra_patterns: ["_{table}_tmp"] }   # mặc định cả hai tool; tools: [] tắt nhận diện
```

Cấu hình:

```yaml
schema: { additive: auto, destructive: pause }   # additive: auto | pause ; destructive: pause | approve
```

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
| `strand` | Source MySQL/MariaDB | `REPLICATION SLAVE, REPLICATION CLIENT ON *.*` (MariaDB 10.5.2+: `REPLICATION CLIENT` = `BINLOG MONITOR`); `SELECT ON app.*` | Đọc binlog, snapshot, verify |
| `strand` | Source không GTID (MariaDB, MySQL 5.x `gtid_mode=OFF`) | `LOCK TABLES ON app.*` | Snapshot logic ở chế độ vị trí binlog |
| `strand` | Source MySQL (không phải MariaDB) có `bootstrap.auto_seed` | `RELOAD ON *.*` | `FLUSH TABLES WITH READ LOCK` ngắn để mở ảnh nhất quán. MariaDB không cần |
| `strand` | Source (khuyến nghị) | `SELECT` trên `performance_schema.variables_by_thread`, `performance_schema.threads`; `PROCESS ON *.*`; MariaDB 10.5+: `SLAVE MONITOR ON *.*` | validate kiểm session; log liệt kê giao dịch mở lâu khi snapshot đứng; tín hiệu replica lag. Thiếu thì chỉ cảnh báo |
| `strand` | Target MySQL | `SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, REFERENCES ON app.*` và trên schema metadata; `CONNECTION_ADMIN ON *.*` | Apply, bảng `_strand_*`, ghi khi `read_only=ON`, ngắt instance cũ. `DROP` còn dùng cho `TRUNCATE` khi auto-seed trên Group Replication làm lại lần nạp dở; `REFERENCES` cho FK |
| `strand` | Target MySQL có FK chéo database | `SELECT` trên database chứa bảng con | Strand phải nhìn thấy mọi FK tham chiếu bảng được đồng bộ |
| tài khoản chạy `strand objects` | Source MySQL/MariaDB | `SHOW VIEW, TRIGGER, EVENT ON app.*`; `SELECT ON mysql.*` | Đọc view, trigger, event, định nghĩa routine, user và quyền. Thiếu `TRIGGER`/`EVENT` thì số đếm ra 0 mà không báo lỗi |
| tài khoản chạy `strand objects` | Target MySQL | `CREATE VIEW, SHOW VIEW, CREATE ROUTINE, ALTER ROUTINE, TRIGGER, EVENT ON app.*`; `--phase users`: `CREATE USER ON *.*`, `GRANT OPTION ON app.*` và các quyền sẽ cấp cho user ứng dụng | Đích bật binlog: thêm `SET PERSIST log_bin_trust_function_creators=ON` (hoặc `SUPER`), thiếu thì function/trigger lỗi 1419. Có thể dùng tài khoản quản trị qua `--source-dsn`/`--target-dsn` |
| `strand` | Target PG | `CONNECT, TEMPORARY` trên database; `USAGE, CREATE` trên schema; `SELECT, INSERT, UPDATE, DELETE` trên bảng; `UPDATE` trên sequence | Apply, `_strand_*`, setval khi promote |
| `strand_admin` | Source và target MySQL | `SYSTEM_VARIABLES_ADMIN ON *.*` | Chỉ `promote` |
| (PG promote) | Source và target PG | Quyền `ALTER SYSTEM`, `ALTER ROLE`, `pg_terminate_backend`, GRANT/REVOKE trên bảng của ứng dụng | Chỉ `promote` |

Để tạo schema, target cần quyền CREATE trên database/schema đích (và quyền tạo database nếu chưa có). `validate` chỉ kiểm vài quyền phụ (`SLAVE MONITOR`, đọc `performance_schema`) và in cảnh báo; thiếu các quyền khác chỉ lộ ra khi chạy.

Target MySQL là Group Replication / InnoDB Cluster: không cần thêm quyền cho `strand`. DBA cần bật `SET PERSIST sql_generate_invisible_primary_key=ON` trên mọi member nếu phạm vi có bảng không khóa, và trỏ Strand vào member primary. Chi tiết: [MARIADB-MIGRATION.md](MARIADB-MIGRATION.md) mục A bước 3.

Strand không ghi gì vào source khi đồng bộ, với hai ngoại lệ:

- `promote` fence source cũ.
- PG snapshot logic gọi `pg_logical_emit_message`. Hàm này chỉ ghi một message vào WAL, không ghi bảng.

Metadata `_strand_pipeline`, `_strand_checkpoint`, `_strand_worker_delta` và `_strand_promote` nằm trong database/schema của target DSN.

---

## 5. Yêu cầu và giới hạn của bảng

### 5.1 Khóa hàng: PRIMARY KEY, unique key NOT NULL, hoặc chế độ keyless

Apply dùng upsert và delete theo khóa, và việc gán thay đổi vào chunk snapshot cũng theo khóa, nên mỗi bảng cần một **khóa hàng**. Strand chọn theo thứ tự:

1. **PRIMARY KEY**, nếu có.
2. Nếu không có PK: một **UNIQUE index mà mọi cột đều NOT NULL** (MySQL gọi là "primary key equivalent"). Chọn xác định: ít cột nhất; nếu kiểu cột của index không được snapshot hỗ trợ (mục 5.2) thì thử index kế tiếp; hòa thì theo tên index. Không đủ điều kiện: index có cột prefix (`code(10)`), index biểu thức (functional), index chứa cột NULL được, index chứa cột sinh (generated).
3. Không có gì dùng được: exit 3 `missing_pk`, kèm **gợi ý cho từng bảng** (mục 5.1.2). Ngoại lệ là chế độ keyless (mục 5.1.3).

Khóa đang dùng được in ở `validate` (`table app.t: no primary key, row key is UNIQUE uk_email (email)`), ở `plan`, ở log lúc khởi động và ở cột `key:` của tab Tables (Web UI) / `admin` `TableInfo.key`. Thay đổi khóa bằng DDL được xử lý như thay đổi PK: nếu DDL làm bảng mất khóa dùng được thì `sync` dừng (exit 10 `missing_pk`, kèm gợi ý).

**Theo nguồn:**

| Nguồn | Điều kiện |
| --- | --- |
| MySQL 5.5–8.x, MariaDB | `binlog_row_image=FULL` (đã bắt buộc) nên ảnh trước/sau chứa đủ cột khóa. Cột `INVISIBLE` (MySQL 8.0.23+, MariaDB 10.3.3+) được đọc như cột thường |
| PostgreSQL | UPDATE/DELETE chỉ mang các cột của **REPLICA IDENTITY**, nên khóa phải là index của `REPLICA IDENTITY USING INDEX` (index unique, không partial, không deferrable, mọi cột NOT NULL; PG tự kiểm lúc `ALTER`). Bảng không PK mà replica identity là DEFAULT/NOTHING: `validate` exit 3 và in đúng lệnh `ALTER TABLE "s"."t" REPLICA IDENTITY USING INDEX "idx";` khi có index đủ điều kiện. `REPLICA IDENTITY FULL` không PK **không** được coi là khóa (xem keyless) |

**Ở target:** `strand schema init` / tự tạo bảng sao chép luôn unique index (MySQL→MySQL qua `SHOW CREATE TABLE`; MySQL→PG và PG→PG tạo `CREATE UNIQUE INDEX`/constraint) nên PG `ON CONFLICT (cột khóa)` và MySQL `ON DUPLICATE KEY UPDATE` hoạt động. Nếu bảng đích **đã tồn tại** mà không có unique index khớp khóa của nguồn, `validate` exit 3 (`no matching unique index/constraint`) kèm lệnh tạo index, vì upsert sẽ nhân đôi hàng. UPDATE đổi giá trị khóa chạy như UPDATE đổi PK (xóa khóa cũ, upsert khóa mới). Unique identity ở target phải khớp **đúng tập cột** của khóa nguồn; unique trên tập con không đủ vì có thể gộp hai row nguồn thành một. MySQL cũng từ chối unique cạnh tranh trên tập con đó. Kiểm tra identity không thay thế việc đối chiếu các unique constraint khác giữa source và target.

#### 5.1.2 Bảng chưa có khóa: thêm khóa

`validate`, `plan` và thông báo `missing_pk` in cho **từng** bảng thiếu khóa một lệnh sẵn sàng chạy, theo flavor/phiên bản của nguồn:

| Nguồn | Lệnh gợi ý |
| --- | --- |
| MariaDB ≥ 10.3.3 | ``ALTER TABLE `db`.`t` ADD COLUMN `_strand_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT INVISIBLE, ADD PRIMARY KEY (`_strand_id`);`` |
| MySQL ≥ 8.0.23 | cùng ý, cột `my_row_id` (tên như GIPK). Từ 8.0.30, `SET sql_generate_invisible_primary_key=ON` làm bảng **mới** tự có khóa ẩn `my_row_id`; không đổi bảng đã có |
| MySQL/MariaDB cũ hơn | cột **hiển thị** `_strand_id`. Cảnh báo: `SELECT *` và `INSERT` không liệt kê cột sẽ thấy thêm một cột |
| PostgreSQL | nếu có unique index đủ điều kiện: `ALTER TABLE ... REPLICA IDENTITY USING INDEX ...;`. Nếu không: ``ALTER TABLE ... ADD COLUMN _strand_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;`` (thêm cột nên `SELECT *` đổi; viết lại bảng dưới khóa ACCESS EXCLUSIVE) |

Cột `INVISIBLE` không đổi `SELECT *` hay `INSERT` của ứng dụng. Strand đọc cột theo tên tường minh (binlog khớp theo vị trí cột của `information_schema`, vốn gồm cả cột ẩn); target MySQL→MySQL giữ `INVISIBLE` (qua `SHOW CREATE TABLE`), target PG tạo cột thường. Khi tự kiểm tra bằng `SELECT`, hãy nêu tên cột ẩn (`SELECT _strand_id, ...`).

Khi MySQL đích tự sinh khóa vô hình `my_row_id` còn nguồn dùng khóa UNIQUE thay cho PK, Strand vẫn dùng UNIQUE để tìm hàng. Đích chỉ được giữ `my_row_id` theo chế độ này khi đó là khóa `AUTO_INCREMENT INVISIBLE` riêng của đích và các ràng buộc UNIQUE còn lại tương thích với nguồn.

**Khóa và bảng lớn (> 1 triệu hàng):** thêm cột AUTO_INCREMENT **chặn ghi** trong lúc xây lại bảng (`ALGORITHM=COPY` hoặc INPLACE với `LOCK=SHARED`; đọc vẫn chạy), thời gian tỉ lệ kích thước bảng. `pt-online-schema-change` và `gh-ost` **không** thêm được khóa thay thế cho bảng không có PRIMARY KEY/UNIQUE nào: cả hai cần một khóa chung giữa bảng cũ và bảng mới để sao chép và theo dõi hàng. Lựa chọn: (1) chạy ALTER trong cửa sổ bảo trì; (2) chạy ALTER trên replica trước rồi chuyển vai; (3) nếu có tập cột NOT NULL vốn duy nhất, thêm UNIQUE index online (`CREATE UNIQUE INDEX ... ALGORITHM=INPLACE, LOCK=NONE`) — Strand dùng nó làm khóa hàng ngay, không cần cột mới. PG: thêm cột bigint NULL, điền theo lô, `CREATE UNIQUE INDEX CONCURRENTLY`, `SET NOT NULL`, rồi `REPLICA IDENTITY USING INDEX` (hoặc `ADD PRIMARY KEY USING INDEX`).

#### 5.1.3 Chế độ keyless (`tables.keyless: full_row`), chỉ khi dữ liệu đầu đến từ seed

Dành cho bảng **không thể** thêm khóa. Mặc định tắt (khi đó thiếu khóa là `missing_pk`). Bật bằng:

```yaml
tables:
  include: ["app.*"]
  keyless: full_row           # keyless_allow_float: true  nếu chấp nhận cột FLOAT/DOUBLE
bootstrap: { default: seed }  # hoặc start_at, hoặc per_table[bảng].mode: seed
```

Điều kiện và hành vi:

- **Chỉ với seed.** Dữ liệu đầu của bảng phải đến từ backup (`strand seed plan/apply` + `sync --start-at seed`) hoặc `--start-at` (bạn cam kết target đã khớp source). Snapshot logic của bảng keyless bị từ chối (exit 3 `missing_pk`) vì không có khóa để chia range và đối soát.
- **Nguồn MySQL/MariaDB:** `binlog_row_image=FULL` (`validate` đã bắt buộc). **Nguồn PG:** bảng phải có `REPLICA IDENTITY FULL`; nếu không `validate` exit 3 và in `ALTER TABLE ... REPLICA IDENTITY FULL;`.
- **Apply:** INSERT là INSERT thường (hai hàng giống hệt là hai hàng). UPDATE/DELETE khớp **ảnh trước** theo mọi cột, NULL-safe (MySQL `<=>`, PG `IS NOT DISTINCT FROM`), đúng **một** hàng (MySQL `LIMIT 1`, PG chọn cặp `(tableoid, ctid)` của một row để phân biệt cả các partition). Không khớp hàng nào nghĩa là target lệch: dừng với exit 5 `keyless_row_missing`, không bỏ qua im lặng. Checkpoint commit cùng dữ liệu trong một transaction target nên không có replay đôi.
- **Lập lịch:** mọi thay đổi của một bảng keyless được áp tuần tự theo thứ tự commit (một khóa xung đột chung cho bảng) ở cả `strict` và `relaxed`; bảng khác vẫn song song.
- **Hiệu năng:** mỗi UPDATE/DELETE quét bảng đích trừ khi có index hỗ trợ; hãy tạo index không-unique trên cột chọn lọc ở target. Bảng lớn dưới tải ghi cao sẽ chậm.
- **Kiểu cột bị từ chối:** `FLOAT/DOUBLE/REAL` (so bằng có thể lệch sau chuyển đổi; mở bằng `keyless_allow_float: true` nếu bạn chấp nhận), `JSON`, kiểu hình học (MySQL `geometry`, `point`...; PG `point/line/box/polygon/circle`), `json`/`xml` và `citext` của PG (kể cả domain/array chứa kiểu equality không phù hợp). Cột TEXT theo collation PAD SPACE coi `'a'` và `'a '` là bằng nhau: rủi ro lệch hiếm nhưng có.
- **Verify:** chỉ so **số hàng + checksum không phụ thuộc thứ tự** (`strand verify --mode full`); không chỉ ra hàng lệch và `--repair fix` bị từ chối (nạp lại bảng từ seed). Nguồn thay đổi giữa hai lần hash thì thử lại tối đa 5 lượt; nếu vẫn chưa ổn định, CLI trả `verify_incomplete`, không báo match. Verify theo mẫu/delta trong lúc chạy bỏ qua bảng keyless.
- Web UI / `top`: cột key của bảng hiện `full row (keyless)`.

Trong wizard **New pipeline** trên Web, chọn **Allow full-row matching for tables without a key** để ghi `tables.keyless: full_row`. Điều kiện seed hoặc Start position ở trên vẫn áp dụng.

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

Với `verify --mode full --resume`, Strand chờ target bắt kịp nếu có pipeline
đang chạy và đọc lại tất cả chunk. Trước cutover, chạy full verify mới khi nguồn
đã được chặn ghi và target đã bắt kịp. Xem cả số dòng lệch và chưa thể đối soát
trước khi mở ghi ở đích.

### 5.3 Giới hạn khác

- **UNIQUE ngoài PK:**
  - MySQL: các Txn đụng cùng giá trị unique được xếp tuần tự. Khóa chuỗi collation `_ci` được gộp theo collation. Unique index có cột prefix (ví dụ `UNIQUE(code(3))`) xếp tuần tự mọi Txn chạm index đó trong bảng.
  - PG (relaxed): bảng có UNIQUE ngoài PK, unique index biểu thức/partial hoặc exclusion constraint, mà không đặt `REPLICA IDENTITY FULL`, đi **làn tuần tự**: mọi Txn chạm bảng đó được apply đúng thứ tự commit ở một luồng riêng, gom nhiều Txn liền nhau vào một transaction ở target. Txn khác vẫn chạy song song; Txn phụ thuộc một Txn làn cũng đi vào làn. Lý do: pgoutput không gửi giá trị unique cũ nên không tự phát hiện được xung đột. Các Txn đang chờ trong làn cũng tính vào trần bộ nhớ của scheduler.
  - PG có nhiều Txn chạm bảng như vậy: làn tuần tự làm relaxed gần bằng tốc độ strict. Nếu phần lớn tải đi qua các bảng này, dùng `strict` cho đơn giản; hoặc đặt `ALTER TABLE ... REPLICA IDENTITY FULL` để bảng không phải đi làn (đổi lại WAL lớn hơn).
- **FK:** MySQL thông thường tắt kiểm FK ở phiên ghi của Strand để snapshot có thể chép các bảng độc lập. Pipeline MySQL/MariaDB → MySQL có FK cascade dùng chế độ riêng bên dưới. PostgreSQL kiểm FK và snapshot chép bảng cha trước bảng con.
- **Cascade trên MySQL/MariaDB → MySQL:** Strand hỗ trợ `ON DELETE` và `ON UPDATE` với `CASCADE` hoặc `SET NULL` khi chọn cả bảng cha và bảng con, target là InnoDB và khóa ngoại đích khớp nguồn. Writer bật kiểm tra FK ở MySQL đích, áp theo thứ tự commit ở nguồn và ghi checkpoint cùng transaction. Pipeline mới có thể dùng `bootstrap.auto_seed` với MySQL đích trống và binlog dạng file:position. Khi đã có bản sao nhất quán ở đích, dùng `--start-at seed` hoặc chỉ định vị trí GTID/binlog của bản sao. Pipeline đã có bảng hoàn chỉnh có thể tiếp tục từ checkpoint sau khi đối chiếu dữ liệu và ràng buộc đích. Giữ quy tắc FK ổn định trong thời gian chạy và trong phần binlog sẽ phát lại. Tài khoản đích phải nhìn thấy các FK tham chiếu đến bảng được đồng bộ, kể cả từ schema khác, để Strand đối chiếu phạm vi cascade. Nhóm cascade cần một bản nạp nhất quán. Với auto-seed, các bảng độc lập bên ngoài nhóm vẫn có thể dùng snapshot logic. `snapshot add` và prefetch chưa dùng được cho pipeline có cascade. Dùng `verify --mode full` để kiểm cả các dòng con bị cascade, vì chúng không có sự kiện riêng để verify delta nhận diện.
  - Khi phát lại DDL tạo hoặc thay đổi FK trên bảng đang đồng bộ, Strand có thể dừng tại `schema_change_pending` và giữ checkpoint trước DDL. Nếu gặp chặn do cascade lịch sử, khởi tạo pipeline mới từ seed nhất quán tại vị trí sau thay đổi.
  - Nếu một UPDATE hoặc DELETE có thể tác động tới cùng bảng con qua nhiều ràng buộc cascade, kết quả phụ thuộc thứ tự InnoDB áp các ràng buộc. MariaDB và MySQL đều áp theo tên ràng buộc. Vì vậy Strand đối chiếu tên và định nghĩa tất cả FK trong nhóm nhạy với thứ tự, kể cả `RESTRICT` và `NO ACTION`. Nhóm có thể trải qua nhiều database. Nhóm độc lập không nhạy với thứ tự vẫn có thể đổi tên bằng `tables.rename`. Khi pipeline có cascade, Strand cũng từ chối FK chỉ tồn tại ở đích mà chạm tới một bảng đang được đồng bộ. Pipeline không có cascade nào ở nguồn ghi với `foreign_key_checks=0`, nên FK ở đích không tác động tới dữ liệu đồng bộ và không bị kiểm.
  - Khi FK cascade ở đích thiếu hoặc khác nguồn, thông báo lỗi nêu điểm khác: quy tắc, cột, collation hoặc tên. Nếu bảng đích thiếu hẳn FK, thông báo kèm lệnh `ALTER TABLE ... ADD CONSTRAINT` cần chạy. Với `bootstrap.auto_seed` và `create_schema: auto`, Strand tự thêm FK còn thiếu vào bảng đích đang trống rồi mới nạp dữ liệu.
- **Cascade trên MySQL → PostgreSQL.** Chọn cả bảng cha và bảng con. Strand kiểm FK trên bảng đích có sẵn và kiểm lại sau khi tự tạo schema. FK phải khớp cột và hành động, đang được thực thi và có cùng ngữ nghĩa so khóa. Strand giữ thứ tự commit của nguồn và dừng trước row event ghi với `foreign_key_checks=0`. Khóa văn bản, đường cascade chồng lấn và FK PostgreSQL trên bảng phân vùng được từ chối khi Strand không chứng minh được hành vi tương đương. Khi nạp ban đầu bằng snapshot logic, giữ các ghi gây cascade tạm dừng cho đến khi bảng con nạp xong, hoặc dùng seed nhất quán. Dùng verify full cho các dòng con bị cascade.
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
| `schema.additive` | `auto` | `auto`: tự áp thay đổi schema cộng thêm lên target (mục 3.4); `pause`: dừng chờ `schema approve` |
| `schema.destructive` | `pause` | `pause` \| `approve`: thay đổi phá hủy luôn dừng exit 10 và chờ `strand schema approve <id>` |
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
| `bootstrap.logical.max_parallel` | 1 | Số bảng snapshot logic **cùng lúc** (1–32). Chunk của một bảng vẫn đọc tuần tự; các bảng khác nhau chạy song song và cùng chia **một** ngân sách đọc nguồn (`iops.soft/hard`). Bảng lớn được bắt đầu trước. Source PostgreSQL: bỏ qua (luôn 1, có cảnh báo). Mục 8.1 |
| `bootstrap.logical.windows` | rỗng (mọi lúc) | Khung giờ cho phép snapshot, ví dụ `["22:00-06:00"]` (giờ địa phương của process; qua nửa đêm được). Khi `bootstrap_retention_risk` bật, snapshot chạy cả ngoài khung |
| `bootstrap.create_schema` | `auto` | Mục 3.1 |
| `bootstrap.auto_seed` | `false` | Tự nạp bảng không khóa và nhóm FK cascade trong một ảnh nhất quán rồi chạy CDC. Cần nguồn họ MySQL, đích MySQL, `tables.keyless: full_row`, `bootstrap.default: logical`, không `start_at`/`per_table`/che cột. Nguồn MySQL cần `RELOAD`. Chi tiết: [MARIADB-MIGRATION.md](MARIADB-MIGRATION.md) "Tự nạp bảng thiếu khóa và nhóm khóa ngoại" |
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
| `tables.keyless` | tắt | `full_row`: cho bảng không có khóa chạy keyless, chỉ khi dữ liệu đầu từ seed/`--start-at` (mục 5.1.3) |
| `tables.keyless_allow_float` | `false` | Cho phép cột FLOAT/DOUBLE trong bảng keyless |
| `verify.post_seed_window` | `30m` | Cửa sổ verify sau seed / `--start-at`: sau khi đích bắt kịp, verify mẫu ~1% rồi verify mọi khóa CDC chạm trong cửa sổ này. Bắt kịp lâu hơn cửa sổ thì bằng chứng verify báo `incomplete` (đang chờ bắt kịp) nhưng vẫn chờ tiếp, và vẫn verify cả sau restart |
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
- `checkpoint.store` khác `target`: checkpoint luôn nằm ở target.
- `governor.metrics_source`
- `verify.full.cron`, `verify.full.rate`: không có verify full theo lịch; chạy `strand verify --mode full` bằng cron ngoài.
- `bootstrap.catch_up.binlog_archive` (dùng `seed apply --archive`).

Không cảnh báo khi giá trị đúng là hành vi thật: `read_from: primary`, `checkpoint.store: target`.

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

Tùy chọn thêm (không đặt thì dùng mặc định của Strand, file ghi dạng comment): `--exclude a.log_*,b.tmp`, `--source-user`, `--target-user`, `--server-id`, `--commit-order relaxed|strict`, `--iops-soft N` (`--iops-hard`, mặc định gấp đôi soft), `--workers N`, `--chunk-rows N` (cỡ chunk cố định; mặc định tự chỉnh), `--max-parallel N` (số bảng snapshot cùng lúc, 1–32; mặc định 1), `--snapshot-window 22:00-06:00`, `--breaker-max-ratio X`, `--replica-lag-max 30s`, `--metrics host:port`, `--log-format json|text`.

Ví dụ chuyển cả server MariaDB, đọc nhanh hơn mặc định:

```bash
strand config init --from mariadb --to mysql --name sea --database '*' --exclude 'call_logs.tmp_*' \
  --source-host 10.5.0.21 --target-host 10.5.0.31 --iops-soft 4000 --workers 16 --chunk-rows 20000 --max-parallel 4 -o sea.yaml
```

### 7.0b `sync --background`, `stop`, `status`

`strand sync -c p.yaml --background|-d [--log-file f] [--pid-file f]`: chạy nền (tách khỏi terminal), log và PID mặc định trong `resources.spool.path`; chờ 5 giây để báo lỗi khởi động sớm. Pipeline đang chạy nền thì từ chối chạy lần hai. `strand stop -c p.yaml [--timeout 150s]`: gửi SIGTERM và chờ dừng (không cần biến môi trường mật khẩu). `strand status -c p.yaml`: còn chạy hay không và 5 dòng log cuối (exit 1 nếu không chạy).

### 7.0c `top`

Khi đang snapshot (`Trạng thái: bootstrapping`), `strand top` hiện số bảng xong/tổng, bảng đang chạy, số row đã chép / ước lượng (thống kê của nguồn), tốc độ snapshot (~60 s gần nhất), % và ETA. Breaker đang mở thì hiện `Breaker: MỞ` kèm lý do (snapshot tạm dừng để bảo vệ nguồn); log có dòng `source circuit breaker OPEN` / `closed`. `top` chỉ cần `-c` (không cần biến môi trường mật khẩu).

### 7.1 `validate`

`strand validate -c p.yaml`. Exit 0 khi đạt, exit 3 kèm lý do khi bị từ chối. Cảnh báo in ra stderr.

Nó kiểm:

- **Cấu hình:** kind, `include` không rỗng, `commit_order`, `write_mode`, khóa chưa có tác dụng.
- **Source MySQL:**
  - Biến binlog/GTID global và từng session.
  - Bảng tồn tại và có khóa hàng (PK hoặc unique NOT NULL); bảng thiếu khóa được gom vào một lỗi `missing_pk` kèm gợi ý cho từng bảng. Bảng đích đã tồn tại phải có unique index khớp khóa khi nguồn không có PK.
  - Với `tables.keyless: full_row`: điều kiện seed, kiểu cột, `REPLICA IDENTITY FULL` (PG) và cảnh báo hiệu năng cho từng bảng keyless.
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

### 7.4a `schema diff` / `schema approve <id>`

Xem mục 3.4. `diff` liệt kê thay đổi schema đang chờ duyệt (`--all`: gồm cả đã áp); `approve <id>` chạy DDL đề xuất ở target (`--dry-run` chỉ in), idempotent. Cả hai đọc `target` và `name` từ file cấu hình (`-c`).

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

Trần spool tính cả file payload spill còn giữ trên disk, một lần theo đường dẫn, kể cả khi metadata đã tràn sang bucket. Truncate transaction chưa giải phóng dung lượng file đang được mượn; quota chỉ giảm khi owner đã dọn file vật lý.

Spool prefetch mới lưu provenance và ordered identity contract cùng mỗi transaction (định dạng STX4). `sync` kiểm lại mode, tên/thứ tự key và columns trước khi coi vị trí là đã phủ, rồi kiểm lại trước apply, kể cả payload spill. Spool generic STX2/STX3 vẫn đọc được, nhưng bản ghi prefetch cũ chưa có contract và chưa được checkpoint sẽ bị từ chối; cần dựng lại từ source/archive tin cậy. Không tự xóa spool khi phần log tương ứng đã bị purge. Dùng cùng phiên bản mới cho `seed apply` và `sync`; binary cũ không đọc STX4.

Prefetch trực tiếp từ source chỉ nhận bảng có metadata cột/key rõ ràng từ reader và row image FULL; không suy UNIQUE/keyless từ cột đầu. Khóa UNIQUE NOT NULL được xác định từ catalog source; bảng keyless cần archive với sidecar đã xác minh và metadata FULL, hoặc restore seed không prefetch rồi dùng CDC trực tiếp khi log còn đủ. Metadata do catalog hiện tại cung cấp vẫn không tự chứng minh schema ở một vị trí lịch sử. Với sidecar archive, TABLE_MAP phải có tên cột theo thứ tự và mọi QUERY ngoài BEGIN/COMMIT/ROLLBACK bị từ chối; mirrored expression indexes cũng bị validator từ chối khi chưa chứng minh được tương đương.

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

Bảng thêm chạy song song với snapshot của bootstrap đang dở (không tính vào `max_parallel`, nhưng cùng chia ngân sách đọc nguồn). Bảng còn trong hàng đợi của bootstrap (chưa tới lượt) bị từ chối với `already being snapshotted`: nó sẽ được snapshot khi tới lượt. Pipeline chỉ báo `synced` khi bootstrap và mọi `snapshot add` đều xong.

Bảng thêm bằng lệnh này mà không nằm trong `tables.include` được ghi vào state cục bộ (`pipeline-state.json` trong `resources.spool.path`, trường `added_tables`) và **vẫn thuộc pipeline sau khi restart**: khởi động hợp chúng vào bộ lọc, nạp metadata/schema như bảng thường và không snapshot lại bảng đã xong. Bảng đã bị xóa ở source thì chỉ log WARN và bỏ qua.

Để bỏ một bảng đã thêm: thêm nó vào `tables.exclude` rồi restart. `tables.exclude` luôn thắng; Strand log WARN và quên bảng khỏi danh sách đã thêm (thêm lại sau này phải snapshot lại). Mất file state thì danh sách này mất theo: thêm bảng vào `tables.include` nếu muốn giữ lâu dài.

### 7.8 `verify`

| Chế độ | Lệnh | Cặp | Ghi chú |
| --- | --- | --- | --- |
| sample | `verify --mode sample --table app.t [--rate 0.01]` | Mọi cặp (PG→PG, MySQL→PG: chỉ PK một cột số nguyên) | Lấy mẫu `--rate` phần row theo khoảng khóa, so trực tiếp (khác engine: so chuỗi chính tắc như full) |
| delta | `verify --mode delta` | Mọi cặp | Qua socket: verify các khóa CDC vừa chạm. Lệch thì exit 5 |
| full | `verify --mode full [--table app.t] [--rate N]` | Mọi cặp | Cây Merkle theo chunk 10.000 row, rồi so từng row ở chunk lệch. `--rate` ở đây là chunk/giây (không đặt thì 10) |

- Mọi chế độ kiểm lại row lệch: đọc source, chờ target apply tới vị trí log của source lúc đọc (MySQL: `gtid_executed`; PG: WAL insert LSN, kèm một watermark `pg_logical_emit_message` để pipeline dời checkpoint qua đó cả khi source rảnh), đọc target, đọc lại source. Row đổi ở source trong mọi lần thử (mặc định 5) được đếm `unverifiable_hot` (metric `strand_verify_unverifiable_hot_total`), không báo lệch.
- Bước chờ apply chỉ chạy khi pipeline đang chạy (khóa pipeline trên target đang bị giữ). Pipeline đã dừng thì không chờ; pipeline đang `pause` thì chờ hết hạn (2 phút) rồi lỗi. Source PG đọc qua `--replica-dsn` (standby) không ghi được watermark: source rảnh thì chờ có thể hết hạn.
- `--table` của full: không đặt thì kiểm **mọi bảng của pipeline** (theo `tables.include`/`exclude`, kể cả mẫu `app.*`). `--table` chỉ nhận một bảng `schema.table`, không nhận wildcard.
- `--require-complete`: bắt buộc một lượt full mới trên mọi bảng đã cấu hình, kết quả chưa trọn vẹn thì lệnh thất bại. Dùng trước cutover.
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
- Quyền cần ở hai phía: mục 4.3. Nguồn chỉ liệt kê đối tượng mà tài khoản có quyền xem: thiếu `SHOW VIEW` thì view bị `SKIPPED`, thiếu `TRIGGER`/`EVENT` thì số đếm ra 0 mà không báo lỗi.
- `strand promote --objects-post [--objects-from ./objects]` chạy `post` sau bắt kịp, trước khi mở ghi; lỗi thì target vẫn read-only, chạy lại cùng lệnh. Tắt `event_scheduler` ở nguồn khi fence.
- `strand validate` in cảnh báo "N views, M triggers... not migrated automatically" khi nguồn có các đối tượng này.

### 7.11 `config schema`

`strand config schema` in JSON Schema của file cấu hình.

### 7.12 `archive binlog` (MySQL)

```bash
strand archive binlog --from "mysql://archiver:***@replica1:3306/" --to /archive/orders-db [--server-id 42001]
```

Lưu binlog liên tục từ một MySQL (nên là replica) vào thư mục, thay cho script `mysqlbinlog --raw --stop-never`. Thư mục này dùng trực tiếp cho `seed apply --archive`.

- Khi đọc archive để prefetch/catch-up: cần `binlog_row_metadata=FULL`, `binlog_row_image=FULL`, primary key được xác minh từ TABLE_MAP và không dùng `binlog_row_value_options=PARTIAL_JSON`. Reader từ chối metadata thiếu/row image thiếu cột thay vì đoán dữ liệu rồi đánh dấu GTID đã phủ. Hiện kiểm này áp cho mọi bảng gặp trong archive, kể cả ngoài filter; archive prefetch chưa hỗ trợ bảng keyless hoặc chỉ có unique thay PK khi archive không cung cấp identity được xác minh.
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
| `/usr/lib/systemd/system/strand-web.service` | Unit của Web UI (7.15), chạy `User=strand`; chưa bật sẵn |
| `/usr/share/polkit-1/rules.d/50-strand.rules` | Quy tắc polkit: user `strand` (Web UI) được start/stop/restart `strand@*.service` (7.15) |
| `/etc/strand/` | root:strand 0750; đặt `<name>.yaml` (0640) và `<name>.env` (0600) |
| `/usr/share/doc/strand/` | `INSTALL.md`, `USAGE.md`, ví dụ `pipeline.yaml`, `pipeline.env`, `web.yaml` |
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
- Gỡ gói: dừng và disable mọi `strand@*`, và `strand stop` các pipeline chế độ nền (từng `/etc/strand/*.yaml` trừ `web.yaml` và `/var/lib/strand-web/pipelines/*.yaml`, chạy bằng user `strand`). Không xóa `/etc/strand`, `/var/lib/strand` hay user, kể cả khi purge.
- Chưa có: chữ ký gói (03 §4 yêu cầu ký); ký file `.sha256` bằng khóa phát hành của tổ chức.

### 7.15 `web` (Web UI)

Hướng dẫn từng bước (cài đặt, quyền file, TLS/reverse proxy, sự cố thường gặp): [INSTALL.md](INSTALL.md).

`strand web --config web.yaml` chạy một giao diện web theo kiểu Flink Dashboard: Overview (số pipeline theo trạng thái, lag lớn nhất, tổng rows/s), danh sách pipeline, và trang chi tiết có các tab **Graph** (Source → Assembler → Scheduler → Writer×N → Target kèm số liệu và biểu đồ: 1 giờ trong RAM; chế độ PostgreSQL thêm bộ chọn 6 h, 24 h, 7 d đọc từ lịch sử trong DB và giữ biểu đồ qua lần khởi động lại web), **Tables** (snapshot từng bảng), **Checkpoint**, **Errors** (WARN/ERROR gần nhất, exit code lần dừng gần nhất kèm cách xử lý), **Config** (che mật khẩu, cảnh báo khóa chưa có tác dụng, chế độ sửa YAML và .env), **Logs**. Từ M3 có **thao tác**; M5 có **sửa cấu hình** và **tạo pipeline mới**; từ M4 có **wizard Repair và Promote** (đoạn dưới). REST: `docs/WEB-API.md`. Quyết định: `questions/DECISIONS-WEB-UI-2026-11-15.md`.

**Chỉnh tốc độ snapshot trên web.** Form tạo pipeline có mục "Snapshot speed & safety" (tùy chọn), và tab Config có thẻ **Tuning** cho pipeline đang có: `resources.source.iops.soft/hard`, `bootstrap.logical.chunk_rows`, `bootstrap.logical.max_parallel` ("Tables in parallel"), `bootstrap.logical.windows`, `governor.algorithm`, `governor.lag_slo`, `governor.breaker.*` (`strand_query_p99.max_ratio`, `replica_lag.max`), `resources.workers.max` (mục 6.2, 8.1). Chỉ gồm khóa đã có tác dụng, không gồm khóa ở 6.3. Lưu qua đường sửa cấu hình thường (validate, diff, `.bak`), có hiệu lực sau restart; đổi nóng tốc độ đọc dùng Throttle (tab Graph). `GET/POST /api/v1/pipelines/{name}/tuning` trong `docs/WEB-API.md`.

Không cần thư viện hay mạng ngoài: HTML, CSS, JS nhỏ và biểu đồ SVG đều nằm trong binary.

**Kho metadata PostgreSQL (`database`; chế độ khuyến nghị, mặc định của docker compose).** Không có DSN thì web giữ trạng thái trong file như mô tả bên dưới (cài đặt nhỏ). Chuyển một bản cài file sang DB: `strand web db migrate` (dry-run, nhập, kiểm chứng); xuất DB ra file để sao lưu hoặc quay lại: `strand web db export` (INSTALL.md mục 11). Đặt `database.dsn` trong `web.yaml` hoặc biến `STRAND_WEB_DATABASE_DSN` (biến thắng file) thì web lưu trong PostgreSQL:

```yaml
database:
  dsn: "postgres://strand:***@db:5432/strand?sslmode=disable"   # hoặc env STRAND_WEB_DATABASE_DSN
  audit: both        # db | file | both (mặc định both)
shutdown_stop_pipelines: true   # web thoát (SIGTERM) thì dừng êm pipeline nền; lần sau tự chạy lại. Dùng cho container
```

| Dữ liệu | Chế độ file | Chế độ DB |
| --- | --- | --- |
| Hash mật khẩu admin, hash token | `web.yaml` (`password_hash`, `api_token_hash`) | bảng `admin`; `strand web passwd/token/init` ghi vào DB khi có DSN, có hiệu lực ngay (không cần restart) |
| Phiên đăng nhập | RAM (restart là đăng xuất) | bảng `sessions` (chỉ sha256 của id cookie); luật idle 30 phút / tối đa 12 giờ giữ nguyên; restart không đăng xuất |
| Danh sách pipeline (tạo từ web, khai báo `pipelines:` trong web.yaml, tìm thấy bởi `scan`) | (bản cũ) `<state_dir>/pipelines.json` + web.yaml + glob | bảng `pipelines` cho **mọi** pipeline (cột `origin`: `created`, `web.yaml`, `scan`). YAML trong DB là nguồn sự thật (web hiển thị và sửa bản này); `.env` mã hóa AES-GCM bằng `STRAND_WEB_SECRET_KEY` (bắt buộc); file được dựng lại từ DB trước validate/start/job (0640/0600) vì CLI `strand` chỉ đọc file. Xem "Registry trong DB" bên dưới |
| Trạng thái mong muốn running/stopped | không có | bảng `pipeline_desired`; Start/Restart đặt `running`, Stop đặt `stopped` (chế độ nền); web khởi động thì chạy lại pipeline `running` đang không chạy. Pipeline dừng êm ngoài web (`strand stop`, SIGTERM) được ghi `stopped` (cột `reason` = `observed_stop`, audit `desired.observed_stop`, trang ghi "stopped outside the web") và không tự chạy lại; pipeline chết (lỗi, kill) vẫn `running` |
| Lịch sử job | `<state_dir>/jobs/<id>.json` + `<id>.log` | bảng `jobs` + `job_output` (10 MB đầu mỗi job; `<id>.log` chỉ là spool khi job chạy) |
| Cài đặt web (`scan`, `config_dir`, cookie, X-Forwarded-For, dừng pipeline khi thoát, chu kỳ lấy mẫu) | `web.yaml` | trang **Settings** / bảng `web_settings`; ưu tiên: env > DB > web.yaml > mặc định |
| Wizard Repair/Promote | `<state_dir>/wizards/*.json` | bảng `wizards` |
| Audit log | `<state_dir>/audit.log` (tự xoay theo `audit_rotate_mb`/`audit_rotate_days` thành `audit-<ts>.log.gz` + sha256) | bảng `audit_log` chỉ-thêm (trigger chặn UPDATE/DELETE/TRUNCATE; dòng cũ chỉ rời bảng qua `strand web db audit-archive` / lưu trữ tự động: file `.jsonl.gz` đã kiểm chứng + sha256, dòng `audit.archive` vĩnh viễn; INSTALL.md 12.1). `audit: both` (mặc định) vẫn ghi cả file làm bản sao khi mất DB; `db` chỉ DB; `file` chỉ file |
| Nhiều instance web | không hỗ trợ (một `state_dir` cho một web) | một instance **active** (lease `web_lease`), các instance khác **standby** chỉ đọc, tự tiếp quản khi lease hết hạn (INSTALL.md 12.2) |
| Đồ thị/số liệu | RAM, 1 giờ | bảng `metric_samples` (24 giờ) + `metric_samples_1m` (7 ngày); biểu đồ 1 h / 6 h / 24 h / 7 d |

**Registry trong DB (chế độ DB).** Pipeline khai báo trong `web.yaml` và file tìm thấy bởi `scan` được **phát hiện** vào bảng `pipelines` ở lần quét đầu thấy chúng (chụp YAML, và `.env` mã hóa khi có khóa); từ đó bản DB là nguồn sự thật như pipeline tạo từ web: sửa trên tab Config ghi cả DB lẫn file, trạng thái mong muốn áp dụng. Thứ tự ưu tiên khi trùng tên: khai báo trong `web.yaml` > pipeline tạo từ web > `scan`; đường dẫn/unit của pipeline khai báo lấy theo `web.yaml`. Quy tắc với file trên đĩa:

- **Sửa tay file trên đĩa**: trước validate/start/job, nếu file khác bản DB thì bản DB được ghi lại, bản trên đĩa được **cất** thành `<file>.conflict-<thời gian UTC>` (quyền không rộng hơn file gốc), audit `config.conflict`, và trang pipeline có banner với hai nút: **Adopt disk version** (đi qua đường sửa cấu hình thường: validate, `.bak`, ghi DB + file; validate lỗi thì không đổi gì) hoặc **Keep database version**. Không có ghi đè lặng lẽ.
- **Entry `web.yaml` bị xóa hoặc file scan biến mất**: pipeline **được giữ** trong DB, đánh dấu (`missing_since`), có cảnh báo ở Overview và banner với **Restore file** (dựng lại file từ DB) hoặc **Remove from registry** (gõ lại tên; xóa dòng DB và trạng thái mong muốn, giữ file và lịch sử job). Pipeline bị đánh dấu không được tự dựng lại file.
- Không còn chế độ file: thiếu DSN hoặc `STRAND_WEB_SECRET_KEY` thì web từ chối khởi động (thông báo chỉ tới `strand web init --database-dsn` / `strand web db migrate`).

`.env` không có khóa mã hóa thì chỉ nằm trên đĩa.

Lược đồ do migration nhúng sẵn (`internal/web/store/migrations/NNN_*.sql`) tạo lúc khởi động: mỗi file một transaction, bảng `schema_migrations`, `pg_advisory_lock` nên nhiều instance khởi động cùng lúc không đạp nhau; chạy lại là no-op. Web thử kết nối lại với backoff (0,5 s tới 5 s, tối đa 60 s) rồi mới lỗi; lỗi và log luôn che mật khẩu DSN. `GET /healthz` (không cần đăng nhập) trả `200 {"status":"ok","database":{"mode":"postgres","ok":true}}` hoặc `503` khi DB hỏng.

Lần khởi động đầu có DSN, web **nhập đúng một lần** trạng thái file hiện có: hash mật khẩu/token trong `web.yaml` (chỉ khi DB chưa có admin), `pipelines.json` (kèm đọc YAML và `.env` hiện tại), `jobs/*.json`, `wizards/*.json`, `audit.log` (giữ thời gian gốc), rồi log `imported existing file state into the database ...` kèm số lượng và ghi dòng audit `state.import`. Cờ `files_imported` trong `web_meta` làm các lần sau (và instance song song, qua advisory lock) không nhập lại; file gốc được giữ nguyên. Cài bằng docker compose: INSTALL.md mục 10. `strand web init --if-no-admin --quiet` dành cho bootstrap: chỉ đặt mật khẩu khi DB chưa có admin.

**Cài nhanh: `strand web init`.** Một lệnh thay cho chép `web.yaml`, chown/chmod, `web passwd`, `web token` và enable unit:

```bash
sudo strand web init --enable                          # hỏi mật khẩu hai lần, bật strand-web, chờ /login 200
sudo strand web init --generate-password --token --enable   # mật khẩu và token ngẫu nhiên, in MỘT lần
echo 'mật-khẩu' | sudo strand web init --password-stdin
```

Cờ: `-c/--config` (mặc định `/etc/strand/web.yaml`), `--listen` (127.0.0.1:8080), `--state-dir` (/var/lib/strand-web), `--scan` (lặp lại; mặc định `/etc/strand/*.yaml`), `--tls-cert`, `--tls-key`, `--secure-cookies`, `--trust-forwarded-for`, `--allow-plain-http`, `--password-stdin`, `--generate-password`, `--token`, `--enable`, `--force`. Kiểm tra như lúc `strand web` khởi động (listen ngoài loopback cần TLS hoặc `--allow-plain-http`) trước khi ghi gì. Chạy bằng root và có group `strand` thì `web.yaml` là root:strand 0640 và `state_dir` strand:strand 0750; không thì 0600 thuộc người chạy. Chạy lại an toàn: file có sẵn giữ `listen`/`state_dir`/`scan` trừ khi có `--force`; chỉ đặt lại mật khẩu/token. Không bao giờ in hash. Với máy không có gói: `sudo scripts/install.sh --binary ./strand --with-web`.

**Cài đặt.** Gói `.deb`/`.rpm` cài sẵn `strand-web.service` và ví dụ `/usr/share/doc/strand/examples/web.yaml`. Cài tay: chép binary, `deploy/systemd/strand-web.service` vào `/etc/systemd/system/`, `packaging/web.yaml.example` thành `/etc/strand/web.yaml`.

**`web.yaml`:**

```yaml
listen: 127.0.0.1:8080          # không phải loopback thì cần TLS, hoặc allow_plain_http: true
# allow_plain_http: true        # HTTP thường trên 0.0.0.0 (không mã hoá; chỉ mạng tin cậy)
# tls_cert: /etc/strand/web.crt
# tls_key:  /etc/strand/web.key
state_dir: /var/lib/strand-web  # audit.log (JSON lines, 0600)
# config_dir: /etc/strand       # tùy chọn: thư mục lưu pipeline mới tạo (mặc định /etc/strand nếu ghi được, không thì state_dir/pipelines)
scan:                           # mỗi file là một pipeline; tên pipeline = tên file bỏ .yaml
  - /etc/strand/*.yaml          # (web.yaml bị bỏ qua); <name>.env cùng thư mục được nhận làm env_file
pipelines:                      # khai báo tường minh, thắng scan cùng tên
  - name: orders-sync
    config: /etc/strand/orders-sync.yaml
    env_file: /etc/strand/orders-sync.env     # tùy chọn: KEY=VALUE nạp vào tiến trình con
    systemd_unit: strand@orders-sync          # tùy chọn: có thì dùng systemctl/journalctl
  - name: legacy
    config: /home/strand/legacy.yaml          # không có systemd_unit: chế độ nền
    # log_file, pid_file: tùy chọn (mặc định <resources.spool.path>/strand.log|pid)
```

Tên pipeline chỉ gồm chữ, số, `_`, `.`, `-` (tối đa 63 ký tự, bắt đầu bằng chữ hoặc số); tên khác bị bỏ qua và báo ở đầu trang Overview.

**Hai chế độ quản lý pipeline:**

| Chế độ | Khi nào | Trạng thái khi dừng | Log |
| --- | --- | --- | --- |
| systemd | có `systemd_unit`, hoặc cấu hình đến từ `scan` và unit `strand@<name>` tồn tại | `systemctl show` (ActiveState, ExecMainStatus = exit code) | `journalctl -u <unit>` |
| nền | các trường hợp còn lại: pipeline chạy bằng `strand sync -c <cfg> --background` (7.0b) | file pid còn tiến trình `strand ... sync` hay không | file log (`--log-file` hoặc `<resources.spool.path>/strand.log`) |

Đang chạy thì cả hai chế độ lấy số liệu như nhau: admin socket (`top`, và ba lệnh mới `status`, `tables`, `events`) và `/metrics`, mỗi 5 giây vào ring buffer 1 giờ trong RAM. Pipeline đã dừng: chỉ hiện trạng thái, exit code (systemd; chế độ nền suy ra từ đuôi log) và đuôi log, không đọc vị trí checkpoint từ target. Chế độ nền: nếu tiến trình đã dừng và lần chạy cuối kết thúc bằng lỗi (dòng `Error [code]: ...` hoặc `Error: ...` cuối của CLI, hoặc bản ghi ERROR `pipeline failed`, tính từ mốc khởi động gần nhất) thì trạng thái là `failed` và `last_exit` được điền: lỗi có `[code]` đổi sang exit code theo bảng 8.3 (ví dụ `[log_gap]` là 5); lỗi không gắn mã (ví dụ mất kết nối tới target) là exit 1, như CLI. Lỗi cũ đã được một lần chạy sau đó (chạy rồi dừng êm) thay thế thì không tính. Dừng êm (SIGTERM, `strand stop`) vẫn là `stopped`. Tab Errors giữ các thuộc tính của bản ghi log (`error`, `reason`, `hint`, `table`, ...) cùng khuôn với sự kiện live, và thêm dòng `Error: ...` cuối của CLI.

**Xác thực người dùng và phân quyền (RBAC):**

Web UI sử dụng xác thực bằng tên đăng nhập / mật khẩu cùng hệ thống phân quyền 3 vai trò (RBAC):
- **admin**: Toàn quyền trên toàn bộ hệ thống — quản lý người dùng (`/users`), cài đặt (`/settings`), cấu hình pipeline, xem và sửa biến môi trường `.env`, khôi phục/xóa bản sao lưu (backups restore/delete), promote failover.
- **operator**: Vận hành pipeline — start/stop/restart/pause/resume/throttle, thêm snapshot table, chạy verify/repair, thực thi bản sao lưu (run backup job). Không có quyền quản lý người dùng, cài đặt hệ thống hoặc xem/sửa các bí mật.
- **viewer**: Chỉ đọc (read-only) — theo dõi trạng thái, dashboard, số liệu và tiến độ pipeline. Không thực hiện được bất kỳ thao tác ghi nào, các nút điều khiển bị vô hiệu hóa với tooltip "requires operator", không bao giờ nhìn thấy chuỗi bí mật.

Chính sách mật khẩu: tối thiểu 12 ký tự, tối đa 72 byte, không được trùng với tên đăng nhập; bcrypt cost ≥ 12. Khi tạo người dùng với `--generate-password` hoặc bật `must_change_password`, người dùng bắt buộc phải đổi mật khẩu ở lần đăng nhập đầu tiên. Đăng nhập thất bại quá 5 lần sẽ khóa tài khoản trong 15 phút. Khi người dùng bị vô hiệu hóa, bị xóa, hoặc đổi mật khẩu, toàn bộ các phiên đăng nhập đang hoạt động của người dùng đó sẽ bị thu hồi ngay lập tức.

**Quản lý người dùng qua CLI:**

```bash
# Tạo người dùng mới (vai trò: admin, operator, hoặc viewer)
strand web user add alice --role operator --display-name "Alice Operator"
echo 'SecurePassword123!' | strand web user add bob --role viewer --password-stdin
strand web user add charlie --role admin --generate-password  # in mật khẩu ngẫu nhiên một lần

# Danh sách người dùng
strand web user list

# Vô hiệu hóa hoặc kích hoạt lại người dùng
strand web user disable alice
strand web user enable alice

# Đổi vai trò
strand web user set-role alice admin

# Đặt lại mật khẩu (buộc đổi mật khẩu ở lần đăng nhập kế tiếp)
strand web user reset-password alice --password-stdin

# Đổi mật khẩu cho người dùng cụ thể
strand web passwd --user alice

# Tạo token API người dùng (trên trang My account) hoặc token admin kế thừa
strand web token --config /etc/strand/web.yaml
```

**Nâng cấp từ các bản cài cũ:** Khi nâng cấp lên phiên bản hỗ trợ RBAC, mật khẩu admin đơn lẻ có sẵn trong kho metadata PostgreSQL sẽ tự động chuyển thành tài khoản người dùng `admin` (vai trò `admin`, `must_change_password=false`) một cách an toàn và idempotent. Token API toàn cục cũ tiếp tục hoạt động như một token admin kèm header thông báo `Deprecation: true` cho đến khi bị thu hồi.

**TLS.** Bind khác loopback (`0.0.0.0`, IP máy) cần `tls_cert` và `tls_key`, nếu không từ chối khởi động; ngoại lệ: `allow_plain_http: true` cho phép HTTP thường (mật khẩu, cookie phiên, token đi dạng rõ, web in cảnh báo khi khởi động; chỉ dùng trong mạng tin cậy). Cách thường dùng: để `listen: 127.0.0.1:8080` rồi đặt reverse proxy (nginx, Caddy) làm TLS phía trước, kèm `secure_cookies: true` (cookie có cờ `Secure`) và `trust_forwarded_for: true` (lấy IP client từ `X-Forwarded-For`, chỉ bật khi proxy tin cậy).

**Bảo mật.** Session ngẫu nhiên 256 bit trong RAM (khởi động lại web là đăng nhập lại), cookie `HttpOnly` và `SameSite=Strict`, hết hạn sau 30 phút không thao tác hoặc 12 giờ. Mọi POST cần token CSRF. Đăng nhập sai quá 5 lần/phút từ một IP bị khóa (1 phút, tái phạm thì gấp đôi, tối đa 15 phút). Header `Content-Security-Policy: default-src 'self'`, `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer`. Audit log `<state_dir>/audit.log` ghi đăng nhập, đăng xuất, lần xem cấu hình và mọi thao tác ghi (dòng `started` trước khi chạy, dòng kết quả kèm exit code; không ghi được audit thì không chạy thao tác).

**Chạy bằng user `strand`, không phải root.** `strand web` chạy cùng user với `strand sync` (unit mẫu có `User=strand`); không cần sudo. Hệ quả: chỉ quản lý được pipeline mà user này có quyền:

- đọc được file cấu hình pipeline (`/etc/strand/<name>.yaml`, root:strand 0640 là đủ) và file `.env` nếu dùng `env_file` (gói đặt `.env` ở root:strand 0600: đổi thành 0640 nếu web cần đọc);
- kết nối được admin socket (0700, thuộc user chạy pipeline: cùng user `strand` thì được);
- đọc được log: `journalctl -u strand@<name>` cần user `strand` thuộc nhóm `systemd-journal` (`sudo usermod -aG systemd-journal strand`), hoặc dùng chế độ nền với file log mà user đọc được;
- pipeline do user khác chạy (ví dụ root) thì web không thấy socket và báo `unreachable`/`stopped`.

`systemctl start|stop|restart|enable|disable strand@<name>` chạy bằng chính user này, không sudo. Gói cài `/usr/share/polkit-1/rules.d/50-strand.rules` (cài tay: chép `packaging/polkit/50-strand.rules` vào `/etc/polkit-1/rules.d/`, cần polkit >= 0.106): chỉ cho user `strand` thao tác `start`, `stop`, `restart` trên đúng `strand@<name>.service`; unit khác vẫn bị từ chối. `enable`/`disable` (và `daemon-reload`) cố ý KHÔNG được cấp: polkit action `manage-unit-files` không mang tên unit nên không giới hạn được theo `strand@*`; nút Enable/Disable trả 502 (`Interactive authentication required`), dùng `sudo systemctl enable|disable strand@<name>`. Không được phép thì nút trả lỗi 502 kèm thông báo của systemctl.

**Khởi động lại web không giết tiến trình con.** `strand-web.service` có `KillMode=process`: `systemctl restart|stop strand-web` chỉ gửi tín hiệu cho tiến trình web. Pipeline chế độ nền (`strand sync --background`) và job verify/repair/promote do web sinh ra (process group riêng) vẫn chạy, web mới nhận nuôi chúng qua pid file / job file. Hệ quả: `systemctl stop strand-web` không dừng pipeline chế độ nền (dừng bằng nút Stop hoặc `strand stop -c <cfg>`). Dưới `ProtectSystem=strict` tiến trình con chỉ ghi được vào `/var/lib/strand-web`: đặt `resources.spool.path` của pipeline chế độ nền trong `/var/lib/strand-web/<tên>`, hoặc thêm `ReadWritePaths=` bằng drop-in.

**Thao tác (M3).** Nút nằm ở đầu trang chi tiết và các tab; mọi thao tác đi qua `POST /api/v1/pipelines/<tên>/actions/<action>` (docs/WEB-API.md §4) nên script dùng được bằng token. Hộp thoại xác nhận là phần tử `<dialog>` điều khiển bởi `app.js`, không có script inline (CSP `default-src 'self'`).

| Thao tác | Ở đâu | Cách chạy |
| --- | --- | --- |
| Start, Stop*, Restart* | đầu trang | systemd: `systemctl <verb> <unit>`. Nền: `strand sync -c <cfg> --background` / `strand stop -c <cfg>` (restart = stop rồi start), nạp `env_file` vào môi trường |
| Enable / Disable at boot | đầu trang (chỉ chế độ systemd) | `systemctl enable|disable <unit>` |
| Pause, Resume | đầu trang | admin socket `pause`, `resume` |
| Pause snapshot, Resume (snapshot only), Snapshot add* | tab Tables | admin socket; Snapshot add chọn bảng từ danh sách của source (lệnh socket chỉ đọc `source_tables`, bảng đã thuộc pipeline không hiện) |
| Throttle (soft IOPS) | tab Graph | admin socket `throttle_set --soft`; chỉ tiến trình đang chạy, không ghi vào file cấu hình |
| Checkpoint Rewind** | tab Checkpoint (chỉ MySQL → MySQL, pipeline đang chạy) | admin socket `checkpoint_rewind`; pipeline tự kiểm `gtid_purged` |
| Verify sample / full / delta | tab Jobs | tiến trình con `strand verify -c <cfg> --mode ... [--table= --rate= --resume]` (luôn `--repair report`) |

`*` có hộp thoại xác nhận. `**` phải gõ lại đúng tên pipeline (API: trường `confirm_name`).

**Job verify.** Chạy nền dưới web, output hiện trực tiếp ở `/jobs/<id>` (cập nhật mỗi giây, mật khẩu/DSN bị che). Mỗi pipeline tối đa **một** job đang chạy: bấm thêm thì bị từ chối (409). Lịch sử (JSON + file output mỗi job, 100 job gần nhất mỗi pipeline) nằm ở `<state_dir>/jobs/` (chế độ PostgreSQL: bảng `jobs` + `job_output`, giữ 10 MB đầu của output mỗi job kèm dòng ghi chú khi bị cắt; file `<id>.log` chỉ là spool khi job chạy), xem ở tab Jobs của pipeline và trang Jobs chung; có nút Cancel (SIGTERM). Verify nhận một bảng (`schema.table`) hoặc, với `full`, tất cả bảng. Điều kiện an toàn và exit code là của CLI (lệch dữ liệu: exit 5, hiện ở trạng thái job `exit 5`). Khi web khởi động lại giữa lúc job chạy, tiến trình verify vẫn chạy tiếp (process group riêng, output ghi thẳng vào file) và được web mới nhận nuôi; mã thoát của job loại này không đọc được nên để trống.

**Wizard Repair (M4).** Tab **Repair** của pipeline. Bốn bước, mỗi bước là một thao tác người dùng bấm; server ép đúng thứ tự (gọi bước 3 trước bước 2 thì 409):

1. Chờ lag về gần 0: trang hiện lag đọc trực tiếp từ admin socket và ngưỡng (mặc định 5 s, đặt lúc bắt đầu). Bắt đầu wizard phải gõ lại tên pipeline.
2. **Stop the pipeline**: chỉ chạy được khi lag đọc được và không vượt ngưỡng (server kiểm lại lúc bấm); dùng đúng cách dừng của nút Stop (systemd hoặc `strand stop`). Pipeline đã dừng sẵn thì bỏ qua việc dừng.
3. **Run repair**: gõ lại tên pipeline, rồi chạy job `strand verify -c <cfg> --mode full --repair fix [--table=<t>]` với output trực tiếp. CLI vẫn tự từ chối khi pipeline còn chạy và chỉ hỗ trợ MySQL → MySQL; wizard không thay thế các kiểm tra đó. Job thoát khác 0 thì wizard ở lại bước 3 (mã thoát hiện trên trang), chạy lại được.
4. **Start the pipeline**: chỉ khi job repair thoát 0. Pipeline chạy tiếp từ checkpoint và phát lại những gì source đổi trong lúc repair.

Trạng thái lưu ở `<state_dir>/wizards/repair-<pipeline>.json` (0600): tải lại trang hoặc khởi động lại `strand web` vẫn tiếp tục đúng bước. "Close the wizard" đóng wizard nhưng không tự start lại pipeline đã dừng.

**Wizard Promote (M4).** Tab **Promote**, theo các bước của `strand promote` (7.10); wizard không cài lại logic promote, chỉ gọi CLI:

1. **Check state**: `strand promote -c <cfg> --pipeline=<tên> --engine=<mysql|pg> --target=... --fence=... --dry-run`. CLI nạp `_strand_promote` ở chế độ chỉ đọc, in kế hoạch và log "resuming promote from interrupted step" (bước đang dở và bước đã xong) nếu một lần promote trước bị ngắt; trang tóm tắt dòng đó và hiện output.
2. **Promote**: chỉ sau khi bước 1 thoát 0; gõ lại tên pipeline một lần. Chạy cùng lệnh không có `--dry-run`. Tùy chọn: `--catchup-timeout`, `--fence-timeout`, `--peak-rate`, `--sync-interval`, `--app-role`, `--source-app-roles`, `--schemas`, `--objects-post`, `--override-sequences`.
3. **Cần quyết định (exit 10 `promote_needs_decision`)**: trang hiện nguyên văn khối "PROMOTE NEEDS OPERATOR DECISION" của CLI (kèm phần dữ liệu sẽ mất và sequence gợi ý) và các lựa chọn: cô lập source bằng cách khác rồi bấm **Failover**; chỉ cần giá trị sequence thì chạy Promote lại với `override_sequences`; hoặc đóng wizard (tiến độ vẫn nằm trong `_strand_promote`, lần chạy sau tiếp tục). **Failover** (`--source-fenced-externally`) có hộp thoại riêng giải thích rủi ro và phải gõ lại tên pipeline **hai lần** (hai hộp thoại liên tiếp); không dùng được nếu CLI chưa dừng bằng exit 10.
4. Sau khi xong: chuyển ứng dụng sang target rồi bấm Stop pipeline (promote giữ `sync` chạy).

DSN target (`--target`) và DSN source để fence (`--fence`, tùy chọn khi failover vì source đã chết) do người dùng nhập mỗi lần chạy, **không bao giờ lưu**; web truyền chúng cho tiến trình con `strand promote` qua biến môi trường `STRAND_PROMOTE_TARGET_DSN` / `STRAND_PROMOTE_FENCE_DSN` (không qua đối số, nên không hiện trong `ps`), và không ghi vào audit log hay lịch sử job (`--target=<env>`). Chạy CLI tay cũng dùng được hai biến này thay cho `--target` / `--fence`. Dùng tài khoản promote (ví dụ `strand_admin`, mục 7.10), không dùng tài khoản replication. Trạng thái wizard lưu ở `<state_dir>/wizards/promote-<pipeline>.json` (không có DSN). Mọi bước đi qua `POST .../actions/<action>` (docs/WEB-API.md §4.4), có audit và dùng chung quy tắc một job mỗi pipeline.

**Sửa cấu hình (M5).** Tab **Config** của từng pipeline có chế độ sửa trực tiếp (textarea YAML) và trình quản lý file `.env`:
- Lưu cấu hình YAML: (a) ghi ra file tạm cùng thư mục, (b) chạy `strand validate -c <tmp>` với biến môi trường của pipeline, (c) hiển thị kết quả kiểm tra và diff so với file hiện tại, (d) lưu bản sao lưu `<file>.bak.<timestamp>` và ghi đè nguyên tử `os.Rename`, (e) tùy chọn Restart pipeline sau khi lưu. Nếu cú pháp lỗi, validate thất bại thì không ghi đè.
- Quản lý `.env`: mật khẩu và giá trị bí mật không bao giờ lộ ra ngoài giao diện/API (hiển thị `********`). Khi sửa, giá trị bí mật là write-only (để nguyên `********` thì giữ giá trị cũ, nhập mới thì cập nhật). File `.env` giữ nguyên mode/chủ sở hữu của file cũ (không bao giờ cho phép bit other; file mới 0600), bản sao lưu `.bak.<ts>` có cùng mode.

**Tạo pipeline mới (M5).** Trang `/pipelines/new` cung cấp form trực quan để tạo pipeline mới tương tự `strand config init`:
- Tên pipeline được kiểm tra theo whitelist chặt chẽ (từ chối tên trùng lặp và chặn path traversal).
- Hỗ trợ các engine nguồn (MySQL, MariaDB, PostgreSQL) và đích (PostgreSQL, MySQL).
- Mật khẩu nguồn/đích được ghi trực tiếp vào file `.env` (mode 0600) trong `config_dir` (khai báo ở `web.yaml`, mặc định `/etc/strand` nếu ghi được, ngược lại `<state_dir>/pipelines`). File cấu hình YAML chỉ tham chiếu biến môi trường qua `${SRC_PW}` hoặc `password_env`.
- Sau khi tạo, server tự động thực hiện `strand validate` và `strand plan`, hiển thị kết quả trực tiếp và cung cấp nút **Start** để khởi động pipeline ngay ở chế độ nền (background). Pipeline mới được lưu vào `<state_dir>/pipelines.json` (không cần ghi được `web.yaml`, còn nguyên sau khi `strand-web` khởi động lại) và có hiệu lực ngay lập tức. File `.env` dùng dòng `KEY=VALUE` (không `export`), dùng được cho systemd `EnvironmentFile`; mật khẩu chứa xuống dòng bị từ chối.
- Database: ô `database` (một hoặc nhiều tên cách nhau dấu phẩy) hoặc `databases` chọn database dữ liệu, sinh `tables.include: ["<db>.*", ...]` (nguồn MySQL/MariaDB). Target MySQL: `target.database` là database chứa bảng `_strand_*` của Strand, mặc định `strand` (đổi bằng ô `target_database`), KHÔNG phải database dữ liệu. Database này phải tồn tại trước khi `sync`: `CREATE DATABASE strand;` (validate báo lỗi `Unknown database` kèm gợi ý nếu thiếu).
- Nếu `strand validate` thoát khác 0, pipeline vẫn được tạo (file cấu hình và `.env` được giữ lại để sửa ở tab **Config**) nhưng kết quả là `ok: false`, trang hiện rõ "Validation failed" và không có nút Start; sửa cho đến khi validate qua rồi mới Start.
- Admin socket mặc định là `<spool>/admin.sock`; giới hạn đường dẫn Unix socket là 107 byte (Linux). Nếu quá dài, web chọn `<state_dir>/sock/<tên>.sock`, rồi `/run/strand/<tên>.sock` (nếu ghi được), cuối cùng `<tmp>/strand-<tên>.sock`. `strand validate`/`sync` từ chối `observability.admin_socket` dài quá giới hạn với lỗi nêu khóa và độ dài.
- Cổng `/metrics` mỗi pipeline một cổng riêng: mặc định chọn cổng trống đầu tiên từ `127.0.0.1:9464` trở lên (bỏ cổng đã dùng trong cấu hình pipeline khác và cổng đang bị chiếm); trường `metrics` được tôn trọng, trùng cổng pipeline khác bị từ chối (409).
- Tab Config hiện đúng văn bản file (giữ chú thích, dòng trống, định dạng), chỉ che giá trị bí mật bằng `********`. Lưu lại văn bản đã che sẽ khôi phục bí mật gốc theo từng dòng và giữ nguyên định dạng; nếu một dòng `********` không khôi phục được thì lưu bị từ chối (400), không bao giờ ghi `********` làm mật khẩu.

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
| Snapshot nhiều bảng nhanh hơn khi **ghi target** (flush) là nút cổ chai | `bootstrap.logical.max_parallel` (vd 4) | Khởi động lại |
| Ít commit hơn | `target.batch.max_rows` / `max_bytes` | Khởi động lại |
| Đệm khi target chậm | `spool.hard`, `spool.overflow` | Khởi động lại |
| Không chạm primary (MySQL) | Trỏ source vào replica | Khởi động lại |

**Snapshot song song (`bootstrap.logical.max_parallel` = N).** Tối đa N bảng được snapshot cùng lúc; worker rảnh nhận bảng chờ còn nhiều việc nhất (ước lượng row × độ dài row × phần chưa xong), nên phần đuôi chỉ còn bảng nhỏ. Chunk của một bảng vẫn tuần tự (keyset walk). Điều tốc không đổi:

- **Ngân sách đọc nguồn là chung:** mọi bảng chia một token bucket, tổng tốc độ đọc ≤ R (`iops.soft`, governor, `throttle set`) như khi N = 1. N chỉ giúp khi nút cổ chai là ghi target (flush upsert) hoặc độ trễ mỗi chunk, không giúp khi R đã là giới hạn.
- **Cổng chung:** `pause --snapshot`, breaker, `windows`, governor dừng background giữ **mọi** worker (cả trước khi nhận bảng mới).
- **Router và flush:** mỗi flush chunk vẫn chạy trong barrier (router dừng, mọi Txn đã nhận đã commit). Flush của nhiều bảng dùng chung một lần dừng router và chạy song song, mỗi flush trên một kết nối target riêng; trong lúc đó stream không được route (như khi N = 1, một lần dừng mỗi chunk).
- **Bộ nhớ:** ~N × cỡ chunk (trần byte của chunk: 64 MiB) cho row snapshot đang giữ, cộng buffer event của các chunk đang đọc.
- **Kết nối:** nguồn: N kết nối đọc chunk (MariaDB/MySQL không GTID: thêm một kết nối `LOCK TABLES … READ` ngắn mỗi chunk). Target: N kết nối flush cộng `workers.max` kết nối writer; pool target tự nới khi `workers.max + N + 8` > 64.
- **FK:** target PG có FK (schema tự tạo): bảng con chỉ bắt đầu khi bảng cha đã xong. Target MySQL không kiểm FK khi ghi nên không ràng buộc thứ tự.
- **Restart / kill -9:** mỗi bảng dở dang chạy tiếp từ plan riêng (`pipeline-state.json`); `strand top` và tab Tables của Web UI hiện mọi bảng đang chạy, ETA theo tốc độ riêng của từng bảng.
- **Source PostgreSQL:** luôn tuần tự (khóa được nhận, có cảnh báo khi `validate`/`sync`).

Bộ nhớ: không có trần RSS (`resources.memory` chưa thi hành). Các phần lớn được tràn ra đĩa:

- Assembler tràn theo **số row** (handoff dòng 30).
- Reader PG giữ Txn streaming/2PC trong RAM tới tổng 64 MiB, vượt thì tràn ra `<spool.path>/pg-reader-<name>`.
- Chunk snapshot có trần byte; đọc vượt trần thì chunk được tách.

### 8.2 Metric (`GET /metrics` trên `observability.metrics`)

`GET /healthz` trả 200 khi vòng chính còn tiến (liveness).

| Metric | Nhãn | Ý nghĩa |
| --- | --- | --- |
| `strand_lag_seconds` | pipeline, phase (`bootstrap`\|`synced`) | Độ trễ so với source (gồm cả phần reader chưa đọc). Xem 10.3 |
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
| 3 | `missing_pk` | Bảng không có PK/unique NOT NULL (hoặc PG chưa có REPLICA IDENTITY phù hợp; hoặc bảng đích thiếu unique index khớp; hoặc bảng keyless bị snapshot logic) | Chạy lệnh `ALTER TABLE` được in trong thông báo (mục 5.1.2), loại bảng, hoặc keyless + seed (mục 5.1.3) |
| 5 | `keyless_row_missing` | UPDATE/DELETE của bảng keyless không khớp hàng nào ở target (target lệch) | Nạp lại bảng từ seed; `strand verify --mode full` để xem số hàng/checksum |
| 3 | `type_unmappable` | Kiểu không ánh xạ được; PK không snapshot logic được | Mục 3.2, 5.2 |
| 3 | `collation_looser` | Collation khóa ở target lỏng hơn source | Dùng collation chặt hơn (PG: `"C"`) |
| 3 | `seed_invariant_failed`, `prerelease_tool` | `seed plan`/`apply` từ chối | Sửa đúng điều kiện SD nêu ra |
| 3 | `schema_change_pending` | `validate` với `create_schema: off` mà thiếu bảng | `schema init` |
| 4 | `pipeline_locked`, `fenced` | Instance khác cùng `name` đang chạy hoặc đã thay thế; `repair fix` khi `sync` đang chạy | Bảo đảm chỉ một instance |
| 5 | `log_gap` | Source đã xóa log chưa apply; slot PG không tồn tại | Cấp lại từ archive (`--archive`), hoặc seed lại |
| 5 | `prefetch_exhausted` | Spool prefetch đầy | Tăng spool, thêm tầng bucket |
| 5 | `verify_mismatch` | Verify thấy lệch; UPDATE TOAST mà row không có ở target | Quy trình repair (7.8), điều tra nguyên nhân |
| 10 | `schema_change_pending` | DDL phá hủy (hoặc `additive: pause`) trên bảng của pipeline; DROP/RENAME bảng; source PG | `strand schema diff`, `strand schema approve <id>`, chạy lại `sync` (mục 3.4) |
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

**Bảng của tôi không có PRIMARY KEY, làm sao?** Nếu có UNIQUE index mà mọi cột NOT NULL, Strand dùng nó (mục 5.1). Nếu không, `strand validate` in lệnh `ALTER TABLE` thêm cột khóa ẩn cho từng bảng (mục 5.1.2), hoặc với PG lệnh `REPLICA IDENTITY USING INDEX`. Bảng không thể thêm khóa và có dữ liệu đầu từ seed: chế độ keyless (mục 5.1.3).

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
| exit 10 `schema_change_pending` | DDL phá hủy ở source (hoặc `schema.additive: pause`) | `strand schema diff -c p.yaml`, `strand schema approve <id> -c p.yaml`, chạy lại `sync` (mục 3.4) |
| `verify --repair fix` exit 4 | `sync` đang chạy | Dừng `sync` trước (7.8) |
| repair báo `the source changed while repairing` | Source đổi UNIQUE trong lúc repair | Để pipeline bắt kịp, dừng, repair lại |
| `verify --mode full` báo `must be schema.table (no wildcard)` | `tables.include` có wildcard | Dùng `--table` cho từng bảng |
| Snapshot đứng yên | Governor dừng (zone Red), breaker mở, ngoài `windows`, `pause --snapshot` | `strand top`; xem `strand_governor_*`, `strand_breaker_open` |
| Lag tăng mãi | Target ghi chậm hơn source | `relaxed` + tăng `workers.max`; kiểm IO target |

### 10.3 Giới hạn đã biết

- **Lag:** hàng đợi nội bộ còn việc thì lag = now − thời điểm commit ở source của Txn mới nhất. Hàng đợi rỗng nhưng đầu source đang đi trước vị trí reader đã đọc (`pause`, reader bị chặn bởi backpressure/governor/breaker, mạng treo) thì lag vẫn > 0: monitor thăm dò đầu source mỗi giây (MySQL: `gtid_executed` hoặc vị trí binlog; PG: walsender của slot so với head WAL), dùng cùng mốc thời gian nên đó là **cận dưới** (Txn chưa đọc chắc chắn mới hơn mốc). Thăm dò lỗi hoặc không xác định được thì lùi về cách tính cũ (0 khi hàng đợi rỗng). Source PG: walsender có thể đã gửi hết vào bộ đệm TCP trong khi reader đang pause, nên pause ngắn với tải rất nhẹ có thể chưa hiện lag. Governor bỏ qua lag trong lúc operator `pause` (lag tăng vì ta dừng đọc, không phải quá tải).
- **Verify sau seed với bảng keyless** chỉ so số hàng và checksum. Source thay đổi liên tục trong lúc so thì bằng chứng báo `incomplete`: chạy `verify --mode full` lúc source ít ghi.
- **`validate` không kiểm PG source** (dòng 33) và chỉ kiểm vài quyền phụ (mục 4.3).
- **`target.batch.max_wait`** làm khóa nóng chậm lại ở `strict` (dòng 26); `relaxed` gom chuỗi Txn phụ thuộc vào cùng batch.
- **PG identity `ALWAYS`:** PK bị `UPDATE … SET id = DEFAULT` kèm cột TOAST không đổi làm writer lỗi 428C9 (dòng 27, hiếm).
- **Source PG:** governor `onoff`/`grace-v2` dùng tín hiệu PG (replica lag của standby đang replay, WAL slot); thiếu quyền thì lùi về `static`.
- **Không có:** đồng bộ hai chiều, DDL tự động từ source PG, verify full theo lịch, HA tự failover của chính Strand, tự co giãn worker, mask cột, đọc từ PG standby.
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


## Web UI: tính năng và phần chỉ có ở CLI

Web UI (PostgreSQL bắt buộc) làm được: tạo pipeline cho mọi cặp được hỗ trợ (MySQL/MariaDB/PostgreSQL → MySQL/PostgreSQL, MongoDB → MongoDB) kèm tuning; sửa cấu hình, `.env`, tuning; tải về/tải lên YAML; start/stop/restart/enable/disable, pause/resume, throttle, `snapshot add`, checkpoint show/rewind; `verify`; wizard Repair và Promote; `plan`; duyệt thay đổi schema (`schema diff`/`approve`, xem trước DDL); `schema init --dry-run`; database objects (`objects list`/`apply`); backup (catalog, job); trang MCP; Settings; audit.

Cố ý để lại CLI (lý do):

- `seed plan/apply`: điều phối restore bản backup vật lý lên máy target (cần quyền và đường dẫn trên máy DB, thao tác phá hủy dữ liệu đích); không an toàn để bấm từ web.
- `archive binlog`: tiến trình lưu trữ chạy liên tục, độc lập với pipeline (thường là một unit systemd riêng trên replica), không có trạng thái để web quan sát.
- `spool-inspect`, `top`, `export`, `config schema`, `mongodb sync` (bí danh của `strand sync -c` cho pipeline MongoDB; dạng cờ cũ vẫn nhận), `mongodb repair`, `web db migrate|export|audit-archive` (công cụ quản trị chạy ngoài web: chuyển bản cài cũ, sao lưu/quay về).

### SSH tunnel cho pipeline SQL

Trong form tạo pipeline, có thể bật SSH tunnel riêng cho source hoặc target. Giữ địa chỉ database trong phần kết nối DB như bastion nhìn thấy. Nhập host, cổng và tài khoản SSH trong phần tunnel. `strand` mở cổng loopback tạm trong tiến trình chạy pipeline, rồi đóng cổng khi tiến trình kết thúc.

Ví dụ YAML cho nguồn MySQL:

```yaml
source:
  kind: mysql
  dsn: "${SRC_DSN}"
  ssh_tunnel:
    host: bastion.example.net
    port: 22
    user: strand
    identity_file: /etc/strand/keys/bastion
```

Để dùng chung cấu hình, lưu một profile ở **Tools → SSH Tunnels**, rồi chọn cùng tên ở Source và Target hoặc khai báo `ssh_tunnel: {profile: production-bastion}`. Chọn cùng profile trong **Connections** để dùng cho các job backup. Khi lưu profile, Web kiểm tra đăng nhập SSH trước. Địa chỉ DB được kiểm tra khi cấu hình kết nối. Profile chứa thông tin bastion và cách xác thực. Địa chỉ DB vẫn cấu hình riêng cho từng kết nối. Private key dán vào form được mã hóa trong PostgreSQL metadata bằng `STRAND_WEB_SECRET_KEY`; YAML chỉ chứa tên profile. Dùng HTTPS khi gửi private key lên Web. Tiến trình `strand` chạy trực tiếp hoặc qua systemd cần có `STRAND_WEB_DATABASE_DSN` và `STRAND_WEB_SECRET_KEY` để dùng profile có khóa lưu trong DB.

`target.ssh_tunnel` dùng cùng cấu trúc. Đặt khóa riêng trên máy hoặc container chạy Strand và đặt `STRAND_WEB_SSH_KEY_DIR` thành thư mục chứa các khóa được phép sử dụng. Nếu bỏ `identity_file`, tiến trình cần truy cập được SSH agent qua `SSH_AUTH_SOCK`. Khóa máy chủ SSH phải có trong file tin cậy của dịch vụ. Đặt `STRAND_WEB_SSH_KNOWN_HOSTS` thành đường dẫn file nếu dịch vụ dùng file riêng, hoặc dùng `~/.ssh/known_hosts` của tài khoản chạy Strand. Tunnel vẫn xác minh host key. Cấu hình TLS đến database tiếp tục dùng tên máy chủ database trong DSN.

SSH tunnel tích hợp hỗ trợ các đầu kết nối MySQL, MariaDB và PostgreSQL. MongoDB replica set cần đường mạng truy cập được những địa chỉ member mà cluster công bố.

## Pipeline MongoDB → MongoDB

`source.kind` = `target.kind` = `mongodb` là pipeline bình thường: `strand validate|plan|sync|top|pause|resume|checkpoint show|verify|snapshot add|stop|status`, unit systemd `strand@<tên>`, admin socket, `/metrics` và Web UI như pipeline SQL. Cấu hình (`uri: "${SRC_URI}"`, `database`, `tables.include: ["db.*"]`, mục `mongodb:`), yêu cầu (replica set, MongoDB 4.4–8.0), quyền tối thiểu, cửa sổ oplog, hành vi failover, metric/cảnh báo và runbook: xem [docs/mongodb-cdc.md](mongodb-cdc.md).
