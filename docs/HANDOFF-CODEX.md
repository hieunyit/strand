# Bàn giao Strand CDC cho Codex — trạng thái ngày 2026-11-04

Tài liệu này dành cho agent tiếp quản (Codex). Đọc hết trước khi sửa code. Mọi trạng thái dưới đây đã được kiểm chứng tại thời điểm viết; chỗ nào chưa kiểm chứng được ghi rõ "chưa kiểm".

---

## 0. Đọc gì trước

| Thứ tự | File | Vì sao |
|---|---|---|
| 1 | `spec/02-specs.md` | Đặc tả kỹ thuật — nguồn sự thật. **Không sửa `spec/`** (thuộc owner). |
| 2 | `spec/design.md` | Thiết kế, lý do, bất biến |
| 3 | `TASKS.md` | Danh sách task theo phase + tiêu chí nghiệm thu (AC) |
| 4 | `questions/DECISIONS-2026-11-02.md` | 18 quyết định đã chốt trong code mà spec chưa cập nhật. Khi bạn phải lệch spec: thêm một dòng vào đây (số tiếp theo), không sửa spec |
| 5 | `spec/04-testing.md` | Cách test, gate, soak |
| 6 | `reports/` | Báo cáo từng task (`P1-xx.md`, `P2-0x.md`, `REVIEW-2026-11-02-gate1.md`) |
| 7 | `docs/USAGE.md` | Tài liệu người dùng (đang thiếu — xem việc D) |

Bất biến phải giữ (định nghĩa trong spec 02): **CP-1..5** (checkpoint), **RS-1/RS-2** (range-state snapshot + watermark), **SD-1..4** (seed), **SG-1**, **CO-1/CO-2** (thứ tự commit). Mọi thay đổi ở `internal/snapshot`, `internal/pipeline`, `internal/sched`, `internal/checkpoint`, `internal/writer`, `internal/reader` phải giải thích vì sao bất biến vẫn đúng (comment trong code + báo cáo).

---

## 1. Repo và nhánh

**2026-11-07 — sửa review benchmark:** nhánh `agent/review-bench-fix-20261107`, worktree `/home/hellc/strand-bench-review`, nền `c2fab665`. Sửa fail-open khi thiếu telemetry/cửa sổ p2p, thiếu repeat/governor hoặc workload lỗi; giữ ngưỡng Gate 2, kiểm maximum p2p. Regression đỏ trên runner cũ, test mới qua; chưa chạy benchmark chính thức. Xem `reports/REVIEW-BENCH-FIX-20261107.md`.


- GitHub: `hieunyit/strand` (private). Bản clone của người dùng: `/home/hellc/strand` (remote `origin` = GitHub).
- **GitHub `main` = `9281ffa`** — chứa Gate 1 (P1) + P2 đợt 1 (P2-01 reader PG, P2-02 writer PG, P2-04 typemap, P2-08 Merkle) + seed mysqldump/mydumper.
- **Nhánh tích hợp `review/gate1-fixes` = `f4664a7`** (chưa đẩy lên GitHub) — chứa toàn bộ việc đã xong và đã review:

| Commit merge | Nhánh | Nội dung (task trong TASKS.md) |
|---|---|---|
| `5bfb96f` | `agent/p2-pipe` | `strand sync` chạy được PG→PG và MySQL→PG: engine abstraction (store/applied set/batch writer), checkpoint PG ở target, typemap khi apply, fencing PG, sequence sync |
| `308d87c` | `agent/p2-07` | P2-07 GRACE v2 governor |
| (trong chuỗi) | `agent/p2-06` | P2-06 sequence khi promote (PG) — đã lên main |
| `93f7f2d` | `agent/p2-05` | P2-05 seed PG: pgBackRest / WAL-G / `strand export` |
| `be155d5` | `agent/p2-pgboot` | Snapshot logic từ source PG (low watermark = MVCC snapshot, high = LSN + `pg_logical_emit_message`) và `--start-at seed` cho PG |
| `0354e7e` | `agent/review-fixes` | 5 lỗi thật từ review ngoài + 2 mục phụ (xem §5) |
| `b56c722` | `agent/soak-mem` | Driver soak: so sánh cuối dạng streaming, giới hạn RSS driver |
| `3645f78` | `agent/p2-03` | P2-03 đổi PK khi cột TOAST không đổi (re-read khóa, quyết định 18) |
| `bc8e1e2` | — | Quyết định 17, 18 |
| `f4664a7` | — | Sửa admin báo `synced` sớm (thêm trạng thái `starting`) |

### Danh sách nhánh CHƯA vào GitHub `main` (`9281ffa`) — kiểm ngày 2026-11-04

| Nhánh | Đầu nhánh | Trạng thái | Việc phải làm để vào main |
|---|---|---|---|
| `review/gate1-fixes` | `a6b65be` (18 commit hơn main) | Tích hợp, đã review; chứa mọi nhánh dưới đây trừ 3 nhánh WIP | Full e2e relaxed + strict qua trên đầu nhánh (đang chạy, §4.A) → đẩy lên `main` |
| `agent/p2-pipe` | `5bfb96f` | Đã gộp vào review | Theo `review/gate1-fixes` |
| `agent/p2-07` | `82bd14c` | Đã gộp vào review | Theo `review/gate1-fixes` |
| `agent/p2-05` | `2e4b964` | Đã gộp vào review | Theo `review/gate1-fixes` |
| `agent/p2-pgboot` | `f5fc112` | Đã gộp vào review | Theo `review/gate1-fixes` |
| `agent/review-fixes` | `4f08bd8` | Đã gộp vào review | Theo `review/gate1-fixes` |
| `agent/soak-mem` | `45ab339` | Đã gộp vào review | Theo `review/gate1-fixes` |
| `agent/p2-03` | `f969770` | Đã gộp vào review (xung đột đã giải ở `3645f78`) | Theo `review/gate1-fixes` |
| `agent/schema-init` | `ff963a9` | **WIP** — chỉ có khóa `bootstrap.create_schema` | Làm xong việc B (§4.B), review, gộp vào review, full e2e |
| `agent/generic-pk` | `be155d5` | **Trống** (chưa có commit riêng) | Tạo lại từ đầu nhánh review khi bắt đầu việc C |
| `agent/docs` | `be155d5` | **Trống** | Tạo lại từ đầu nhánh review khi bắt đầu việc D |

Commit trực tiếp trên `review/gate1-fixes` (không qua nhánh agent): `bc8e1e2` quyết định 17/18, `f4664a7` sửa admin `starting`, `a59f190`/`589debc`/`a6b65be` tài liệu bàn giao + `reports/GATE1-soak12.md`.

Các nhánh `agent/*` khác (1TB-A, 1TB-C, 3B, 7, R1–R4, dumplog, mydumper, mysqldump, p2-01, p2-02, p2-04, p2-06, p2-08, soak, spool-seg) **đã nằm trong main**.

Kho chứa các nhánh trên hiện là bản clone làm việc của Claude (`/tmp/claude-1000/.../scratchpad/strand`), **chưa đẩy lên GitHub** — hỏi người dùng nơi lấy (đẩy nhánh lên GitHub hoặc `git fetch` trực tiếp từ đường dẫn đó).
- Quy ước: mỗi việc một nhánh `agent/<tên>` tách từ `review/gate1-fixes`; merge `--no-ff` vào `review/gate1-fixes`; chạy full e2e cả hai chế độ; qua thì fast-forward `main` và đẩy lên GitHub. Commit message tiếng Anh, comment code tiếng Việt (theo style sẵn có).

---

## 2. Môi trường và cách chạy test

```bash
export PATH=$PATH:/usr/local/go/bin GOFLAGS=-buildvcs=false   # go không có trong PATH mặc định
gofmt -l internal cmd tests tools        # phải rỗng
go build ./... && go vet ./...
go test -count=1 $(go list ./... | grep -v tests/e2e)          # 28 package, cần Docker (testcontainers)
STRAND_E2E_COMMIT_ORDER=relaxed go test -count=1 -v -timeout 240m ./tests/e2e/
STRAND_E2E_COMMIT_ORDER=strict  go test -count=1 -v -timeout 240m ./tests/e2e/
```

- Full e2e: ~39 test mỗi chế độ, ~3 giờ mỗi chế độ trên máy này (31 GB RAM). Chạy tuần tự relaxed rồi strict.
- Biến e2e hữu ích: `SEED=<n>` (chạy lại đúng kịch bản; mỗi test in `SEED=` ở dòng đầu), `STRAND_E2E_COMMIT_ORDER`, `STRAND_E2E_PG_SNAPSHOT_ITERATIONS`, `STRAND_E2E_PG_LOAD_SECONDS`.
- Container MySQL trong test phải chạy với `--innodb-use-native-aio=OFF` (máy hết AIO slot khi nhiều container). Harness đã làm sẵn.
- **Không động vào** container `strand-dev-*` (compose của người dùng) và container của người khác. Chỉ xoá container do chính test của bạn tạo (testcontainers/ryuk tự dọn). Có thể còn container rò từ trước (`hardcore_wing`, `musing_elgamal`, …): hỏi người dùng trước khi xoá.
- Không xoá nội dung `/tmp` dùng chung.
- RAM: driver soak cũ từng ăn 21 GB và bị OOM-kill; đã sửa (§3). Khi chạy nhiều e2e song song, theo dõi `free -h`.

Quy tắc chất lượng (người dùng đặt):
- Không dùng `t.Skip`.
- Mỗi bug fix có regression test **thất bại ở code cũ, qua ở code mới** — xác nhận và ghi vào báo cáo.
- Báo cáo trung thực: test fail thì nói fail kèm output; flaky thì nói flaky, không chạy lại cho tới khi xanh rồi im lặng.
- Lệch spec → ghi `questions/DECISIONS-2026-11-02.md`.
- Không commit bí mật.

---

## 3. Gate 1 (P1, MySQL→MySQL) — đã đủ bằng chứng

| Bằng chứng | Kết quả |
|---|---|
| kill -9 ×1.000 (strict, code cuối) | Qua |
| TXN 50 triệu row ở strict | Qua |
| Full e2e (relaxed + strict) | Qua (34/34 mỗi chế độ trên `308d87c`; xem §4.A cho lần cuối) |
| Soak 12 giờ (owner rút từ 72h, quyết định 14) | **0 lệch** — chi tiết `reports/GATE1-soak12.md` |

Soak: 10,65 triệu ops, 103 restart (kill9 ×45, SIGTERM ×43, SIGSTOP ×54), RSS strand ~30 MB phẳng. Driver soak bị OOM-kill ở pha so sánh cuối (nạp 65 triệu row vào RAM) nên không có `report.json`; thay bằng: strand bắt kịp (GTID `1-4452513` = source) rồi `strand verify --mode full` cả 3 bảng: 0 chunk lệch (accounts 1,6M, orders 3,0M, events 65,1M row). Driver đã sửa ở `agent/soak-mem`; **nên chạy lại một soak ngắn (≥1h) với driver mới** để có `report.json` sạch (chưa làm).

Còn thiếu cho Gate 1: cập nhật báo cáo gate (`reports/P1-22.md` hoặc file gate riêng) dẫn tới các bằng chứng trên, đẩy lên main.

---

## 4. Việc còn lại — theo thứ tự ưu tiên

### A. Chốt full e2e trên `f4664a7` rồi đẩy lên GitHub main  ⏳ đang chạy

- Full e2e trên `f4664a7` đã được khởi chạy lúc 2026-11-04 16:39 (log: `<scratchpad>/e2e-final-relaxed.txt`, rồi `e2e-final-strict.txt`; file strict có sẵn trước 16:39 là log cũ của lần chạy khác — nếu bạn không thấy log mới thì chạy lại).
- Tiến độ lần chạy này khi bàn giao: relaxed: 9 pass, 0 fail (lúc 17:00); strict chạy sau relaxed.
- Lần trước (trên `be155d5`): relaxed 37 qua / 1 lỗi, strict 38/38. Lỗi duy nhất là `TestP2_E2E_MySQLToPGSnapshotAndStream` ("snapshot finished before the load started") — nguyên nhân thật là **admin báo `synced` trước khi pipeline quyết định bootstrap** (admin socket mở ở đầu `Run`, cờ `bootstrap` bật muộn hơn). Đã sửa ở `f4664a7` (trạng thái `starting`, test `TestAdminStateStartingBeforeRunning`). Cần xác nhận test này hết flaky trong lần chạy mới.
- Khi cả hai chế độ qua: `git push origin review/gate1-fixes:main` (từ `/home/hellc/strand` sau khi pull nhánh về; repo đó có cảnh báo `dubious ownership` khi chạy git bằng user khác — dùng đúng user sở hữu hoặc `safe.directory`). Hỏi người dùng nếu không chắc quyền.

### B. Tự tạo schema/bảng ở target (quyết định 17)  🟡 mới bắt đầu

**Vấn đề:** Strand hiện **không tạo** schema/bảng ở target. Với bootstrap logic (kịch bản phổ biến nhất) người dùng phải tạo tay mọi bảng trước. Spec chỉ giả định schema đến từ backup khi seed. Người dùng yêu cầu: phải tự tạo.

**Trạng thái:** nhánh `agent/schema-init` (worktree `<scratchpad>/wt/SCHEMA`) mới có: khóa `bootstrap.create_schema` (`auto|off`, mặc định `auto`) trong `internal/config/config.go` (+ `CreateSchemaAuto()`, validate giá trị) và `internal/config/schema.go`. Chưa commit — commit nó làm điểm xuất phát (hoặc làm lại, rất nhỏ).

**Yêu cầu đầy đủ:**
1. `bootstrap.create_schema: auto | off` (mặc định auto) — xong phần config; cần `strand config schema` hiển thị, env override chạy.
2. Khi `strand sync` khởi động (sau khi lọc bảng include/exclude, trước khi đăng ký snapshot/stream): với mỗi bảng source chưa có ở target → tạo database/schema nếu thiếu, rồi tạo bảng. Bảng đã có: **không sửa**, kiểm tương thích như cũ. Ở chế độ seed/`--start-at`: bảng có trong bản restore thì đã có; bảng sẽ snapshot logic (thiếu trong seed) thì tạo. Ghi rõ lựa chọn.
3. Lệnh mới `strand schema init -c pipeline.yaml [--dry-run]`: in DDL (dry-run) hoặc áp; idempotent; exit code theo `internal/exitcode`.
4. Sinh DDL theo hướng:
   - **MySQL→MySQL:** `SHOW CREATE TABLE` từ source; bỏ option `AUTO_INCREMENT=N` của bảng (giữ thuộc tính cột); giữ PK, index, FK, charset/collation, partition; tạo với `FOREIGN_KEY_CHECKS=0` trong session; `CREATE DATABASE` theo charset/collation mặc định của source. **Không copy trigger/event/routine** (chúng sẽ chạy lại trên row Strand ghi → nhân đôi) — log danh sách bị bỏ. View: bỏ qua (log). Tôn trọng `tables.rename` (tên đích qua `filter.Target`).
   - **PG→PG:** dựng từ catalog (không gọi pg_dump): `CREATE SCHEMA IF NOT EXISTS`; cột (`format_type`), NOT NULL, default (sequence `nextval` → tạo sequence sở hữu, hoặc identity đúng increment/start), COLLATE, PK, UNIQUE/CHECK/exclusion qua `pg_get_constraintdef`, index qua `pg_get_indexdef` (đổi schema/tên bảng), FK sau cùng. Cột generated: xem writer xử lý thế nào rồi quyết. Kiểu người dùng (enum, domain): tạo nếu thiếu, hoặc báo lỗi rõ.
   - **MySQL→PG:** dùng lại mapping của `internal/typemap` (đừng nhân bản): kiểu cột, NOT NULL, PK, UNIQUE, index phụ; cột khóa dùng collation deterministic sao cho `strand validate` **qua** trên schema sinh ra; `AUTO_INCREMENT` → `GENERATED BY DEFAULT AS IDENTITY` (promote cần sequence gắn qua `pg_depend` — kiểm `internal/promote`); ENUM theo enum policy; default chỉ khi dịch an toàn. Quyết định cách quote tên (nhiều khả năng giữ nguyên tên có quote) và bảo đảm writer dùng cùng tên.
5. Bảng mới tạo ở source khi đang chạy (CREATE TABLE khớp bộ lọc — `internal/pipeline/ddl.go` `handleDDL`, `ddlNewTables`): với auto, tạo bảng ở target tại barrier DDL (MySQL: `SHOW CREATE TABLE` hiện tại; nếu bảng source đã bị ALTER sau vị trí đó thì quay về đường exit 10), rồi đi tiếp. ALTER/DROP/RENAME bảng có sẵn: giữ nguyên exit 10 (02 §12).
6. `strand validate` với auto: báo "N bảng sẽ được tạo" thay vì lỗi bảng thiếu.

**Test:** unit kiểu golden cho DDL cả 3 hướng (kiểu khó, PK nhiều cột, FK, enum, identity, bảng partition, tên cần quote, rename); integration với container mỗi hướng (~6 bảng đa dạng → `schema init` → so cấu trúc qua information_schema/catalog, validate qua); e2e bootstrap logic dưới tải vào target **rỗng** cho MySQL→MySQL, MySQL→PG, PG→PG với `app.*`/`*.*` — trạng thái cuối giống hệt, `verify --mode full` 0 lệch; CREATE TABLE khi đang stream được tạo + replicate. Các e2e cũ tạo sẵn bảng ở target phải vẫn chạy (bảng có rồi → không đụng).

Sau khi xong: cập nhật dòng quyết định 17 (bỏ chữ "Đang làm", ghi file).

**Khảo sát đã làm (2026-11-04, Claude) — dùng làm điểm xuất phát:**
- Nhánh `agent/schema-init` đã merge `review/gate1-fixes` (commit `27c3e1f`, chỉ trong worktree `<scratchpad>/wt/SCHEMA`) — đã có P2-03, review-fixes… Chưa có code sinh DDL.
- **Điểm cài vào `strand sync`** (`internal/pipeline/pipeline.go`, hàm `Run`): sau khối tính `pending` (danh sách bảng chưa `IsDone` — khoảng dòng 410–420 bản hiện tại) và **trước** khối "MySQL→PG: bảng PK không phải số nguyên rỗng → `writer.PGClearTable`" (khối đó cần bảng đích đã tồn tại). Chỉ tạo cho bảng trong `pending`: bảng đã `Done` (vd. đến từ seed/restore) mà thiếu ở target thì **không** tạo rỗng (sẽ che mất việc restore thiếu) — giữ lỗi như cũ.
- `p.conv.load` (MySQL→PG, `engine.go` ~534) chỉ đọc **source** (`InspectMySQLColumns`), không đọc target → không cần dời. `p.loadTables` cũng chỉ đọc source. Metadata `_strand_*` được `p.store.EnsureTables` tạo trước đó (schema của target DSN).
- Tên đích: `p.filter.Target(id)` (áp `tables.rename`).
- MySQL→PG: `typemap.MapMySQLColumn(col, typemap.DefaultOptions())` trả `ColumnMapping.TargetType` (chuỗi kiểu PG: smallint/integer/bigint/numeric(20,0)/text/varchar(n)/bytea/timestamp(p)/timestamptz(p)/jsonb/text[]/`<col>_enum`…). Với `EnumPolicy=check` kiểu là `text` → phải tự thêm `CHECK (col IN (...))` từ `col.EnumValues`; với `type` cần `CREATE TYPE <col>_enum AS ENUM`. Cột khóa: sinh `COLLATE "C"` cho text/varchar để qua `typemap.CheckKeyCollation` (02 §8: `_ci` → `C` an toàn; cấm citext). Cột khóa PK lấy bằng `typemap.InspectMySQLPrimaryKey`.
- MySQL→MySQL: `SHOW CREATE TABLE` đã không chứa trigger; vẫn nên đọc `information_schema.TRIGGERS` để log bảng nào có trigger bị bỏ qua. Bỏ `AUTO_INCREMENT=N` bằng regex trên phần table options (sau dấu `)` cuối).
- Gợi ý package mới `internal/schemagen` (3 hàm `MySQLToMySQL`, `MySQLToPG`, `PGToPG` trả `[]string` câu DDL + cảnh báo), dùng chung cho `strand sync` và lệnh `strand schema init` (file mới `cmd/strand/schema.go`, đăng ký trong `root.go`).
- Chỗ khác cần gọi: `handleDDL` (`internal/pipeline/ddl.go`) — với bảng trong `newCreated` khi auto: tạo ở target trước bước `schemaDiff`; và đường `strand snapshot add` / thêm bảng do đổi `tables.include` (đi qua `pending` lúc restart nên đã được phủ).

### C. Snapshot logic cho PK không phải một cột số nguyên  ⬜ chưa bắt đầu (người dùng đã duyệt)

**Hiện trạng:** chunk là khoảng `int64` `[StartPK, EndPK]`; `range_state` giữ `BufferedChanges map[int64]…`; event stream được gán chunk bằng so sánh int64 trong Go. Bảng có PK không phải một cột số nguyên bị từ chối nếu có dữ liệu (`internal/pipeline/pipeline.go` quanh dòng 423–453: "needs a logical snapshot but its primary key is not a single integer column"), chỉ nhận khi rỗng. Merkle (`internal/verify/merkle.go` ~629) và verify sau seed/sample/delta (`internal/verify/live.go`, `internal/pipeline/verify.go` ~121) cũng chỉ hỗ trợ PK số nguyên. Snapshot PG (`internal/snapshot/pg_snapshot.go`, `pg_watermark.go`) dùng cùng khoảng int64.

**Yêu cầu cốt lõi:** thứ tự Go dùng để quyết "khóa thuộc chunk nào" và biên keyset phải **giống hệt** thứ tự DB dùng cho `ORDER BY pk` / `WHERE pk > ?`; phép bằng trong Go phải khớp phép bằng của unique index trong DB. Lệch = mất dữ liệu âm thầm.

Phạm vi (người dùng đã duyệt mức 1–2, mức 3 để mở):
- **Mức 1:** PK nhiều cột và PK đơn không phải số nguyên gồm: số nguyên (kể cả BIGINT UNSIGNED), DECIMAL, DATE/DATETIME/TIMESTAMP/TIME, YEAR, BINARY/VARBINARY, CHAR/VARCHAR collation nhị phân (MySQL `*_bin`, charset binary; PG `"C"`/`"POSIX"`, bytea), PG uuid, PG text `"C"`. So tuple theo thứ tự cột PK. Cẩn thận: MySQL PAD SPACE (`utf8mb4_bin` là PAD SPACE: `'a' = 'a '`) vs NO PAD (`utf8mb4_0900_bin`), chuẩn hoá DECIMAL (1.0 vs 1.00), TIMESTAMP và múi giờ.
- **Mức 2:** MySQL CHAR/VARCHAR collation không nhị phân (`utf8mb4_0900_ai_ci`, `general_ci`…): lấy khóa sắp xếp từ chính MySQL bằng `WEIGHT_STRING(col)`. Biên chunk đọc kèm weight; với event stream phải có weight — thiết kế cách lấy (query theo lô tới source chỉ khi bảng đang snapshot là chấp nhận được; tự cài lại collation trong Go **không** được trừ khi chứng minh giống hệt). Danh tính khóa trong buffer phải theo weight (`'a'` và `'A'` dưới ci là một khóa). Kiểm thứ tự WEIGHT_STRING == ORDER BY trên nhiều chuỗi Unicode ngẫu nhiên (khoảng trắng cuối, dấu, hoa/thường, emoji, chuỗi rỗng). Xét cả event đổi PK (khóa cũ/mới) và phép bằng khóa của writer.
- **Mức 3 (ngoài phạm vi):** PG collation ICU/libc không phải "C" — tiếp tục từ chối với thông báo rõ (gợi ý seed hoặc collation "C"); viết câu hỏi mở `questions/OPEN-generic-pk-pg-collation.md` kèm phương án.
- Merkle full verify và live verify (sample/delta/sau seed) phải chạy cho bảng mức 1/2 (keyset theo tuple; với key chuỗi ci khi khác engine MySQL→PG, bảo đảm chuẩn hoá khóa nhất quán hoặc từ chối rõ ràng).

**Gợi ý thiết kế:** mã hoá tuple PK thành `[]byte` giữ thứ tự (memcomparable như TiDB/CockroachDB: int big-endian lật bit dấu, bytes có escape, weight string cho ci), để `range_state` dùng khóa chuỗi + so byte thống nhất; đường int64 hiện tại biểu diễn được bằng cùng mã hoá — đo benchmark trước/sau, không được chậm đi đáng kể. Viết ghi chú thiết kế + lập luận đúng đắn (file mới, ví dụ `internal/snapshot/pkey.go`). Lưu ý xung đột: P2-03 vừa thêm `SnapshotPlan.Reread []int64` và chunk theo tập khóa (`Chunk.Keys`) — phải chuyển sang khóa mã hoá luôn.

**Test:** unit cho mã hoá (tính giữ thứ tự so với DB thật MySQL 8.4 + PG 17, nhiều giá trị ngẫu nhiên mỗi kiểu/collation, các ca bằng/PAD SPACE); unit range_state với khóa ghép và chuỗi; e2e: MySQL→MySQL snapshot dưới tải với (a) PK int ghép, (b) varchar `utf8mb4_0900_ai_ci` có biến thể hoa/dấu và UPDATE đổi PK xuyên chunk, (c) varchar `_bin`, (d) datetime+int ghép; MySQL→PG với PK int ghép và varchar ci (hoặc test từ chối rõ ràng); PG→PG với PK ghép, text `COLLATE "C"`, uuid. Mỗi e2e: trạng thái cuối giống hệt, mỗi phiên bản row apply đúng một lần, `verify --mode full` 0 lệch; chạy ≥3 lần ở mỗi chế độ relaxed/strict. Regression: các e2e snapshot hiện có + test resume sau kill -9.

### D. Viết lại `docs/USAGE.md` đầy đủ  ⬜ chưa bắt đầu

Người dùng phàn nàn: (1) không hiểu tại sao "phải cho table vào mà không đồng bộ full cả db" — tài liệu không nói `tables.include` là glob (`app.*` = cả database, `*.*` = mọi schema người dùng; schema hệ thống tự bỏ: MySQL `mysql/sys/information_schema/performance_schema` — `internal/pipeline/schema.go` `loadSourceTables`; PG `pg_catalog/information_schema/pg_*` — `internal/pipeline/engine.go` ~132); (2) tài liệu chưa đầy đủ (tiêu đề còn "P1 / Gate 1", P2 bị nhét vào một mục).

**Quy tắc cứng:** mọi câu phải kiểm với code (build `./cmd/strand`, chạy `strand <cmd> --help`, `strand config schema`). Khóa được nhận mà không có tác dụng → ghi rõ. Không hỗ trợ → ghi rõ.

Cấu trúc đề xuất:
1. Tổng quan + ma trận hỗ trợ (MySQL→MySQL, MySQL→PG, PG→PG; PG→MySQL có/không — kiểm): snapshot logic, loại seed, verify sample/delta/full, promote, governor, checkpoint rewind; phiên bản tối thiểu.
2. Chọn phạm vi đồng bộ: một bảng / nhiều / cả DB / cả instance; include/exclude/rename; bảng mới (CREATE/RENAME); đổi include giữa các lần chạy; checklist "đồng bộ cả instance".
3. Chuẩn bị target (sau việc B: mặc định tự tạo; trước đó: phải tạo sẵn — `mysqldump --no-data` không kèm trigger/event, `pg_dump --schema-only`, MySQL→PG theo typemap). Bảng mapping kiểu MySQL→PG đầy đủ từ `internal/typemap` + policy (zero date, bigint unsigned, enum, collation cột khóa, JSON…), những gì `validate` kiểm. DDL về sau: luồng exit 10.
4. Yêu cầu theo engine: biến MySQL (đã có), PG source (`wal_level=logical`, slot/wal_senders, publication `FOR ALL TABLES`/`FOR TABLE`, REPLICA IDENTITY, quyền REPLICATION/SELECT/`pg_logical_emit_message`, `create_slot`, failover slot PG17+, decode trên standby PG16+), quyền target PG.
5. Yêu cầu/giới hạn bảng: cần PK; snapshot logic chỉ PK một cột số nguyên (cho tới khi việc C xong) — **giải thích lý do** (gán chunk cần thứ tự giống hệt collation DB); TOAST; bảng có UNIQUE ngoài PK apply tuần tự; partial update lỗi; TRUNCATE chưa hỗ trợ; không hỗ trợ đồng bộ hai chiều.
6. Tham chiếu cấu hình: mọi khóa (sinh từ code), mặc định, có tác dụng hay không; 3 dạng kết nối (tách trường / URL / DSN, `password_env`).
7. Tham chiếu CLI: mọi lệnh và cờ (`--help`), ví dụ: validate, plan, sync (`--start-at` các dạng), seed plan/apply theo loại (xtrabackup, mysqldump, mydumper bắt buộc `--dump-log`, pgBackRest, WAL-G, `strand export`), snapshot add, verify (sample/delta/full, `--repair`), pause/resume/throttle/checkpoint/top, promote, export, config.
8. Vận hành, tuning, metric (danh sách từ `internal/metrics` + ý nghĩa, chỉ metric thật sự được cập nhật), bảng exit code (`internal/exitcode`) kèm cách xử lý.
9. Công thức đầu-cuối: (a) MySQL→MySQL cả instance bootstrap logic; (b) MySQL→MySQL seed xtrabackup; (c) MySQL→PG cả database; (d) PG→PG seed `strand export`; (e) cutover/promote.
10. FAQ / xử lý sự cố / giới hạn đã biết.

Admin state mới: `starting` → `bootstrapping` → `synced` (+ `paused`, `failed`) — ghi vào tài liệu.

### E. P2-09 Benchmark Gate 2  ⬜ chưa bắt đầu

`TASKS.md` dòng 166: "Đọc S04 §5. AC: chạy 3 governor theo đúng giao thức, xuất JSON." Chạy sau khi máy rảnh (không chạy song song e2e/soak vì tranh tài nguyên làm sai số đo). Governor: `onoff`, `static`, `grace` (GRACE v2, `internal/governor/grace.go`).

### F. Soak lại với driver mới  ⬜

Driver đã sửa (`tools/soak/compare.go`, `--driver-rss-limit-mb` mặc định 2048, `--compare-only`, `--pprof-addr`). Chạy một soak ≥1h (hoặc 12h nếu owner muốn) để có `report.json` với `pass=true`. Lệnh mẫu trong `tools/soak/README.md`. Driver cũng đã sửa lỗi mẫu RSS = 0 khi strand đang restart làm `rss_growth` giả.

---

## 5. Danh sách lỗi đã biết / backlog nhỏ

| # | Mô tả | Mức |
|---|---|---|
| 1 | Merkle: target có cột PK unsigned với row > MaxInt64 (source ≤ MaxInt64) → row đó không được hash | Thấp |
| 2 | Không dùng được chính Strand làm synchronous standby của PG (confirm start-LSN commit record, sync rep chờ end; hold 2PC chặn PREPARE) → đăng ký snapshot khóa chết trong barrier. Ghi vào USAGE như giới hạn | Trung bình |
| 3 | Verify tự động sau seed chưa có cho PG (chỉ log cảnh báo, người dùng chạy `verify --mode full`) | Trung bình |
| 4 | P2-03: ưu tiên 30 s chỉ có unit test; kill -9 giữa lúc re-read chỉ có unit test restart | Thấp |
| 5 | Harness e2e `waitSynced`: trước `f4664a7` có thể trả sớm; sau fix nên ổn — xác nhận rồi gỡ workaround "chờ log bootstrap finished" trong `tests/e2e/pg_toast_pk_test.go` nếu thừa | Thấp |
| 6 | `bootstrap_retention_risk` tắt giãn nhịp chunk khi lag > `lag_slo` — đúng thiết kế nhưng làm e2e khó tạo tải đồng thời; test cần đặt `lag_slo` lớn | Ghi chú |
| 7 | Promote sequence giảm dần trên bảng rỗng: `0 ∓ margin` có thể chạm MINVALUE → setval lỗi (giống trường hợp tăng dần) | Thấp |
| 8 | Logical snapshot PG cần PG14+ (pgoutput `messages`), primary (không phải standby), quyền `pg_logical_emit_message`; PG < 17 watermark dựa vào WAL writer flush | Ghi chú |

**Review ngoài 2026-11-04 — các mục đã kết luận KHÔNG phải lỗi (đừng sửa lại):** 1.1 FORMAT() (code không dùng), 1.3 đổi PK TOAST giữ row đích (đúng 02 §8), 1.4 `%T` trong pkString (một cột luôn một kiểu Go), 2.1 Stream Abort subxact (PG gửi abort trước event của savepoint sau, giống apply worker của PG), 2.2 2PC sau restart (cơ chế hold; test `TestAC3_StreamingAndTwoPhase` phần g), 3.2 collation mặc định DB ICU (luôn deterministic, đã thử PG17), 4.4 spool Truncate (không có code production gọi). Đã sửa: 3.1, 4.1, 1.2, 4.3, 3.3, 1.5, 4.2.

---

## 6. Bản đồ code

| Package | Vai trò |
|---|---|
| `cmd/strand` | CLI (cobra): sync, validate, plan, seed, snapshot, verify, promote, export, pause/resume/throttle/checkpoint/top, config |
| `internal/config` | YAML + env `STRAND_*` + kết nối 3 dạng (`conn.go`), JSON schema (`schema.go`), cảnh báo khóa chưa có tác dụng (`unimplemented.go`) |
| `internal/pipeline` | Điều phối: `pipeline.go` (Run, router, snapshot), `engine.go` (trừu tượng MySQL/PG, snapshot PG), `ddl.go`, `batch.go` (group commit), `verify.go`, `admin.go`, `state.go` (state file local) |
| `internal/reader/mysql`, `internal/reader/pg` | Đọc binlog (go-mysql) / pgoutput v4 (streaming, two-phase, messages) |
| `internal/assemble` | Ghép event thành `model.Txn` |
| `internal/sched` | Scheduler writeset, CO-1/CO-2, chế độ serial (strict) |
| `internal/writer` | Writer MySQL; writer PG (`pg.go`: COPY+MERGE / INSERT ON CONFLICT, partial TOAST; `pg_range.go`: thay range / tập khóa) |
| `internal/snapshot` | Range-state (`range_state.go`), snapshot MySQL (`mysql_snapshot.go`), PG (`pg_snapshot.go`, `pg_watermark.go`), P2-03 (`toast_pk.go`), chunker |
| `internal/checkpoint` | Applied set GTID / LSN thưa (PGAppliedSet), store |
| `internal/seed` | xtrabackup, mysqldump, mydumper, pgBackRest, WAL-G, export, prefetch |
| `internal/spool` | Pebble + tầng bucket S3 (chỉ dùng khi prefetch) |
| `internal/governor` | onoff, static, GRACE v2 |
| `internal/verify` | Merkle full, live (sample/delta/sau seed) |
| `internal/typemap` | Mapping MySQL→PG, validate kiểu/collation |
| `internal/promote` | Promote/failover, sequence (driver MySQL/PG) |
| `internal/validate` | Preflight (biến binlog từng session, quyền, glob bảng) |
| `tools/soak` | Driver soak |
| `tests/e2e` | E2E (harness MySQL `harness_test.go`, PG `pg_harness_test.go`) |

---

## 7. Bài học đã trả giá (đừng lặp lại)

1. **MySQL ghi binlog trước khi engine commit** → event bị bỏ ở range "Chưa đọc" có thể chưa visible với SELECT của chunk. Phải `WAIT_FOR_EXECUTED_GTID_SET` các GTID đã bỏ trên kết nối đọc chunk (quyết định 10).
2. **PG stream commit record trước `ProcArrayEndTransaction`** → low watermark của PG phải là MVCC snapshot (`pg_current_snapshot()`), không phải LSN (`pg_watermark.go`). Test race dùng `pg_receivewal --synchronous`.
3. **`sql_log_bin=0` trên kết nối pool** rò sang lần dùng sau (bug XtraBackup 1062): luôn `SET SESSION sql_log_bin = 1` trước khi trả kết nối. Tương tự `sql_mode` trong test typemap.
4. **Tràn int64**: không bao giờ `end+1`/`max-min+1` khi có thể chạm MaxInt64 (dùng `SpanRows`, `MidPK`, cờ `SnapshotPlan.Finished`).
5. **Admin `synced`** phải phản ánh trạng thái thật (đã sửa `starting`).
6. **Đừng so sánh cuối bằng cách nạp cả bảng vào RAM** (soak OOM).
7. **Vòng lặp shell chạy `go` nhớ `PATH`** — từng có "pass" giả vì `go` không tìm thấy.
8. **Theo dõi đúng PID** khi gửi tín hiệu (pgrep có thể khớp chính shell bọc lệnh) — dùng `pgrep -x` hoặc lưu PID lúc khởi chạy.
9. MySQL 8.4 strict + `NO_ZERO_DATE`: so cột với `'0000-00-00'` là ERROR 1525 — dùng `CAST(col AS CHAR) LIKE …`.
10. Agent `agy` (Gemini CLI) đã dùng cho P2 đợt 1: không đọc được file ngoài worktree (phải chép tài liệu API vào `.ref/`), hay lỗi compile/logic — luôn review và chạy test độc lập.

---

## 8. Bám sát plan: ma trận Gate 2 (spec/01-plan.md dòng 122–128, spec/04-testing.md §5)

P2 chỉ được coi là xong khi **Gate 2** đạt. Bảng dưới đối chiếu từng test bắt buộc của Gate 2 với test hiện có (đã grep ngày 2026-11-04). Cột "Cần làm" là việc còn thiếu — đây là phần **chưa** nằm trong §4 và phải làm trước khi tuyên bố Gate 2.

| # | Test bắt buộc Gate 2 | Hiện có | Cần làm |
|---|---|---|---|
| G2-1 | Chạy trên **PG 16, 17, 18** và MySQL→PG | e2e PG dùng `pgImage()` = `postgres:18` mặc định, đổi bằng `STRAND_E2E_PG_IMAGE` | Chạy toàn bộ e2e PG/MySQL→PG với `STRAND_E2E_PG_IMAGE=postgres:16` và `postgres:17` (ảnh đã có trên máy), ghi kết quả. PG16 không có failover slot / tham số flush của `pg_logical_emit_message` — kiểm đường dự phòng |
| G2-2 | TOAST > 2 KB không bị NULL ghi đè | `tests/e2e/pg_test.go`, `pg_toast_pk_test.go`, `pg_snapshot_test.go` (kiểm `doc` không NULL) | Đủ; giữ trong matrix PG 16–18 |
| G2-3 | Commit đúng tại `consistent_point` (không trùng, không hổng) | `internal/reader/pg/pg_test.go`, `internal/seed/export_integration_test.go`, `pgbackrest_test.go`, e2e `TestP2_E2E_PGSeedExportStartAtSeed` | Kiểm lại có ca Txn commit **đúng tại** LSN P (applied set ≤ P−1, gap check `cf > lwd+1`) |
| G2-4 | Failover rồi insert ngay không trùng key | `internal/promote/pg_driver_test.go` (+ `descending_test.go`) | Cần e2e: PG→PG và MySQL→PG dưới tải → `promote` → ứng dụng insert ngay trên target, 0 lỗi duplicate key (bảng identity + sequence độc lập) |
| G2-5 | Collation `_ci` sang `C`, 0 lệch | `tests/e2e/mysql_pg_test.go` (bảng PK varchar `utf8mb4_0900_ai_ci`), `internal/typemap/*` | Hiện bảng đó chỉ đi qua stream (rỗng lúc đầu). Sau việc C, thêm ca có dữ liệu sẵn + snapshot |
| G2-6 | Transaction 30 phút ở chế độ `relaxed` | **Không có** | Viết e2e: một Txn mở 30 phút (ghi rải rác, có lúc im lặng) trên PG và MySQL→PG ở relaxed, các Txn khác chạy song song; checkpoint không phình (02 §6: delta ≤ 64 KB, holes ≤ 4096), không mất, 0 lệch. Có thể chạy với biến môi trường rút ngắn trong CI nhưng bản gate phải đủ 30 phút |
| G2-7 | Đổi PK với partial TOAST, kể cả PK mới trong range "đang đọc" | `tests/e2e/pg_toast_pk_test.go` (3/3 relaxed, 3/3 strict), unit `toast_pk_test.go`, mô phỏng `toast_pk_sim_test.go` | Thêm e2e kill -9 giữa lúc re-read; e2e chứng minh ưu tiên 30 s (hiện chỉ unit) |
| G2-8 | 100 transaction × 49.999 khóa không OOM | Chỉ unit `internal/sched/scheduler_test.go` | Viết e2e đo RSS: 100 Txn, mỗi Txn 49.999 khóa (ngay dưới ngưỡng `table_level_threshold` 50.000), cả MySQL→PG và PG→PG; RSS đỉnh có giới hạn (ghi số), `writeset_budget` hoạt động |
| G2-9 | Breaker khi RLS chưa đủ dữ liệu bật `strand_sensitivity_unknown` | `internal/governor/grace_test.go`, `tests/e2e/metrics_test.go` | Kiểm e2e có thật sự đưa GRACE vào trạng thái thiếu dữ liệu và quan sát metric = 1 (không chỉ kiểm metric tồn tại) |
| G2-10 | `promote` khi source không truy cập được với `CACHE 1000` → dừng và hỏi (exit 10) | `internal/promote/pg_driver_test.go` | Thêm e2e: source PG/MySQL bị cắt (toxiproxy hoặc dừng container) → `promote` trả exit 10 với thông tin quyết định |
| G2-11 | Hot row 1.000 update/s báo `unverifiable_hot`, nhưng lệch thật cài vào vẫn bị phát hiện | `tests/e2e/metrics_test.go` (kiểm metric) | Viết e2e: 1 row nóng 1.000 update/s + cài lệch thật vào một row khác ở target → `verify` báo `unverifiable_hot` cho row nóng **và** báo mismatch cho row bị cài |
| G2-12 | Benchmark 3 governor (`static`, `onoff`, `grace-v2`), giao thức cố định | Chưa có (việc E) | Theo 04 §5: sysbench/pgbench p99 phía client, tải sóng vuông 10 phút + spike, 3 lần/governor thứ tự ngẫu nhiên, khôi phục source về cùng snapshot mỗi lần; đo thời gian snapshot, p99, số lần vượt hard limit, peak-to-peak log R (grace ≤ 0,3); quy tắc: grace mặc định khi snapshot ngắn hơn onoff ≥ 30% và p99 không xấu hơn; xuất JSON + biểu đồ từ dữ liệu thật. **Không đổi giao thức sau khi thấy số** (nếu đổi, ghi lý do) |

Tương tự cho **Gate 1** (dòng 113–120): e2e mặc định chạy `mysql:8.4` (`mysqlImage()` trong `harness_test.go`); phải chạy thêm toàn bộ e2e MySQL với `STRAND_E2E_MYSQL_IMAGE=mysql:9.7` và smoke với `STRAND_E2E_SOURCE_IMAGE=mysql:8.0` (ảnh đã có trên máy) rồi ghi vào báo cáo gate — **chưa xác nhận đã từng chạy**; "seed bảng 1 TB trên staging, 0 IOPS đọc bảng" cần staging thật (không làm được trên máy dev — ghi rõ là chờ owner).

Rủi ro trong `spec/01-plan.md` §7 gắn Gate 2: #4 TOAST, #5 đổi PK TOAST, #6 RAM writeset, #7 checkpoint phình, #9 breaker RLS, #10 collation, #11 hot row — mỗi rủi ro phải trỏ được tới một test ở bảng trên trong báo cáo Gate 2.

---

## 9. Checklist review — áp dụng cho MỌI thay đổi trước khi merge

Người review (bạn) không tin báo cáo của agent/người viết; tự chạy lại.

**Tính đúng**
- [ ] Đọc lại đoạn spec liên quan (ghi số mục 02 §x trong mô tả commit/báo cáo). Lệch spec → có dòng trong DECISIONS kèm lý do và ví dụ phản chứng.
- [ ] Bất biến bị chạm (CP/RS/SD/CO) được nêu và có lập luận trong comment code.
- [ ] Không đọc trạng thái "tương lai" vượt checkpoint (lý do 02 §8 cấm point read).
- [ ] Mọi lỗi I/O/SQL được trả lên — không `err == nil && …` nuốt lỗi; không `_ =` với lỗi có ý nghĩa.
- [ ] Số học khóa: không `x+1`, `max-min+1`, `(a+b)/2` khi có thể tràn int64.
- [ ] Kết nối pool: mọi `SET SESSION` đổi trạng thái (sql_log_bin, sql_mode, time_zone, FOREIGN_KEY_CHECKS) được trả lại hoặc dùng `db.Conn` riêng rồi đóng.
- [ ] Khóa (mutex) — không vòng khóa giữa routeMu / planMu / range-state mu / failMu.
- [ ] Trạng thái hiển thị (admin, metric, log) phản ánh đúng sự thật, không báo "xong/synced" sớm.

**Test**
- [ ] Regression test thất bại ở code cũ (tự kiểm bằng `git stash` phần sửa hoặc revert tạm) và qua ở code mới.
- [ ] Không `t.Skip`; không sleep cố định thay cho điều kiện; điều kiện tiền đề của test (ví dụ "snapshot phải chạy dưới tải") được kiểm và báo fail rõ ràng.
- [ ] E2E tính đúng chạy ở **cả** `relaxed` và `strict`; việc chạm snapshot/pipeline chạy ≥3 lần.
- [ ] Có ca kill -9 / restart nếu thay đổi có trạng thái bền (plan, checkpoint, state file).
- [ ] Unit (28 package) + full e2e cả hai chế độ trên **nhánh tích hợp sau merge**, không chỉ trên nhánh tính năng.
- [ ] Ghi số liệu thật: số test qua/lỗi, thời gian, SEED của test lỗi.

**Vận hành / tài liệu**
- [ ] Khóa cấu hình mới: struct + JSON schema (`internal/config/schema.go`) + env override + `docs/USAGE.md` + `strand config schema`.
- [ ] Cờ CLI mới: `--help` đúng, ghi vào USAGE và DECISIONS mục 6 (cờ mới cho 02 §10).
- [ ] Metric mới: thật sự được cập nhật (không hằng 0), có trong tài liệu.
- [ ] Exit code: đúng bảng `internal/exitcode`, đúng nhóm (Rejected/DataError/NeedsDecision…) — exit 2/3/4/5/10 không được systemd tự restart (03 §4).

**Merge**
- [ ] Merge `--no-ff` vào `review/gate1-fixes`; xung đột ở `internal/snapshot`/`internal/pipeline` phải giữ ý nghĩa của **cả hai** bên (xem ví dụ merge `3645f78`: giữ `Finished` + `Reread`, vòng walk dùng `mainDone`).
- [ ] Sau merge: build/vet/unit/full e2e lại rồi mới đẩy `main`.

### Checklist riêng cho từng việc còn lại

**B (schema-init)** — review kỹ: (1) không bao giờ sửa/xoá bảng đã có ở target; (2) trigger/event/routine không bị copy; (3) MySQL→PG: `strand validate` qua trên schema sinh ra, writer ghi đúng tên (quote/case), promote tìm được sequence identity qua `pg_depend`; (4) idempotent khi chạy lại/khi crash giữa chừng (một nửa bảng đã tạo); (5) quyền: user `strand` cần CREATE ở target — cập nhật bảng quyền 03 §2 trong USAGE; (6) `CREATE TABLE` trong stream: bảng được tạo đúng tại barrier, không có row nào của bảng mới bị bỏ trước khi tạo; (7) `tables.rename` được tôn trọng.

**C (generic PK)** — review kỹ: (1) có test chứng minh thứ tự mã hoá == `ORDER BY` của DB thật cho từng kiểu/collation (không chỉ test Go với Go); (2) PAD SPACE: `'a'` và `'a '` là cùng khóa dưới `utf8mb4_bin`/`_ci` PAD SPACE; (3) `'a'`/`'A'`/`'á'` cùng khóa dưới `_0900_ai_ci`; (4) event đổi PK giữa hai khóa "bằng nhau theo collation" (`'a'` → `'A'`) — DB coi là UPDATE cùng row hay đổi PK? xử lý đúng; (5) BufferedChanges, `Reread`, plan JSON, metric đều dùng khóa mã hoá; state file cũ (int64) vẫn đọc được; (6) benchmark đường int64 trước/sau; (7) Merkle cross-engine với khóa chuỗi ci: hoặc đúng hoặc từ chối rõ.

**D (docs)** — review: chọn ngẫu nhiên ≥15 khẳng định trong tài liệu và kiểm với code/`--help`; mọi khóa trong `strand config schema` có mặt; không có khóa nào được mô tả "có tác dụng" mà `internal/config/unimplemented.go` cảnh báo là chưa có.

**E (benchmark)** — review: giao thức được commit **trước** khi chạy; dữ liệu thô (JSON) được lưu; biểu đồ sinh từ dữ liệu; không chạy song song việc khác.

---

## 10. Nhật ký vấn đề đã gặp (triệu chứng → nguyên nhân gốc → sửa → test)

| # | Triệu chứng | Nguyên nhân gốc | Sửa | Test giữ lại |
|---|---|---|---|---|
| 1 | Snapshot add dưới tải thiếu row (`pk tồn tại ở source nhưng thiếu ở target`), hiếm | MySQL ghi binlog trước engine commit: event bị bỏ (range chưa đọc) chưa visible với SELECT chunk | Chờ GTID đã bỏ (`WAIT_FOR_EXECUTED_GTID_SET`) trước SELECT; RS-1: event nằm trong low watermark thì xoá luôn ảnh buffer cũ hơn (quyết định 10) | `TestRS1_*` trong `internal/snapshot/rs2_wait_test.go`; e2e `TestG1_E2E_SnapshotAddUnderLoadWithPKChangeAcrossRanges` |
| 2 | Seed XtraBackup: lỗi 1062 duplicate sau restore | Kết nối bulk-load đặt `sql_log_bin=0` rồi trả về pool | `SET SESSION sql_log_bin = 1` trước Close. (Giả thuyết sai đã loại: PS-7873, binlog truncation, XA recovery) | `tests/e2e/xtrabackup_test.go` |
| 3 | Soak treo không kết thúc | Supervisor gắn với `soakCtx` đã huỷ | Dùng rootCtx, fail nhanh khi supervisor chết | `tools/soak` |
| 4 | Test prefetch C lỗi exit 5 | Test purge binlog sau restore (sai kịch bản); pipeline nuốt lỗi đọc spool | Marker `restore.done`; trả lỗi đọc spool; KeyRanges gộp range | `tests/e2e/prefetch_test.go` |
| 5 | Validate từ chối `app.*` | Glob không được mở rộng trong validate | `ExpandTables` | `internal/validate` |
| 6 | Vòng lặp tái hiện báo "pass" giả | `go` không có trong PATH của shell vòng lặp | Luôn `export PATH` | — |
| 7 | AIO cạn, container MySQL không khởi động | Quá nhiều container cũ | `--innodb-use-native-aio=OFF`; dọn container (hỏi người dùng) | harness |
| 8 | Writer PG: MERGE vi phạm unique khi hoán đổi giá trị UNIQUE | MERGE gộp ảnh của nhiều row | Bảng có UNIQUE/exclusion ngoài PK apply tuần tự (quyết định 16) | `internal/writer` |
| 9 | Reader PG: tag two-phase sai; P2-04 `colliculocale` đổi tên ở PG17 và lỗi bị nuốt | Code agy | Đọc qua `to_jsonb`; trả lỗi | `internal/typemap`, `internal/reader/pg` |
| 10 | GRACE: breaker giảm một nửa mỗi tick | Logic agy | Sửa theo §7 spec 02 | `internal/governor/grace_test.go` |
| 11 | Soak 12h không có report | Driver nạp 65M row × 2 vào map (~536 B/row) → OOM-kill 21 GB | So sánh streaming, giới hạn RSS driver | `tools/soak/compare_test.go` |
| 12 | SIGTERM tới strand cứu soak không có tác dụng | `pgrep -f` khớp shell bọc lệnh | Dùng PID lưu lúc chạy | — |
| 13 | `TestP2_E2E_MySQLToPGSnapshotAndStream` flaky "snapshot finished before the load started" | Admin báo `synced` trước khi pipeline bật cờ bootstrap | Trạng thái `starting` (`f4664a7`) | `TestAdminStateStartingBeforeRunning` |
| 14 | Validate cho PASS bảng có zero date | So `= '0000-00-00'` lỗi 1525 dưới NO_ZERO_DATE, lỗi bị nuốt | `CAST … LIKE`, trả lỗi scan | `TestReview31_*` |
| 15 | Snapshot/verify treo hoặc coi bảng là rỗng khi PK chạm MaxInt64 | Tràn int64 | `Finished`, `SpanRows`, `MidPK`, break tường minh | `TestReview41_*`, `TestKeyRangesInt64Extremes` |
| 16 | P2-03: "chờ hai range flush rồi SQL điều kiện" (theo spec) làm mất row / row sống lại | Spec không xét range khóa cũ chưa đọc và event sau chạm khóa mới | Re-read khóa mới qua giao thức chunk (quyết định 18) | `toast_pk_*_test.go`, `pg_toast_pk_test.go` |
| 17 | P2-03 e2e strict "not synced after 10m" (2 lần, trước khi sửa test) | Tải không giới hạn + strict một worker → lag mãi; chưa loại trừ hoàn toàn khả năng khoá chết (không có goroutine dump) | Giới hạn tải 60 s; watcher SIGQUIT chưa từng kích hoạt sau đó | Theo dõi: nếu gặp lại, lấy `kill -QUIT` goroutine dump |
| 18 | Logical snapshot PG với sync rep trỏ vào Strand khoá chết | Strand xác nhận start-LSN, sync rep chờ end-LSN; hold 2PC | Chưa sửa — giới hạn đã biết | test dùng `pg_receivewal` thay |

| 62 | Runner benchmark đề xuất GRACE khi telemetry thiếu/NaN hoặc chưa đủ lần đo | Decision bỏ NaN, thiếu kiểm completeness | Fail incomplete, yêu cầu coverage/repeat/client p99, maximum p2p | `TestReviewBenchRejectsIncompleteEvidence`, `TestReviewBenchAcceptsCompleteEvidence` |

Khi gặp lỗi mới: thêm một dòng vào bảng này trong cùng commit sửa lỗi.

---

## 11. Cách báo cáo cho người dùng

Người dùng đọc tiếng Việt. Báo cáo ngắn, có số liệu thật (số test qua/lỗi, thời gian, commit hash), nói rõ việc nào chưa kiểm. Hỏi trước khi: đẩy lên `main`, xoá container không phải của mình, thay đổi lệch spec lớn.
