# TASKS — Strand CDC

Agent làm lần lượt từ trên xuống, mỗi task một nhánh `agent/<ID>`, tuân thủ GEMINI.md.
Xong một task: chạy `bash scripts/check.sh <ID>`, viết `reports/<ID>.md`, merge vào main rồi làm task kế tiếp.
Gặp dòng **DỪNG** thì dừng hẳn, chờ người dùng.
Viết tắt: S02 = spec/02-specs.md, D = spec/design.md, S01/S03/S04/S05 = các file spec còn lại; "§n" là mục n.

---

## PHASE 0 — Giảm rủi ro

### P0-01 Môi trường docker compose
Đọc: S03 §2, §3.
Làm: `dev/compose.yaml` + `dev/init/` dựng 7 service có healthcheck: mysql-src (mysql:8.4, GTID, binlog ROW/FULL, binlog_row_metadata FULL), mysql-dst (mysql:8.4, read_only=ON, super_read_only=OFF), pg-src (postgres:18, wal_level=logical), pg-dst (postgres:18), toxiproxy, victoriametrics, grafana. Tạo user `strand` với quyền đúng S03 §2. Thêm target `up`, `down` vào Makefile.
AC:
- AC1 `make up` xong, mọi service healthy trong ≤ 300 s.
- AC2 mysql-src: gtid_mode=ON, binlog_row_image=FULL, binlog_row_metadata=FULL.
- AC3 User `strand` có trên cả 4 DB; trên pg-src có REPLICATION.
- AC4 mysql-dst: read_only=1, super_read_only=0.
- AC5 `make down` không để lại volume.

### P0-02 Lấy binlog production — **người dùng làm, agent bỏ qua**

### P0-03a Audit go-mysql
Đọc: D §9 (audit thư viện), S02 §3.
Làm: `docs/audit/go-mysql.md` + test trong `tests/audit/mysql/`. Ghim đúng một commit SHA của thư viện.
AC:
- AC1 Đọc được binlog qua stream có tagged GTID của MySQL 8.4.
- AC2 Parse đủ event của MySQL 9.7 (testcontainers mysql:9.7).
- AC3 Resume bằng `COM_BINLOG_DUMP_GTID` với GTID set có lỗ: server gửi lại đúng các transaction còn thiếu.
- AC4 Mọi khẳng định trong báo cáo trỏ tới file:dòng tại SHA đã ghim, hoặc tới test.
- AC5 Báo cáo có mục "Khuyến nghị fork: có/không + lý do". Quyết định thuộc người dùng.

### P0-03b Audit pglogrepl
Đọc: D §9, S02 §3, §5.
Làm: `docs/audit/pglogrepl.md` + `tests/audit/pg/`.
AC:
- AC1 pgoutput proto v1–v4 chạy trên PG 16, 17, 18.
- AC2 Nhận đúng transaction đang streaming (proto v2+).
- AC3 Tạo slot kèm exported snapshot; `consistent_point` khớp vị trí bắt đầu stream.
- AC4 Logical decoding trên standby PG16+.
- AC5 Báo cáo có trích dẫn và mục khuyến nghị fork, như P0-03a.

### P0-04 Kiểu dữ liệu lõi và schema seed.json
Đọc: S02 §1, §2 (CP-3), §3, §9.
Làm: `internal/model` (Pos, Txn, RowChange, ColValue.Unchanged), `internal/checkpoint` (AppliedSet cho GTID set của MySQL và LSN + holes của PG), `schema/seed.schema.json`.
AC:
- AC1 Property test: Merge giao hoán, kết hợp, lũy đẳng; Contains đúng sau Merge.
- AC2 TestCP3: serialize Txn/Checkpoint không có trường `seq`.
- AC3 Lwd/High/Holes đúng với các chuỗi Add ngẫu nhiên.
- AC4 Schema chấp nhận ví dụ seed.json ở S02 §9, từ chối khi thiếu trường bắt buộc.

### P0-05 Prototype writer theo writeset + công cụ replay
Đọc: S02 §6, D §6.2, S04 §5 (Gate 0).
Làm: `internal/sched` (Admit đúng S02 §6), `cmd/replay` đọc file binlog → lắp Txn → scheduler → apply vào MySQL target, có cờ `--workers`. `tools/genload` sinh tải có hot row. Xuất JSON gồm txn/s, rows/s, histogram độ rộng DAG, tỉ lệ hot row.
AC:
- AC1 TestCO1: hai Txn có writeset giao nhau commit đúng thứ tự source.
- AC2 Txn vượt 50.000 khóa được nâng lên khóa cấp bảng; map row được dọn.
- AC3 Replay tải genload từ mysql-src sang mysql-dst: checksum từng bảng bằng nhau.
- AC4 Chạy được với `--workers` 4, 8, 16 và xuất file JSON số liệu.

### P0-06 Thí nghiệm watermark (RS-2)
Đọc: S02 §2 (RS-2), §5; D §6.1.
Làm: `tests/experiments/watermark/`. Writer đồng thời ghi lại GTID của chính mình (session_track_gtids). Reader đọc low = gtid_executed, chạy SELECT, rồi kiểm mọi row có GTID ∈ low đều thấy được.
AC:
- AC1 Chạy N vòng (mặc định 10.000) trên MySQL 8.4 và 9.7; xuất JSON {iterations, violations}.
- AC2 Tự kiểm: bản cố ý đọc low SAU câu SELECT phải phát hiện violations > 0.

### P0-07 Mô hình TLA+
Đọc: S02 §2, §12.
Làm: `tla/` gồm Scheduler, Checkpoint, RangeState, Handover; `tla/run.sh` chạy TLC với biên ở S02 §12.
AC:
- AC1 Mỗi module kiểm đúng các bất biến ghi ở S02 §12, tên bất biến trùng mã (CP1, RS1…).
- AC2 TLC không tìm thấy vi phạm ở biên đã ghi.
- AC3 Mỗi module có một biến thể cố ý cài lỗi, và TLC PHẢI tìm ra phản ví dụ.

### P0-08 Đo sizing, P0-10 Báo cáo Gate 0 — **người dùng làm**

**DỪNG — Gate 0. Chờ người dùng xác nhận trước khi sang P1.**

---

## PHASE 1 — MVP MySQL→MySQL

### P1-01 Cấu hình và khung CLI
Đọc: S02 §10, §12. Làm: Cobra CLI, decode YAML strict, `strand config schema`, khung `validate`, bảng exit code.
AC: key lạ trả exit 2; `config schema` in JSON Schema hợp lệ; mỗi exit code trong S02 §10 có test.

### P1-02 Reader MySQL
Đọc: S02 §3, D §1. AC: đọc stream có tagged GTID; resume từ AppliedSet đúng; đổi replica giữa hai lần chạy vẫn resume đúng (TestCP2).

### P1-03 Txn Assembler
Đọc: S02 §3. AC: lắp đúng Txn nhiều bảng; UPDATE đổi PK có OldKey; Txn 50 triệu row không giữ hết trong RAM.

### P1-04 Spool Pebble (tầng local)
Đọc: S02 §3, D §1. AC: Append/Iter/Truncate đúng sau restart; Usage báo đúng số byte.

### P1-05 Bảng metadata, khóa pipeline, epoch
Đọc: S02 §4 (quy tắc ghi 1–2). AC: TestSG1: instance thứ hai trả `pipeline_locked`; epoch tăng mỗi lần khởi động; tạo đủ 3 bảng trên MySQL và PG target.

### P1-06 Checkpoint toàn cục và compaction
Đọc: S02 §4 (quy tắc 4), D §1. AC: TestCP1 kill ở mọi bước compaction đều không mất; delta ≤ 64 KB, vượt thì compaction đồng bộ; holes > 4.096 thì scheduler ngừng nhận (TestCP4).

### P1-07 Writer ghi delta có điều kiện epoch
Đọc: S02 §4 (quy tắc 3, 6). AC: epoch cũ thì rollback, trả `fenced`; delta ghi cùng transaction với dữ liệu; MySQL không bật CLIENT_FOUND_ROWS vẫn đếm đúng.

### P1-08 Scheduler writeset đầy đủ
Đọc: S02 §6. AC: writeset gồm PK, unique key, FK; vượt `writeset_budget` thì bảng lớn nhất chuyển sang cấp bảng (TestCO2); 100 Txn × 49.999 khóa không OOM.

### P1-09 Writer MySQL và group commit
Đọc: S02 §6. AC: multi-row upsert; một Txn nguồn không bị tách; chế độ `strict` cho đích luôn là prefix của source (TestCP5).

### P1-10 Snapshot chunk + range-state (MySQL)
Đọc: S02 §5, D §6.1. AC: TestRS1 `snapshot add` dưới tải cập nhật dày, 0 lệch; SELECT > 5 s thì chia đôi chunk, chunk 1 row thì không áp timeout; row lớn hơn `max_chunk_bytes` không gây vòng lặp; chunk đuôi xử lý đúng.

### P1-11 UPDATE đổi PK qua hai range
Đọc: S02 §5, §8; D §6.1. AC: PK cũ và PK mới ở hai range khác trạng thái: đích không còn PK cũ, 0 lệch; đổi liên tiếp 1→2→3 đúng.

### P1-12 Seed từ XtraBackup
Đọc: S02 §9, D §3.3. AC: `seed plan` đọc P từ metadata, đối chiếu chéo; backup hỏng (thiếu file, sai GTID, log có khoảng trống) bị từ chối 100%; backup XtraBackup bản RC bị từ chối nếu không có `--allow-prerelease-tool`.

### P1-13 Seed từ mydumper và CLONE
Đọc: S02 §9, D §3.3. AC: như P1-12; mydumper chế độ `NO_LOCK` bị từ chối.

### P1-14 Archive catch-up, prefetch, spool tràn
Đọc: S02 §9. AC: xóa archive trong lúc `seed apply` thì prefetch vẫn cứu được; tắt prefetch thì báo `log_gap`; mọi tầng spool đầy thì báo `prefetch_exhausted`; spool tràn sang bucket (MinIO trong compose).

### P1-15 Verify sample/delta
Đọc: D §7, S02 §12. AC: phát hiện lệch được cài sẵn; hot row được báo `unverifiable_hot`, không báo lệch giả.

### P1-16 Governor onoff + breaker
Đọc: S02 §7. AC: tín hiệu đọc trực tiếp từ DB; P ≥ 1 thì dừng background, P < 0,9 trong 3 tick thì chạy lại; breaker cắt R còn 50%; không phụ thuộc metrics_source.

### P1-17 Promote
Đọc: S02 §8, S05 §5. AC: ứng dụng không ghi được trước khi setval xong; chạy lại được từ bước đang dở; source không truy cập được thì trả exit 10 `promote_needs_decision`.

### P1-18 Admin socket và lệnh vận hành
Đọc: S02 §10. AC: `top`, `pause`, `resume --snapshot-only`, `throttle set`, `checkpoint show|rewind` hoạt động qua socket.

### P1-19 Metric và /healthz
Đọc: S02 §11, §12. AC: đủ metric trong S02 §11, tên đúng; `/healthz` trả 200 khi vòng lặp chính còn tiến.

### P1-20 Unit systemd và manifest FKE
Đọc: S03 §4, §5. AC: unit có `RestartPreventExitStatus=2 3 4 5 10`; SIGTERM thì ghi checkpoint rồi thoát trong thời gian cho phép.

### P1-21 Checker độc lập + bộ fault injection
Đọc: S04 §3, §4. AC: 5 phép kiểm của S04 §3; checker bắt được 3 bản cài lỗi (bỏ Txn, apply trùng, đảo thứ tự); các kịch bản fault của S04 §4 chạy được bằng `make fault`.

### P1-22 Bộ test Gate 1
Đọc: S01 §6 (Gate 1). AC: mỗi test Gate 1 có một test tự động; 1.000 lần kill -9 cho kết quả 0 lệch.

**DỪNG — Gate 1. Chờ người dùng.**

---

## PHASE 2 — PostgreSQL, MySQL→PG, GRACE

### P2-01 Reader pgoutput, decode trên standby — AC: resume từ slot; failover slot PG17+.
### P2-02 Writer PG (COPY + MERGE), partial TOAST — AC: cột TOAST > 2 KB không bị ghi đè NULL.
### P2-03 Đổi PK khi TOAST không đổi — Đọc S02 §8. AC: SQL điều kiện đúng; `max_pending_cross_range` và đẩy ưu tiên sau 30 s.
### P2-04 MySQL→PG: mapping kiểu, collation, múi giờ — Đọc S02 §8. AC: `_ci` sang `C` 0 lệch; đích lỏng hơn source thì `validate` từ chối.
### P2-05 Seed pgBackRest/WAL-G + `strand export` — Đọc S02 §9. AC: PITR đúng LSN của slot; commit tại đúng `consistent_point` không trùng, không hổng.
### P2-06 Sequence khi promote — Đọc S02 §8. AC: sequence độc lập + `CACHE 1000`, source mất kết nối thì trả exit 10; insert ngay sau promote không trùng key.
### P2-07 GRACE v2 — Đọc S02 §7. AC: đúng luật 3 zone; P_lag theo phase; snapshot PG với lag_slo nhỏ không livelock; `strand_sensitivity_unknown` hoạt động.
### P2-08 Verify full bằng cây Merkle — AC: phát hiện lệch đơn lẻ trong bảng lớn.
### P2-09 Benchmark Gate 2 — Đọc S04 §5. AC: chạy 3 governor theo đúng giao thức, xuất JSON.

**DỪNG — Gate 2. Chờ người dùng.**

---

## PHASE 3 — Thích ứng

### P3-01 Schema evolution + `schema diff/approve` — AC: thay đổi cộng thêm tự áp; thay đổi phá hủy dừng chờ approve.
### P3-02 Nhận diện online DDL gh-ost/pt-osc — Đọc D §7. AC: đích nhận đúng 1 ALTER tại barrier, 0 lệch.
### P3-03 `strand archive binlog` — AC: archive liên tục, catch-up đọc được.
### P3-04 Sink Kafka có transaction — AC: không trùng, không mất khi kill.
### P3-05 Gói .deb/.rpm — AC: cài và chạy unit systemd được.

**DỪNG — Gate 3. Chờ người dùng.**

---

## PHASE 4 — Vận hành tập trung

### P4-01 HA bằng lease etcd + fencing epoch — AC: kill active thì instance khác nhận trong TTL, writer cũ bị chặn, 0 lệch.
### P4-02 Control plane gRPC/REST — AC: tạo, xem, dừng pipeline qua API.
### P4-03 Helm chart FKE — AC: cài lên cluster test, pipeline chạy được.
### P4-04 Web UI — AC: hiện trạng thái, lag, tiến độ seed, zone governor, kết quả verify.

**DỪNG — Gate 4. Hết backlog.**
