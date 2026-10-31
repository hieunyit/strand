# Strand CDC — 01 Implementation Plan

Oct 31, 2026 · @hieuny

## 1. Phạm vi & nguồn sự thật

Kế hoạch 37 tuần, 5 phase (P0–P4): P0 chi tiết đến task theo tuần, P1–P4 ở mức epic + gate vì breakdown chỉ đáng tin sau khi có kết quả Gate 0.

- **Nguồn sự thật:** [Strand CDC — Kiến trúc Tổng thể & Kế hoạch Triển khai](https://claude.ai/artifact/GKQWmLMMgJ6M1MMHcmsJ1V), rev 89. Khi kế hoạch và thiết kế lệch nhau, thiết kế thắng; điểm lệch ghi vào mục 7 để sửa một trong hai.
- **Tên gọi, invariant, interface:** theo Strand CDC — 02 Technical Specifications; tài liệu này không tự định nghĩa thuật ngữ.
- **Định vị:** công cụ niche DB→DB — MySQL→MySQL, PG→PG, MySQL→PG — cho bảng cỡ TB đã có backup, IOPS source hạn chế, không cần Kafka. Không thay replication native cho DR cùng engine khi native đủ dùng; không cạnh tranh Debezium/Flink ở bài toán stream ra Kafka hay data lake.

| Tài liệu | Nội dung | Đợt |
| --- | --- | --- |
| 01 Implementation Plan | Phase, gate, phụ thuộc, rủi ro | 1 |
| 02 Technical Specifications | Glossary, invariant, interface, schema, metrics contract | 1 |
| 03 Deployment Guide | FKE, VM / bare metal, docker compose | 2 |
| 04 Testing Strategy | Fault injection, checker tính đúng, benchmark | 2 |
| 05 Operations Manual | Runbook, alert, promote, sự cố | 2 |

## 2. Ma trận hỗ trợ phiên bản theo phase

Mục tiêu chính là hai dòng LTS MySQL 8.4 và 9.7, cùng PG 16–18. MySQL 8.0 chỉ là source best-effort, PG 19 ở mức thử nghiệm. Một ô chỉ được ghi "Chính" khi nó có trong CI matrix và đã qua test của gate tương ứng.

| Phiên bản | Vai trò | Vào từ | Mức hỗ trợ | Công cụ seed | Ghi chú |
| --- | --- | --- | --- | --- | --- |
| MySQL 8.4 LTS | Source + target | P1 | Chính | XtraBackup 8.4.x, CLONE, mydumper | Có tagged GTID |
| MySQL 9.7 LTS | Source + target | P1, cuối phase | Chính | CLONE, mydumper; XtraBackup 9.7 chỉ khi có bản GA | 9.7.0 GA ngày 21/4/2026 |
| MySQL 8.0 (8.0.46) | Chỉ source | P1 | Best-effort, dùng để di chuyển dữ liệu ra | XtraBackup 8.0.35-xx | EoL ngày 30/4/2026 |
| MySQL 9.0–9.6 | — | — | Không hỗ trợ | — | Bản Innovation; nâng lên 9.7 |
| PostgreSQL 16 | Source + target | P2 | Chính | pgBackRest, WAL-G | Decode trên standby |
| PostgreSQL 17 | Source + target | P2 | Chính | pgBackRest, WAL-G | Failover slot, P4 dùng cho HA |
| PostgreSQL 18 | Source + target | P2 | Chính | pgBackRest, WAL-G | GA ngày 25/9/2025 |
| PostgreSQL 19 | Source + target | P2 | Thử nghiệm | pgBackRest, WAL-G | Beta 4 ngày 24/9/2026 |
| PostgreSQL 14/15 | — | — | Không hỗ trợ | — | PG 14 hết fix ngày 12/11/2026 |

- **MySQL→PG:** CI chạy source MySQL 8.4 và 9.7 sang target PG 17 và 18. Ba ghép cặp còn lại chỉ chạy nightly.
- **PG 19:** nâng lên "Chính" khi có GA cộng một bản minor. Bản beta đã có logical replication cho giá trị sequence. Nếu tính năng này vào GA, `promote` có thể dùng nó thay cho setval tính margin; việc này ghi là tùy chọn ở P2.
- **XtraBackup 9.7:** hiện mới có 9.7.1-rc1. `seed plan` phải từ chối backup tạo bằng bản RC, trừ khi có cờ `--allow-prerelease-tool`.
- **Ràng buộc build:** linux/amd64 và linux/arm64; binary < 50 MB; RAM request 1–2 GB, limit 4 GB. Số RAM chốt sau khi đo ở P0.

Nguồn: [MySQL 8.0 release notes](https://dev.mysql.com/doc/relnotes/mysql/8.0/en/index.html) · [MySQL 9.7.0 LTS](https://www.mysql.com/news-and-events/) · [PostgreSQL 18](https://www.postgresql.org/about/featurematrix/detail/380) · [PostgreSQL 19 Beta 1](https://postgresql.org/about/news/postgresql-19-beta-1-released-3313/) · [postgresql.org (Beta 4, lịch PG 14)](https://www.postgresql.org/?lang=ja-JP) · [XtraBackup 9.7.1-rc1](https://docs.percona.com/percona-xtrabackup/9.7/release-notes/9.7.1-rc1.html)

## 3. Ma trận đích triển khai theo phase

Có ba đích triển khai. Docker compose có từ P0 và chỉ dùng cho dev, CI và PoC. VM / bare metal là đích production đầu tiên, từ P1. FKE chạy được từ P1 bằng manifest tối thiểu; Helm chart chính thức ra ở P4. Chi tiết từng đích nằm trong tài liệu 03.

| Đích | Mục đích | Từ phase | Gói phân phối | Spool | Chỉ một instance active |
| --- | --- | --- | --- | --- | --- |
| Docker compose | Dev, CI e2e, PoC | P0 | `compose.yaml`: MySQL 8.4/9.7, PG 16–18, Toxiproxy, VictoriaMetrics, Grafana | Named volume | Không áp dụng |
| VM / bare metal | Production | P1 | Binary tĩnh + unit systemd; `.deb`/`.rpm` từ P3 | SSD/NVMe local, có fsync | Khóa trên target (P1–P3), lease etcd (P4) |
| FKE | Production | P1 manifest, P4 Helm | StatefulSet `replicas: 1` + PVC; Helm chart ở P4 | PVC block storage, không dùng NFS | Khóa trên target (P1–P3), lease etcd (P4) |

- **Chặn chạy trùng trước P4 (đề xuất mới):** khi khởi động, pipeline giữ một khóa theo `pipeline_id` trên target — `GET_LOCK` với MySQL, `pg_advisory_lock` với PG — suốt vòng đời kết nối. Khóa chỉ chặn lúc khởi động, và StatefulSet cũng không đảm bảo một pod duy nhất khi node bị phân vùng mạng. Vì vậy mọi transaction ghi còn kiểm tra cột epoch (fencing token), để chặn writer cũ đang chạy dở (02, mục 4). Cần đưa vào thiết kế rev 90.
- **Dừng an toàn:** khi nhận SIGTERM, ngừng đọc, chờ writer xong, ghi checkpoint rồi thoát. `terminationGracePeriodSeconds` trên FKE và `TimeoutStopSec` trên systemd phải lớn hơn thời gian này; giá trị cụ thể đo ở P1.
- **PDB trên FKE:** không dùng `minAvailable: 1` cho một replica duy nhất, vì nó chặn drain node. Restart do drain vốn an toàn nhờ checkpoint.
- **VM cài MySQL từ repo Oracle:** ghim dòng 8.4 hoặc 9.7 trong repo. Lúc 9.7 ra GA, lỗi [MySQL Bug #120315](https://infoq.com/news/2026/05/mysql-97-lts/) đã lặng lẽ chuyển repo từ 8.4 sang 9.7.
- **Secret:** FKE dùng Secret hoặc External Secrets; VM dùng file mode 0600 nạp qua `EnvironmentFile`. Không để mật khẩu trong YAML pipeline.

## 4. Phase 0 — Giảm rủi ro (02/11–20/11/2026)

P0 kéo dài 3 tuần với 2 kỹ sư (E1, E2) và trả lời một câu hỏi: writer theo writeset có apply được ≥ 1,5 × tốc độ sinh binlog ở peak trên tải thật hay không. Review Gate 0 ngày 23/11/2026.

| ID | Tuần | Task | Người | Đầu ra | Xong khi |
| --- | --- | --- | --- | --- | --- |
| P0-01 | 1 | Môi trường compose: MySQL 8.4 và 9.7, PG 18, Toxiproxy, VictoriaMetrics, Grafana | E2 | `compose.yaml`, `make up` | Dựng sạch dưới 5 phút trên máy dev |
| P0-02 | 1 | Lấy binlog production từ archive, ít nhất một ngày có peak | E1 | Bộ binlog kèm số liệu peak (event/s, MB/s) | Có phê duyệt đưa dữ liệu lên staging (mục 8) |
| P0-03 | 1–2 | Audit go-mysql và pglogrepl: tagged GTID 8.4, event 9.7, pgoutput v4 | E2 | Báo cáo audit, danh sách issue | Quyết định fork hay không, có lý do |
| P0-04 | 1 | Chốt định dạng `Txn` và schema `seed.json` | E1 | Ghi vào 02 mục 3 và mục 9 | Cả hai kỹ sư review xong |
| P0-05 | 2 | Prototype writer theo writeset; replay P0-02 lên staging | E1 | Throughput, độ rộng DAG, tỉ lệ hot row | Có số liệu ở 3 mức worker: 4, 8, 16 |
| P0-06 | 2 | Test watermark: GTID trong `gtid_executed` đã visible với read view, gồm cả `Binlog_snapshot_gtid_executed` của Percona | E2 | Test tự động trong CI | 10.000 vòng, 0 vi phạm trên 8.4 và 9.7 |
| P0-07 | 2–3 | Mô hình TLA+ cho 4 kịch bản trong 02 mục 12 | E2 | Spec + cấu hình TLC | TLC không tìm thấy vi phạm ở biên đã ghi |
| P0-08 | 3 | Đo RAM và CPU trên tải replay | E1 | Bảng sizing cho 03 | Có số p50/p99 RSS, CPU ở peak |
| P0-09 | 3 | Thí nghiệm GRACE dạng sidecar trên incremental snapshot của Debezium | E1 | Số liệu sớm cho Gate 2 | Stretch: trượt thì chuyển sang P1, không chặn Gate 0 |
| P0-10 | 3 | Báo cáo Gate 0 | E1, E2 | Quyết định đi tiếp hay dừng | Review ngày 23/11/2026 |

**Điều kiện qua Gate 0:**

- Throughput apply ≥ 1,5 × peak đo ở P0-02 trên workload có hot row. Đây là điều kiện cứng.
- Test watermark P0-06 đạt. Nếu trượt, chế độ A trên MySQL phải thiết kế lại trước khi vào P1.
- Có quyết định audit P0-03. Fork thì cộng 1–2 tuần vào P1.
- TLC không phát hiện vi phạm. Nếu có, sửa thiết kế trước khi viết code cho phần đó.

**Nếu throughput không đạt:** dừng và xem xét Debezium Server kết hợp sidecar GRACE, hoặc BladePipe, theo thiết kế mục 10.

## 5. Phase 1–4: epic và phụ thuộc

Tổng thời gian 39 tuần, từ 02/11/2026 đến 30/07/2027. Con số này đã gồm 2 tuần buffer Tết ở cuối P1. Breakdown đến task cho P1 chỉ viết sau báo cáo Gate 0.

&#91;embedded content: lộ trình 5 phase · 5 gate\]

Gate 0 là điểm quyết định duy nhất có thể dừng cả dự án. Các gate sau chỉ làm trượt phase kế tiếp, hoặc đổi một mặc định.

| Phase | Thời gian | Epic | Phụ thuộc |
| --- | --- | --- | --- |
| P1 — MVP MySQL→MySQL | 23/11/2026 – 26/02/2027 | CLI `validate`, `plan`, `sync`, `checkpoint`, `pause`/`resume`, `top`; writer giữ nguyên transaction, checkpoint hai tầng, writeset phân cấp; chế độ A (snapshot logic, throttle tĩnh, khung giờ); chế độ B (seed XtraBackup/mydumper/CLONE, Archive Catch-up Reader); chế độ C (`--start-at` kèm verify bắt buộc); `verify sample\|delta`; `promote`; breaker p99; khóa chống chạy trùng; manifest FKE và unit systemd | Qua Gate 0; quyết định audit P0-03 |
| P2 — PostgreSQL, MySQL→PG | 01/03 – 07/05/2027 | PG→PG (decode trên standby, partial TOAST); MySQL→PG (mapping kiểu, collation, múi giờ); seed pgBackRest/WAL-G, `strand export`; GRACE v2; `commit_order strict\|relaxed`; `verify full` bằng cây Merkle | Qua Gate 1; có staging PG 16–18 |
| P3 — Thích ứng | 10/05 – 18/06/2027 | Schema evolution với `approve`; nhận diện online DDL; `strand archive binlog`; sink Kafka có transaction; XA và 2PC; pgoutput `streaming = parallel`; gói `.deb`/`.rpm` | Qua Gate 2 |
| P4 — Vận hành tập trung | 21/06 – 30/07/2027 | HA bằng lease etcd và fencing token; failover slot PG 17+; control plane gRPC/REST; Helm chart cho FKE; governor dùng chung theo source identity; Web UI | Qua Gate 3 |

## 6. Gates và tiêu chí đo

Có năm gate. Gate 0, 1 và 2 lấy nguyên từ thiết kế, chỉ sửa cách diễn đạt. Gate 3 và 4 là đề xuất mới, cần chốt. Phase sau chỉ bắt đầu khi phase trước qua gate; kịch bản test chi tiết nằm trong tài liệu 04.

| Gate | Review | Điều kiện cứng | Không đạt thì |
| --- | --- | --- | --- |
| 0 | 23/11/2026 | Apply ≥ 1,5 × peak binlog trên tải thật | Xem xét Debezium Server + sidecar GRACE, hoặc BladePipe |
| 1 | 01/03/2027 | 1.000 lần kill -9, 0 lệch; seed hỏng bị từ chối 100% | Không vào P2; sửa và chạy lại |
| 2 | 10/05/2027 | Không lần nào vượt hard limit; dao động log R ≤ 0,3 | Gate không trượt; chỉ đổi governor mặc định (bên dưới) |
| 3 (mới) | 21/06/2027 | gh-ost/pt-osc trên bảng đang ghi: đích nhận đúng 1 `ALTER`, 0 lệch | Tắt nhận diện online DDL, giữ chế độ dừng và `approve` |
| 4 (mới) | 02/08/2027 | Kill instance active: instance khác nhận lease trong TTL, writer cũ bị fencing token chặn, 0 lệch | Phát hành không có HA, chỉ chạy một instance |

**Gate 1 — test bắt buộc (MySQL 8.4 và 9.7; 8.0 chỉ smoke test với vai trò source):**

- Seed bảng 1 TB trên staging: **0 IOPS đọc dữ liệu bảng trên source**. Binlog vẫn được đọc và được đo riêng.
- Đổi bộ lọc và đổi replica giữa các lần restart; `snapshot add` khi stream đang chạy; transaction 50 triệu row; UPDATE đổi PK qua hai range khác trạng thái. Tất cả đều 0 lệch.
- Archive bị xóa trong lúc `seed apply`: prefetch vẫn cứu được. Tắt prefetch thì phải báo `log_gap`. Restore mất 2 × ETA thì spool tràn sang bucket; đầy mọi tầng thì báo `prefetch_exhausted`.
- Retention sát ngưỡng với `lag_slo` nhỏ: không tự khóa chết, có `bootstrap_retention_risk`. Row lớn hơn `max_chunk_bytes` không gây vòng lặp chia chunk.
- `validate` từ chối partial JSON. Seed từ backup của replica không tự replicate. `promote` dưới tải: ứng dụng không ghi được trước khi setval xong.
- **Mới:** hai instance cùng `pipeline_id` thì instance thứ hai phải từ chối khởi động (khóa trên target, mục 3).

**Gate 2 — test bắt buộc (PG 16–18 và MySQL→PG):** TOAST > 2 KB không bị NULL ghi đè; commit đúng tại `consistent_point`; failover rồi insert ngay không trùng key; collation `_ci` sang `C`; transaction 30 phút ở chế độ `relaxed`; đổi PK với partial TOAST, kể cả khi PK mới nằm trong range "đang đọc"; 100 transaction × 49.999 khóa không OOM; breaker khi RLS chưa đủ dữ liệu bật `strand_sensitivity_unknown`; `promote` khi source không truy cập được với `CACHE 1000` thì dừng và hỏi; hot row 1.000 update/giây được báo `unverifiable_hot`, nhưng lệch thật cài vào vẫn bị phát hiện.

**Nhánh GRACE → bật/tắt:** Gate 2 so ba governor: throttle tĩnh, bật/tắt kiểu gh-ost và GRACE v2. GRACE chỉ làm mặc định khi rút ngắn snapshot ≥ 30% mà p99 của ứng dụng trên source không xấu hơn bật/tắt. Không đạt thì:

- Bật/tắt thành mặc định (`governor.mode: onoff`), GRACE thành tùy chọn, trạng thái ghi là experimental.
- Không tốn thêm thời gian: P3 và P4 không phụ thuộc GRACE. UI ở P4 chỉ hiện zone khi GRACE đang bật.
- Circuit breaker theo p99 vẫn giữ ở mọi governor, vì nó có từ P1.

## 7. Known issues, rủi ro và điểm lệch so với thiết kế

Bảng dưới đã cập nhật đến hết vòng phản biện thứ 6. Mỗi vấn đề đều đã có cách xử lý trong thiết kế và có test ở một gate; đặc tả nằm trong tài liệu 02.

| # | Vấn đề | Cách xử lý | Kiểm ở |
| --- | --- | --- | --- |
| 1 | Watermark MySQL: GTID trong `gtid_executed` phải visible với read view | Test riêng; Percona dùng `Binlog_snapshot_gtid_executed` | Gate 0 |
| 2 | Sequence sau `promote` | Standby luôn bằng hoặc đi trước, nên rủi ro thật là độ trễ đồng bộ Δt\_sync; margin theo công thức ở 02 mục 8; source không truy cập được thì dừng và hỏi | Gate 1, 2 |
| 3 | Spool tràn khi restore lâu | Spool phân tầng (local rồi bucket staging); bỏ prefetch khi đã có archive bền; kiểm ETA × 2; báo `prefetch_exhausted` | Gate 1 |
| 4 | Ghi đè NULL do TOAST | Partial UPDATE mặc định; `REPLICA IDENTITY FULL` là opt-in | Gate 2 |
| 5 | Đổi PK khi cột TOAST không đổi | SQL điều kiện `UPDATE … WHERE NOT EXISTS` rồi `DELETE`; `max_pending_cross_range` = 1024, nâng ưu tiên sau 30 s | Gate 1, 2 |
| 6 | RAM của writeset khi có transaction lớn | Writeset phân cấp: ngưỡng 50.000 khóa thì nâng lên mức bảng; `writeset_budget` 2 triệu entry | Gate 2 |
| 7 | Row checkpoint phình khi có transaction dài | Checkpoint hai tầng; delta ≤ 64 KB; holes ≤ 4096; `fillfactor=50` | Gate 2 |
| 8 | Livelock lúc bootstrap vì P\_lag | P\_lag theo phase: bootstrap dùng vị trí reader/spool, sau `synced` dùng lag của target | Gate 1, 2 |
| 9 | Breaker nhả phanh khi RLS chưa đủ dữ liệu | Giữ polite floor; bật `strand_sensitivity_unknown` | Gate 2 |
| 10 | Collation đích lỏng hơn source | Quy tắc: target không được lỏng hơn source; không dùng `citext` | Gate 2 |
| 11 | Báo lệch giả trên hot row khi verify | Kiểm tra theo image-membership; báo riêng `unverifiable_hot` | Gate 2 |
| 12 | Online DDL của gh-ost / pt-osc | Nhận diện `_gho`/`_ghc`, `_new` kèm trigger; sinh `ALTER` từ schema diff | Gate 3 |
| 13 | Ứng dụng ghi trong lúc promote | Fence, read-only, setval, rồi mới mở ghi | Gate 1 |
| 14 (mới) | Hai instance cùng pipeline trước khi có HA | Khóa theo `pipeline_id` trên target | Gate 1 |
| 15 (mới) | Backup tạo bằng XtraBackup 9.7 bản RC | `seed plan` từ chối, trừ khi có `--allow-prerelease-tool` | Gate 1 |

**Rủi ro của kế hoạch:**

| Rủi ro | Ảnh hưởng | Giảm thiểu |
| --- | --- | --- |
| Tết Đinh Mùi (mùng 1 ngày 06/02/2027) rơi vào cuối P1 | Mất khoảng 2 tuần | Thêm 2 tuần buffer; Gate 1 dời từ 15/02 sang 01/03/2027 |
| Phải fork go-mysql hoặc pglogrepl | +1–2 tuần ở P1 | Quyết định ngay trong P0 (P0-03) |
| Không được dùng binlog production cho P0 | Kết luận Gate 0 kém tin cậy | Dùng workload tổng hợp có hot row; ghi rõ hạn chế trong báo cáo Gate 0 |
| Nhóm chỉ có 2 kỹ sư | Một người nghỉ thì phase trượt gần gấp đôi | Review chéo mọi PR; mỗi module có người chính và người phụ |
| So sánh collation giữa Go và MySQL không khớp | Lệch thứ tự chunk | Đẩy việc so sánh xuống DB khi không chắc |
| Online DDL trên bảng TB | Barrier chờ lâu | Gate 3; fallback dừng và `approve` |

**Điểm lệch so với thiết kế rev 89, cần đưa vào rev 90:**

- Tổng thời gian 39 tuần thay vì 37, do buffer Tết.
- Ma trận phiên bản ở mục 2: MySQL 8.0 chỉ còn là source best-effort; thêm 9.7 LTS, PG 18, PG 19 thử nghiệm; bỏ PG 14 và 15.
- Khóa chống chạy trùng kèm fencing bằng cột epoch (issue 14), cờ `--allow-prerelease-tool` (issue 15), Gate 3 và Gate 4.
- Cách diễn đạt Gate 1: "0 IOPS đọc dữ liệu bảng", không phải "0 IOPS".

## 8. Nhân lực, giả định và việc còn mở

Kế hoạch giả định 2 kỹ sư Go làm toàn thời gian. E1 phụ trách đường dữ liệu: reader, writer, scheduler, seed. E2 phụ trách tính đúng và hạ tầng: TLA+, test, compose/FKE, observability.

**Hạ tầng cần có từ P0:**

- Staging MySQL 8.4 và 9.7, có một bảng ≥ 1 TB kèm backup XtraBackup. Bảng này dùng cho Gate 1.
- Staging PG 16, 17, 18 có standby. Phải sẵn sàng trước P2.
- Bucket S3-compatible cho tầng spool thứ hai và archive binlog.
- VictoriaMetrics/Prometheus và Grafana hiện có, dùng để scrape metric của Strand. Breaker và governor không phụ thuộc chúng (rev 91).

**Việc còn mở:**

- [ ] Phê duyệt đưa binlog production lên staging cho P0-02. Chưa có thì dùng workload tổng hợp.
- [ ] Yêu cầu compliance: mã hóa spool và bucket khi lưu, audit log của `promote` và `approve`.
- [ ] License mã nguồn mở (Apache-2.0 hay MPL-2.0), cần chốt trước commit công khai đầu tiên.
- [ ] Bảng giá FPT Cloud, làm input cho ước tính chi phí trong tài liệu 03.
- [ ] Có cần tài liệu cho người dùng cuối ngoài tài liệu 05 hay không.
- [ ] Ai là người review độc lập ở mỗi gate.
