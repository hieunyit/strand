# Strand CDC — 04 Testing Strategy

Oct 31, 2026 · @hieuny

## 1. Mục tiêu và ánh xạ bất biến → test

Test của Strand có một đích: chứng minh mười lăm bất biến ở tài liệu 02, mục 2 vẫn đúng khi có lỗi. Coverage theo dòng code không phải mục tiêu. Tiêu chí đầu ra là mỗi bất biến có ít nhất một test tự động có thể làm nó gãy, và checker (mục 3) phát hiện được khi nó gãy.

| Bất biến | Property / unit | E2E + fault | TLA+ | Gate |
| --- | --- | --- | --- | --- |
| CP-1 không mất | Gộp AppliedSet, compaction | kill -9 ở mọi bước compaction | `Checkpoint` | 1 |
| CP-2 không apply hai lần | Scheduler bỏ Txn đã apply | Restart lặp lại, đổi bộ lọc, đổi replica | `Checkpoint` | 1 |
| CP-3 không ghi `seq` | Lint + test serialize | Đổi bộ lọc giữa hai lần chạy | — | 1 |
| CP-4 holes, delta có trần | Ngưỡng 4.096 và 64 KB | Txn chạy 30 phút, coordinator bị treo | `Checkpoint` | 2 |
| CP-5 prefix ở `strict` | Commit Orderer | Snapshot target liên tục, kiểm prefix | — | 2 |
| RS-1 không lùi trạng thái | Bảng chuyển trạng thái range | `snapshot add` dưới tải cập nhật dày, đổi PK qua hai range | `RangeState` | 1, 2 |
| RS-2 watermark visible | — | 10.000 vòng trên MySQL 8.4 và 9.7 | — | 0 |
| CO-1 thứ tự theo writeset | Admit với writeset sinh ngẫu nhiên | Txn 50 triệu row trộn Txn nhỏ | `Scheduler` | 1 |
| CO-2 RAM writeset | `writeset_budget` | 100 Txn × 49.999 khóa | — | 2 |
| SD-1..SD-4 seed | Parser metadata từng công cụ | Backup hỏng có chủ đích bị từ chối 100% | `Handover` | 1 |
| PR-1 promote | Thứ tự 6 bước | Promote dưới tải; source mất kết nối với `CACHE 1000` | — | 1, 2 |
| SG-1 một writer | Điều kiện `epoch` | Hai instance cùng `pipeline_id`; pod bị force-delete | — | 1, 4 |

Số liệu hiệu năng (Gate 0, Gate 2) nằm ở mục 5 và tách khỏi test tính đúng. Một lần benchmark không được phép thay cho test tính đúng, và ngược lại.

## 2. Các tầng test

Có tám tầng test. Ba tầng đầu chạy mỗi PR và phải xong dưới 10 phút. Các tầng nặng hơn chạy nightly hoặc trước gate.

| Tầng | Công cụ | Chạy khi | Kiểm gì |
| --- | --- | --- | --- |
| Unit | `go test -race` | Mỗi PR | Logic từng module, data race |
| Property-based | `rapid` (pgregory.net/rapid) | Mỗi PR | AppliedSet merge, `Admit` với writeset ngẫu nhiên, chuyển trạng thái range |
| Fuzz | Go native fuzzing | Mỗi PR (30 s), nightly (1 h) | Parser binlog, pgoutput, metadata của XtraBackup/mydumper/pgBackRest |
| Integration | testcontainers-go với MySQL/PG thật | Mỗi PR, một phiên bản; nightly, đủ ma trận | Reader, writer, checkpoint, quyền DB tối thiểu |
| E2E | Compose + bộ sinh tải + checker (mục 3) | Nightly | Pipeline đầy đủ, cả 3 chế độ khởi tạo |
| Fault injection | Toxiproxy, script kill/pause, lỗi đĩa (mục 4) | Nightly, và bản đầy đủ trước gate | Bất biến dưới lỗi |
| Soak | Staging, tải đều + lỗi định kỳ | 72 giờ trước mỗi gate từ Gate 1 | Rò rỉ bộ nhớ, phình checkpoint, lệch tích lũy |
| Mô hình | TLA+ / TLC (`spec/tla/`) | Khi spec đổi | Bốn module ở tài liệu 02, mục 12 |

**Quy tắc:**

- Mỗi bug liên quan đến tính đúng phải có một test tái hiện, test này vào bộ nightly vĩnh viễn.
- Test e2e và fault dùng seed ngẫu nhiên cố định và in seed ra log, để chạy lại đúng kịch bản.
- Test flaky bị cách ly trong 48 giờ. Không sửa được thì phải ghi là bug, không được xóa test.

## 3. Checker tính đúng

Checker là một chương trình độc lập với Strand, theo tinh thần Jepsen: ghi lại lịch sử thao tác ở phía client, rồi kiểm target bằng lịch sử đó. Checker không được dùng code decode của Strand, vì một bug ở đó sẽ làm cả hai cùng sai.

**Bộ sinh tải** ghi vào source và log mọi transaction đã commit kèm vị trí của nó:

- MySQL: bật `session_track_gtids=OWN_GTID` để lấy GTID của chính transaction.
- PG: một slot riêng dùng plugin `test_decoding` ghi xid → commit LSN. Nguồn này độc lập với pgoutput.
- Mỗi row có cột `ver` tăng mỗi lần ghi, để so thứ tự mà không cần so toàn bộ row.
- Các núm workload: khóa nóng (phân bố Zipf), UPDATE đổi PK, Txn rất lớn, cột TOAST > 2 KB giữ nguyên, unique key phụ, FK cha–con, `nextval` trên sequence độc lập.

**Năm phép kiểm:**

| Kiểm | Cách làm | Phát hiện vi phạm |
| --- | --- | --- |
| Trạng thái cuối bằng nhau | Dừng tải, chờ lag về 0, so hash từng row theo thứ tự PK giữa source và target | CP-1, CP-2, dữ liệu hỏng |
| Prefix tại checkpoint (`strict`) | Trong một transaction, đọc target cùng vị trí X trong `_strand_checkpoint`; dựng lại trạng thái kỳ vọng từ lịch sử đến X; so sánh | CP-5 |
| `ver` không giảm | Lấy mẫu target liên tục; `ver` của mỗi khóa chỉ được tăng | RS-1 |
| Không hồi sinh | Khóa đã DELETE ở vị trí ≤ checkpoint thì không được xuất hiện lại ở target | RS-1, đổi PK |
| Ràng buộc | Target có đủ unique và FK như source; mọi lỗi ràng buộc khi apply đều là vi phạm | CO-1, collation |

Với `relaxed`, phép kiểm prefix thay bằng kiểm "tập đã apply": trạng thái target phải bằng kết quả apply đúng các Txn trong tập `lwd`/`high`/`holes`, theo thứ tự source.

**Tự kiểm checker.** Mỗi bản checker phải bắt được lỗi cài sẵn: một bản Strand cố ý bỏ qua một Txn, apply trùng một Txn, hoặc đảo hai Txn xung đột. Checker không báo lỗi với các bản này thì kết quả "0 lệch" của nó không có giá trị.

## 4. Fault injection

Không dùng Chaos Monkey: công cụ đó kill service ngẫu nhiên và chỉ chạy trên K8s, nên không nhắm được đúng chỗ nguy hiểm của CDC. Strand dùng hai cách bổ trợ nhau:

- **Failpoint trong code**, bật bằng build tag (ví dụ thư viện `pingcap/failpoint`), để crash đúng giữa hai bước. Ví dụ: giữa bước 2 và 3 của compaction, hay sau khi commit dữ liệu nhưng trước khi báo `Done`.
- **Lỗi từ bên ngoài** bằng Toxiproxy, script điều khiển process, và lỗi đĩa.

| Lỗi | Cách gây | Kỳ vọng | Gate |
| --- | --- | --- | --- |
| Crash ngẫu nhiên | `kill -9` theo thời điểm ngẫu nhiên, 1.000 lần | 0 lệch | 1 |
| Crash tại điểm nguy hiểm | Failpoint tại mọi bước của compaction, flush range, promote | 0 lệch; promote chạy tiếp đúng bước | 1 |
| Coordinator treo | Failpoint chặn compaction | Delta chạm 64 KB, worker ngừng, `StrandCompactionStuck` báo | 2 |
| Process bị đóng băng | `SIGSTOP` 30–120 s rồi `SIGCONT` | 0 lệch; từ P4, lease hết hạn thì process bị fence | 1, 4 |
| Mạng tới source | Toxiproxy: latency 200 ms, giới hạn băng thông, `reset_peer`, `timeout` | Reconnect, resume đúng vị trí | 1 |
| Mạng tới target | Toxiproxy cắt giữa lúc commit batch | Không apply trùng, 0 lệch | 1 |
| Mạng tới S3 | Toxiproxy trên endpoint S3 | Retry; hết giới hạn thì lỗi rõ ràng, không hổng log | 1 |
| Đổi nguồn đọc | Đổi replica giữa hai lần chạy; promote standby PG | Resume đúng theo GTID hoặc slot | 1, 2 |
| Spool đầy | Volume nhỏ hoặc quota; restore chậm 2 × ETA | Tràn sang bucket; đầy mọi tầng thì `prefetch_exhausted` | 1 |
| Đĩa chậm, fsync lỗi | `dm-delay` / `dm-flakey` trên VM test | Governor giảm R; lỗi fsync thì dừng an toàn | 2 |
| Archive bị xóa | Xóa file trong lúc `seed apply` | Prefetch cứu được; tắt prefetch thì báo `log_gap` | 1 |
| Backup hỏng | Thiếu file, sai GTID, log có khoảng trống, mydumper `NO_LOCK`, XtraBackup bản RC | `seed plan` từ chối 100% với mã đúng | 1 |
| Hai writer | Chạy instance thứ hai; force-delete pod khi pod cũ còn sống | `pipeline_locked` hoặc `fenced`, 0 lệch | 1 |
| Lệch đồng hồ | Dịch giờ máy Strand ± 5 phút | Chỉ metric lag sai; tính đúng không đổi | 1 |
| Khóa phía target | Client test giữ lock trên row đang được apply | Retry, governor giảm L, 0 lệch | 2 |

Mỗi lần chạy fault đều kết thúc bằng checker ở mục 3. Một fault chỉ được ghi "đạt" khi cả hai điều kiện đúng: checker báo 0 lệch, và mã lỗi hoặc alert đúng như kỳ vọng.

## 5. Benchmark cho Gate 0 và Gate 2

Hai benchmark này quyết định hướng dự án, nên giao thức chạy được cố định trước khi có kết quả. Đổi giao thức sau khi đã thấy số thì phải ghi lại lý do trong báo cáo gate.

**Gate 0 — throughput apply (P0-05).**

1. Đo tốc độ sinh binlog ở peak trên bộ binlog production (P0-02), tính theo cửa sổ 1 phút, ra event/s và MB/s.
2. Replay bộ binlog đó từ file archive lên target staging, cấu hình giống target DR thật. Prototype writer theo writeset chạy không bật governor, ở 4, 8 và 16 worker.
3. Ghi lại throughput duy trì được, phân bố độ rộng DAG, tỉ lệ Txn chạm hot row, RSS và CPU.
4. **Đạt** khi throughput duy trì ≥ 1,5 × peak ở ít nhất một mức worker, sau đó checker xác nhận 0 lệch.

**Gate 2 — so sánh ba governor.**

| Thành phần | Thiết lập |
| --- | --- |
| Governor | `static`, `onoff`, `grace-v2` |
| Tải ứng dụng | `sysbench oltp_read_write` (MySQL) hoặc `pgbench` (PG) chạy trên source, p99 đo ở phía client |
| Tải nền | Sóng vuông giữa hai mức tải ứng dụng, chu kỳ 10 phút; spike ngắn định kỳ. Biên độ chốt ở P2 theo năng lực staging |
| Việc của Strand | Snapshot logic một bảng có kích thước cố định, cộng stream liên tục |
| Lặp | 3 lần cho mỗi governor, thứ tự ngẫu nhiên, khôi phục source về cùng snapshot trước mỗi lần |
| Đo | Thời gian snapshot, p99 ứng dụng, số lần vượt hard limit, peak-to-peak của log R ở trạng thái xác lập, lag |

**Quy tắc quyết định** (theo thiết kế):

- Điều kiện cứng cho mọi governor: 0 lần vượt hard limit. Riêng `grace-v2` cần thêm peak-to-peak của log R ≤ 0,3.
- `grace-v2` làm mặc định khi median thời gian snapshot ngắn hơn `onoff` ít nhất 30%, và median p99 ứng dụng không xấu hơn `onoff`.
- Không đạt thì `onoff` là mặc định, GRACE thành tùy chọn experimental.

Báo cáo gate có biểu đồ R, P và p99 theo thời gian cho từng lần chạy. Biểu đồ làm từ dữ liệu thật sau khi chạy, không vẽ trước.

## 6. CI matrix

Ma trận bám theo tài liệu 01, mục 2. Một ô chỉ được ghi "Chính" khi nó xanh ở nightly và đã qua test của gate tương ứng. Phase nào chưa tới thì job của phase đó chưa bật.

| Lịch | Source → target | Kiến trúc | Tầng test | Từ phase |
| --- | --- | --- | --- | --- |
| Mỗi PR | MySQL 8.4 → 8.4 | amd64 | Unit, property, fuzz 30 s, integration | P1 |
| Mỗi PR | PG 18 → 18 | amd64 | Integration | P2 |
| Mỗi PR | — | amd64, arm64 | Build, binary < 50 MB, lint (cấm ghi `seq`) | P0 |
| Nightly | MySQL 8.4 → 8.4, 9.7 → 9.7 | amd64, arm64 | E2E, fault, fuzz 1 h | P1 |
| Nightly | MySQL 8.0 → 8.4 | amd64 | Smoke test, vai trò source | P1 |
| Nightly | PG 16 → 16, 17 → 17, 18 → 18 | amd64, arm64 | E2E, fault | P2 |
| Nightly | MySQL 8.4 / 9.7 → PG 17 / 18 | amd64 | E2E, collation, kiểu dữ liệu | P2 |
| Nightly, cho phép fail | PG 19 beta → 19 | amd64 | E2E | P2 |
| Hàng tuần | Các ghép cặp MySQL→PG còn lại | amd64 | E2E | P2 |
| Trước mỗi gate | Theo danh sách test của gate (tài liệu 01, mục 6) | amd64 | Fault đầy đủ, soak 72 giờ | P1 |
| Khi `spec/tla/` đổi | — | — | TLC với biên ở tài liệu 02, mục 12 | P0 |

**Quy định merge:** PR chỉ được merge khi mọi job PR xanh. Nightly đỏ trên một ô "Chính" thì mở bug ưu tiên cao, và chặn release cho đến khi xanh lại.

**Runner:** job arm64 chạy trên runner arm64 thật, không giả lập bằng QEMU, vì QEMU quá chậm cho e2e và làm sai số đo thời gian.

## 7. Môi trường và dữ liệu test

Có ba môi trường. Chỉ staging mới đủ lớn để đo gate hiệu năng; compose và CI chỉ dùng để kiểm tính đúng.

| Môi trường | Dùng cho | Dữ liệu |
| --- | --- | --- |
| Compose trên máy dev | Unit, integration, e2e nhỏ | Bộ sinh tải tổng hợp, vài GB |
| CI runner | PR và nightly | Như compose; seed ngẫu nhiên cố định |
| Staging | Gate 0, Gate 1 (seed 1 TB), Gate 2, soak 72 giờ | Bảng ≥ 1 TB kèm backup; binlog production nếu được duyệt |

**Dữ liệu production trên staging.** Việc dùng binlog production cho P0-02 vẫn chờ phê duyệt (tài liệu 01, mục 8). Nếu được dùng thì:

- Binlog nằm trong bucket riêng, mã hóa, chỉ staging đọc được, và xóa sau Gate 0.
- Không copy dữ liệu sang compose hay CI.
- Báo cáo gate chỉ chứa số liệu tổng hợp, không chứa giá trị row.

Nếu không được duyệt, Gate 0 chạy trên workload tổng hợp mô phỏng production: cùng số bảng, kích thước row, tỉ lệ hot row và cỡ transaction. Các thông số này lấy từ `performance_schema` của production. Báo cáo Gate 0 ghi rõ hạn chế này.

**Bảng 1 TB cho Gate 1:** sinh bằng bộ sinh tải với PK số và PK chuỗi, có cột TOAST/JSON. Sau đó backup bằng XtraBackup và mydumper để test cả hai đường seed. Một bản backup dùng chung cho nhiều lần chạy, để các lần đo so sánh được.
