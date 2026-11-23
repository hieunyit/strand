# Pilot CDC keyless và phục hồi backup

## Đối soát và chuyển ghi

- Xác định cặp engine, phiên bản, bảng, ánh xạ, checkpoint seed và người chịu trách nhiệm duyệt chuyển ghi. Chạy `strand validate -c pipeline.yaml` trước khi bắt đầu. Với nguồn PostgreSQL, kiểm tra `REPLICA IDENTITY FULL` cho các bảng keyless.
- Theo dõi `strand top -c pipeline.yaml` hoặc trang pipeline. `caught_up` cho biết đầu nguồn và hàng đợi apply đã bắt kịp tại lần lấy mẫu. Phần đối soát cho biết phương pháp, bảng, mốc và thời điểm đã kiểm. `incomplete` cần kiểm lại ở cửa sổ yên. `mismatch` cần xác định phạm vi lệch và sửa hoặc seed lại trước khi chuyển ghi.
- Trong cửa sổ chuyển ghi, khóa ghi của ứng dụng tại nguồn, giữ CDC chạy đến khi bắt kịp, rồi chạy `strand verify -c pipeline.yaml --mode full --require-complete`. Chỉ duyệt chuyển ghi khi lệnh thành công và quyền ghi ở nguồn vẫn bị khóa. Ghi lại checkpoint, phạm vi, thời điểm và kết quả. Với bảng keyless trong pilot, đối chiếu thêm số lần xuất hiện của từng hàng canonical tại cùng mốc. Delta chỉ kiểm thay đổi có khóa.
- Ghi thời điểm đích nhận ghi đầu tiên. Sau mốc đó, chuyển lại nguồn cần kế hoạch đồng bộ các ghi mới tại đích.

CLI `promote` và wizard chuyển ghi kiểm tra điều kiện fence và bắt kịp theo luồng hiện có. Với chuyển ghi có kế hoạch, thực hiện bước đối soát tường minh ở trên trước khi chạy `promote`. Quy trình failover khi nguồn mất kết nối cần quyết định vận hành riêng vì không thể lấy bằng chứng đối soát mới từ nguồn.

## Xử lý sự cố

| Triệu chứng | Thao tác |
| --- | --- |
| Lag tăng hoặc target mất kết nối | Ghi checkpoint và đầu WAL/binlog, kiểm lỗi kết nối, writer, backlog và dung lượng lưu log tại nguồn. Sau khi sửa, theo dõi bắt kịp rồi đối soát lại tại mốc mới. |
| Spool gần đầy | Kiểm dung lượng thực và tốc độ tăng, giảm tải ghi nếu có thể, khôi phục target hoặc tăng dung lượng theo cấu hình. Giữ spool và checkpoint để dùng khi khởi động lại. Nếu log cần thiết đã bị xóa, lập mốc seed mới. |
| Thiếu khóa backup | Giữ lựa chọn mã hóa của job. Tìm đúng khóa theo key ID trên Catalog, phục hồi keyring và thử giải mã một backup mẫu trước khi chạy lại. Khi xoay khóa, giữ khóa cũ cho đến khi hết nhu cầu đọc payload tương ứng. |
| Mất Web metadata | Tạm dừng thao tác ghi quản trị, khôi phục PostgreSQL metadata và khóa bí mật Web từ bản đã kiểm, xác nhận lease scheduler và đối chiếu Catalog với storage trước khi chạy lại job. Kiểm pipeline đang chạy qua CLI và admin socket. |
| Restore lỗi hoặc dở dang | Giữ log, backup ID, phiên bản object và đích thử. Kiểm công cụ native, khóa, checksum và chuỗi log. Lập lại preflight và chạy vào đích mới. Chỉ bàn giao đích sau khi kiểm dữ liệu và quyền truy cập. |

Bài diễn tập trên cấu hình pilot cần có restore backup mã hóa và backup không mã hóa từ repository thực, kể cả backup dùng khóa cũ. Nếu dùng PITR, thử khoảng thiếu log, backlog và timeline mới theo engine. Ghi thời gian phát hiện, thời gian phục hồi, giao dịch cuối cùng có thể phục hồi và kết quả đối soát. Ghi RPO/RTO đo được và người phê duyệt cho từng kịch bản trước khi mở rộng rollout.

## Phép đo truy vấn PostgreSQL

Bộ đo [keyless-pg.sql](../tools/bench/keyless-pg.sql) dùng cùng ảnh trước cho truy vấn hiện tại và ứng viên. Dữ liệu chứa hàng trùng, NULL và bytea. `EXCEPT ALL` theo cả hai chiều kiểm đúng số lần xuất hiện của mỗi hàng. Bảng tạm và `work_mem` 4 MB cho phép phép đối chiếu tràn sang đĩa khi cần.

```sh
psql -X -v rows=100000 -f tools/bench/keyless-pg.sql
psql -X -v rows=1000000 -f tools/bench/keyless-pg.sql
```

Phép đo phòng lab dùng PostgreSQL 18.7, image ID `sha256:22fac36945da9a4145a11bc9a6198ebc00975b8323c8d454740e477c4e84593a`, Debian 13.7 amd64, cgroup 4 CPU và 32 GiB RAM. Bảng có thêm một bản sao của hàng được chọn. Mỗi trường hợp có một lượt làm nóng và ba lượt ghi số đo. Thời gian dưới đây là `Execution Time` của `EXPLAIN ANALYZE`, tính bằng ms.

| Số hàng sinh | Hiện tại, không index | Hiện tại, có index | Ứng viên, có index |
| ---: | ---: | ---: | ---: |
| 100.000 | 4,093 (3,953–4,131) | 5,120 (4,963–6,711) | 0,018 (0,016–0,020) |
| 1.000.000 | 51,863 (51,822–53,035) | 56,887 (55,616–57,984) | 0,017 (0,016–0,020) |

Truy vấn hiện tại dùng sequential scan và loại 999.999 hàng ở tập lớn dù có index. Ứng viên dùng điều kiện bằng trên cột số đã biết có giá trị và dùng index. Các cột còn lại vẫn so sánh NULL-safe, truy vấn vẫn giới hạn một cặp `(tableoid, ctid)` và cả hai kích thước qua đối chiếu đa tập.

Trước khi đưa ứng viên vào writer, cần so với baseline CDC đầu cuối trên cặp engine pilot và dữ liệu thật. Chạy lặp tải cố định và tải bão hòa với đối chứng có khóa. Đo lag p50/p95/p99, rows/s, CPU/RSS của Strand và DB, I/O, spool, chi phí duy trì index và thời gian phục hồi. Đo riêng bảng có giá trị nóng, transaction chạm nhiều bảng và giai đoạn target gián đoạn. Chốt tiêu chí nhận bản tối ưu cùng đội vận hành sau khi có SLO và độ nhiễu số đo.

## Baseline CDC PostgreSQL đầu cuối

```sh
STRAND_KEYLESS_BENCH=1 go test ./tests/e2e -run '^TestPGKeylessPerformanceBaseline$' -v -count=1 -timeout 10m
```

Bộ đo CDC dùng PostgreSQL 18.7 ở nguồn và đích, bật `fsync` và `synchronous_commit`, tắt synchronized sequential scan ở đích để kiểm soát điểm bắt đầu scan. Bảng keyless và đối chứng có khóa có cùng kiểu cột. Các UPDATE/DELETE chọn ID phân bố xác định bằng bước 104729. Mỗi transaction thay đổi một hàng tải và ghi một marker trong bảng có khóa. Điểm lấy mẫu lag là khi marker xuất hiện ở đích, với chu kỳ thăm dò khoảng 5 ms. Báo cáo từng lượt có TPS đã apply, lag p50/p95/p99, RSS đỉnh của Strand và số byte trong thư mục spool. Mỗi trường hợp làm nóng 10 transaction và chạy ba lượt 60 transaction. Chế độ phát theo nhịp có khoảng cách danh nghĩa 10 ms, chế độ phát liên tục là burst hữu hạn. Cả hai bản đều qua đối chiếu đa tập chính xác ở cuối mỗi nhóm kích thước và kiểu bảng.

Ứng viên dùng so sánh bằng cho tham số ảnh trước có kiểu integer và giữ điều kiện NULL-safe ở các cột còn lại. Lượt ứng viên tạo index không unique trên cột ID của bảng keyless ở đích. Các số dưới đây là median và khoảng min–max.

| Hàng | Tải | Chỉ số | Hiện tại | Ứng viên |
| ---: | --- | --- | ---: | ---: |
| 100.000 | Hỗn hợp | p99 có nhịp, ms | 82,9 (79,4–84,4) | 68,7 (64,4–78,6) |
| 100.000 | UPDATE/DELETE-heavy | p99 có nhịp, ms | 103,7 (97,0–121,5) | 64,8 (63,5–65,3) |
| 1.000.000 | Hỗn hợp | p99 có nhịp, ms | 633,4 (414,8–978,7) | 68,9 (63,8–77,7) |
| 1.000.000 | UPDATE/DELETE-heavy | p99 có nhịp, ms | 1.201,0 (1.056,9–1.201,7) | 63,1 (61,9–113,5) |
| 1.000.000 | UPDATE/DELETE-heavy | TPS burst đã apply | 43,6 (34,6–46,4) | 401,9 (388,6–411,5) |

Đối chứng có khóa ở một triệu hàng dao động đến 24% giữa hai lượt đo dù dùng cùng đường writer. Do đó phép thử chưa xác lập được ngưỡng hồi quy 5% của kế hoạch. Ở lượt đó, ứng viên được giữ lại để đo tiếp. Phép đo này chưa bao phủ CPU/RSS của DB dưới tải dài, chi phí ghi duy trì index, giá trị nóng, spool gần giới hạn, hay tải chung với backup. Số liệu dùng để chọn kịch bản thử tiếp trong pilot.

## Đo lại thay đổi writer PostgreSQL

Để chạy lại, build binary tham chiếu từ writer hiện tại. Áp [bản vá ứng viên](../tools/bench/keyless-pg-integer-equality.patch) bằng `git apply` trong một bản sao làm việc rồi chạy benchmark từ bản sao đó. Benchmark tự build binary ứng viên và chỉ dùng các cơ sở dữ liệu thử nghiệm.

```sh
STRAND_KEYLESS_BENCH=1 STRAND_KEYLESS_REFERENCE_BIN=/abs/path/strand-reference \
  go test ./tests/e2e -run '^TestPGKeylessPerformanceBaseline$' -v -count=1 -timeout 15m
```

So sánh trên PostgreSQL 18, với cùng B-tree không unique ở đích cho cả hai writer và dữ liệu một triệu hàng. Tải danh nghĩa là 100 transaction/giây. Mỗi lượt keyless có 50 transaction làm nóng cho mỗi chế độ tải, rồi đo 300 transaction, đảo thứ tự hai binary qua bốn cặp. Sau mỗi lượt, benchmark so chính xác đa tập ở nguồn và đích.

| Keyless UPDATE/DELETE-heavy | Writer tham chiếu | Writer ứng viên |
| --- | ---: | ---: |
| p99 có nhịp, ms | 5.706 (5.296–6.711) | 98,8 (63,0–151,4) |
| TPS burst đã apply | 41,5 (39,3–42,6) | 629,2 (526,1–765,9) |
| RSS đỉnh của Strand trong burst, KiB | 54.944 (54.588–55.164) | 53.858 (53.380–54.004) |

Cả bốn cặp đều khớp đa tập. Đối chứng có khóa khi dựng lại dữ liệu cho từng lượt có p99 median 60,9 ms và 81,7 ms, dù TPS có nhịp gần như nhau. Lượt đối chứng tiếp theo luân phiên hai binary qua sáu cặp trên cùng bảng đã làm nóng, tiếp tục từ checkpoint trước đó. Mỗi lượt đo 400 transaction sau 50 transaction làm nóng. Đặt `GOMAXPROCS=2` cho lượt này và dùng `-run '^TestPGKeylessPerformanceBaseline$/^keyed_control$'` để chạy riêng đối chứng. Đối chiếu đa tập sau sáu cặp cũng khớp.

| Đối chứng có khóa trên cùng dữ liệu | Writer tham chiếu | Writer ứng viên |
| --- | ---: | ---: |
| p99 có nhịp, ms | 61,8 (60,5–62,8) | 78,4 (61,3–113,1) |
| TPS có nhịp đã apply | 98,9 (98,8–99,1) | 99,0 (98,8–99,4) |
| TPS burst đã apply | 685,4 (579,2–807,6) | 753,9 (630,2–796,6) |

**Quyết định gate.** Tạm giữ writer mặc định vì p99 có nhịp của đối chứng có khóa cao hơn 26,8%, vượt ngưỡng 5%. Chưa xác định nguyên nhân của chênh lệch này. Bản vá ứng viên có thể dùng lại để đo và lấy profile trên máy thử ổn định hơn. Ứng viên dùng phép bằng cho giá trị số nguyên khác NULL, giữ điều kiện so khớp toàn bộ ảnh trước và khóa vật lý `(tableoid, ctid)`. Mức tăng tốc đo được áp dụng cho bảng thử có B-tree phù hợp.

## Diễn tập MinIO tại lab

Hai bản PostgreSQL logical dump chứa cùng 5.000 hàng và dùng cùng cấu hình nén. Bản không mã hóa có 9.124 byte lưu trữ, mất khoảng 231 ms để backup và 237 ms để restore. Bản mã hóa bằng khóa cũ có 9.189 byte, mất khoảng 203 ms để backup và 207 ms để restore sau khi đổi khóa active. Hai đích đều qua kiểm payload theo từng ID. Bản tạo sau khi xoay khóa ghi nhận key ID mới. Đây là thời gian một lượt trên fixture nhỏ.

## Diễn tập metadata Web tại lab

Trên PostgreSQL 17, dùng `strand web init` để tạo metadata, khởi động một Web instance, ghi policy mốc rồi dump bằng `pg_dump -Fc`. Ghi thêm policy sau mốc dump, dừng Web và dùng `pg_restore --exit-on-error` để nạp dump vào database mới. Khởi động lại Web với cùng khóa `STRAND_WEB_SECRET_KEY`, cấu hình cũ và DSN mới. Bản dump 73.314 byte phục hồi đủ 22 migration, admin và policy mã hóa tại mốc dump. Policy ghi sau dump vắng mặt như dự kiến. `/healthz` xác nhận Web active và lease đổi từ epoch 1 sang 2 sau 31,04 giây kể từ lúc bắt đầu restore. Thời gian này bao gồm chờ lease mặc định 30 giây. Kết quả thuộc fixture lab và mất dữ liệu sau mốc dump được kiểm bằng hai bản ghi đánh dấu.

## Hợp đồng CDC keyless theo engine

| Nguồn → đích | Ảnh hàng nguồn | Kiểu và phép so đích | UPDATE/DELETE |
| --- | --- | --- | --- |
| MySQL → MySQL | Binlog ROW/FULL. Phải có cả ảnh trước và ảnh sau khi UPDATE. | So NULL-safe `<=>`. Cột chuỗi còn so byte sau khi chuyển sang utf8mb4 để giữ phân biệt hoa/thường, dấu và khoảng trắng. Cột nhị phân so theo giá trị nhị phân. | `LIMIT 1`, kiểm số hàng khớp. |
| MariaDB → MySQL | Binlog ROW/FULL. Kiểm cả bộ ký tự và collation sau ánh xạ. | Cùng writer MySQL và điều kiện so toàn ảnh. Cột JSON dạng LONGTEXT của MariaDB được xử lý như text khi schema đích tương thích. | `LIMIT 1`, kiểm số hàng khớp. |
| MySQL → PostgreSQL | Binlog ROW/FULL. Dùng giá trị đã chuẩn hóa theo kiểu chuyển đổi đã được validate. | So từng cột bằng `IS NOT DISTINCT FROM`. Collation đích phải deterministic. | Subquery chọn một cặp `(tableoid, ctid)`, kiểm đúng một hàng. |
| PostgreSQL → PostgreSQL | pgoutput với `REPLICA IDENTITY FULL` và ảnh trước đầy đủ. TOAST không đổi ở ảnh sau được giữ nguyên ở đích. | So NULL-safe và từ chối collation nondeterministic hoặc citext. Dùng cùng cặp định danh vật lý cho bảng partitioned. | Subquery chọn một cặp `(tableoid, ctid)`, kiểm đúng một hàng. |

Keyless dùng dữ liệu đã seed. Các kiểu hình học hoặc kiểu thiếu phép so đáng tin cậy bị từ chối. Float mặc định bị từ chối và cần opt-in `tables.keyless_allow_float`. Các kiểu được ánh xạ qua engine khác vẫn phải đạt kiểm tra mapping của pipeline.

Reader chuyển ảnh hàng tới assembler. Assembler giữ dữ liệu đầy đủ của một transaction, tạo khóa xung đột hằng theo bảng keyless và có thể spill payload. Scheduler giữ thứ tự cho các giao dịch chạm cùng bảng keyless, kể cả giao dịch chạm thêm một bảng có khóa. Writer ghi dữ liệu và checkpoint bền trong cùng transaction đích. Ảnh hàng bị thiếu dừng ở assembler trước khi giao việc cho writer. Ảnh trước không tìm thấy ở đích trả `keyless_row_missing`, hủy toàn bộ transaction đích. Với UPDATE đã xác nhận ảnh trước bằng ảnh sau, writer kiểm sự tồn tại mà không sửa dữ liệu. Restart bỏ qua transaction đã commit dựa vào checkpoint bền, kể cả trường hợp process chết khi chưa nhận được xác nhận commit.

## Phép đo truy vấn MySQL

Dùng MySQL 8.4.13 và chạy [keyless-mysql.sql](../tools/bench/keyless-mysql.sql) trong database thử nghiệm. Fixture có 100.005 hàng gồm hai bản sao cần khớp và các hàng khác nhau ở NULL, hoa/thường, khoảng trắng và byte. Chạy một lượt làm nóng và ba lượt đo trên mỗi cấu hình index. Đo UPDATE/DELETE thực trong một transaction, hoàn tác giữa các lượt và kiểm số lần xuất hiện của các hàng sau lượt cuối.

| Index trên cột số | EXPLAIN UPDATE / DELETE | UPDATE median (ms) | DELETE median (ms) |
| --- | --- | ---: | ---: |
| Không có | Full scan, ước tính 100.331 hàng | 23,22 | 22,67 |
| B-tree không unique | Range, ước tính 6 ứng viên | 0,080 | 0,052 |

Cùng predicate full-row của writer hiện hành được dùng cho cả hai cấu hình. Bộ đếm `Handler_read_rnd_next` tăng 800.036 lần cho tám câu không index và giữ nguyên với index. Kết quả đo truy vấn này hỗ trợ lựa chọn index thử nghiệm trên MySQL. Thông lượng và lag đầu cuối PostgreSQL được báo riêng ở trên.

## Đối chứng có khóa PostgreSQL khi đo timestamp commit

Đo lại đối chứng PostgreSQL ở tải cố định sau khi thêm timestamp commit tại đích. Hai binary cùng dùng một bảng có khóa, dữ liệu một triệu hàng và checkpoint đã làm nóng. So ba cặp AB/BA/AB, 400 giao dịch đo cho mỗi biến thể mỗi mức tải. Dùng `STRAND_KEYLESS_CONTROL_PAIRS=3` và chỉ chạy subtest `keyed_control`. Median lag p99 tải thường là 62,17 → 61,57 ms. Median p99 từ marker nguồn đến commit đích là 56,51 → 56,41 ms, còn từ commit đến lần quan sát thấy marker là 7,26 → 6,60 ms. Ở tải phát liên tục, median 703,8 → 763,3 TPS và median lag p99 66,78 → 70,24 ms (+5,18%). Mọi lượt khớp đa tập. Kết quả tải cố định trước đó chưa tái hiện. Một đợt phát sinh đuôi trễ ở lượt burst cuối làm tăng độ biến động. Bản vá PostgreSQL vẫn là ứng viên do p99 burst sát ngưỡng và số đo từ máy lab.

### Lượt đối chứng PostgreSQL tiếp theo

Chạy thêm chín cặp AB/BA trên cùng bảng có khóa một triệu hàng, 400 giao dịch đo sau warm-up cho mỗi mức tải. Lệnh ở trên dùng thêm `STRAND_KEYLESS_CONTROL_PAIRS=9` và `-run '^TestPGKeylessPerformanceBaseline$/^keyed_control$'`. Cả hai binary dùng cùng dữ liệu và tiếp tục từ checkpoint. Mọi lượt qua đối chiếu đa tập.

| Đối chứng có khóa | Tham chiếu | Ứng viên |
| --- | ---: | ---: |
| Lag p99 có nhịp, ms, trung vị (min–max) | 100,06 (61,88–385,26) | 129,42 (62,01–532,17) |
| Đến commit đích p99 có nhịp, ms | 92,93 | 90,18 |
| Lag p99 burst, ms | 65,93 | 67,89 |
| TPS burst | 796,7 | 716,2 |

Chênh lệch lag tải có nhịp chưa tách được khỏi độ nhiễu khi quan sát và khi source phát tải. Thông lượng burst của đối chứng giảm 10,1%, vượt biên sàng lọc 5%. Quyết định giữ bản vá index PostgreSQL ở dạng thử nghiệm.

## Diễn tập thiếu WAL và đổi timeline

`go test ./internal/backup/runner -run '^TestPITRPostgres$' -count=1` dùng PostgreSQL 17 và MinIO. Cùng bài chạy kiểm phục hồi 200 hàng trên bản dump vật lý và WAL, thiếu một object WAL không công bố thư mục khôi phục, và PostgreSQL từ chối thời điểm vượt WAL có sẵn. Sau khi bản restore promote sang timeline mới, archiver dùng job cũ dừng do lineage khác và không để lại slot giữ WAL. Muốn tiếp tục bảo vệ timeline mới, tạo base backup mới với job archive đã chọn cho timeline đó rồi kiểm restore. Web cho phép yêu cầu với chuỗi WAL đủ để thử replay và ghi rõ PostgreSQL sẽ xác nhận thời điểm khi khởi động recovery.
