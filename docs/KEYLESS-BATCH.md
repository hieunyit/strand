# Chi phí writer và group commit trong lab keyless

## PostgreSQL → PostgreSQL, P3

Hai container PostgreSQL 18.7 dùng image ID `22fac36945da9a4145a11bc9a6198ebc00975b8323c8d454740e477c4e84593a`, bật fsync và synchronous commit, tắt autovacuum trong fixture để giảm nhiễu. Cả hai có 8.192 hàng (bigint và md5), index B-tree không unique trên id và một bảng marker có PK. Strand chạy strict, một worker, `GOMAXPROCS=2`, source dùng `REPLICA IDENTITY FULL`. Mỗi giao dịch gồm một thay đổi keyless và một marker có khóa. Tỷ lệ INSERT/UPDATE/DELETE là 10/60/30. Mỗi cấu hình bắt đầu với cùng dữ liệu, làm nóng 30 giao dịch ở từng mức tải, rồi đo 250 giao dịch tại 100 TPS và 250 giao dịch phát liên tục. Ba lượt xoay thứ tự cấu hình.

Trung vị của ba lượt, đơn vị độ trễ là ms. TPS đo từ đầu phát đến khi quan sát đủ marker ở đích. Lag tính từ timestamp marker tại nguồn đến lần đầu kiểm tra thấy marker ở đích, với chu kỳ kiểm tra 5 ms.

| max_rows | max_bytes | max_wait | 100 TPS thực đạt | Lag p95 / p99 tại 100 TPS | TPS phát liên tục theo từng lượt | Lag p95 / p99 phát liên tục |
| ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 2000 | 4 MiB | 50 ms | 99,5 | 67,39 / 72,26 | 641,6 / 464,1 / 641,2 | 178,18 / 187,80 |
| 2000 | 4 MiB | 2 ms | 100,0 | 11,80 / 17,57 | 789,6 / 690,2 / 780,4 | 21,64 / 23,98 |
| 16 | 4 MiB | 2 ms | 100,0 | 10,38 / 17,24 | 870,1 / 804,0 / 637,2 | 19,86 / 22,20 |
| 2000 | 1 KiB | 2 ms | 100,1 | 10,43 / 12,66 | 811,6 / 766,3 / 603,4 | 16,35 / 17,98 |

Thời gian sau dùng timestamp trong tiến trình Strand. Dispatch chỉ lấy giao dịch đầu mỗi batch, từ lúc router trả về sau Admit đến lúc worker lấy được nó (chặn giá trị âm ở 0 khi worker nhận trước lúc router ghi timestamp). Gather gồm thời gian gom batch. Commit gồm lời gọi tới driver và chờ xác nhận của DB. Mọi cặp số theo thứ tự tải 100 TPS / tải phát liên tục.

| Bộ giới hạn | Dispatch p95 (ms) | Gather p95 (ms) | Commit p95 (ms) | Giao dịch/commit |
| --- | ---: | ---: | ---: | ---: |
| 2000 / 4 MiB / 50 ms | 7,22 / 68,03 | 50,99 / 50,69 | 1,79 / 1,37 | 5,81 / 62,5 |
| 2000 / 4 MiB / 2 ms | 0,009 / 9,26 | 2,30 / 2,94 | 0,97 / 1,18 | 1,01 / 6,76 |
| 16 / 4 MiB / 2 ms | 0,005 / 7,50 | 2,30 / 2,78 | 0,94 / 0,93 | 1,00 / 6,25 |
| 2000 / 1 KiB / 2 ms | 0,008 / 5,07 | 2,31 / 2,95 | 0,85 / 0,99 | 1,00 / 5,10 |

Trong fixture này, `16 / 4MiB / 2ms` đạt trung vị 804 TPS khi phát liên tục, tăng 25% từ 641,2 TPS. Giảm `max_wait` giải thích phần lớn cải thiện lag ở tải thường. Trần 1 KiB có p99 thấp nhất trong fixture gồm các hàng nhỏ. Chọn cấu hình theo phân bố kích thước giao dịch và SLO của pilot. Mười hai lần restart đều bắt lại từ checkpoint sau khi ghi thêm hai hàng trùng chứa NULL lúc Strand dừng. Mỗi lượt đối chiếu toàn bộ giá trị và số bản sao, kiểm cả số marker đã commit.

```sh
GOMAXPROCS=2 STRAND_BATCH_SWEEP=1 go test -count=1 -timeout=10m -v ./tests/e2e -run '^TestPGKeylessGroupCommitSweep$'
```

## MySQL, P2

Fixture dùng MySQL 8.4 với image ID `0f1a0ccadda3a1734d1db557e203ec58621d56fa248525016e85fe4f631311ec`, driver go-sql-driver/mysql 1.8.1, cùng DB, cùng index và cùng transaction cho cả hai biến thể. Bộ đếm trên kết nối TCP nhận dạng từng gói COM_QUERY từ writer. Mỗi giao dịch có bốn lần INSERT/UPDATE/DELETE keyless, tổng cộng 12 thao tác, giữ nguyên các hàng trùng, NULL và biến thể hoa/thường sau commit. Writer cũ gửi 4 lần flush INSERT, 8 lần ghi cần RowsAffected, cộng BEGIN, checkpoint và COMMIT.

Ứng viên trong `tools/bench/keyless-mysql-checked-group.patch` gom câu lệnh và đọc kết quả `SELECT ROW_COUNT()` ngay sau từng UPDATE/DELETE qua `sql.Tx.QueryContext` và `Rows.NextResultSet`. Giới hạn 64 kết quả mỗi lần gửi cùng các giới hạn byte và tham số hiện có. Writer xác nhận từng giá trị bằng 1 rồi mới ghi checkpoint. Các lệnh sau lần kiểm cuối cũng được đọc để phát hiện lỗi SQL. Đường chạy thường trong checkout vẫn dùng writer hiện tại.

Chạy thử so chéo theo thứ tự AB, BA, AB (256 transaction mỗi biến thể mỗi lượt cho phép đo p95/p99) từ gốc repository. Overlay chứa hai file ứng viên, truyền qua GOFLAGS để cả binary Strand do E2E build cũng dùng đúng code.

```sh
p2root=$PWD
p2tmp=$(mktemp -d)
cp --parents internal/writer/{writer,keyless}.go "$p2tmp"
(cd "$p2tmp" && patch -s -p1 < "$p2root/tools/bench/keyless-mysql-checked-group.patch")
python3 - "$p2root" "$p2tmp" <<'PY'
import json
import pathlib
import sys
root, trial = map(pathlib.Path, sys.argv[1:])
files = ("internal/writer/writer.go", "internal/writer/keyless.go")
(trial / "overlay.json").write_text(json.dumps({"Replace": {str(root / f): str(trial / f) for f in files}}))
PY
GOMAXPROCS=2 GOFLAGS="-overlay=$p2tmp/overlay.json" STRAND_BATCH_SWEEP=1 STRAND_EXPECT_GROUPED=1 go test -count=1 -v ./internal/writer -run '^TestMySQLKeylessProtocolRoundTrips$'
GOMAXPROCS=2 GOFLAGS="-overlay=$p2tmp/overlay.json" go test -count=1 -timeout=10m ./tests/e2e -run '^TestKL_E2E_(MySQLToMySQL_Keyless|DivergenceFailsClosed)$'
```

| Biến thể | COM_QUERY/transaction | Thời gian trung vị (ms) | Khoảng min–max (ms) |
| --- | ---: | ---: | ---: |
| Keyless hiện tại | 15 | 13,02 | 6,61–16,16 |
| Keyless ứng viên | 4 | 6,72 | 4,58–8,48 |
| Có khóa hiện tại | 4 | 3,41 | 2,73–3,83 |
| Có khóa với ứng viên | 4 | 2,93 | 2,29–3,99 |

Transaction lỗi SQL, không khớp hàng và hủy context đều được kiểm trên MySQL thật. Các giao dịch lớn qua ranh giới 64 kết quả và các ca E2E MySQL→MySQL với bản vá ứng viên kiểm chính xác đa tập hoặc rollback và checkpoint tương ứng. Giữ ứng viên ở dạng bản vá thí nghiệm vì số đo keyed còn nhiễu và chưa có SLO triển khai.

## Đo lại P3 với timestamp commit tại đích

Bộ đo mới thu thêm timestamp commit của PostgreSQL. Chạy lại 12 case (bốn cấu hình, ba lượt) và đối chiếu sau restart. Trung vị ở lượt đo lại:

| max_rows / max_bytes / max_wait | p99 tại 100 TPS (ms) | p99 đến commit đích tại 100 TPS (ms) | TPS phát liên tục |
| --- | ---: | ---: | ---: |
| 2000 / 4 MiB / 50 ms | 66,01 | 61,39 | 675,6 |
| 2000 / 4 MiB / 2 ms | 13,39 | 8,33 | 796,7 |
| 16 / 4 MiB / 2 ms | 13,46 | 7,73 | 753,5 |
| 2000 / 1 KiB / 2 ms | 20,24 | 13,19 | 766,2 |

Dùng `2000 / 4 MiB / 2 ms` làm cấu hình thử pilot vì thay đổi riêng `max_wait` cải thiện ổn định qua cả hai đợt. Trong lượt đo lại, gather p95 ở tải cố định giảm 51,02 → 2,29 ms, còn commit p95 là 0,84 → 0,94 ms. Kết quả này hỗ trợ giả thuyết thời gian gom batch chi phối đuôi trễ của fixture.

## P2 qua pipeline MySQL → MySQL

Chạy hai binary từ cùng checkout. Binary tham chiếu giữ writer trong cây làm việc. Binary ứng viên build với overlay từ bản vá MySQL ở trên. Sau 30 giao dịch làm nóng, mỗi lượt đo 300 giao dịch ở nhịp 50 TPS và 300 giao dịch phát liên tục. Trong mỗi giao dịch nguồn có bốn nhóm INSERT/UPDATE/DELETE và một marker có khóa. Đo từ timestamp ghi marker ở nguồn tới lúc poll thấy marker đã commit ở đích. Cùng fixture MySQL 8.4, cùng batch strict `max_wait=2ms`, kiểm đủ số marker và khớp đa tập. Ba cặp chạy AB/BA/AB.

| Loại bảng và tải | TPS tham chiếu → ứng viên | Lag p95 tham chiếu → ứng viên, ms | Lag p99 tham chiếu → ứng viên, ms |
| --- | ---: | ---: | ---: |
| Keyless, 50 TPS | 50,0 → 50,0 | 25,70 → 17,21 | 30,45 → 24,43 |
| Keyless, burst hữu hạn | 90,8 → 113,4 | 33,39 → 19,45 | 48,05 → 26,13 |
| Có khóa, 50 TPS | 50,0 → 50,0 | 18,04 → 20,07 | 33,11 → 31,94 |
| Có khóa, burst hữu hạn | 109,8 → 120,6 | 17,05 → 17,36 | 24,15 → 27,49 |

Bộ đo driver riêng tăng lên 256 giao dịch mỗi biến thể mỗi lượt. Phép đo keyless ghi 15 → 4 COM_QUERY mỗi giao dịch và p99 apply 14,47 → 10,01 ms. Đối chứng chỉ có khóa vẫn gửi 4 COM_QUERY và p99 writer 6,48 → 5,73 ms. Lượt CDC trong bảng trên trùng với một suite MongoDB. Một lượt CDC keyless ứng viên ở tải cố định có p99 262,94 ms và chỉ phát đạt 45 TPS.

Lượt lặp lại ba cặp với cùng binary và điều kiện, sau khi các suite dùng DB khác đã dừng, cho kết quả trung vị sau. Tất cả các lượt đều khớp đa tập đích.

| Loại bảng và tải | TPS tham chiếu → ứng viên | Lag p95 tham chiếu → ứng viên, ms | Lag p99 tham chiếu → ứng viên, ms |
| --- | ---: | ---: | ---: |
| Keyless, 50 TPS | 50,0 → 50,0 | 16,36 → 17,05 | 21,94 → 42,00 |
| Keyless, burst hữu hạn | 122,4 → 128,2 | 27,26 → 16,07 | 30,22 → 23,32 |
| Có khóa, 50 TPS | 50,0 → 50,1 | 12,82 → 14,48 | 20,96 → 21,46 |
| Có khóa, burst hữu hạn | 140,5 → 138,7 | 13,90 → 14,03 | 20,56 → 17,36 |

Giữ ứng viên ở dạng bản vá thử nghiệm vì p99 keyless tại 50 TPS tăng 91,4% trong lượt lặp lại. Burst hữu hạn có thể chạm trần phía nguồn.

```sh
STRAND_MYSQL_KEYLESS_REFERENCE=/abs/path/strand-reference \
  STRAND_KEYLESS_CANDIDATE_BIN=/abs/path/strand-mysql-trial \
  GOMAXPROCS=2 go test -p 1 -count=1 -timeout=10m -v ./tests/e2e \
  -run '^TestMySQLKeylessRoundTripPipeline$'
```
