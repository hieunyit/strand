# Gate 2 — Giao thức benchmark so sánh ba governor (P2-09)

Tài liệu này được commit **trước** khi chạy bất kỳ lần đo nào (spec/04-testing.md §5: "giao thức chạy được cố định trước khi có kết quả. Đổi giao thức sau khi đã thấy số thì phải ghi lại lý do trong báo cáo gate"). Mọi thay đổi sau commit này phải ghi ở mục "Thay đổi giao thức" cuối file kèm lý do.

Công cụ: `tools/bench` (`go run ./tools/bench --help`). Nguồn đặc tả: spec/04-testing.md §5, spec/02-specs.md §7, spec/01-plan.md (Nhánh GRACE → bật/tắt).

## 1. Môi trường

| Mục | Chính thức (Gate 2) | Bản chạy trên máy dev (tham khảo) |
|---|---|---|
| Máy | Staging (02/04: "Staging — Gate 2") | Máy dev 16 core / 31 GB, MySQL 8.4 trong Docker |
| Source/target | MySQL 8.4 → MySQL 8.4 (và PG 16–18 với pgbench khi có staging PG) | MySQL 8.4 → MySQL 8.4 |
| Điều kiện | Không chạy việc khác trên cùng máy | Không chạy e2e/soak song song; ghi `uptime` đầu/cuối mỗi lần |

Kết quả trên máy dev **chỉ để tham khảo**, không dùng để chốt governor mặc định; báo cáo phải ghi rõ.

## 2. Thiết lập (cố định)

| Thành phần | Thiết lập |
|---|---|
| Governor | `static`, `onoff`, `grace-v2` (`governor.algorithm`) |
| Giới hạn tài nguyên | `resources.source.iops: { soft: S, hard: H }` — cùng giá trị cho cả ba governor; dev: S = 400, H = 600 (token/giây) |
| `governor` | `tick: 1s`, `lag_slo: 5s` |
| Bảng snapshot | `bench.big`: N row cố định (dev: 2.000.000 row ≈ 220 B/row), sinh tất định theo seed — cùng nội dung ở mọi lần |
| Tải ứng dụng | Bộ sinh tải kiểu `sysbench oltp_read_write` viết trong `tools/bench` (sysbench không có trên máy dev): mỗi transaction = 10 point select + 1 update cột có index + 1 update cột không index + 1 delete + 1 insert trên `bench.sbtest` (100.000 row); latency đo ở phía client cho cả transaction |
| Tải nền | Sóng vuông giữa mức thấp và mức cao, chu kỳ 10 phút (5 phút mỗi mức); spike ×2 mức cao trong 10 giây, mỗi 3 phút. Dev: thấp 50 txn/s, cao 200 txn/s, 16 client. Biên độ chính thức chốt theo năng lực staging (02/04 §5) |
| Việc của Strand | Snapshot logic `bench.big` + stream liên tục (tải ứng dụng ghi `bench.sbtest`, cũng thuộc pipeline) |
| Khởi động mỗi lần | Dựng lại `bench` ở source từ seed (tương đương khôi phục cùng snapshot dữ liệu; buffer pool không được khôi phục — ghi chú hạn chế), xoá database đích + metadata `_strand_*`, khởi động lại cả hai container MySQL để buffer pool lạnh như nhau |
| Trình tự một lần | Dựng dữ liệu → khởi động lại MySQL → tải ứng dụng chạy 120 s khởi động → khởi động Strand → đo tới khi log `bootstrap finished` → thêm 120 s stream → dừng tải → chờ target bắt kịp (`gtid_executed` của source ⊆ tập đã apply, tối đa 10 phút) → dừng Strand (SIGTERM, phải exit 0) → so khớp |
| Lặp | 3 lần cho mỗi governor (9 lần), thứ tự ngẫu nhiên theo seed in ở đầu báo cáo |
| Pha sóng | Sóng vuông bắt đầu ở mức thấp tại thời điểm khởi động tải, độc lập với Strand |

## 3. Đo

Lấy mẫu mỗi 1 giây từ endpoint metrics của Strand và từ bộ sinh tải:

| Đại lượng | Nguồn |
|---|---|
| Thời gian snapshot | Từ lúc khởi động `strand sync` tới log `bootstrap finished: pipeline synced` |
| p99 ứng dụng | Latency transaction phía client, tính p99 trên toàn khoảng snapshot (và theo cửa sổ 10 s để vẽ) |
| R | `strand_governor_rate_tokens{resource="source_iops"}` |
| Số lần vượt hard limit | Số mẫu có R > H (token bucket phải chặn nên kỳ vọng 0) |
| Peak-to-peak của log R ở trạng thái xác lập | Với mỗi nửa chu kỳ tải không đổi nằm trọn trong khoảng snapshot, bỏ 60 s đầu (chuyển tiếp), tính max(ln R) − min(ln R) trên phần còn lại; báo cáo **trung vị** và **max** qua các nửa chu kỳ. Chỉ áp cho `grace-v2` (`onoff` có R ∈ {0, S}, `static` hằng số). Nửa chu kỳ có R = 0 bị loại khỏi phép tính và được đếm riêng |
| Lag | `strand_lag_seconds` |
| Zone / pressure / breaker | `strand_governor_zone`, `strand_governor_pressure{signal="total"}`, `strand_breaker_open` |

## 4. Quy tắc quyết định (spec/04-testing.md §5, spec/01-plan.md)

1. Điều kiện cứng cho mọi governor: **0 lần vượt hard limit** ở mọi lần chạy.
2. Riêng `grace-v2`: **peak-to-peak của log R ≤ 0,3** — dùng trung vị qua các nửa chu kỳ của mỗi lần chạy, và mọi lần chạy phải đạt.
3. `grace-v2` làm mặc định khi: median (qua 3 lần) thời gian snapshot của `grace-v2` ngắn hơn median của `onoff` **ít nhất 30%**, **và** median (qua 3 lần) p99 ứng dụng của `grace-v2` **không lớn hơn** median của `onoff`.
4. Không đạt → `onoff` là mặc định, GRACE thành tùy chọn experimental (spec/01-plan.md: "Nhánh GRACE → bật/tắt").
5. Một lần chạy lỗi (Strand thoát khác 0, mất dữ liệu ở bước so khớp cuối, máy bị tác động ngoài): ghi lại nguyên nhân, **không** loại khỏi kết quả mà không ghi; chạy bù một lần ở cuối thứ tự, ghi rõ.

Sau mỗi lần chạy: so khớp `bench.big` và `bench.sbtest` giữa source và target (đếm + checksum theo chunk) để chắc snapshot đúng; lệch = lần chạy lỗi.

## 5. Đầu ra

- `results.json`: cấu hình, seed, thứ tự, mỗi lần chạy (governor, thời gian snapshot, p99, số lần vượt H, p2p log R trung vị/max, số mẫu, kết quả so khớp), tổng hợp theo governor (median) và kết luận theo §4.
- `timeseries-<run>.csv`: t, R, ln R, zone, pressure, breaker, lag, rate tải mục tiêu, p99 cửa sổ 10 s.
- Biểu đồ R, P và p99 theo thời gian cho từng lần chạy được dựng **từ dữ liệu thật sau khi chạy** (`tools/bench --plot`).

## 6. Hạn chế đã biết (ghi trước khi chạy)

- Máy dev không phải staging; Docker chia sẻ I/O với hệ điều hành.
- "Khôi phục snapshot" bằng dựng lại dữ liệu tất định + khởi động lại MySQL, không phải khôi phục snapshot đĩa.
- Bộ sinh tải tự viết thay sysbench (cùng cấu trúc transaction như `oltp_read_write` nhưng ít câu hơn).

## Thay đổi giao thức

- (trước lần đo đầu tiên) Dừng tải và chờ target bắt kịp trước khi dừng Strand, để so khớp cuối có nghĩa.


## Làm rõ điều kiện dữ liệu trước lượt đo chính thức — 2026-11-07

Review phát hiện runner cũ có thể khuyến nghị GRACE khi toàn bộ scrape lỗi hoặc không có cửa sổ xác lập. Đây là sửa việc thi hành giao thức, không thay ngưỡng sau khi thấy kết quả. Chưa chạy benchmark chính thức trong lần sửa này.

- Decision yêu cầu đủ static/onoff/grace-v2, đúng ba repeat độc lập mỗi loại.
- Snapshot phải có telemetry hợp lệ gần đủ số giây đo (cho phép lệch hai mẫu ở biên); scrape lỗi trong khoảng snapshot làm kết quả incomplete. JSON ghi valid_samples_during_snapshot và invalid_samples_during_snapshot.
- Thiếu client p99, workload có lỗi, không có cửa sổ p2p hợp lệ hoặc p2p NaN/Inf đều không đủ bằng chứng quyết định.
- Ngưỡng 0,3 kiểm mọi cửa sổ xác lập của GRACE qua maximum p2p, không chỉ median.

Smoke/ngắn vẫn hữu ích kiểm runner nhưng không được dùng làm kết quả Gate 2. Review này không xác nhận runner và môi trường đã đáp ứng toàn bộ yêu cầu sysbench/pgbench/staging trong spec; cần kiểm giao thức trước lượt benchmark chính thức.

Sampler chỉ nhận mẫu đủ R, pressure, zone, breaker và lag hữu hạn từ HTTP 200; pressure bằng 0 vẫn là giá trị hợp lệ. Thiếu metric hoặc mẫu không hữu hạn làm phép đo incomplete.
