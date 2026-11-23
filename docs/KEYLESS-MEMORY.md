# Bộ nhớ khi đích PostgreSQL chậm

`TestPGKeylessSlowTargetMemory` chạy hai máy chủ PostgreSQL, một process `strand sync` riêng và một bảng không khóa ở chế độ `full_row`. Nguồn dùng `REPLICA IDENTITY FULL`. Cả hai DB bật `fsync` và `synchronous_commit`. Pipeline chạy `commit_order: relaxed`, tối đa tám worker, batch 2.000 hàng / 4 MiB / 50 ms. Các transaction cùng bảng keyless vẫn phụ thuộc theo thứ tự commit.

## Phép đo

1. Giữ khóa `SHARE` trên bảng đích để chặn writer. Nguồn lần lượt ghi một hàng, hai transaction 12.000 hàng rồi 16 transaction 256 hàng. Mỗi hàng có một trong 32 ID lặp lại. Payload dài 256 byte hoặc NULL. Hai transaction lớn vượt ngưỡng spill 10.000 hàng.
2. Mỗi 100 ms, bộ đo đọc kích thước tất cả file thường trong thư mục state/spool, đếm file `.spill` và lấy `strand_writeset_entries` cùng `strand_scheduler_pending_bytes`. Pipeline cập nhật hai gauge mỗi giây. Pending bytes là số byte ước tính đã tính vào ngân sách scheduler, gồm cả làn tuần tự. RSS lấy từ `VmHWM` của process Strand. Các đỉnh của từng chỉ số có thể xảy ra ở thời điểm khác nhau.
3. Sau khi marker trên bảng độc lập đến đích, chờ thêm một nhịp cập nhật gauge để đo cùng backlog. Khi có ít nhất hai spill và hơn 1 MiB đang chờ, gửi `SIGUSR1` cho bản CLI gắn probe để thu heap sau GC. Probe nằm trong overlay chỉ dùng khi build cho bài đo. Mẫu 50 ms của Go runtime cho biết đỉnh heap object đã quan sát đến thời điểm này. `go tool pprof -sample_index=inuse_space` chỉ ra các stack còn giữ bộ nhớ lúc đích bị khóa.
4. Nhả khóa rồi đo thời gian đến khi marker sang đích, checkpoint bền đã qua vị trí WAL trước marker và pipeline báo synced. Đối soát toàn bộ hàng đã sắp theo ID và payload với collation C, phân biệt NULL và đếm mọi bản sao. Kill tiến trình đã bắt kịp, ghi thêm một transaction 12.000 hàng khi nó dừng, chạy lại cùng binary từ checkpoint và lặp đối soát.
5. Ngân sách hồi quy của fixture là 256 MiB RSS, 256 MiB thư mục spool và 60 giây cho từng lượt bắt kịp. Ngưỡng vận hành trên dữ liệu thực phụ thuộc tài nguyên máy đích và RTO của người vận hành.

Chạy trên Linux với Docker và Go theo phiên bản trong `go.mod`. Đặt `STRAND_E2E_KEEP_DIR` thành thư mục riêng đã tạo nếu cần giữ profile và log chi tiết. Thư mục giữ lại chứa config kết nối của fixture.

```sh
GOMAXPROCS=2 STRAND_KEYLESS_RAM_BENCH=1 \
  go test -p 1 -count=3 -timeout=15m -v ./tests/e2e -run '^TestPGKeylessSlowTargetMemory$'
```

## Kết quả đối chứng

PostgreSQL 18.7, giới hạn 4 CPU, `GOMAXPROCS=2`. Chạy ba cặp AB/BA/AB bằng cùng mã và cấu hình. Binary tham chiếu giữ thao tác ghi mục cleanup theo từng lần xuất hiện của khóa. Binary ứng viên giữ một mục cho mỗi khóa do cùng transaction sở hữu. Cả hai có cùng metric pending bytes. Bộ đo dùng `STRAND_KEYLESS_RAM_REFERENCE_SCHEDULER` với đường dẫn tuyệt đối tới `scheduler.go` tham chiếu để overlay đúng một file khi build CLI.

| Chỉ số | Tham chiếu, trung vị (min–max) | Ứng viên, trung vị (min–max) |
| --- | ---: | ---: |
| Heap còn sống tại điểm đo, MiB | 11,96 (11,96–11,96) | 10,41 (10,40–10,41) |
| Đỉnh heap object đã lấy mẫu, MiB | 28,15 (28,13–29,03) | 32,75 (30,86–33,04) |
| Đỉnh RSS, MiB | 76,32 (75,29–80,23) | 79,75 (79,05–80,12) |
| Pending đã tính vào ngân sách, MiB | 4,09 (4,09–4,09) | 4,09 (4,09–4,09) |
| Đỉnh spool, MiB | 15,09 (15,09–15,84) | 15,34 (15,34–16,84) |
| Số entry writeset đỉnh | 33 (33–33) | 33 (33–33) |
| Bắt kịp sau nhả khóa, giây | 3,64 (3,46–3,65) | 3,85 (3,24–3,86) |
| Bắt kịp sau restart, giây | 2,45 (2,24–2,69) | 2,29 (2,23–2,69) |

Heap còn sống giảm khoảng 13% và RSS trung vị tăng khoảng 4,5%. Profile của baseline giữ 0,9–2,8 MiB mẫu tại `Scheduler.Admit`. Ứng viên còn khoảng 0,5 MiB ở một lượt và dưới ngưỡng hiển thị ở hai lượt. Sáu lượt đều trong ngân sách lab và khớp toàn bộ 28.097 hàng sau khi nhả khóa, cộng 12.000 hàng ghi khi Strand dừng. Giữ thay đổi cục bộ ở scheduler vì test xác nhận số mục cleanup giảm theo thiết kế, mức giữ heap lặp lại qua ba cặp và thứ tự giao dịch được giữ. Tiêu chí RSS production cần ngân sách từ môi trường triển khai.

Tạo file tham chiếu khi cần tái lập cặp đối chứng từ checkout này. Bản tham chiếu lấy scheduler ở mốc kế hoạch và bổ sung đúng phương thức đọc metric.

```sh
reference_scheduler=$(mktemp /tmp/strand-scheduler-reference.XXXXXX.go)
{
  git show a64c98d:internal/sched/scheduler.go
  sed -n '/^\/\/ PendingBytes is /,/^}/p' internal/sched/scheduler.go
} > "$reference_scheduler"
GOMAXPROCS=2 STRAND_KEYLESS_RAM_REFERENCE_SCHEDULER="$reference_scheduler" \
  STRAND_KEYLESS_RAM_BENCH=1 go test -p 1 -count=1 -timeout=5m -v ./tests/e2e -run '^TestPGKeylessSlowTargetMemory$'
```
