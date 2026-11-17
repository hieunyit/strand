> Review bổ sung và quyết định gộp hai nhánh: [merge-review/REVIEW.md](../merge-review/REVIEW.md).

# Kết quả sửa các finding của review ab5160d

Các bản sửa đã có trong commit `e3d19bb` trên nhánh hiện tại `fix/comprehensive-hardening`. Review gốc đánh giá `ab5160d`; bằng chứng trong thư mục này được thu thập trong quá trình sửa, trước commit tổng hợp. Báo cáo này không xác nhận các thay đổi bổ sung của commit ngoài phạm vi finding bên dưới.

Đã chia việc cho agent GPT-6.1 Terra (data/identity), GPT-6.1 Sol (web/storage và diagnostic/e2e); root tích hợp, sửa identity/profiling và kiểm tra chéo. Không dùng agy/Claude trong đợt sửa này.

## Đối chiếu từng finding

| Mã | Bản sửa và kết quả |
|---|---|
| B01 | Prefetch chỉ đọc ArchiveSchema khai báo rõ khi dùng archive; không đọc SQL SchemaSnapshot như JSON sidecar. |
| B02 | STX4 lưu nguồn và identity có thứ tự. Pipeline kiểm mode, tên/thứ tự key và columns trước coverage và trước apply; kiểm từng row cả khi spill. Key PK/unique NULL bị từ chối; keyless cho phép NULL. |
| B03 | Sidecar từ chối QUERY ngoài BEGIN/COMMIT/ROLLBACK; TABLE_MAP phải có tên cột đúng thứ tự, không nhận metadata MINIMAL thiếu tên. |
| B04 | Functional UNIQUE không còn được coi tương đương chỉ vì cùng cờ expression; từ chối nếu không chứng minh được ngữ nghĩa. |
| B05 | Đọc indnullsnotdistinct qua to_jsonb để tương thích catalog cũ; không fallback sang metadata thiếu thông tin khi catalog lỗi. |
| B06 | Phát hiện secret từ scalar YAML đã decode; DSN multiline chứa secret không thể mask an toàn trả lỗi và output rỗng. |
| B07 | Restore credential theo giá trị đã decode và kiểu quote mới, parse lại để kiểm giá trị; không đưa raw YAML escape cũ vào ngữ cảnh mới. |
| B08 | Import inventory kiểm lại filesystem và accounting hiện tại dưới mutex, giữ refs và size live thay vì tin báo cáo cũ. |
| B09 | Mỗi lần mở spool đều reconcile orphan regular file trong spill tree chuẩn, charge bằng batch đồng bộ; không đi theo symlink. |
| B10 | Ghi nhận lỗi/short write từ CPU profile writer và trả lại khi Stop, kể cả lỗi bất đồng bộ. |
| B11 | Canonical hóa output path, kiểm SameFile và reserve cả hai handle trước truncate; từ chối alias tương đối/tuyệt đối. |
| B12 | Profile regular file có quyền 0600; harness dùng thư mục 0700, artifact 0600, config bí mật tạm được xóa và export đã redact. Không chmod thư mục công khai có sẵn. |
| B13 | Container tên ngẫu nhiên và label riêng; cleanup chỉ dùng full ID thực sự tạo được, không xóa theo tên cố định. |
| B14 | External DSN cần tắt auto-Docker và bật allow-destructive-existing-databases rõ ràng trước thao tác phá dữ liệu benchmark. |
| B15 | Đổi metric thành rows_per_source_txn; không diễn giải thành kích thước batch commit ở target hoặc trần hiệu năng. |
| B16 | Crash gate yêu cầu đúng plan app.items có bounds/LSN, tiến độ đã lưu và việc còn lại; kiểm checkpoint và giữ nguyên bounds sau resume. |
| B17 | Phase dùng atomic, join sampler/child trước đọc kết quả và teardown. |
| Q01 | Rút kết luận nguyên nhân 280–300 TPS chưa được chứng minh trên máy người dùng. |
| Q02 | Thiếu gh-ost là SKIP; required gate phải FAIL khi thiếu binary. |
| Q03 | Browser test tác động fetch refresh thật của app, kiểm response sai tab và response cũ sau query navigation. |

## Bằng chứng kiểm thử

- `go vet ./...`, build `./cmd/strand ./cmd/replay`: PASS tại đợt tích hợp.
- Race regression identity/prefetch/profile, gồm reader MySQL, cmd, pipeline, spool: PASS; parser thật → prefetch → reopen spool có PK ghép, unique và keyless.
- Toàn bộ package web/spool: PASS. Các regression constraint/archive/web/storage chạy race: PASS.
- Toàn bộ ba package diagnostic chạy race: PASS (`tools-final-race.log`). Regex ở lượt integration không match test browser/remaining-gate; không tính lượt không có test đó làm bằng chứng.
- Chromium thật: 10 scenario PASS (`browser-live.log`).
- PostgreSQL crash/resume thật: PASS, parity 64.725 dòng/4 bảng; 21.400 workload operations, không lỗi client. Lúc kill còn pending ranges, reread_keys=0 (`crash-live.log`).
- PG smoke với race: PASS; chỉ 20 dòng, một client, warmup 1 giây và đo 2 giây. Đây là kiểm tra chức năng/cleanup, không phải benchmark hay chứng minh root cause (`pg-smoke-live.log`).
- Required gh-ost cố ý FAIL khi thiếu binary: xác nhận gate hoạt động, không phải kiểm thử gh-ost thật.

Chi tiết từng agent và log nằm trong [evidence](evidence/). Các repro lịch sử thiết kế để chứng minh bug cũ không được dùng nguyên trạng làm tiêu chí PASS sau sửa nếu assertion mong hành vi lỗi.

## Giới hạn và tương thích cần chú ý

1. Prefetch cũ chưa checkpoint và không có provenance bị từ chối. Cần dựng lại từ nguồn/archive tin cậy; không tự xóa nếu log đã purge. Dùng cùng phiên bản mới cho seed/sync; binary cũ không đọc STX4. Spool generic STX2/STX3 vẫn đọc được.
2. Source prefetch yêu cầu metadata identity rõ ràng và FULL row image; không đoán khóa bằng cột đầu. Review bổ sung đã sửa việc truyền mode UNIQUE NOT NULL từ catalog (M01 trong merge-review). Archive unique/keyless có sidecar đúng contract được hỗ trợ. Direct CDC không bị cấm chỉ vì bảng không có PK. Metadata từ catalog hiện tại chưa chứng minh schema tại thời điểm log lịch sử.
3. Archive sidecar không dựng lịch sử DDL; yêu cầu FULL named TABLE_MAP và từ chối QUERY không được hỗ trợ. Expression index thực sự tương đương vẫn có thể bị từ chối khi chưa chứng minh được.
4. B05 có regression fixture, chưa chạy matrix PostgreSQL trước/sau phiên bản 15 thật.
5. Chưa chạy full repository suite, stress 15 lượt, kill ngay trong active reread, hoặc gh-ost binary thật. Chưa chốt nguyên nhân race TOAST hiếm gặp, trần TPS, MariaDB breaker hay lỗi chỉ xuất hiện trên máy người dùng. Một lượt crash/resume PASS không chứng minh hết race.
6. Báo cáo này được soạn trước review gộp; trạng thái commit/gộp cuối cùng xem merge-review/REVIEW.md và git log.
