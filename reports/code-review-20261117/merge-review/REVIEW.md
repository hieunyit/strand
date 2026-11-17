# Review tích hợp hai nhánh trước khi gộp main

## Phạm vi và cách chia agent

Base main/origin/main: `5273009`. Nhánh `fix/03-pg-profiling-review`: `ab5160d`. Nhánh `fix/comprehensive-hardening` trước sửa bổ sung: `e3d19bb`. Hai nhánh nối tiếp nhau; hardening chứa toàn bộ nhánh profiling. Review đánh giá các thay đổi so với main và các đường liên quan; không khẳng định toàn bộ repository không còn bug.

- GPT-6.1 Terra/high: identity, prefetch, reader, snapshot/TOAST, unique/keyless và constraints.
- GPT-6.1 Sol/high: web, bảo mật, spool, profiling và diagnostic safety.
- GPT-6.1 Sol/high: promote, verify, fencing, dependencies và tính đúng của đo đạc.
- Root: đối chiếu finding, review bản sửa, chạy kiểm tra tích hợp và quyết định gộp. Dùng phạm vi riêng và log ngắn; không giao mỗi agent đọc lại toàn bộ báo cáo. Không sử dụng agy/Claude trong lượt này.

## Bug tìm thêm và bản sửa

| Mã | Mức | Lỗi được xác nhận | Bản sửa / regression |
|---|---|---|---|
| M01 | P1 | Source prefetch ghi mode pk cho bảng UNIQUE NOT NULL; sync từ chối contract đã lưu. FULL TABLE_MAP không PK còn bỏ qua catalog UNIQUE. | Truyền mode identity thực từ catalog qua reader tới STX4, kiểm tên/thứ tự/type với TABLE_MAP. Test minimal/FULL, mismatch, spool roundtrip và consumer compatibility. |
| M02 | P1 | Export InnoDB status còn lộ SQL continuation và dữ liệu physical record. | Chỉ xuất dòng diagnostic số theo allowlist; bỏ query/record/free text. Test multiline VALUES, dump hex/ASCII và semaphore parser. |
| M03 | P1 | Snapshot diagnostic ghi đè file vẫn giữ mode0644, đi theo symlink và sửa hardlink khác. | Temp file0600 rồi rename nguyên tử; telemetry export cùng helper. Test file cũ, symlink, hardlink và JSON hợp lệ. |
| M04 | P1 | Redact DSN lộ password với username rỗng hoặc password chứa@. | Theo delimiter grammar của driver, validate đầu vào và che toàn bộ nếu không parse được. Test các DSN được driver chấp nhận. |
| M05 | P2 | Tuning đọc/sửa trước khi lấy mutex lưu nên mất thay đổi đồng thời ở trường khác. | Mutex bao trọn read/apply/save, helper lưu có lock để không khóa lồng. Test giữ đủ năm thay đổi độc lập. |
| M06 | P2 | Tuning JSON không có giới hạn body như các endpoint config. | MaxBytesReader1MiB và regression từ chối request quá cỡ. |
| M07 | P2 | Promote quote làm mất khoảng trắng/backtick ở biên tên catalog và đổi identifier. | Escape nguyên giá trị raw, không trim/unquote; qualified name theo contract schema.table. Test SQL phát ra từ ApplyAutoIncrement, FindColumnMax, Setval. |
| M08 | P1 | Verify resume bỏ qua lỗi Mark/Wait/context, có thể trả sạch từ cache rồi xóa progress. | Trả lỗi có context/%w và giữ progress khi catch-up thất bại. Test cả lỗi và success. |
| M09 | P2 | PG diagnostic ParseInt không đọc counter Prometheus dạng1e+06, báo0 và sai TPS. | Parse counter dạng scientific có kiểm hữu hạn, nguyên, không âm và giới hạn int64. Test scrape HTTP và biên. |

M05/M06 là khoảng trống còn lại từ code có trước nhánh; các finding khác được xác nhận trên code được review. Repro trước sửa và kiểm tra sau sửa nằm trong ghi chú agent ở evidence. Không gộp nguyên trạng e3d19bb; cần chứa toàn bộ bản sửa bổ sung này.

## Sửa test fixture phát hiện bởi kiểm tra diện rộng

Lượt chạy tất cả package cmd/internal/tools/checker ban đầu FAIL ở pipeline và writer; log gốc giữ tại `evidence/packages-initial.log`.

- Simulator F11 gọi snapshot completion thật nhưng chưa cấp fake target DB/epoch và final watermark. Bổ sung mô hình DB/checkpoint, final marker qua simulated source, và assertion đã thực hiện completion watermark/checkpoint reads. Không bỏ kiểm tra durability trong production. Simulation mặc định30 seeds đã PASS.
- Ba test writer chưa khởi tạo pipeline epoch trước EnsureWorkerRow. Fixture dùng AdvanceEpoch thật và truyền epoch được trả về cho writer/delta. Không nới fencing production. Toàn bộ writer đã PASS sau sửa.

## Kiểm chứng và quyết định gộp

**Chấp nhận gộp sau khi bao gồm các bản sửa M01–M09 và hai nhóm fixture.** Không còn finding cụ thể chưa xử lý trong phạm vi review. Đây là quyết định dựa trên source review và các gate dưới đây, không phải chứng nhận không còn bug.

| Gate | Kết quả |
|---|---|
| Build toàn repository, go vet, go mod tidy -diff | PASS, không có tidy diff. |
| Broad cmd/internal/tools/checker với -short | Các package khác PASS; pipeline/writer FAIL ban đầu do fixture kể trên. Log gốc giữ nguyên. |
| Writer sau sửa fixture | Toàn package PASS85.680s. |
| Pipeline sau sửa simulator | Hoàn thành81 test đầu không có assertion lỗi; timeout tổng6phút khi test tiếp theo đang khởi động DB31giây. Rerun riêng42 test còn lại PASS102.703s. Không coi lượt timeout là PASS hay che log thất bại. |
| Regression tích hợp có -race sau tất cả bản sửa production | PASS cả8 package; xem final-race.log. |
| Web/breaker/PG-strict toàn package sau sửa, có -race | PASS93.319s /15.555s /2.268s. |
| PG→PG keyless, strict | E2E PASS40.26s. |
| MySQL→MySQL UNIQUE, strict | E2E PASS62.23s. |
| PG logical snapshot TOAST+đổi PK, strict | Ba lượt PASS; tổng test486.50s (command496.580s). Reread425/590/490 khóa, parity42.572/45.864/51.499 dòng ở4bảng,0lỗi client ở cả ba lượt. Kiểm doc không NULL, version đơn điệu và mỗi row version đúng một lần. |
| Git whitespace | PASS. |

Lệnh và log nằm trong commands.txt/evidence; log chỉ được bỏ khoảng trắng cuối dòng để qua whitespace check. Không chạy lại toàn bộ e2e. Các gate có -short có thể bỏ qua test tự khai báo short; không diễn giải thành full-suite PASS.

Lượt thử TOAST đầu bị dừng chủ động do nhầm count3 với test vốn có3 iteration (thành9 thay vì3), không tính là kết quả kiểm thử. Hai container của lượt đó được dọn bằng ID đã ghi trong log. Lượt cuối đặt rõ3 iteration/count1 như commands.txt. Những container còn lại do test bị ngắt/timeout được đối chiếu session hash theo PID/thời điểm khởi động testcontainers trước khi dọn; evidence/owned-fixtures.json ghi ownership. Không dọn container benchmark/soak có sẵn.

Hai nhánh nối tiếp nên gộp nhánh hardening sau commit sửa bổ sung sẽ đưa cả nhánh profiling vào main, giữ nguyên hai nhánh để tra cứu.

## Giới hạn cần giữ khi vận hành

- Không có chứng minh nguyên nhân giới hạn280–300TPS hoặc lỗi chỉ có trên máy người dùng; PG smoke cũ chỉ kiểm chức năng.
- Spool prefetch cũ chưa checkpoint và thiếu provenance bị từ chối; không tự xóa nếu log đã purge. Seed/sync cần cùng phiên bản STX4.
- Source UNIQUE NOT NULL được hỗ trợ sau M01. Source keyless prefetch vẫn cần archive sidecar xác minh hoặc seed không prefetch rồi CDC trực tiếp khi log còn đủ. Metadata catalog hiện tại không chứng minh lịch sử schema.
- Archive sidecar yêu cầu FULL named TABLE_MAP, QUERY ngoài transaction control bị từ chối. Equivalent expression indexes chưa chứng minh được vẫn bị từ chối.
- Chưa chạy matrix PG trước/sau15, gh-ost thật hoặc toàn bộ e2e repository. Bounded crash test cũ không phải kill ngay trong active reread. Các kết quả TOAST hữu hạn không chứng minh race hiếm đã bị loại trừ hoàn toàn.
- Diagnostic snapshot thay thế an toàn destination link; không khẳng định bảo vệ trước attacker thay ancestor directory đồng thời.
