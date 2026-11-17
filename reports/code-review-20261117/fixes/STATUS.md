# Kết quả xử lý report — 17/11/2026

Phạm vi: working tree trên commit report `5273009`, giữ nguyên và tích hợp các bản sửa chưa commit từ lượt 16–17/11. **Đã triển khai bản sửa cho 17 phát hiện riêng biệt của lượt review thứ hai/keyless**; S02 và NK05 là cùng một lỗi. Đây là trạng thái triển khai và kiểm chứng bên dưới, không phải chứng nhận toàn bộ sản phẩm hết lỗi. Chưa commit/push/deploy các thay đổi mã sản phẩm trong lượt này.

## Phân công và dùng model

- Agent gốc GPT-6.1 Terra / high: fencing worker và compactor, tính bền checkpoint khi kết thúc snapshot, archive fail-closed, quota spill; người điều phối kiểm tra biên module và kiểm chứng DB/E2E.
- Agent gốc GPT-6.1 Sol / medium: giám sát keyless, metadata/validation/kiểu dữ liệu, regression và kiểm tra chéo bản sửa.
- Agent gốc GPT-6 Luna / medium: giám sát Web/config/governor và chạy kiểm tra phù hợp.
- `agy` Opus/Sonnet bắt đầu một số bản sửa; khi Claude hết quota (429) đã ngừng sử dụng theo yêu cầu. Opus để lại bản sửa NK01/NK05; các phần được agent gốc kiểm tra và bổ sung test.
- `agy` **Gemini 3.8 Flash** được dùng tiếp. Task metadata rộng đọc quá nhiều mà chưa sửa nên đã dừng và chia nhỏ; task NK06 hẹp tạo bản sửa verifier/CLI được giữ lại sau kiểm tra, dù CLI hết print timeout. Các lượt không tạo thay đổi hữu ích đã dừng, phần quan trọng hoàn tất bằng agent gốc. Một đánh giá Flash chỉ dựa trên mô tả đưa ra rủi ro không được bằng chứng hỗ trợ, nên không nhận làm kết luận.
- Không có số đo token/chi phí đáng tin cậy để khẳng định phần trăm tiết kiệm. Cách giảm lãng phí thực tế: context riêng, quyền sở hữu file rõ ràng, task ngắn, chỉ đọc report/hàm liên quan, dừng lượt đọc lan man, chỉ chạy lại kiểm tra khi code thay đổi hoặc có khoảng trống cụ thể.

## Trạng thái các phát hiện

| ID | Bản sửa đã triển khai | Bằng chứng chính |
|---|---|---|
| S01 | Watermark hoàn tất cả bảng rỗng/plan finished; chờ ngoài barrier; dùng bounds guard đã lưu trước registration; drain + checkpoint bền phủ watermark trước Done; giữ/làm lại nghĩa vụ reread | Regression MVCC/LSN, lỗi checkpoint, drain, restart guard; TOAST strict E2E 3/3 |
| S02 / NK05 | Định danh `(tableoid,ctid)` và bắt UPDATE/DELETE tác động đúng 1 row | Fake ApplyBatch và PostgreSQL thật với CTID trùng giữa partition |
| S03 | Tạo worker trong transaction giữ khóa chia sẻ và kiểm epoch pipeline trước worker; compactor không bỏ kiểm epoch khi no-work | Regression thứ tự khóa/rollback và DB thật cho worker tạo sau takeover |
| S04 | Từ chối partial JSON trước commit, kể cả bên trong compressed payload; kiểm event không decode/không hỗ trợ | Synthetic binlog qua parser → archive reader → assembler; không có transaction bị xác nhận |
| S05 | Không đoán metadata/PK; từ chối thiếu tên/PK, bitmap bỏ cột, TABLE_MAP sai hoặc row image sai | Synthetic binlog và decoded compressed-event tests |
| S06 | Mask scalar theo vị trí YAML node, hỗ trợ compact JSON/flow YAML | Regression compact/flow, whitespace, alias fail-closed |
| S07 | Quét password keyword DSN có quote, escape và khoảng trắng | Regression các kiểu quote/escape và chuỗi chưa đóng |
| S08 | Không lưu placeholder không khôi phục được | Round-trip và unmatched/reordered compact rejection |
| S09 | Giữ nguyên string env override; chỉ trim giá trị cần parse | Config regression whitespace/password |
| S10 | Truyền lỗi/mất counter DB signals; fallback replica syntax có kiểm soát | Fake DB failure cases và package governor có MySQL thật |
| S11 | Quota tính file spill một lần theo path, bền qua reopen/overflow/truncate; không bỏ charge khi file còn trên disk; migrate state cũ | Filesystem + bucket giả; cache segment migration tránh GET/decode lặp |
| NK01 | So row image có phân biệt kiểu/NULL bằng DeepEqual, gồm nil slice khác empty slice | Assembler → ApplyBatch hai engine, strict checkpoint, cả hai chiều NULL/literal |
| NK02 | Giữ thứ tự logical identity đã resolve khi Relation có cùng tập cột; vẫn nhận key membership/DDL thay đổi | Regression index `(b,a)` với cột vật lý `(a,b)` |
| NK03 | Đòi unique identity đúng tập cột; chặn MySQL unique trên tập con; không từ chối schema mirrored có unique khác hợp lệ | Actual CheckTargetKey với fake SQL, bao gồm PRIMARY(id)+UNIQUE(email) |
| NK04 | Scan nullable COLUMN_NAME; loại toàn bộ functional index khỏi candidate, giữ dependency cần thiết | NULL-first/NULL-last và unique fallback còn hợp lệ |
| NK06 | Chỉ match khi nguồn ổn định; retry có giới hạn; CLI trả verify_incomplete nếu chưa kết luận được | Interleaving fake SQL, có/không ApplyWaiter |
| NK07 | Từ chối equality citext, xuyên domain và array ở nguồn/đích | Regression gate và PostgreSQL thật: citext[], domain/array lồng nhau; safe arrays vẫn được nhận |

## Kiểm chứng

- [TOAST strict E2E](toast-strict-e2e.log): `SEED=20261117`, 3 lượt, khoảng 607 giây tổng. Mỗi lượt có reread thật, trạng thái cuối giống nguồn, TOAST không NULL, phiên bản không giảm/không ghi trùng. Đây không phải chuỗi 15 lượt hay bằng chứng flaky bằng 0.
- [PostgreSQL writer](postgres-integration.log): keyless partition UPDATE/DELETE và stale-worker creation sau takeover đạt.
- [PostgreSQL citext/domain/array](citext-postgres.log): gate nguồn/đích và validator thực thi trên DB thật đạt.
- [MySQL fencing](mysql-fencing.log): kiểm worker epoch hiện tại và worker chưa tồn tại sau takeover trên MySQL thật.
- [Race regression](race-regression.log): 8 package có test khớp pattern đạt; chỉ các regression được chọn, không phải full race suite.
- [Archive race regression](archive-regression.log), [snapshot/pipeline regression](snapshot-regression.log), [writer race sau kiểm tra typed-nil/empty](writer-race-final.log).
- Build `./cmd/strand ./cmd/replay` và `go vet ./...`: xem [build.log](build.log), [vet.log](vet.log); file log rỗng khi thành công.
- Agent surface đã chạy đạt full config/governor/typemap và masking tests chọn lọc. Governor có container MySQL riêng. Chưa chạy full repository suite.

## Giới hạn cần giữ rõ

1. Archive prefetch hiện **từ chối** bảng keyless/unique-only nếu không xác minh được PK từ archive, kể cả bảng ngoài filter gặp trên đường đọc. Đây là fail-closed để không đánh dấu covered với dữ liệu đoán; chưa bổ sung schema resolver lịch sử cho archive. Nhánh CDC keyless trực tiếp vẫn là nhánh riêng.
2. Web từ chối block/multiline secret hoặc alias không mask chắc chắn. Compact JSON đổi thứ tự section chứa placeholder hiện fail-closed; cần nhập lại secret hoặc giữ cấu trúc. Không tráo mật khẩu giữa source/target.
3. Validation target chưa so toàn bộ unique constraint nguồn/đích khi chúng không liên quan identity; cần metadata nguồn bổ sung để chứng minh target-only constraint khác có tương thích. Không suy ra mọi extra unique đều nguy hiểm rồi từ chối schema hợp lệ.
4. Migration spool cũ chỉ tìm được sidecar còn reference và file trong thư mục spill chuẩn. File không reference nằm ngoài cây đó không có đường dẫn để tự khám phá. Payload còn trên disk tiếp tục chiếm quota cho tới khi owner dọn; spool không xóa file đang được mượn.
5. Chưa có server-generated archive fixture, compressed binary wire fixture, live MySQL functional-index fixture hoặc DB fixture cho reversed replica-identity order. Có regression logic tương ứng; không đánh đồng với kiểm chứng DB.
6. Các vấn đề gốc chưa có nguyên nhân chắc chắn — PG strict 280–300 TPS, browser CSRF/tab jump, MinIO 503 còn lại, máy MariaDB breaker và phần nguyên nhân sâu của p2p spike — không tự được tuyên bố đã giải quyết qua các bản sửa này. Chưa profile throughput mới. Giữ bằng chứng và phân biệt rõ với lỗi code đã có regression.
7. Kiểm tra chéo S01 đã rút lại nghi ngờ replay trong range đã flush: barrier của mỗi chunk drain trước flush; vị trí row event đã bền trong global checkpoint hoặc worker delta và được LoadApplied loại khi restart. Crash trước save plan giữ chunk pending để clear/reread. Đường rỗng/no-work thiếu barrier này là phần được bản sửa completion bổ sung.

Chi tiết triển khai/kiểm tra từng nhóm: [fencing](fencing-notes.md), [snapshot](snapshot-notes.md), [archive](archive-notes.md), [spool](spool-notes.md), [keyless](keyless-notes.md), [surface](surface-notes.md). Bản sửa R01–R13 từ report đầu vẫn trong working tree; lượt này không tự nâng mọi vấn đề gốc thành “đã hết lỗi”.

Tài liệu vận hành `docs/USAGE.md` đã cập nhật exact unique identity, cặp physical row ở partition, citext/domain/array, verify_incomplete, quota payload và điều kiện đọc archive.
