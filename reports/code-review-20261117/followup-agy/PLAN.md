# Giao việc sửa/điều tra phần còn mở cho agy — 17/11/2026

Mọi phần triển khai và điều tra của lượt này giao cho **agy Gemini 3.8 Flash**. Người điều phối viết yêu cầu, đọc diff, kiểm bất biến và chạy kiểm chứng; nếu bản sửa sai thì trả lại agy bằng yêu cầu sửa cụ thể. Không dùng Claude, không giao agent gốc tự sửa thay trong lượt này.

Working tree đã có các bản sửa trước chưa commit. Bản sao 86 file pending được lưu tại `/tmp/strand-agy-followup-baseline`; không reset/stash/ghi đè các sửa trước. Không push/deploy trong lượt này. Mỗi task có file giao việc độc lập, file kết quả và log riêng; không fork lịch sử hội thoại vào agy.

## Phân việc

| Task | Model | Vấn đề |
|---|---|---|
| 01 | Flash high | Khôi phục secret theo scalar path, khắc phục compact JSON đổi thứ tự |
| 02 | Flash high | Đối chiếu toàn bộ unique constraint nguồn–đích |
| 03 | Flash high | Profiler/harness xác định trần PG strict 280–300 TPS |
| 04 | Flash high | Identity có kiểm chứng cho archive/keyless/fallback unique |
| 05 | Flash high | Kiểm kê/import charge cho spill cũ nằm ngoài thư mục chuẩn |
| 06 | Flash medium | Tái hiện MinIO 503, PGKill9 và mất dòng quanh gh-ost cut-over |
| 07 | Flash high | Browser thật: CSRF đăng nhập và trang tự nhảy tab |
| 08 | Flash medium | Thu bằng chứng breaker MariaDB và nguyên nhân sâu p2p spike |
| 09 | Flash medium | Gate TOAST nhiều lượt, kill/restart và phân loại mức nghiệm thu |

Chạy ba luồng, tuần tự trong mỗi luồng: `01→07→06`, `02→04→05`, `03→08→09`. Các luồng sở hữu file khác nhau. Task phải bắt đầu sửa/test sau một vòng đọc giới hạn; không đọc lại toàn repo. Không tự tăng agent con. Test DB dùng fixture riêng; không dừng dịch vụ benchmark/soak đang có.

## 01 — Web: bỏ việc ghép secret theo dòng

**Nguyên nhân đã biết:** `yamlLinePaths` gom nhiều scalar trên một dòng; reorder compact JSON hoặc trộn một password thật và một placeholder làm mất phép ghép một-một.

**Thiết kế giao agy:**

1. Parse YAML node của bản gốc và bản sửa; định danh mỗi scalar bằng mapping path có phân cách không nhập nhằng và sequence index. Không dùng thứ tự section hoặc số dòng làm identity.
2. Với secret scalar `********`, chỉ lấy giá trị ở đúng path tương ứng. Với DSN nhúng, bind placeholder theo path của scalar và thứ tự thành phần nhạy cảm bên trong scalar đó; số token không khớp thì từ chối.
3. Thay byte span của bản người dùng sửa; khi chuyển block↔flow/JSON phải quote giá trị đúng syntax mới, không chép nguyên raw scalar có dấu phẩy/quote vào context khác. Giữ chỉnh sửa thật của người dùng và phần comment/nonsecret.
4. Parse lại kết quả, đối chiếu semantic secret, đảm bảo không còn placeholder cần khôi phục. Alias/multiline chưa chứng minh mask an toàn tiếp tục bị từ chối.

**Nghiệm thu:** đảo source/target trong compact JSON; một password mới + một placeholder trên cùng dòng; tách/gộp dòng; sequence; DSN quote/escape; secret có dấu phẩy/quote chuyển sang flow; path lạ/alias fail-closed. Không tráo hay lộ secret, không sửa dấu sao trong comment.

Giao việc: [01-web-scalar.md](tasks/01-web-scalar.md).

## 02 — Unique constraint: so với nguồn thay vì từ chối mọi extra unique

**Nguyên nhân/giới hạn:** validator chỉ biết logical key nên không phân biệt được extra UNIQUE hợp lệ được sao từ nguồn với constraint chỉ có ở target có thể gộp mất row.

**Thiết kế:** lấy descriptor unique của cả hai DB tại lệnh validate có sẵn cả source/target; áp rename; kiểm primary và fallback. Target constraint được chấp nhận khi uniqueness của nguồn đủ để suy ra constraint đó với equality tương thích. Source unique trên tập con có thể suy ra uniqueness trên tập lớn hơn; chiều ngược lại không đúng. Nullable, prefix, expression và collation phải được xét riêng; không đồng nhất chúng chỉ vì cùng tên index. Trường hợp chưa chứng minh được cần lỗi/thông tin rõ, không tự drop constraint hoặc sửa SQL upsert để né.

**Nghiệm thu:** `PRIMARY(id)+UNIQUE(email)` mirrored vẫn qua; extra target-only email bị phát hiện; subset/superset; NULL; expression/prefix; rename; target chưa tồn tại; cả nguồn PK lẫn fallback. Giữ exact identity rule đã sửa và tránh hồi quy trên schema bình thường.

Giao việc: [02-target-constraints.md](tasks/02-target-constraints.md).

## 03 — PG strict: đo rồi mới tối ưu

**Chưa biết nguyên nhân chính**, nên không giao thay batch/fsync theo phỏng đoán.

**Thiết kế:** thêm profiling ra file theo tùy chọn của lệnh sync, không mở cổng pprof công khai. CPU/block profile phải khởi động/dừng đúng cả lỗi và cancellation; file được bảo vệ, không vô tình ghi đè; khi tắt không có overhead đáng kể. Harness cô lập phải cố định phiên bản DB/Go, schema, seed, fsync, CPU, workload, warmup và khoảng đo. Thu đồng thời read/route/apply TPS, batch/commit, CPU/block và DB waits. Phân biệt backpressure với thời gian thực sự tiêu ở reader.

**Cách sửa có điều kiện:** chỉ tối ưu hàm hiện rõ trong profile; đo trước/sau cùng cấu hình. Không giảm durability, bỏ strict/fencing/MVCC, bỏ mẫu breaker hoặc đổi metric để đạt số đẹp. Nếu không tái hiện 280–300 trên lab thì bàn giao collector và ghi nguyên nhân môi trường gốc còn mở.

Giao việc: [03-pg-profile.md](tasks/03-pg-profile.md).

## 04 — Archive: identity đúng thời điểm log, không lấy schema hiện tại để đoán

**Thiết kế:** một hợp đồng metadata bất biến/versioned đi kèm archive hoặc seed có columns/types theo thứ tự, fingerprint, key columns/order và explicit full_row. So với TABLE_MAP; DDL/version/fingerprint không khớp phải dừng trước khi transaction được tính covered. Keyless chỉ bật khi explicit và FULL images; fallback unique phải khớp identity pipeline dùng khi replay. Nếu cần thay durable format để mang đủ key metadata/before image mà chưa hoàn tất được thì giữ fail-closed, không tuyên bố đã hỗ trợ.

Scope/filter chỉ được áp khi có scope đã xác minh; transaction chứa cả bảng loại và bảng giữ vẫn phải giữ đúng boundary. Đồng thời xử lý hai đường lỗi có thể kiểm chứng: archive lỗi mà không có source fallback thì phải trả lỗi; EOF giữa transaction không được báo thành công/covered.

**Nghiệm thu:** parser→assembler fixtures cho PK, unique/keyless có metadata rõ, duplicate/NULL, schema/DDL mismatch, mixed tables trong một transaction, archive lỗi không source và partial EOF. Không có col_0 hoặc key đoán đi vào spool.

Giao việc: [04-archive-identity.md](tasks/04-archive-identity.md).

## 05 — Spill cũ ngoài cây chuẩn: explicit inventory

Không thể khôi phục đường dẫn mà state cũ chưa từng ghi, vì vậy không được tự scan toàn máy hay tự xóa file lạ.

**Thiết kế:** API/tool kiểm kê root do operator chỉ rõ, mặc định read-only; thao tác import accounting phải explicit. Chuẩn hóa path, chính sách symlink/escape, file thường, overlap/duplicate, permission/missing file. Merge vào durable index idempotent, không ghi đè live refcounts, không tính trùng path. Sau import quota tính cả file còn trên disk; không xóa file đang được mượn. Cập nhật phải atomic/resumable khi ngắt.

**Nghiệm thu:** orphan ngoài root chuẩn; nhiều root trùng; symlink thoát root; missing/permission; reopen; lỗi giữa chừng; quota sau import; không xóa payload borrowed.

Giao việc: [05-spool-inventory.md](tasks/05-spool-inventory.md).

## 06 — MinIO, PGKill9 và gh-ost

- **MinIO 503:** ghi version/digest và timeline `/ready`→tạo bucket→put/get/delete lặp; thu HTTP/S3 code, request-id và health/container logs. Chỉ thêm retry khi xác nhận lỗi transient và operation idempotent; không retry để che lỗi credential/quyền/dữ liệu.
- **PGKill9:** inject kill ở hai phía mốc lưu vị trí bắt đầu bền. Chờ milestone thật, không chỉ sleep hoặc health sống. Khởi động lại phải resume đúng hoặc từ chối rõ theo hợp đồng, không âm thầm chọn vị trí mới. Test readiness thay đổi chỉ được coi là sửa test nếu có bằng chứng.
- **gh-ost:** trì hoãn DDL/schema refresh quanh rename cut-over trong khi nguồn ghi, có witness id/version và so trạng thái cuối. Ghi rõ binary thật hay simulator. Chỉ sửa data path khi có kịch bản chứng minh mất row; thiếu binary/không tái hiện thì giữ harness và kết luận chưa xác nhận.

Giao việc: [06-runtime-repros.md](tasks/06-runtime-repros.md).

## 07 — Trình duyệt: CSRF và tự chuyển tab

Dùng Playwright/browser thật trong môi trường tạm, fixture local, không dùng tài khoản thật. Chạy trực tiếp và reverse proxy theo trust contract; cookie thiếu/cũ/hết hạn; retry và nhiều tab. Thu reason, origin, path, cookie attributes nhưng che token/password/cookie value. Cross-origin/forged Origin vẫn phải bị chặn.

Đối với nhảy tab: chủ động trì hoãn phản hồi refresh rồi chuyển tab/back/forward; đưa phản hồi cũ về sau phản hồi mới, kiểm URL, active tab, DOM và scroll. Chỉ nhận response còn thuộc navigation hiện tại. Có trace/screenshot khi lỗi, fixture lặp lại được. Không chữa bằng hardcode một tab hoặc bỏ CSRF.

Giao việc: [07-browser-repros.md](tasks/07-browser-repros.md).

## 08 — Breaker và p2p

p2p `ln(400/144.5)=1.018...` do breaker giảm tốc đã được chứng minh; phần chưa rõ là nguyên nhân latency spike.

Collector chỉ đọc và chỉ kết nối khi operator chỉ rõ DSN. Thu version, timestamp/counter delta `Innodb_log_waits`, buffer-pool waits, commit/fsync, safe variables, DB waits và CPU/IO; đồng bộ với baseline/p99/R/reason. Xử lý counter reset, dữ liệu thiếu và timezone. Không ghi query text hoặc credential; file output được bảo vệ.

Có thể tái hiện giao dịch lớn/redo pressure trong MariaDB lab để kiểm cơ chế, nhưng không coi đó là nguyên nhân trên máy người dùng. Analyzer giữ mẫu breaker và metric gốc. Chưa có capture đúng lúc lỗi thì nguyên nhân máy gốc tiếp tục mở.

Giao việc: [08-breaker-spike.md](tasks/08-breaker-spike.md).

## 09 — Nghiệm thu TOAST và crash/restart

Runner tách rõ unit/build/vet/race, E2E chọn lọc và full suite; lưu tree hash, phiên bản, seed, output và exit code. Có chế độ 15 lượt strict và kill/restart trên plan mới; chạy tuần tự, cleanup đúng fixture của mình, giữ artifact khi lỗi. Chạy trước một crash/resume có ý nghĩa; stress/full suite chỉ ghi đạt khi thực sự chạy xong. Không lấy 3 lượt trước làm bằng chứng cho 15 lượt hay full repository.

Giao việc: [09-toast-crash-gate.md](tasks/09-toast-crash-gate.md).

## Tiêu chí review của người điều phối

1. So diff với baseline pending, đảm bảo không mất bản sửa trước hoặc có file ngoài scope.
2. Kiểm các bất biến dữ liệu/checkpoint/epoch, quyền truy cập và secret handling; phản ví dụ phải trở thành regression có ý nghĩa.
3. Bản sửa do agy tạo phải compile và test đúng nhánh; không nhận regex chạy 0 test là bằng chứng.
4. Test fixture không được nới assertion để qua; lỗi gốc, lab reproduction, lỗi test và tính năng còn thiếu được báo riêng.
5. Nếu review phát hiện sai, giao lại agy kèm file/điều kiện/assertion cần sửa. Người điều phối không tự vá mã sản phẩm trong lượt này.
6. Kết quả mỗi task và log nằm trong `results/`; trạng thái chưa có kết quả không được coi là đã sửa.
