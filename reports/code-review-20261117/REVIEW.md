# Kiểm tra chéo theo model — 17/11/2026

> Cập nhật xử lý: xem [kết quả sửa và kiểm chứng](fixes/STATUS.md). Nội dung bên dưới là phát hiện tại thời điểm review trước sửa.

Đối tượng: working tree đang chứa các bản sửa từ lượt 16–17/11. Lượt này **không sửa mã sản phẩm**; chỉ bổ sung báo cáo và test overlay ngoài source. Các phát hiện dưới đây còn mở.

## Phân công để giảm token

| Vai trò | Model / reasoning | Phạm vi |
|---|---|---|
| CDC và snapshot | GPT-6.1 Terra / high | reader, assembler, snapshot, model, rowkey, typemap; đặc biệt các bất biến dữ liệu và kiểm tra chéo bản sửa |
| Độ bền dữ liệu | GPT-6.1 Sol / medium | writer, checkpoint, scheduler, spool, seed, promote, verify, objects |
| Bề mặt vận hành | GPT-6 Luna / medium | CLI, shell, deployment, schema; build và syntax check |
| Điều phối | Model chính | biên giữa module, Web/bảo mật, config/governor và kiểm chứng lại kết luận |

Agent dùng context riêng (`fork_turns=none`), chỉ được giao đường dẫn và bất biến liên quan. Không fork toàn bộ hội thoại dài. Agent không sửa cùng file và chỉ trả vị trí/điều kiện/bằng chứng, thay vì chép source vào báo cáo. Không tự tạo agent con.

Model nhẹ phù hợp kiểm tra có tiêu chí rõ như đường dẫn, flag, shell và build. Model mạnh tập trung vào ordering, checkpoint, MVCC và crash/restart. Bản tổng hợp cuối kiểm tra lại tác động và độ chắc chắn; kết quả “không tìm thấy lỗi” của agent không được coi là chứng minh an toàn.

Không đo chi phí/token thực tế của từng model trong lượt này, nên không đưa ra phần trăm tiết kiệm hoặc giá tiền giả định.

## Bổ sung kiểm tra bảng không có khóa chính

Xem [NO-PRIMARY-KEY.md](NO-PRIMARY-KEY.md): 6 phát hiện mới NK01–NK04, NK06–NK07 và kiểm tra lại S02 (không đếm trùng). Có bằng chứng UPDATE NULL bị bỏ qua nhưng checkpoint vẫn commit, thứ tự composite key PostgreSQL bị đổi và validation chấp nhận unique đích không tương thích.

## Phát hiện mới còn mở

### S01 — P1: gỡ bộ lọc TOAST trước khi router theo kịp với bảng rỗng

- Vị trí: `internal/snapshot/mysql_snapshot.go` completion; `internal/pipeline/pipeline.go:2077–2083`.
- Khi bounds mới là rỗng, walk không có chunk nên không gọi rendezvous. `snapshotTable` đánh dấu Done rồi RemoveTable, làm mất BoundSnap vừa bổ sung trong bản sửa R01 trước đó.
- Event partial UPDATE cũ, visible với bounds snapshot, bị Discard khi manager còn đăng ký nhưng trở thành Apply sau RemoveTable. Target vừa ClearTarget không còn row nền, nên writer có thể trả ToastRowMissingError.
- Overlay tái hiện ba đường: new-empty, resume-empty, resume-finished. Đường resume-finished còn phụ thuộc checkpoint replay lag; new-empty là điều kiện đủ độc lập.
- Mức kiểm chứng: control flow/routing xác định, chưa chạy PostgreSQL thật. Đây là thiếu sót của bản sửa trước, không phải kết luận R01 đã hết flaky.
- Hướng sửa: có mốc catch-up/rendezvous cho cả đường không đọc chunk, trước khi bỏ range-state; test cả unregister và restart.

### S02 — P1: keyless PostgreSQL có thể xóa/sửa row ở partition khác

- Vị trí: `internal/writer/keyless.go:188–194`, kiểm RowsAffected ở 265.
- SQL nhận diện physical row chỉ bằng `ctid`. CTID có thể trùng giữa các child relation, nên subquery chọn một row ở partition A nhưng outer DELETE/UPDATE có thể khớp row khác ở partition B cùng CTID.
- Writer chỉ từ chối zero affected rows; nhiều row vẫn được coi thành công và checkpoint.
- Target partitioned/inherited không bị pipeline keyless check từ chối. Điều kiện: keyless full_row, bảng target có child relations.
- Mức kiểm chứng: generated SQL và lập luận physical-row identity; có `evidence/storage-keyless_partition_repro.sql` nhưng **chưa chạy SQL trên DB thật**.
- Hướng sửa: nhận diện `(tableoid, ctid)` và kiểm đúng một row, hoặc từ chối rõ target chưa hỗ trợ.

### S03 — P1: relaxed writer cũ có thể tự tạo worker row sau takeover

- Vị trí: `internal/writer/writer.go:294–304`; tương tự `internal/writer/pg.go:170–176`; `internal/checkpoint/*_store.go:AdvanceEpoch`.
- AdvanceEpoch chỉ tăng epoch các worker row đã tồn tại. EnsureWorkerRow cho INSERT mới bằng epoch caller mà không kiểm pipeline epoch.
- Nếu advisory connection mất giữa lúc khởi tạo worker, instance mới takeover và tăng epoch; pool instance cũ reconnect rồi tạo worker ID chưa có với epoch cũ. ApplyBatch relaxed chỉ kiểm row đó nên có thể commit.
- Một lần kill kết nối cũ lúc takeover không ngăn pool mở connection mới. Startup compaction có đường no-work trả về mà không kiểm global epoch.
- Mức kiểm chứng: fake SQL state chạy các hàm EnsureWorkerRow → Compact → ApplyBatch thật, quan sát `pipelineEpoch=2, workerEpoch=1, dataWrites=1, committed=true`. Không phải thử takeover đa tiến trình với DB thật.
- Strict global checkpoint vẫn có epoch check nên chặn kịch bản này.
- Hướng sửa: tạo worker trong transaction kiểm/khóa pipeline epoch; không cho worker row mới trở thành giấy phép ghi độc lập với epoch hiện tại.

### S04 — P1: archive bỏ partial JSON update nhưng có thể đánh dấu GTID đã phủ

- Vị trí: switch event ở `internal/reader/archive/reader.go:121–315`; luồng covered ở `cmd/strand/prefetch.go:129–138`.
- Archive không có nhánh PARTIAL_UPDATE_ROWS_EVENT, nhưng vẫn phát XID sau đó. Transaction update trở thành rỗng rồi có thể vào spool/covered.
- Fallback source bắt đầu từ tập covered, nên không tự lấy lại update đã bỏ. Live MySQL reader có nhánh từ chối event này, archive không có bảo vệ tương ứng.
- Điều kiện: archive chứa PARTIAL_JSON update và được dùng seed/prefetch.
- Mức kiểm chứng: trace code path, **chưa chạy real binlog**.
- Hướng sửa: decode đầy đủ hoặc fail-closed trước khi commit transaction/covered; thêm fixture binlog đúng loại event.

### S05 — P1: archive metadata MINIMAL tạo tên cột/khóa đoán trong spool

- Vị trí: `internal/reader/archive/reader.go:183–203`; `cmd/strand/prefetch.go:91`; fallback ở `internal/assemble/schema.go`.
- Archive TABLE_MAP không có column names sẽ phát Columns/PKColumns rỗng. Prefetch dùng assembler không có source schema registry, nên fallback col_0, col_1 và khóa cột đầu.
- `--source` ở đường archive hiện dùng nạp collation, không bổ sung TableInfo/schema history như live reader.
- Row change sai metadata được spool và GTID được tính covered; replay có thể dừng vì cột không tồn tại hoặc dùng sai key.
- Điều kiện: archive metadata MINIMAL/MySQL cũ, qua prefetch.
- Mức kiểm chứng: trace code path, chưa chạy fixture binlog/database thật.
- Hướng sửa: bổ sung metadata có kiểm chứng theo vị trí log, hoặc từ chối thiếu metadata; không đoán khóa rồi xác nhận covered.

### S06 — P2: compact JSON/YAML vẫn lộ password qua MaskConfigText

- Vị trí: `internal/web/masktext.go:15,67–87`; Web config view gọi tại `views.go:438`.
- YAML hợp lệ `{"source":{"password":"compact-canary"}}` không có khoảng trắng sau dấu ':'. Regex bắt buộc khoảng trắng nên trả nguyên secret; mask dùng trong preview/diff cũng bị ảnh hưởng.
- Đã tái hiện bằng safety assertion thất bại. Đây là API đã xác thực, không phải bypass đăng nhập.
- Hướng sửa: xác định scalar bí mật bằng YAML node/token ranges, hoặc fail-closed với cú pháp không mask chắc chắn.

### S07 — P2: password trong PostgreSQL keyword DSN có quote không bị che

- Vị trí: `internal/web/mask.go:22–30`.
- `host=localhost user=u password='quoted canary' dbname=d` được MaskSecrets trả lại password nguyên văn; regex chỉ nhận value không bắt đầu bằng quote.
- Tác động tới log/job output và DSN trong config. Safety assertion tái hiện được.
- Hướng sửa: nhận đúng quoting/escaping của keyword DSN; không chỉ sửa riêng một ví dụ có dấu cách.

### S08 — P2: placeholder compact có thể được lưu thành mật khẩu thật

- Vị trí: `internal/web/masktext.go:UnmaskConfigText/hasMaskedSecret`.
- Với syntax compact của S06, original secret không được nhận diện. Gửi lại password='********' ở JSON compact không restore được nhưng cũng không bị từ chối; hàm trả thành công với placeholder còn nguyên.
- Safety assertion tái hiện được. Cần giải quyết cùng parser/masking của S06, không chỉ che giao diện.

### S09 — P2: env override cắt khoảng trắng của mật khẩu

- Vị trí: `internal/config/env.go:157–168`.
- `STRAND_SOURCE_PASSWORD=" canary "` bị TrimSpace thành `canary` trước khi gán string. Mật khẩu có khoảng trắng đầu/cuối là giá trị khác, có thể gây lỗi đăng nhập khó đối chiếu.
- Safety assertion với ApplyEnvOverrides thất bại; không cần DB.
- Hướng sửa: trim ở parser numeric/bool/duration; giữ nguyên các giá trị string nhạy cảm.

### S10 — P2: mất toàn bộ DB signals MySQL vẫn được báo là thành công

- Vị trí: `internal/governor/signals.go:188–240`.
- Lỗi SHOW GLOBAL STATUS bị nuốt; cả SHOW REPLICA STATUS và fallback SHOW SLAVE STATUS lỗi cũng trả `signals,nil`.
- GRACE không nhận error nên không chạy nhánh blindTick; metric thiếu bị đối xử như áp suất thấp/zero. Tình trạng query permission thiếu hoặc đường quan sát DB lỗi có thể làm governor tiếp tục/tăng tốc theo tín hiệu không tồn tại.
- Fake SQL driver cho mọi query lỗi, ReadSignals vẫn nil error: đã tái hiện.
- Hướng sửa: phân biệt primary không có replica status với query thất bại; truyền lỗi thiếu tín hiệu cần thiết về governor, có kiểm tra phiên bản cho fallback.

### S11 — P2: spool hard limit bỏ qua payload file spill

- Vị trí: `internal/spool/pebble.go:343–382` và codec SpilledPath.
- Quota chỉ tính encoded metadata của transaction khi payload nằm ở file spill, không tính sidecar được giữ bền cho prefetch.
- Repro thật trên filesystem: spill 262.182 byte, total hard limit 65.536 byte, Append thành công, Usage chỉ báo 200 byte.
- Nhiều transaction lớn có thể làm đầy disk dù quota spool chưa báo đầy.
- Hướng sửa: tính đủ payload bền ở mọi tầng và ownership/lifetime; tránh tính trùng hoặc xóa sidecar còn được mượn.

## Loại khỏi danh sách lỗi xác nhận

- Promote FTWRL re-fence nhưng giữ bước catch-up/setval completed: có test chứng minh control flow, nhưng tác động cần ghi vào source khi promote vắng mặt. Đây là caveat vận hành FTWRL đã được code/tài liệu cảnh báo. Giữ như mục hardening có điều kiện, không gộp với P1 trên.
- Spool thiếu PG XID: production prefetch hiện chỉ mở cho source MySQL; chưa chứng minh đường PG reachable, nên không kết luận là bug.
- Các lỗi chưa tái hiện như CSRF trình duyệt, trang nhảy tab, MinIO 503 còn lại không tự được xem là đã sửa qua lượt audit này.

## Bằng chứng và kiểm tra

- [Chạy lại](reproduce.py) dùng Go overlay, không sửa source. Các safety assertion root/storage được kỳ vọng **fail trên code hiện tại**. Test completion core là test quan sát và **pass khi tái hiện đúng đường xử lý sai**; pass đó không có nghĩa bản sửa an toàn.
- [Repro tổng hợp](evidence/combined-repro.log), [root](evidence/root-repro.log), [governor](evidence/root-governor-repro.log), [storage](evidence/storage-repro.log).
- Build strand/replay và shell syntax check đạt. Các test hiện hữu chọn lọc được agent chạy và đạt.
- Không chạy DB/Docker/E2E/full-suite/race để tránh nhiễu benchmark đang chạy. SQL keyless, archive event fixtures và takeover process thật là các khoảng trống kiểm chứng quan trọng.

## Phạm vi và cách tiếp tục hiệu quả

[coverage.csv](coverage.csv) lập danh mục **274 file triển khai và vận hành** trong phạm vi đã chọn (Go production, SQL, shell, YAML/JSON, Web assets, Dockerfile/Makefile; không bao gồm mọi test/docs).

Trong **riêng lượt này**: 31 file được ghi nhận đọc đầy đủ, 67 file kiểm các hàm/đường dữ liệu liên quan, 176 file mới ở mức danh mục/search/static-check. Đây là phạm vi đọc của review, **không phải test coverage**. Phân công một file cho agent hoặc build thành công không có nghĩa file đó đã được review sâu. Lượt trước có các kiểm tra bổ sung, nhưng không tự cộng thành “100% đã sạch”.

Bản chi tiết của từng agent nằm trong `evidence/*-coverage.json`. Manifest còn ghi các nơi chưa đọc sâu, như nhiều seed planners, object SQL conversion/discovery, bucket HTTP, applied-set algebra và một số lệnh CLI/tooling. Không thể khẳng định đã review sâu toàn repository từ lượt này.

Để tiết kiệm token mà vẫn kiểm soát phạm vi toàn bộ:

1. Giữ manifest làm danh sách công việc chung; mỗi lượt chỉ giao các mục chưa kiểm hoặc đã thay đổi.
2. Chạy build/vet/syntax và tìm các mẫu nguy hiểm trên toàn phạm vi; agent chỉ đọc hàm liên quan và caller/callee cần thiết để kiểm bất biến.
3. Dành model mạnh cho đường có thể mất dữ liệu/ghi sai/checkpoint sai; model nhẹ cho tác vụ xác định và module ít rủi ro.
4. Mỗi lỗi chỉ cần vị trí, điều kiện, hậu quả, bằng chứng và test mục tiêu. Agent không gửi dump source và không lặp lại báo cáo cũ.
5. Chỉ chạy test đắt sau khi có giả thuyết xác định và fixture riêng; lúc nghiệm thu phải chạy E2E/race phù hợp trên môi trường cô lập.
6. Người điều phối loại các lỗi không reachable, tách giả thuyết khỏi tái hiện và kiểm tra phần nối module. Không giảm token bằng cách bỏ qua bước này hoặc tuyên bố an toàn dựa trên số file đã liệt kê.

Ưu tiên xử lý trước: S01/S02/S03 và archive S04/S05; sau đó khép kín masking S06–S08, quota và đường mất metrics. Chưa thay đổi thêm mã sản phẩm trong lượt đánh giá này.
