# Review đồng bộ bảng không có khóa chính — 17/11/2026

Bổ sung cho [báo cáo chính](REVIEW.md). Phạm vi là working tree hiện tại, gồm các bản sửa chưa commit từ lượt trước. Lượt này chỉ review và bổ sung bằng chứng, **chưa sửa mã sản phẩm**.

## Phân công theo model

| Agent | Model / reasoning | Trách nhiệm |
|---|---|---|
| nokey_reader | GPT-6.1 Terra / high | Chọn unique thay PK, metadata reader, assembler, thứ tự khóa và validation |
| nokey_writer | GPT-6.1 Sol / high | UPDATE/DELETE full-row, NULL, duplicate, atomic checkpoint, partition |
| nokey_verify | GPT-6 Luna / medium | Verify, CLI, hợp đồng cấu hình và khoảng trống test |
| Điều phối | Model chính | Kiểm tra chéo tác động, chạy test chọn lọc, lưu bằng chứng và tổng hợp |

Context riêng, không fork hội thoại dài; phân module tránh đọc lặp; không chạy benchmark/E2E nặng. Không đo token thực tế nên không khẳng định tỷ lệ tiết kiệm.

## Phát hiện còn mở

### NK01 — P1: UPDATE keyless NULL ↔ chuỗi "<nil>" bị bỏ qua nhưng checkpoint vẫn tiến

- Vị trí: `internal/writer/writer.go:194–204`, `internal/writer/keyless.go:39–43,129,239`.
- `keysDiffer` dùng `fmt.Sprint` để so giá trị. NULL và chuỗi literal `"<nil>"` có cùng biểu diễn; writer kết luận ảnh trước/sau không đổi và bỏ UPDATE.
- Assembler vẫn giữ đúng OldKey/Key khác nhau. Writer sau đó tăng rows_written và commit checkpoint, khiến restart không tự lấy lại update bị bỏ qua.
- Tác động: cả writer MySQL và PostgreSQL ở chế độ full_row; không cần concurrency hay lỗi mạng.
- **Đã tái hiện** qua assembler và ApplyBatch thật với fake transaction: MySQL cả hai chiều và PostgreSQL NULL→literal đều chỉ ghi checkpoint; control NULL→"ordinary" có UPDATE. Đây là test logic/transaction giả, chưa phải DB E2E.
- Hướng sửa: so giá trị có phân biệt kiểu/NULL, thống nhất với so sánh row image của assembler; regression cả hai chiều và cả hai engine.
- Bằng chứng: [writer-null.log](evidence/no-primary-key/writer-null.log), [test](evidence/no-primary-key/writer-null_test.go.txt).

### NK02 — P1: PostgreSQL replica-identity unique ghép có thứ tự khác thứ tự cột bảng bị đảo khóa

- Vị trí: `internal/rowkey/pg.go:28–38`; `internal/reader/pg/stream.go:580–593`; `internal/assemble/assembler.go:441–449`.
- Ví dụ bảng có cột vật lý `(a,b)`, REPLICA IDENTITY USING INDEX trên unique `(b,a)`. Pipeline/writer đăng ký key theo index `[b,a]`; Relation event phát các cột có cờ key theo thứ tự vật lý `[a,b]`, assembler ghi đè registry bằng thứ tự này.
- Row `(a=1,b=2)` chuyển từ key `[2,1]` sang `[1,2]` trong khi writer vẫn dùng tên khóa `[b,a]`. DELETE/UPDATE có thể không tìm thấy hoặc tác động row khác có giá trị đảo ngược.
- **Đã tái hiện** việc đổi thứ tự bằng tableMapEvent/SchemaRegistry thật qua overlay; chưa chạy DELETE/UPDATE với PostgreSQL thật.
- Hướng sửa: bảo toàn thứ tự logical key đã resolve, hoặc chuẩn hóa đồng thời mọi consumer khi nhận Relation; thêm integration với `(b,a)` và hai row giá trị đảo nhau.
- Bằng chứng: [reader-order.log](evidence/no-primary-key/reader-order.log), [test](evidence/no-primary-key/reader-order_test.go.txt).

### NK03 — P1: chấp nhận unique đích trên tập con khóa nguồn, có thể gộp mất row

- Vị trí: `internal/validate/rowkey.go:74–101` và đường CheckTargetKey khoảng dòng 181.
- MySQL cho phép `uniqueIndexCovers(..., exactSet=false)` chấp nhận target UNIQUE(a) với source fallback UNIQUE(a,b).
- Source cho phép `(1,1)` và `(1,2)` cùng tồn tại; target UNIQUE(a) không thể chứa cả hai. ON DUPLICATE KEY UPDATE có thể cập nhật đè row trước thay vì giữ đủ hai row.
- **Đã tái hiện** validation trả accepted=true bằng helper thật. Tác động ghi dựa trên SQL upsert đang dùng; chưa chạy DB fixture mất row.
- Hướng sửa: yêu cầu khóa đích tương thích đầy đủ với row identity nguồn; kiểm cả unique khác trên đích có thể xung đột. Một unique chặt hơn không chứng minh dữ liệu nguồn biểu diễn được trên đích.
- Bằng chứng: [target-subset.log](evidence/no-primary-key/target-subset.log), [test](evidence/no-primary-key/target-subset_test.go.txt).

### NK04 — P2: functional unique index MySQL làm đọc metadata lỗi trước khi chọn fallback/full_row

- Vị trí: `internal/pipeline/schema.go:278–284`; đối chiếu `internal/rowkey/mysql.go` dùng sql.NullString cho tên cột expression.
- INFORMATION_SCHEMA.STATISTICS trả COLUMN_NAME=NULL cho functional index, nhưng pipeline scan vào string và trả lỗi ngay.
- Bảng không PK có index expression và có một unique fallback hợp lệ khác vẫn có thể không khởi động; bật full_row cũng không đi qua được bước metadata để phân loại.
- **Bằng chứng code path**, chưa chạy MySQL 8 fixture. Không coi đây là lỗi ghi mất dữ liệu; đây là lỗi từ chối cấu hình hợp lệ.
- Hướng sửa: xử lý NULL tên cột, loại nguyên expression index khỏi candidate thay vì lỗi toàn bảng; test cả có/không có unique fallback khác.

### NK05 — P1: keyless target PostgreSQL partition/inheritance dùng ctid thiếu tableoid

Đây là **S02 đã có trong báo cáo chính**, không đếm thêm một lỗi mới. `internal/writer/keyless.go:188–194` chỉ dùng ctid để chọn physical row; CTID có thể trùng giữa child tables. Outer UPDATE/DELETE có thể sửa nhiều row, trong khi kiểm affected rows chỉ từ chối 0. Cần `(tableoid,ctid)` hoặc từ chối loại target này, đồng thời bảo đảm affected rows đúng 1. Có SQL reproduction tại [evidence](evidence/storage-keyless_partition_repro.sql), chưa chạy DB thật.

### NK06 — P2: full verify có thể báo match khi nguồn đã đổi trong lượt kiểm tra

- Vị trí: `internal/verify/keyless.go:109–116`; `cmd/strand/verify.go:203–204`.
- Trình tự S1 hash source → mark/wait → target vẫn S1 → source hash S2 khác S1: verifier trả ngay khi Match=true mặc dù StableOnSource=false, thay vì retry như comment mô tả.
- CLI chỉ in/kiểm Match, không cảnh báo StableOnSource=false; có thể trả thành công khi target mới bằng trạng thái nguồn cũ. Không khẳng định mất dữ liệu bền vững: có thể chỉ là lag khi đang ghi.
- **Bằng chứng control flow**, chưa có test interleaving DB. Hướng sửa: chỉ kết luận match khi nguồn ổn định, retry có giới hạn rồi trả trạng thái không kết luận được; CLI cần hiển thị trạng thái đó.

### NK07 — P1: citext được chấp nhận nhưng full-row predicate có thể chọn sai row

- Vị trí: `internal/rowkey/keyless.go:36–43`, `internal/pipeline/keyerr.go` checkKeylessTargetCollations, `internal/writer/keyless.go:188–194`.
- Gate chấp nhận kiểu citext và chỉ kiểm nondeterministic collation. Tuy nhiên citext so sánh không phân biệt hoa/thường ngay cả khi dùng collation deterministic; đây là hành vi được [PostgreSQL xác nhận](https://www.postgresql.org/docs/current/citext.html).
- Bảng keyless một cột citext có thể giữ cả 'alice' và 'ALICE'. Source DELETE với điều kiện `v::text='ALICE'` phát đúng ảnh trước 'ALICE', nhưng target full-row predicate IS NOT DISTINCT FROM có thể chọn 'alice' và LIMIT 1 không bảo đảm đúng spelling.
- **Đã tái hiện gate chấp nhận bằng overlay**, chưa chạy PostgreSQL DELETE thực tế. SQL semantics và source code cho thấy nguy cơ chọn sai row; có [SQL reproduction](evidence/no-primary-key/citext-repro.sql), [gate log](evidence/no-primary-key/citext-gate.log), [test](evidence/no-primary-key/citext-gate_test.go.txt).
- Hướng sửa: từ chối citext/kiểu equality không bảo toàn ảnh byte, hoặc dùng phép so byte phù hợp có kiểm chứng cho full-row. Chỉ kiểm collation chưa đủ.

## Hợp đồng và hiệu năng cần giữ rõ

- Không có PK nhưng có unique đủ điều kiện là nhánh fallback key; không đồng nghĩa với full_row. PostgreSQL cần REPLICA IDENTITY USING INDEX phù hợp; MySQL unique nullable/prefix/expression không làm row identity an toàn.
- Bảng thực sự keyless cần `tables.keyless=full_row`, row image đầy đủ/PG REPLICA IDENTITY FULL và seed hoặc start position phù hợp. Logical snapshot keyless hiện bị từ chối theo thiết kế, không tính là bug.
- Duplicate được giữ bằng INSERT thường; UPDATE/DELETE phải tiêu thụ đúng một ảnh trước với NULL-safe equality. MySQL còn có điều kiện byte-exact cho chuỗi. Những cơ chế này không loại trừ NK01/NK05.
- Scheduler dùng writeset cố định theo bảng keyless để giữ thứ tự, kể cả relaxed. Nhiều worker không giúp tăng song song trong cùng bảng keyless; đây là ràng buộc đúng đắn dữ liệu.
- Full-row UPDATE/DELETE có thể scan toàn bảng cho từng row nếu thiếu index hỗ trợ: batch lớn có thể gần O(số event × số row target). Tạo non-unique index phù hợp các cột lọc chọn lọc, kiểm EXPLAIN trên dữ liệu thật; không tự thêm unique làm mất khả năng giữ duplicate. Chưa profile/benchmark nên chưa định lượng mức cải thiện.
- Verify keyless full hiện dùng count + tổng checksum không phụ thuộc thứ tự; không định vị từng row hay repair. Đây là kiểm tra xác suất, không phải chứng minh hai multiset bằng nhau tuyệt đối. CLI hiện chưa kiểm StableOnSource đúng mức (NK06).

## Kiểm chứng và giới hạn

Test chọn lọc `^Test(Keyless|UniqueIndex|KeylessColumn)` cho rowkey, assemble, writer, verify, validate chạy đạt; rowkey/verify không có test khớp regex trong lượt chạy này. Test hiện có đạt không bao phủ các counterexample mới. Các overlay có thể chạy lại bằng `python3 reproduce-no-primary-key.py`; NK01/NK07 cố ý có safety assertions thất bại trên mã còn lỗi, NK02/NK03 là observational tests đạt và in trạng thái sai.

Chưa chạy DB/Docker/E2E hoặc benchmark trong lượt này. Không kết luận đã kiểm chứng toàn bộ kiểu dữ liệu, mọi collation, generated column, TOAST, partition và crash/restart. Các case ưu tiên bổ sung: literal giống biểu diễn NULL, composite RI đổi thứ tự, unique đích chặt hơn nguồn, functional index, CTID trùng giữa partition, duplicate có NULL/TOAST qua restart.

Phạm vi đọc chéo: rowkey/mysql, pg, keyless; reader/mysql tableinfo/TABLE_MAP/schemahist; reader/pg relation/tuple; assemble schema/row enrichment; pipeline metadata/keyless gates/registration/conversion; writer keyless và checkpoint; model writesets/scheduler admission; verify keyless/canonical helpers và CLI full; docs §5.1.3. Đây là phạm vi kiểm tra source, không phải coverage runtime.
