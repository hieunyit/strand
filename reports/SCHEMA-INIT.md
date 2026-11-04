# Việc B — target schema init (đang kiểm tra)

Nhánh `agent/schema-init`, base `27c3e1f`, clone `/home/hellc/strand-codex`. **NOT DONE**: chờ owner chốt giới hạn FK vòng/tự tham chiếu, chưa full e2e toàn repo trên nhánh và chưa merge/push. Code chính `efb3b1b`, sửa sequence/default `cdd7f6c`.

## Thiết kế và file

- `internal/schemagen/{plan,mysql,pg}.go`: lập kế hoạch trước khi ghi, bỏ qua bảng đã có; MySQL dùng SHOW CREATE TABLE và mapping định danh, PostgreSQL dùng catalog, MySQL→PG dùng `typemap` mặc định. PK/index/FK, identity/sequence, generated PG và partition một cấp được kiểm bằng DB thật.
- `cmd/strand/schema.go`, `root.go`, `validate.go`: `schema init [--dry-run]`, cùng planner dùng trong validate; đếm bảng thiếu, `off` từ chối validate. `internal/typemap/validate.go` kiểm tên đích sau rename và danh sách đã mở rộng glob/exclude; giữ API cũ.
- `internal/pipeline/{schema_init,pipeline,ddl}.go`: tạo chỉ cho pending trước snapshot; CREATE MySQL trong stream đi qua drain + metadata lock đọc + kiểm binlog tới head đã chụp. Không biết schema tại vị trí CREATE thì exit 10, không ghi checkpoint cho DDL.
- `internal/writer/pg.go`: OVERRIDING SYSTEM VALUE cho identity ALWAYS, dùng cả ở upsert snapshot. Regression riêng cho INSERT, COPY+MERGE và snapshot range.
- Test config, generator/DB, thứ tự snapshot và e2e nằm trong các file `*schema*test.go` mới. Test DDL cũ giữ nguyên assertion và đặt `create_schema: off` tường minh để tiếp tục kiểm hợp đồng áp schema thủ công.
- `docs/USAGE.md`: thêm phần schema-init đã đối chiếu code/CLI; việc D viết lại toàn tài liệu chưa làm. `questions/OPEN-schema-init-fk.md`: giới hạn FK có vòng, chưa tự tắt FK/trigger.

## Lập luận đúng đắn (02 §2, §4, §5, §6, §8, §10, §12; quyết định 17)

- CP-1..5: tạo schema không thay tập applied hoặc seq. Dữ liệu/checkpoint vẫn commit nguyên tử trong writer cũ; override identity chỉ giữ đúng ID source. Lỗi DDL không đánh dấu DDL đã apply. Không sao chép engine MySQL phi InnoDB.
- RS-1/2: schema có trước đăng ký/đọc chunk; không đổi low/high/rendezvous, PendingDiscards hay XID visibility. Với PG, cha trước con; barrier trước flush con đã drain event cha tới high. Không tắt FK.
- SD-1..4: không tạo bảng rỗng cho Done (seed/restore/start-at), không thay P, schema backup, log coverage hay kiểm engine/version.
- SG-1: bootstrap chạy sau khi giữ lock/epoch; CREATE streaming giữ routeMu và drain worker. Schema-init tường minh chỉ tạo đối tượng còn thiếu, không ghi row/checkpoint.
- CO-1/2: không thay scheduler/writeset/budget. CREATE phải xong trước khi metadata bảng mới tham gia router. Không copy trigger/event/routine để tránh tác dụng ghi thêm ngoài Txn source.
- MySQL SET SESSION dùng một connection riêng và khôi phục đúng giá trị cũ bằng context cleanup; nếu khôi phục lỗi, loại connection khỏi pool và trả lỗi.
- PG áp toàn kế hoạch DDL trong một transaction. MySQL auto-commit từng bảng; kiểm ca tạo nửa kế hoạch rồi lập lại, không ALTER bảng đã có.

## Bằng chứng đã chạy (chưa phải kết quả cuối)

Log gốc tại `/home/hellc/strand-codex-evidence/` và các file `/home/hellc/strand-codex-*.log`.

| Kiểm tra | Kết quả thực |
|---|---|
| Integration generator v4, MySQL 8.4 + PG17 | Qua, 29,639s; 5 subtest DB (3 hướng, kiểu không hỗ trợ, session/atomicity), 4 unit DDL; MySQL→PG 6 bảng, PG→PG 6 bảng |
| Config + pipeline | Qua: 0,081s + 0,486s, log `config-pipeline.log` |
| Identity trên writer cũ, trước sửa | Hai subtest lỗi 428C9, `strand-codex-identity-red.log` |
| Identity sau sửa | Hai subtest qua; 6,065s, `strand-codex-identity-green.log` |
| Tạm revert riêng writer rồi chạy regression | Exit 1, `identity-revert-red.log`; đã khôi phục bản sửa |
| E2E fixture cuối relaxed lần đầu | 3/3 hướng qua, 104,37s (go test 114,847s), SEED=2026110401, `schema-e2e-relaxed-final-1.log` |
| Full verify của e2e trên | Mọi bảng được đối chiếu: 0 mismatched chunks / 0 mismatches; CREATE streaming hai hướng MySQL, restart và CREATE→ALTER tương lai được kiểm |
| CLI help / JSON schema | Đã chạy, `schema-init-help.txt`, `config-schema.json` |
| Build/vet trong khi phát triển | Qua; cần chạy lại trên code cuối |

Đã đủ ma trận targeted cho core `efb3b1b`: relaxed 2, 3, 4 và strict 1, 2, 3, tổng **18/18 ca theo hướng qua**, tổng thời gian test 305,27s relaxed và 310,77s strict. Không tính lượt relaxed 1 vào bộ ba vì guard `off` được chốt sau khi lượt đó bắt đầu. JSON dẫn tới log gốc: `SCHEMA-INIT-results.json`. Bản sửa bổ sung `cdd7f6c` chỉ ở generator, được chạy lại riêng toàn package; chưa có full e2e toàn repo trên bản này.

## Các lần lỗi, không che bằng rerun

1. Helper testcontainers không biên dịch: `cannot use port (variable of type string) as nat.Port`. Sửa helper dùng `nat.Port`.
2. Integration PG assertion quét numeric vào int: `converting driver.Value type string ("10.000") to a int: invalid syntax`. Sửa SELECT cast cho assertion; không đổi giá trị kỳ vọng.
3. E2E PG thiếu fixture marker: `relation "app.marker" does not exist (SQLSTATE 42P01)`. Bổ sung bảng mà helper catch-up yêu cầu.
4. FK thật, SEED=1793791411320457054 (log `strand-codex-schema-fk-red.log`): `snapshot app.children ... violates foreign key constraint "children_pid_fkey" (SQLSTATE 23503)`. Sửa thứ tự cha trước con; unit và e2e giữ regression.
5. E2E tải ban đầu, SEED=1793791475869843793: MySQL→MySQL qua; MySQL→PG và PG→PG hết timeout catch-up. MySQL→PG còn thiếu `3755-8291`. PG vẫn tiến: tổng batch 2174→2879 trong 38s. Source 100 update/s một hot key, target max_wait 50ms áp khoảng 18,5 batch/s. Fixture cuối 10 update/s, vẫn kiểm commit chồng lên snapshot, không đổi invariant/assertion kết quả.

## Giới hạn và việc chưa kiểm

- FK vòng/tự tham chiếu: logical bootstrap auto target PG từ chối sớm; câu hỏi mở cần owner review.
- Enum/domain PG và subpartition: cần tạo bảng thủ công; lỗi rõ trước Apply. MySQL→PG từ chối generated, index prefix/expression/fulltext và AUTO_INCREMENT→numeric.
- Database metadata trong DSN target cần tồn tại/kết nối được; tự tạo chỉ các schema/bảng dữ liệu. Runtime CREATE tự động hiện là đường source MySQL; source PG dùng snapshot add/restart.
- Chưa có kiểm tự động quyền tối thiểu mới trên tài khoản không phải admin; chưa thử timeout/cancel đúng lúc trả session MySQL.
- Đã kiểm 29 package có test + 4 package không có test trên core; generator được chạy lại sau sửa bổ sung. Chưa full e2e relaxed/strict toàn repo; chưa kiểm đầy đủ kill -9 giữa tạo schema bằng process (integration đã mô phỏng partial commit MySQL và rollback PG).
- C–F, Gate 2 còn thiếu và staging 1TB chưa làm; không dùng số liệu việc B để tuyên bố các gate đó đạt.

## Sửa bổ sung sau review: sequence/default PG

Code giữ `pg_sequence.seqtypid` thay vì lấy kiểu cột sở hữu. Giữ nguyên biểu thức default (ví dụ `nextval(seq)*2`), chỉ đổi literal regclass sang sequence đích; không đổi literal thường hay toán tử. Không chạm pipeline/snapshot/writer hoặc các bất biến commit/checkpoint.

- Code cũ: `sequence-type-red.log`, lỗi 22023 do MAXVALUE bigint với AS integer.
- Sau sửa kiểu nhưng trước sửa default: `sequence-default-red.log`, đọc ID=9 thay vì 18.
- Sau cả hai sửa: toàn package generator qua, 29,511s, log `sequence-default-green.log` trong worktree kiểm chứng.
- Tạm revert từng phần: `sequence-type-revert-red.log` và `sequence-default-revert-red.log`, đều exit 1 đúng lỗi tương ứng; đã khôi phục.
- Lượt toàn repo trên `efb3b1b`: 29 package có test qua, 4 package không có test, 0 lỗi (`unit-final.log`). Bản sửa bổ sung đã chạy lại toàn package bị ảnh hưởng trên nhánh chính của việc B, qua trong 29,248s (`schemagen-final.log`); build/vet mới nhất qua, gofmt rỗng. Chưa có full e2e toàn repo cho nhánh.

E2E DDL cũ với `create_schema: off` đã qua trên `cdd7f6c`: relaxed 62,22s (12.303 row / 5 bảng giống hệt), strict 62,47s (11.472 row / 5 bảng giống hệt). Log `legacy-ddl-relaxed.log`, `legacy-ddl-strict.log`.

Chưa chốt thiết kế/đề nghị merge: cần owner chọn chính sách FK vòng trong `questions/OPEN-schema-init-fk.md` theo quy tắc hỏi trước khi lệch spec lớn; sau đó mới chốt kiểm tra full e2e trên thiết kế cuối. C–F giữ nguyên thứ tự và chưa bắt đầu.
