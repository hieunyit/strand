# Review mã nguồn Strand — 16–17/11/2026

> Cập nhật kiểm tra chéo 17/11: R01 còn thiếu đường hoàn tất bảng rỗng/plan finished; masking cũng còn trường hợp compact/quoted DSN. Xem [báo cáo lượt hai](../code-review-20261117/REVIEW.md). Các bản sửa dưới đây chưa được coi là đã khép kín mọi trường hợp.

Baseline: `f830bfd8aa3d6f9118cd79aebce69245086700cc`. Các bản sửa hiện nằm trong working tree, chưa commit/deploy.

## Kết quả chính

Đã tìm và sửa 13 nhóm lỗi về tính đúng, bảo mật, lưu cấu hình và hiệu năng. Phân tích đi qua CLI/pipeline, reader/assembler, snapshot/range-state, scheduler/writer/checkpoint, seed/promote/verify, governor/spool và Web. Phạm vi kiểm chứng sâu tập trung vào các phát hiện dưới đây; không phải chứng nhận toàn bộ repository không còn lỗi.

Bằng chứng trước sửa: [repro-final.log](repro-final.log), mã test overlay trong các file `*.go.txt`. Mười test tái hiện thất bại trên baseline, gồm các trường hợp lỗi dữ liệu và bảo mật. [reproduce.py](reproduce.py) dựng baseline trong thư mục tạm rồi chạy lại, không ghi đè working tree; exit 1 là kết quả mong đợi. Test chính thức sau sửa được thêm trực tiếp vào các package liên quan.

## Các phát hiện và bản sửa

### R01 — P1: TOAST cũ ngoài biên snapshot bị apply xuống target vừa xóa

Vị trí: `internal/snapshot/range_state.go`, `toast_pk.go`, `pg_snapshot.go`, `mysql_snapshot.go`.

Router bị giữ trong barrier khi đọc MIN/MAX và ClearTarget. Các transaction đã commit ở source nhưng router chưa xử lý vẫn đến sau đó. Khóa dưới MIN hoặc trên MAX được coi là “Xong”; UPDATE có TOAST `Unchanged` bị apply trực tiếp dù row nền đã bị ClearTarget xóa.

Có bằng chứng khớp lỗi gốc người dùng: hai artifact thất bại có `app.items.min_pk = next_pk = 3`, nhưng writer báo thiếu khóa 2 và 1; một lần xảy ra 304 ms sau `snapshot start`. Xem [original-toast-evidence.json](original-toast-evidence.json).

Kịch bản xác định: source T1 đổi 1→101, T2 đổi 101→2; đọc bounds sau T2 thấy [2,100], router còn chậm tại trước T1. T1 không có ảnh đầy đủ của TOAST, target đã rỗng, writer dừng trước khi T2/snapshot có thể đưa dữ liệu về.

Sửa: ghi nhận snapshot MVCC đúng lúc đọc bounds; chỉ bỏ partial event ngoài biên khi transaction đã visible với snapshot đó. Transaction chưa visible vẫn đi qua xử lý bình thường. Snapshot MVCC và mốc WAL LSN được lưu cùng SnapshotPlan và phục hồi qua restart; LSN ngăn transaction mới bị nhận nhầm là cũ khi XID 32 bit quay vòng. Với khóa tổng quát, hai truy vấn biên dùng cùng transaction REPEATABLE READ; nạp metadata trước khi giữ connection để tránh chờ connection của chính transaction khi pool chỉ có một connection.

Kiểm chứng: test historical move, low/high outside bounds, active xid, serialize/restore plan, actual registration/resume, WAL fence sau khi XID quay vòng và fake SQL pool một connection. Chưa chạy lại chuỗi E2E ngẫu nhiên 15 lần. Plan cũ thiếu `bound_snapshot` giữ hành vi cũ; không thể tự suy ra MVCC của lần đọc biên trước đó. Không tự xóa state của pipeline đang chạy.

### R02 — P1: RereadBounds O(n²) chặn router

Vị trí: `internal/snapshot/toast_pk.go:RereadBounds`, gọi qua `PersistReread` trước Admit.

Mỗi khóa trong map lại quét toàn bộ KeySet của chunk để tìm Bound tương ứng. Nhiều khóa cùng một chunk tạo O(n²) cho một lần lấy danh sách; việc persist lặp lại còn gồm sort, serialize state và fsync file/thư mục.

Sửa: quét mỗi chunk một lần, lọc bằng map sở hữu khóa và sort kết quả. Giữ đúng khóa đã bị chunk mới thay thế, kiểu Bound và quyền sở hữu slice/byte.

Cùng microbenchmark, trên máy có tải:

| Số khóa | Trước | Sau | Tỉ lệ |
|---|---:|---:|---:|
| 500 | 2,49 ms | 0,129 ms | ~19× |
| 5.000 | 197 ms | 1,94 ms | ~101× |
| 10.000 | 539 ms | 4,93 ms | ~109× |

Bằng chứng: [trước](reread-bench.log), [sau](reread-bench-after.log). Đây là phép đo hàm, không phải phép đo thông lượng pipeline. Fsync toàn bộ state trên mỗi nghĩa vụ mới vẫn còn; hướng tối ưu tiếp là gom việc ghi bền hoặc journal, nhưng phải giữ điều kiện nghĩa vụ đọc lại bền trước khi checkpoint vượt transaction.

### R03 — P1: spill reader PG làm mất barrier schema

Vị trí: `internal/reader/pg/eventlog.go:encodeEvent/readEvent`.

EventTableMap có `SchemaChanged=true` trong RAM nhưng codec spill không lưu cờ. Sau replay, assembler không đặt DDL barrier, pipeline có thể apply schema mới trước khi drain/evolve metadata. Tác động có thể là dừng lỗi cột/kiểu hoặc bỏ qua chính sách chặn schema change.

Sửa: encode/decode cờ và từ chối giá trị cờ không hợp lệ. Test round-trip hiện có được tăng cường với TableMap đổi schema ở nhiều thời điểm spill và rollback subtransaction. Spill là file tạm của một lần reader chạy, không phải định dạng archive lâu dài.

### R04 — P1: BIT khác nhau giữa snapshot và CDC

Vị trí: `internal/typemap/typemap.go`, `internal/snapshot/mysql_snapshot.go`.

BIT(8)=49 được SQL trả về packed byte 0x31. Snapshot đổi thành string rồi converter hiểu là binary text “1”; binlog trả int64(49) rồi converter cho “110001”. Policy bytea cũng trả raw bytes ở một đường và ASCII chữ số ở đường khác. Giá trị zero/leading zero và BIT(64) high bit có thêm sai khác.

Sửa: snapshot giữ BIT dạng []byte; converter phân biệt packed bytes, integer từ decoder và explicit binary text. Chuẩn hóa theo độ rộng khai báo; bytea là big-endian packed bytes. Giữ bit pattern của int64 âm khi decoder biểu diễn BIT(64).

Kiểm chứng: widths 1/8/9/64, zero, byte giống ASCII, high bit, varbit và bytea; helper snapshot giữ đúng representation.

### R05 — P2: ENUM/SET bị cắt khoảng trắng hợp lệ

Vị trí: `internal/typemap/typemap.go`.

ENUM(' a','b') với giá trị “ a” bị TrimSpace thành “a” rồi bị từ chối. SET(' a','b') “ a,b” bị ghi thành ['a','b'], sai dữ liệu.

Sửa: giữ nguyên ký tự của giá trị string/[]byte. Regression kiểm cả hai kiểu đầu vào.

### R06 — P1: đổi thứ tự YAML làm tráo mật khẩu source/target

Vị trí: `internal/web/masktext.go:UnmaskConfigText`.

Hai dòng password sau mask giống nhau được ghép lại theo thứ tự xuất hiện, không theo cấu trúc YAML. Đưa target trước source khiến target nhận source-secret và ngược lại; validation có thể từ chối lưu hoặc dùng sai thông tin kết nối.

Sửa: xác định vị trí bằng YAML mapping path và sequence index, không phụ thuộc thứ tự section. Trường hợp không ghép chắc chắn bị từ chối thay vì đoán.

### R07 — P1: audit ghi plaintext bí mật config/env

Vị trí: `internal/web/actions.go`, `configedit.go:registerConfigEditOps`.

Các action config-validate/config-save/env-save thiếu khai báo tham số bí mật. API ghi `yaml=...` hoặc `entries={...password...}` thẳng vào audit file/DB trước khi chạy action. Env keys dạng động cũng bị lộ. Quyền admin của HTTP không ngăn việc bí mật bị nhân bản ra audit/backups hoặc kho log.

Sửa: che toàn bộ nội dung YAML và toàn bộ giá trị tham số của env-save. Test HTTP với secret canary xác nhận audit không chứa giá trị gốc.

### R08 — P2: preview diff lộ mật khẩu đã mask

Vị trí: `internal/web/configedit.go:validateConfigContent`, `tuning.go:saveTuning`.

Diff được tính sau khi unmask, nên chỉ thêm một comment gần dòng password cũng khiến secret gốc xuất hiện trong context của diff/API response.

Sửa: mask cả hai phía trước khi tính diff. Có regression canary.

### R09 — P2: lưu .env thành công nhưng không được dùng sau Refresh/restart

Vị trí: `internal/web/configedit.go`, `pipelines.go`, `meta.go`.

Nếu pipeline chưa có env_file, save chỉ gán vào object RAM; Refresh dựng object mới và làm mất đường dẫn. DB persistence cũng đọc EnvPath cũ rỗng, nên không lưu env mới.

Sửa: phát hiện file env theo đường dẫn quy ước khi chưa có đường dẫn cấu hình tường minh; lưu EnvPath mới và nội dung mã hóa vào DB. Không sửa trực tiếp object Pipeline đang được các request khác đọc. Test cả Refresh, registry mới và store.Mem.

### R10 — P2: DB persist lỗi nhưng API vẫn báo lưu thành công

Vị trí: `internal/web/configedit.go`, `meta.go`.

Sau rename file, lỗi lưu metadata chỉ được log Warn. DB là nguồn sự thật; lần materialize kế tiếp có thể ghi đè file mới bằng cấu hình cũ dù API đã báo OK.

Sửa: trả lỗi và cố gắng rollback file, serialize các lần sửa config/env bằng mutex. Regression inject lỗi UpsertPipeline xác nhận không báo thành công và phục hồi file.

Giới hạn: filesystem và DB chưa có transaction chung; rollback là best effort khi chính filesystem lỗi. External writer hoặc lifecycle materialization không tham gia mutex sửa cấu hình.

### R11 — P2: diff dùng RAM/CPU tăng theo tích số dòng

Vị trí: `internal/web/configedit.go:unifiedDiff` và handlers.

Ma trận LCS kích thước (n+1)(m+1) int: hai file 10.000 dòng cần khoảng 800 MB chỉ cho phần tử ma trận trên kiến trúc 64 bit. Các handler JSON không có giới hạn body riêng.

Sửa: giới hạn body 1 MiB; giới hạn LCS 1 triệu ô và trả thông báo bỏ diff khi vượt. Không chặn việc validate/lưu chỉ vì diff quá lớn.

### R12 — P2: login limiter giữ IP thất bại vô hạn

Vị trí: `internal/web/auth.go:LoginLimiter`.

IP chỉ bị xóa khi đăng nhập đúng; IP đã thử sai một lần rồi không quay lại vẫn tồn tại mãi. Endpoint login công khai có thể tích lũy entries dài hạn.

Sửa: thu hồi entries hết hạn và giới hạn 10.000 entries; từ chối IP mới khi đầy thay vì cấp phát vô hạn. Test đồng hồ giả xác nhận thu hồi.

### R13 — P2: tên biến env không được kiểm tra

Vị trí: `internal/web/envfile.go:formatEnvLine`.

Formatter chỉ kiểm giá trị, nhưng key có thể chứa newline, dấu '=' hoặc ký tự shell và tạo dòng .env sai/injected. Đây là endpoint đã xác thực, không phải đường bypass đăng nhập.

Sửa: tên phải khớp `[A-Za-z_][A-Za-z0-9_]*`; regression cho newline, '=', '$', chữ số đầu và identifier hợp lệ.

## Đối chiếu 9 vấn đề người dùng cung cấp

| # | Vấn đề | Kết luận hiện tại |
|---|---|---|
| 1 | G2-7 TOAST + đổi PK | Đã xác định cơ chế ngoài biên, khớp artifact lỗi gốc; sửa và unit regression đạt. Chưa lặp E2E đủ để công bố hết flaky. |
| 2 | PG strict 280–300 txn/s | Đã chứng minh và tối ưu nút thắt RereadBounds; fsync, partial-update SQL đồng bộ và COPY/temp-table round trips vẫn là ứng viên. Chưa có CPU/block profile của đúng workload để quy toàn bộ trần TPS cho một nguyên nhân. |
| 3 | MinIO 503 | Code hiện chờ /ready; chưa tái hiện lại lỗi 503 còn lại, không đổi retry/backend theo suy đoán. |
| 4 | PGKill9 | Test hiện chờ pipeline running trước lần kill đầu; khớp yêu cầu vị trí --start-at được ghi bền. Chưa có tái hiện độc lập lỗi gốc. |
| 5 | gh-ost sau 8fad122 | Đã rà cổng DDL, chưa tái hiện kịch bản mất row gốc. Lỗi spill PG R03 là lỗi riêng, không dùng để giải thích gh-ost MySQL. |
| 6 | Breaker MariaDB | Code hiện log HardReason/counter; chưa có log môi trường lỗi thực tế để chốt nguyên nhân. |
| 7 | CSRF login | Cơ chế cookie/origin hiện hữu; chưa có browser trace và audit reason của lần lỗi nên chưa xác nhận nguyên nhân thực tế. Không thêm bypass xác thực. |
| 8 | Trang nhảy sang Tables | Refresh có kiểm URL/sequence trước khi thay DOM; chưa tái hiện browser gốc. Không tuyên bố đã giải quyết bằng các sửa Web khác. |
| 9 | grace-v2#2 p2p max 1,018 | Đã tính lại từ telemetry và khớp log breaker; chi tiết dưới đây. |

## p2p 1,018: đã xác định nguyên nhân trực tiếp

Nguồn: `scratchpad/g2pg/pg/01-grace-v2-2/{run.json,telemetry.csv,strand.log}` của lượt đo có sẵn trên máy. Tính lại theo đúng cửa sổ của `tools/g2bench/analysis.go:p2pLogR` (bỏ 60 s đầu và các khoảng spike).

- R cao nhất trong cửa sổ: 400, lúc 18:40:37 ngày 16/11, giờ Việt Nam.
- Breaker mở lúc 18:42:57, log ghi query p99 latency ratio 3,48 > max_ratio 3,00.
- R thấp nhất: 144,5, lúc 18:43:07; zone Red, breaker=1, pressure≈1,218.
- `ln(400/144,5) = 1,0181850395554957`, khớp số bất thường.
- Breaker đóng lúc 18:44:06. Các cửa sổ kế cận có p2p≈0,00995.

Đợt giảm tốc khi breaker mở tạo ra maximum này. Chưa đủ dữ liệu để nói latency spike do source quá tải thật, baseline chưa phù hợp hay tranh chấp tài nguyên ngoài workload. Không xóa các mẫu breaker khỏi metric để làm Gate 2 đạt. [Dữ liệu tính lại](p2p-analysis.json).

## Kiểm tra sau sửa

- `go vet ./...` và build `cmd/strand`: đạt, [log vet](vet-final.log).
- Test tích hợp chọn lọc tại reader/pg, snapshot, typemap, web, pipeline: đạt, [log](integrated-tests.log).
- Kiểm tra bổ sung sau khi thêm WAL fence tại snapshot/pipeline: đạt, [log](bounds-final-tests.log).
- Các agent chạy thêm bộ test chuyển kiểu và các test Web liên quan; đạt.
- `git diff --check` và gofmt các file thay đổi: đạt.
- Full `go test ./...` đã khởi chạy rồi dừng vì kéo theo E2E kéo dài/container trên máy đang đo benchmark. Không dùng lượt đó làm bằng chứng full-suite đạt.
- Chưa chạy full E2E/race toàn repository, chưa đo lại thông lượng PG strict trên môi trường cô lập, chưa deploy.

Ưu tiên nghiệm thu tiếp: chạy ToastPKMove strict nhiều lần cùng kill/restart trên plan mới, rồi profile đúng workload PG strict và so trước/sau bằng cùng tải. Plan snapshot cũ không có MVCC cần được xử lý theo quy trình vận hành riêng, tránh xóa state khi pipeline đang chạy.
