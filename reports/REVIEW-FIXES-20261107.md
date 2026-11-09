# Sửa các phát hiện review local — 2026-11-07

## Phạm vi

Clone riêng `/home/hellc/strand-review-fixes`, nhánh `agent/review-fixes-20261107`, nền tích hợp local `751e5ca869f9e3e70ef13d2c3d9e360be3ca4c4a` (hơn main 181 commit). Không sửa worktree người khác, không sửa spec, không merge/push main. Nhánh benchmark sửa riêng tại `/home/hellc/strand-bench-review`, HEAD `872db7d` (gồm `c254d59`) trên `agent/review-bench-fix-20261107`; chưa tích hợp công cụ benchmark vào nhánh production này.

Báo cáo review đầu vào: `/tmp/strand-review-codex/local-all-2026-11-07/REVIEW.md`. Bằng chứng chạy: `/tmp/strand-fix-evidence-20261107/`. Báo cáo chốt sau khi hoàn tất bộ kiểm định dưới đây. Các phần chưa kiểm được ghi riêng; dữ liệu máy đọc nằm trong `reports/REVIEW-FIXES-20261107-results.json`.

## Thiết kế và lập luận đúng đắn

| Review | Sửa | Bất biến và giới hạn |
|---|---|---|
| R01 / F11 | Helper E2E lấy WAL insert LSN trước marker; chỉ trả khi durable LWD bao phủ cutoff và marker hiện diện | Marker riêng không là prefix trong relaxed. Không đổi reader/writer/checkpoint production; không dùng kết quả helper cũ để kết luận CP-1 hỏng. |
| R02 | Fence trên một session quản trị đặt writable trước reload, chờ session quan sát reload, khóa đăng nhập role ứng dụng rồi ngắt/chờ backend thoát; loại session quản trị khỏi pool ở mọi đường thoát | PR-1: không xác nhận fence khi mới gửi SIGHUP; không làm ALTER ROLE trong read-only; không rò SET qua pool. Không khai báo appRoles vẫn chỉ là fence hợp tác, không chống client tự tắt read-only. |
| R03 | Khi bind list lớn, staging toàn bộ keep keys và nếu cần scope keys trong bảng TEMP; INSERT chia lô theo giới hạn tham số, index/statistics cho join; DELETE anti-join và upsert cùng transaction hiện có | RS-1: không xóa row sống để cascade nhầm FK; SG-1: dùng transaction có epoch fence; rollback bỏ cả TEMP, DELETE và upsert. CP/CO/SD và watermark không đổi. Chunk nhỏ giữ đường SQL cũ. |
| R04 | Đọc bảng cha qua pg_constraint.confrelid và namespace; rewrite FK theo ID catalog, không suy schema từ pretty-print | Schema bootstrap giữ đúng bảng tham chiếu khi search_path cho phép tên cha không qualify; rename áp trên ID đúng. |
| R05 | Lỗi encode khóa đánh dấu tracking incomplete cho từng consumer; delta trả verify_incomplete; post-seed không xóa VerifyPending khi tracking incomplete | Không đổi CDC apply/CP/CO. Thiếu bằng chứng verify không được thành trạng thái synced. Overflow post-seed nay cũng fail rõ thay vì kiểm mẫu rồi báo pass. |
| R07 | Admission giữ ngân sách cho tất cả Txn chưa Done, tối đa MaxHoles+1 ở relaxed | Khi bất kỳ Txn nào Done nâng high, tối đa MaxHoles Txn còn thiếu tạo holes: CP-4. Không đổi cạnh writeset (CO-1), không lưu seq (CP-3). Strict giữ đường commit tuần tự. Cận này bảo thủ hơn và có thể giảm lượng in-flight khi backlog lớn. |

R06 benchmark: xem `reports/REVIEW-BENCH-FIX-20261107.md` trên nhánh benchmark. Chỉ sửa kiểm chứng dữ liệu; chưa đo hiệu năng hoặc đổi governor mặc định.

Các file code/test chính: `tests/e2e/{pg_harness_test.go,review_f11_barrier_test.go,review_pg_wide_test.go}`; `internal/promote/{pg_driver.go,pg_driver_test.go,pg_fence_hard_test.go,review_fence_test.go}`; `internal/pipeline/{generated.go,review_pg_params_test.go,verify.go,review_encode_test.go,touched_consumers_test.go}`; `internal/snapshot/keysql_pg.go`; `internal/schemagen/{pg.go,review_fk_test.go}`; `internal/sched/{scheduler.go,scheduler_test.go,review_holes_test.go}`. Chỉ gofmt hai file nền chưa chuẩn: `internal/writer/pg.go`, `internal/pipeline/f11_combined_sim_test.go`.

## Regression: bằng chứng đỏ trên code cũ

Đã tạm thay các file production bằng nội dung từ 751e5ca, giữ test mới, chạy tuần tự rồi khôi phục trong finally. Log `revert-regressions.log`:

- F11: `barrier returned while A was still missing; marker alone is not a prefix`.
- Fence: `25006: cannot execute ALTER ROLE in a read-only transaction`.
- Snapshot range và keys: `extended protocol limited to 65535 parameters` trên PG thật, PK 14 cột × 5.000 row.
- FK: sinh `REFERENCES "child_schema"."customers"` thay vì target_schema.customers; xác nhận bằng catalog PG thật.
- Tracking: `delta falsely complete after encoding failure`.
- CP-4: `third admission must wait for hole budget: <nil>`.

Không coi mọi test được regex chọn là regression mới; `TestReview43_DescendingSequencesPromote` là test có sẵn tình cờ khớp regex. Benchmark có log revert logic riêng; đã ghi rõ một lượt revert nguyên file chỉ lỗi compile do thiếu trường mới và không được tính là bằng chứng hành vi.

## Lỗi fixture mới trong quá trình kiểm định

Test có sẵn `internal/governor/governor_test.go` dò MySQL tại IP compose cố định 172.23.0.5, không timeout, và có thể báo AC1 PASS dù DB không kết nối được. Lượt non-E2E mắc ở test đó; đã xác nhận PID/cwd rồi ngắt riêng binary `governor.test` PID 1450532 do lượt chạy này tạo, không ngắt tiến trình E2E của người khác. Lượt core đầu vì vậy có package governor FAIL do interrupt, không được ghi là một lượt full green.

Sửa fixture dùng container MySQL testcontainers riêng (`owned_mysql_test.go`), timeout khởi tạo/kết nối/query rõ, fail khi DB không sẵn sàng, cleanup chỉ container vừa tạo. Không sửa governor production và không dùng t.Skip. Package governor đã chạy lại đầy đủ: 27 test/subtest qua trong 22,317 giây. Đây là lỗi test mới tìm được khi thực thi, không phải lỗi đã chứng minh trong datapath. Log interrupt và rerun được giữ cạnh log core.

## Lỗi pool marker mới được test toàn package phát hiện

Lượt core đầu có lỗi thật `TestPR1_AC1_IndependentSequenceCache1000`: `source still writable after fence`. Đường `PGTargetDriver.fenceMarker` đặt default_transaction_read_only=off rồi `conn.Close()` trả session writable về pool nguồn. Sửa loại connection khỏi pool trên mọi đường thoát, giữ nguyên vị trí và nội dung marker; PR-1 không còn bị phá qua phiên dùng lại. Không đổi CP/RS/CO.

Đã chạy test AC1 qua sau sửa, rồi tạm revert riêng cleanup marker để xác nhận đỏ và khôi phục. Giữ `promote-marker-revert.log` và `promote-marker-green.json`. Toàn package promote đã chạy lại: 34 test/subtest qua trong 47,204 giây; không che lỗi lượt đầu bằng lần chạy xanh sau.

## Cô lập fixture replay có DDL

Trước khi core đến package replay, rà fixture phát hiện `tests/replay/replay_test.go` tự dò localhost hoặc docker inspect strand-dev-* rồi DROP gen_orders. Thay bằng hai MySQL testcontainers riêng; fixture gán MYSQL_SRC_DSN/MYSQL_DST_DSN tới Unix socket riêng không tồn tại để test đỏ nếu đường dùng DSN ngoài fixture quay lại. Không truy cập container dev. Thêm native AIO OFF cho fixture audit/watermark cũ theo yêu cầu môi trường. Không thay ngưỡng gate hoặc giảm số vòng.

## Fixture MinIO không độc lập

Lượt core đầu có thêm hai lỗi môi trường: `TestPF4_SpoolOverflowToBucket_MinIO` và `TestMinIOBackend_Direct` đều connection refused tại localhost:9000. Sửa test dùng `internal/testutil.MinIOEndpoint`, tạo MinIO riêng, chờ health, cleanup riêng. Không sửa spool/seed production hoặc đổi dung lượng/điều kiện test. Giữ log lỗi đầu và chạy lại đầy đủ các package seed/spool.

## Fixture sample verify vượt giới hạn CTE

`TestSampleKeysDB_MySQLToPGCompositeInt` lỗi Error 3636 tại bước INSERT fixture: 5.000 vòng vượt cte_max_recursion_depth mặc định 1.000. Thay bằng tích chữ số sinh đúng 5.000 row, không hạ số row và không SET session/global. Đây là lỗi fixture, chưa chạy tới logic verify; giữ log đỏ và chạy lại test/toàn package.

## Ghi chú lượt core đầu

Lượt core đầu không xanh: ngoài các lỗi nêu trên, replay build bị lỗi import vì fixture được sửa trong khi lệnh Go đã nạp đồ thị dependency cũ. Không có replay DDL nào được chạy ở lượt lỗi build đó. Package replay đã chạy lại bằng lệnh mới trên source cố định: qua trong 51,652 giây. Không tính lỗi build này là lỗi datapath, cũng không giấu khỏi báo cáo.

## Kiểm định

Kết quả đã có: `go build ./...`, `go vet ./...` qua, `gofmt -l internal cmd tests tools` rỗng trên code commit `5455606`. Non-E2E cuối: **910 test/subtest pass, 0 fail, 0 test skip**; 30 package có test pass, 4 package không có test. Đây là kết quả tổng hợp lượt core đầu và các package được chạy lại đầy đủ sau sửa, không phải khẳng định lệnh core đầu đã xanh. Nguồn: `core-final-summary.json`.

Race chọn lọc scheduler/tracking/F11 barrier và fencing: **8 test/subtest pass**, 0 lỗi (`race-final.json`, `promote-race.json`). Benchmark trên nhánh riêng: **20 test/subtest pass, 20 với race**; build/vet toàn repo nhánh benchmark và gofmt đều qua; không đo tải governor.

E2E chọn lọc: **24/24 pass**, 0 fail, 0 skip, ba seed lịch sử ở mỗi chế độ. Mỗi lượt chạy đủ bốn ca: PG snapshot add F11, PK tổng quát dưới tải, resume snapshot sau kill -9 và PK rộng 14 cột × 5.000 row bằng binary thật. Ca PK rộng kiểm audit để bảo đảm đã flush đủ 5.000 row trong một transaction; unit còn kiểm FK cascade và rollback/retry. Không dùng t.Skip, không hạ kích thước fixture để biến test lỗi thành pass.

| Chế độ | Seed | Thời gian lệnh (giây) | Kết quả |
|---|---:|---:|---|
| relaxed | 1793896467143368426 | 191.20 | 4/4 pass |
| relaxed | 1793967817576330754 | 206.87 | 4/4 pass |
| relaxed | 1793957136901575401 | 207.00 | 4/4 pass |
| strict | 1793896467143368426 | 443.45 | 4/4 pass |
| strict | 1793967817576330754 | 600.55 | 4/4 pass |
| strict | 1793957136901575401 | 541.16 | 4/4 pass |

Thời gian trên là wall-clock của lệnh, gồm build/khởi tạo và dọn container, không phải số đo hiệu năng sản phẩm.

Mô phỏng F11 30 seed cũng qua: 19.449 Txn, 384 Txn lớn spill, 6 lượt hủy, 6.858 flush; 66,86 giây (`core-all.json`).


Khi bắt đầu kiểm định có full E2E của worktree khác chạy transaction 50 triệu row. Không dừng/xóa process hoặc container đó, và không lấy kết quả của process ấy làm bằng chứng cho nhánh sửa này. Chưa đủ checklist đề nghị merge trước khi full E2E relaxed/strict trên bản tích hợp sửa được kiểm định.

## F11 chưa được phép kết luận quá mức

Đã sửa lỗi barrier có phản ví dụ xác định và regression đỏ/xanh. Các log F11 lịch sử thiếu commit LSN của transaction bị thiếu và applied set lúc helper trả về. Chưa thể chứng minh mọi F11 cũ cùng nguyên nhân, hoặc khẳng định root CDC không có lỗi khác. 24 ca E2E mới là bằng chứng bổ sung, không thay cho trace gắn XID/LSN để đóng mọi giả thuyết lịch sử.

## Vận hành và việc còn lại

- PG target cần TEMPORARY đã được nêu trong docs/USAGE; không thêm quyền mới ngoài cơ chế writer hiện có.
- Post-seed tracking incomplete trả exit 5 và giữ VerifyPending; người vận hành cần xử lý nguyên nhân trước khi tiếp tục.
- Cần full E2E hai chế độ trên bản tích hợp sau khi mọi sửa được gộp; chưa chạy benchmark/soak hay full E2E ma trận PG16/17/MySQL9.7 trong lượt sửa này; các test audit/integration có sẵn trong non-E2E không thay thế full E2E ma trận.
- Không tự push main. Các nhánh khác chưa triển khai (SSH/Web UI) không được tính là tính năng đã xong.

## Checklist và tệp bằng chứng

- Gofmt rỗng; build/vet qua; non-E2E đã kiểm hết 34 package, kể cả các package lỗi ban đầu được chạy lại đầy đủ.
- Mỗi lỗi logic review có regression đỏ trên phần code cũ và xanh sau sửa; log revert giữ nguyên trong thư mục bằng chứng.
- Snapshot/pipeline có ba lượt E2E mỗi chế độ, gồm kill/restart; các bất biến và thay đổi vận hành được giải thích ở bảng thiết kế.
- Chưa đủ điều kiện đề nghị merge: full E2E toàn repo relaxed/strict trên bản tích hợp sửa vẫn chưa chạy. Không thay spec, không merge/push main.
- Các log lỗi ban đầu và output cụ thể nằm trong `core-all.json`, `revert-regressions.log`, `promote-marker-revert-bounded-pool.log`, `replay-isolation-revert.log`; log xanh theo package là `*-rerun.json`, E2E là `e2e-{relaxed,strict}-{1,2,3}.json`.
- Bản báo cáo ngoài repo: `/tmp/strand-fix-evidence-20261107/REPORT.md`; số liệu tổng hợp: `/tmp/strand-fix-evidence-20261107/RESULTS.json`.
