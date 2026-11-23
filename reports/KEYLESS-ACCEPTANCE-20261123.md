# Nghiệm thu CDC keyless và backup

Rà trên cây làm việc tại HEAD `a64c98d`. Các số đo dùng Docker trong lab với fixture xác định. Chưa có cặp engine, schema, tải và ngân sách SLO của môi trường triển khai. Hai bản vá writer hiệu năng vẫn ở dạng ứng viên để đo lại bằng cùng điều kiện.

## Phát hiện và bản sửa

| Mức độ | Phát hiện | Hành vi sau sửa |
| --- | --- | --- |
| Cao | Trạng thái CDC có thể được đọc như kết quả kiểm đủ, kể cả khi bỏ qua keyless. [verify.go](../internal/pipeline/verify.go#L504) | Báo CDC và đối soát riêng qua CLI, API và Web. Ghi bảng, phương pháp, thời điểm và checkpoint. Khớp phạm vi hiện hành trước khi công bố kết quả cũ sau restart. |
| Cao | Full verify có thể dùng replica chưa theo kịp hoặc không thấy một bảng được yêu cầu. [verify.go](../cmd/strand/verify.go#L438) | Gate `--require-complete` dùng nguồn cấu hình, kiểm cả bảng thêm bằng `snapshot add`, kiểm lại phạm vi trước khi trả thành công và dừng khi thiếu bằng chứng. |
| Cao | Hai định danh PostgreSQL chỉ khác hoa/thường có thể khiến full verify hash cùng một cột đích hai lần và báo khớp sai. [merkle.go](../internal/verify/merkle.go#L414) | Ghép tên theo ngữ nghĩa của engine. Regression tạo cặp cột `"V"` và `"v"`, sửa một cột ở đích và kiểm nhận mismatch. |
| Cao | Binlog NOBLOB có thể làm giá trị vắng mặt trông giống NULL trong ảnh hàng và đi vào assembler. [assembler.go](../internal/assemble/assembler.go#L463) | Reader giữ thông tin ảnh thiếu và assembler từ chối transaction keyless. Test MySQL thay chế độ theo session sau khi pipeline khởi động và kiểm cả bảng có khóa, dữ liệu keyless, checkpoint. |
| Cao | UPDATE keyless có hai ảnh giống nhau có thể bỏ qua luôn việc hàng đã biến mất ở đích. [keyless.go](../internal/writer/keyless.go#L286) | Writer kiểm điều kiện full-row trước khi xác nhận UPDATE không cần ghi. Ca partition PostgreSQL kiểm rollback của câu xóa trước đó trong cùng transaction và checkpoint. |
| Trung bình | Bỏ qua giao dịch đã áp dụng sau restart có thể bỏ lại file spill do runtime sở hữu. [pipeline.go](../internal/pipeline/pipeline.go#L1285) | Dọn file runtime khi router tiêu thụ transaction. Regression kiểm riêng sidecar thuộc prefetch spool. |
| Trung bình | Scheduler giữ nhiều mục dọn dẹp cho cùng một khóa trong một giao dịch. [scheduler.go](../internal/sched/scheduler.go#L443) | Chỉ lưu một mục cho mỗi quyền sở hữu khóa theo giao dịch, giữ phụ thuộc giữa các giao dịch. |
| Trung bình | Wizard có thể suy lựa chọn mã hóa theo khóa có sẵn hoặc làm tròn lịch 91 giây thành 120 giây trong một lần sửa khác. [backup_jobs.go](../internal/web/backup_jobs.go#L519), [backups_ui.js](../internal/web/static/backups_ui.js#L134) | Backend lưu ý định bật, tắt hoặc kế thừa. Widget giữ giây nguyên khi round-trip qua trường phút. |

## Bằng chứng lab

| Phạm vi | Lệnh hoặc bài kiểm | Kết quả |
| --- | --- | --- |
| Verify sau seed và cutover | `go test ./tests/e2e -run '^(TestPostSeedVerificationScopeAtCatchUp|TestKL_E2E_PostSeedDetectsKeylessDrift)$' -count=1` | PostgreSQL chạy đủ keyed, mixed và keyless, có restart, drift, nguồn không ổn định và phạm vi bị thiếu. MySQL chạy drift cả keyless-only và mixed. |
| Commit và replay | `go test ./tests/e2e -run '^TestKL_E2E_PostgresCommitInterruption$' -count=1` | Qua bốn tổ hợp strict/relaxed và trước/sau commit. Đối soát đa tập và mốc WAL với giao dịch hỗn hợp có TOAST. |
| Không đủ ảnh hàng | `go test ./tests/e2e -run '^TestKL_E2E_MissingBeforeImageFailsClosed$' -count=1` | Qua MySQL 8.4. Transaction nguồn đã commit, pipeline từ chối ảnh thiếu và giữ checkpoint. |
| Writer và phạm vi trên API/Web | `go test ./internal/writer -run 'Test(Keyless\|NK05\|NK01)' -count=1` và `go test ./internal/web -run '^TestPipelineVerificationEvidenceReachesAPIAndPage$' -count=1` | Qua kiểm ảnh hàng, partition và trạng thái bắt kịp cùng danh sách kiểm/thiếu. |
| Backup | `go test ./internal/web -run '^TestJobEncryptionChoicePersistsAcrossPolicyAndJobEdits$' -count=1`, Chromium chạy `tools/diagnostics/browser-repro/backup-schedule.mjs` | Qua hợp đồng cập nhật job và payload UI khi sửa lịch/retention. |
| Restore | `go test -race ./internal/backup/runner -run '^TestPITR(MySQL\|Postgres\|Mongo)$' -count=1` và `TestBuiltinMethodsMySQLAndPostgres` | Các lượt lab PITR trên DB thật và restore MinIO qua khóa cũ đã chạy. Diễn tập metadata ghi ở [runbook](../docs/KEYLESS-PILOT.md). |
| P1, P2, P3, P4 | [Lookup](../docs/KEYLESS-PILOT.md), [batch và writer](../docs/KEYLESS-BATCH.md), [heap và restart](../docs/KEYLESS-MEMORY.md) | Tài liệu ghi cấu hình, số lượt, đối soát và giới hạn số đo. |

## Quyết định

**CHƯA MERGE toàn bộ thay đổi như một đợt rollout.** Gate operator chỉ duyệt cutover có kế hoạch khi full verify strict thành công trong cửa sổ dừng ghi. CLI promote và Web promote vẫn dùng các điều kiện fence và catch-up của chính luồng chuyển quyền ghi. Lab đã diễn tập thiếu WAL và chuyển timeline. Hiệu năng, RPO/RTO và hạn mức RAM/spool cần được xác nhận trên tải và storage sẽ triển khai. Các thay đổi và test hiện nằm trong workspace. [Lượt review bổ sung](REVIEW-FOLLOWUP-20261123.md)

| Phạm vi xét riêng | Quyết định review | Cơ sở |
| --- | --- | --- |
| Trạng thái verify, scope strict và hiển thị | MERGE | Ma trận PostgreSQL chạy qua CLI và admin sau restart. Web API, HTML và các action promote hiện có có regression. |
| Ảnh hàng, TOAST, commit/restart và sở hữu spill | MERGE | Transaction thiếu ảnh không được giao tới writer. Giao dịch không có hàng khớp rollback cả batch. Regression kiểm checkpoint bền và đa tập. |
| Backup encryption và bảo toàn lịch | MERGE | API giữ override, Chromium giữ payload khi sửa lịch/retention. Restore MinIO dùng lại khóa theo ID cũ sau rotation. |
| P1 PostgreSQL và P2 MySQL | CHƯA MERGE | Hai ứng viên có dữ liệu lab và test parity. Lượt đo lại P1 có TPS đối chứng burst giảm 10,1%. Lượt đo độc lập P2 có p99 keyless tại 50 TPS tăng 91,4%. |
| P3 cấu hình batch | MERGE tài liệu | Hai đợt sweep qua restart, cùng cho thấy giảm thời gian chờ batch. Cấu hình thử là `target.batch.max_wait: 2ms`. |
| P4 xóa mục cleanup trùng | MERGE | Sáu lượt quay vòng đạt ngân sách fixture và dữ liệu khớp. Heap còn sống giảm 13%, RSS trung vị tăng 4,5%. |

## Kiểm tra trước bàn giao

- `go vet ./...` và `git diff --check` qua. Bốn file JavaScript thay đổi qua `node --check`.
- Các regression được chọn chạy `go test -race -p 2` qua tám package gồm assembler, scheduler, reader MySQL và PostgreSQL, pipeline, verify, writer và Web.
- Hai patch thử nghiệm đều qua `git apply --check`. Overlay PostgreSQL qua test writer gồm định danh vật lý của partition và TOAST. Overlay MySQL qua test protocol thật gồm giới hạn 64 kết quả, hủy context, không khớp và rollback sau no-op trong cùng batch.
- Bài kiểm strict verify bổ sung đã qua. Một bảng được thêm từ state có drift làm lệnh trả mismatch. Truyền `--replica-dsn` vào gate strict bị từ chối.
- Lượt cuối `go test ./internal/backup/runner -run '^(TestBuiltinMethodsMySQLAndPostgres|TestPITR(MySQL|Postgres|Mongo))$' -count=1` qua cả bốn bài và mất khoảng 158 giây.
- Lượt E2E tổng hợp gặp trùng cổng khi Docker tạo container MySQL cho ca post-seed drift. Chạy lại riêng ca này qua cả hai subtest.
