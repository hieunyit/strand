# Rà soát và nghiệm thu bổ sung Strand

Cây làm việc dùng HEAD `a64c98d` cùng các thay đổi đã có. Phạm vi rà gồm compile, vet và quét phụ thuộc ở cấp repository, chạy test có race detector và đọc các luồng lưu checkpoint, khôi phục, archive, verify và cấu hình vận hành.

| Mức độ | Vấn đề đã sửa | Kiểm hồi quy |
| --- | --- | --- |
| Cao | Restore có thể lặp vô hạn với chuỗi incremental tham chiếu vòng. Plan rỗng làm entry point restore panic. Chuỗi có log làm parent bị từ chối. [pitr.go](../internal/backup/runner/pitr.go#L178), [restore.go](../internal/backup/runner/restore.go#L64) | `TestPlanRestoreRejectsInvalidParentChain`, `TestPlanRestoreRefusesIncompleteRestorePoints`. |
| Cao | Restore có thể áp dụng riêng payload cuối của chuỗi incremental cũ khi metadata gắn một phương thức restore tích hợp. CLI, API và wizard hướng về `strand backup fetch` và công cụ gốc. [pitr.go](../internal/backup/runner/pitr.go#L179), [backups_pro.go](../internal/web/backups_pro.go#L373) | `TestPlanRestoreRejectsInvalidParentChain`, `TestRestoreLegacyIncrementalRequiresNativeTool`. |
| Cao | PostgreSQL PITR chạy được qua CLI nhưng Web yêu cầu một cửa sổ thời gian liên tục đã xác minh. PostgreSQL chỉ chứng minh thời điểm sau replay. Web dùng cùng planner với CLI và wizard hiện trạng thái chờ replay. [backups_pro.go](../internal/web/backups_pro.go#L348), [backups_restore.js](../internal/web/static/backups_restore.js#L245) | `TestPostgresRestoreOffersReplayProofWithoutContinuousWindow`, Chromium 1.280 và 390 px, các test Web restore hiện có. |
| Cao | `Archiver.Executed()` đọc GTID map đang được cập nhật, race detector tái hiện trên MySQL thật. Observer đọc GTID dạng chuỗi được công bố sau khi đồng bộ state. [archiver.go](../internal/reader/archive/archiver.go#L128) | Các bài restart và purged-gap trong `internal/reader/archive` với `-race`. |
| Cao | Archive có thể chọn bản sao log vừa tạo làm cursor resume, lấy mốc của segment cũ hoặc lineage khác. Ba archiver lấy segment gốc mới nhất. WAL xác minh lineage trước khi giữ slot. [logs.go](../internal/backup/runner/logs.go#L25), [wal.go](../internal/backup/runner/wal.go#L74) | `TestArchiveCursorSkipsNewerCopies`, PostgreSQL/MinIO thiếu WAL và đổi timeline thật. |
| Trung bình | Backup mới thiếu log có thể che một base cũ còn đủ log. Chế độ tự chọn thử base đủ điều kiện theo thứ tự mới nhất, lựa chọn theo ID giữ nguyên. Bản sao log không tạo gap giả. [pitr.go](../internal/backup/runner/pitr.go#L207), [verify.go](../internal/backup/runner/verify.go#L143) | `TestMySQLRecoveryWindowKeepsStandaloneSnapshotsOutside`. |
| Trung bình | Verify backup bị hủy có thể trả success dù chưa kiểm xong. [verify.go](../internal/backup/runner/verify.go#L87) | `TestVerifyJobReportsCancellation`. |
| Trung bình | State verify sau seed có thể được dùng lại sau khi sửa schema lúc pipeline dừng và tên bảng không đổi. Fingerprint dùng cấu hình, cột, kiểu, khóa và metadata schema đã đọc. Bằng chứng cũ thiếu fingerprint mới hiển thị là chưa kiểm sau nâng cấp. [verify.go](../internal/pipeline/verify.go#L659) | `TestVerificationEvidenceInvalidatedByOfflineSchemaChange` với `-race` và ma trận restart trên PostgreSQL. |
| Trung bình | Test helper có thể bỏ lại container nếu khởi động trả về đồng thời container và lỗi. SDK Docker test cũ xuất hiện trong 14 cảnh báo reachable của lượt quét toàn repository. Đã bổ sung cleanup và nâng Testcontainers lên 0.45.0. [harness_test.go](../tests/e2e/harness_test.go#L154), [go.mod](../go.mod) | Compile các package, kiểm lại container thật và chạy lại govulncheck trên snapshot Go vulnerability DB. |

## Các việc còn mở từ đợt trước

- PostgreSQL P1 thêm chín cặp đối chứng. TPS burst có khóa giảm 10,1%, vượt ngưỡng 5%. Bản vá lookup giữ dạng thử nghiệm. [Số đo P1](../docs/KEYLESS-PILOT.md)
- MySQL P2 đo cả writer và CDC. Lượt CDC lặp lại độc lập ba cặp khớp đa tập, p99 keyless burst giảm 30,22 → 23,32 ms và p99 keyless tại 50 TPS tăng 21,94 → 42,00 ms. Bản vá nhóm kết quả giữ dạng thử nghiệm. [Số đo P2](../docs/KEYLESS-BATCH.md)
- P3/P4 có thông số lab. Lựa chọn batch và hạn mức RAM/spool production cần số liệu tải thực. [Batch](../docs/KEYLESS-BATCH.md), [Bộ nhớ](../docs/KEYLESS-MEMORY.md)
- PITR PostgreSQL đã diễn tập block khi thiếu WAL hoặc đổi timeline, và recovery thành công tới mốc kiểm được. Storage, failover và RPO/RTO production chưa xác minh. [Diễn tập](../docs/KEYLESS-PILOT.md)

## Trạng thái chạy

Lượt `go test -short -p 3 ./...` bị ngắt sau khi đầy đĩa. Nhiều integration suite vẫn khởi động Docker ở chế độ `-short`. Các container test bị bỏ lại được xác định và dọn, sau đó chạy compile toàn repo và các nhóm regression với mức song song giới hạn. Hạng mục này không có kết quả pass cho một lượt full suite duy nhất.

Quyết định vẫn là **CHƯA MERGE toàn bộ rollout**. Các bản sửa ở workspace để review theo từng nhóm.

### Lệnh nghiệm thu

- `go test -p 2 -run '^$' ./...` compile và khởi tạo 62 package sau cập nhật Testcontainers. `go vet ./...`, kiểm cú pháp Node và `git diff --check` được chạy trên cây làm việc cuối.
- `go test -race -short -p 2 -count=1` qua nhóm 32 package bổ sung được chọn theo fixture, trong đó 25 package có test. Nhóm gồm assembler, scheduler, backup, công cụ bench và diagnostics. Các package còn lại có kiểm compile và các ca integration riêng theo thay đổi.
- `go test -race -p 1 -count=1 ./internal/reader/archive` qua ca resume và purged gap trên MySQL thật sau khi sửa race.
- `go test -race -p 1 -count=1 ./internal/backup/runner -run 'Test(PlanRestore|PITR|VerifyJobReportsCancellation|ArchiveCursorSkipsNewerCopies|MySQLRecoveryWindowKeepsStandaloneSnapshotsOutside)'` qua chín ca, gồm MySQL, PostgreSQL, MongoDB và MinIO thật. PostgreSQL diễn tập thiếu WAL và đổi timeline.
- `go test -race -p 1 -count=1 ./internal/web -run 'Test(Restore|APIRestore|PostgresRestoreOffersReplayProof)'` qua cả test lỗi và thành công. Để kiểm lại giao diện, đặt `STRAND_DUMP_UI_DIR` khi chạy test PostgreSQL và truyền thư mục kết quả vào `tools/diagnostics/browser-repro/restore-postgres-pitr.mjs`.
- Lượt cuối đồng thời chọn các ca restore, PITR và legacy incremental trong `internal/backup/runner`, `internal/web` và `cmd/strand` với `-race`. Cả ba package qua test.
- `go test -count=1 ./tests/e2e -run '^TestPostSeedVerificationScopeAtCatchUp$'` qua ba tổ hợp verify và restart. Unit fingerprint, tải spill và delta qua với `-race`.
- `go test -p 1 -count=1 ./internal/backup/runner ./internal/web -run '^(TestBuiltinMethodsMySQLAndPostgres|TestJobEncryptionChoicePersistsAcrossPolicyAndJobEdits)$'` qua cả hai bài sau cập nhật Testcontainers. Artifact MinIO vẫn restore được bằng key ID cũ sau khi đổi active key.
- `govulncheck -db file:///.../data/osv ./...` chạy với snapshot Go Vulnerability Database `08d4efa` ngày 17/11/2026 vì endpoint công khai trả 403. Không còn phát hiện trong snapshot đó sau cập nhật Testcontainers và `golang.org/x/crypto` 0.59.0.
