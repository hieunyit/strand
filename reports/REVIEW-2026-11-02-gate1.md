# Review Gate 1: regression tests và bản sửa (2026-11-02)

Nhánh `review/gate1-fixes`. Mỗi lỗi có một test tái hiện (file `regression_test.go` của từng package,
riêng CP-4 ở `internal/checkpoint/compaction_regression_test.go`). Test được commit TRƯỚC bản sửa
(`test(review): ...` rồi `fix(review): ...`) và đã được xác nhận đỏ trên code cũ. Ngoại lệ:
`TestCP4_LargeDeltaIsCompactedNotRejected` được commit chung với bản sửa.

## Đã sửa

| # | Lỗi | Test tái hiện | Package |
|---|-----|---------------|---------|
| C1 | `Done(seq, err)` bỏ qua `err`: TXN lỗi vẫn giải phóng TXN phụ thuộc và đẩy `lwd` | `TestCO1_DoneWithErrorDoesNotReleaseDependents` | sched |
| C2 | Deadlock: gửi vào `readyCh` khi đang giữ mutex | `TestCO1_DoneDoesNotDeadlockWhenReadyQueueFull` | sched |
| P | `Done` tốn O(toàn bộ writeset) | `TestCO2_DoneCostIndependentOfLiveWriteset` | sched |
| C3 | TXN tràn đĩa mất toàn bộ giá trị cột; writeset rỗng | `TestCP1_SpilledTxnKeepsAllColumns`, `TestCO1_SpilledTxnOrdersAgainstConflictingTxns`, `TestCO1_SpilledTxnWithoutTablesRejected` | assemble, model, sched |
| C5 | TXN DDL bị gộp vào TXN sau; GTID mở chưa đóng; schema đoán `col_N` | `TestCP1_DDLTxnEmittedSeparately`, `TestCP1_GTIDInsideOpenTxnIsError`, `TestTableMapMetadataDefinesSchema` | assemble, reader |
| H | Parse GTID dạng tag gốc của MySQL 8.4 sai; phân biệt hoa thường; chuỗi hỏng bị nuốt | `TestCP2_TaggedGTIDSetNativeFormat`, `TestCP2_GTIDUUIDCaseInsensitive`, `TestCP1_ParseAppliedSetRejectsGarbage` | checkpoint |
| C6 | Instance cũ ghi đè epoch mới (`EnsureWorkerRow`, `RecordWorkerDelta` MySQL) | `TestSG1_StaleInstanceCannotResetWorkerEpoch` | writer, checkpoint |
| H | Compaction đồng bộ khi delta > 64 KB không có tác dụng; lỗi compaction nền bị nuốt | `TestCP4_LargeDeltaIsCompactedNotRejected` | checkpoint |
| P | `RangeSet.AddRange` O(n) kèm cấp phát mỗi lần (test CP-4 từ 97 s xuống 21 s, phần lớn là thời gian khởi động container) | (cùng test trên) | checkpoint |
| C4 | Writer dùng cứng `WHERE id = ?`, chỉ `Key[0]`, không quote tên bảng; UPDATE row chưa có ở target bị mất im lặng | `TestCP1_WriterUsesRealPrimaryKey`, `TestCP1_WriterRejectsUnknownCompositeKey` | writer |
| H | Strict: commit lỗi làm treo các worker khác; batch có seq không liền nhau phá CP-5; `WaitTurn` cho qua seq đã lỡ lượt | `TestCP5_StrictOrdererFailsInsteadOfHanging`, `TestCP5_StrictRejectsNonContiguousBatch`, `TestCP5_StrictWaitTurnRejectsAlreadyPassedSeq` | writer |
| C8 | `seed plan`: SD-2 chỉ xét `pHigh` của một UUID; tool version mặc định "8.4.0-6"; `Compatible`/`PrepareOK` gán cứng; clone không đối chiếu chéo; GTID hỏng bị bỏ qua | `TestSD2_PurgedBeyondArchiveIsLogGap`, `TestSD2_GapOnSecondUUIDDetected`, `TestSD1_CorruptGTIDInMetadataRejected`, `TestSD4_MissingXtrabackupInfoRejected`, `TestSD4_PhysicalBackupVersionLineMismatchRejected`, `TestSD1_PrepareOKReflectsBackupType`, `TestSD1_CloneCrossCheckMismatchRejected` | seed |
| C7 | Promote bước 3 `ApplyRemaining` là `return nil` | `TestPR1_ApplyRemainingWithoutSourceRefuses`, `TestPR1_MissingFromApplied` | promote, cmd |
| C9 (một phần) | Snapshot: PK không ánh xạ được thì `ActionApply` (bỏ qua range-state); PK `[]byte`; tìm chunk tuyến tính; rendezvous được phép nil; giả định PK là cột đầu | `TestRS1_UnmappablePKIsRejectedNotApplied`, `TestRS1_ByteSlicePKRouted`, `TestRS1_SnapshotAddRequiresRendezvous`, `TestRS1_FindChunkIsNotLinear` | snapshot |
| H | Spool xóa object trong bucket trước khi commit Pebble, crash ở giữa làm spool không đọc được | `TestSpoolTruncateCrashNeverLeavesDanglingBucketRefs` | spool |
| H | Verify panic khi PK là `[]byte` | `TestVerifyByteSlicePKDoesNotPanic` | verify |
| G1 | `validate` không kết nối source: không chặn partial JSON, row image, thiếu PK | `TestValidateChecksLiveMySQLSource` | validate, cmd |
| H | Lỗi kết nối reader bị gán exit 3; systemd `RestartPreventExitStatus=3` nên không restart | `TestReaderConnectFailureIsNotConfigRejection` | reader/mysql |

Thay đổi test cũ: chỉ một chỗ. `TestSD4_PrereleaseTool_RejectedWithoutFlag` được bổ sung input
`TargetVersion: "9.7.1"`, vì SD-4 không chứng minh được khi không biết version của target.
Không assertion nào bị nới.

Thay đổi hành vi cần biết:
- `ParseAppliedSet` trả lỗi khi gặp chuỗi hỏng, không còn trả về một tập thiếu.
- `Scheduler.Done(seq, err != nil)` làm scheduler dừng nhận TXN (`Err()`).
- Writer coi INSERT và UPDATE với row image FULL là upsert. UPDATE đổi PK = DELETE khóa cũ + upsert.
  PG partial UPDATE (TOAST) yêu cầu đúng 1 row bị ảnh hưởng.
- Writer bảng có PK nhiều cột phải gọi `SetPrimaryKey`. Bảng chưa đăng ký mà khóa có 1 cột thì vẫn
  mặc định tên `id`, để tương thích test cũ.
- Thêm trường cộng thêm (additive): `Txn.SpilledTables`, `Txn.DDL`, `reader.Event.Columns/PKColumns`,
  cờ `promote --catchup-timeout`, `snapshot.ActionReject`.

## CHƯA sửa: cần thiết kế hoặc quyết định của owner

1. **`strand sync` chưa nối pipeline.** Chưa có reader → assembler → lọc bảng → spool → scheduler
   → writer → compactor, và chưa khởi động admin socket hay metrics server. Đây là việc lớn nhất
   còn lại. Khi nối cần:
   (a) đánh dấu đã apply cho TXN bị lọc và TXN DDL, nếu không holes sẽ tăng mãi;
   (b) gọi `SetPrimaryKey` từ schema;
   (c) gọi `CommitOrderer.Skip` cho TXN đã apply (CP-2) ở chế độ strict.
2. **Test Gate 1 và test fault vẫn là test giả** (`tests/gate1`, `tests/fault`). Chúng không chạy
   process, không kill, không dùng Toxiproxy. Chỉ viết lại được sau mục 1. Tôi không xóa chúng (R9).
3. **Snapshot flush không ghi checkpoint hay trạng thái range.** Crash giữa chừng thì chạy lại từ
   đầu, và các event đã buffer có thể đã bị coi là applied. Cần thiết kế chỗ lưu trạng thái range.
4. **AppliedSet của PG coi LSN là các số nguyên liền nhau**, nên 2 commit liên tiếp sinh ~1,7 triệu
   "holes". Cần thiết kế lại trước P2: holes phải là các commit LSN đã thấy mà chưa apply.
5. ~~**Spec mâu thuẫn ở promote (MySQL)**~~: đã có quyết định, đã sửa (xem `questions/P1-17.md`).
   Câu 3 (bước 3 khi failover) còn mở. Nội dung gốc:
   (a) bước 2 bật `super_read_only` thì writer của Strand không apply được bước 3;
   (b) failover với source không truy cập được thì MySQL không có sequence, nên promote mở ghi mà
   không có điểm dừng hỏi người vận hành, dù source chưa bị fence (nguy cơ split-brain khi bị
   phân vùng mạng). Bước 3 hiện chỉ xác minh được khi source còn truy cập được.
6. Chế độ strict chưa theo quy tắc 5 (ghi row toàn cục trong TXN cuối của batch, không dùng delta).
7. Hiệu năng spool chưa sửa: fsync từng TXN; `Truncate` quét và decode toàn bộ spool, kể cả GET
   bucket. Codec spool làm mất thông tin: kiểu lạ bị đổi thành chuỗi qua `%v`, `time` mất location.
   TXN tràn đĩa được ghi vào spool chỉ bằng đường dẫn file tạm.
8. SD-4 chưa kiểm kiến trúc CPU (metadata không có). `prepare_ok` giờ phản ánh đúng
   `backup_type` nhưng plan chưa từ chối backup chưa prepare; owner quyết có nên từ chối không.
9. `deps.allow` thiếu `github.com/google/uuid`. `go.mod` ghi pebble, prometheus, cobra là `indirect`.
10. Verify: placeholder `?` không chạy trên PG; source và target đọc không cùng watermark.
    Interface `Governor` (`Decide`/`Mode`) chưa đúng spec §3.

## Tiến độ Gate 1 (cập nhật 2026-11-02, phiên bị dừng do hết hạn mức)

Đã có pipeline thật (`internal/pipeline`, `strand sync`) và bộ e2e trong `tests/e2e`. Các test
dưới đây dùng MySQL 8.4 thật và binary `strand` chạy như process riêng.

| Test e2e | Kết quả |
|---|---|
| `TestE2E_BootstrapAndStreamUnderLoad` | PASS (49.223 row giống hệt) |
| `TestG1_E2E_Kill9RandomCrashes` | PASS với 50 lần kill -9 (155.394 row). Bản 1.000 lần CHƯA chạy xong: đã dừng vì harness dùng 18 GB RAM (đã sửa bộ lấy mẫu ver, cần chạy lại) |
| `TestG1_E2E_TwoInstancesSamePipeline` | PASS (exit 4 `pipeline_locked`; instance cũ bị đóng băng thì bị `fenced`) |
| `TestG1_E2E_ValidateRejectsPartialJSON` | PASS (exit 3) |
| `TestG1_E2E_FilterChangeBetweenRestarts` | PASS (124.654 row) |
| `TestG1_E2E_SnapshotAddUnderLoadWithPKChangeAcrossRanges` | FAIL: chưa sync sau 5 phút. Nguyên nhân: PK thưa → chia chunk theo dải số sinh hàng nghìn chunk rỗng. Cần hiện thực keyset walk (02 §5) |
| `TestG1_E2E_PromoteUnderLoad`, `TestG1_E2E_RetentionRiskNoLivelock` | Đã viết, CHƯA chạy |

Lỗi thật mà e2e bắt được và đã sửa:
- CP-1: worker cắt delta theo "toàn cục ∪ delta" lúc khởi động, làm Txn bị apply lại.
- Snapshot lại sau crash bị trùng unique key: lưu plan theo chunk, snapshot mới bắt đầu từ bảng
  target rỗng, flush trong barrier.
- Deadlock InnoDB: retry.
- FK ở target trong lúc bootstrap: tắt `foreign_key_checks` trong phiên ghi.

Còn lại cho Gate 1:
- Keyset walk.
- Chạy lại kill -9 đủ 1.000 lần.
- Chạy test promote và test retention risk.
- Viết test đổi replica, TXN 50 triệu row, ma trận MySQL 9.7 và 8.0.
- Lệnh `seed plan`/`seed apply` (CLONE / XtraBackup 0 IOPS, chặn replica tự replicate).
- Fault Toxiproxy.
- Staging 1 TB và soak 72 giờ (cần môi trường thật).
- Bảng `_strand_promote` là bảng metadata thứ 4, lệch với 02 §4.

## Kết quả e2e Gate 1 (cập nhật 2026-11-02 chiều)

Log đầy đủ nằm ở scratchpad: `gate1-kill9-1000.txt`, `matrix-src80-dst84.txt`, `e2e-batch*.txt`.

**MySQL 8.4 → 8.4** (các test chạy theo lô):

- Mọi test e2e PASS.
- kill -9 **1.000 lần** (SEED=20261102): 337.234 thao tác tải, **2.218.262 row giống hệt**, ver không giảm.
- Đổi replica, seed (0 lượt đọc bảng trên source), fault mạng Toxiproxy, promote dưới tải,
  `snapshot add` + đổi PK, retention risk, đổi bộ lọc, hai instance: PASS.
- TXN 2 triệu row PASS. TXN 50 triệu row đang chạy.

**MySQL 8.0.46 (source) → 8.4 (target), smoke test theo 01 §2:**

- 13/13 PASS.
- Giảm quy mô: kill -9 100 lần, TXN 2 triệu row.

Chưa chạy: MySQL 9.7, bảng 1 TB trên staging, soak 72 giờ.
