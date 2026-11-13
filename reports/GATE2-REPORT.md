# Báo cáo Gate 2 — 2026-11-13

**Kết luận: CHƯA ĐẠT.** 10/10 test chức năng bắt buộc PASS trên PG 16/17/18, MySQL→PG, strict và relaxed. Benchmark ba governor (G2-12) chưa có số chính thức (cần máy staging; xem mục 3).

Mã: `main` @ `fa94613` (lượt chạy 2026-11-13 12:01–14:57: strict 105/105, PG 16 29/29, PG 17 29/29, LongTxn 30 phút 2/2; relaxed PG 18 trên mdb-all 83/84 + chạy lại pass). Sau đó gộp `fix/g2-flakes` (sửa sched, dừng lúc khởi động, test hot-row) và `feat/g2-bench` (công cụ benchmark); các test liên quan chạy lại PASS trên bản gộp.
Tiêu chí: spec/01-plan.md (Gate 2 — test bắt buộc; nhánh GRACE → bật/tắt), spec/04-testing.md §5.

## 1. Test chức năng bắt buộc — PG 16 / 17 / 18 và MySQL→PG

| # | Yêu cầu | Test e2e | PG 18 relaxed | PG 18 strict | PG 16 | PG 17 |
| --- | --- | --- | --- | --- | --- | --- |
| G2-2 | TOAST > 2 KB không bị NULL ghi đè | TestP2_E2E_PGToast*, PG snapshot/toast | PASS | PASS | PASS | PASS |
| G2-3 | Commit đúng tại consistent_point | TestG2_E2E_PGSeedCommitExactlyAtP | PASS | PASS | PASS | PASS |
| G2-4 | Failover rồi insert ngay không trùng key | TestG2_E2E_PromoteNoDuplicateKey{PGToPG,MySQLToPG} | PASS | PASS | PASS | PASS |
| G2-5 | Collation `_ci` → `C` (snapshot + kill -9 + verify) | TestG2_E2E_MySQLCIToPGCSnapshotKill9AndVerify | PASS | PASS | PASS | PASS |
| G2-6 | Transaction 30 phút, relaxed | TestG2_E2E_LongTxnRelaxed{PG,MySQLToPG} với STRAND_E2E_LONG_TXN=30m | PASS (2 phút); **30 phút: PASS** (PG 1840 s, MySQL→PG 1836 s) | PASS (2 phút) | PASS (2 phút) | PASS (2 phút) |
| G2-7 | Đổi PK với partial TOAST, kể cả range đang đọc | TestG2_E2E_PGToastPK{KillDuringReread,Reread30sPriority} | PASS | PASS | PASS | PASS |
| G2-8 | 100 Txn × 49.999 khóa không OOM | TestG2_E2E_BigWritesetNoOOM{PG,MySQLToPG} (ghi RSS đỉnh) | PASS | PASS | PASS | PASS |
| G2-9 | Breaker khi RLS thiếu dữ liệu → strand_sensitivity_unknown | TestG2_E2E_GraceSensitivityUnknownMetric | PASS | PASS | PASS | PASS |
| G2-10 | promote khi source mất, CACHE 1000 → dừng, hỏi (exit 10) | TestG2_E2E_PromoteSourceUnreachableCache1000{PG,MySQL} | PASS | PASS | PASS | PASS |
| G2-11 | Hot row → unverifiable_hot; lệch thật vẫn bị bắt | TestG2_E2E_HotRowUnverifiableAndPlantedMismatch, …HotRowUnverifiable (PG, MySQL→PG) | PASS | PASS | PASS | PASS |

## 2. Rủi ro (spec/01-plan.md §7) → test

| Rủi ro | Test chứng minh |
| --- | --- |
| #4 Ghi đè NULL do TOAST | G2-2 |
| #5 Đổi PK khi cột TOAST không đổi | G2-7 |
| #6 RAM writeset với transaction lớn | G2-8: RSS đỉnh (VmHWM) 465–591 MiB / giới hạn test 1024 MiB qua PG 16/17/18, strict/relaxed; writeset_entries đỉnh ≤ 849.983 / budget 2.000.000; 4.999.900 dòng |
| #7 Checkpoint phình khi transaction dài | G2-6 30 phút: row global ≤ 1225 B, row delta ≤ 322 B (≤ 64 KB), holes ≤ 22 (≤ 4096), lag ≤ 2,0 s; lwd vẫn tiến khi Txn dài đang mở |
| #9 Breaker khi RLS thiếu dữ liệu | G2-9 |
| #10 Collation đích lỏng hơn nguồn | G2-5 |
| #11 Lệch giả trên hot row | G2-11. Test package `TestRecheckDB_*HotRow*` từng chập chờn ~1/8: nguyên nhân là test (vòng cập nhật chỉ 12–15 lần/s trên máy tải nặng, không rơi vào cửa sổ kiểm lại), không phải lỗi phân loại; test nay cập nhật hot row trước mỗi lần đọc nguồn (20/20), thêm test đối chứng lệch ổn định vẫn bị báo |

## 3. Benchmark ba governor (G2-12)

- Giao thức: `reports/GATE2-BENCH-PROTOCOL.md` (cố định trước khi đo; amendment 2026-11-13 viết trước mọi lần đo, không đổi ngưỡng).
- Công cụ: `tools/g2bench` (sysbench/pgbench phía client, p99 từ log thô, khôi phục fixture giống hệt có hash, `--official` chặn khi thiếu điều kiện). Hướng dẫn: `reports/GATE2-BENCH-RUNBOOK.md`.
- Smoke trên máy dev (KHÔNG chính thức): MySQL 8.4 và PG 18 với `onoff` chạy trọn vòng, nguồn/đích khớp.
- **Kết quả chính thức: CHƯA CÓ.** Cần: máy staging chuyên dụng (≥ 8 core, 32 GB RAM, 100 GB đĩa nhanh; ~10–11 giờ mỗi engine); hiệu chỉnh mức bão hòa và pilot overhead công cụ, commit trước khi đo.
- **Phát hiện trên PG (chưa điều tra):** `grace-v2` kẹt ở R = 1 token/s, snapshot không xong trong 10 phút; breaker mở làm snapshot dừng khi chạy smoke (phải nới bằng tùy chọn chỉ dùng cho dev). Phải sửa hoặc giải thích trước khi đo chính thức trên PG; nếu không, quy tắc spec cho ra `onoff` mặc định.

## 4. Sửa trong đợt Gate 2

- `internal/sched`: `Admit` kẹt vĩnh viễn khi `readyCh` và `overflow` đầy mà consumer rút `readyCh` không gọi `Done` (lost wakeup; làm test package treo 60 phút khi máy tải nặng). Sửa: rút overflow trước mỗi lần xét và thử lại mỗi 5 ms. Test hồi quy kẹt trên code cũ, pass trên code mới; package pass ở GOMAXPROCS=1, thường, `-race`. Giữ nguyên ca MaxHoles=4096.
- `strand sync` nhận SIGINT/SIGTERM lúc đang khởi động trả exit 1 → nay exit 0; lỗi thật vẫn khác 0. Test dừng ở 14 thời điểm khởi động trên MySQL thật.

## 5. Vấn đề mở

- G2-12 chính thức (mục 3) và GRACE/breaker trên PG.
- Strict PG→PG ~280–300 Txn/s, chưa rõ nút thắt (không thuộc tiêu chí Gate 2 nhưng ảnh hưởng benchmark PG strict).
