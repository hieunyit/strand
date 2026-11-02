# Quyết định của owner — 2026-11-02 (P1 / Gate 1)

Trả lời: `1A, 2B, 3B, 4A, 5 ok, 6 ok, 7 gộp` (cùng với P1-17 câu 1 và 2: hướng 1, xem `questions/P1-17.md`).

| # | Quyết định | Code | Spec owner cần sửa |
|---|---|---|---|
| 1A | `strict` theo 02 §4 quy tắc 5: không row delta; row toàn cục ghi trong transaction commit của batch (điều kiện epoch); batch gồm seq liên tiếp; worker nhận Txn theo thứ tự seq | `internal/writer/writer.go` (`DeltaRow.Global`), `internal/pipeline/pipeline.go`, `internal/pipeline/strict.go` | Không đổi nội dung. Nên ghi thêm vào 02 §6: ở `strict`, Txn được giao cho worker theo thứ tự seq (tránh deadlock khi mọi worker giữ seq lớn) |
| 2B | Snapshot logic (chế độ A, `snapshot add`) chỉ hỗ trợ PK một cột số nguyên ở P1; bảng khác phải seed (chế độ B/C). Hỗ trợ PK chuỗi/nhiều cột chuyển sang P2 | Từ chối với `type_unmappable` (exit 3) | 02 §5 "Chia chunk": thêm giới hạn P1; 01 §5 bảng P2: thêm epic "snapshot logic PK chuỗi/nhiều cột" |
| 3B | `strand pause --snapshot` chỉ dừng snapshot (stream vẫn chạy); `strand resume --snapshot-only` chỉ chạy lại snapshot; `pause`/`resume` không cờ giữ nguyên | Task 3B | 02 §10 bảng CLI: dòng `strand pause` thêm `[--snapshot]`, mô tả ngữ nghĩa hai cờ |
| 4A | Giữ bảng `_strand_promote` (1 row/pipeline, không phình) để promote chạy lại được từ bước dở | `internal/promote/state.go` | 02 §4: "4 bảng" thay cho "3 bảng", thêm DDL của `_strand_promote` |
| 5a | Phiên ghi của Strand trên target đặt `foreign_key_checks=0`; thứ tự cha–con khi stream do writeset FK bảo đảm; e2e kiểm không có row con mồ côi | `internal/pipeline/pipeline.go` (`targetDSN`) | 02 §8: thêm mục "FK ở target" |
| 5b | Instance mới (đã giữ khóa pipeline) `KILL` mọi kết nối target của instance khác cùng `pipeline_id` trước khi tăng epoch; nhận diện bằng `connectionAttributes` (`strand_pipeline`, `strand_instance`) | `killStaleInstances`, `targetDSN` | 02 §4 quy tắc 1–2 và SG-1 (02 §2); 03 §2: ghi rõ `CONNECTION_ADMIN` còn dùng để ngắt kết nối instance cũ |
| 6 | Cờ/khóa mới (cộng thêm) | xem dưới | 02 §10 |
| 7 | Failover: lần dừng exit 10 (không fence được source) hiển thị tập GTID đã apply ở target và thời điểm checkpoint cuối — phần dữ liệu mất nếu tiếp tục; xác nhận một lần bằng `--source-fenced-externally` | Task 7 | 02 §8 bước 1–3 và 05 §5 cột "Failover khẩn cấp" |
| 9 | mydumper v1.0.3-1 không ghi chế độ khóa vào metadata; seed plan bắt buộc --dump-log (log stderr của mydumper), từ chối NO_LOCK; backup cũ không log phải dùng --accept-unverified-lock-mode và nhận cảnh báo | internal/seed/mydumper.go, cmd/strand/seed.go | 02 §9: ghi rõ cách kiểm NO_LOCK và cờ mới; 02 §10 thêm --dump-log, --accept-unverified-lock-mode |

## Mục 6 — cờ và khóa cấu hình mới cần thêm vào 02 §10

| Lệnh / khóa | Thêm |
|---|---|
| `strand sync` | `--seed-manifest <path>` (mặc định `seed.json`) |
| `strand seed plan` | `--source <dsn>`, `--target <dsn>`, `--archive-until <gtid>`, `--tables`, `--out`, --dump-log <file>, --accept-unverified-lock-mode |
| `strand seed apply` | `--target <dsn>`, `--prefetch` (mặc định bật), `--spool <dir>`, `--spool-hard <bytes>`, `--spool-local-hard <bytes>`, `--overflow s3://bucket/prefix`, `--archive <dir>`, `--source <dsn>` |
| `strand plan` | `--force` (đã có trong mô tả 02 §10, nay hiện thực) |
| `strand verify` | `--rate <0..1>`, `--socket <path>` (delta qua admin socket) |
| `strand promote` | `--catchup-timeout <dur>`, `--source-fenced-externally`, `--pipeline <id>` |
| `strand pause` | `--snapshot` (3B) |
| `strand snapshot add` | `--socket <path>` |
| Cấu hình | `verify.post_seed_window` (mặc định `30m`, 02 §9) |
| Biến môi trường | `STRAND_S3_ENDPOINT`, `STRAND_S3_ACCESS_KEY`, `STRAND_S3_SECRET_KEY` (tầng bucket của spool) |
