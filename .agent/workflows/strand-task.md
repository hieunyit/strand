---
description: Làm trọn một task Strand: lập kế hoạch → viết test → code → tự kiểm → review độc lập → báo cáo
---
Đầu vào: mã task, ví dụ `/strand-task P0-05`. Đọc GEMINI.md trước. Chạy tuần tự, không bỏ bước.

## Bước 0 — Chuẩn bị
- `git switch main && git switch -c agent/<ID>` (đã có nhánh thì switch sang).
- Đọc mục <ID> trong TASKS.md và mọi mục spec mà task card liệt kê.

## Bước 1 — Vai PLANNER
- Viết reports/<ID>.md mục "Kế hoạch": hiểu task là gì (2–4 câu), file sẽ sửa, AC → test nào.
- Có điểm không rõ hoặc mâu thuẫn spec → ghi vào "Cần người dùng quyết", trạng thái BLOCKED, DỪNG.

## Bước 2 — Vai TEST WRITER (chỉ nhìn spec và task card, không viết code tính năng)
- Viết test nghiệm thu cho MỖI AC vào tests/acceptance/<ID>_test.go (hoặc <ID>.sh cho task hạ tầng).
- Chạy test, xác nhận test FAIL vì chưa có code (đây là bằng chứng test thật sự kiểm tra gì đó).
- `git add -A && git commit -m "test(<ID>): acceptance"`. Từ đây các file này bị khóa.

## Bước 3 — Vai IMPLEMENTER
- Code tối thiểu để test nghiệm thu PASS, đúng spec. Được thêm unit test riêng, không được sửa file test đã khóa.
- `git commit -m "feat(<ID>): ..."` (có thể nhiều commit).

## Bước 4 — Vai SELF-CHECK
- Chạy `bash scripts/check.sh <ID>`. Đọc file .strand/runs/<ID>-<sha>.txt nó in ra.
- Kết quả FAIL → quay lại Bước 3. Tối đa 5 vòng; quá 5 vòng → trạng thái FAIL, sang Bước 6.

## Bước 5 — Vai REVIEWER (ngữ cảnh mới)
- Nếu có thể, giao bước này cho một subagent/agent mới với prompt trong .agent/workflows/strand-review.md.
  Nếu không, tự đóng vai reviewer: chỉ đọc spec, task card và `git diff main...HEAD`, quên cách bạn đã code.
- Reviewer trả về APPROVE hoặc danh sách lỗi có trích file:dòng và mục spec.
- Có lỗi → sửa ở Bước 3, chạy lại Bước 4 và 5. Tối đa 3 vòng review; quá → trạng thái FAIL.

## Bước 6 — Báo cáo cuối
- Chạy lại `bash scripts/check.sh <ID>` trên HEAD cuối cùng.
- Hoàn thiện reports/<ID>.md theo reports/TEMPLATE.md, trích đúng đường dẫn file run và SHA.
- `git add reports/<ID>.md && git commit -m "docs(<ID>): report"`. DỪNG, không làm task khác.
