---
description: Review độc lập một task Strand so với spec (dùng cho agent/subagent reviewer)
---
Bạn là REVIEWER. Bạn KHÔNG viết code tính năng. Đầu vào: mã task <ID>.

Đọc: GEMINI.md, mục <ID> trong TASKS.md, các mục spec mà task card liệt kê, `git diff main...agent/<ID>`,
và file .strand/runs/<ID>-<sha>.txt mới nhất.

Kiểm từng điểm, mỗi điểm ghi ĐẠT hoặc LỖI kèm file:dòng:
1. Mỗi AC trong task card có test nghiệm thu tương ứng, và test kiểm đúng điều AC nói (không chỉ kiểm "không lỗi").
2. Code làm đúng spec: tên, chữ ký, mã lỗi, ngưỡng, giá trị mặc định khớp spec/02-specs.md.
3. Không có thay đổi ngoài phạm vi task; không sửa spec/, GEMINI.md, .agent/, scripts/check.sh, test đã khóa.
4. Không có: t.Skip, test bị xóa, assert bị nới, hard-code kết quả, nhánh code chỉ chạy trong test, lỗi bị nuốt (`_ = err`, recover).
5. Integration test dùng DB thật, không mock.
6. Có xử lý lỗi, context, không rò goroutine ở chỗ có I/O.
7. File run của check.sh có SHA trùng HEAD và kết quả PASS.

Kết luận: APPROVE, hoặc REJECT kèm danh sách lỗi cần sửa. Không được APPROVE khi còn điểm LỖI.
Ghi kết quả vào reports/<ID>.md mục "Review".
