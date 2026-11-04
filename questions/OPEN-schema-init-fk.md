# Bootstrap PostgreSQL và FK có vòng

Phạm vi việc B, 2026-11-04. Không sửa spec.

**Đã chốt:** owner chọn hướng 1 — giữ FK hoạt động, dùng seed cho FK vòng/tự tham chiếu. Quyết định 19 trong `DECISIONS-2026-11-02.md`. Hướng 2 ngoài phạm vi việc B.

`spec/02-specs.md` §5 yêu cầu flush từng range trong một transaction; §6 yêu cầu giữ ranh giới Txn nguồn. Snapshot hiện chạy từng bảng. Bảng đích có FK thật được tạo trước snapshot.

Ca phản chứng: `a(id, b_id REFERENCES b)` và `b(id, a_id REFERENCES a)`, mỗi bảng có một row tham chiếu row của bảng kia. Không có thứ tự snapshot bảng nào ghi được row đầu tiên khi FK không deferred. Tự tham chiếu qua hai chunk cũng có vấn đề tương tự. Đổi thứ tự chỉ giải được đồ thị không có vòng.

Bản việc B đang kiểm tra dùng thứ tự cha trước con cho target PG; phát hiện vòng trong tập pending thì từ chối trước khi tạo DDL, yêu cầu seed nhất quán. Không tắt trigger/FK, không thêm quyền superuser. Đây là giới hạn owner đã duyệt theo quyết định 19; không tuyên bố hỗ trợ FK vòng.

Hai hướng mở rộng cần quyết định riêng:

1. Giữ giới hạn logical snapshot; seed/restore schema và dữ liệu cho thành phần có vòng.
2. Thiết kế chế độ replica cho writer/snapshot, quyền SET `session_replication_role` và kiểm FK sau bootstrap; phải xét cả trigger do người dùng chủ động đặt trên target, restart và snapshot add. Không tự triển khai hướng này trong việc B.
